//! Per-schema VM control loop: find-or-create-or-restart the `pg-<schema>`
//! microVM, open a raw-TCP tunnel to its Postgres, and bootstrap the database.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use deadpool_postgres::{Config as PgConfig, Pool, Runtime};
use heyo_sdk::{
    HeyoClientOptions, HeyoError, P2pTunnel, Sandbox, SandboxCreateOptions, SandboxDriver,
    SandboxSize, DEFAULT_LOCAL_BASE_URL,
};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::config::Config;
use crate::registry::SchemaEntry;

const VM_PG_PORT: u16 = 5432;

/// Fresh options targeting the local heyvmd daemon. Built per call so we don't
/// rely on `HeyoClientOptions: Clone`.
fn local_opts() -> HeyoClientOptions {
    HeyoClientOptions {
        base_url: Some(DEFAULT_LOCAL_BASE_URL.to_string()),
        ..Default::default()
    }
}

/// Bring up (or reattach to) the VM for `schema` and return a ready entry.
/// `known_id` is the sandbox id from a prior bring-up of this schema (if any);
/// reattaching by id avoids a data-loss race where a just-stopped VM is briefly
/// absent from list-by-name and we'd otherwise create a duplicate with a fresh
/// (empty) data disk.
pub async fn ensure_vm(cfg: &Config, schema: &str, known_id: Option<&str>) -> Result<Arc<SchemaEntry>> {
    let name = format!("pg-{schema}");
    let keepalive = cfg.is_keepalive(schema);

    let sandbox = resolve_sandbox(cfg, &name, keepalive, known_id).await?;

    // Pin keep-alive schemas idempotently: TTL 0 = never auto-stopped. This
    // covers a VM created before its schema was pinned (or created with a
    // non-zero TTL) — a freshly-created keep-alive VM is already TTL 0, so this
    // is a harmless no-op there. Best-effort: a failure here shouldn't block
    // serving the connection, so we warn rather than bail.
    if keepalive {
        if let Err(e) = sandbox.set_ttl(0).await {
            warn!("failed to pin keep-alive VM {name} (set_ttl 0): {e:#}");
        }
    }

    // Resolve the splice target: the VM's Postgres, reached either directly over
    // the host tap (guest_ip:5432) when the pooler shares the host with the VM,
    // or via a local iroh tunnel otherwise. Direct connect skips iroh entirely —
    // no relay dependency, lower latency, faster bring-up.
    let (target, tunnel) = if cfg.direct_connect {
        match direct_target(&sandbox).await {
            Ok(Some(addr)) => {
                info!("direct connection to {name} at {addr} (no tunnel)");
                (addr, None)
            }
            Ok(None) => {
                warn!("{name}: daemon reported no guest_ip; falling back to iroh tunnel");
                let (addr, t) = open_tunnel(cfg, &sandbox, &name).await?;
                (addr, Some(t))
            }
            Err(e) => {
                warn!("{name}: guest_ip lookup failed ({e:#}); falling back to iroh tunnel");
                let (addr, t) = open_tunnel(cfg, &sandbox, &name).await?;
                (addr, Some(t))
            }
        }
    } else {
        let (addr, t) = open_tunnel(cfg, &sandbox, &name).await?;
        (addr, Some(t))
    };

    // deadpool against the VM's default `postgres` db: probe readiness (the VM
    // status can be Running before Postgres accepts connections) and create the
    // per-schema database the client will ask for.
    let host = target.ip().to_string();
    let pool = build_pool(
        &host,
        target.port(),
        "postgres",
        &cfg.pg_user,
        cfg.pg_password.as_deref(),
    )?;
    wait_pg_ready(&pool, cfg.ready_timeout, &name).await?;
    ensure_database(&pool, schema).await?;

    Ok(Arc::new(SchemaEntry::new(
        sandbox, target, tunnel, pool, keepalive,
    )))
}

/// Find or bring up the VM. Prefers reattaching by `known_id` (a prior bring-up
/// of this schema): querying a sandbox by id is consistent, whereas a VM that
/// was just stopped is briefly missing from list-by-name — reattaching by name
/// in that window would create a *duplicate* VM with a fresh, empty data disk
/// and silently lose the schema's data. Only when there's no known id (a
/// genuinely new schema) or it was deleted do we list-by-name / create.
async fn resolve_sandbox(
    cfg: &Config,
    name: &str,
    keepalive: bool,
    known_id: Option<&str>,
) -> Result<Sandbox> {
    // 1. Reattach to the VM we last used for this schema, by id.
    if let Some(id) = known_id {
        match bring_up_existing(cfg, name, id).await {
            Ok(Some(sb)) => return Ok(sb),
            Ok(None) => info!("known VM {name} ({id}) is gone; find-or-create by name"),
            Err(e) => warn!("reattaching {name} ({id}) failed ({e:#}); find-or-create by name"),
        }
    }

    // 2. Fall back to find-by-name (first connect on a fresh pooler, or the
    //    known id was deleted).
    if let Some(info) = Sandbox::list(local_opts())
        .await
        .context("listing sandboxes")?
        .into_iter()
        .find(|s| s.name == name)
    {
        if let Some(sb) = bring_up_existing(cfg, name, &info.id).await? {
            return Ok(sb);
        }
    }

    // 3. Genuinely new schema: create it.
    create_vm(cfg, name, keepalive).await
}

/// Connect to an existing sandbox by id and force it to a running, ready state.
/// `Ok(None)` means it no longer exists (deleted out-of-band → caller creates).
///
/// Issues `start()` directly rather than checking status first. Two reasons:
/// (1) a status check via `get()` has a *side effect* on the daemon — for a
/// stopped Firecracker VM it rehydrates a handle that reports `running`, which
/// then makes the subsequent `start()` a no-op (VM stays down) and previously
/// deadlocked the daemon. (2) `start()` is the right primitive regardless: it
/// starts a stopped VM and no-ops a genuinely running one. A `NotFound` means
/// the sandbox was deleted, so the caller should create a fresh one.
async fn bring_up_existing(cfg: &Config, name: &str, id: &str) -> Result<Option<Sandbox>> {
    let sb = Sandbox::connect(id.to_string(), local_opts())
        .with_context(|| format!("connecting to VM {name} by id {id}"))?;
    info!("bringing up existing VM {name} ({id})");
    match sb.start().await {
        Ok(()) => {}
        Err(HeyoError::NotFound(_)) => return Ok(None),
        Err(e) => return Err(anyhow::Error::new(e).context(format!("starting VM {name}"))),
    }
    sb.wait_for_ready(cfg.ready_timeout)
        .await
        .with_context(|| format!("waiting for VM {name}"))?;
    Ok(Some(sb))
}

/// Create a brand-new VM for a schema (with its persistent data disk).
async fn create_vm(cfg: &Config, name: &str, keepalive: bool) -> Result<Sandbox> {
    info!("creating VM {name}{}", if keepalive { " (keep-alive)" } else { "" });
    Sandbox::create(
        SandboxCreateOptions {
            name: Some(name.to_string()),
            image: Some(cfg.image.clone()),
            driver: Some(SandboxDriver::Firecracker),
            open_ports: vec![VM_PG_PORT],
            size_class: Some(SandboxSize::Micro),
            // Persistent data disk → /dev/vdb → /workspace → PGDATA, so the
            // schema's data survives VM stop/start/restart.
            disk_size_gb: Some(cfg.data_disk_gb),
            // Always 0: the pooler owns VM lifecycle. Keep-alive schemas stay up;
            // others are stopped by the pooler's idle reaper, which tracks
            // connections — something the daemon's absolute TTL can't do.
            ttl_seconds: Some(0),
            wait_for_ready: Some(cfg.ready_timeout),
            ..Default::default()
        },
        local_opts(),
    )
    .await
    .with_context(|| format!("creating VM {name}"))
}

/// Resolve the VM's direct host-reachable Postgres address from the daemon's
/// `guest_ip` (populated for tap backends). `None` when the daemon doesn't
/// report one (non-tap backend, or not yet assigned) so the caller can fall
/// back to a tunnel.
async fn direct_target(sandbox: &Sandbox) -> Result<Option<SocketAddr>> {
    let info = sandbox.get().await.context("fetching sandbox info")?;
    let Some(ip) = info.guest_ip.as_deref().filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let addr: IpAddr = ip
        .parse()
        .with_context(|| format!("parsing guest_ip {ip:?}"))?;
    Ok(Some(SocketAddr::new(addr, VM_PG_PORT)))
}

/// Expose the VM's Postgres over an iroh tunnel and return the local splice
/// address plus the tunnel handle (aborted when dropped, so the caller must
/// hold it for the entry's lifetime). `P2pTunnel::connect` has no internal
/// timeout — when iroh's relays churn (host IP flapping on WiFi) it can block
/// for minutes — so bound the whole handshake and fail fast for a retry.
async fn open_tunnel(cfg: &Config, sandbox: &Sandbox, name: &str) -> Result<(SocketAddr, P2pTunnel)> {
    let handshake = async {
        let ticket = sandbox
            .expose_tcp(VM_PG_PORT)
            .await
            .context("exposing VM Postgres port")?;
        P2pTunnel::connect(&ticket, None)
            .await
            .context("connecting P2P tunnel")
    };
    let tunnel = match tokio::time::timeout(cfg.connect_timeout, handshake).await {
        Ok(res) => res?,
        Err(_) => bail!(
            "tunnel setup for {name} timed out after {:?} — iroh relays likely \
             churning (host network unstable); will retry on next connect",
            cfg.connect_timeout
        ),
    };
    let local_port = tunnel.local_port();
    info!("tunnel for {name} ready on 127.0.0.1:{local_port}");
    Ok((SocketAddr::from(([127, 0, 0, 1], local_port)), tunnel))
}

fn build_pool(
    host: &str,
    port: u16,
    dbname: &str,
    user: &str,
    password: Option<&str>,
) -> Result<Pool> {
    let mut pg = PgConfig::new();
    pg.host = Some(host.to_string());
    pg.port = Some(port);
    pg.dbname = Some(dbname.to_string());
    pg.user = Some(user.to_string());
    // Only set a password when configured; leaving it None keeps `trust` auth
    // working (an empty-string password would be sent as a real credential).
    pg.password = password.map(str::to_string);
    pg.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
        .context("building deadpool pool")
}

/// Retry until Postgres answers a trivial query or the timeout elapses. Logs a
/// periodic warning while it waits so a VM that boots but never brings Postgres
/// up (e.g. a missing data disk → no PGDATA) shows the reason in the log
/// instead of the caller silently blocking for the whole `timeout`.
async fn wait_pg_ready(pool: &Pool, timeout: Duration, name: &str) -> Result<()> {
    let start = Instant::now();
    let deadline = start + timeout;
    let mut last_log = start;
    loop {
        let last_err = match pool.get().await {
            Ok(client) => match client.simple_query("SELECT 1").await {
                Ok(_) => return Ok(()),
                Err(e) => e.to_string(),
            },
            Err(e) => e.to_string(),
        };
        if Instant::now() >= deadline {
            bail!("Postgres on {name} not ready within {timeout:?}: {last_err}");
        }
        if last_log.elapsed() >= Duration::from_secs(15) {
            warn!(
                "still waiting for Postgres on {name} ({:?} elapsed, timeout {timeout:?}): {last_err}",
                start.elapsed()
            );
            last_log = Instant::now();
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// `CREATE DATABASE` has no `IF NOT EXISTS`, so check the catalog first. The
/// schema name is client-supplied — it's already validated in main, and we
/// double-quote-escape it here as defense in depth (identifiers can't be bound
/// as parameters).
async fn ensure_database(pool: &Pool, schema: &str) -> Result<()> {
    let client = pool.get().await.context("checkout for db bootstrap")?;
    let exists = client
        .query_opt("SELECT 1 FROM pg_database WHERE datname = $1", &[&schema])
        .await
        .context("checking pg_database")?
        .is_some();
    if !exists {
        let quoted = schema.replace('"', "\"\"");
        client
            .batch_execute(&format!("CREATE DATABASE \"{quoted}\""))
            .await
            .with_context(|| format!("creating database {schema}"))?;
        info!("created database {schema}");
    }
    Ok(())
}

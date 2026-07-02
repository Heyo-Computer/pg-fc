//! Per-schema VM control loop: find-or-create-or-restart the `pg-<schema>`
//! microVM, open a raw-TCP tunnel to its Postgres, and bootstrap the database.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use deadpool_postgres::{Config as PgConfig, Pool, Runtime};
use heyo_sdk::{
    HeyoClientOptions, P2pTunnel, Sandbox, SandboxCreateOptions, SandboxDriver, SandboxSize,
    SandboxStatus, DEFAULT_LOCAL_BASE_URL,
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
pub async fn ensure_vm(cfg: &Config, schema: &str) -> Result<Arc<SchemaEntry>> {
    let name = format!("pg-{schema}");
    let keepalive = cfg.is_keepalive(schema);

    let existing = Sandbox::list(local_opts())
        .await
        .context("listing sandboxes")?
        .into_iter()
        .find(|s| s.name == name);

    let sandbox = match existing {
        None => {
            info!("creating VM {name}{}", if keepalive { " (keep-alive)" } else { "" });
            Sandbox::create(
                SandboxCreateOptions {
                    name: Some(name.clone()),
                    image: Some(cfg.image.clone()),
                    driver: Some(SandboxDriver::Firecracker),
                    open_ports: vec![VM_PG_PORT],
                    size_class: Some(SandboxSize::Micro),
                    // Persistent data disk → /dev/vdb → /workspace → PGDATA, so
                    // the schema's data survives VM stop/start/restart.
                    disk_size_gb: Some(cfg.data_disk_gb),
                    // Always 0: the pooler owns VM lifecycle. Keep-alive schemas
                    // stay up; others are stopped by the pooler's idle reaper,
                    // which tracks connections — something the daemon's absolute
                    // TTL can't do. A non-zero daemon TTL would just fight it.
                    ttl_seconds: Some(0),
                    wait_for_ready: Some(cfg.ready_timeout),
                    ..Default::default()
                },
                local_opts(),
            )
            .await
            .with_context(|| format!("creating VM {name}"))?
        }
        Some(info) => {
            let sb = Sandbox::connect(info.id.clone(), local_opts())
                .with_context(|| format!("connecting to VM {name}"))?;
            match info.status {
                SandboxStatus::Running => info!("reusing running VM {name}"),
                SandboxStatus::Stopped
                | SandboxStatus::Paused
                | SandboxStatus::ColdStored => {
                    info!("starting stopped VM {name}");
                    sb.start().await.with_context(|| format!("starting VM {name}"))?;
                }
                SandboxStatus::Failed => {
                    info!("restarting failed VM {name}");
                    sb.restart().await.with_context(|| format!("restarting VM {name}"))?;
                }
                SandboxStatus::Provisioning | SandboxStatus::Unknown => {
                    info!("waiting on VM {name} (status {:?})", info.status);
                }
            }
            sb.wait_for_ready(cfg.ready_timeout)
                .await
                .with_context(|| format!("waiting for VM {name}"))?;
            sb
        }
    };

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

    // Expose the VM's Postgres over an iroh tunnel and dial it locally. The
    // tunnel task is aborted when the returned P2pTunnel drops, so SchemaEntry
    // owns it for the process lifetime.
    let ticket = sandbox
        .expose_tcp(VM_PG_PORT)
        .await
        .context("exposing VM Postgres port")?;
    let tunnel = P2pTunnel::connect(&ticket, None)
        .await
        .context("connecting P2P tunnel")?;
    let local_port = tunnel.local_port();
    info!("tunnel for {name} ready on 127.0.0.1:{local_port}");

    // deadpool against the VM's default `postgres` db: probe readiness (the VM
    // status can be Running before Postgres accepts connections) and create the
    // per-schema database the client will ask for.
    let pool = build_pool(local_port, "postgres", &cfg.pg_user)?;
    wait_pg_ready(&pool, cfg.ready_timeout).await?;
    ensure_database(&pool, schema).await?;

    Ok(Arc::new(SchemaEntry::new(
        sandbox, tunnel, local_port, pool, keepalive,
    )))
}

fn build_pool(port: u16, dbname: &str, user: &str) -> Result<Pool> {
    let mut pg = PgConfig::new();
    pg.host = Some("127.0.0.1".to_string());
    pg.port = Some(port);
    pg.dbname = Some(dbname.to_string());
    pg.user = Some(user.to_string());
    pg.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
        .context("building deadpool pool")
}

/// Retry until Postgres answers a trivial query or the timeout elapses.
async fn wait_pg_ready(pool: &Pool, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let last_err = match pool.get().await {
            Ok(client) => match client.simple_query("SELECT 1").await {
                Ok(_) => return Ok(()),
                Err(e) => e.to_string(),
            },
            Err(e) => e.to_string(),
        };
        if Instant::now() >= deadline {
            bail!("Postgres not ready within {timeout:?}: {last_err}");
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

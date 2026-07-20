//! Per-schema VM control loop: find-or-create-or-restart the `pg-<schema>`
//! microVM, open a raw-TCP tunnel to its Postgres, and bootstrap the database.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use deadpool_postgres::{Config as PgConfig, Pool, Runtime};
use heyo_sdk::{
    CommandRunOptions, DEFAULT_LOCAL_BASE_URL, HeyoClientOptions, HeyoError, P2pTunnel, Sandbox,
    SandboxCreateOptions, SandboxDriver,
};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::config::Config;
use crate::registry::SchemaEntry;
use crate::s3::S3Config;

const VM_PG_PORT: u16 = 5432;

/// How long Postgres gets to answer (or at least speak) before we conclude the
/// server process is dead inside a live VM. Generous on purpose: a healthy VM
/// answers in milliseconds, and a freshly booted one binds its port within a
/// couple of seconds of HEYVM_READY — only a crashed/absent postmaster stays
/// silent this long.
const PG_PROBE_WINDOW: Duration = Duration::from_secs(15);

/// Per-attempt bound inside that window. Only guards against a connect that
/// hangs forever (the pool has no create timeout); it is not a health
/// threshold — exceeding it yields `PgProbe::Stalled`, never `Unreachable`.
const PG_PROBE_ATTEMPT: Duration = Duration::from_secs(3);

/// Fresh options targeting the local heyvmd daemon. Built per call so we don't
/// rely on `HeyoClientOptions: Clone`. Shared with the dashboard so its control
/// actions hit the same daemon.
pub(crate) fn local_opts() -> HeyoClientOptions {
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
pub async fn ensure_vm(
    cfg: &Config,
    schema: &str,
    known_id: Option<&str>,
    restore: Option<&S3Config>,
) -> Result<Arc<SchemaEntry>> {
    let name = format!("pg-{schema}");
    let keepalive = cfg.is_keepalive(schema);

    // An archived schema's VM was killed, so its stored id is dead — never try
    // to reattach; force the create-fresh path so the restore lands on a clean
    // data disk rather than (racily) on top of a stale one.
    let known_id = if restore.is_some() { None } else { known_id };
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

    let (target, tunnel, pool) = ready_pg(cfg, &sandbox, &name).await?;
    ensure_database(&pool, schema).await?;

    // Restore from S3 into the freshly-created, empty database before the entry
    // is handed to any client. A failure here must abort the bring-up: serving
    // an empty DB in place of a restored one would look like silent data loss.
    if let Some(s3) = restore {
        restore_from_s3(cfg, &sandbox, schema, s3)
            .await
            .with_context(|| format!("restoring schema {schema} from S3"))?;
    }

    let slots = client_slot_budget(&pool, &name).await;

    Ok(Arc::new(SchemaEntry::new(
        sandbox, target, tunnel, pool, keepalive, slots,
    )))
}

/// Validity window for a presigned S3 URL handed to the guest. Generous enough
/// to cover a slow upload/download of a large dump, short enough that a URL that
/// leaks (e.g. into a guest shell-history) expires quickly.
const PRESIGN_TTL: Duration = Duration::from_secs(3600);

/// Cap on the guest-side dump/restore exec. A one-workbook database dumps and
/// uploads in seconds-to-minutes; this bound only stops a wedged transfer from
/// hanging the sweep forever.
const ARCHIVE_EXEC_TIMEOUT: Duration = Duration::from_secs(1800);

/// Fixed in-guest scratch paths for the dump. One VM backs exactly one schema,
/// so a constant name is unambiguous — and unlike a schema-derived name it can't
/// be broken by a schema containing `/` or a quote.
const DUMP_PATH: &str = "/workspace/_archive.dump";
const RESTORE_PATH: &str = "/workspace/_restore.dump";

/// Dump `schema`'s database to S3 using the guest's own `pg_dump` + `curl`
/// against a pooler-presigned PUT URL. The dump bytes stream straight from the
/// guest to S3 and never transit the pooler. Dumps to a file first (not a pipe)
/// so `curl -T` sends a `Content-Length` — S3 rejects a chunked PUT.
pub async fn dump_to_s3(cfg: &Config, sandbox: &Sandbox, schema: &str, s3: &S3Config) -> Result<()> {
    let key = s3.object_key(schema);
    let url = s3.presign_put(&key, PRESIGN_TTL);
    let db = shell_squote(schema);
    let user = shell_squote(&cfg.pg_user);
    // `set -e`: any step failing aborts the script with a non-zero exit, which
    // `run_guest` turns into an error — so a failed pg_dump never uploads a
    // truncated object and reports success.
    let cmd = format!(
        "set -e; \
         pg_dump -h 127.0.0.1 -U {user} -Fc -d {db} -f {DUMP_PATH}; \
         curl -fsS -T {DUMP_PATH} '{url}'; \
         rm -f {DUMP_PATH}"
    );
    run_guest(cfg, sandbox, &cmd, "pg_dump→S3 upload").await
}

/// Restore `schema`'s database from S3 into the (already-created, empty) target
/// database, using the guest's `curl` + `pg_restore` against a presigned GET.
async fn restore_from_s3(cfg: &Config, sandbox: &Sandbox, schema: &str, s3: &S3Config) -> Result<()> {
    let key = s3.object_key(schema);
    let url = s3.presign_get(&key, PRESIGN_TTL);
    let db = shell_squote(schema);
    let user = shell_squote(&cfg.pg_user);
    let cmd = format!(
        "set -e; \
         curl -fsS '{url}' -o {RESTORE_PATH}; \
         pg_restore -h 127.0.0.1 -U {user} --no-owner --no-privileges -d {db} {RESTORE_PATH}; \
         rm -f {RESTORE_PATH}"
    );
    run_guest(cfg, sandbox, &cmd, "S3→pg_restore").await
}

/// Run one guest shell command, passing the backend Postgres password (when
/// configured) via `PGPASSWORD` in the exec environment rather than on the
/// command line — so it never lands in the guest's process list. Fails on a
/// non-zero exit, surfacing a bounded slice of the guest's output.
async fn run_guest(cfg: &Config, sandbox: &Sandbox, command: &str, what: &str) -> Result<()> {
    let env = cfg.pg_password.as_ref().map(|pw| {
        let mut m = HashMap::new();
        m.insert("PGPASSWORD".to_string(), pw.clone());
        m
    });
    let opts = CommandRunOptions {
        timeout: Some(ARCHIVE_EXEC_TIMEOUT),
        env,
        ..Default::default()
    };
    let res = sandbox
        .commands()
        .run(command, opts)
        .await
        .with_context(|| format!("{what}: guest exec failed"))?;
    if res.exit_code != 0 {
        let detail = if res.output.trim().is_empty() {
            res.stderr.trim()
        } else {
            res.output.trim()
        };
        bail!(
            "{what} failed in guest (exit {}): {}",
            res.exit_code,
            truncate(detail, 800)
        );
    }
    Ok(())
}

/// Single-quote a string for POSIX `sh`, escaping embedded single quotes as
/// `'\''`. Schema/user names are already validated (no control chars) upstream;
/// this is defense in depth so a name with a space or quote can't break out.
fn shell_squote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Trim guest output to `max` bytes (on a char boundary) so an error log can't
/// dump a whole dump-tool backtrace.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// How many client connections this VM's Postgres can actually take, read from
/// the server itself rather than assumed.
///
/// init.sh derives `max_connections` per size class, so the pooler must not
/// hardcode it: the number differs across size classes, and a VM that changes
/// class picks up a new one on its next boot. Ask the server.
///
/// The budget is what's left for *ordinary clients* after the two claims that
/// aren't theirs: `superuser_reserved_connections`, and this pooler's own
/// housekeeping pool (probes, bootstrap, stats, pre-stop CHECKPOINT), which
/// connects as superuser and so draws from the same well.
///
/// On any failure, fall back to a conservative floor rather than refusing to
/// serve — an unknown limit shouldn't take the VM down, and a low guess only
/// costs queueing.
async fn client_slot_budget(pool: &Pool, name: &str) -> usize {
    const FALLBACK_SLOTS: usize = 20;
    let read = async {
        let client = pool.get().await.ok()?;
        let max: i64 = client
            .query_one("SELECT current_setting('max_connections')::int8", &[])
            .await
            .ok()?
            .get(0);
        let reserved: i64 = client
            .query_one(
                "SELECT current_setting('superuser_reserved_connections')::int8",
                &[],
            )
            .await
            .ok()?
            .get(0);
        Some((max, reserved))
    };
    match read.await {
        Some((max, reserved)) => {
            let slots = slots_from_limits(max, reserved);
            info!(
                "{name}: admitting at most {slots} client connections \
                 (max_connections={max}, superuser_reserved={reserved}, \
                 pooler pool={POOL_MAX_SIZE})"
            );
            slots
        }
        None => {
            warn!("{name}: could not read max_connections; admitting at most {FALLBACK_SLOTS}");
            FALLBACK_SLOTS
        }
    }
}

/// Client slots left over from `max_connections` once the reserved superuser
/// slots and the pooler's own housekeeping pool are subtracted.
///
/// Saturates at 1 rather than 0: admitting nobody would make the VM useless,
/// and a guest configured this tightly is better served by letting one client
/// through at a time than by refusing every client. Never returns more than the
/// arithmetic allows — over-admitting is the exact failure this exists to stop.
fn slots_from_limits(max: i64, reserved: i64) -> usize {
    let budget = max - reserved - POOL_MAX_SIZE as i64;
    usize::try_from(budget.max(1)).unwrap_or(1)
}

/// Resolve the splice target and connection pool for a running VM's Postgres:
/// reached either directly over the host tap (guest_ip:5432) when the pooler
/// shares the host with the VM, or via a local iroh tunnel otherwise. Direct
/// connect skips iroh entirely — no relay dependency, lower latency, faster
/// bring-up.
async fn connect_pg(
    cfg: &Config,
    sandbox: &Sandbox,
    name: &str,
) -> Result<(SocketAddr, Option<P2pTunnel>, Pool)> {
    let (target, tunnel) = if cfg.direct_connect {
        match direct_target(sandbox).await {
            Ok(Some(addr)) => {
                info!("direct connection to {name} at {addr} (no tunnel)");
                (addr, None)
            }
            Ok(None) => {
                warn!("{name}: daemon reported no guest_ip; falling back to iroh tunnel");
                let (addr, t) = open_tunnel(cfg, sandbox, name).await?;
                (addr, Some(t))
            }
            Err(e) => {
                warn!("{name}: guest_ip lookup failed ({e:#}); falling back to iroh tunnel");
                let (addr, t) = open_tunnel(cfg, sandbox, name).await?;
                (addr, Some(t))
            }
        }
    } else {
        let (addr, t) = open_tunnel(cfg, sandbox, name).await?;
        (addr, Some(t))
    };

    // deadpool against the VM's default `postgres` db: used to probe readiness
    // (the VM status can be Running before Postgres accepts connections) and to
    // create the per-schema database the client will ask for.
    let host = target.ip().to_string();
    let pool = build_pool(
        &host,
        target.port(),
        "postgres",
        &cfg.pg_user,
        cfg.pg_password.as_deref(),
    )?;
    Ok((target, tunnel, pool))
}

/// Get the VM's Postgres to a ready state, power-cycling the VM if the server
/// process is dead inside it.
///
/// Postgres can crash while its VM stays alive (OOM kill, segfault): init.sh
/// runs Postgres as a background child of the PID-1 shell, so the sandbox
/// still reports Running, `start()` no-ops, and without this check every
/// connect would burn the full `ready_timeout` against a port nobody listens
/// on. Instead, probe briefly and classify what's there:
///   - answers `SELECT 1`      → ready, proceed;
///   - speaks Postgres protocol (e.g. 57P03 "the database system is starting
///     up" during WAL replay)  → the server is alive, wait out `ready_timeout`
///     like before — restarting mid-recovery would only restart recovery;
///   - stalled (accepted but never answered) → ambiguous, and a power-cycle
///     here is destructive: an ingest-loaded VM can hold a connect past the
///     probe bound while perfectly healthy. Treat it like `Responding` and
///     wait. If it really is wedged, the client gets a timeout error and the
///     next connect re-probes from scratch — recoverable, unlike a reboot
///     that kills an in-flight load;
///   - refusing                → the postmaster is gone; stop+start the VM
///     (a fresh boot re-runs init.sh, which relaunches Postgres) and wait for
///     readiness on the rebuilt connection. One cycle per connect attempt —
///     if PG still won't come up on a fresh boot, that's a real error the
///     client should see (and the next connect retries from scratch).
async fn ready_pg(
    cfg: &Config,
    sandbox: &Sandbox,
    name: &str,
) -> Result<(SocketAddr, Option<P2pTunnel>, Pool)> {
    let (target, tunnel, pool) = connect_pg(cfg, sandbox, name).await?;
    match probe_pg_window(&pool, PG_PROBE_WINDOW).await {
        PgProbe::Ready => Ok((target, tunnel, pool)),
        PgProbe::Responding(msg) => {
            info!("{name}: Postgres up but not ready yet ({msg}); waiting");
            wait_pg_ready(&pool, cfg.ready_timeout, name).await?;
            Ok((target, tunnel, pool))
        }
        PgProbe::Stalled(msg) => {
            warn!(
                "{name}: Postgres slow to answer ({msg}); waiting out \
                 ready_timeout before considering a power-cycle"
            );
            // Don't reboot on a stall alone — but don't wedge forever either.
            // A loaded server answers well inside ready_timeout; a black-holed
            // forward never answers at all. Silence for the *whole* window is
            // the evidence that separates them, so the reboot survives for the
            // dead-tunnel case it exists for without firing at a busy VM.
            if wait_pg_ready(&pool, cfg.ready_timeout, name).await.is_ok() {
                return Ok((target, tunnel, pool));
            }
            warn!("{name}: still silent after ready_timeout; power-cycling the VM");
            power_cycle(cfg, sandbox, name, pool, tunnel).await
        }
        PgProbe::Unreachable(msg) => {
            warn!(
                "{name}: Postgres unreachable inside a running VM ({msg}); \
                 power-cycling the VM"
            );
            power_cycle(cfg, sandbox, name, pool, tunnel).await
        }
    }
}

/// Stop+start the VM and reconnect. A fresh boot re-runs init.sh, which
/// relaunches Postgres and rebuilds the tunnel. One cycle per connect attempt —
/// if PG still won't come up on a fresh boot, that's a real error the client
/// should see (and the next connect retries from scratch).
///
/// Destructive: the stop is an unclean kill, so anything in flight on this VM
/// dies with it. Only call this on evidence that nothing is listening — never
/// on evidence that the server is merely slow.
async fn power_cycle(
    cfg: &Config,
    sandbox: &Sandbox,
    name: &str,
    pool: Pool,
    tunnel: Option<P2pTunnel>,
) -> Result<(SocketAddr, Option<P2pTunnel>, Pool)> {
    // Drop the stale pool/tunnel before the restart so nothing holds the old
    // forward open across the reboot.
    drop(pool);
    drop(tunnel);
    sandbox
        .stop()
        .await
        .with_context(|| format!("stopping {name} for power-cycle"))?;
    sandbox
        .start()
        .await
        .with_context(|| format!("restarting {name} after power-cycle"))?;
    sandbox
        .wait_for_ready(cfg.ready_timeout)
        .await
        .with_context(|| format!("waiting for {name} after power-cycle"))?;
    // Reconnect from scratch: the guest_ip/tunnel from before the reboot may no
    // longer be valid.
    let (target, tunnel, pool) = connect_pg(cfg, sandbox, name).await?;
    wait_pg_ready(&pool, cfg.ready_timeout, name).await?;
    info!("{name}: Postgres recovered after power-cycle");
    Ok((target, tunnel, pool))
}

/// What a bounded `SELECT 1` attempt tells us about the server behind `pool`.
pub(crate) enum PgProbe {
    Ready,
    /// Got a Postgres protocol response that isn't readiness (server error
    /// with a SQLSTATE, e.g. "starting up") — the process is alive.
    Responding(String),
    /// The attempt ran out of time with no answer either way. Ambiguous: a
    /// loaded server can take seconds to fork a backend, so this is NOT
    /// evidence the postmaster is gone. See `probe_pg`.
    Stalled(String),
    /// No protocol response at all: connection refused or closed. Nothing is
    /// listening on the port.
    Unreachable(String),
}

pub(crate) async fn probe_pg(pool: &Pool) -> PgProbe {
    use deadpool_postgres::PoolError;
    let attempt = async {
        match pool.get().await {
            Ok(client) => match client.simple_query("SELECT 1").await {
                Ok(_) => PgProbe::Ready,
                Err(e) => classify_pg_error(&e),
            },
            Err(PoolError::Backend(e)) => classify_pg_error(&e),
            // Everything else `PoolError` reports (queued past `wait`, pool
            // closed, no runtime) is a fact about *our* pool, not about the
            // VM's postmaster — a probe that never left this process is not
            // evidence the server is gone, and must never reach the verdict
            // that power-cycles it.
            Err(e) => PgProbe::Stalled(format!("pool checkout failed locally: {e}")),
        }
    };
    // The pool has no create timeout, so a black-holed TCP connect (dead iroh
    // tunnel forward) would hang `get()` — bound each attempt.
    //
    // A timeout is deliberately NOT `Unreachable`. A dead postmaster means a
    // closed port, and a closed port answers *fast* (ECONNREFUSED) — it does
    // not hang. Hanging means something accepted the connection and is slow to
    // finish it: a backend fork behind heavy checkpoint I/O, or an allocation
    // stalling under the guest's strict overcommit. Calling that "dead" is how
    // a busy-but-healthy VM used to get power-cycled mid-ingest, which is
    // strictly worse than the slowness it was reacting to.
    match tokio::time::timeout(PG_PROBE_ATTEMPT, attempt).await {
        Ok(probe) => probe,
        Err(_) => PgProbe::Stalled(format!("no answer within {PG_PROBE_ATTEMPT:?}")),
    }
}

/// A SQLSTATE means the *server* composed an error message — the postmaster is
/// alive whatever the code says. No SQLSTATE means we never got a protocol
/// reply (io error, refused, EOF): nothing is listening.
fn classify_pg_error(e: &tokio_postgres::Error) -> PgProbe {
    if e.code().is_some() {
        PgProbe::Responding(e.to_string())
    } else {
        PgProbe::Unreachable(e.to_string())
    }
}

/// Probe until the window closes: `Ready`/`Responding` short-circuit (the
/// server exists — the caller decides how long to wait for readiness). Only a
/// full window of *refusals* returns `Unreachable`; if anything in the window
/// merely stalled, the port was open at least once and `Stalled` wins, since
/// the caller must not take a destructive action on that evidence.
async fn probe_pg_window(pool: &Pool, window: Duration) -> PgProbe {
    let deadline = Instant::now() + window;
    let mut last_err = String::new();
    let mut stalled: Option<String> = None;
    loop {
        match probe_pg(pool).await {
            PgProbe::Unreachable(msg) => last_err = msg,
            PgProbe::Stalled(msg) => stalled = Some(msg),
            verdict => return verdict,
        }
        if Instant::now() >= deadline {
            return match stalled {
                Some(msg) => PgProbe::Stalled(msg),
                None => PgProbe::Unreachable(last_err),
            };
        }
        sleep(Duration::from_millis(500)).await;
    }
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
    info!(
        "creating VM {name}{}",
        if keepalive { " (keep-alive)" } else { "" }
    );
    Sandbox::create(
        SandboxCreateOptions {
            name: Some(name.to_string()),
            image: Some(cfg.image.clone()),
            driver: Some(SandboxDriver::Firecracker),
            open_ports: vec![VM_PG_PORT],
            size_class: Some(cfg.size_class),
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
async fn open_tunnel(
    cfg: &Config,
    sandbox: &Sandbox,
    name: &str,
) -> Result<(SocketAddr, P2pTunnel)> {
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

/// Cap on the pooler's own connections to a VM's Postgres.
///
/// This pool is not the client data path — client bytes are spliced straight to
/// the VM — so it only ever serves the pooler's own housekeeping: the liveness
/// probe, the one-time database bootstrap, the dashboard's stat queries, and
/// the pre-stop CHECKPOINT. A handful of slots covers all of that concurrently.
///
/// Left unset, deadpool defaults `max_size` to `logical_cpus * 2`, sized for a
/// pool that *is* the data path. That default is read off the **pooler host**,
/// which has nothing to do with the guest's `max_connections` — a 16-core host
/// yields 32, so the pooler could hold a third of a large VM's 100 connections
/// just to ask "are you alive?". Worse, `entry_alive` probes on every client
/// checkout, so a burst of client connects grows this pool straight to its cap
/// at exactly the moment the VM can least afford it, and the pool connects as
/// superuser — so it eats the reserved slots and survives while the app starves.
const POOL_MAX_SIZE: usize = 4;

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
    pg.pool = Some(deadpool_postgres::PoolConfig {
        max_size: POOL_MAX_SIZE,
        // Bound the queue for a slot. Callers treat a local checkout failure as
        // `Stalled` (never `Unreachable`), so this can only cost a probe, never
        // trigger a power-cycle. `create` bounds the TCP connect itself, which
        // otherwise has no timeout at all.
        timeouts: deadpool_postgres::Timeouts {
            wait: Some(PG_PROBE_ATTEMPT),
            create: Some(PG_PROBE_ATTEMPT),
            recycle: Some(PG_PROBE_ATTEMPT),
        },
        ..Default::default()
    });
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

#[cfg(test)]
mod tests {
    use super::*;

    fn pool_at(port: u16) -> Pool {
        build_pool("127.0.0.1", port, "postgres", "postgres", None).unwrap()
    }

    /// The pooler's pool is housekeeping-only and must not scale with the
    /// *pooler host's* core count — that number is unrelated to the guest's
    /// max_connections, and the default (logical_cpus * 2 = 32 on a 16-core
    /// host) would let the pooler hold a third of a large VM's connections
    /// just to run liveness probes.
    #[test]
    fn pool_is_capped_independently_of_host_cores() {
        let p = pool_at(5432);
        assert_eq!(
            p.status().max_size,
            POOL_MAX_SIZE,
            "pool must be explicitly capped, not inherited from host cores"
        );
        assert!(
            POOL_MAX_SIZE * 4 < 100,
            "several schema pools must still fit inside a guest's max_connections"
        );
    }

    /// A checkout that fails inside our own pool (queued past `wait`, pool
    /// closed) says nothing about the VM. It must never reach `Unreachable`,
    /// which is the verdict that power-cycles.
    #[tokio::test]
    async fn local_pool_exhaustion_is_not_unreachable() {
        // A listener that accepts but never speaks: checkouts occupy every slot
        // and stall, so further checkouts queue past `wait` and fail locally.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                std::mem::forget(sock);
            }
        });
        let pool = std::sync::Arc::new(pool_at(port));
        // Saturate every slot, then probe against the exhausted pool.
        for _ in 0..POOL_MAX_SIZE {
            let p = pool.clone();
            tokio::spawn(async move { p.get().await.map(|c| std::mem::forget(c)) });
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !matches!(probe_pg(&pool).await, PgProbe::Unreachable(_)),
            "a local pool checkout failure must not be reported as Unreachable"
        );
    }

    /// The budget must never exceed what the server will actually accept —
    /// over-admitting reintroduces the `too many clients` FATAL this exists to
    /// prevent. A guest with a tiny max_connections must clamp down, not fall
    /// back to some default larger than the server allows.
    #[test]
    fn slot_budget_never_over_admits() {
        // The `large` VM in init.sh: 100 max, 5 reserved, 4 for our pool.
        assert_eq!(slots_from_limits(100, 5), 91);
        // The `micro` VM: 25 max.
        assert_eq!(slots_from_limits(25, 5), 16);
        // Degenerate guests: clamp to 1, never to a fallback bigger than the
        // server's own limit.
        for (max, reserved) in [(10, 5), (9, 5), (5, 5), (3, 5), (1, 0), (0, 0)] {
            let slots = slots_from_limits(max, reserved);
            assert!(slots >= 1, "must admit at least one client");
            assert!(
                slots as i64 <= max.max(1),
                "slots_from_limits({max}, {reserved}) = {slots} exceeds max_connections={max}"
            );
        }
    }

    #[tokio::test]
    async fn refused_port_probes_unreachable() {
        // Bind-then-drop to find a port nothing listens on.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        // A closed port is the dead-postmaster signal: it refuses immediately.
        // This is the one verdict that may power-cycle, so it must stay exact.
        match probe_pg_window(&pool_at(port), Duration::from_secs(1)).await {
            PgProbe::Unreachable(_) => {}
            PgProbe::Ready => panic!("refused port reported Ready"),
            PgProbe::Responding(m) => panic!("refused port reported Responding: {m}"),
            PgProbe::Stalled(m) => panic!("refused port reported Stalled: {m}"),
        }
    }

    #[tokio::test]
    async fn black_holed_listener_probes_stalled_not_unreachable() {
        // Accepts TCP but never answers. Two very different things share this
        // shape: a tunnel whose far end is dead, and a healthy Postgres too
        // loaded to finish a backend fork inside the probe bound. They are
        // indistinguishable here, so the probe must report the ambiguity
        // (`Stalled`) rather than assert death — `ready_pg` resolves it by
        // waiting out ready_timeout, which only the live server survives.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                std::mem::forget(sock); // hold the socket open, say nothing
            }
        });

        match probe_pg_window(&pool_at(port), Duration::from_secs(1)).await {
            PgProbe::Stalled(_) => {}
            PgProbe::Ready => panic!("black-holed listener reported Ready"),
            PgProbe::Responding(m) => panic!("black-holed listener reported Responding: {m}"),
            PgProbe::Unreachable(m) => {
                panic!(
                    "black-holed listener reported Unreachable ({m}) — this verdict can power-cycle a VM, and an accepted-but-slow connect is exactly what a loaded server looks like"
                )
            }
        }
    }

    /// The regression that motivated `Stalled`: a warm VM that accepts but is
    /// slow must stay in the map. Evicting it drops into a re-init that
    /// power-cycles the VM, killing whatever load made it slow in the first
    /// place.
    #[tokio::test]
    async fn slow_listener_is_not_evicted_from_the_warm_path() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                std::mem::forget(sock);
            }
        });

        // `entry_alive` keeps the entry for anything that isn't Unreachable.
        assert!(
            !matches!(probe_pg(&pool_at(port)).await, PgProbe::Unreachable(_)),
            "a slow-but-listening VM must not be classified Unreachable"
        );
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

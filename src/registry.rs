//! Schema -> VM registry. One entry per schema, created once and reused.
//! A background reaper stops VMs that go idle (no connections) for too long.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use deadpool_postgres::Pool;
use heyo_sdk::{P2pTunnel, Sandbox};
use tokio::sync::{Mutex, OnceCell, OwnedSemaphorePermit, Semaphore};
use tracing::{info, warn};

use crate::config::Config;
use crate::store::{Store, StoreRecord};
use crate::vm;

/// Bound on the pre-stop CHECKPOINT the reaper issues before killing an idle
/// VM. An immediate checkpoint flushes at most shared_buffers of dirty pages
/// to virtio-SSD storage — seconds on any size class — so a longer wait means
/// something is wedged and the stop should proceed.
const PRE_STOP_CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(30);

/// A ready, warm VM for one schema. `target` is where client bytes are spliced
/// — either the VM's guest IP directly (same-host, no tunnel) or the local end
/// of an iroh tunnel. Holding `tunnel` (when present) keeps that forward alive;
/// holding `pool` keeps a bootstrap/health connection warm.
pub struct SchemaEntry {
    pub sandbox: Sandbox,
    /// Splice destination for this schema's Postgres.
    pub target: SocketAddr,
    /// Some in tunnel mode (kept alive for the entry's lifetime); None when
    /// dialing the guest IP directly.
    #[allow(dead_code)]
    pub tunnel: Option<P2pTunnel>,
    #[allow(dead_code)]
    pub pool: Pool,
    /// Exempt from idle reaping (a permanent keep-alive schema).
    pub keepalive: bool,
    /// Admission control for the VM's Postgres. The pooler splices client
    /// connections 1:1, so without a bound here the guest's `max_connections`
    /// is enforced by *Postgres*, as a `FATAL: sorry, too many clients
    /// already` on the (N+1)th client. That FATAL is what an application sees
    /// as a hard connection error mid-import, and the usual reaction — tear
    /// the pool down and retry — strands every transaction already in flight.
    ///
    /// Holding a permit for each spliced connection converts that rejection
    /// into a wait: over-eager clients queue at the pooler instead of being
    /// refused by the database. This bounds the guest; it does not multiplex
    /// (see `checkout`).
    slots: Arc<Semaphore>,
    /// What `slots` started with, for reporting (a `Semaphore` only exposes
    /// what's currently free).
    slot_limit: usize,
    /// Number of client connections currently spliced through this entry.
    active: AtomicUsize,
    /// Last time a connection started or ended. `active == 0` plus a stale
    /// `last_active` is what marks the VM idle. Refreshed at checkout so an
    /// entry handed out but not yet counted in `active` isn't reaped mid-race.
    last_active: StdMutex<Instant>,
}

impl SchemaEntry {
    pub fn new(
        sandbox: Sandbox,
        target: SocketAddr,
        tunnel: Option<P2pTunnel>,
        pool: Pool,
        keepalive: bool,
        slots: usize,
    ) -> Self {
        Self {
            sandbox,
            target,
            tunnel,
            pool,
            keepalive,
            slots: Arc::new(Semaphore::new(slots)),
            slot_limit: slots,
            active: AtomicUsize::new(0),
            last_active: StdMutex::new(Instant::now()),
        }
    }

    /// Free client slots right now (0 = the next client will queue).
    pub fn free_slots(&self) -> usize {
        self.slots.available_permits()
    }

    /// Total client slots this VM's Postgres was measured to allow.
    pub fn slot_limit(&self) -> usize {
        self.slot_limit
    }

    fn touch(&self) {
        *self.last_active.lock().unwrap() = Instant::now();
    }

    /// Live client connections currently spliced through this entry. Read-only
    /// view of the private `active` counter for the dashboard; the proxy path
    /// mutates it only through `ConnGuard`.
    pub fn active_count(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    /// How long since the last connect/disconnect on this entry.
    pub fn idle_for(&self) -> Duration {
        self.last_active.lock().unwrap().elapsed()
    }

    /// The sandbox id of the VM backing this entry.
    pub fn sandbox_id(&self) -> String {
        self.sandbox.sandbox_id().to_string()
    }

    /// True when reached over an iroh tunnel rather than a direct guest IP.
    pub fn is_tunneled(&self) -> bool {
        self.tunnel.is_some()
    }

    /// Idle = not keep-alive, no live connections, and quiet for `>= timeout`.
    fn is_idle(&self, timeout: Duration) -> bool {
        !self.keepalive
            && self.active.load(Ordering::SeqCst) == 0
            && self.last_active.lock().unwrap().elapsed() >= timeout
    }
}

/// RAII marker for one in-flight client connection. Bumps the entry's active
/// count for its lifetime and refreshes activity on both ends, so the reaper
/// never stops a VM with (or that just had) a live connection.
///
/// Also owns the entry's admission permit, so the guest's connection budget is
/// released on exactly the same event that ends the splice — including an
/// error or a panic on the proxy path. A permit leak here would silently
/// shrink the VM's usable connection count until a restart, so it must not be
/// released anywhere but `Drop`.
pub struct ConnGuard(Arc<SchemaEntry>, #[allow(dead_code)] OwnedSemaphorePermit);

impl ConnGuard {
    /// Take an admission permit, then mark the entry active. Waits up to
    /// `timeout` for a free slot; `None` means every slot is busy and the
    /// caller should fail this client rather than queue forever.
    async fn acquire(entry: Arc<SchemaEntry>, timeout: Duration) -> Option<Self> {
        let permit = match entry.slots.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                // Queueing is the whole point, but it is also the moment the
                // client stops getting what it asked for — say so. Silent
                // backpressure reads as "the pooler is slow"; this names it.
                let waited = Instant::now();
                warn!(
                    "all {} client slots busy on this VM; client is queueing \
                     (up to {timeout:?}) instead of being refused by Postgres",
                    entry.slot_limit()
                );
                let p = tokio::time::timeout(timeout, entry.slots.clone().acquire_owned())
                    .await
                    .ok()?
                    .ok()?;
                info!("client admitted after queueing {:?}", waited.elapsed());
                p
            }
        };
        entry.active.fetch_add(1, Ordering::SeqCst);
        entry.touch();
        Some(Self(entry, permit))
    }

    pub fn entry(&self) -> &SchemaEntry {
        &self.0
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.touch();
    }
}

/// Bound on the dashboard's per-VM stat queries (DB and guest-OS stats) so a
/// wedged VM can't hang a detail-page render.
const STATS_TIMEOUT: Duration = Duration::from_secs(3);

/// Live database usage for a warm entry, read over its warm pool.
pub struct DbStats {
    pub db_size_bytes: i64,
    pub backends: i32,
}

/// Live guest-OS stats for a warm, pooler-managed VM, read over the same warm
/// PG pool as [`DbStats`] — never the guest console. `/proc` reads use
/// `pg_read_file` (needs superuser or `pg_read_server_files`; the default
/// `postgres` user qualifies); disk usage runs `df` as an ordinary fork under
/// the Postgres backend via `COPY FROM PROGRAM` (`pg_execute_server_program`).
/// Each piece degrades to `None` independently, so a locked-down role still
/// shows whatever it can.
pub struct GuestStats {
    /// Guest RAM (total, available) in bytes, from `/proc/meminfo`.
    pub mem: Option<(u64, u64)>,
    /// 1/5/15-minute load averages, from `/proc/loadavg`.
    pub load: Option<(f64, f64, f64)>,
    /// Filesystem holding the Postgres data directory: (total, used,
    /// available) bytes, from `df -kP` on `current_setting('data_directory')`.
    pub disk: Option<(u64, u64, u64)>,
}

/// A plain, owned point-in-time view of one warm schema entry — no `Sandbox`,
/// `Pool`, or lock handles — safe to hand to the dashboard's render layer.
pub struct EntrySnapshot {
    pub schema: String,
    pub sandbox_id: String,
    pub target: SocketAddr,
    pub active: usize,
    /// Client slots free / total on this VM's Postgres. `free == 0` means new
    /// clients are queueing at the pooler.
    pub free_slots: usize,
    pub slot_limit: usize,
    pub idle_secs: u64,
    pub keepalive: bool,
    pub tunneled: bool,
}

pub struct SchemaRegistry {
    cfg: Config,
    // Outer Mutex guards the map only; the per-schema OnceCell serializes the
    // (slow) first VM bring-up without blocking other schemas. A failed init
    // leaves the cell empty so the next client retries.
    entries: Mutex<HashMap<String, Arc<OnceCell<Arc<SchemaEntry>>>>>,
    // Persistent schema → sandbox-id map. Outlives entry eviction and process
    // restarts, so a reconnect after a stop/reap/restart reattaches to the same
    // VM (by id) rather than creating a duplicate with a fresh, empty data disk.
    store: Store,
    // Schemas whose VM is mid-archive (dump + kill in flight). A checkout for a
    // schema in this set waits until it clears, then cold-starts — which
    // restores from S3. Guards against a client bringing a VM back up while the
    // archiver is dumping and killing it. Held for the whole archive operation.
    archiving: StdMutex<HashSet<String>>,
}

impl SchemaRegistry {
    pub fn new(cfg: Config) -> Self {
        let store = Store::load(cfg.state_file.clone());
        Self {
            cfg,
            entries: Mutex::new(HashMap::new()),
            store,
            archiving: StdMutex::new(HashSet::new()),
        }
    }

    /// Password clients must present before the pooler proxies them anywhere;
    /// `None` if `PG_VM_POOL_PASSWORD` is unset (no client auth gate).
    pub fn client_password(&self) -> Option<&str> {
        self.cfg.pg_password.as_deref()
    }

    /// The configured idle-reaping timeout (`None` when reaping is disabled), so
    /// a dashboard can label how close a warm VM is to being stopped.
    pub fn idle_timeout(&self) -> Option<Duration> {
        self.cfg.idle_timeout
    }

    /// Whether the S3 eviction tier is configured — gates the dashboard's manual
    /// "reap to S3" control.
    pub fn archive_enabled(&self) -> bool {
        self.cfg.archive.is_some()
    }

    /// Point-in-time view of every *warm* schema entry (VMs the pooler currently
    /// holds). Takes the same map lock the reaper/checkout use, but holds it only
    /// for a fast, await-free read — no meaningful contention. Stopped/reaped
    /// schemas aren't warm; pair with [`Self::store_records`] for those.
    pub async fn snapshot(&self) -> Vec<EntrySnapshot> {
        let map = self.entries.lock().await;
        map.iter()
            .filter_map(|(schema, cell)| {
                cell.get().map(|e| EntrySnapshot {
                    schema: schema.clone(),
                    sandbox_id: e.sandbox_id(),
                    target: e.target,
                    active: e.active_count(),
                    free_slots: e.free_slots(),
                    slot_limit: e.slot_limit(),
                    idle_secs: e.idle_for().as_secs(),
                    keepalive: e.keepalive,
                    tunneled: e.is_tunneled(),
                })
            })
            .collect()
    }

    /// The durable per-schema records the pooler has ever backed, surviving
    /// eviction and restarts — used to recover the schema name for a VM that's
    /// currently stopped (not warm) and to surface archived (killed) schemas
    /// that no longer appear in the daemon's inventory at all.
    pub fn store_records(&self) -> Vec<(String, StoreRecord)> {
        self.store.records()
    }

    /// Live database stats for a warm, pooler-managed VM, read over the pooler's
    /// own warm Postgres pool — the *same* safe TCP path the liveness probe uses,
    /// **not** a guest console exec, so it never disturbs the VM. `None` when the
    /// VM isn't warm or the query fails/times out.
    pub async fn db_stats(&self, sandbox_id: &str, schema: &str) -> Option<DbStats> {
        let entry = self.warm_entry(sandbox_id).await?;
        let query = async {
            let client = entry.pool.get().await.ok()?;
            let row = client
                .query_opt(
                    "SELECT pg_database_size(datname), numbackends \
                     FROM pg_stat_database WHERE datname = $1",
                    &[&schema],
                )
                .await
                .ok()??;
            Some(DbStats {
                db_size_bytes: row.get(0),
                backends: row.get(1),
            })
        };
        tokio::time::timeout(STATS_TIMEOUT, query).await.ok()?
    }

    /// Live guest-OS memory/load/disk for a warm, pooler-managed VM (see
    /// [`GuestStats`] for how each piece is read and degrades). `None` when
    /// the VM isn't warm or nothing could be read within [`STATS_TIMEOUT`].
    pub async fn guest_stats(&self, sandbox_id: &str) -> Option<GuestStats> {
        let entry = self.warm_entry(sandbox_id).await?;
        let query = async {
            let mut client = entry.pool.get().await.ok()?;
            // /proc reads: no fork, just the backend reading two pseudo-files.
            // The explicit (offset, length) form is required — /proc files
            // stat as 0 bytes, so the whole-file form reads nothing.
            let mem_load = client
                .query_opt(
                    "SELECT pg_read_file('/proc/meminfo', 0, 8192), \
                            pg_read_file('/proc/loadavg', 0, 256)",
                    &[],
                )
                .await
                .ok()
                .flatten();
            let (mem, load) = mem_load
                .map(|row| (parse_meminfo(row.get(0)), parse_loadavg(row.get(1))))
                .unwrap_or((None, None));
            let disk = df_data_dir(&mut client).await;
            (mem.is_some() || load.is_some() || disk.is_some()).then_some(GuestStats {
                mem,
                load,
                disk,
            })
        };
        tokio::time::timeout(STATS_TIMEOUT, query).await.ok()?
    }

    /// The warm entry backing `sandbox_id`, if any (brief map-lock read).
    async fn warm_entry(&self, sandbox_id: &str) -> Option<Arc<SchemaEntry>> {
        let map = self.entries.lock().await;
        map.values().find_map(|cell| {
            let e = cell.get()?;
            (e.sandbox_id() == sandbox_id).then(|| e.clone())
        })
    }

    /// Check out the entry for `schema`, bringing the VM up on first request.
    /// The returned guard keeps the VM off the reaper's radar until dropped.
    /// Concurrent callers for the same schema share one bring-up.
    pub async fn checkout(&self, schema: &str) -> Result<ConnGuard> {
        // Refresh durable activity up front so the S3 eviction sweep sees this
        // schema as recently used even long after its VM leaves the warm map
        // (the in-memory `SchemaEntry::last_active` doesn't survive that).
        self.store.touch(schema);
        loop {
            // If this schema is mid-archive (dump + kill in flight), don't race
            // the archiver by bringing the VM back up. Wait for it to clear; the
            // subsequent cold start restores from S3.
            if self.is_archiving(schema) {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            // Warm path: claim the entry under the map lock, which the reaper
            // also takes — so it can't evict this entry between its idle-check
            // and our claim. `touch()` is what does the claiming: it makes the
            // entry non-idle for a full idle_timeout, which covers the window
            // between dropping the lock and `active` actually being
            // incremented. The permit is deliberately NOT taken here — it can
            // block for `admit_timeout`, and holding the map lock across that
            // would stall checkouts for every *other* schema behind this one.
            let (cell, warm) = {
                let mut map = self.entries.lock().await;
                let cell = map
                    .entry(schema.to_string())
                    .or_insert_with(|| Arc::new(OnceCell::new()))
                    .clone();
                let warm = cell.get().inspect(|e| e.touch()).cloned();
                (cell, warm)
            };

            if let Some(entry) = warm {
                let Some(guard) = ConnGuard::acquire(entry, self.cfg.admit_timeout).await else {
                    bail!(
                        "schema {schema}: all client connection slots busy after {:?}; \
                         the VM's Postgres is at its connection limit",
                        self.cfg.admit_timeout
                    );
                };
                // A VM stopped out-of-band (manual stop) or a tunnel dropped by a
                // network change leaves a cached entry whose local tunnel still
                // accepts but never reaches Postgres — splicing to it would hang.
                // Probe first; reuse only if it actually answers, else evict and
                // fall through to re-init (which restarts the VM).
                if self.entry_alive(guard.entry()).await {
                    return Ok(guard);
                }
                warn!("schema {schema}: cached VM unreachable, restarting it");
                drop(guard);
                self.evict(schema, &cell).await;
                continue;
            }

            // Cold path: bring the VM up without holding the map lock. Concurrent
            // first-connects share one bring-up via the OnceCell (one runs
            // ensure_vm, the rest await it here). Log entry/exit with timing so a
            // slow or stuck bring-up is visible — otherwise a client just parks
            // here silently until ready_timeout, which reads as a hang in the log.
            let started = Instant::now();
            info!("schema {schema}: cold start, bringing up VM (or awaiting in-progress bring-up)");
            // Reattach to the VM we last used for this schema (survives eviction
            // and process restarts), else find-or-create by name. If the schema
            // was archived to S3 (VM killed), bring up a fresh VM and restore the
            // dump into it before serving — the stored id is dead so we don't
            // reattach.
            let record = self.store.record(schema);
            let known_id = record.as_ref().map(|r| r.sandbox_id.clone());
            let restore = match record.as_ref() {
                Some(r) if r.archived => match self.cfg.archive.as_ref() {
                    Some(a) => Some(a.s3.clone()),
                    None => bail!(
                        "schema {schema} is archived to S3, but the eviction tier is not \
                         configured (set PG_VM_POOL_ARCHIVE_AFTER_SECS + PG_VM_POOL_S3_*) — \
                         cannot restore it"
                    ),
                },
                _ => None,
            };
            match cell
                .get_or_try_init(|| {
                    vm::ensure_vm(&self.cfg, schema, known_id.as_deref(), restore.as_ref())
                })
                .await
            {
                Ok(entry) => {
                    // Remember which VM now backs this schema so a later restart
                    // reattaches to it instead of creating a duplicate. `put`
                    // also clears any `archived` flag (this is a fresh VM id), so
                    // a just-restored schema is durably marked live again.
                    self.store.put(schema, entry.sandbox.sandbox_id());
                    info!("schema {schema}: VM ready in {:?}", started.elapsed());
                    let Some(guard) =
                        ConnGuard::acquire(entry.clone(), self.cfg.admit_timeout).await
                    else {
                        bail!(
                            "schema {schema}: all client connection slots busy after {:?}; \
                             the VM's Postgres is at its connection limit",
                            self.cfg.admit_timeout
                        );
                    };
                    return Ok(guard);
                }
                Err(e) => {
                    warn!(
                        "schema {schema}: bring-up failed after {:?}: {e:#}",
                        started.elapsed()
                    );
                    return Err(e);
                }
            }
        }
    }

    /// Remove `cell` from the map iff it's still the current cell for `schema`,
    /// so a concurrent re-init that already installed a fresh cell isn't lost.
    async fn evict(&self, schema: &str, cell: &Arc<OnceCell<Arc<SchemaEntry>>>) {
        let mut map = self.entries.lock().await;
        if matches!(map.get(schema), Some(cur) if Arc::ptr_eq(cur, cell)) {
            map.remove(schema);
        }
    }

    /// Liveness probe on the warm path: is anything still listening for this
    /// entry? Catches a VM stopped out-of-band and a dead tunnel forward.
    /// Cheap on a healthy VM (a local round-trip), so it's safe per checkout.
    ///
    /// Only a *refusal* counts as dead. This used to require a successful
    /// `SELECT 1` within 3s and treat everything else — a slow answer, a
    /// server error, connection saturation — as a dead VM, which then evicted
    /// the entry and dropped into a re-init that power-cycles it. Every one of
    /// those is survivable on its own; the reboot isn't, since the pooler stops
    /// VMs with an unclean kill and takes any in-flight ingest with it. A
    /// stalled probe in particular is the *expected* reading of a VM under a
    /// heavy load, so the old check reliably killed VMs for being busy.
    async fn entry_alive(&self, entry: &SchemaEntry) -> bool {
        !matches!(
            crate::vm::probe_pg(&entry.pool).await,
            crate::vm::PgProbe::Unreachable(_)
        )
    }

    /// Spawn the background idle-reaper if an idle timeout is configured.
    pub fn spawn_reaper(self: &Arc<Self>) {
        let Some(timeout) = self.cfg.idle_timeout else {
            info!("idle reaping disabled (PG_VM_POOL_IDLE_TIMEOUT_SECS=0)");
            return;
        };
        info!("idle reaper: stopping VMs after {timeout:?} without connections");
        let registry = self.clone();
        // Check a few times per timeout window so shutdown lands close to the
        // deadline, but not so often it busies the daemon.
        let tick = (timeout / 4).max(Duration::from_secs(5));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tick).await;
                registry.reap_idle(timeout).await;
            }
        });
    }

    /// Evict and stop every idle VM. Eviction (removing the map cell) happens
    /// under the lock so a concurrent `checkout` either sees the entry before
    /// eviction (and bumps `active`, sparing it) or misses it and brings up a
    /// fresh VM. The actual stop happens after the lock is released.
    async fn reap_idle(&self, timeout: Duration) {
        let mut victims: Vec<(String, Arc<SchemaEntry>)> = Vec::new();
        {
            let mut map = self.entries.lock().await;
            map.retain(|schema, cell| match cell.get() {
                Some(entry) if entry.is_idle(timeout) => {
                    victims.push((schema.clone(), entry.clone()));
                    false
                }
                _ => true,
            });
        }
        for (schema, entry) in victims {
            info!("idle-stopping VM for schema {schema} (no connections for >= {timeout:?})");
            // Checkpoint before the kill. `sandbox.stop()` is an unclean
            // power-off (Postgres never sees a shutdown signal), and the VMs
            // run synchronous_commit=off — so without this, up to the last
            // ~600ms of acked commits ride on luck and every restart replays
            // WAL back to the previous checkpoint. One CHECKPOINT over the
            // warm pool flushes everything acked and empties the replay
            // queue, making the next boot's recovery a no-op. Best-effort:
            // if it fails or times out we stop anyway — crash recovery
            // handles it, that's the design.
            let checkpoint = async {
                let client = entry.pool.get().await?;
                client.batch_execute("CHECKPOINT").await?;
                Ok::<(), anyhow::Error>(())
            };
            match tokio::time::timeout(PRE_STOP_CHECKPOINT_TIMEOUT, checkpoint).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!("pre-stop CHECKPOINT for schema {schema} failed: {e:#}"),
                Err(_) => warn!(
                    "pre-stop CHECKPOINT for schema {schema} timed out after {PRE_STOP_CHECKPOINT_TIMEOUT:?}"
                ),
            }
            if let Err(e) = entry.sandbox.stop().await {
                warn!("failed to stop idle VM for schema {schema}: {e:#}");
            }
            // Dropping the last Arc here tears down the tunnel + pool. Data on
            // the VM's /dev/vdb persists; a later connect restarts the VM.
        }
    }

    // ---- S3 eviction tier ---------------------------------------------------

    /// Spawn the background archive sweep if the S3 eviction tier is configured.
    /// This is the slow, disk-reclaiming counterpart to the idle reaper: it
    /// offloads a long-idle schema's data to S3 and *kills* the VM (freeing its
    /// data disk), where the reaper only stops it.
    pub fn spawn_archiver(self: &Arc<Self>) {
        let Some(archive) = self.cfg.archive.clone() else {
            info!("S3 eviction disabled (PG_VM_POOL_ARCHIVE_AFTER_SECS unset/0)");
            return;
        };
        info!(
            "S3 eviction: offloading VMs idle >= {:?} to s3://{}/{} (sweep every {:?})",
            archive.archive_after, archive.s3.bucket, archive.s3.prefix, archive.sweep_interval
        );
        let registry = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(archive.sweep_interval).await;
                registry.sweep_archive(archive.archive_after).await;
            }
        });
    }

    /// One eviction pass: archive every non-keepalive schema untouched for at
    /// least `threshold`, skipping any that is currently warm-and-busy.
    async fn sweep_archive(&self, threshold: Duration) {
        let now = now_unix();
        let threshold_secs = threshold.as_secs();

        // Live cross-check: a schema warm with active connections, or one whose
        // in-memory idle clock is younger than the threshold, is not really cold
        // even if its durable `last_active` drifted stale (one long-lived
        // connection with no new checkouts). Refresh those and skip them.
        let live: HashMap<String, (usize, u64)> = self
            .snapshot()
            .await
            .into_iter()
            .map(|e| (e.schema, (e.active, e.idle_secs)))
            .collect();

        let mut candidates: Vec<String> = Vec::new();
        for (schema, rec) in self.store_records() {
            match classify_candidate(
                &rec,
                self.cfg.is_keepalive(&schema),
                now,
                threshold_secs,
                live.get(&schema).copied(),
            ) {
                SweepAction::Skip => {}
                // Warm-and-busy but durably stale: keep its clock honest so it
                // isn't re-flagged every sweep.
                SweepAction::Refresh => self.store.touch(&schema),
                SweepAction::Archive => candidates.push(schema),
            }
        }

        if candidates.is_empty() {
            return;
        }
        info!("S3 eviction sweep: {} candidate schema(s)", candidates.len());
        for schema in candidates {
            if let Err(e) = self.archive_schema(&schema).await {
                warn!("archiving schema {schema} to S3 failed (will retry next sweep): {e:#}");
            }
        }
    }

    /// Offload one schema's database to S3 and kill its VM to reclaim the disk.
    /// Also the target of the dashboard's manual "reap" button. Refuses if the
    /// VM has live client sessions. Serializes against [`Self::checkout`] via the
    /// `archiving` set so a client can't bring the VM back up mid-operation.
    pub async fn archive_schema(&self, schema: &str) -> Result<()> {
        let Some(archive) = self.cfg.archive.clone() else {
            bail!("S3 eviction tier is not configured (set PG_VM_POOL_ARCHIVE_AFTER_SECS + PG_VM_POOL_S3_*)");
        };
        if self.store.record(schema).map(|r| r.archived).unwrap_or(false) {
            bail!("schema {schema} is already archived to S3");
        }

        // Claim the archiving slot; a Drop guard clears it on every exit path so
        // checkouts stuck waiting are released even on error/panic.
        let _guard = match ArchivingGuard::claim(&self.archiving, schema) {
            Some(g) => g,
            None => bail!("schema {schema} is already being archived"),
        };

        // Evict the warm entry under the map lock, refusing if it has live
        // sessions. Because the `archiving` set was inserted *before* this lock,
        // a checkout that slips in either grabbed the entry first (active > 0 →
        // we refuse) or will see the set and wait.
        {
            let mut map = self.entries.lock().await;
            if let Some(entry) = map.get(schema).and_then(|c| c.get()) {
                let active = entry.active_count();
                if active > 0 {
                    bail!("schema {schema} has {active} live session(s); refusing to archive");
                }
            }
            map.remove(schema);
        }

        // Bring the VM up ready for pg_dump (starts it if idle-stopped, reattaches
        // by id if it's still the same VM), dump to S3, then mark archived and
        // kill. Mark before kill: if we crash between them the data is safely in
        // S3 and the store says "archived", so the next connect restores — the
        // reverse order could lose the mapping to a killed VM.
        let known_id = self.store.record(schema).map(|r| r.sandbox_id);
        let entry = vm::ensure_vm(&self.cfg, schema, known_id.as_deref(), None)
            .await
            .with_context(|| format!("bringing up VM for schema {schema} to archive it"))?;

        vm::dump_to_s3(&self.cfg, &entry.sandbox, schema, &archive.s3)
            .await
            .with_context(|| format!("dumping schema {schema} to S3"))?;

        self.store.mark_archived(schema);
        info!("schema {schema}: dumped to s3://{}/{}", archive.s3.bucket, archive.s3.object_key(schema));

        if let Err(e) = entry.sandbox.kill().await {
            // The dump is safe in S3 and the store is marked archived, so this
            // only orphans a (stopped) VM + disk — undesirable but not data loss.
            warn!("schema {schema}: archived to S3 but killing the VM failed (orphaned): {e:#}");
        } else {
            info!("schema {schema}: VM killed, disk reclaimed");
        }
        // Dropping `entry` tears down its pool/tunnel.
        Ok(())
    }

    fn is_archiving(&self, schema: &str) -> bool {
        self.archiving.lock().unwrap().contains(schema)
    }
}

/// RAII claim on the `archiving` set: inserts on `claim`, removes on `Drop`, so
/// a schema is never left stuck "archiving" if the operation errors or panics.
struct ArchivingGuard<'a> {
    set: &'a StdMutex<HashSet<String>>,
    schema: String,
}

impl<'a> ArchivingGuard<'a> {
    /// `Some` if this call inserted the schema; `None` if it was already present
    /// (another archive is in flight).
    fn claim(set: &'a StdMutex<HashSet<String>>, schema: &str) -> Option<Self> {
        if set.lock().unwrap().insert(schema.to_string()) {
            Some(Self {
                set,
                schema: schema.to_string(),
            })
        } else {
            None
        }
    }
}

impl Drop for ArchivingGuard<'_> {
    fn drop(&mut self) {
        self.set.lock().unwrap().remove(&self.schema);
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// What the archive sweep should do with one schema.
#[derive(Debug, PartialEq)]
enum SweepAction {
    /// Not a candidate (keepalive, already archived, or not idle long enough).
    Skip,
    /// Durably stale but actually live (warm with sessions, or a young in-memory
    /// idle clock) — refresh its `last_active` and leave it running.
    Refresh,
    /// Genuinely cold: offload to S3 and kill the VM.
    Archive,
}

/// Pure decision for one schema, factored out of [`SchemaRegistry::sweep_archive`]
/// so the cross-check between durable and live state is testable. `live` is the
/// schema's warm `(active_sessions, in_memory_idle_secs)` if it's in the map.
fn classify_candidate(
    rec: &StoreRecord,
    keepalive: bool,
    now: u64,
    threshold_secs: u64,
    live: Option<(usize, u64)>,
) -> SweepAction {
    if rec.archived || keepalive {
        return SweepAction::Skip;
    }
    if now.saturating_sub(rec.last_active) < threshold_secs {
        return SweepAction::Skip;
    }
    if let Some((active, idle_secs)) = live
        && (active > 0 || idle_secs < threshold_secs)
    {
        return SweepAction::Refresh;
    }
    SweepAction::Archive
}

#[cfg(test)]
mod archive_tests {
    use super::*;

    fn rec(last_active: u64, archived: bool) -> StoreRecord {
        StoreRecord {
            sandbox_id: "sb-x".into(),
            last_active,
            archived,
        }
    }

    #[test]
    fn classify_candidate_covers_the_cross_check() {
        let now = 1_000_000;
        let week = 604_800;

        // Cold and stopped (not in the warm map) → archive.
        assert_eq!(
            classify_candidate(&rec(now - week - 1, false), false, now, week, None),
            SweepAction::Archive
        );
        // Not idle long enough → skip.
        assert_eq!(
            classify_candidate(&rec(now - 10, false), false, now, week, None),
            SweepAction::Skip
        );
        // Already archived → skip (don't re-archive).
        assert_eq!(
            classify_candidate(&rec(now - week - 1, true), false, now, week, None),
            SweepAction::Skip
        );
        // Keepalive schema → never archived, however stale.
        assert_eq!(
            classify_candidate(&rec(0, false), true, now, week, None),
            SweepAction::Skip
        );
        // Durably stale but warm with a live session → refresh, don't archive
        // (a long-lived single connection with no new checkouts).
        assert_eq!(
            classify_candidate(&rec(now - week - 1, false), false, now, week, Some((1, week + 5))),
            SweepAction::Refresh
        );
        // Durably stale, warm, no sessions, but in-memory idle clock is young →
        // refresh (it's genuinely been used recently).
        assert_eq!(
            classify_candidate(&rec(now - week - 1, false), false, now, week, Some((0, 30))),
            SweepAction::Refresh
        );
        // Durably stale, warm, no sessions, and in-memory idle also past the
        // threshold → archive.
        assert_eq!(
            classify_candidate(&rec(now - week - 1, false), false, now, week, Some((0, week + 5))),
            SweepAction::Archive
        );
    }
}

/// Filesystem usage of the guest's Postgres data directory via
/// `COPY FROM PROGRAM 'df -kP …'` — `df` needs statvfs, which no `/proc`
/// read can provide. Runs in one transaction with an `ON COMMIT DROP` temp
/// table so nothing leaks onto the pooled session; any failure (no superuser,
/// no `df` in the image) rolls back and yields `None`.
async fn df_data_dir(client: &mut deadpool_postgres::Object) -> Option<(u64, u64, u64)> {
    let tx = client.transaction().await.ok()?;
    let datadir: String = tx
        .query_one("SELECT current_setting('data_directory')", &[])
        .await
        .ok()?
        .get(0);
    // The path is trusted (it's the server's own data_directory) but still
    // SQL-quoted (' → '') and shell-double-quoted for hygiene.
    let sql = format!(
        "CREATE TEMP TABLE _dash_df(line text) ON COMMIT DROP; \
         COPY _dash_df FROM PROGRAM 'df -kP \"{}\"'",
        datadir.replace('\'', "''")
    );
    tx.batch_execute(&sql).await.ok()?;
    let rows = tx.query("SELECT line FROM _dash_df", &[]).await.ok()?;
    let _ = tx.commit().await;
    parse_df(rows.iter().map(|r| r.get(0)))
}

/// Pull `MemTotal`/`MemAvailable` out of `/proc/meminfo` text → bytes.
fn parse_meminfo(s: &str) -> Option<(u64, u64)> {
    let kb = |line: &str| line.split_whitespace().nth(1)?.parse::<u64>().ok();
    let mut total = None;
    let mut avail = None;
    for line in s.lines() {
        if line.starts_with("MemTotal:") {
            total = kb(line);
        } else if line.starts_with("MemAvailable:") {
            avail = kb(line);
        }
    }
    Some((total? * 1024, avail? * 1024))
}

/// First three fields of `/proc/loadavg` (1/5/15-minute load).
fn parse_loadavg(s: &str) -> Option<(f64, f64, f64)> {
    let mut it = s.split_whitespace();
    Some((
        it.next()?.parse().ok()?,
        it.next()?.parse().ok()?,
        it.next()?.parse().ok()?,
    ))
}

/// Parse `df -kP` (POSIX portable) output → (total, used, available) bytes.
/// Finds the first data line by its all-numeric 1024-block column, so the
/// header (whose second field is "1024-blocks") is skipped regardless of row
/// order.
fn parse_df<'a>(lines: impl Iterator<Item = &'a str>) -> Option<(u64, u64, u64)> {
    for line in lines {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() >= 6 && !f[1].is_empty() && f[1].bytes().all(|b| b.is_ascii_digit()) {
            let total = f[1].parse::<u64>().ok()?;
            let used = f[2].parse::<u64>().ok()?;
            let avail = f[3].parse::<u64>().ok()?;
            return Some((total * 1024, used * 1024, avail * 1024));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meminfo_yields_total_and_available_bytes() {
        let s = "MemTotal:        8028896 kB\n\
                 MemFree:          734500 kB\n\
                 MemAvailable:    7600004 kB\n\
                 Buffers:           12345 kB\n";
        assert_eq!(parse_meminfo(s), Some((8_028_896 * 1024, 7_600_004 * 1024)));
        // Missing MemAvailable (ancient kernel) → None rather than garbage.
        assert_eq!(parse_meminfo("MemTotal: 100 kB\n"), None);
    }

    #[test]
    fn loadavg_yields_three_floats() {
        assert_eq!(
            parse_loadavg("0.52 0.30 0.18 2/213 4189\n"),
            Some((0.52, 0.30, 0.18))
        );
        assert_eq!(parse_loadavg(""), None);
    }

    #[test]
    fn df_skips_header_and_parses_first_data_line() {
        let out = [
            "Filesystem     1024-blocks    Used Available Capacity Mounted on",
            "/dev/vdb           4062912  950000   3112912      24% /workspace",
        ];
        assert_eq!(
            parse_df(out.into_iter()),
            Some((4_062_912 * 1024, 950_000 * 1024, 3_112_912 * 1024))
        );
        // Header only (df failed mid-flight) → None.
        assert_eq!(parse_df(out[..1].iter().copied()), None);
    }
}

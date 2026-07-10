//! Schema -> VM registry. One entry per schema, created once and reused.
//! A background reaper stops VMs that go idle (no connections) for too long.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use deadpool_postgres::Pool;
use heyo_sdk::{P2pTunnel, Sandbox};
use tokio::sync::{Mutex, OnceCell};
use tracing::{info, warn};

use crate::config::Config;
use crate::store::Store;
use crate::vm;

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
    ) -> Self {
        Self {
            sandbox,
            target,
            tunnel,
            pool,
            keepalive,
            active: AtomicUsize::new(0),
            last_active: StdMutex::new(Instant::now()),
        }
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
pub struct ConnGuard(Arc<SchemaEntry>);

impl ConnGuard {
    fn new(entry: Arc<SchemaEntry>) -> Self {
        entry.active.fetch_add(1, Ordering::SeqCst);
        entry.touch();
        Self(entry)
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

/// Live database usage for a warm entry, read over its warm pool.
pub struct DbStats {
    pub db_size_bytes: i64,
    pub backends: i32,
}

/// A plain, owned point-in-time view of one warm schema entry — no `Sandbox`,
/// `Pool`, or lock handles — safe to hand to the dashboard's render layer.
pub struct EntrySnapshot {
    pub schema: String,
    pub sandbox_id: String,
    pub target: SocketAddr,
    pub active: usize,
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
}

impl SchemaRegistry {
    pub fn new(cfg: Config) -> Self {
        let store = Store::load(cfg.state_file.clone());
        Self {
            cfg,
            entries: Mutex::new(HashMap::new()),
            store,
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

    /// Point-in-time view of every *warm* schema entry (VMs the pooler currently
    /// holds). Takes the same map lock the reaper/checkout use, but holds it only
    /// for a fast, await-free read — no meaningful contention. Stopped/reaped
    /// schemas aren't warm; pair with [`Self::store_entries`] for those.
    pub async fn snapshot(&self) -> Vec<EntrySnapshot> {
        let map = self.entries.lock().await;
        map.iter()
            .filter_map(|(schema, cell)| {
                cell.get().map(|e| EntrySnapshot {
                    schema: schema.clone(),
                    sandbox_id: e.sandbox_id(),
                    target: e.target,
                    active: e.active_count(),
                    idle_secs: e.idle_for().as_secs(),
                    keepalive: e.keepalive,
                    tunneled: e.is_tunneled(),
                })
            })
            .collect()
    }

    /// The durable `schema → sandbox-id` pairs the pooler has ever backed,
    /// surviving eviction and restarts — used to recover the schema name for a
    /// VM that's currently stopped (not warm).
    pub fn store_entries(&self) -> Vec<(String, String)> {
        self.store.entries()
    }

    /// Live database stats for a warm, pooler-managed VM, read over the pooler's
    /// own warm Postgres pool — the *same* safe TCP path the liveness probe uses,
    /// **not** a guest console exec, so it never disturbs the VM. `None` when the
    /// VM isn't warm or the query fails/times out.
    pub async fn db_stats(&self, sandbox_id: &str, schema: &str) -> Option<DbStats> {
        let entry = {
            let map = self.entries.lock().await;
            map.values().find_map(|cell| {
                let e = cell.get()?;
                (e.sandbox_id() == sandbox_id).then(|| e.clone())
            })
        }?;
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
        tokio::time::timeout(Duration::from_secs(3), query).await.ok()?
    }

    /// Check out the entry for `schema`, bringing the VM up on first request.
    /// The returned guard keeps the VM off the reaper's radar until dropped.
    /// Concurrent callers for the same schema share one bring-up.
    pub async fn checkout(&self, schema: &str) -> Result<ConnGuard> {
        loop {
            // Warm path: take a guard while still holding the map lock, which the
            // reaper also takes — so it can't evict this entry between its
            // idle-check and our increment. The guard also keeps `active > 0`
            // during the liveness probe below, so the reaper leaves it alone.
            let (cell, warm) = {
                let mut map = self.entries.lock().await;
                let cell = map
                    .entry(schema.to_string())
                    .or_insert_with(|| Arc::new(OnceCell::new()))
                    .clone();
                let warm = cell.get().map(|e| ConnGuard::new(e.clone()));
                (cell, warm)
            };

            if let Some(guard) = warm {
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
            // and process restarts), else find-or-create by name.
            let known_id = self.store.get(schema);
            match cell
                .get_or_try_init(|| vm::ensure_vm(&self.cfg, schema, known_id.as_deref()))
                .await
            {
                Ok(entry) => {
                    // Remember which VM now backs this schema so a later restart
                    // reattaches to it instead of creating a duplicate.
                    self.store.put(schema, entry.sandbox.sandbox_id());
                    info!("schema {schema}: VM ready in {:?}", started.elapsed());
                    return Ok(ConnGuard::new(entry.clone()));
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

    /// Timeout-bounded liveness probe: can we run `SELECT 1` on the VM's Postgres
    /// through the tunnel? Catches both a stopped VM and a dead tunnel (either
    /// makes `pool.get()`/the query hang, which the timeout bounds). Cheap on a
    /// healthy warm VM (a local round-trip), so it's safe per checkout.
    async fn entry_alive(&self, entry: &SchemaEntry) -> bool {
        let probe = async {
            let client = entry.pool.get().await?;
            client.simple_query("SELECT 1").await?;
            Ok::<(), anyhow::Error>(())
        };
        matches!(
            tokio::time::timeout(Duration::from_secs(3), probe).await,
            Ok(Ok(()))
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
            if let Err(e) = entry.sandbox.stop().await {
                warn!("failed to stop idle VM for schema {schema}: {e:#}");
            }
            // Dropping the last Arc here tears down the tunnel + pool. Data on
            // the VM's /dev/vdb persists; a later connect restarts the VM.
        }
    }
}

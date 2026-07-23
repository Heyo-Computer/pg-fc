//! Warm-spare VM pool: pre-created, pre-booted, initdb-complete VMs that a
//! cold bring-up can claim instead of paying create + boot + initdb.
//!
//! The expensive part of a cold start — creating the sandbox, booting the
//! kernel, running initdb and first-boot tuning — is identical for every
//! schema, so it can be done *ahead of time*. A background replenisher keeps
//! `PG_VM_POOL_WARM_SPARES` VMs (named `spare-pg-*`, TTL 0) booted with
//! Postgres up and an empty cluster. When a schema needs a brand-new VM
//! (first connect, or a restore from S3 whose old VM was killed), it claims a
//! spare: create the schema database on it, restore if needed, done — the
//! S3-restore path drops from create+boot+initdb+restore to just restore.
//!
//! A claimed spare **keeps its `spare-pg-*` name** (the SDK has no rename);
//! the durable registry's `schema → sandbox-id` mapping is what binds it, as
//! it already does for every VM. Consequences: the dashboard shows the spare
//! name (the schema still resolves via the registry map), and the
//! find-by-name rescue path can't find these VMs if the registry file is ever
//! lost — one more reason `PG_VM_POOL_STATE_FILE` should be an absolute,
//! durable path.
//!
//! Who is a spare, authoritatively: a *running* `spare-pg-*` sandbox whose id
//! is neither bound to a schema in the registry (the claim outlives restarts
//! through that binding) nor claimed in this process's memory. The daemon list
//! is the source of truth, so the pool needs no persistence of its own and
//! adopts surviving spares after a pooler restart.

use std::collections::HashSet;
use std::sync::Mutex as StdMutex;
use std::time::{SystemTime, UNIX_EPOCH};

use heyo_sdk::{Sandbox, SandboxStatus};
use tracing::{info, warn};

use crate::config::Config;
use crate::vm;

/// Name prefix for spare VMs. Deliberately does *not* start with `pg-`: the
/// dashboard and the find-by-name path treat `pg-<schema>` as pooler-managed,
/// and a spare must never be mistaken for (or found as) a schema VM.
pub const SPARE_PREFIX: &str = "spare-pg-";

/// Upper bound on the configured pool size — spares hold RAM and a thin disk
/// each, and a typo like `WARM_SPARES=100` shouldn't boot a fleet.
pub const MAX_SPARES: usize = 16;

pub struct SparePool {
    target: usize,
    /// Sandbox ids claimed by this process (bound to a schema, or mid-claim).
    /// In-memory only: after a restart the registry's id bindings provide the
    /// same exclusion for successful claims, and an id claimed by a bring-up
    /// that then *failed* simply returns to the pool.
    claimed: StdMutex<HashSet<String>>,
}

impl SparePool {
    pub fn new(target: usize) -> Self {
        Self {
            target: target.min(MAX_SPARES),
            claimed: StdMutex::new(HashSet::new()),
        }
    }

    /// Claim one warm spare, excluding `bound` ids (schema-bound per the
    /// registry). Returns a connected handle, or `None` when no spare is
    /// available (caller falls back to a cold create).
    pub async fn take(&self, bound: &HashSet<String>) -> Option<Sandbox> {
        let infos = match Sandbox::list(vm::local_opts()).await {
            Ok(l) => l,
            Err(e) => {
                warn!("listing sandboxes to claim a warm spare failed: {e:#}");
                return None;
            }
        };
        let id = {
            let mut claimed = self.claimed.lock().unwrap();
            let id = infos
                .iter()
                .filter(|s| {
                    s.status == SandboxStatus::Running
                        && s.name.starts_with(SPARE_PREFIX)
                        && !bound.contains(&s.id)
                        && !claimed.contains(&s.id)
                })
                .map(|s| s.id.clone())
                .next()?;
            claimed.insert(id.clone());
            id
        };
        match Sandbox::connect(id.clone(), vm::local_opts()) {
            Ok(sb) => Some(sb),
            Err(e) => {
                warn!("connecting to claimed warm spare {id} failed: {e:#}");
                self.claimed.lock().unwrap().remove(&id);
                None
            }
        }
    }

    /// One replenish pass: count available spares (running, spare-named,
    /// neither schema-bound nor claimed) and create the deficit, serially.
    /// Returns how many were created, for the supervisor's heartbeat. A
    /// creation failure ends the pass — the supervisor retries next tick, and
    /// grinding on during an outage (disk full, daemon down) helps nobody.
    pub async fn replenish(&self, cfg: &Config, bound: &HashSet<String>) -> usize {
        let infos = match Sandbox::list(vm::local_opts()).await {
            Ok(l) => l,
            Err(e) => {
                warn!("warm-spares: listing sandboxes failed; skipping pass: {e:#}");
                return 0;
            }
        };
        let available = {
            let claimed = self.claimed.lock().unwrap();
            infos
                .iter()
                .filter(|s| {
                    s.name.starts_with(SPARE_PREFIX)
                        && s.status == SandboxStatus::Running
                        && !bound.contains(&s.id)
                        && !claimed.contains(&s.id)
                })
                .count()
        };
        let deficit = self.target.saturating_sub(available);
        let mut created = 0usize;
        for _ in 0..deficit {
            let name = format!("{SPARE_PREFIX}{}", suffix());
            match vm::create_spare(cfg, &name).await {
                Ok(sb) => {
                    info!("warm-spares: created spare {name} ({})", sb.sandbox_id());
                    created += 1;
                }
                Err(e) => {
                    warn!("warm-spares: creating {name} failed; ending pass (will retry): {e:#}");
                    break;
                }
            }
        }
        if created > 0 {
            info!(
                "warm-spares: pool at {}/{} after creating {created}",
                available + created,
                self.target
            );
        }
        created
    }
}

/// Short unique-enough suffix for a spare name (time-derived; collisions are
/// rejected by the daemon's create as a duplicate name and retried next pass).
fn suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 ^ (d.as_secs() << 20))
        .unwrap_or(0);
    format!("{:08x}", (nanos ^ (std::process::id() as u64)) & 0xffff_ffff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_is_capped() {
        assert_eq!(SparePool::new(100).target, MAX_SPARES);
        assert_eq!(SparePool::new(2).target, 2);
    }

    #[test]
    fn spare_prefix_is_not_schema_shaped() {
        // The dashboard/find-by-name convention treats `pg-<schema>` as a
        // schema VM; a spare name must never parse that way.
        assert!(!SPARE_PREFIX.starts_with("pg-"));
    }
}

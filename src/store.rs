//! Tiny persistent per-schema registry: `schema -> (sandbox-id, last-active,
//! state)`.
//!
//! When the pooler brings up a VM for a schema it records the sandbox id here
//! and flushes it to disk. On the next bring-up — including after a full pooler
//! restart — it reattaches to *that* VM by id instead of finding one by name.
//! That closes a data-loss race: a VM that was just stopped is briefly absent
//! from list-by-name, and reattaching by name in that window would create a
//! duplicate VM with a fresh, empty data disk. The schema (the client's db name)
//! is the key, so the file records both the db name and its VM.
//!
//! Two more fields drive the S3 eviction tier:
//!   - `last_active`: unix seconds of the last client checkout. This survives
//!     the VM leaving the warm map (idle-stop), so the hourly archive sweep can
//!     find schemas untouched for a week even though their VM is already
//!     stopped — the in-memory `SchemaEntry::last_active` is gone by then.
//!   - `state`: `live` or `archived`. `archived` means the VM was killed and the
//!     data lives only in S3, so the next checkout must restore it before use.
//!
//! Format: one `schema\tsandbox_id\tlast_active_unix\tstate` line per entry.
//! Older 2-column files (`schema\tsandbox_id`) still parse: the missing
//! `last_active` defaults to *load time* (so an upgrade doesn't make every
//! pre-existing schema instantly eligible for eviction) and state to `live`.
//! Schema names are validated upstream to contain no control chars (so never a
//! tab or newline), so this needs no escaping.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use tracing::{info, warn};

/// `touch()` bumps `last_active` in memory on every client checkout, but only
/// flushes to disk when the on-disk value is older than this — the sweep reads
/// the in-memory value, so disk freshness only matters across a pooler restart,
/// where minutes of staleness are harmless against a week-long threshold. Keeps
/// a busy schema from fsync-storming the registry on every connect.
const FLUSH_DEBOUNCE_SECS: u64 = 60;

/// A durable, owned view of one schema's registry entry.
#[derive(Clone)]
pub struct StoreRecord {
    pub sandbox_id: String,
    /// Unix seconds of the last client checkout for this schema.
    pub last_active: u64,
    /// True once the VM has been dumped to S3 and killed — the data is only in
    /// S3 and the next checkout must restore it.
    pub archived: bool,
}

/// Internal map value: a [`StoreRecord`] plus the `last_active` value currently
/// on disk, used to debounce flushes.
struct Rec {
    sandbox_id: String,
    last_active: u64,
    archived: bool,
    flushed_last_active: u64,
}

impl Rec {
    fn view(&self) -> StoreRecord {
        StoreRecord {
            sandbox_id: self.sandbox_id.clone(),
            last_active: self.last_active,
            archived: self.archived,
        }
    }
}

pub struct Store {
    path: PathBuf,
    map: Mutex<HashMap<String, Rec>>,
}

impl Store {
    /// Load the store from `path`. A missing file starts empty; a partially
    /// corrupt file keeps whatever lines parse (never fatal — a lost mapping
    /// only costs us a find-by-name on next connect).
    pub fn load(path: PathBuf) -> Self {
        let now = now_unix();
        let map = match std::fs::read_to_string(&path) {
            Ok(s) => parse(&s, now),
            Err(_) => HashMap::new(),
        };
        if !map.is_empty() {
            info!(
                "loaded {} schema→VM mapping(s) from {}",
                map.len(),
                path.display()
            );
        }
        Store {
            path,
            map: Mutex::new(map),
        }
    }

    /// The full durable record for `schema`, if any.
    pub fn record(&self, schema: &str) -> Option<StoreRecord> {
        self.map.lock().unwrap().get(schema).map(Rec::view)
    }

    /// Every `(schema, record)` known to the store — the durable list of schemas
    /// the pooler has backed, including those whose VM is stopped or archived.
    pub fn records(&self) -> Vec<(String, StoreRecord)> {
        self.map
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.view()))
            .collect()
    }

    /// Record `schema -> id` (a fresh, live VM) and flush to disk. Refreshes
    /// `last_active` and clears any `archived` flag. Best-effort: a write
    /// failure is logged, not fatal. Skips the write when the mapping is
    /// unchanged and still live (then it behaves like a debounced `touch`).
    pub fn put(&self, schema: &str, id: &str) {
        let now = now_unix();
        let snapshot = {
            let mut map = self.map.lock().unwrap();
            match map.get_mut(schema) {
                Some(r) if r.sandbox_id == id && !r.archived => {
                    // Unchanged live mapping: just a debounced activity bump.
                    r.last_active = now;
                    if now.saturating_sub(r.flushed_last_active) >= FLUSH_DEBOUNCE_SECS {
                        r.flushed_last_active = now;
                        Some(serialize(&map))
                    } else {
                        None
                    }
                }
                _ => {
                    map.insert(
                        schema.to_string(),
                        Rec {
                            sandbox_id: id.to_string(),
                            last_active: now,
                            archived: false,
                            flushed_last_active: now,
                        },
                    );
                    Some(serialize(&map))
                }
            }
        };
        self.maybe_write(snapshot);
    }

    /// Bump `last_active` for `schema` to now (in memory), flushing only past
    /// [`FLUSH_DEBOUNCE_SECS`]. No-op if the schema isn't known yet.
    pub fn touch(&self, schema: &str) {
        let now = now_unix();
        let snapshot = {
            let mut map = self.map.lock().unwrap();
            let Some(r) = map.get_mut(schema) else {
                return;
            };
            r.last_active = now;
            if now.saturating_sub(r.flushed_last_active) >= FLUSH_DEBOUNCE_SECS {
                r.flushed_last_active = now;
                Some(serialize(&map))
            } else {
                None
            }
        };
        self.maybe_write(snapshot);
    }

    /// Mark `schema` archived (VM killed, data only in S3) and flush. The
    /// archived flag is *cleared* by [`Self::put`] when a fresh VM id is recorded
    /// after a restore — that's the same event that makes the data live again.
    pub fn mark_archived(&self, schema: &str) {
        let snapshot = {
            let mut map = self.map.lock().unwrap();
            let Some(r) = map.get_mut(schema) else {
                return;
            };
            r.archived = true;
            Some(serialize(&map))
        };
        self.maybe_write(snapshot);
    }

    fn maybe_write(&self, snapshot: Option<String>) {
        if let Some(snapshot) = snapshot
            && let Err(e) = write_atomic(&self.path, &snapshot)
        {
            warn!(
                "failed to persist pooler registry to {}: {e:#}",
                self.path.display()
            );
        }
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Parse the TSV, tolerating the legacy 2-column format. `now` fills in a
/// missing `last_active` so upgraded entries start their eviction clock fresh.
fn parse(s: &str, now: u64) -> HashMap<String, Rec> {
    s.lines()
        .filter_map(|line| {
            let mut f = line.split('\t');
            let schema = f.next()?;
            let id = f.next()?;
            if schema.is_empty() || id.is_empty() {
                return None;
            }
            let last_active = f.next().and_then(|v| v.parse::<u64>().ok()).unwrap_or(now);
            let archived = matches!(f.next(), Some("archived"));
            Some((
                schema.to_string(),
                Rec {
                    sandbox_id: id.to_string(),
                    last_active,
                    archived,
                    flushed_last_active: last_active,
                },
            ))
        })
        .collect()
}

fn serialize(map: &HashMap<String, Rec>) -> String {
    let mut out = String::new();
    for (k, v) in map {
        let state = if v.archived { "archived" } else { "live" };
        out.push_str(k);
        out.push('\t');
        out.push_str(&v.sandbox_id);
        out.push('\t');
        out.push_str(&v.last_active.to_string());
        out.push('\t');
        out.push_str(state);
        out.push('\n');
    }
    out
}

/// Write via a temp file + rename so a crash mid-write can't corrupt the store.
fn write_atomic(path: &Path, contents: &str) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut f =
            std::fs::File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        f.write_all(contents.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path).with_context(|| format!("renaming into {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_new_four_column_format() {
        let map = parse("wb1\tsb-1\t1700000000\tlive\nwb2\tsb-2\t1700000500\tarchived\n", 999);
        let wb1 = map.get("wb1").unwrap();
        assert_eq!(wb1.sandbox_id, "sb-1");
        assert_eq!(wb1.last_active, 1_700_000_000);
        assert!(!wb1.archived);
        let wb2 = map.get("wb2").unwrap();
        assert!(wb2.archived);
        assert_eq!(wb2.last_active, 1_700_000_500);
    }

    #[test]
    fn legacy_two_column_format_defaults_to_now_and_live() {
        // The pre-eviction on-disk format. Missing last_active must default to
        // load time — NOT 0, or every upgraded schema would look week-stale and
        // get mass-archived on the first sweep after upgrade.
        let map = parse("wb\tsb-legacy\n", 12345);
        let r = map.get("wb").unwrap();
        assert_eq!(r.sandbox_id, "sb-legacy");
        assert_eq!(r.last_active, 12345);
        assert!(!r.archived);
    }

    #[test]
    fn round_trips_through_serialize() {
        let map = parse("a\tsb-a\t100\tlive\nb\tsb-b\t200\tarchived\n", 0);
        let reparsed = parse(&serialize(&map), 0);
        assert_eq!(reparsed.get("a").unwrap().sandbox_id, "sb-a");
        assert_eq!(reparsed.get("a").unwrap().last_active, 100);
        assert!(!reparsed.get("a").unwrap().archived);
        assert!(reparsed.get("b").unwrap().archived);
        assert_eq!(reparsed.get("b").unwrap().last_active, 200);
    }

    #[test]
    fn put_touch_mark_clear_flow() {
        let dir = std::env::temp_dir().join(format!("pgvmpool-store-{}", std::process::id()));
        let path = dir.join("registry.tsv");
        let _ = std::fs::remove_file(&path);
        let store = Store::load(path.clone());

        store.put("wb", "sb-1");
        let r = store.record("wb").unwrap();
        assert_eq!(r.sandbox_id, "sb-1");
        assert!(!r.archived);

        store.mark_archived("wb");
        assert!(store.record("wb").unwrap().archived);

        // Reload from disk: archived state survives a restart.
        let reloaded = Store::load(path.clone());
        assert!(reloaded.record("wb").unwrap().archived);

        // Recording a fresh VM id (a restore) clears the archived flag — the
        // schema is live again.
        reloaded.put("wb", "sb-2");
        let r = reloaded.record("wb").unwrap();
        assert!(!r.archived);
        assert_eq!(r.sandbox_id, "sb-2");

        let _ = std::fs::remove_file(&path);
    }
}

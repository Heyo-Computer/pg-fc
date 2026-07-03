//! Tiny persistent `schema -> sandbox-id` map.
//!
//! When the pooler brings up a VM for a schema it records the sandbox id here
//! and flushes it to disk. On the next bring-up — including after a full pooler
//! restart — it reattaches to *that* VM by id instead of finding one by name.
//! That closes a data-loss race: a VM that was just stopped is briefly absent
//! from list-by-name, and reattaching by name in that window would create a
//! duplicate VM with a fresh, empty data disk. The schema (the client's db name)
//! is the key, so the file records both the db name and its VM.
//!
//! Format: one `schema\tsandbox_id` line per entry. Schema names are validated
//! upstream to contain no control chars (so never a tab or newline), so this
//! needs no escaping.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use tracing::{info, warn};

pub struct Store {
    path: PathBuf,
    map: Mutex<HashMap<String, String>>,
}

impl Store {
    /// Load the store from `path`. A missing file starts empty; a partially
    /// corrupt file keeps whatever lines parse (never fatal — a lost mapping
    /// only costs us a find-by-name on next connect).
    pub fn load(path: PathBuf) -> Self {
        let map = match std::fs::read_to_string(&path) {
            Ok(s) => parse(&s),
            Err(_) => HashMap::new(),
        };
        if !map.is_empty() {
            info!("loaded {} schema→VM mapping(s) from {}", map.len(), path.display());
        }
        Store {
            path,
            map: Mutex::new(map),
        }
    }

    /// The sandbox id last known to back `schema`, if any.
    pub fn get(&self, schema: &str) -> Option<String> {
        self.map.lock().unwrap().get(schema).cloned()
    }

    /// Record `schema -> id` and flush to disk. Best-effort: a write failure is
    /// logged, not fatal (the in-memory map still serves this process). Skips
    /// the write when the mapping is unchanged.
    pub fn put(&self, schema: &str, id: &str) {
        let snapshot = {
            let mut map = self.map.lock().unwrap();
            if map.get(schema).map(String::as_str) == Some(id) {
                return;
            }
            map.insert(schema.to_string(), id.to_string());
            serialize(&map)
        };
        if let Err(e) = write_atomic(&self.path, &snapshot) {
            warn!(
                "failed to persist pooler registry to {}: {e:#}",
                self.path.display()
            );
        }
    }
}

fn parse(s: &str) -> HashMap<String, String> {
    s.lines()
        .filter_map(|line| line.split_once('\t'))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn serialize(map: &HashMap<String, String>) -> String {
    let mut out = String::new();
    for (k, v) in map {
        out.push_str(k);
        out.push('\t');
        out.push_str(v);
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

//! Schema -> VM registry. One entry per schema, created once and reused.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use deadpool_postgres::Pool;
use heyo_sdk::{P2pTunnel, Sandbox};
use tokio::sync::{Mutex, OnceCell};

use crate::config::Config;
use crate::vm;

/// A ready, warm VM for one schema. Holding `tunnel` keeps the iroh forward
/// alive; holding `pool` keeps a bootstrap/health connection warm.
pub struct SchemaEntry {
    #[allow(dead_code)]
    pub sandbox: Sandbox,
    #[allow(dead_code)]
    pub tunnel: P2pTunnel,
    /// Local 127.0.0.1 port the tunnel listens on — the splice target.
    pub local_port: u16,
    #[allow(dead_code)]
    pub pool: Pool,
}

pub struct SchemaRegistry {
    cfg: Config,
    // Outer Mutex guards the map only; the per-schema OnceCell serializes the
    // (slow) first VM bring-up without blocking other schemas. A failed init
    // leaves the cell empty so the next client retries.
    entries: Mutex<HashMap<String, Arc<OnceCell<Arc<SchemaEntry>>>>>,
}

impl SchemaRegistry {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Get the entry for `schema`, bringing the VM up on first request.
    /// Concurrent callers for the same schema share one bring-up.
    pub async fn get_or_init(&self, schema: &str) -> Result<Arc<SchemaEntry>> {
        let cell = {
            let mut map = self.entries.lock().await;
            map.entry(schema.to_string())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let entry = cell
            .get_or_try_init(|| vm::ensure_vm(&self.cfg, schema))
            .await?;
        Ok(entry.clone())
    }
}

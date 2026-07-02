//! pg-vm-pool — a per-schema Postgres pooler over Heyo microVMs.
//!
//! Listens on one Postgres endpoint. The database name in each client's startup
//! packet selects a schema; the pooler lazily boots (or restarts/reuses) the
//! `pg-<schema>` Firecracker VM, tunnels to its Postgres, and splices the
//! connection through. One isolated VM per schema, behind a single URL.

mod config;
mod proxy;
mod registry;
mod startup;
mod vm;

use std::sync::Arc;

use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use config::Config;
use registry::SchemaRegistry;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,pg_vm_pool=info")),
        )
        .init();

    let cfg = Config::from_env()?;
    let listen_addr = cfg.listen_addr;
    let registry = Arc::new(SchemaRegistry::new(cfg));
    registry.spawn_reaper();

    let listener = TcpListener::bind(listen_addr).await?;
    info!("pg-vm-pool listening on {listen_addr}");

    loop {
        let (sock, peer) = listener.accept().await?;
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(sock, registry).await {
                warn!("connection {peer} closed: {e:#}");
            }
        });
    }
}

async fn handle_conn(mut client: TcpStream, registry: Arc<SchemaRegistry>) -> Result<()> {
    let info = startup::read_startup(&mut client).await?;
    let schema = info.database.clone();
    if !is_valid_schema(&schema) {
        anyhow::bail!("rejecting invalid schema name {schema:?}");
    }
    info!("client requested schema {schema}");

    // Hold the guard for the whole connection: it keeps the VM off the idle
    // reaper's radar until the client disconnects.
    let guard = registry.checkout(&schema).await?;
    proxy::splice(client, guard.entry(), &info.raw).await
}

/// Conservative guard on the client-supplied schema name: it becomes both a
/// Postgres database identifier and part of the VM name, so cap length (PG's
/// 63-byte identifier limit) and reject control characters.
fn is_valid_schema(s: &str) -> bool {
    !s.is_empty() && s.len() <= 63 && s.chars().all(|c| !c.is_control())
}

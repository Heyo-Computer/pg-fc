//! pg-vm-pool — a per-schema Postgres pooler over Heyo microVMs.
//!
//! Listens on one Postgres endpoint. The database name in each client's startup
//! packet selects a schema; the pooler lazily boots (or restarts/reuses) the
//! `pg-<schema>` Firecracker VM, tunnels to its Postgres, and splices the
//! connection through. One isolated VM per schema, behind a single URL.

mod auth;
mod config;
mod dashboard;
mod proxy;
mod registry;
mod startup;
mod store;
mod tls;
mod vm;

use std::sync::Arc;

use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use config::Config;
use registry::SchemaRegistry;
use tls::TlsReloader;

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
    // Client-facing TLS: certbot (or any external renewer) owns the PEM files;
    // the reloader picks up rotations without a restart. Built before the
    // registry so a bad cert fails startup fast.
    let tls = match (cfg.tls_cert.clone(), cfg.tls_key.clone()) {
        (Some(cert), Some(key)) => {
            info!("TLS enabled (cert={}, hot-reload on change)", cert.display());
            Some(Arc::new(TlsReloader::new(cert, key)?))
        }
        _ => {
            info!("TLS disabled (set PG_VM_POOL_TLS_CERT/KEY to enable)");
            None
        }
    };
    if cfg.pg_password.is_some() && tls.is_none() && !listen_addr.ip().is_loopback() {
        warn!(
            "PG_VM_POOL_PASSWORD is set but TLS is not and PG_VM_POOL_LISTEN \
             ({listen_addr}) is not loopback — client passwords will cross \
             the network in cleartext; set PG_VM_POOL_TLS_CERT/KEY"
        );
    }
    // Pull the dashboard settings out before `cfg` is moved into the registry.
    let dashboard_cfg = cfg.dashboard.clone();
    let registry = Arc::new(SchemaRegistry::new(cfg));
    registry.spawn_reaper();

    // Optional admin dashboard: enabled only when PG_VM_POOL_DASHBOARD_LISTEN is
    // set. Runs in its own task sharing the registry, so it never blocks the PG
    // accept loop below.
    if let Some(dash) = dashboard_cfg {
        if !dash.listen.ip().is_loopback() && dash.basic_auth.is_none() {
            warn!(
                "dashboard bound to non-loopback {} without basic auth — anyone who \
                 can reach it can stop/resize every VM; set PG_VM_POOL_DASHBOARD_USER/\
                 PASSWORD or bind a loopback address",
                dash.listen
            );
        }
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = dashboard::serve(dash, registry).await {
                warn!("dashboard exited: {e:#}");
            }
        });
    }

    let listener = TcpListener::bind(listen_addr).await?;
    info!("pg-vm-pool listening on {listen_addr}");

    loop {
        let (sock, peer) = listener.accept().await?;
        let registry = registry.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(sock, registry, tls).await {
                warn!("connection {peer} closed: {e:#}");
            }
        });
    }
}

async fn handle_conn(
    client: TcpStream,
    registry: Arc<SchemaRegistry>,
    tls: Option<Arc<TlsReloader>>,
) -> Result<()> {
    let (mut client, info) = startup::read_startup(client, tls.as_deref()).await?;
    if let Some(password) = registry.client_password() {
        auth::require_password(&mut client, password).await?;
    }
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

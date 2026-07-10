//! Optional server-side-rendered admin dashboard for the pooler.
//!
//! Runs in-process (a `tokio::spawn` in `main`) so it can read the pooler's
//! live in-memory session counts (`SchemaRegistry::snapshot`) alongside the
//! daemon's VM inventory (`Sandbox::list`). Enabled only when
//! `PG_VM_POOL_DASHBOARD_LISTEN` is set; gated behind HTTP Basic auth when
//! `PG_VM_POOL_DASHBOARD_USER`/`PASSWORD` are configured.
//!
//! It lists every heyvmd sandbox with power state, allocated size (vCPU/RAM),
//! uptime, and live pooler sessions; `/sandboxes` is the same data as a paged,
//! searchable view (the daemon's list endpoint has no server-side paging, so
//! the filter/page slice happens in-process and only the visible page is
//! joined and rendered). On the detail page it also shows live DB
//! size/backends read over the pooler's own warm PG pool. It drives
//! stop/start/reboot/resize on any VM, and tails the pooler / heyvmd / per-VM
//! Postgres logs.
//!
//! Guest-console access (SDK `commands()` exec) goes through the VM's PID-1
//! serial-console shell on this image and can halt the VM, so the browsable
//! pages (index + detail) perform **no** guest access — only daemon reads and
//! safe PG-pool queries. The one guest exec, the per-VM Postgres log tail, is
//! confined to its own explicitly-navigated `/logs/vm/{id}` page. Every daemon
//! and guest call is timeout-bounded so one wedged VM can't hang a request.

mod auth;
mod error;
mod handlers;
mod logs;
mod model;
mod router;
mod state;
mod views;

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tracing::info;

use crate::config::DashboardConfig;
use crate::registry::SchemaRegistry;
use state::DashState;

/// Bind the dashboard's HTTP listener and serve until the process exits. Shares
/// the pooler's `Arc<SchemaRegistry>` for live session data.
pub async fn serve(cfg: DashboardConfig, registry: Arc<SchemaRegistry>) -> Result<()> {
    let addr = cfg.listen;
    let state = DashState {
        registry,
        cfg: Arc::new(cfg),
    };
    let app = router::build(state);
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding dashboard on {addr}"))?;
    info!("dashboard listening on {addr}");
    axum::serve(listener, app)
        .await
        .context("dashboard server error")?;
    Ok(())
}

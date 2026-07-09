//! Merge the daemon's VM inventory with the pooler's live session snapshot and
//! its durable schema↔VM map into a flat list of rows for rendering.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use heyo_sdk::{Sandbox, SandboxStatus};

use crate::vm;

use super::state::DashState;
use super::sysinfo::{self, ResourceUsage};

const LIST_TIMEOUT: Duration = Duration::from_secs(10);
/// Cap on concurrent guest usage probes so a host with many VMs doesn't open a
/// probe to every one of them at once.
const USAGE_CONCURRENCY: usize = 6;

/// One VM as shown in the dashboard: daemon facts left-joined with pooler state.
pub struct VmRow {
    pub id: String,
    pub name: String,
    /// Recovered schema (warm entry → store → `pg-` name prefix), if any.
    pub schema: Option<String>,
    /// True when this VM is one the pooler manages (`pg-<schema>`).
    pub pool_managed: bool,
    pub status: SandboxStatus,
    pub size_class: Option<String>,
    pub uptime_secs: u64,
    pub ttl_seconds: Option<u64>,
    pub guest_ip: Option<String>,
    pub error_message: Option<String>,
    /// Live client sessions through the pooler; `None` when no warm entry exists.
    pub live_sessions: Option<usize>,
    pub idle_secs: Option<u64>,
    pub keepalive: bool,
    /// Where the pooler splices client bytes (warm entries only).
    pub target: Option<std::net::SocketAddr>,
    /// True when reached over an iroh tunnel rather than a direct guest IP.
    pub tunneled: Option<bool>,
    /// CPU/memory/disk from the guest; `None` for non-running or unreachable VMs.
    pub usage: Option<ResourceUsage>,
}

impl VmRow {
    pub fn is_running(&self) -> bool {
        self.status == SandboxStatus::Running
    }
}

/// Build the full VM list: list sandboxes, join the registry snapshot + store,
/// then fetch guest resource usage for running VMs in bounded parallel.
pub async fn build_rows(st: &DashState) -> Result<Vec<VmRow>> {
    let list = tokio::time::timeout(LIST_TIMEOUT, Sandbox::list(vm::local_opts()))
        .await
        .context("listing sandboxes timed out")?
        .context("listing sandboxes")?;

    // Warm entries: sandbox_id → live snapshot.
    let snap: HashMap<String, _> = st
        .registry
        .snapshot()
        .await
        .into_iter()
        .map(|e| (e.sandbox_id.clone(), e))
        .collect();

    // Durable schema names for VMs that aren't currently warm: sandbox_id → schema.
    let store: HashMap<String, String> = st
        .registry
        .store_entries()
        .into_iter()
        .map(|(schema, id)| (id, schema))
        .collect();

    let mut rows: Vec<VmRow> = list
        .into_iter()
        .map(|info| {
            let entry = snap.get(&info.id);
            let schema = entry
                .map(|e| e.schema.clone())
                .or_else(|| store.get(&info.id).cloned())
                .or_else(|| info.name.strip_prefix("pg-").map(str::to_string));
            let pool_managed = schema.is_some() || info.name.starts_with("pg-");
            VmRow {
                id: info.id.clone(),
                name: info.name.clone(),
                schema,
                pool_managed,
                status: info.status.clone(),
                size_class: info.size_class.clone(),
                uptime_secs: info.uptime_secs,
                ttl_seconds: info.ttl_seconds,
                guest_ip: info.guest_ip.clone(),
                error_message: info.error_message.clone(),
                live_sessions: entry.map(|e| e.active),
                idle_secs: entry.map(|e| e.idle_secs),
                keepalive: entry.map(|e| e.keepalive).unwrap_or(false),
                target: entry.map(|e| e.target),
                tunneled: entry.map(|e| e.tunneled),
                usage: None,
            }
        })
        .collect();

    // Fetch usage only for running VMs, in bounded parallel — a hung or stopped
    // VM yields None (rendered "—") rather than failing or stalling the page.
    let running: Vec<(usize, String)> = rows
        .iter()
        .enumerate()
        .filter(|(_, r)| r.is_running())
        .map(|(i, r)| (i, r.id.clone()))
        .collect();

    let usages = stream::iter(running)
        .map(|(i, id)| async move { (i, sysinfo::fetch_by_id(&id).await.ok()) })
        .buffer_unordered(USAGE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    for (i, usage) in usages {
        rows[i].usage = usage;
    }

    // Pooler-managed VMs first, then alphabetical by name.
    rows.sort_by(|a, b| {
        b.pool_managed
            .cmp(&a.pool_managed)
            .then_with(|| a.name.cmp(&b.name))
    });

    Ok(rows)
}

/// Find one VM row by id (rebuilds the full list; fine for an admin tool with a
/// handful of VMs).
pub async fn find_row(st: &DashState, id: &str) -> Result<Option<VmRow>> {
    Ok(build_rows(st).await?.into_iter().find(|r| r.id == id))
}

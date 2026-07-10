//! Merge the daemon's VM inventory with the pooler's live session snapshot and
//! its durable schema↔VM map into a flat list of rows for rendering.
//!
//! Note: this deliberately performs **no guest-console access** (no `commands()`
//! exec, no `files()` read). Those go through the VM's PID-1 serial-console
//! shell on this image and can halt the VM, so the dashboard only reads the
//! daemon's inventory here and live DB stats over the pooler's own PG pool.
//!
//! We fetch `/deployed-sandboxes` as raw JSON (via the SDK's HTTP client) rather
//! than through `Sandbox::list`, because the typed `SandboxInfo` drops the
//! concrete `cpus`/`memory`/`disk_size_gb` the daemon stores — and those (not
//! the mostly-unpopulated `size_class`) are what actually describe a VM's size.
//! `/deployed-sandboxes` alone misses persisted-but-stopped sandboxes the
//! daemon hasn't loaded into memory (see [`fetch_inventory`]), so the inventory
//! also walks `GET /sandboxes/inactive`.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::http::Method;
use heyo_sdk::{HeyoClient, RequestOptions, SandboxStatus};
use serde::Deserialize;
use tracing::warn;

use crate::registry::EntrySnapshot;
use crate::vm;

use super::state::DashState;

const LIST_TIMEOUT: Duration = Duration::from_secs(10);

/// Default / maximum rows per page on the browse-all view. The cap bounds how
/// much HTML one request can render no matter what `?per=` a client asks for.
pub const DEFAULT_PER: usize = 25;
pub const MAX_PER: usize = 200;

/// State filter values, in display order. Each is a [`status_str`] label; the
/// sentinel [`STATE_ALL`] disables the filter. The view defaults to showing
/// only running sandboxes — on a busy host the stopped tail dwarfs the live
/// set.
pub const STATE_FILTERS: [&str; 7] = [
    "running",
    "provisioning",
    "stopped",
    "paused",
    "cold-stored",
    "failed",
    "unknown",
];
pub const DEFAULT_STATE: &str = "running";
pub const STATE_ALL: &str = "all";

/// The subset of the daemon's raw sandbox record we render. Extra JSON fields
/// are ignored; missing ones default, so this tolerates daemon schema drift.
#[derive(Deserialize)]
struct RawSandbox {
    id: String,
    #[serde(default)]
    name: String,
    status: SandboxStatus,
    #[serde(default)]
    size_class: Option<String>,
    #[serde(default)]
    uptime_secs: u64,
    #[serde(default)]
    ttl_seconds: Option<u64>,
    #[serde(default)]
    guest_ip: Option<String>,
    #[serde(default)]
    error_message: Option<String>,
    #[serde(default)]
    status_changed_at: Option<String>,
    #[serde(default)]
    image: Option<String>,
    #[serde(default)]
    region: Option<String>,
    // Concrete resource config (from the sandbox record / sandbox.yaml).
    #[serde(default)]
    cpus: Option<u32>,
    #[serde(default)]
    memory: Option<u64>, // bytes
    #[serde(default)]
    disk_size_gb: Option<u32>,
}

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
    pub cpus: Option<u32>,
    pub memory_bytes: Option<u64>,
    pub disk_size_gb: Option<u32>,
    pub image: Option<String>,
    pub region: Option<String>,
    pub status_changed_at: Option<String>,
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
}

impl VmRow {
    pub fn is_running(&self) -> bool {
        self.status == SandboxStatus::Running
    }
}

/// Page envelope of `GET /sandboxes/inactive`. The records are the daemon's
/// *native* `SandboxInfo` shape, not the compat shape `/deployed-sandboxes`
/// returns — close enough that [`RawSandbox`]'s serde defaults absorb the
/// differences (no `uptime_secs`/`status_changed_at`; same lowercase status
/// strings).
#[derive(Deserialize)]
struct InactivePage {
    #[serde(default)]
    sandboxes: Vec<RawSandbox>,
    #[serde(default)]
    next_cursor: Option<String>,
}

const INACTIVE_PAGE_SIZE: usize = 200;
/// Backstop on the inactive-list walk so a pathological daemon answer can't
/// spin the dashboard: 25 pages × 200 = 5000 sandboxes per request, far above
/// any real inventory.
const MAX_INACTIVE_PAGES: usize = 25;

/// Fetch the daemon's full sandbox inventory, no guest access anywhere:
///
/// * `GET /deployed-sandboxes` — sandboxes in the daemon's in-memory map.
///   This is *not* everything: for the Firecracker backend the daemon only
///   reconciles **running** VMs from disk into memory, so a persisted sandbox
///   stopped before the last daemon restart never shows up here.
/// * `GET /sandboxes/inactive` (cursor-paged) — exactly those on-disk,
///   not-in-memory sandboxes, reported as `stopped`. Best-effort: an older
///   daemon without the route, or a walk timeout, degrades to the deployed
///   list rather than failing the page.
///
/// Every HTTP call is bounded by [`LIST_TIMEOUT`]; the walk is bounded by
/// [`MAX_INACTIVE_PAGES`]. Neither endpoint filters server-side, so callers
/// slice/filter in-process.
async fn fetch_inventory() -> Result<Vec<RawSandbox>> {
    let client = HeyoClient::new(vm::local_opts()).context("building heyo client")?;
    let mut all = tokio::time::timeout(
        LIST_TIMEOUT,
        client.request::<Vec<RawSandbox>>(
            Method::GET,
            "/deployed-sandboxes",
            None::<&()>,
            RequestOptions::default(),
        ),
    )
    .await
    .context("listing sandboxes timed out")?
    .context("listing sandboxes")?;

    // The two lists are disjoint by construction (inactive = on disk but not
    // in memory), but a sandbox can start between the two reads — dedupe by
    // id, keeping the live deployed record.
    let mut seen: std::collections::HashSet<String> =
        all.iter().map(|s| s.id.clone()).collect();

    let mut cursor: Option<String> = None;
    for _ in 0..MAX_INACTIVE_PAGES {
        let mut opts = RequestOptions::default();
        opts.query
            .push(("count".to_string(), INACTIVE_PAGE_SIZE.to_string()));
        if let Some(c) = &cursor {
            opts.query.push(("cursor".to_string(), c.clone()));
        }
        let page = match tokio::time::timeout(
            LIST_TIMEOUT,
            client.request::<InactivePage>(Method::GET, "/sandboxes/inactive", None::<&()>, opts),
        )
        .await
        {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => {
                warn!("listing inactive sandboxes failed (showing deployed only): {e:#}");
                break;
            }
            Err(_) => {
                warn!("listing inactive sandboxes timed out (showing deployed only)");
                break;
            }
        };
        for s in page.sandboxes {
            if seen.insert(s.id.clone()) {
                all.push(s);
            }
        }
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    Ok(all)
}

/// The pooler-side join inputs, keyed by sandbox id: warm-entry snapshots and
/// the durable schema↔VM map. Both are cheap in-process reads (one brief map
/// lock, no I/O).
async fn pooler_maps(
    st: &DashState,
) -> (HashMap<String, EntrySnapshot>, HashMap<String, String>) {
    let snap = st
        .registry
        .snapshot()
        .await
        .into_iter()
        .map(|e| (e.sandbox_id.clone(), e))
        .collect();
    let store = st
        .registry
        .store_entries()
        .into_iter()
        .map(|(schema, id)| (id, schema))
        .collect();
    (snap, store)
}

/// Left-join one raw daemon record with the pooler's state.
fn join_row(
    s: RawSandbox,
    snap: &HashMap<String, EntrySnapshot>,
    store: &HashMap<String, String>,
) -> VmRow {
    let entry = snap.get(&s.id);
    let schema = entry
        .map(|e| e.schema.clone())
        .or_else(|| store.get(&s.id).cloned())
        .or_else(|| s.name.strip_prefix("pg-").map(str::to_string));
    let pool_managed = schema.is_some() || s.name.starts_with("pg-");
    VmRow {
        id: s.id,
        name: s.name,
        schema,
        pool_managed,
        status: s.status,
        size_class: s.size_class,
        cpus: s.cpus,
        memory_bytes: s.memory,
        disk_size_gb: s.disk_size_gb,
        image: s.image,
        region: s.region,
        status_changed_at: s.status_changed_at,
        uptime_secs: s.uptime_secs,
        ttl_seconds: s.ttl_seconds,
        guest_ip: s.guest_ip,
        error_message: s.error_message,
        live_sessions: entry.map(|e| e.active),
        idle_secs: entry.map(|e| e.idle_secs),
        keepalive: entry.map(|e| e.keepalive).unwrap_or(false),
        target: entry.map(|e| e.target),
        tunneled: entry.map(|e| e.tunneled),
    }
}

/// Build the full VM list: fetch the raw sandbox inventory, join the registry
/// snapshot + store. No guest access — safe to call on every page load/refresh.
pub async fn build_rows(st: &DashState) -> Result<Vec<VmRow>> {
    let list = fetch_inventory().await?;
    let (snap, store) = pooler_maps(st).await;

    let mut rows: Vec<VmRow> = list
        .into_iter()
        .map(|s| join_row(s, &snap, &store))
        .collect();

    // Pooler-managed VMs first, then alphabetical by name.
    rows.sort_by(|a, b| {
        b.pool_managed
            .cmp(&a.pool_managed)
            .then_with(|| a.name.cmp(&b.name))
    });

    Ok(rows)
}

/// One page of the browse-all view.
pub struct SandboxPage {
    /// Only the rows on the current page — everything off-page stays a raw
    /// record and is never joined or rendered.
    pub rows: Vec<VmRow>,
    /// Total sandboxes the daemon reported (pre-filter).
    pub total: usize,
    /// How many matched the search.
    pub matched: usize,
    /// Current page, 1-based, clamped to `pages`.
    pub page: usize,
    /// Total pages for the current match set (>= 1).
    pub pages: usize,
    pub per: usize,
    /// The search text as applied (trimmed), echoed back into the form.
    pub q: String,
    /// The state filter as applied: a [`STATE_FILTERS`] label or [`STATE_ALL`].
    pub state: String,
    /// Sandboxes per state label among the search-matched set (state filter
    /// not applied), in [`STATE_FILTERS`] order — drives the filter pills.
    pub state_counts: Vec<(&'static str, usize)>,
}

/// Build one page of the searchable all-sandboxes view. Same system footprint
/// as [`build_rows`] — one bounded daemon read plus in-process pooler lookups,
/// no guest access — but lazier: the filters run on the raw records and only
/// the `per` rows in the page window are joined into full [`VmRow`]s.
pub async fn build_page(
    st: &DashState,
    q: &str,
    state: &str,
    page: usize,
    per: usize,
) -> Result<SandboxPage> {
    let list = fetch_inventory().await?;
    let total = list.len();
    let (snap, store) = pooler_maps(st).await;

    let needle = q.trim().to_lowercase();
    let state = {
        let s = state.trim().to_lowercase();
        if s.is_empty() { DEFAULT_STATE.to_string() } else { s }
    };

    // Search filter first (state-agnostic) — this set also feeds the per-state
    // pill counts, so switching state filters shows where the matches live.
    let q_hits: Vec<RawSandbox> = list
        .into_iter()
        .filter(|s| {
            needle.is_empty() || {
                // Recovered schema names come from the pooler maps; a schema
                // implied by a `pg-` name prefix is already covered by the
                // name match.
                let schema = snap
                    .get(&s.id)
                    .map(|e| e.schema.as_str())
                    .or_else(|| store.get(&s.id).map(String::as_str));
                matches_query(s, schema, &needle)
            }
        })
        .collect();

    let mut state_counts: Vec<(&'static str, usize)> =
        STATE_FILTERS.iter().map(|l| (*l, 0)).collect();
    for s in &q_hits {
        let label = status_str(&s.status);
        if let Some(c) = state_counts.iter_mut().find(|(l, _)| *l == label) {
            c.1 += 1;
        }
    }

    let mut hits: Vec<RawSandbox> = q_hits
        .into_iter()
        .filter(|s| state_matches(s, &state))
        .collect();
    // Stable order (name, then id as tie-break) so page boundaries don't
    // shuffle between requests.
    hits.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));

    let matched = hits.len();
    let (page, pages) = page_window(matched, page, per);
    let rows = hits
        .into_iter()
        .skip((page - 1) * per)
        .take(per)
        .map(|s| join_row(s, &snap, &store))
        .collect();

    Ok(SandboxPage {
        rows,
        total,
        matched,
        page,
        pages,
        per,
        q: q.trim().to_string(),
        state,
        state_counts,
    })
}

/// Does this sandbox pass the state filter? `state` is a lowercased
/// [`STATE_FILTERS`] label or [`STATE_ALL`]; an unrecognized value simply
/// matches nothing (an admin typo'd a URL — an empty page is clearer than a
/// silently ignored filter).
fn state_matches(s: &RawSandbox, state: &str) -> bool {
    state == STATE_ALL || status_str(&s.status) == state
}

/// Case-insensitive substring match against the fields an operator actually
/// searches by. `needle` must already be lowercased and non-empty.
fn matches_query(s: &RawSandbox, schema: Option<&str>, needle: &str) -> bool {
    let hay = |v: &str| v.to_lowercase().contains(needle);
    hay(&s.id)
        || hay(&s.name)
        || schema.is_some_and(hay)
        || hay(status_str(&s.status))
        || s.image.as_deref().is_some_and(hay)
        || s.guest_ip.as_deref().is_some_and(hay)
}

/// Clamp a 1-based page request against the match count → `(page, pages)`.
/// Zero matches still yields one (empty) page so the view always has a valid
/// page number to render.
fn page_window(matched: usize, page: usize, per: usize) -> (usize, usize) {
    let pages = matched.div_ceil(per).max(1);
    (page.clamp(1, pages), pages)
}

/// Human label for a sandbox status — shared by the status badges and the
/// search filter, so searching "running" matches what the badge shows.
pub fn status_str(status: &SandboxStatus) -> &'static str {
    match status {
        SandboxStatus::Running => "running",
        SandboxStatus::Provisioning => "provisioning",
        SandboxStatus::Stopped => "stopped",
        SandboxStatus::Paused => "paused",
        SandboxStatus::Failed => "failed",
        SandboxStatus::ColdStored => "cold-stored",
        SandboxStatus::Unknown => "unknown",
    }
}

/// Find one VM row by id (rebuilds the full list; fine for an admin tool with a
/// handful of VMs).
pub async fn find_row(st: &DashState, id: &str) -> Result<Option<VmRow>> {
    Ok(build_rows(st).await?.into_iter().find(|r| r.id == id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_sandbox_captures_cpus_and_memory() {
        // Shape mirrors the daemon's sandbox record (cf. the sandbox.yaml a
        // `large` VM reports: cpus: 4, memory: 8589934592, disk_size_gb: 4).
        let json = r#"{
            "id": "sb-f88677f0",
            "name": "pg-aNb6mPp6",
            "status": "running",
            "image": "pg",
            "ttl_seconds": 0,
            "disk_size_gb": 4,
            "cpus": 4,
            "memory": 8589934592,
            "guest_ip": "172.25.223.194",
            "status_changed_at": "2026-07-09T00:00:00Z"
        }"#;
        let s: RawSandbox = serde_json::from_str(json).unwrap();
        assert_eq!(s.cpus, Some(4));
        assert_eq!(s.memory, Some(8_589_934_592));
        assert_eq!(s.disk_size_gb, Some(4));
        assert_eq!(s.status, SandboxStatus::Running);
    }

    #[test]
    fn raw_sandbox_tolerates_missing_size_fields() {
        // Older/other daemons may omit cpus/memory — must not fail the list.
        let json = r#"{"id":"sb-x","name":"pg-y","status":"stopped","status_changed_at":"t"}"#;
        let s: RawSandbox = serde_json::from_str(json).unwrap();
        assert_eq!(s.cpus, None);
        assert_eq!(s.memory, None);
        assert_eq!(s.status, SandboxStatus::Stopped);
    }

    #[test]
    fn inactive_page_parses_native_sandbox_info() {
        // The native `SandboxInfo` shape the /sandboxes/inactive route returns:
        // `image` is a bare string, `uptime` is a serde Duration object, and
        // there is no `uptime_secs`/`status_changed_at`/`cpus`/`memory`.
        let json = r#"{
            "sandboxes": [{
                "id": "sb-cold1",
                "name": "pg-old",
                "sandbox_type": "vm",
                "status": "stopped",
                "image": "pg",
                "uptime": {"secs": 0, "nanos": 0},
                "cpu_usage": null,
                "memory_usage": null,
                "remotely_accessible": false,
                "guest_ip": "172.25.0.2"
            }],
            "cursor": null,
            "next_cursor": "sb-cold1"
        }"#;
        let page: InactivePage = serde_json::from_str(json).unwrap();
        assert_eq!(page.sandboxes.len(), 1);
        let s = &page.sandboxes[0];
        assert_eq!(s.status, SandboxStatus::Stopped);
        assert_eq!(s.uptime_secs, 0);
        assert_eq!(s.guest_ip.as_deref(), Some("172.25.0.2"));
        assert_eq!(page.next_cursor.as_deref(), Some("sb-cold1"));
    }

    fn raw(id: &str, name: &str, status: &str) -> RawSandbox {
        let json = format!(r#"{{"id":"{id}","name":"{name}","status":"{status}"}}"#);
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn query_matches_id_name_schema_and_status_case_insensitively() {
        let s = raw("sb-F88677", "pg-aNb6mPp6", "running");
        assert!(matches_query(&s, None, "sb-f88"));
        assert!(matches_query(&s, None, "anb6"));
        assert!(matches_query(&s, Some("tenant_west"), "west"));
        assert!(matches_query(&s, None, "running"));
        assert!(!matches_query(&s, None, "stopped"));
        assert!(!matches_query(&s, Some("tenant_west"), "east"));
    }

    #[test]
    fn state_filter_matches_label_or_all() {
        let running = raw("sb-1", "pg-a", "running");
        let stopped = raw("sb-2", "pg-b", "stopped");
        assert!(state_matches(&running, "running"));
        assert!(!state_matches(&stopped, "running"));
        assert!(state_matches(&stopped, "stopped"));
        assert!(state_matches(&running, STATE_ALL));
        assert!(state_matches(&stopped, STATE_ALL));
        // Unrecognized filter matches nothing rather than everything.
        assert!(!state_matches(&running, "bogus"));
        // Every filter label is a status_str label, so pill counts line up.
        assert!(STATE_FILTERS.contains(&status_str(&running.status)));
    }

    #[test]
    fn page_window_clamps_to_valid_pages() {
        // Zero matches still renders one empty page.
        assert_eq!(page_window(0, 1, 25), (1, 1));
        assert_eq!(page_window(0, 7, 25), (1, 1));
        // 51 rows at 25/page → 3 pages; out-of-range requests clamp.
        assert_eq!(page_window(51, 1, 25), (1, 3));
        assert_eq!(page_window(51, 3, 25), (3, 3));
        assert_eq!(page_window(51, 9, 25), (3, 3));
        assert_eq!(page_window(51, 0, 25), (1, 3));
        // Exact multiple has no phantom extra page.
        assert_eq!(page_window(50, 2, 25), (2, 2));
    }
}

//! Server-side-rendered HTML (maud). All values are interpolated through maud,
//! which HTML-escapes by default; no secrets are ever rendered.

use heyo_sdk::SandboxStatus;
use maud::{html, Markup, DOCTYPE};

use crate::registry::{DbStats, GuestStats};

use super::handlers::Banner;
use super::model::{self, SandboxPage, VmRow};
use super::state::DashState;

const SIZE_CLASSES: [&str; 5] = ["micro", "mini", "small", "medium", "large"];

/// Shared page chrome: `<head>` with inline CSS, a nav bar, and the body.
fn shell(title: &str, body: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "pg-vm-pool · " (title) }
                style { (STYLE) }
            }
            body {
                header.nav {
                    a.brand href="/" { "pg-vm-pool" }
                    nav {
                        a href="/" { "Databases" }
                        a href="/logs/pooler" { "pooler log" }
                        a href="/logs/heyvmd" { "heyvmd log" }
                    }
                }
                main { (body) }
            }
        }
    }
}

/// Render the `?msg=`/`?err=` one-shot banner, if present.
fn banner(b: &Banner) -> Markup {
    html! {
        @if let Some(m) = &b.msg {
            div.banner.ok { (m) }
        }
        @if let Some(e) = &b.err {
            div.banner.err { "error: " (e) }
        }
    }
}

/// The main Databases view: a paged, searchable list of every daemon sandbox.
pub fn databases_page(st: &DashState, p: &SandboxPage) -> Markup {
    let first = (p.page - 1) * p.per + 1;
    let last = first + p.rows.len().saturating_sub(1);
    shell(
        "Databases",
        html! {
            div.pagehead {
                h1 { "Databases" }
                a.button-link href=(list_href(&p.q, &p.state, p.page, p.per)) { "↻ refresh" }
            }
            form.search method="get" action="/" {
                input type="search" name="q" value=(p.q)
                    placeholder="filter by id, name, schema, status, image, or guest ip";
                @if p.state != model::DEFAULT_STATE {
                    input type="hidden" name="state" value=(p.state);
                }
                @if p.per != model::DEFAULT_PER {
                    input type="hidden" name="per" value=(p.per);
                }
                button type="submit" { "search" }
                @if !p.q.is_empty() { a.button-link href=(list_href("", &p.state, 1, p.per)) { "clear" } }
            }
            (state_pills(p))
            div.summary {
                @if p.matched == 0 {
                    span { "no sandboxes match" }
                } @else {
                    span { "showing " b { (first) "–" (last) } " of " b { (p.matched) } }
                }
                span.dim { (p.total) " total" }
                @if st.cfg.basic_auth.is_none() {
                    span.warn { "auth: OFF" }
                }
            }
            @if p.matched > 0 {
                (vm_table(&p.rows))
                (pager(p))
            }
        },
    )
}

/// State filter pills: "all" plus every state with matches (and the selected
/// state even at zero, so the active filter stays visible/escapable). Counts
/// are within the current search, so they show where the matches live.
fn state_pills(p: &SandboxPage) -> Markup {
    let all: usize = p.state_counts.iter().map(|(_, n)| n).sum();
    html! {
        div.pills {
            a.pill.selected[p.state == model::STATE_ALL]
                href=(list_href(&p.q, model::STATE_ALL, 1, p.per)) {
                "all " span.count { (all) }
            }
            @for (label, n) in &p.state_counts {
                @if *n > 0 || p.state == *label {
                    a.pill.selected[p.state == *label]
                        href=(list_href(&p.q, label, 1, p.per)) {
                        (label) " " span.count { (n) }
                    }
                }
            }
        }
    }
}

/// Prev/next page controls; hidden when everything fits on one page.
fn pager(p: &SandboxPage) -> Markup {
    html! {
        @if p.pages > 1 {
            div.pager {
                @if p.page > 1 {
                    a.button-link href=(list_href(&p.q, &p.state, p.page - 1, p.per)) { "← prev" }
                }
                span { "page " (p.page) " of " (p.pages) }
                @if p.page < p.pages {
                    a.button-link href=(list_href(&p.q, &p.state, p.page + 1, p.per)) { "next →" }
                }
            }
        }
    }
}

/// Build a Databases-list link, omitting params that hold their default value.
fn list_href(q: &str, state: &str, page: usize, per: usize) -> String {
    let mut href = String::from("/?");
    if page != 1 {
        href.push_str(&format!("page={page}&"));
    }
    if state != model::DEFAULT_STATE {
        href.push_str(&format!("state={}&", urlenc(state)));
    }
    if !q.is_empty() {
        href.push_str(&format!("q={}&", urlenc(q)));
    }
    if per != model::DEFAULT_PER {
        href.push_str(&format!("per={per}&"));
    }
    href.truncate(href.len() - 1); // trailing '&' or the bare '?'
    href
}

/// Minimal RFC 3986 percent-encoder for a query value (search text is
/// user-typed, so it can contain anything).
fn urlenc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

pub fn vm_detail_page(
    st: &DashState,
    r: &VmRow,
    db: Option<&DbStats>,
    guest: Option<&GuestStats>,
    b: &Banner,
) -> Markup {
    shell(
        &r.name,
        html! {
            p { a href="/" { "← Databases" } }
            (banner(b))
            div.pagehead {
                h1 { (r.name) " " (status_badge(&r.status)) }
                // A plain GET back to the canonical URL: re-reads daemon +
                // pooler state and drops any one-shot ?msg/?err banner.
                a.button-link href={ "/vm/" (r.id) } { "↻ refresh" }
            }

            h2 { "configuration" }
            dl.detail {
                dt { "id" }          dd { code { (r.id) } }
                dt { "schema" }      dd { (r.schema.as_deref().unwrap_or("—")) }
                dt { "pooler-managed" } dd { (yesno(r.pool_managed)) }
                dt { "vCPUs" } dd { (r.cpus.map(|c| c.to_string()).unwrap_or_else(|| "—".into())) }
                dt { "memory" } dd { (r.memory_bytes.map(human_bytes).unwrap_or_else(|| "—".into())) }
                dt { "disk" } dd { (r.disk_size_gb.map(|g| format!("{g} GB")).unwrap_or_else(|| "—".into())) }
                @if let Some(sc) = &r.size_class { dt { "size class" } dd { (sc) } }
                @if let Some(img) = &r.image { dt { "image" } dd { (img) } }
                @if let Some(reg) = &r.region { dt { "region" } dd { (reg) } }
                dt { "uptime" } dd { (if r.is_running() { human_secs(r.uptime_secs) } else { "—".into() }) }
                dt { "guest ip" } dd { (r.guest_ip.as_deref().unwrap_or("—")) }
                @if let Some(t) = r.ttl_seconds {
                    dt { "ttl" } dd { (if t == 0 { "0 (pinned)".to_string() } else { human_secs(t) }) }
                }
                @if let Some(c) = &r.status_changed_at { dt { "status changed" } dd.dim { (c) } }
                dt { "keep-alive" } dd { (yesno(r.keepalive)) }
                @if let Some(target) = r.target {
                    dt { "splice target" }
                    dd { code { (target.to_string()) } (if r.tunneled == Some(true) { " (tunnel)" } else { " (direct)" }) }
                }
                @if let Some(err) = &r.error_message { dt { "error" } dd.err { (err) } }
            }

            h2 { "resource usage" }
            dl.detail {
                dt { "allocated" }
                dd { (allocated_str(r).unwrap_or_else(|| "—".into())) }
                @if let Some(cpu) = r.cpu_percent {
                    dt { "cpu" }
                    dd {
                        (format!("{cpu:.1}%"))
                        @if let Some(c) = r.cpus {
                            span.dim { " of " (c as u64 * 100) "% (" (c) " vCPU)" }
                        } @else {
                            span.dim { " of one core per vCPU" }
                        }
                    }
                }
                dt { "live sessions" } dd { (sessions_cell(r)) }
                @if let Some(idle) = r.idle_secs {
                    dt { "idle for" }
                    dd {
                        (human_secs(idle))
                        @if let Some(t) = st.registry.idle_timeout() {
                            span.dim { " (reaped after " (human_secs(t.as_secs())) ")" }
                        }
                    }
                }
                @if let Some(s) = db {
                    dt { "database size" } dd { (human_bytes(s.db_size_bytes.max(0) as u64)) }
                    dt { "backend conns" } dd { (s.backends) }
                } @else if r.pool_managed && r.is_running() {
                    dt { "database" } dd.dim { "(VM not warm in the pooler — no live DB stats)" }
                }
                @if let Some(g) = guest {
                    @if let Some((total, avail)) = g.mem {
                        dt { "guest memory" }
                        dd {
                            (human_bytes(total.saturating_sub(avail))) " used"
                            span.dim { " of " (human_bytes(total)) }
                        }
                    }
                    @if let Some((l1, l5, l15)) = g.load {
                        dt { "load average" }
                        dd { (format!("{l1:.2} / {l5:.2} / {l15:.2}")) span.dim { " (1 / 5 / 15 min)" } }
                    }
                    @if let Some((total, used, avail)) = g.disk {
                        dt { "data disk" }
                        dd {
                            (human_bytes(used)) " used"
                            span.dim { " of " (human_bytes(total)) " · " (human_bytes(avail)) " free" }
                        }
                    }
                }
            }
            @if guest.is_some() || r.cpu_percent.is_some() {
                p.note {
                    @if r.cpu_percent.is_some() {
                        "CPU is heyvmd's host-side sample of the VM's "
                        "process(es), " code { "top" } " convention: 100% = one "
                        "core, so a busy guest can exceed 100%. "
                    }
                    @if guest.is_some() {
                        "Guest memory/load/disk are read over the pooler's Postgres "
                        "connection (" code { "pg_read_file" } " on " code { "/proc" } ", "
                        code { "df" } " via " code { "COPY FROM PROGRAM" } ") — "
                        "no guest-console access."
                    }
                }
            }

            section.controls {
                h2 { "controls" }
                div.actions { (action_buttons(&r.id, &r.status)) }
                form.resize method="post" action={ "/vm/" (r.id) "/resize" } {
                    label { "resize to " }
                    select name="size_class" {
                        @for s in SIZE_CLASSES {
                            option value=(s) selected[r.size_class.as_deref() == Some(s)] { (s) }
                        }
                    }
                    button type="submit" { "resize" }
                }
                p.note {
                    "Pooler-managed VMs stopped here auto-restart on the next client "
                    "connection; a resize takes effect on the VM's next boot."
                }
            }

            section {
                h2 { "logs" }
                p {
                    a.button-link href={ "/logs/vm/" (r.id) } { "view Postgres log →" }
                }
                p.note {
                    "Opening the VM log runs "
                    code { "tail" }
                    " inside the guest — a deliberate action, kept off this page so "
                    "simply viewing a VM never touches it."
                }
            }
        },
    )
}

pub fn log_page(title: &str, source: &str, text: &str) -> Markup {
    shell(
        title,
        html! {
            p { a href="/" { "← Databases" } }
            h1 { "log · " (title) }
            p.dim { code { (source) } }
            pre.log { (text) }
        },
    )
}

pub fn not_found_page(id: &str) -> Markup {
    shell(
        "not found",
        html! {
            p { a href="/" { "← Databases" } }
            h1 { "VM not found" }
            p { "No sandbox with id " code { (id) } " is known to the daemon." }
        },
    )
}

pub fn error_page(err: &anyhow::Error) -> Markup {
    shell(
        "error",
        html! {
            p { a href="/" { "← Databases" } }
            h1 { "Something went wrong" }
            pre.log { (format!("{err:#}")) }
        },
    )
}

// ---- fragments -------------------------------------------------------------

/// The VM list table, shared by the index and the paged all-sandboxes view.
fn vm_table(rows: &[VmRow]) -> Markup {
    html! {
        table {
            thead {
                tr {
                    th { "VM" }
                    th { "schema" }
                    th { "status" }
                    th { "size" }
                    th { "cpu" }
                    th { "uptime" }
                    th { "sessions" }
                    th { "actions" }
                }
            }
            tbody {
                @for r in rows {
                    tr {
                        td { a href={ "/vm/" (r.id) } { (r.name) } }
                        td.dim { (r.schema.as_deref().unwrap_or("—")) }
                        td { (status_badge(&r.status)) }
                        td { (size_cell(r)) }
                        td { (cpu_cell(r)) }
                        td { (if r.is_running() { human_secs(r.uptime_secs) } else { "—".into() }) }
                        td { (sessions_cell(r)) }
                        td.actions { (action_buttons(&r.id, &r.status)) }
                    }
                }
            }
        }
    }
}

fn action_buttons(id: &str, status: &SandboxStatus) -> Markup {
    let running = *status == SandboxStatus::Running;
    html! {
        @if running {
            form method="post" action={ "/vm/" (id) "/stop" } { button.stop { "stop" } }
            form method="post" action={ "/vm/" (id) "/reboot" } { button { "reboot" } }
        } @else {
            form method="post" action={ "/vm/" (id) "/start" } { button.start { "start" } }
        }
    }
}

fn size_cell(r: &VmRow) -> Markup {
    // Prefer the concrete cpus/memory the daemon reports; fall back to a size
    // class label, then to nothing.
    html! {
        @match (r.cpus, r.memory_bytes) {
            (Some(c), Some(m)) => {
                (format!("{c} vCPU"))
                span.dim.sub { (human_bytes(m)) }
            }
            _ => @match r.size_class.as_deref() {
                Some(s) => { (s) }
                None => span.dim { "—" },
            },
        }
    }
}

/// Human-readable allocated resources for the detail page ("4 vCPU · 8.0 GiB ·
/// 4 GB disk"), from whatever the daemon reported.
fn allocated_str(r: &VmRow) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    if let Some(c) = r.cpus {
        parts.push(format!("{c} vCPU"));
    }
    if let Some(m) = r.memory_bytes {
        parts.push(human_bytes(m));
    }
    if let Some(g) = r.disk_size_gb {
        parts.push(format!("{g} GB disk"));
    }
    if parts.is_empty() {
        r.size_class.clone()
    } else {
        Some(parts.join(" · "))
    }
}

/// Daemon-sampled CPU (`top` convention: 100% = one core), same source as the
/// detail page's cpu row.
fn cpu_cell(r: &VmRow) -> Markup {
    html! {
        @match r.cpu_percent {
            Some(cpu) => { (format!("{cpu:.1}%")) }
            None => span.dim { "—" },
        }
    }
}

fn sessions_cell(r: &VmRow) -> Markup {
    html! {
        @match r.live_sessions {
            Some(n) if n > 0 => span.badge.active { (n) },
            Some(n) => span.dim { (n) },
            None => span.dim { "—" },
        }
    }
}

fn status_badge(status: &SandboxStatus) -> Markup {
    let class = match status {
        SandboxStatus::Running => "s-running",
        SandboxStatus::Provisioning => "s-prov",
        SandboxStatus::Stopped | SandboxStatus::Paused | SandboxStatus::ColdStored => "s-stopped",
        SandboxStatus::Failed => "s-failed",
        SandboxStatus::Unknown => "s-unknown",
    };
    // Label comes from the model so searching e.g. "running" matches the badge.
    html! { span.badge class=(class) { (model::status_str(status)) } }
}

fn yesno(b: bool) -> &'static str {
    if b { "yes" } else { "no" }
}

fn human_bytes(b: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

fn human_secs(s: u64) -> String {
    let d = s / 86_400;
    let h = (s % 86_400) / 3_600;
    let m = (s % 3_600) / 60;
    let sec = s % 60;
    if d > 0 {
        format!("{d}d {h}h")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {sec}s")
    } else {
        format!("{sec}s")
    }
}

// Light/dark via CSS custom properties; every color is defined for both schemes
// so contrast holds up in dark mode (the earlier version left light badge/pre
// colors on a dark page).
const STYLE: &str = r#"
:root {
  color-scheme: light dark;
  --bg:#f6f7f9; --fg:#1a1a1a; --dim:#6b7280; --muted:#8a8f98;
  --card:#ffffff; --border:#e3e5e9; --th-bg:#f0f2f5; --row-hover:#f6f8fb;
  --link:#2563eb; --code-bg:#eef0f3; --pre-bg:#0f1115; --pre-fg:#d4d7dd;
  --btn-bg:#ffffff; --btn-border:#c7ccd3; --btn-hover:#9aa1ab;
  --ok-bg:#e3f6e5; --ok-fg:#166534; --ok-border:#a9e0b3;
  --err-bg:#fde4e4; --err-fg:#b00020; --err-border:#e6a9a9;
  --warn:#b00020;
  --run-bg:#dcfce7; --run-fg:#166534;
  --stop-bg:#e5e7eb; --stop-fg:#4b5563;
  --prov-bg:#fef3c7; --prov-fg:#92600a;
  --fail-bg:#fee2e2; --fail-fg:#b00020;
  --sess-bg:#dbeafe; --sess-fg:#1e40af;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg:#15171c; --fg:#e6e8eb; --dim:#9aa1ab; --muted:#7b828c;
    --card:#1e2129; --border:#2f333c; --th-bg:#252a33; --row-hover:#252a33;
    --link:#7ab7ff; --code-bg:#0f1115; --pre-bg:#0d0f13; --pre-fg:#cdd3db;
    --btn-bg:#252a33; --btn-border:#3a404b; --btn-hover:#565e6b;
    --ok-bg:#123020; --ok-fg:#6ee7a8; --ok-border:#1f5136;
    --err-bg:#3a1618; --err-fg:#ff9ba0; --err-border:#5e2529;
    --warn:#ff9ba0;
    --run-bg:#123020; --run-fg:#5edb93;
    --stop-bg:#2a2f38; --stop-fg:#aab1bd;
    --prov-bg:#3a2f12; --prov-fg:#f2cd6b;
    --fail-bg:#3a1618; --fail-fg:#ff9ba0;
    --sess-bg:#16294a; --sess-fg:#9cc4ff;
  }
}
* { box-sizing: border-box; }
body { margin:0; font:14px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
       color:var(--fg); background:var(--bg); }
a { color:var(--link); text-decoration:none; }
a:hover { text-decoration:underline; }
.nav { display:flex; align-items:center; gap:1.5rem; padding:.7rem 1.2rem;
       background:var(--card); border-bottom:1px solid var(--border); }
.nav .brand { font-weight:700; color:var(--fg); }
.nav nav { display:flex; gap:1rem; }
main { padding:1.2rem; max-width:1200px; margin:0 auto; }
h1 { font-size:1.3rem; }
.pagehead { display:flex; align-items:center; justify-content:space-between; gap:1rem; }
.pagehead h1 { margin:.6rem 0; }
h2 { font-size:1rem; margin:1.4rem 0 .5rem; }
.summary { display:flex; gap:1.5rem; margin:.5rem 0 1rem; color:var(--dim); }
.summary b { color:var(--fg); }
.summary .warn { color:var(--warn); font-weight:600; }
table { width:100%; border-collapse:collapse; background:var(--card);
        border:1px solid var(--border); border-radius:8px; overflow:hidden; }
th, td { text-align:left; padding:.5rem .7rem; border-bottom:1px solid var(--border); white-space:nowrap; }
th { background:var(--th-bg); font-weight:600; font-size:.78rem; text-transform:uppercase; letter-spacing:.03em; color:var(--dim); }
tr:last-child td { border-bottom:0; }
tr:hover td { background:var(--row-hover); }
td.dim, .dim { color:var(--muted); }
.sub { display:block; font-size:.78rem; }
.badge { display:inline-block; padding:.05rem .5rem; border-radius:999px; font-size:.78rem; font-weight:600; }
.s-running { background:var(--run-bg); color:var(--run-fg); }
.s-stopped { background:var(--stop-bg); color:var(--stop-fg); }
.s-prov { background:var(--prov-bg); color:var(--prov-fg); }
.s-failed { background:var(--fail-bg); color:var(--fail-fg); }
.s-unknown { background:var(--stop-bg); color:var(--stop-fg); }
.badge.active { background:var(--sess-bg); color:var(--sess-fg); }
td.actions { display:flex; gap:.35rem; }
.actions form { display:inline; margin:0; }
button, .button-link { font:inherit; padding:.25rem .6rem; border:1px solid var(--btn-border);
         border-radius:6px; background:var(--btn-bg); cursor:pointer; color:var(--fg); display:inline-block; }
button:hover, .button-link:hover { border-color:var(--btn-hover); text-decoration:none; }
button.stop { color:var(--err-fg); border-color:var(--err-border); }
button.start { color:var(--ok-fg); border-color:var(--ok-border); }
.banner { padding:.55rem .8rem; border-radius:6px; margin-bottom:1rem; border:1px solid; }
.banner.ok { background:var(--ok-bg); border-color:var(--ok-border); color:var(--ok-fg); }
.banner.err { background:var(--err-bg); border-color:var(--err-border); color:var(--err-fg); }
dl.detail { display:grid; grid-template-columns:max-content 1fr; gap:.35rem 1rem;
            background:var(--card); border:1px solid var(--border); border-radius:8px; padding:1rem; max-width:680px; }
dl.detail dt { color:var(--muted); }
dl.detail dd { margin:0; }
dd.err, .err { color:var(--err-fg); }
section.controls { background:var(--card); border:1px solid var(--border); border-radius:8px;
                   padding:1rem; margin:1.2rem 0; max-width:680px; }
section.controls h2 { margin-top:0; }
.controls .actions { display:flex; gap:.5rem; margin-bottom:.8rem; }
form.resize { display:flex; align-items:center; gap:.4rem; }
select { font:inherit; padding:.2rem; background:var(--btn-bg); color:var(--fg);
         border:1px solid var(--btn-border); border-radius:6px; }
form.search { display:flex; align-items:center; gap:.4rem; margin:.8rem 0; }
form.search input[type=search] { flex:1; max-width:420px; font:inherit; padding:.3rem .6rem;
        background:var(--btn-bg); color:var(--fg); border:1px solid var(--btn-border); border-radius:6px; }
.pager { display:flex; align-items:center; gap:1rem; margin:1rem 0; color:var(--dim); }
.pills { display:flex; flex-wrap:wrap; gap:.4rem; margin:.6rem 0; }
.pill { display:inline-block; padding:.15rem .65rem; border-radius:999px; font-size:.85rem;
        border:1px solid var(--btn-border); background:var(--btn-bg); color:var(--fg); }
.pill:hover { border-color:var(--btn-hover); text-decoration:none; }
.pill.selected { background:var(--sess-bg); color:var(--sess-fg); border-color:var(--sess-fg); font-weight:600; }
.pill .count { color:var(--dim); font-weight:400; }
.pill.selected .count { color:var(--sess-fg); }
.note { color:var(--muted); font-size:.85rem; }
code { background:var(--code-bg); padding:.1rem .3rem; border-radius:4px; font-size:.85em; }
pre.log { background:var(--pre-bg); color:var(--pre-fg); padding:1rem; border-radius:8px; overflow-x:auto;
          font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace; white-space:pre; max-height:70vh; }
"#;

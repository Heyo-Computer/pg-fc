//! Server-side-rendered HTML (maud). All values are interpolated through maud,
//! which HTML-escapes by default; no secrets are ever rendered.

use heyo_sdk::{SandboxInfo, SandboxStatus};
use maud::{html, Markup, DOCTYPE};

use crate::registry::DbStats;

use super::handlers::Banner;
use super::model::VmRow;
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
                        a href="/" { "VMs" }
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

pub fn index_page(st: &DashState, rows: &[VmRow], b: &Banner) -> Markup {
    let active = rows.iter().filter(|r| r.is_running()).count();
    let sessions: usize = rows.iter().filter_map(|r| r.live_sessions).sum();
    shell(
        "VMs",
        html! {
            (banner(b))
            div.summary {
                span { b { (rows.len()) } " VMs" }
                span { b { (active) } " running" }
                span { b { (sessions) } " live sessions" }
                @if st.cfg.basic_auth.is_none() {
                    span.warn { "auth: OFF" }
                }
            }
            table {
                thead {
                    tr {
                        th { "VM" }
                        th { "schema" }
                        th { "status" }
                        th { "size" }
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
                            td { (size_cell(r.size_class.as_deref())) }
                            td { (if r.is_running() { human_secs(r.uptime_secs) } else { "—".into() }) }
                            td { (sessions_cell(r)) }
                            td.actions { (action_buttons(&r.id, &r.status)) }
                        }
                    }
                }
            }
        },
    )
}

pub fn vm_detail_page(
    st: &DashState,
    r: &VmRow,
    info: Option<&SandboxInfo>,
    db: Option<&DbStats>,
    b: &Banner,
) -> Markup {
    // Prefer the authoritative per-VM info where present, fall back to the row.
    let size_class = info
        .and_then(|i| i.size_class.clone())
        .or_else(|| r.size_class.clone());
    let image = info.map(|i| i.image.clone());
    let region = info.and_then(|i| i.region.clone());
    let guest_ip = info
        .and_then(|i| i.guest_ip.clone())
        .or_else(|| r.guest_ip.clone());
    let ttl = info.and_then(|i| i.ttl_seconds).or(r.ttl_seconds);
    let changed = info.map(|i| i.status_changed_at.clone());
    let error = info
        .and_then(|i| i.error_message.clone())
        .or_else(|| r.error_message.clone());

    shell(
        &r.name,
        html! {
            p { a href="/" { "← all VMs" } }
            (banner(b))
            h1 { (r.name) " " (status_badge(&r.status)) }

            h2 { "configuration" }
            dl.detail {
                dt { "id" }          dd { code { (r.id) } }
                dt { "schema" }      dd { (r.schema.as_deref().unwrap_or("—")) }
                dt { "pooler-managed" } dd { (yesno(r.pool_managed)) }
                dt { "size class" }
                dd {
                    (size_class.as_deref().unwrap_or("unknown"))
                    @if let Some(res) = size_class.as_deref().and_then(size_resources) {
                        span.dim { " · " (res) }
                    }
                }
                @if let Some(img) = &image { dt { "image" } dd { (img) } }
                @if let Some(reg) = &region { dt { "region" } dd { (reg) } }
                dt { "uptime" } dd { (if r.is_running() { human_secs(r.uptime_secs) } else { "—".into() }) }
                dt { "guest ip" } dd { (guest_ip.as_deref().unwrap_or("—")) }
                @if let Some(t) = ttl {
                    dt { "ttl" } dd { (if t == 0 { "0 (pinned)".to_string() } else { human_secs(t) }) }
                }
                @if let Some(c) = &changed { dt { "status changed" } dd.dim { (c) } }
                dt { "keep-alive" } dd { (yesno(r.keepalive)) }
                @if let Some(target) = r.target {
                    dt { "splice target" }
                    dd { code { (target.to_string()) } (if r.tunneled == Some(true) { " (tunnel)" } else { " (direct)" }) }
                }
                @if let Some(err) = &error { dt { "error" } dd.err { (err) } }
            }

            h2 { "resource usage" }
            dl.detail {
                dt { "allocated" }
                dd {
                    @match size_class.as_deref().and_then(size_resources) {
                        Some(res) => (res),
                        None => "—",
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
            }

            section.controls {
                h2 { "controls" }
                div.actions { (action_buttons(&r.id, &r.status)) }
                form.resize method="post" action={ "/vm/" (r.id) "/resize" } {
                    label { "resize to " }
                    select name="size_class" {
                        @for s in SIZE_CLASSES {
                            option value=(s) selected[size_class.as_deref() == Some(s)] { (s) }
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
            p { a href="/" { "← all VMs" } }
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
            p { a href="/" { "← all VMs" } }
            h1 { "VM not found" }
            p { "No sandbox with id " code { (id) } " is known to the daemon." }
        },
    )
}

pub fn error_page(err: &anyhow::Error) -> Markup {
    shell(
        "error",
        html! {
            p { a href="/" { "← all VMs" } }
            h1 { "Something went wrong" }
            pre.log { (format!("{err:#}")) }
        },
    )
}

// ---- fragments -------------------------------------------------------------

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

fn size_cell(size: Option<&str>) -> Markup {
    html! {
        @match size {
            Some(s) => {
                (s)
                @if let Some(res) = size_resources(s) { span.dim.sub { (res) } }
            }
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
    let (label, class) = match status {
        SandboxStatus::Running => ("running", "s-running"),
        SandboxStatus::Provisioning => ("provisioning", "s-prov"),
        SandboxStatus::Stopped => ("stopped", "s-stopped"),
        SandboxStatus::Paused => ("paused", "s-stopped"),
        SandboxStatus::Failed => ("failed", "s-failed"),
        SandboxStatus::ColdStored => ("cold-stored", "s-stopped"),
        SandboxStatus::Unknown => ("unknown", "s-unknown"),
    };
    html! { span.badge class=(class) { (label) } }
}

/// Allocated resources per size tier (matches the tier table in config/README).
fn size_resources(size: &str) -> Option<&'static str> {
    match size {
        "micro" => Some("0.25 vCPU · 512 MB"),
        "mini" => Some("0.5 vCPU · 1 GB"),
        "small" => Some("1 vCPU · 2 GB"),
        "medium" => Some("2 vCPU · 4 GB"),
        "large" => Some("4 vCPU · 8 GB"),
        _ => None,
    }
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
.note { color:var(--muted); font-size:.85rem; }
code { background:var(--code-bg); padding:.1rem .3rem; border-radius:4px; font-size:.85em; }
pre.log { background:var(--pre-bg); color:var(--pre-fg); padding:1rem; border-radius:8px; overflow-x:auto;
          font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace; white-space:pre; max-height:70vh; }
"#;

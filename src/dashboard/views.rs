//! Server-side-rendered HTML (maud). All values are interpolated through maud,
//! which HTML-escapes by default; no secrets are ever rendered.

use heyo_sdk::SandboxStatus;
use maud::{html, Markup, DOCTYPE};

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
                        th { "load" }
                        th { "mem" }
                        th { "disk" }
                        th { "actions" }
                    }
                }
                tbody {
                    @for r in rows {
                        tr {
                            td { a href={ "/vm/" (r.id) } { (r.name) } }
                            td.dim { (r.schema.as_deref().unwrap_or("—")) }
                            td { (status_badge(&r.status)) }
                            td { (r.size_class.as_deref().unwrap_or("—")) }
                            td { (if r.is_running() { human_secs(r.uptime_secs) } else { "—".into() }) }
                            td { (sessions_cell(r)) }
                            td { (usage_load(r)) }
                            td { (usage_mem(r)) }
                            td { (usage_disk(r)) }
                            td.actions { (action_buttons(&r.id, &r.status)) }
                        }
                    }
                }
            }
        },
    )
}

pub fn vm_detail_page(st: &DashState, r: &VmRow, log: &str, b: &Banner) -> Markup {
    shell(
        &r.name,
        html! {
            p { a href="/" { "← all VMs" } }
            (banner(b))
            h1 { (r.name) " " (status_badge(&r.status)) }
            dl.detail {
                dt { "id" }          dd { code { (r.id) } }
                dt { "schema" }      dd { (r.schema.as_deref().unwrap_or("—")) }
                dt { "pooler-managed" } dd { (yesno(r.pool_managed)) }
                dt { "size class" }  dd { (r.size_class.as_deref().unwrap_or("—")) }
                dt { "uptime" }      dd { (if r.is_running() { human_secs(r.uptime_secs) } else { "—".into() }) }
                dt { "guest ip" }    dd { (r.guest_ip.as_deref().unwrap_or("—")) }
                @if let Some(target) = r.target {
                    dt { "splice target" }
                    dd { code { (target.to_string()) } (if r.tunneled == Some(true) { " (tunnel)" } else { " (direct)" }) }
                }
                dt { "keep-alive" }  dd { (yesno(r.keepalive)) }
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
                @if let Some(ttl) = r.ttl_seconds {
                    dt { "ttl" } dd { (if ttl == 0 { "0 (pinned)".to_string() } else { human_secs(ttl) }) }
                }
                @if let Some(err) = &r.error_message {
                    dt { "error" } dd.err { (err) }
                }
                @if let Some(u) = &r.usage {
                    dt { "cpu" }  dd { (format!("load {:.2} · {} vCPU", u.load1, u.nproc)) }
                    dt { "memory" } dd { (human_bytes(u.mem_used_b())) " / " (human_bytes(u.mem_total_b)) }
                    dt { "disk (/workspace)" } dd { (human_bytes(u.disk_used_b)) " / " (human_bytes(u.disk_total_b)) }
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
                    "Note: pooler-managed VMs stopped here will auto-restart on the next client "
                    "connection; a resize takes effect on the next boot."
                }
            }

            section {
                h2 { "Postgres log " a.small href={ "/logs/vm/" (r.id) } { "(open)" } }
                pre.log { (log) }
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

fn sessions_cell(r: &VmRow) -> Markup {
    html! {
        @match r.live_sessions {
            Some(n) if n > 0 => span.badge.active { (n) },
            Some(n) => span.dim { (n) },
            None => span.dim { "—" },
        }
    }
}

fn usage_load(r: &VmRow) -> Markup {
    html! {
        @match &r.usage {
            Some(u) => span { (format!("{:.2}", u.load1)) },
            None => span.dim { "—" },
        }
    }
}

fn usage_mem(r: &VmRow) -> Markup {
    html! {
        @match &r.usage {
            Some(u) => span { (human_bytes(u.mem_used_b())) " / " (human_bytes(u.mem_total_b)) },
            None => span.dim { "—" },
        }
    }
}

fn usage_disk(r: &VmRow) -> Markup {
    html! {
        @match &r.usage {
            Some(u) => span { (human_bytes(u.disk_used_b)) " / " (human_bytes(u.disk_total_b)) },
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

const STYLE: &str = r#"
:root { color-scheme: light dark; }
* { box-sizing: border-box; }
body { margin: 0; font: 14px/1.5 system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
       color: #1a1a1a; background: #f6f7f9; }
@media (prefers-color-scheme: dark) {
  body { color: #e6e6e6; background: #16181d; }
  .nav, table, .banner, dl.detail, section.controls { background: #1e2129 !important; border-color:#30343d !important; }
  th { background:#252932 !important; }
  tr:hover td { background:#252932 !important; }
  code, pre.log { background:#0f1115 !important; }
  a { color:#7ab7ff; }
}
.nav { display:flex; align-items:center; gap:1.5rem; padding:.7rem 1.2rem;
       background:#fff; border-bottom:1px solid #e3e5e9; }
.nav .brand { font-weight:700; text-decoration:none; color:inherit; }
.nav nav { display:flex; gap:1rem; }
.nav a { color:#3a6ea5; text-decoration:none; }
.nav a:hover { text-decoration:underline; }
main { padding:1.2rem; max-width:1200px; margin:0 auto; }
h1 { font-size:1.3rem; }
.summary { display:flex; gap:1.5rem; margin:.5rem 0 1rem; color:#555; }
.summary b { color:inherit; }
.summary .warn { color:#b00020; font-weight:600; }
table { width:100%; border-collapse:collapse; background:#fff; border:1px solid #e3e5e9;
        border-radius:8px; overflow:hidden; }
th, td { text-align:left; padding:.5rem .7rem; border-bottom:1px solid #eceef1; white-space:nowrap; }
th { background:#f0f2f5; font-weight:600; font-size:.8rem; text-transform:uppercase; letter-spacing:.03em; }
tr:last-child td { border-bottom:0; }
tr:hover td { background:#f6f8fb; }
td.dim, .dim { color:#8a8f98; }
.badge { display:inline-block; padding:.05rem .5rem; border-radius:999px; font-size:.78rem; font-weight:600; }
.s-running { background:#e3f6e5; color:#1c7c2e; }
.s-stopped { background:#eceef1; color:#5a6069; }
.s-prov { background:#fff6da; color:#8a6d00; }
.s-failed { background:#fde4e4; color:#b00020; }
.s-unknown { background:#eceef1; color:#5a6069; }
.badge.active { background:#e3eefd; color:#1a5fb4; }
td.actions { display:flex; gap:.35rem; }
.actions form { display:inline; margin:0; }
button { font:inherit; padding:.25rem .6rem; border:1px solid #c7ccd3; border-radius:6px;
         background:#fff; cursor:pointer; color:inherit; }
button:hover { border-color:#9aa1ab; }
button.stop { color:#b00020; border-color:#e6a9a9; }
button.start { color:#1c7c2e; border-color:#a9e0b3; }
.banner { padding:.55rem .8rem; border-radius:6px; margin-bottom:1rem; border:1px solid; }
.banner.ok { background:#e3f6e5; border-color:#a9e0b3; color:#1c7c2e; }
.banner.err { background:#fde4e4; border-color:#e6a9a9; color:#b00020; }
dl.detail { display:grid; grid-template-columns:max-content 1fr; gap:.35rem 1rem;
            background:#fff; border:1px solid #e3e5e9; border-radius:8px; padding:1rem; max-width:640px; }
dl.detail dt { color:#8a8f98; }
dl.detail dd { margin:0; }
dd.err, .err { color:#b00020; }
section.controls { background:#fff; border:1px solid #e3e5e9; border-radius:8px; padding:1rem;
                   margin:1.2rem 0; max-width:640px; }
section.controls h2 { margin-top:0; font-size:1rem; }
.controls .actions { display:flex; gap:.5rem; margin-bottom:.8rem; }
form.resize { display:flex; align-items:center; gap:.4rem; }
select { font:inherit; padding:.2rem; }
.note { color:#8a8f98; font-size:.85rem; }
code { background:#f0f2f5; padding:.1rem .3rem; border-radius:4px; font-size:.85em; }
pre.log { background:#0f1115; color:#d4d7dd; padding:1rem; border-radius:8px; overflow-x:auto;
          font:12px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace; white-space:pre; max-height:70vh; }
a.small { font-size:.8rem; font-weight:400; }
h2 a.small { margin-left:.4rem; }
"#;

//! Request handlers. Reads render maud pages; POST actions drive the SDK and
//! use Post-Redirect-Get so a refresh/back doesn't re-fire the action.

use std::time::Duration;

use axum::extract::{Form, Path, Query, State};
use axum::response::Redirect;
use heyo_sdk::Sandbox;
use maud::Markup;
use serde::Deserialize;

use crate::config::parse_size_class;
use crate::vm;

use super::error::AppError;
use super::state::DashState;
use super::{logs, model, views};

/// Lifecycle actions can be slow (a cold boot re-runs init.sh), so give them a
/// generous bound — but still bound them, so one wedged VM can't hang a request.
const ACTION_TIMEOUT: Duration = Duration::from_secs(60);

/// One-shot status banner carried across a redirect (`?msg=` / `?err=`).
#[derive(Deserialize)]
pub struct Banner {
    pub msg: Option<String>,
    pub err: Option<String>,
}

pub async fn index(
    State(st): State<DashState>,
    Query(banner): Query<Banner>,
) -> Result<Markup, AppError> {
    let rows = model::build_rows(&st).await?;
    Ok(views::index_page(&st, &rows, &banner))
}

pub async fn vm_detail(
    State(st): State<DashState>,
    Path(id): Path<String>,
    Query(banner): Query<Banner>,
) -> Result<Markup, AppError> {
    let Some(row) = model::find_row(&st, &id).await? else {
        return Ok(views::not_found_page(&id));
    };
    // Enrich with authoritative daemon info (read-only GET, no guest access);
    // only for running VMs to avoid the get() rehydrate side effect on stopped
    // Firecracker VMs. Best-effort.
    let info = if row.is_running() {
        model::get_info(&id).await.ok()
    } else {
        None
    };
    // Live DB usage over the pooler's warm PG pool (safe TCP path, no guest exec)
    // — only meaningful for a warm, pooler-managed VM.
    let db = match &row.schema {
        Some(schema) if row.is_running() => st.registry.db_stats(&id, schema).await,
        _ => None,
    };
    Ok(views::vm_detail_page(&st, &row, info.as_ref(), db.as_ref(), &banner))
}

pub async fn logs_pooler(State(st): State<DashState>) -> Result<Markup, AppError> {
    let text = logs::tail_file(&st.cfg.pooler_log, st.cfg.log_lines).await?;
    let src = st.cfg.pooler_log.display().to_string();
    Ok(views::log_page("pooler", &src, &text))
}

pub async fn logs_heyvmd(State(st): State<DashState>) -> Result<Markup, AppError> {
    let text = logs::tail_file(&st.cfg.heyvmd_log, st.cfg.log_lines).await?;
    let src = st.cfg.heyvmd_log.display().to_string();
    Ok(views::log_page("heyvmd", &src, &text))
}

pub async fn logs_vm(
    State(st): State<DashState>,
    Path(id): Path<String>,
) -> Result<Markup, AppError> {
    let text = logs::tail_vm_log(&id, st.cfg.log_lines).await?;
    let title = format!("vm {id}");
    let src = format!("{id}:/workspace/pgdata/log/postgresql-*.log");
    Ok(views::log_page(&title, &src, &text))
}

// ---- control actions -------------------------------------------------------

enum Lifecycle {
    Start,
    Stop,
    Reboot,
}

async fn run_lifecycle(id: &str, act: Lifecycle) -> anyhow::Result<()> {
    use anyhow::Context;
    let sb = Sandbox::connect(id.to_string(), vm::local_opts()).context("connecting to VM")?;
    let fut = async move {
        match act {
            Lifecycle::Start => sb.start().await,
            Lifecycle::Stop => sb.stop().await,
            Lifecycle::Reboot => sb.restart().await,
        }
    };
    tokio::time::timeout(ACTION_TIMEOUT, fut)
        .await
        .context("action timed out")??;
    Ok(())
}

fn redirect(id: &str, result: anyhow::Result<()>, ok_msg: &str) -> Redirect {
    match result {
        Ok(()) => Redirect::to(&format!("/vm/{id}?msg={}", qenc(ok_msg))),
        Err(e) => Redirect::to(&format!("/vm/{id}?err={}", qenc(&e.to_string()))),
    }
}

pub async fn action_start(Path(id): Path<String>) -> Redirect {
    let r = run_lifecycle(&id, Lifecycle::Start).await;
    redirect(&id, r, "started")
}

pub async fn action_stop(Path(id): Path<String>) -> Redirect {
    let r = run_lifecycle(&id, Lifecycle::Stop).await;
    redirect(&id, r, "stopped")
}

pub async fn action_reboot(Path(id): Path<String>) -> Redirect {
    let r = run_lifecycle(&id, Lifecycle::Reboot).await;
    redirect(&id, r, "rebooting")
}

#[derive(Deserialize)]
pub struct ResizeForm {
    pub size_class: String,
}

pub async fn action_resize(Path(id): Path<String>, Form(form): Form<ResizeForm>) -> Redirect {
    use anyhow::Context;
    let size = match parse_size_class(&form.size_class) {
        Ok(s) => s,
        Err(_) => return Redirect::to(&format!("/vm/{id}?err={}", qenc("invalid size class"))),
    };
    let result = async {
        let sb =
            Sandbox::connect(id.clone(), vm::local_opts()).context("connecting to VM")?;
        // Firecracker may reject a resize on a running VM; surface that verbatim
        // rather than appearing to succeed.
        tokio::time::timeout(ACTION_TIMEOUT, sb.resize(size))
            .await
            .context("resize timed out")??;
        anyhow::Ok(())
    }
    .await;
    redirect(&id, result, &format!("resized to {}", size.as_str()))
}

/// Minimal query-value encoder: keep readable chars, map space→`+`, drop the
/// rest, and cap length. `+` decodes back to a space via `Query`/serde_urlencoded.
fn qenc(s: &str) -> String {
    s.chars()
        .map(|c| if c == ' ' { '+' } else { c })
        .filter(|c| c.is_ascii_alphanumeric() || "+.,:_-()".contains(*c))
        .take(200)
        .collect()
}

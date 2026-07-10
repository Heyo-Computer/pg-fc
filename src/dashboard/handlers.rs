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

/// Query params for the Databases list. All optional so `/` bare works; junk
/// numeric input is a 400 from the extractor, which is fine for an admin tool.
#[derive(Deserialize)]
pub struct ListParams {
    #[serde(default)]
    pub q: String,
    /// State filter; defaults to showing only running sandboxes.
    pub state: Option<String>,
    pub page: Option<usize>,
    pub per: Option<usize>,
}

/// The main Databases view: a paged, searchable list of every daemon sandbox.
/// Lazy by construction: one bounded daemon read, filtering happens
/// in-process, and only the visible page is joined with pooler state and
/// rendered. No guest access.
pub async fn databases(
    State(st): State<DashState>,
    Query(p): Query<ListParams>,
) -> Result<Markup, AppError> {
    let per = p.per.unwrap_or(model::DEFAULT_PER).clamp(1, model::MAX_PER);
    let page = p.page.unwrap_or(1);
    let state = p.state.as_deref().unwrap_or(model::DEFAULT_STATE);
    let pg = model::build_page(&st, &p.q, state, page, per).await?;
    Ok(views::databases_page(&st, &pg))
}

pub async fn vm_detail(
    State(st): State<DashState>,
    Path(id): Path<String>,
    Query(banner): Query<Banner>,
) -> Result<Markup, AppError> {
    let Some(row) = model::find_row(&st, &id).await? else {
        return Ok(views::not_found_page(&id));
    };
    // Live DB usage and guest-OS stats over the pooler's warm PG pool (safe
    // TCP path, no guest-console access) — only meaningful for a warm,
    // pooler-managed VM. Fetched concurrently; each is independently bounded.
    let (db, guest) = match &row.schema {
        Some(schema) if row.is_running() => tokio::join!(
            st.registry.db_stats(&id, schema),
            st.registry.guest_stats(&id),
        ),
        _ => (None, None),
    };
    Ok(views::vm_detail_page(&st, &row, db.as_ref(), guest.as_ref(), &banner))
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

//! Local dump store + the tiny HTTP server guests use to reach it.
//!
//! The frozen tier (see `store::Tier::Frozen`) keeps a schema's data as a
//! `pg_dump -Fc` file on the host instead of a whole VM: ~1-5MB per typical
//! workbook versus a ~200MB+ filesystem-image floor, which is what makes high
//! cold-schema density possible. The dump bytes move the same way the S3 tier
//! moves them — the *guest* streams `pg_dump`/`pg_restore` through `curl` —
//! but against this server on the host instead of S3, addressed as
//! `http://$GW:<port>/d/<token>` where `$GW` is the guest's default gateway
//! (the host side of its tap, resolved in-guest from `/proc/net/route`).
//!
//! Auth is capability tokens, mirroring S3 presigned URLs: the pooler issues a
//! random single-purpose token per operation (PUT for a freeze, GET for a
//! thaw), bound to one schema and expiring after [`TOKEN_TTL`]. Guests are
//! semi-trusted (they run tenant workloads), so without tokens any guest could
//! fetch any schema's dump off a well-known URL.
//!
//! Uploads land in `<dir>/<schema>.dump` via a temp file + rename, so a
//! half-written upload never looks like a complete dump; completion (and the
//! byte count) is recorded on the token grant, which the freeze path polls
//! in-process — a stronger signal than the S3 tier's HEAD, since it's our own
//! filesystem.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{Path as UrlPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, put};
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};

/// How long an issued token stays valid — generous enough for a slow dump or
/// restore of a large workbook, mirroring the S3 presign TTL.
const TOKEN_TTL: Duration = Duration::from_secs(3600);

/// Hard cap on one uploaded dump. Far above any real single-workbook dump
/// (the S3 tier's single PUT already caps at 5GB); bounds a runaway or
/// malicious upload from filling the host disk.
const MAX_DUMP_BYTES: u64 = 5 * 1024 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Put,
    Get,
}

struct Grant {
    schema: String,
    mode: Mode,
    issued: Instant,
    /// Set by the PUT handler once the upload is fully written and renamed
    /// into place: the completed dump's byte count.
    completed: Option<u64>,
}

pub struct DumpServer {
    dir: PathBuf,
    tokens: Mutex<HashMap<String, Grant>>,
}

impl DumpServer {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            tokens: Mutex::new(HashMap::new()),
        }
    }

    /// The on-disk dump path for a schema. Schema names are validated upstream
    /// (identifier-shaped, no control chars); they still pass through one
    /// path-safety gate in [`issue`] so a hostile name can't traverse.
    pub fn dump_path(&self, schema: &str) -> PathBuf {
        self.dir.join(format!("{schema}.dump"))
    }

    /// Issue a single-purpose token for one schema. The returned token is the
    /// URL path segment; anyone holding it can perform exactly `mode` on
    /// exactly this schema's dump until the TTL expires.
    pub fn issue(&self, schema: &str, mode: Mode) -> Result<String> {
        anyhow::ensure!(
            !schema.contains(['/', '\\']) && !schema.is_empty(),
            "schema name {schema:?} is not dump-path safe"
        );
        let token = random_token()?;
        let mut tokens = self.tokens.lock().unwrap();
        // Opportunistic expiry sweep so the map can't grow unboundedly.
        tokens.retain(|_, g| g.issued.elapsed() < TOKEN_TTL);
        tokens.insert(
            token.clone(),
            Grant {
                schema: schema.to_string(),
                mode,
                issued: Instant::now(),
                completed: None,
            },
        );
        Ok(token)
    }

    /// Byte count of a completed upload on `token`, if the PUT has finished.
    pub fn upload_completed(&self, token: &str) -> Option<u64> {
        self.tokens.lock().unwrap().get(token).and_then(|g| g.completed)
    }

    /// Look up a token for `mode`, returning its schema if valid.
    fn authorize(&self, token: &str, mode: Mode) -> Option<String> {
        let tokens = self.tokens.lock().unwrap();
        let g = tokens.get(token)?;
        (g.mode == mode && g.issued.elapsed() < TOKEN_TTL).then(|| g.schema.clone())
    }

    /// Bind and serve until process exit. Any bind/serve error is returned so
    /// the caller can log it — the freeze tier is inert without this listener.
    pub async fn serve(self: Arc<Self>, listen: std::net::SocketAddr) -> Result<()> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("creating dump dir {}", self.dir.display()))?;
        let app = axum::Router::new()
            .route("/d/{token}", put(handle_put))
            .route("/d/{token}", get(handle_get))
            .with_state(self.clone());
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .with_context(|| format!("binding dump server on {listen}"))?;
        info!("local dump server listening on {listen} (dir {})", self.dir.display());
        axum::serve(listener, app).await.context("dump server error")?;
        Ok(())
    }
}

/// Fill a token from the kernel CSPRNG — no new dependency needed.
fn random_token() -> Result<String> {
    use std::io::Read;
    let mut f = std::fs::File::open("/dev/urandom").context("opening /dev/urandom")?;
    let mut buf = [0u8; 16];
    f.read_exact(&mut buf).context("reading /dev/urandom")?;
    Ok(buf.iter().map(|b| format!("{b:02x}")).collect())
}

async fn handle_put(
    State(srv): State<Arc<DumpServer>>,
    UrlPath(token): UrlPath<String>,
    body: Body,
) -> impl IntoResponse {
    let Some(schema) = srv.authorize(&token, Mode::Put) else {
        return StatusCode::FORBIDDEN;
    };
    let dest = srv.dump_path(&schema);
    let tmp = dest.with_extension("dump.tmp");
    let result: Result<u64> = async {
        let mut file = tokio::fs::File::create(&tmp)
            .await
            .with_context(|| format!("creating {}", tmp.display()))?;
        let mut written: u64 = 0;
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("reading upload body")?;
            written += chunk.len() as u64;
            anyhow::ensure!(written <= MAX_DUMP_BYTES, "upload exceeds {MAX_DUMP_BYTES} bytes");
            file.write_all(&chunk).await.context("writing dump")?;
        }
        // Durable before rename: a crash right after must not leave a rename
        // pointing at unwritten data — the freeze path kills the VM once this
        // upload is reported complete.
        file.sync_all().await.context("fsync dump")?;
        drop(file);
        tokio::fs::rename(&tmp, &dest)
            .await
            .with_context(|| format!("renaming into {}", dest.display()))?;
        Ok(written)
    }
    .await;
    match result {
        Ok(bytes) => {
            if let Some(g) = srv.tokens.lock().unwrap().get_mut(&token) {
                g.completed = Some(bytes);
            }
            info!("dump upload complete for schema {schema}: {bytes} bytes -> {}", dest.display());
            StatusCode::OK
        }
        Err(e) => {
            warn!("dump upload for schema {schema} failed: {e:#}");
            let _ = std::fs::remove_file(&tmp);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn handle_get(
    State(srv): State<Arc<DumpServer>>,
    UrlPath(token): UrlPath<String>,
) -> axum::response::Response {
    let Some(schema) = srv.authorize(&token, Mode::Get) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    let path = srv.dump_path(&schema);
    match tokio::fs::File::open(&path).await {
        Ok(file) => {
            let stream = tokio_util_compat_stream(file);
            Body::from_stream(stream).into_response()
        }
        Err(e) => {
            warn!("dump download for schema {schema} failed opening {}: {e}", path.display());
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

/// Minimal file→byte-stream adapter (avoids pulling in tokio-util just for
/// `ReaderStream`).
fn tokio_util_compat_stream(
    file: tokio::fs::File,
) -> impl futures::Stream<Item = std::io::Result<Vec<u8>>> {
    futures::stream::unfold(file, |mut file| async move {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 64 * 1024];
        match file.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok(buf), file))
            }
            Err(e) => Some((Err(e), file)),
        }
    })
}

/// Best-effort local dump metadata: size in bytes, `None` when absent.
pub fn dump_size(path: &Path) -> Option<u64> {
    std::fs::metadata(path).ok().map(|m| m.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_mode_and_schema_bound() {
        let srv = DumpServer::new(std::env::temp_dir());
        let put_tok = srv.issue("wb1", Mode::Put).unwrap();
        let get_tok = srv.issue("wb2", Mode::Get).unwrap();
        assert_ne!(put_tok, get_tok);
        assert_eq!(srv.authorize(&put_tok, Mode::Put).as_deref(), Some("wb1"));
        // Wrong mode for the token → refused.
        assert_eq!(srv.authorize(&put_tok, Mode::Get), None);
        assert_eq!(srv.authorize(&get_tok, Mode::Get).as_deref(), Some("wb2"));
        // Unknown token → refused.
        assert_eq!(srv.authorize("nope", Mode::Get), None);
    }

    #[test]
    fn path_hostile_schema_names_are_refused() {
        let srv = DumpServer::new(std::env::temp_dir());
        assert!(srv.issue("../etc/passwd", Mode::Put).is_err());
        assert!(srv.issue("a/b", Mode::Get).is_err());
        assert!(srv.issue("", Mode::Get).is_err());
        assert!(srv.issue("ok_schema-1", Mode::Get).is_ok());
        // A dotted name is a legal schema; joined as `<name>.dump` it stays a
        // single path component, so it must be accepted.
        assert!(srv.issue("v1.2", Mode::Get).is_ok());
    }

    #[tokio::test]
    async fn put_then_get_roundtrip_with_token_gating() {
        let dir = std::env::temp_dir().join(format!("pgfc-dumpsrv-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let srv = Arc::new(DumpServer::new(dir.clone()));

        // Upload under a PUT token: file lands atomically, completion recorded.
        let put_tok = srv.issue("wb", Mode::Put).unwrap();
        let code = handle_put(
            State(srv.clone()),
            UrlPath(put_tok.clone()),
            Body::from("dumpdata"),
        )
        .await
        .into_response()
        .status();
        assert_eq!(code, StatusCode::OK);
        assert_eq!(srv.upload_completed(&put_tok), Some(8));
        assert_eq!(std::fs::read_to_string(dir.join("wb.dump")).unwrap(), "dumpdata");

        // Unknown token → refused; PUT token can't download either.
        let resp = handle_get(State(srv.clone()), UrlPath("bogus".into())).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let resp = handle_get(State(srv.clone()), UrlPath(put_tok)).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // GET token streams the bytes back.
        let get_tok = srv.issue("wb", Mode::Get).unwrap();
        let resp = handle_get(State(srv.clone()), UrlPath(get_tok)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(&body[..], b"dumpdata");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tokens_are_long_and_unique() {
        let srv = DumpServer::new(std::env::temp_dir());
        let a = srv.issue("s", Mode::Get).unwrap();
        let b = srv.issue("s", Mode::Get).unwrap();
        assert_eq!(a.len(), 32);
        assert_ne!(a, b);
    }
}

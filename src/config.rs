//! Runtime configuration, sourced from the environment with sensible defaults.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use heyo_sdk::SandboxSize;

#[derive(Clone)]
pub struct Config {
    /// Where the pooler listens for Postgres clients.
    pub listen_addr: SocketAddr,
    /// Firecracker image name to boot per schema (`heyvm mvm build --name pg`).
    pub image: String,
    /// VM resource tier for every schema's VM (same tier for all of them —
    /// there's no per-schema override). `Micro` (the default) is 1 vCPU
    /// throttled to 0.25 core and 512MB memory; see heyo's `SizeClass` for
    /// the full tier table. Env `PG_VM_POOL_SIZE_CLASS`.
    pub size_class: SandboxSize,
    /// Postgres role the pooler uses for the readiness probe + bootstrap. With
    /// the pg-fc image's `trust` host auth this needs no password.
    pub pg_user: String,
    /// Password for [`Self::pg_user`], and — doing double duty — the password
    /// the pooler itself requires from clients before proxying them anywhere.
    /// `None` (unset) means both: no client auth gate (any client that reaches
    /// `listen_addr` is proxied straight through) and the pg-fc image's
    /// `trust` host auth for the probe. Set it whenever the pooler is
    /// reachable beyond localhost (see [`Self::listen_addr`]) — the backend
    /// VM's own Postgres can stay on `trust`, since this is the layer meant to
    /// gate access instead. Sent in the clear absent client TLS, so pair it
    /// with [`Self::tls_cert`]/[`Self::tls_key`] on any non-loopback listener.
    pub pg_password: Option<String>,
    /// Inactivity timeout: a non-keep-alive VM is stopped after this long with
    /// no open client connections. `None` disables idle reaping (VMs stay up
    /// until manually stopped). The pooler tracks connections and owns this —
    /// the daemon's own TTL can't, since it's absolute from VM boot and the
    /// daemon doesn't see connections. Keep-alive schemas are exempt.
    pub idle_timeout: Option<Duration>,
    /// How long to wait for a VM (and then Postgres) to become ready.
    pub ready_timeout: Duration,
    /// Cap on the iroh tunnel handshake (`expose_tcp` + `P2pTunnel::connect`).
    /// These have no internal timeout, so when iroh's relays churn (e.g. the
    /// host IP flapping on WiFi → "Local IP no longer valid") a bring-up can
    /// block for minutes. Bounding it lets the pooler fail fast and the client
    /// retry, instead of hanging past what the app tolerates. Much shorter than
    /// `ready_timeout` on purpose.
    pub connect_timeout: Duration,
    /// How long a client waits for a free connection slot on its schema's VM
    /// before the pooler gives up and errors it.
    ///
    /// The pooler splices 1:1, so the guest's `max_connections` would
    /// otherwise be enforced by Postgres as a hard `FATAL: too many clients`
    /// on the (N+1)th client. Queueing here turns that into backpressure. Env
    /// `PG_VM_POOL_ADMIT_TIMEOUT_SECS`; `0` disables the wait (fail
    /// immediately when full).
    pub admit_timeout: Duration,
    /// Size (GiB) of the per-schema persistent data disk attached at
    /// `/dev/vdb` and mounted at `/workspace` (where `PGDATA` lives). This is
    /// what makes a schema's data survive a VM stop/start/restart — without it
    /// the VM falls back to the ephemeral rootfs.
    pub data_disk_gb: u32,
    /// Schemas whose VM should be pinned as a permanent keep-alive: exempt from
    /// idle reaping. For a DB under constant access that shouldn't churn through
    /// stop/restart. Others are subject to [`Self::idle_timeout`].
    pub keepalive_schemas: HashSet<String>,
    /// When true (the default), dial the VM's Postgres directly at its host-
    /// reachable `guest_ip` and skip the iroh tunnel — valid only when the
    /// pooler shares the host with the VMs (the local-daemon deployment). Set
    /// `PG_VM_POOL_DIRECT_CONNECT=0` to force the tunnel path (e.g. if the
    /// pooler ever runs on a different machine than the VMs). Falls back to a
    /// tunnel automatically if the daemon reports no `guest_ip`.
    pub direct_connect: bool,
    /// Where the `schema → sandbox-id` map is persisted so the pooler reattaches
    /// to the right VM (by id) after a restart instead of creating a duplicate
    /// with a fresh data disk. Env `PG_VM_POOL_STATE_FILE`.
    pub state_file: PathBuf,
    /// TLS certificate chain + private key (PEM) for client-facing TLS. Both
    /// set → the pooler answers the Postgres `SSLRequest` with `S` and speaks
    /// TLS; unset → it declines (`N`) as before. Files are re-read on change,
    /// so an external renewer (certbot) rotating them needs no restart.
    /// Envs `PG_VM_POOL_TLS_CERT` / `PG_VM_POOL_TLS_KEY`.
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    /// Admin dashboard settings. `dashboard.is_none()` (the default) means the
    /// dashboard is disabled — it's enabled by setting `PG_VM_POOL_DASHBOARD_LISTEN`.
    pub dashboard: Option<DashboardConfig>,
    /// S3 cold-storage (eviction) tier. `None` (the default) disables it. When
    /// `Some`, a background sweep offloads any schema untouched for
    /// [`ArchiveConfig::archive_after`] to S3 and kills its VM to reclaim disk;
    /// the next connect restores it. Enabled by `PG_VM_POOL_ARCHIVE_AFTER_SECS`.
    pub archive: Option<ArchiveConfig>,
}

/// Settings for the optional S3 eviction tier. Present (`Some`) only when
/// `PG_VM_POOL_ARCHIVE_AFTER_SECS` is a positive number — that env var is the
/// on/off switch. When on, the S3 bucket and credentials are required (a
/// half-configured tier fails fast, mirroring the TLS/dashboard pairings).
#[derive(Clone)]
pub struct ArchiveConfig {
    /// A non-keepalive schema untouched (no client connections) for at least
    /// this long is dumped to S3 and its VM killed. This is the slow, disk-
    /// reclaiming tier that sits *below* [`Config::idle_timeout`] (which only
    /// stops the VM). Env `PG_VM_POOL_ARCHIVE_AFTER_SECS`.
    pub archive_after: Duration,
    /// How often the archive sweep scans for eviction candidates. Env
    /// `PG_VM_POOL_ARCHIVE_SWEEP_SECS` (default 3600).
    pub sweep_interval: Duration,
    /// S3 addressing + credentials the pooler uses to presign the guest's
    /// dump upload / restore download.
    pub s3: crate::s3::S3Config,
}

/// Settings for the optional server-side-rendered admin dashboard. Present
/// (`Some`) only when `PG_VM_POOL_DASHBOARD_LISTEN` is set — that env var is the
/// on/off switch.
#[derive(Clone)]
pub struct DashboardConfig {
    /// Where the dashboard's HTTP server listens. Prefer a loopback/private
    /// address; pair a public bind with basic auth. Env `PG_VM_POOL_DASHBOARD_LISTEN`.
    pub listen: SocketAddr,
    /// HTTP Basic auth `(user, password)`. `None` disables the auth gate (a
    /// warning is logged when bound non-loopback). Both must be set together.
    /// Envs `PG_VM_POOL_DASHBOARD_USER` / `PG_VM_POOL_DASHBOARD_PASSWORD`.
    pub basic_auth: Option<(String, String)>,
    /// Path to the pooler's own log file (supervisord captures stdout+stderr
    /// here). Env `PG_VM_POOL_POOLER_LOG`.
    pub pooler_log: PathBuf,
    /// Path to heyvmd's log file. Env `PG_VM_POOL_HEYVMD_LOG`.
    pub heyvmd_log: PathBuf,
    /// How many trailing lines to show when tailing a log. Env
    /// `PG_VM_POOL_DASHBOARD_LOG_LINES` (default 200).
    pub log_lines: usize,
    /// Where the monitoring page's webhook alert rules persist. Defaults to a
    /// sibling of the schema registry under the heyo data dir. Env
    /// `PG_VM_POOL_DASHBOARD_ALERTS_FILE`.
    pub alerts_file: PathBuf,
    /// How often the background evaluator samples host metrics and fires any
    /// crossed alerts. Env `PG_VM_POOL_DASHBOARD_ALERT_INTERVAL_SECS` (default 60).
    pub alert_interval: std::time::Duration,
}

impl Config {
    /// Whether `schema`'s VM should be a permanent keep-alive (TTL 0).
    pub fn is_keepalive(&self, schema: &str) -> bool {
        self.keepalive_schemas.contains(schema)
    }
}

/// Every env var the pooler reads. `from_env` warns about any other
/// `PG_VM_POOL_*` in the environment: a typo'd name (PG_VM_POOL_SIZE for
/// PG_VM_POOL_SIZE_CLASS) otherwise silently falls back to the default and
/// reads as "the pooler ignored my config".
const KNOWN_VARS: &[&str] = &[
    "PG_VM_POOL_LISTEN",
    "PG_VM_POOL_IMAGE",
    "PG_VM_POOL_SIZE_CLASS",
    "PG_VM_POOL_USER",
    "PG_VM_POOL_PASSWORD",
    "PG_VM_POOL_IDLE_TIMEOUT_SECS",
    "PG_VM_POOL_READY_TIMEOUT_SECS",
    "PG_VM_POOL_CONNECT_TIMEOUT_SECS",
    "PG_VM_POOL_ADMIT_TIMEOUT_SECS",
    "PG_VM_POOL_DIRECT_CONNECT",
    "PG_VM_POOL_DATA_DISK_GB",
    "PG_VM_POOL_KEEPALIVE_SCHEMAS",
    "PG_VM_POOL_STATE_FILE",
    "PG_VM_POOL_TLS_CERT",
    "PG_VM_POOL_TLS_KEY",
    "PG_VM_POOL_DASHBOARD_LISTEN",
    "PG_VM_POOL_DASHBOARD_USER",
    "PG_VM_POOL_DASHBOARD_PASSWORD",
    "PG_VM_POOL_POOLER_LOG",
    "PG_VM_POOL_HEYVMD_LOG",
    "PG_VM_POOL_DASHBOARD_LOG_LINES",
    "PG_VM_POOL_DASHBOARD_ALERTS_FILE",
    "PG_VM_POOL_DASHBOARD_ALERT_INTERVAL_SECS",
    "PG_VM_POOL_ARCHIVE_AFTER_SECS",
    "PG_VM_POOL_ARCHIVE_SWEEP_SECS",
    "PG_VM_POOL_S3_BUCKET",
    "PG_VM_POOL_S3_PREFIX",
    "PG_VM_POOL_S3_REGION",
    "PG_VM_POOL_S3_ENDPOINT",
    "PG_VM_POOL_S3_ACCESS_KEY_ID",
    "PG_VM_POOL_S3_SECRET_ACCESS_KEY",
];

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        for (key, _) in std::env::vars() {
            if key.starts_with("PG_VM_POOL_") && !KNOWN_VARS.contains(&key.as_str()) {
                tracing::warn!(
                    "ignoring unknown env var {key} — not a pooler setting \
                     (check the name against the README's config table)"
                );
            }
        }

        let listen =
            std::env::var("PG_VM_POOL_LISTEN").unwrap_or_else(|_| "127.0.0.1:6432".to_string());
        let listen_addr: SocketAddr = listen
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid PG_VM_POOL_LISTEN {listen:?}: {e}"))?;

        let image = std::env::var("PG_VM_POOL_IMAGE").unwrap_or_else(|_| "pg".to_string());
        let size_class = match std::env::var("PG_VM_POOL_SIZE_CLASS") {
            Ok(v) => parse_size_class(&v)?,
            Err(_) => SandboxSize::Micro,
        };
        let pg_user = std::env::var("PG_VM_POOL_USER").unwrap_or_else(|_| "postgres".to_string());
        // Optional; unset means no password (trust auth). An empty value is
        // treated as unset so `PG_VM_POOL_PASSWORD=` doesn't force an empty
        // password.
        let pg_password = std::env::var("PG_VM_POOL_PASSWORD")
            .ok()
            .filter(|p| !p.is_empty());
        // Idle timeout in seconds; default 15 min, `0` disables reaping.
        let idle_timeout = match std::env::var("PG_VM_POOL_IDLE_TIMEOUT_SECS") {
            Ok(v) => match v.parse::<u64>() {
                Ok(0) => None,
                Ok(secs) => Some(Duration::from_secs(secs)),
                Err(_) => Some(Duration::from_secs(900)),
            },
            Err(_) => Some(Duration::from_secs(900)),
        };
        let ready_secs = std::env::var("PG_VM_POOL_READY_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300u64);
        let connect_secs = std::env::var("PG_VM_POOL_CONNECT_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30u64);
        let admit_secs = std::env::var("PG_VM_POOL_ADMIT_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30u64);
        let data_disk_gb = std::env::var("PG_VM_POOL_DATA_DISK_GB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4u32);
        // Default on; only "0"/"false"/"no" (case-insensitive) disables it.
        let direct_connect = match std::env::var("PG_VM_POOL_DIRECT_CONNECT") {
            Ok(v) => !matches!(v.trim().to_ascii_lowercase().as_str(), "0" | "false" | "no"),
            Err(_) => true,
        };
        // Persistent schema→VM map; defaults under the heyo data dir.
        let state_file = std::env::var("PG_VM_POOL_STATE_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
                PathBuf::from(home).join(".heyo/pg-vm-pool/registry.tsv")
            });
        // TLS cert/key PEM paths; empty treated as unset (like PASSWORD above).
        // Setting only one of the pair is a configuration mistake — fail fast
        // rather than silently serving plaintext.
        let tls_cert = std::env::var("PG_VM_POOL_TLS_CERT")
            .ok()
            .filter(|p| !p.is_empty())
            .map(PathBuf::from);
        let tls_key = std::env::var("PG_VM_POOL_TLS_KEY")
            .ok()
            .filter(|p| !p.is_empty())
            .map(PathBuf::from);
        if tls_cert.is_some() != tls_key.is_some() {
            anyhow::bail!(
                "PG_VM_POOL_TLS_CERT and PG_VM_POOL_TLS_KEY must be set together (or neither)"
            );
        }
        // Comma-separated schema names; blanks/whitespace ignored.
        let keepalive_schemas = std::env::var("PG_VM_POOL_KEEPALIVE_SCHEMAS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();

        let dashboard = DashboardConfig::from_env()?;
        let archive = ArchiveConfig::from_env()?;

        Ok(Self {
            listen_addr,
            image,
            size_class,
            pg_user,
            pg_password,
            idle_timeout,
            ready_timeout: Duration::from_secs(ready_secs),
            connect_timeout: Duration::from_secs(connect_secs),
            admit_timeout: Duration::from_secs(admit_secs),
            data_disk_gb,
            keepalive_schemas,
            direct_connect,
            state_file,
            tls_cert,
            tls_key,
            dashboard,
            archive,
        })
    }
}

impl DashboardConfig {
    /// Build the dashboard config from the environment. Returns `Ok(None)` when
    /// `PG_VM_POOL_DASHBOARD_LISTEN` is unset (dashboard disabled). Errors on an
    /// unparseable listen address or a half-set basic-auth credential.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        // Empty string treated as unset, matching the PASSWORD/TLS handling above.
        let Some(listen) = std::env::var("PG_VM_POOL_DASHBOARD_LISTEN")
            .ok()
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let listen: SocketAddr = listen
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid PG_VM_POOL_DASHBOARD_LISTEN {listen:?}: {e}"))?;

        let user = std::env::var("PG_VM_POOL_DASHBOARD_USER")
            .ok()
            .filter(|s| !s.is_empty());
        let password = std::env::var("PG_VM_POOL_DASHBOARD_PASSWORD")
            .ok()
            .filter(|s| !s.is_empty());
        // Half-set credentials are a config mistake: fail fast rather than
        // silently serve unauthenticated (mirrors the TLS cert/key pairing rule).
        let basic_auth = match (user, password) {
            (Some(u), Some(p)) => Some((u, p)),
            (None, None) => None,
            _ => anyhow::bail!(
                "PG_VM_POOL_DASHBOARD_USER and PG_VM_POOL_DASHBOARD_PASSWORD must be \
                 set together (or neither)"
            ),
        };

        let pooler_log = std::env::var("PG_VM_POOL_POOLER_LOG")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/var/log/pg-vm-pool/pg-vm-pool.log"));
        let heyvmd_log = std::env::var("PG_VM_POOL_HEYVMD_LOG")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/var/log/heyvmd/heyvmd.log"));
        let log_lines = std::env::var("PG_VM_POOL_DASHBOARD_LOG_LINES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200usize);
        let alerts_file = std::env::var("PG_VM_POOL_DASHBOARD_ALERTS_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
                PathBuf::from(home).join(".heyo/pg-vm-pool/alerts.tsv")
            });
        let alert_interval = std::env::var("PG_VM_POOL_DASHBOARD_ALERT_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&s| s > 0)
            .map(std::time::Duration::from_secs)
            .unwrap_or_else(|| std::time::Duration::from_secs(60));

        Ok(Some(Self {
            listen,
            basic_auth,
            pooler_log,
            heyvmd_log,
            log_lines,
            alerts_file,
            alert_interval,
        }))
    }
}

impl ArchiveConfig {
    /// Build the S3 eviction config from the environment. `Ok(None)` when
    /// `PG_VM_POOL_ARCHIVE_AFTER_SECS` is unset or `0` (tier disabled). When the
    /// tier is on, the bucket and both credentials are required — a partial
    /// config is a mistake, so it fails fast rather than silently never
    /// archiving. Credentials fall back to the standard `AWS_ACCESS_KEY_ID` /
    /// `AWS_SECRET_ACCESS_KEY` when the namespaced vars are unset.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let archive_after = match std::env::var("PG_VM_POOL_ARCHIVE_AFTER_SECS") {
            Ok(v) => match v.trim().parse::<u64>() {
                Ok(0) | Err(_) => return Ok(None),
                Ok(secs) => Duration::from_secs(secs),
            },
            Err(_) => return Ok(None),
        };
        let sweep_interval = std::env::var("PG_VM_POOL_ARCHIVE_SWEEP_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|s| *s > 0)
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(3600));

        let nonempty = |k: &str| std::env::var(k).ok().filter(|s| !s.trim().is_empty());
        let Some(bucket) = nonempty("PG_VM_POOL_S3_BUCKET") else {
            anyhow::bail!(
                "PG_VM_POOL_ARCHIVE_AFTER_SECS is set but PG_VM_POOL_S3_BUCKET is not — \
                 the S3 eviction tier needs a bucket"
            );
        };
        let access_key_id = nonempty("PG_VM_POOL_S3_ACCESS_KEY_ID")
            .or_else(|| nonempty("AWS_ACCESS_KEY_ID"));
        let secret_access_key = nonempty("PG_VM_POOL_S3_SECRET_ACCESS_KEY")
            .or_else(|| nonempty("AWS_SECRET_ACCESS_KEY"));
        let (Some(access_key_id), Some(secret_access_key)) = (access_key_id, secret_access_key)
        else {
            anyhow::bail!(
                "PG_VM_POOL_ARCHIVE_AFTER_SECS is set but S3 credentials are missing — \
                 set PG_VM_POOL_S3_ACCESS_KEY_ID/PG_VM_POOL_S3_SECRET_ACCESS_KEY (or the \
                 standard AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)"
            );
        };
        let region = nonempty("PG_VM_POOL_S3_REGION").unwrap_or_else(|| "us-east-1".to_string());
        let prefix = std::env::var("PG_VM_POOL_S3_PREFIX")
            .unwrap_or_else(|_| "pg-vm-pool/".to_string());
        let endpoint = nonempty("PG_VM_POOL_S3_ENDPOINT");

        Ok(Some(Self {
            archive_after,
            sweep_interval,
            s3: crate::s3::S3Config {
                bucket,
                prefix,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
            },
        }))
    }
}

/// Case-insensitive, matching heyo's own CLI parsing convention.
pub(crate) fn parse_size_class(v: &str) -> anyhow::Result<SandboxSize> {
    match v.trim().to_ascii_lowercase().as_str() {
        "micro" => Ok(SandboxSize::Micro),
        "mini" => Ok(SandboxSize::Mini),
        "small" => Ok(SandboxSize::Small),
        "medium" => Ok(SandboxSize::Medium),
        "large" => Ok(SandboxSize::Large),
        other => anyhow::bail!(
            "invalid PG_VM_POOL_SIZE_CLASS {other:?}: expected one of micro, mini, small, medium, large"
        ),
    }
}

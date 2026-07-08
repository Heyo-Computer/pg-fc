//! Runtime configuration, sourced from the environment with sensible defaults.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    /// Where the pooler listens for Postgres clients.
    pub listen_addr: SocketAddr,
    /// Firecracker image name to boot per schema (`heyvm mvm build --name pg`).
    pub image: String,
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
}

impl Config {
    /// Whether `schema`'s VM should be a permanent keep-alive (TTL 0).
    pub fn is_keepalive(&self, schema: &str) -> bool {
        self.keepalive_schemas.contains(schema)
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let listen = std::env::var("PG_VM_POOL_LISTEN")
            .unwrap_or_else(|_| "127.0.0.1:6432".to_string());
        let listen_addr: SocketAddr = listen
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid PG_VM_POOL_LISTEN {listen:?}: {e}"))?;

        let image = std::env::var("PG_VM_POOL_IMAGE").unwrap_or_else(|_| "pg".to_string());
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

        Ok(Self {
            listen_addr,
            image,
            pg_user,
            pg_password,
            idle_timeout,
            ready_timeout: Duration::from_secs(ready_secs),
            connect_timeout: Duration::from_secs(connect_secs),
            data_disk_gb,
            keepalive_schemas,
            direct_connect,
            state_file,
            tls_cert,
            tls_key,
        })
    }
}

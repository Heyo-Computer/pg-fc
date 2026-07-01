//! Runtime configuration, sourced from the environment with sensible defaults.

use std::net::SocketAddr;
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
    /// Optional VM TTL — idle VMs self-stop after this many seconds. `None`
    /// leaves them running.
    pub ttl_seconds: Option<u64>,
    /// How long to wait for a VM (and then Postgres) to become ready.
    pub ready_timeout: Duration,
    /// Size (GiB) of the per-schema persistent data disk attached at
    /// `/dev/vdb` and mounted at `/workspace` (where `PGDATA` lives). This is
    /// what makes a schema's data survive a VM stop/start/restart — without it
    /// the VM falls back to the ephemeral rootfs.
    pub data_disk_gb: u32,
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
        let ttl_seconds = std::env::var("PG_VM_POOL_TTL_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok());
        let ready_secs = std::env::var("PG_VM_POOL_READY_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300u64);
        let data_disk_gb = std::env::var("PG_VM_POOL_DATA_DISK_GB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4u32);

        Ok(Self {
            listen_addr,
            image,
            pg_user,
            ttl_seconds,
            ready_timeout: Duration::from_secs(ready_secs),
            data_disk_gb,
        })
    }
}

//! Raw byte splice between a client and its schema's VM Postgres.

use anyhow::{Context, Result};
use tokio::io::{copy_bidirectional, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::registry::SchemaEntry;

/// Dial the schema's VM Postgres (guest IP directly, or the tunnel's local end),
/// replay the buffered StartupMessage, then pipe both directions until either
/// side closes.
pub async fn splice(mut client: TcpStream, entry: &SchemaEntry, startup_raw: &[u8]) -> Result<()> {
    let mut upstream = TcpStream::connect(entry.target)
        .await
        .with_context(|| format!("connecting to VM Postgres at {}", entry.target))?;

    upstream
        .write_all(startup_raw)
        .await
        .context("replaying startup packet upstream")?;
    upstream.flush().await?;

    copy_bidirectional(&mut client, &mut upstream)
        .await
        .context("proxying client <-> VM")?;
    Ok(())
}

//! Raw byte splice between a client and its schema's VM Postgres.

use anyhow::{Context, Result};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, copy_bidirectional};
use tokio::net::TcpStream;
use tracing::warn;

use crate::registry::SchemaEntry;

/// Dial the schema's VM Postgres (guest IP directly, or the tunnel's local end),
/// replay the buffered StartupMessage, then pipe both directions until either
/// side closes. Generic over the client stream: plain TCP or the TLS-upgraded
/// stream from `read_startup` (upstream is always plaintext — TLS terminates
/// at the pooler).
pub async fn splice<C>(mut client: C, entry: &SchemaEntry, startup_raw: &[u8]) -> Result<()>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    let mut upstream = TcpStream::connect(entry.target)
        .await
        .with_context(|| format!("connecting to VM Postgres at {}", entry.target))?;

    // Disable Nagle on the pooler->VM leg. Postgres and libpq both set
    // TCP_NODELAY on their own sockets precisely because the wire protocol is
    // request/response; a byte-splicing proxy in the middle that leaves Nagle
    // on reintroduces the per-round-trip (Nagle + delayed-ACK) latency the
    // endpoints were avoiding, which is what makes a chatty many-small-statement
    // client slower through the pooler than on a direct connection. Best-effort:
    // a failed sockopt must not drop an otherwise-healthy connection.
    if let Err(e) = upstream.set_nodelay(true) {
        warn!(
            "could not set TCP_NODELAY on upstream to {}: {e}",
            entry.target
        );
    }

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

#[cfg(test)]
mod tests {
    use tokio::net::{TcpListener, TcpStream};

    /// The splice path disables Nagle on both legs; this pins that the option
    /// actually takes on the `tokio::net::TcpStream` type the proxy uses, so a
    /// future refactor that drops the call (or a platform where it silently
    /// fails) is caught here rather than as a latency regression in production.
    #[tokio::test]
    async fn set_nodelay_takes_on_both_legs() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept = tokio::spawn(async move { listener.accept().await.unwrap().0 });
        let client = TcpStream::connect(addr).await.unwrap(); // the pooler->VM leg shape
        let server = accept.await.unwrap(); // the client->pooler (accepted) leg shape

        for (sock, leg) in [(&client, "connect"), (&server, "accept")] {
            assert!(
                !sock.nodelay().unwrap(),
                "{leg}: expected Nagle on by default"
            );
            sock.set_nodelay(true).unwrap();
            assert!(sock.nodelay().unwrap(), "{leg}: TCP_NODELAY did not take");
        }
    }
}

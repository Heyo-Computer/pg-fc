//! Minimal Postgres frontend/backend startup parsing.
//!
//! We only read enough of the v3 protocol to (a) answer the SSL/GSS preamble so
//! `psql`/libpq proceed, and (b) pull the `database` parameter out of the
//! `StartupMessage` — that name is our schema/VM routing key. The raw startup
//! bytes are kept so the proxy can replay them verbatim to the VM, leaving the
//! rest of the session a pure byte splice.

use anyhow::{bail, Result};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// Magic protocol "version" codes that aren't real StartupMessages.
const SSL_REQUEST_CODE: i32 = 80877103;
const GSS_ENC_REQUEST_CODE: i32 = 80877104;
const PROTOCOL_V3: i32 = 196608; // 3.0

pub struct StartupInfo {
    /// The database name from the startup packet — our schema key.
    pub database: String,
    #[allow(dead_code)]
    pub user: String,
    /// The full StartupMessage bytes (length prefix included) to replay upstream.
    pub raw: Vec<u8>,
}

/// Read the client's startup handshake, transparently rejecting SSL/GSS
/// encryption requests (we reply `N`), and return the resolved schema plus the
/// StartupMessage to forward to the VM.
pub async fn read_startup(client: &mut TcpStream) -> Result<StartupInfo> {
    loop {
        let mut len_buf = [0u8; 4];
        client.read_exact(&mut len_buf).await?;
        let len = i32::from_be_bytes(len_buf);
        if len < 8 {
            bail!("startup message length too small: {len}");
        }
        let body_len = (len - 4) as usize;
        let mut body = vec![0u8; body_len];
        client.read_exact(&mut body).await?;

        let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        match code {
            SSL_REQUEST_CODE | GSS_ENC_REQUEST_CODE => {
                // Decline encryption; libpq then sends a plain StartupMessage.
                client.write_all(b"N").await?;
                client.flush().await?;
                continue;
            }
            PROTOCOL_V3 => {
                let params = parse_params(&body[4..]);
                let user = params.get("user").cloned().unwrap_or_default();
                let database = params
                    .get("database")
                    .cloned()
                    .filter(|d| !d.is_empty())
                    .unwrap_or_else(|| user.clone());
                if database.is_empty() {
                    bail!("startup packet had neither database nor user");
                }

                let mut raw = Vec::with_capacity(len as usize);
                raw.extend_from_slice(&len_buf);
                raw.extend_from_slice(&body);
                return Ok(StartupInfo { database, user, raw });
            }
            other => bail!("unsupported startup protocol code: {other}"),
        }
    }
}

/// Parameters are a flat `key\0value\0...\0` list terminated by an extra `\0`.
fn parse_params(bytes: &[u8]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut parts = bytes.split(|&b| b == 0);
    loop {
        let key = match parts.next() {
            Some(k) if !k.is_empty() => k,
            _ => break, // empty key == terminator (or no more pairs)
        };
        let val = parts.next().unwrap_or(&[]);
        map.insert(
            String::from_utf8_lossy(key).into_owned(),
            String::from_utf8_lossy(val).into_owned(),
        );
    }
    map
}

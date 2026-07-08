//! Client-facing password gate, run right after the startup handshake and
//! before any bytes reach a schema's VM. This is separate from — and doesn't
//! touch — backend auth: the VM's own Postgres can stay on `trust`, since
//! `PG_VM_POOL_PASSWORD` unset means the pooler was reachable only on
//! loopback; once it's exposed more broadly, this challenge is the layer that
//! actually gates access.
//!
//! Plain `AuthenticationCleartextPassword`, the same auth type pgbouncer's
//! `auth_type=plain` uses: simple enough to implement correctly by hand, at
//! the cost of sending the password in the clear absent client TLS — pair
//! `PG_VM_POOL_PASSWORD` with `PG_VM_POOL_TLS_CERT`/`KEY` on any listener
//! reachable beyond localhost.

use anyhow::{bail, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const AUTH_CLEARTEXT_PASSWORD: i32 = 3;

/// Challenge the client for a password and check it against `expected`.
///
/// On success, the StartupMessage the caller already buffered (`info.raw`)
/// is still what gets replayed upstream — the backend runs its own (typically
/// `trust`) auth and that result is what the client sees next, so from the
/// client's point of view this is indistinguishable from a normal password
/// exchange with the real server.
///
/// On failure, sends a Postgres `ErrorResponse` so the client gets a proper
/// "password authentication failed" rather than a dropped connection, then
/// returns `Err` — the caller must not proceed to the backend.
pub async fn require_password<S>(stream: &mut S, expected: &str) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // AuthenticationCleartextPassword: 'R', len=8, Int32(3).
    let mut challenge = Vec::with_capacity(9);
    challenge.push(b'R');
    challenge.extend_from_slice(&8i32.to_be_bytes());
    challenge.extend_from_slice(&AUTH_CLEARTEXT_PASSWORD.to_be_bytes());
    stream.write_all(&challenge).await?;
    stream.flush().await?;

    let mut tag = [0u8; 1];
    stream.read_exact(&mut tag).await?;
    if tag[0] != b'p' {
        bail!(
            "expected PasswordMessage ('p'), got {:?} instead",
            tag[0] as char
        );
    }
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = i32::from_be_bytes(len_buf);
    if len < 5 {
        bail!("PasswordMessage length too small: {len}");
    }
    let mut body = vec![0u8; (len - 4) as usize];
    stream.read_exact(&mut body).await?;
    let password = body.strip_suffix(&[0]).unwrap_or(&body);

    if constant_time_eq(password, expected.as_bytes()) {
        Ok(())
    } else {
        send_auth_failed(stream).await?;
        bail!("password authentication failed");
    }
}

async fn send_auth_failed<S: AsyncWrite + Unpin>(stream: &mut S) -> Result<()> {
    // ErrorResponse: 'E', len, then NUL-terminated (field-code, value) pairs,
    // closed by a final NUL.
    let mut fields = Vec::new();
    fields.push(b'S');
    fields.extend_from_slice(b"FATAL\0");
    fields.push(b'C');
    fields.extend_from_slice(b"28P01\0"); // invalid_password
    fields.push(b'M');
    fields.extend_from_slice(b"password authentication failed\0");
    fields.push(0);

    let mut msg = Vec::with_capacity(5 + fields.len());
    msg.push(b'E');
    msg.extend_from_slice(&((fields.len() + 4) as i32).to_be_bytes());
    msg.extend_from_slice(&fields);
    stream.write_all(&msg).await?;
    stream.flush().await?;
    Ok(())
}

/// Fixed-time comparison so a client probing the pooler directly can't learn
/// the password one byte at a time from response latency.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncReadExt};

    fn password_message(password: &str) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.push(b'p');
        msg.extend_from_slice(&((5 + password.len()) as i32).to_be_bytes());
        msg.extend_from_slice(password.as_bytes());
        msg.push(0);
        msg
    }

    #[tokio::test]
    async fn accepts_correct_password() {
        let (mut pooler, mut client) = duplex(256);
        let server = tokio::spawn(async move { require_password(&mut pooler, "secret").await });

        let mut challenge = [0u8; 9];
        client.read_exact(&mut challenge).await.unwrap();
        assert_eq!(challenge[0], b'R');

        client
            .write_all(&password_message("secret"))
            .await
            .unwrap();
        drop(client);

        assert!(server.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn rejects_wrong_password_with_error_response() {
        let (mut pooler, mut client) = duplex(256);
        let server = tokio::spawn(async move { require_password(&mut pooler, "secret").await });

        let mut challenge = [0u8; 9];
        client.read_exact(&mut challenge).await.unwrap();

        client
            .write_all(&password_message("wrong"))
            .await
            .unwrap();

        let mut tag = [0u8; 1];
        client.read_exact(&mut tag).await.unwrap();
        assert_eq!(tag[0], b'E');

        assert!(server.await.unwrap().is_err());
    }

    #[test]
    fn constant_time_eq_matches_slice_eq() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secre1"));
        assert!(!constant_time_eq(b"secret", b"short"));
    }
}

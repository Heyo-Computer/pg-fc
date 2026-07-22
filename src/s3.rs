//! Minimal AWS Signature Version 4 **query-string** presigner for S3.
//!
//! The pooler never uploads or downloads a dump itself — the guest VM does,
//! streaming `pg_dump | curl -T` and `curl | pg_restore` straight to/from S3.
//! What the pooler produces is a *presigned URL*: a plain HTTPS URL carrying an
//! `X-Amz-Signature` query param that authorizes one `PUT`/`GET` of one object
//! for a bounded window. The signature is derived from the secret key but does
//! not contain it, so the URL is safe to hand to a guest shell command; the
//! secret stays in this process.
//!
//! Only what S3 object storage needs is implemented: `SignedHeaders=host`, an
//! `UNSIGNED-PAYLOAD` body hash (so the guest can stream a body of unknown size
//! under a signature that doesn't cover its bytes), and virtual-hosted-style
//! addressing with an optional path-style override for S3-compatible stores
//! (MinIO/R2). The signing math is checked against AWS's own documented example
//! vector in the tests.

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Everything needed to address and sign S3 requests for the archive tier.
/// Owned by [`crate::config::ArchiveConfig`]; cloned into the registry.
#[derive(Clone)]
pub struct S3Config {
    pub bucket: String,
    /// Key prefix (e.g. `pg-vm-pool/`); joined with `{schema}.dump`.
    pub prefix: String,
    pub region: String,
    /// Custom endpoint for an S3-compatible store (MinIO/R2), e.g.
    /// `https://minio.internal:9000`. `None` → AWS virtual-hosted addressing.
    /// When set, path-style addressing is used (`{endpoint}/{bucket}/{key}`).
    pub endpoint: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl S3Config {
    /// The object key for a schema's dump: `{prefix}{schema}.dump`.
    pub fn object_key(&self, schema: &str) -> String {
        format!("{}{}.dump", self.prefix, schema)
    }

    /// The virtual-hosted S3 host (`{bucket}.s3.{region}.amazonaws.com`) that a
    /// guest `curl` should be pinned to via `--resolve`, or `None` for a custom
    /// endpoint (MinIO/R2) — there the endpoint is expected to be resolvable by
    /// the guest itself, so we don't override its DNS. Mirrors the AWS branch of
    /// [`Self::address`].
    pub fn resolve_host(&self) -> Option<String> {
        match &self.endpoint {
            Some(_) => None,
            None => Some(format!("{}.s3.{}.amazonaws.com", self.bucket, self.region)),
        }
    }

    /// Presign a `PUT` of `key`, valid for `expires`. Feed to `curl -T file`.
    pub fn presign_put(&self, key: &str, expires: std::time::Duration) -> String {
        self.presign("PUT", key, expires.as_secs(), now_unix())
    }

    /// Presign a `GET` of `key`, valid for `expires`. Feed to `curl -o file`.
    pub fn presign_get(&self, key: &str, expires: std::time::Duration) -> String {
        self.presign("GET", key, expires.as_secs(), now_unix())
    }

    /// Presign a `HEAD` of `key`. SigV4 signs the method, so this cannot be a
    /// GET URL used with a HEAD request — it needs its own signature.
    pub fn presign_head(&self, key: &str, expires: std::time::Duration) -> String {
        self.presign("HEAD", key, expires.as_secs(), now_unix())
    }

    /// What the object at `key` currently is, as far as S3 is concerned:
    /// `Ok(None)` for "no such object", `Ok(Some(id))` for one that exists.
    ///
    /// Used to confirm an archive actually landed before its source VM's disk is
    /// reclaimed. Unlike the guest's own report, this asks the system of record —
    /// and it travels the pooler's network, not the guest's serial console, so it
    /// still answers when the guest exec channel is wedged.
    pub async fn head_object(
        &self,
        http: &reqwest::Client,
        key: &str,
        timeout: std::time::Duration,
    ) -> anyhow::Result<Option<ObjectId>> {
        use anyhow::Context;
        let url = self.presign_head(key, HEAD_PRESIGN_TTL);
        let resp = http
            .head(&url)
            .timeout(timeout)
            .send()
            .await
            .with_context(|| format!("HEAD s3://{}/{key}", self.bucket))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            anyhow::bail!("HEAD s3://{}/{key} returned {}", self.bucket, resp.status());
        }
        let header = |name: &str| {
            resp.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string()
        };
        Ok(Some(ObjectId {
            etag: header("etag"),
            last_modified: header("last-modified"),
            content_length: resp.content_length().unwrap_or_default(),
        }))
    }

    /// Resolve `(scheme://host, canonical_uri)` for `key`. Virtual-hosted by
    /// default; path-style when a custom endpoint is configured. `canonical_uri`
    /// is the URI-encoded object path with `/` preserved — exactly what goes
    /// into both the signature and the final URL.
    fn address(&self, key: &str) -> (String, String, String) {
        match &self.endpoint {
            Some(ep) => {
                let ep = ep.trim_end_matches('/');
                let (scheme, host) = split_scheme_host(ep);
                let uri = format!("/{}/{}", self.bucket, encode_uri_path(key));
                (scheme.to_string(), host.to_string(), uri)
            }
            None => {
                let host = format!("{}.s3.{}.amazonaws.com", self.bucket, self.region);
                let uri = format!("/{}", encode_uri_path(key));
                ("https".to_string(), host, uri)
            }
        }
    }

    /// Core SigV4 query-string presign. `unix_secs` is the signing time (the
    /// tests pin it; the public methods pass the wall clock).
    fn presign(&self, method: &str, key: &str, expires: u64, unix_secs: u64) -> String {
        let (scheme, host, canonical_uri) = self.address(key);
        presign_core(PresignParams {
            method,
            scheme: &scheme,
            host: &host,
            canonical_uri: &canonical_uri,
            region: &self.region,
            access_key: &self.access_key_id,
            secret_key: &self.secret_access_key,
            unix_secs,
            expires,
        })
    }
}

/// Short-lived: a HEAD the pooler issues immediately, unlike the guest URLs that
/// must outlive a long transfer.
const HEAD_PRESIGN_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Identity of a stored object, enough to tell "the archive I just asked for"
/// from "the one left by the previous archive of this schema" — the keys are the
/// same, so presence alone proves nothing.
///
/// `etag` alone would be ambiguous for a dump whose bytes are identical to the
/// previous one (ETag is the content MD5 for a single-part PUT); `last_modified`
/// alone has 1-second resolution. Together they are decisive in practice — and a
/// `pg_dump -Fc` archive embeds its own creation timestamp, so byte-identical
/// consecutive dumps do not really occur.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectId {
    pub etag: String,
    pub last_modified: String,
    pub content_length: u64,
}

struct PresignParams<'a> {
    method: &'a str,
    scheme: &'a str,
    host: &'a str,
    canonical_uri: &'a str,
    region: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
    unix_secs: u64,
    expires: u64,
}

/// The SigV4 signing procedure, decoupled from `S3Config`/wall-clock so a fixed
/// input yields a fixed signature the tests can assert (see AWS's documented
/// example). Service is always `s3`; signed headers are always `host`.
fn presign_core(p: PresignParams) -> String {
    let (date, amz_date) = format_amz_time(p.unix_secs);
    let scope = format!("{date}/{}/s3/aws4_request", p.region);
    let credential = format!("{}/{scope}", p.access_key);

    // Query params that participate in the signature, sorted by key (they are
    // already in sorted order here, which S3 requires for the canonical query).
    let mut query: Vec<(String, String)> = vec![
        ("X-Amz-Algorithm".into(), "AWS4-HMAC-SHA256".into()),
        ("X-Amz-Credential".into(), credential),
        ("X-Amz-Date".into(), amz_date.clone()),
        ("X-Amz-Expires".into(), p.expires.to_string()),
        ("X-Amz-SignedHeaders".into(), "host".into()),
    ];
    query.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical_query = query
        .iter()
        .map(|(k, v)| format!("{}={}", encode_uri_component(k), encode_uri_component(v)))
        .collect::<Vec<_>>()
        .join("&");

    let canonical_headers = format!("host:{}\n", p.host);
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\nhost\nUNSIGNED-PAYLOAD",
        p.method, p.canonical_uri, canonical_query, canonical_headers
    );

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(sha256(canonical_request.as_bytes()))
    );

    let signing_key = signing_key(p.secret_key, &date, p.region, "s3");
    let signature = hex::encode(hmac(&signing_key, string_to_sign.as_bytes()));

    format!(
        "{}://{}{}?{}&X-Amz-Signature={}",
        p.scheme, p.host, p.canonical_uri, canonical_query, signature
    )
}

/// Derive the SigV4 signing key: HMAC chain over date → region → service →
/// `aws4_request`, seeded with `"AWS4" + secret`.
fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    hmac(&k_service, b"aws4_request")
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256(data: &[u8]) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().to_vec()
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Split `scheme://host[:port]` into `("https"|"http", "host[:port]")`.
/// Defaults to `https` and treats the whole string as host if no scheme.
fn split_scheme_host(url: &str) -> (&str, &str) {
    if let Some(rest) = url.strip_prefix("https://") {
        ("https", rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        ("http", rest)
    } else {
        ("https", url)
    }
}

/// RFC 3986 encode a URI **path**: unreserved chars pass through, `/` is
/// preserved (segment separators), everything else is `%XX`.
fn encode_uri_path(s: &str) -> String {
    encode(s, true)
}

/// RFC 3986 encode a query component: like [`encode_uri_path`] but `/` is also
/// escaped (`%2F`) — required for the slashes inside `X-Amz-Credential`.
fn encode_uri_component(s: &str) -> String {
    encode(s, false)
}

fn encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Format a UNIX timestamp as SigV4's `(YYYYMMDD, YYYYMMDDTHHMMSSZ)` pair, in
/// UTC, with no external date crate. Uses Howard Hinnant's civil-from-days.
fn format_amz_time(secs: u64) -> (String, String) {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (hour, min, sec) = (rem / 3600, (rem % 3600) / 60, rem % 60);

    // days since 1970-01-01 → (year, month, day), Gregorian.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = year + i64::from(month <= 2);

    let date = format!("{year:04}{month:02}{day:02}");
    let datetime = format!("{date}T{hour:02}{min:02}{sec:02}Z");
    (date, datetime)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS's own documented SigV4 query-string example, which pins the exact
    /// signature for a known key/date/region/object. If our canonical-request
    /// assembly, scope, or HMAC chain drifts by a single byte, this fails.
    /// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
    #[test]
    fn matches_aws_documented_example() {
        // 2013-05-24T00:00:00Z.
        let unix = 1_369_353_600;
        let url = presign_core(PresignParams {
            method: "GET",
            scheme: "https",
            host: "examplebucket.s3.amazonaws.com",
            canonical_uri: "/test.txt",
            region: "us-east-1",
            access_key: "AKIAIOSFODNN7EXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            unix_secs: unix,
            expires: 86_400,
        });
        assert!(
            url.ends_with(
                "&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
            ),
            "unexpected presigned URL: {url}"
        );
        // Sanity on the non-signature portions.
        assert!(url.starts_with("https://examplebucket.s3.amazonaws.com/test.txt?"));
        assert!(url.contains("X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request"));
        assert!(url.contains("X-Amz-Date=20130524T000000Z"));
        assert!(url.contains("X-Amz-Expires=86400"));
        assert!(url.contains("X-Amz-SignedHeaders=host"));
    }

    /// SigV4 signs the HTTP method, so the archive-confirmation HEAD needs its
    /// own signature — reusing the GET URL would 403 and make every dump look
    /// like it never landed.
    #[test]
    fn head_is_signed_as_its_own_method() {
        let unix = 1_369_353_600;
        let params = |method| PresignParams {
            method,
            scheme: "https",
            host: "examplebucket.s3.amazonaws.com",
            canonical_uri: "/test.txt",
            region: "us-east-1",
            access_key: "AKIAIOSFODNN7EXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            unix_secs: unix,
            expires: 60,
        };
        let head = presign_core(params("HEAD"));
        let get = presign_core(params("GET"));
        assert_ne!(
            head, get,
            "HEAD and GET must not share a signature — the method is signed"
        );
        // Everything but the signature is identical, so the difference is the
        // signature itself rather than some incidental URL change.
        let strip = |u: &str| u.split("&X-Amz-Signature=").next().unwrap().to_string();
        assert_eq!(strip(&head), strip(&get));
    }

    #[test]
    fn civil_time_formats_utc() {
        assert_eq!(
            format_amz_time(1_369_353_600),
            ("20130524".into(), "20130524T000000Z".into())
        );
        // Epoch.
        assert_eq!(
            format_amz_time(0),
            ("19700101".into(), "19700101T000000Z".into())
        );
        // A leap-year date with non-zero time: 2024-02-29T13:37:11Z.
        assert_eq!(
            format_amz_time(1_709_213_831),
            ("20240229".into(), "20240229T133711Z".into())
        );
    }

    #[test]
    fn virtual_hosted_address_uses_region_host() {
        let cfg = S3Config {
            bucket: "wb".into(),
            prefix: "pg-vm-pool/".into(),
            region: "us-west-2".into(),
            endpoint: None,
            access_key_id: "AK".into(),
            secret_access_key: "sk".into(),
        };
        let (scheme, host, uri) = cfg.address(&cfg.object_key("tenant_west"));
        assert_eq!(scheme, "https");
        assert_eq!(host, "wb.s3.us-west-2.amazonaws.com");
        assert_eq!(uri, "/pg-vm-pool/tenant_west.dump");
    }

    #[test]
    fn custom_endpoint_uses_path_style() {
        let cfg = S3Config {
            bucket: "wb".into(),
            prefix: "".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://minio.internal:9000/".into()),
            access_key_id: "AK".into(),
            secret_access_key: "sk".into(),
        };
        let (scheme, host, uri) = cfg.address(&cfg.object_key("s"));
        assert_eq!(scheme, "http");
        assert_eq!(host, "minio.internal:9000");
        assert_eq!(uri, "/wb/s.dump");
    }

    #[test]
    fn resolve_host_only_for_aws_virtual_hosted() {
        let aws = S3Config {
            bucket: "wb".into(),
            prefix: "".into(),
            region: "us-east-2".into(),
            endpoint: None,
            access_key_id: "AK".into(),
            secret_access_key: "sk".into(),
        };
        assert_eq!(aws.resolve_host().as_deref(), Some("wb.s3.us-east-2.amazonaws.com"));
        // Custom endpoint: leave DNS to the guest, so no --resolve host.
        let custom = S3Config {
            endpoint: Some("http://minio.internal:9000".into()),
            ..aws
        };
        assert_eq!(custom.resolve_host(), None);
    }

    #[test]
    fn query_component_escapes_slash_but_path_keeps_it() {
        assert_eq!(encode_uri_component("a/b"), "a%2Fb");
        assert_eq!(encode_uri_path("a/b"), "a/b");
        // Unreserved set is preserved verbatim.
        assert_eq!(encode_uri_component("Az9-_.~"), "Az9-_.~");
    }
}

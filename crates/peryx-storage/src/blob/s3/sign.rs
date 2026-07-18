//! AWS Signature Version 4 for S3-compatible endpoints.
//!
//! Only the header-based signing S3 needs is here: a canonical request, a string to sign, the
//! date-scoped signing-key chain, and the `Authorization` header. HMAC-SHA256 is spelled out against
//! `sha2` directly so the crate keeps one digest version instead of pulling the `hmac` crate's older
//! one. Signing takes an explicit timestamp so a test pins a known signature.

use base64::Engine as _;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;

use super::config::S3Credentials;

/// The sha256 of an empty body, sent as `x-amz-content-sha256` for requests without a payload.
pub const EMPTY_PAYLOAD_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

const SERVICE: &str = "s3";
const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// RFC 3986 unreserved set: everything else is percent-encoded. The path form additionally keeps
/// `/` so key separators survive; the query form encodes `/` as well.
const QUERY: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_').remove(b'~');
const PATH: &AsciiSet = &QUERY.remove(b'/');

#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

#[must_use]
pub fn sha256_base64(digest: &[u8; 32]) -> String {
    base64::engine::general_purpose::STANDARD.encode(digest)
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.iter().map(|byte| byte ^ 0x36).collect::<Vec<_>>());
    inner.update(message);
    let mut outer = Sha256::new();
    outer.update(block.iter().map(|byte| byte ^ 0x5c).collect::<Vec<_>>());
    outer.update(inner.finalize());
    outer.finalize().into()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

/// The pieces of a signed request that the caller attaches to the outgoing headers.
pub struct Signed {
    pub authorization: String,
    pub amz_date: String,
    pub content_sha256: String,
    pub security_token: Option<String>,
}

/// A request to sign: its method, path, sorted query, extra signed headers, and payload hash.
pub struct CanonicalRequest<'request> {
    pub method: &'request str,
    pub host: &'request str,
    pub path: &'request str,
    pub query: &'request [(String, String)],
    pub extra_headers: &'request [(&'request str, String)],
    pub payload_hash: &'request str,
}

/// Sign `request` for `region`, returning the headers to send. `now` scopes the credential and dates
/// the signature.
#[must_use]
pub fn sign(request: &CanonicalRequest<'_>, credentials: &S3Credentials, region: &str, now: OffsetDateTime) -> Signed {
    let amz_date = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    );
    let datestamp = &amz_date[..8];
    let scope = format!("{datestamp}/{region}/{SERVICE}/aws4_request");

    let mut headers: Vec<(String, String)> = vec![
        ("host".to_owned(), request.host.to_owned()),
        ("x-amz-content-sha256".to_owned(), request.payload_hash.to_owned()),
        ("x-amz-date".to_owned(), amz_date.clone()),
    ];
    if let Some(token) = &credentials.session_token {
        headers.push(("x-amz-security-token".to_owned(), token.clone()));
    }
    for (name, value) in request.extra_headers {
        headers.push(((*name).to_owned(), value.clone()));
    }
    headers.sort_by(|left, right| left.0.cmp(&right.0));
    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers = headers.iter().fold(String::new(), |mut acc, (name, value)| {
        acc.push_str(name);
        acc.push(':');
        acc.push_str(value.trim());
        acc.push('\n');
        acc
    });

    let mut query = request.query.to_vec();
    query.sort();
    let canonical_query = query
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                utf8_percent_encode(key, QUERY),
                utf8_percent_encode(value, QUERY)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    let canonical_path = utf8_percent_encode(request.path, PATH).to_string();

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method, canonical_path, canonical_query, canonical_headers, signed_headers, request.payload_hash,
    );
    let string_to_sign = format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let date_key = hmac_sha256(
        format!("AWS4{}", credentials.secret_access_key).as_bytes(),
        datestamp.as_bytes(),
    );
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, SERVICE.as_bytes());
    let signing_key = hmac_sha256(&service_key, b"aws4_request");
    let signature = hex(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    let authorization = format!(
        "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        credentials.access_key_id,
    );
    Signed {
        authorization,
        amz_date,
        content_sha256: request.payload_hash.to_owned(),
        security_token: credentials.session_token.clone(),
    }
}

#[cfg(test)]
mod tests {
    use time::{Date, Month, OffsetDateTime, Time};

    use super::{CanonicalRequest, EMPTY_PAYLOAD_HASH, sha256_base64, sha256_hex, sign};
    use crate::blob::S3Credentials;

    fn at(year: i32, month: Month, day: u8) -> OffsetDateTime {
        OffsetDateTime::new_utc(
            Date::from_calendar_date(year, month, day).unwrap(),
            Time::from_hms(0, 0, 0).unwrap(),
        )
    }

    #[test]
    fn test_sign_matches_the_aws_get_object_example() {
        let credentials = S3Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_owned(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_owned(),
            session_token: None,
        };
        let signed = sign(
            &CanonicalRequest {
                method: "GET",
                host: "examplebucket.s3.amazonaws.com",
                path: "/test.txt",
                query: &[],
                extra_headers: &[("range", "bytes=0-9".to_owned())],
                payload_hash: EMPTY_PAYLOAD_HASH,
            },
            &credentials,
            "us-east-1",
            at(2013, Month::May, 24),
        );
        assert_eq!(signed.amz_date, "20130524T000000Z");
        assert!(signed.security_token.is_none());
        assert_eq!(
            signed.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn test_sign_signs_query_and_session_token() {
        let credentials = S3Credentials {
            access_key_id: "AKID".to_owned(),
            secret_access_key: "secret".to_owned(),
            session_token: Some("token".to_owned()),
        };
        let signed = sign(
            &CanonicalRequest {
                method: "POST",
                host: "127.0.0.1:9000",
                path: "/bucket/sha256/deadbeef",
                query: &[("uploadId".to_owned(), "abc-123".to_owned())],
                extra_headers: &[],
                payload_hash: EMPTY_PAYLOAD_HASH,
            },
            &credentials,
            "us-east-1",
            at(2024, Month::January, 2),
        );
        assert_eq!(signed.security_token.as_deref(), Some("token"));
        assert!(signed.authorization.contains("x-amz-security-token"));
        assert!(
            signed
                .authorization
                .starts_with("AWS4-HMAC-SHA256 Credential=AKID/20240102/us-east-1/s3/")
        );
    }

    #[test]
    fn test_hmac_handles_a_key_longer_than_the_block() {
        // RFC 4231 test case 6 exercises the key-longer-than-block hashing path.
        let mac = super::hmac_sha256(&[0xaa; 131], b"Test Using Larger Than Block-Size Key - Hash Key First");
        assert_eq!(
            super::hex(&mac),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn test_sha256_helpers() {
        assert_eq!(sha256_hex(b""), EMPTY_PAYLOAD_HASH);
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256_base64(&[0u8; 32]),
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        );
    }
}

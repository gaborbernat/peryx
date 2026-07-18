//! A minimal S3 REST client: the object and multipart calls the blob backend needs, signed with
//! `SigV4` and retried within a bound.
//!
//! Request bodies are in-memory `Bytes` so a payload hash is always available for signing and memory
//! stays bounded by the part size; a `GET` body streams straight through instead. Idempotent,
//! digest-keyed objects make every call safe to retry, so transient transport failures and 5xx/429
//! responses back off and try again up to the configured limit.

use std::fmt::Write as _;
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use reqwest::{Method, StatusCode, Url};
use time::OffsetDateTime;

use super::config::{S3Addressing, S3Config, S3Credentials};
use super::sign::{self, CanonicalRequest, EMPTY_PAYLOAD_HASH};

/// An S3 request that did not succeed.
#[derive(Debug)]
pub enum S3Error {
    /// The object or bucket is absent (404).
    NotFound,
    /// A response outside the success range. `code` is the S3 error code when the body carried one.
    Unexpected {
        status: u16,
        code: Option<String>,
        message: String,
    },
    /// The request never produced a response within the retry budget.
    Transport(String),
}

impl From<reqwest::Error> for S3Error {
    fn from(error: reqwest::Error) -> Self {
        Self::Transport(error.to_string())
    }
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => formatter.write_str("object not found"),
            Self::Unexpected { status, code, message } => match code {
                Some(code) => write!(formatter, "s3 responded {status} ({code}): {message}"),
                None => write!(formatter, "s3 responded {status}: {message}"),
            },
            Self::Transport(message) => write!(formatter, "s3 request failed: {message}"),
        }
    }
}

/// Object metadata a `HEAD` returns.
#[derive(Debug, Clone, Copy)]
pub struct S3Head {
    pub bytes: u64,
}

/// A streaming `GET` response: the object's full size plus the ranged body.
pub struct S3Get {
    pub total_bytes: u64,
    pub body: BoxStream<'static, Result<Bytes, S3Error>>,
}

/// One finished multipart part, echoed back to `CompleteMultipartUpload`.
#[derive(Debug, Clone)]
pub struct S3Part {
    pub number: u32,
    pub etag: String,
}

/// A signed, retrying S3 REST client bound to one bucket.
#[derive(Debug, Clone)]
pub struct S3Client {
    http: reqwest::Client,
    config: S3Config,
    credentials: S3Credentials,
}

impl S3Client {
    /// Build a client for `config` using `credentials`.
    ///
    /// # Panics
    /// Panics if the HTTP client cannot be built, which requires the TLS backend to fail to
    /// initialize; the installed ring provider rules that out.
    #[must_use]
    pub fn new(config: S3Config, credentials: S3Credentials) -> Self {
        // reqwest is built with `rustls-no-provider`, so a process-wide crypto provider must be
        // installed before the first client is built. Installing is idempotent across clients. The
        // build only fails when the TLS backend cannot initialize, which the installed provider rules
        // out.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .expect("reqwest client builds with the installed ring TLS provider");
        Self {
            http,
            config,
            credentials,
        }
    }

    #[must_use]
    pub const fn config(&self) -> &S3Config {
        &self.config
    }

    fn object_url(&self, key: &str) -> (Url, String, String) {
        let mut url = self.config.endpoint.clone();
        let host = self.config.endpoint.host_str().unwrap_or_default();
        let (authority, path) = match self.config.addressing {
            S3Addressing::Path => {
                let host = self
                    .config
                    .endpoint
                    .port()
                    .map_or_else(|| host.to_owned(), |port| format!("{host}:{port}"));
                (host, format!("/{}/{key}", self.config.bucket))
            }
            S3Addressing::VirtualHost => {
                let host = format!("{}.{host}", self.config.bucket);
                let authority = self
                    .config
                    .endpoint
                    .port()
                    .map_or_else(|| host.clone(), |port| format!("{host}:{port}"));
                url.set_host(Some(&host)).ok();
                (authority, format!("/{key}"))
            }
        };
        url.set_path(&path);
        (url, authority, path)
    }

    fn bucket_url(&self) -> (Url, String, String) {
        let (mut url, host, _) = self.object_url("");
        let path = match self.config.addressing {
            S3Addressing::Path => format!("/{}", self.config.bucket),
            S3Addressing::VirtualHost => "/".to_owned(),
        };
        url.set_path(&path);
        (url, host, path)
    }

    /// Confirm the bucket is reachable with the configured credentials.
    ///
    /// # Errors
    /// Returns [`S3Error`] on a failed or unauthorized listing.
    pub async fn health(&self) -> Result<(), S3Error> {
        let (mut url, host, path) = self.bucket_url();
        url.set_query(Some("list-type=2&max-keys=0"));
        let query = vec![
            ("list-type".to_owned(), "2".to_owned()),
            ("max-keys".to_owned(), "0".to_owned()),
        ];
        self.send(Method::GET, url, &host, &path, &query, &[], None, EMPTY_PAYLOAD_HASH)
            .await
            .map(drop)
    }

    /// `HEAD` an object, returning `None` when it is absent.
    ///
    /// # Errors
    /// Returns [`S3Error`] on a non-404 failure.
    pub async fn head(&self, key: &str) -> Result<Option<S3Head>, S3Error> {
        let (url, host, path) = self.object_url(key);
        match self
            .send(Method::HEAD, url, &host, &path, &[], &[], None, EMPTY_PAYLOAD_HASH)
            .await
        {
            // S3 always answers a successful HEAD with Content-Length.
            Ok(response) => Ok(Some(S3Head {
                bytes: content_length_header(&response).unwrap_or_default(),
            })),
            Err(S3Error::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// `GET` an object, optionally an end-exclusive byte range.
    ///
    /// # Errors
    /// Returns [`S3Error::NotFound`] when absent, or [`S3Error`] on another failure.
    pub async fn get(&self, key: &str, range: Option<Range<u64>>) -> Result<S3Get, S3Error> {
        let (url, host, path) = self.object_url(key);
        let header = range.as_ref().map(|range| {
            // HTTP ranges are inclusive; the caller's is end-exclusive.
            (
                "range",
                format!("bytes={}-{}", range.start, range.end.saturating_sub(1)),
            )
        });
        let headers = header.as_slice();
        let response = self
            .send(Method::GET, url, &host, &path, &[], headers, None, EMPTY_PAYLOAD_HASH)
            .await?;
        let total_bytes = total_bytes(&response);
        let body = response.bytes_stream().map_err(S3Error::from).boxed();
        Ok(S3Get { total_bytes, body })
    }

    /// `PUT` a whole object in one request.
    ///
    /// # Errors
    /// Returns [`S3Error`] on a failed write.
    pub async fn put(&self, key: &str, body: Bytes, payload_hash: &str, checksum: &str) -> Result<(), S3Error> {
        let (url, host, path) = self.object_url(key);
        let headers = [("x-amz-checksum-sha256", checksum.to_owned())];
        self.send(Method::PUT, url, &host, &path, &[], &headers, Some(body), payload_hash)
            .await
            .map(drop)
    }

    /// Delete an object. S3 delete is idempotent, so absence is not an error here.
    ///
    /// # Errors
    /// Returns [`S3Error`] on a failed delete.
    pub async fn delete(&self, key: &str) -> Result<(), S3Error> {
        let (url, host, path) = self.object_url(key);
        match self
            .send(Method::DELETE, url, &host, &path, &[], &[], None, EMPTY_PAYLOAD_HASH)
            .await
        {
            Ok(_) | Err(S3Error::NotFound) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Start a multipart upload, returning its id.
    ///
    /// # Errors
    /// Returns [`S3Error`] on failure or a response without an upload id.
    pub async fn create_multipart(&self, key: &str) -> Result<String, S3Error> {
        let (mut url, host, path) = self.object_url(key);
        url.set_query(Some("uploads="));
        let query = vec![("uploads".to_owned(), String::new())];
        let response = self
            .send(Method::POST, url, &host, &path, &query, &[], None, EMPTY_PAYLOAD_HASH)
            .await?;
        let body = read_body(response).await?;
        extract_tag(&body, "UploadId").ok_or_else(|| S3Error::Unexpected {
            status: 200,
            code: None,
            message: "multipart response carried no UploadId".to_owned(),
        })
    }

    /// Upload one part, returning its `ETag`.
    ///
    /// # Errors
    /// Returns [`S3Error`] on failure or a response without an `ETag`.
    pub async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        number: u32,
        body: Bytes,
        payload_hash: &str,
    ) -> Result<S3Part, S3Error> {
        let (mut url, host, path) = self.object_url(key);
        let query = vec![
            ("partNumber".to_owned(), number.to_string()),
            ("uploadId".to_owned(), upload_id.to_owned()),
        ];
        url.set_query(Some(&format!("partNumber={number}&uploadId={upload_id}")));
        let response = self
            .send(Method::PUT, url, &host, &path, &query, &[], Some(body), payload_hash)
            .await?;
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .ok_or_else(|| S3Error::Unexpected {
                status: response.status().as_u16(),
                code: None,
                message: "upload part response carried no ETag".to_owned(),
            })?;
        Ok(S3Part { number, etag })
    }

    /// Complete a multipart upload from its finished parts.
    ///
    /// # Errors
    /// Returns [`S3Error`] on a failed completion.
    pub async fn complete_multipart(&self, key: &str, upload_id: &str, parts: &[S3Part]) -> Result<(), S3Error> {
        let (mut url, host, path) = self.object_url(key);
        let query = vec![("uploadId".to_owned(), upload_id.to_owned())];
        url.set_query(Some(&format!("uploadId={upload_id}")));
        let mut xml = String::from("<CompleteMultipartUpload>");
        for part in parts {
            let _ = write!(
                xml,
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                part.number,
                escape_xml(&part.etag)
            );
        }
        xml.push_str("</CompleteMultipartUpload>");
        let body = Bytes::from(xml);
        let payload_hash = sign::sha256_hex(&body);
        let response = self
            .send(Method::POST, url, &host, &path, &query, &[], Some(body), &payload_hash)
            .await?;
        // S3 can report a completion failure inside a 200 body; treat an Error element as a failure.
        let body = read_body(response).await?;
        if let Some(code) = extract_tag(&body, "Code") {
            return Err(S3Error::Unexpected {
                status: 200,
                code: Some(code),
                message: extract_tag(&body, "Message").unwrap_or_default(),
            });
        }
        Ok(())
    }

    /// Abort a multipart upload so its uploaded parts stop consuming storage.
    ///
    /// # Errors
    /// Returns [`S3Error`] on a failed abort.
    pub async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<(), S3Error> {
        let (mut url, host, path) = self.object_url(key);
        let query = vec![("uploadId".to_owned(), upload_id.to_owned())];
        url.set_query(Some(&format!("uploadId={upload_id}")));
        match self
            .send(Method::DELETE, url, &host, &path, &query, &[], None, EMPTY_PAYLOAD_HASH)
            .await
        {
            Ok(_) | Err(S3Error::NotFound) => Ok(()),
            Err(error) => Err(error),
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one private request builder keeps signing in one place"
    )]
    async fn send(
        &self,
        method: Method,
        url: Url,
        host: &str,
        path: &str,
        query: &[(String, String)],
        extra_headers: &[(&str, String)],
        body: Option<Bytes>,
        payload_hash: &str,
    ) -> Result<reqwest::Response, S3Error> {
        let mut backoff = Duration::from_millis(50);
        let mut attempt = 0;
        loop {
            let signed = sign::sign(
                &CanonicalRequest {
                    method: method.as_str(),
                    host,
                    path,
                    query,
                    extra_headers,
                    payload_hash,
                },
                &self.credentials,
                &self.config.region,
                OffsetDateTime::now_utc(),
            );
            let mut request = self
                .http
                .request(method.clone(), url.clone())
                .header(reqwest::header::HOST, host)
                .header("x-amz-date", &signed.amz_date)
                .header("x-amz-content-sha256", &signed.content_sha256)
                .header(reqwest::header::AUTHORIZATION, &signed.authorization);
            if let Some(token) = &signed.security_token {
                request = request.header("x-amz-security-token", token);
            }
            for (name, value) in extra_headers {
                request = request.header(*name, value);
            }
            if let Some(body) = &body {
                request = request.body(body.clone());
            }
            let outcome = match request.send().await {
                Ok(response) => classify(response).await,
                Err(error) => Err(S3Error::from(error)),
            };
            match outcome {
                Ok(response) => return Ok(response),
                Err(error) if attempt < self.config.max_retries && is_retryable(&error) => {
                    attempt += 1;
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
                Err(error) => return Err(error),
            }
        }
    }
}

fn total_bytes(response: &reqwest::Response) -> u64 {
    response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.rsplit('/').next())
        .and_then(|total| total.trim().parse().ok())
        .or_else(|| response.content_length())
        .unwrap_or_default()
}

fn content_length_header(response: &reqwest::Response) -> Option<u64> {
    response
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

async fn classify(response: reqwest::Response) -> Result<reqwest::Response, S3Error> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    if status == StatusCode::NOT_FOUND {
        return Err(S3Error::NotFound);
    }
    let status = status.as_u16();
    let body = read_body(response).await.unwrap_or_default();
    Err(S3Error::Unexpected {
        status,
        code: extract_tag(&body, "Code"),
        message: extract_tag(&body, "Message").unwrap_or_default(),
    })
}

async fn read_body(response: reqwest::Response) -> Result<String, S3Error> {
    response.text().await.map_err(S3Error::from)
}

const fn is_retryable(error: &S3Error) -> bool {
    match error {
        S3Error::Transport(_) => true,
        S3Error::Unexpected { status, .. } => *status == 429 || *status >= 500,
        S3Error::NotFound => false,
    }
}

fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(unescape_xml(&xml[start..end]))
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn unescape_xml(value: &str) -> String {
    value
        .replace("&quot;", "\"")
        .replace("&gt;", ">")
        .replace("&lt;", "<")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::{S3Client, S3Error, extract_tag};
    use crate::blob::{S3Config, S3Credentials, S3Settings};

    #[test]
    fn test_s3_error_display_covers_every_variant() {
        assert_eq!(S3Error::NotFound.to_string(), "object not found");
        assert_eq!(
            S3Error::Unexpected {
                status: 403,
                code: Some("AccessDenied".to_owned()),
                message: "no".to_owned(),
            }
            .to_string(),
            "s3 responded 403 (AccessDenied): no"
        );
        assert_eq!(
            S3Error::Unexpected {
                status: 500,
                code: None,
                message: "boom".to_owned(),
            }
            .to_string(),
            "s3 responded 500: boom"
        );
        assert_eq!(
            S3Error::Transport("reset".to_owned()).to_string(),
            "s3 request failed: reset"
        );
    }

    fn client(endpoint: &str, path_style: bool) -> S3Client {
        let settings = S3Settings {
            endpoint: endpoint.to_owned(),
            bucket: "bucket".to_owned(),
            prefix: String::new(),
            region: "us-east-1".to_owned(),
            path_style,
            request_timeout: std::time::Duration::from_secs(5),
            max_retries: 0,
            multipart_threshold: 8,
            part_size: 8,
            upload_concurrency: 1,
        };
        S3Client::new(
            S3Config::new(settings).unwrap(),
            S3Credentials {
                access_key_id: "a".to_owned(),
                secret_access_key: "b".to_owned(),
                session_token: None,
            },
        )
    }

    #[test]
    fn test_path_style_addresses_the_bucket_in_the_path() {
        let client = client("https://s3.example.com", true);
        let (url, host, path) = client.object_url("sha256/key");
        assert_eq!(host, "s3.example.com");
        assert_eq!(path, "/bucket/sha256/key");
        assert_eq!(url.as_str(), "https://s3.example.com/bucket/sha256/key");
        assert_eq!(client.bucket_url().2, "/bucket");
    }

    #[test]
    fn test_virtual_host_moves_the_bucket_into_the_host() {
        let client = client("https://s3.example.com", false);
        let (url, host, path) = client.object_url("sha256/key");
        assert_eq!(host, "bucket.s3.example.com");
        assert_eq!(path, "/sha256/key");
        assert_eq!(url.host_str().unwrap(), "bucket.s3.example.com");
        assert_eq!(client.bucket_url().2, "/");
    }

    #[test]
    fn test_virtual_host_keeps_a_custom_port() {
        let client = client("https://s3.example.com:9000", false);
        assert_eq!(client.object_url("sha256/key").1, "bucket.s3.example.com:9000");
    }

    #[test]
    fn test_extract_tag_reads_the_first_match() {
        assert_eq!(extract_tag("<A>one</A><A>two</A>", "A").as_deref(), Some("one"));
        assert!(extract_tag("<A>x", "A").is_none());
        assert!(extract_tag("no tags", "A").is_none());
    }
}

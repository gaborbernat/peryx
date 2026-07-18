//! An S3-compatible [`BlobBackend`](super::BlobBackend).
//!
//! Streamed writes stage to a local temp file exactly like the filesystem backend, so the digest is
//! known before anything reaches S3 and readers can tail an in-progress stage. Commit uploads the
//! finished stage under its digest key — one `PUT` below the multipart threshold, bounded concurrent
//! parts above it — then drops the local stage. A failed multipart aborts its upload so the parts do
//! not linger. Objects are immutable and digest-keyed, so every request is safe to retry.

mod client;
mod config;
mod sign;

use std::io::{Read as _, Seek as _, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;

pub use self::client::{S3Client, S3Error};
use self::client::{S3Get, S3Part};
pub use self::config::{S3Addressing, S3Config, S3ConfigError, S3Credentials, S3Settings};
use super::store::BlobStore;
use super::{
    BlobBackend, BlobCapabilities, BlobDurability, BlobError, BlobLease, BlobMetadata, BlobOperation, BlobRead,
    BlobReadBody, BlobStaged, BlobSupport, BlobWrite, Digest,
};

/// The S3-compatible blob backend.
#[derive(Debug, Clone)]
pub struct S3Backend {
    client: S3Client,
    staging: BlobStore,
}

impl S3Backend {
    /// Build a backend for `config`, staging local writes and downloads under `staging_dir`.
    #[must_use]
    pub fn new(config: S3Config, credentials: S3Credentials, staging_dir: PathBuf) -> Self {
        Self {
            client: S3Client::new(config, credentials),
            staging: BlobStore::new(staging_dir),
        }
    }

    fn key_for(&self, digest: &Digest) -> String {
        self.client.config().key_for(digest.as_str())
    }

    async fn open_inner(&self, digest: &Digest, range: Option<Range<u64>>) -> Result<BlobRead, BlobError> {
        let key = self.key_for(digest);
        let total = if range.is_some() {
            self.client
                .head(&key)
                .await
                .map_err(|error| blob_error(error, Some(digest)))?
                .ok_or_else(|| BlobError::not_found(digest))?
                .bytes
        } else {
            0
        };
        if let Some(range) = &range
            && (range.start > range.end || range.end > total)
        {
            return Err(BlobError::invalid_range(range.start, range.end, total));
        }
        let response = self
            .client
            .get(&key, range.clone())
            .await
            .map_err(|error| blob_error(error, Some(digest)))?;
        let total = range.as_ref().map_or(response.total_bytes, |_| total);
        let range = range.unwrap_or(0..total);
        Ok(BlobRead::new(
            "s3",
            digest.clone(),
            BlobMetadata {
                bytes: total,
                modified: None,
            },
            range,
            BlobReadBody::Stream(stream_body(response)),
        ))
    }

    async fn head_inner(&self, digest: &Digest) -> Result<Option<BlobMetadata>, BlobError> {
        Ok(self
            .client
            .head(&self.key_for(digest))
            .await
            .map_err(|error| blob_error(error, Some(digest)))?
            .map(|head| BlobMetadata {
                bytes: head.bytes,
                modified: None,
            }))
    }

    async fn verify_inner(&self, digest: &Digest) -> Result<bool, BlobError> {
        let response = match self.client.get(&self.key_for(digest), None).await {
            Ok(response) => response,
            Err(S3Error::NotFound) => return Err(BlobError::not_found(digest)),
            Err(error) => return Err(blob_error(error, Some(digest))),
        };
        let mut hasher = Sha256::new();
        let mut body = stream_body(response);
        while let Some(chunk) = body.try_next().await? {
            hasher.update(&chunk);
        }
        Ok(hex(&hasher.finalize()) == digest.as_str())
    }

    async fn delete_inner(&self, digest: &Digest) -> Result<bool, BlobError> {
        let key = self.key_for(digest);
        let existed = self
            .client
            .head(&key)
            .await
            .map_err(|error| blob_error(error, Some(digest)))?
            .is_some();
        self.client
            .delete(&key)
            .await
            .map_err(|error| blob_error(error, Some(digest)))?;
        Ok(existed)
    }

    async fn materialize_inner(&self, digest: &Digest) -> Result<BlobLease, BlobError> {
        let response = match self.client.get(&self.key_for(digest), None).await {
            Ok(response) => response,
            Err(S3Error::NotFound) => return Err(BlobError::not_found(digest)),
            Err(error) => return Err(blob_error(error, Some(digest))),
        };
        let dir = self.staging.staging_dir();
        std::fs::create_dir_all(&dir).map_err(BlobError::from)?;
        let (file, temp_path) = tempfile::Builder::new()
            .prefix(".peryx-s3-")
            .tempfile_in(&dir)
            .map_err(BlobError::from)?
            .into_parts();
        let mut file = tokio::fs::File::from_std(file);
        let mut body = stream_body(response);
        while let Some(chunk) = body.try_next().await? {
            file.write_all(&chunk).await.map_err(BlobError::from)?;
        }
        file.flush().await.map_err(BlobError::from)?;
        Ok(BlobLease::downloaded(temp_path))
    }

    async fn upload(&self, staged: &BlobStaged) -> Result<(), BlobError> {
        let digest = staged.digest().clone();
        let len = staged.len();
        let path = staged.with_materialized(Path::to_path_buf);
        let key = self.key_for(&digest);
        let result = if len <= self.client.config().multipart_threshold {
            self.put_whole(&key, &digest, len, &path).await
        } else {
            self.put_multipart(&key, len, &path).await
        };
        result
            .map_err(|error| blob_error(error, Some(&digest)).with_context("s3", BlobOperation::Commit, Some(&digest)))
    }

    async fn put_whole(&self, key: &str, digest: &Digest, len: u64, path: &Path) -> Result<(), S3Error> {
        let body = read_chunk(path.to_owned(), 0, len).await?;
        self.client
            .put(key, body, digest.as_str(), &sign_checksum(digest))
            .await
    }

    async fn put_multipart(&self, key: &str, len: u64, path: &Path) -> Result<(), S3Error> {
        let upload_id = self.client.create_multipart(key).await?;
        match self.upload_parts(key, &upload_id, len, path).await {
            Ok(parts) => self.client.complete_multipart(key, &upload_id, &parts).await,
            Err(error) => {
                let _ = self.client.abort_multipart(key, &upload_id).await;
                Err(error)
            }
        }
    }

    async fn upload_parts(&self, key: &str, upload_id: &str, len: u64, path: &Path) -> Result<Vec<S3Part>, S3Error> {
        let config = self.client.config();
        let mut parts = Vec::new();
        let mut number = 1u32;
        let mut offset = 0u64;
        while offset < len {
            let mut batch = Vec::new();
            while offset < len && batch.len() < config.upload_concurrency {
                let this = config.part_size.min(len - offset);
                batch.push(self.upload_one(key, upload_id, number, path.to_owned(), offset, this));
                number += 1;
                offset += this;
            }
            parts.extend(futures_util::future::try_join_all(batch).await?);
        }
        Ok(parts)
    }

    async fn upload_one(
        &self,
        key: &str,
        upload_id: &str,
        number: u32,
        path: PathBuf,
        offset: u64,
        len: u64,
    ) -> Result<S3Part, S3Error> {
        let body = read_chunk(path, offset, len).await?;
        let hash = sign::sha256_hex(&body);
        self.client.upload_part(key, upload_id, number, body, &hash).await
    }
}

async fn read_chunk(path: PathBuf, offset: u64, len: u64) -> Result<Bytes, S3Error> {
    tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::open(&path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buffer = vec![0u8; usize::try_from(len).unwrap_or(usize::MAX)];
        file.read_exact(&mut buffer)?;
        Ok(Bytes::from(buffer))
    })
    .await
    .expect("blob part read task never panics")
    .map_err(|error: std::io::Error| S3Error::Transport(error.to_string()))
}

fn sign_checksum(digest: &Digest) -> String {
    sign::sha256_base64(&hex_to_bytes(digest.as_str()))
}

fn hex_to_bytes(hex: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (index, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        out[index] = nibble(chunk[0]) << 4 | nibble(chunk[1]);
    }
    out
}

const fn nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        _ => byte - b'a' + 10,
    }
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

/// Adapt a streamed S3 body into a blob-error stream. A mid-transfer error is always transport, so
/// the conversion needs no digest and reuses the shared `From` impl; the operation's digest context
/// is attached by the caller. Sharing one non-capturing mapper keeps open, verify, and materialize
/// from each emitting a distinct stream-error function.
fn stream_body(response: S3Get) -> BoxStream<'static, Result<Bytes, BlobError>> {
    response.body.map_err(BlobError::from).boxed()
}

impl From<S3Error> for BlobError {
    fn from(error: S3Error) -> Self {
        blob_error(error, None)
    }
}

/// Map an S3 client error to a blob error, attaching the digest for a not-found.
fn blob_error(error: S3Error, digest: Option<&Digest>) -> BlobError {
    match error {
        S3Error::NotFound => digest.map_or_else(
            || BlobError::io(std::io::Error::from(std::io::ErrorKind::NotFound)),
            BlobError::not_found,
        ),
        other => BlobError::io(std::io::Error::other(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::{BlobError, S3Error};
    use crate::blob::BlobErrorKind;

    #[test]
    fn test_blob_error_from_s3_error() {
        assert_eq!(BlobError::from(S3Error::NotFound).kind(), BlobErrorKind::Io);
        assert_eq!(
            BlobError::from(S3Error::Transport("reset".to_owned())).kind(),
            BlobErrorKind::Io
        );
    }
}

impl BlobBackend for S3Backend {
    fn capabilities(&self) -> BlobCapabilities {
        BlobCapabilities {
            durability: BlobDurability::ObjectStore,
            create_if_absent: BlobSupport::Emulated,
            range: BlobSupport::Native,
            checksum: BlobSupport::Native,
            delete: BlobSupport::Native,
            list: BlobSupport::Unsupported,
            local_tail: BlobSupport::Native,
        }
    }

    async fn health(&self) -> Result<(), BlobError> {
        self.client
            .health()
            .await
            .map_err(|error| blob_error(error, None).with_context("s3", BlobOperation::Health, None))
    }

    async fn open(&self, digest: Digest, range: Option<Range<u64>>) -> Result<BlobRead, BlobError> {
        self.open_inner(&digest, range)
            .await
            .map_err(|error| error.with_context("s3", BlobOperation::Open, Some(&digest)))
    }

    async fn head(&self, digest: Digest) -> Result<Option<BlobMetadata>, BlobError> {
        self.head_inner(&digest)
            .await
            .map_err(|error| error.with_context("s3", BlobOperation::Head, Some(&digest)))
    }

    async fn begin(&self) -> Result<BlobWrite, BlobError> {
        let inner = BlobWrite::filesystem(self.staging.clone())
            .map_err(|error| error.with_context("s3", BlobOperation::Write, None))?;
        Ok(BlobWrite::s3(S3Write {
            inner: Box::new(inner),
            backend: self.clone(),
        }))
    }

    async fn verify(&self, digest: Digest) -> Result<bool, BlobError> {
        self.verify_inner(&digest)
            .await
            .map_err(|error| error.with_context("s3", BlobOperation::Verify, Some(&digest)))
    }

    async fn delete(&self, digest: Digest) -> Result<bool, BlobError> {
        self.delete_inner(&digest)
            .await
            .map_err(|error| error.with_context("s3", BlobOperation::Delete, Some(&digest)))
    }

    async fn materialize(&self, digest: Digest) -> Result<BlobLease, BlobError> {
        self.materialize_inner(&digest)
            .await
            .map_err(|error| error.with_context("s3", BlobOperation::Materialize, Some(&digest)))
    }
}

/// A streamed S3 write: a local filesystem stage whose commit uploads to S3 instead of publishing
/// into a content tree.
pub struct S3Write {
    inner: Box<BlobWrite>,
    backend: S3Backend,
}

impl S3Write {
    pub(crate) async fn write_chunk(&mut self, chunk: Bytes) -> Result<(), BlobError> {
        // The inner write is always a filesystem stage, but the shared `BlobWrite` type is recursive
        // through this wrapper, so its future is boxed to keep the size finite.
        Box::pin(self.inner.write_chunk(chunk)).await
    }

    pub(crate) async fn flush(&mut self) -> Result<u64, BlobError> {
        Box::pin(self.inner.flush()).await
    }

    pub(crate) fn tail(&self) -> Option<super::BlobTail> {
        self.inner.tail()
    }

    pub(crate) async fn finish(self) -> Result<BlobStaged, BlobError> {
        Ok(BlobStaged::s3(S3Staged {
            inner: Box::new(Box::pin(self.inner.finish()).await?),
            backend: self.backend,
        }))
    }

    pub(crate) async fn commit(self, expected: &Digest) -> Result<(), BlobError> {
        self.finish().await?.commit_as(expected).await
    }

    pub(crate) async fn abort(self) -> Result<(), BlobError> {
        Box::pin(self.inner.abort()).await
    }
}

/// A finished S3 stage: a local temp file uploaded to S3 on commit and discarded either way.
#[derive(Debug)]
pub struct S3Staged {
    inner: Box<BlobStaged>,
    backend: S3Backend,
}

impl S3Staged {
    pub(crate) const fn digest(&self) -> &Digest {
        self.inner.digest()
    }

    pub(crate) const fn len(&self) -> u64 {
        self.inner.len()
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// The finished local stage. Non-generic on purpose: routing `with_materialized` through a
    /// generic S3 method would drag a dead monomorphization into every crate that inspects a
    /// filesystem stage, which the `x86_64` coverage gate counts as uncovered.
    pub(crate) const fn inner(&self) -> &BlobStaged {
        &self.inner
    }

    pub(crate) async fn commit(self) -> Result<(), BlobError> {
        self.backend.upload(&self.inner).await?;
        Box::pin(self.inner.abort()).await
    }

    pub(crate) async fn abort(self) -> Result<(), BlobError> {
        Box::pin(self.inner.abort()).await
    }
}

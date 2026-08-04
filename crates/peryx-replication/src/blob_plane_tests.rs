use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};

use async_trait::async_trait;
use bytes::Bytes;
use peryx_storage::blob::{BlobStorage, Digest};
use peryx_storage::meta::MetaStore;

use crate::blob::{BlobRequest, BlobTransport, LoopbackBlobSource};
use crate::blob_plane::{BlobPlaneReport, pull_referenced};
use crate::error::SyncError;
use crate::peer::{TransferLimits, TransportError};

fn stores() -> (tempfile::TempDir, MetaStore, BlobStorage) {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    let blobs = BlobStorage::filesystem(dir.path().join("blobs"));
    (dir, meta, blobs)
}

fn limits() -> TransferLimits {
    TransferLimits {
        max_operations: NonZeroUsize::new(256).unwrap(),
        max_encoded_bytes: NonZeroU64::new(1 << 20).unwrap(),
    }
}

fn loopback(digest: &Digest, bytes: &'static [u8]) -> LoopbackBlobSource {
    LoopbackBlobSource::new(HashMap::from([(digest.clone(), Bytes::from_static(bytes))]), limits())
}

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

async fn seed_local(blobs: &BlobStorage, digest: &Digest, bytes: &'static [u8]) {
    let mut write = blobs.begin().await.unwrap();
    write.write_chunk(Bytes::from_static(bytes)).await.unwrap();
    write.commit(digest).await.unwrap();
}

struct Faulty(TransportError);

#[async_trait]
impl BlobTransport for Faulty {
    async fn fetch_blob(&self, _request: BlobRequest) -> Result<Vec<u8>, TransportError> {
        Err(self.0.clone())
    }
}

#[tokio::test]
async fn test_pull_referenced_fetches_absent_blobs_and_marks_them_local() {
    let (_dir, meta, blobs) = stores();
    let bytes = b"artifact";
    let digest = Digest::of(bytes);
    let source = loopback(&digest, bytes);

    let report = pull_referenced(&source, &blobs, &meta, &[(digest.clone(), bytes.len() as u64)], nz(2))
        .await
        .unwrap();

    assert_eq!(report, BlobPlaneReport { fetched: 1, pending: 0 });
    assert!(blobs.head(&digest).await.unwrap().is_some());
    assert!(blobs.verify(&digest).await.unwrap());
    let placement = meta.get_artifact_placement(digest.as_str()).unwrap().unwrap();
    assert!(placement.availability.is_local());
}

#[tokio::test]
async fn test_pull_referenced_skips_a_present_blob() {
    let (_dir, meta, blobs) = stores();
    let bytes = b"present";
    let digest = Digest::of(bytes);
    seed_local(&blobs, &digest, bytes).await;
    // A transport that errors if asked, proving a present blob is never fetched.
    let source = Faulty(TransportError::BlobNotFound {
        digest: digest.as_str().to_owned(),
    });

    let report = pull_referenced(&source, &blobs, &meta, &[(digest.clone(), bytes.len() as u64)], nz(2))
        .await
        .unwrap();

    assert_eq!(report, BlobPlaneReport { fetched: 0, pending: 0 });
}

#[tokio::test]
async fn test_pull_referenced_leaves_a_backpressured_blob_pending() {
    let (_dir, meta, blobs) = stores();
    let digest = Digest::of(b"busy");
    let source = Faulty(TransportError::AtCapacity);

    let report = pull_referenced(&source, &blobs, &meta, &[(digest, 4)], nz(2))
        .await
        .unwrap();

    assert_eq!(report, BlobPlaneReport { fetched: 0, pending: 1 });
}

#[tokio::test]
async fn test_pull_referenced_surfaces_a_terminal_fetch_failure() {
    let (_dir, meta, blobs) = stores();
    let digest = Digest::of(b"gone");
    let source = Faulty(TransportError::BlobNotFound {
        digest: digest.as_str().to_owned(),
    });

    let error = pull_referenced(&source, &blobs, &meta, &[(digest.clone(), 4)], nz(2))
        .await
        .unwrap_err();

    let SyncError::BlobFetchFailed { reason, digest: failed } = error else {
        panic!("expected a terminal blob-fetch failure, got {error:?}");
    };
    assert_eq!(reason, "blob_not_found");
    assert_eq!(failed, digest.as_str());
}

#[tokio::test]
async fn test_pull_referenced_fails_closed_on_a_wrong_sized_blob() {
    let (_dir, meta, blobs) = stores();
    let bytes = b"artifact";
    let digest = Digest::of(bytes);
    let source = loopback(&digest, bytes);

    // The reference declares a size the fetched blob does not match.
    let error = pull_referenced(&source, &blobs, &meta, &[(digest.clone(), 999)], nz(2))
        .await
        .unwrap_err();

    let SyncError::BlobSizeMismatch {
        digest: mismatched,
        expected,
        actual,
    } = error
    else {
        panic!("expected a size mismatch, got {error:?}");
    };
    assert_eq!(mismatched, digest.as_str());
    assert_eq!(expected, 999);
    assert_eq!(actual, bytes.len() as u64);
    // The wrong-sized blob is not left present.
    assert!(blobs.head(&digest).await.unwrap().is_none());
}

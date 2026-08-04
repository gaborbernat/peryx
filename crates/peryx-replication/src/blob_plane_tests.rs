use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};

use async_trait::async_trait;
use bytes::Bytes;
use peryx_storage::blob::{BlobStorage, Digest};
use peryx_storage::meta::MetaStore;

use crate::blob::{BlobRequest, BlobTransport, LoopbackBlobSource};
use crate::blob_plane::{BLOB_VIEW, BlobPlaneReport, advance_blob_frontier, pull_outstanding, pull_referenced};
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

fn seed_serial(meta: &MetaStore, after: u64, blobs: &[(&str, u64)]) {
    meta.commit_replica_txn(after, |txn| {
        for (sha256, size) in blobs {
            txn.reference_blob(sha256, *size);
        }
        Ok::<_, SyncError>(((), vec![format!("event-{}", after + 1).into_bytes()]))
    })
    .unwrap();
}

#[tokio::test]
async fn test_advance_holds_below_an_absent_blob_then_moves_past_it_once_present() {
    let (_dir, meta, blobs) = stores();
    let one = Digest::of(b"blob-one");
    let two = Digest::of(b"blob-two");
    seed_serial(&meta, 0, &[(one.as_str(), 8)]);
    seed_serial(&meta, 1, &[(two.as_str(), 8)]);
    seed_local(&blobs, &one, b"blob-one").await;

    // `two` is absent, so the frontier holds at serial 1.
    assert_eq!(advance_blob_frontier(&meta, &blobs, nz(10)).await.unwrap(), 1);
    assert_eq!(meta.view_frontier(BLOB_VIEW).unwrap(), Some(1));

    // Once `two`'s byte lands the next pass advances past it.
    seed_local(&blobs, &two, b"blob-two").await;
    assert_eq!(advance_blob_frontier(&meta, &blobs, nz(10)).await.unwrap(), 2);
    assert_eq!(meta.view_frontier(BLOB_VIEW).unwrap(), Some(2));

    // Re-deriving from the same durable journal and blob store is idempotent.
    assert_eq!(advance_blob_frontier(&meta, &blobs, nz(10)).await.unwrap(), 2);
    assert_eq!(meta.view_frontier(BLOB_VIEW).unwrap(), Some(2));
}

#[tokio::test]
async fn test_advance_is_bounded_by_the_batch() {
    let (_dir, meta, blobs) = stores();
    for (after, content) in [(0, b"one" as &[u8]), (1, b"two"), (2, b"three")] {
        let digest = Digest::of(content);
        seed_serial(&meta, after, &[(digest.as_str(), content.len() as u64)]);
        seed_local(&blobs, &digest, content).await;
    }

    // A batch of two examines serials 1 and 2 only, so the frontier reaches 2 this pass, not 3.
    assert_eq!(advance_blob_frontier(&meta, &blobs, nz(2)).await.unwrap(), 2);
    // The next pass carries it the rest of the way.
    assert_eq!(advance_blob_frontier(&meta, &blobs, nz(2)).await.unwrap(), 3);
}

#[tokio::test]
async fn test_advance_lets_a_serial_without_blobs_through() {
    let (_dir, meta, blobs) = stores();
    seed_serial(&meta, 0, &[]);

    assert_eq!(advance_blob_frontier(&meta, &blobs, nz(10)).await.unwrap(), 1);
    assert_eq!(meta.view_frontier(BLOB_VIEW).unwrap(), Some(1));
}

#[tokio::test]
async fn test_advance_fails_closed_on_an_unparseable_journal_digest() {
    let (_dir, meta, blobs) = stores();
    seed_serial(&meta, 0, &[("not-a-valid-sha256", 4)]);

    // The digest cannot be probed, so the blob counts as absent and the frontier holds below serial 1.
    assert_eq!(advance_blob_frontier(&meta, &blobs, nz(10)).await.unwrap(), 0);
    assert_eq!(meta.view_frontier(BLOB_VIEW).unwrap(), None);
}

#[tokio::test]
async fn test_pull_outstanding_retries_a_blob_from_the_journal_tail() {
    let (_dir, meta, blobs) = stores();
    let bytes = b"outstanding";
    let digest = Digest::of(bytes);
    // The blob is referenced by the journal, not by any current page: a self-healing pull finds it.
    seed_serial(&meta, 0, &[(digest.as_str(), bytes.len() as u64)]);
    let source = loopback(&digest, bytes);

    let report = pull_outstanding(&source, &meta, &blobs, nz(10), nz(2)).await.unwrap();

    assert_eq!(report, BlobPlaneReport { fetched: 1, pending: 0 });
    assert!(blobs.head(&digest).await.unwrap().is_some());
    let placement = meta.get_artifact_placement(digest.as_str()).unwrap().unwrap();
    assert!(placement.availability.is_local());
}

#[tokio::test]
async fn test_pull_outstanding_skips_a_present_tail_blob() {
    let (_dir, meta, blobs) = stores();
    let bytes = b"already-here";
    let digest = Digest::of(bytes);
    seed_serial(&meta, 0, &[(digest.as_str(), bytes.len() as u64)]);
    seed_local(&blobs, &digest, bytes).await;
    // A transport that errors if asked proves the present tail blob is not re-pulled.
    let source = Faulty(TransportError::BlobNotFound {
        digest: digest.as_str().to_owned(),
    });

    let report = pull_outstanding(&source, &meta, &blobs, nz(10), nz(2)).await.unwrap();

    assert_eq!(report, BlobPlaneReport { fetched: 0, pending: 0 });
}

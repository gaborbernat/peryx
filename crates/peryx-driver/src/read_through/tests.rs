use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use peryx_identity::ArtifactDigest;
use peryx_replication::{
    BlobRequest, BlobTransport, ByteRange, CapacityLimited, CircuitConfig, DEFAULT_RECONNECT_POLICY, ReconnectPolicy,
    TransportError,
};
use peryx_storage::blob::{BlobError, BlobStorage, Digest};
use peryx_storage::meta::{
    BackendId, BackendLocation, BlobPlacementFailure, BlobPlacementKey, BlobPlacementRecord, BlobPlacementState,
    BlobPlacementTransition, DataCenterId, MetaStore,
};

use super::{
    DEFAULT_READ_THROUGH_LIMITS, DcTransport, MonotonicClock, ReadThroughError, ReadThroughLimits, ReadThroughOutcome,
    RemotePlacementReader, fill_from_remote_placement, representative, verified_size,
};

/// How a peer mangles the bytes it serves, once it has stopped failing outright.
#[derive(Clone, Copy)]
enum Corruption {
    /// Serve the requested bytes verbatim.
    None,
    /// Serve the right number of bytes with the wrong content, so reassembly fails the digest check.
    Content,
    /// Serve one byte fewer than asked, so the piece adapter rejects the short range.
    Short,
}

/// An in-process [`BlobTransport`] a test drives without a socket: it fails its first `fail_first`
/// fetches with `error`, then serves ranges of `content`, optionally corrupted.
struct Peer {
    content: Bytes,
    fail_first: AtomicUsize,
    error: TransportError,
    corruption: Corruption,
}

#[async_trait]
impl BlobTransport for Peer {
    async fn fetch_blob(&self, request: BlobRequest) -> Result<Vec<u8>, TransportError> {
        if self
            .fail_first
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
            .is_ok()
        {
            return Err(self.error.clone());
        }
        let range = request.range.unwrap_or(ByteRange {
            offset: 0,
            length: self.content.len(),
        });
        let start = range.offset.min(self.content.len());
        let end = start.saturating_add(range.length).min(self.content.len());
        let mut bytes = self.content[start..end].to_vec();
        match self.corruption {
            Corruption::None => {}
            Corruption::Content => bytes.iter_mut().for_each(|byte| *byte ^= 0xFF),
            Corruption::Short => {
                bytes.pop();
            }
        }
        Ok(bytes)
    }
}

fn peer(content: Bytes, fail_first: usize, error: TransportError, corruption: Corruption) -> DcTransport {
    Arc::new(Peer {
        content,
        fail_first: AtomicUsize::new(fail_first),
        error,
        corruption,
    })
}

fn serving(content: &Bytes) -> DcTransport {
    peer(content.clone(), 0, TransportError::Disconnected, Corruption::None)
}

fn delegates<const N: usize>(pairs: [(&str, DcTransport); N]) -> HashMap<String, DcTransport> {
    pairs
        .into_iter()
        .map(|(dc, transport)| (dc.to_owned(), transport))
        .collect()
}

fn stores() -> (tempfile::TempDir, MetaStore, BlobStorage) {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    let blobs = BlobStorage::filesystem(dir.path().join("blobs"));
    (dir, meta, blobs)
}

fn key(digest: &Digest, dc: &str, backend: &str, location: &str) -> BlobPlacementKey {
    BlobPlacementKey {
        digest: ArtifactDigest::from_sha256(digest.as_str()).unwrap(),
        backend: BackendId::new(backend).unwrap(),
        data_center: DataCenterId::new(dc).unwrap(),
        location: BackendLocation::new(location).unwrap(),
    }
}

/// Seed one verified placement of `digest` (of `size` bytes) in data center `dc`.
fn seed_verified(meta: &MetaStore, digest: &Digest, dc: &str, backend: &str, location: &str, size: u64) {
    let key = key(digest, dc, backend, location);
    let artifact = ArtifactDigest::from_sha256(digest.as_str()).unwrap();
    meta.apply_blob_placement(&key, &BlobPlacementTransition::Stage, 1, 0)
        .unwrap();
    meta.apply_blob_placement(
        &key,
        &BlobPlacementTransition::Verify {
            observed: artifact,
            size,
        },
        1,
        0,
    )
    .unwrap();
}

fn frozen_clock(seconds: u64) -> (Arc<AtomicU64>, MonotonicClock) {
    let ticks = Arc::new(AtomicU64::new(seconds));
    let handle = Arc::clone(&ticks);
    let clock: MonotonicClock = Arc::new(move || i64::try_from(handle.load(Ordering::SeqCst)).unwrap_or(i64::MAX));
    (ticks, clock)
}

fn reader(local_dc: &str, delegates: HashMap<String, DcTransport>, limits: ReadThroughLimits) -> RemotePlacementReader {
    RemotePlacementReader::new(
        DataCenterId::new(local_dc).unwrap(),
        delegates,
        limits,
        frozen_clock(0).1,
    )
}

fn reader_with_clock(
    local_dc: &str,
    delegates: HashMap<String, DcTransport>,
    limits: ReadThroughLimits,
    clock: MonotonicClock,
) -> RemotePlacementReader {
    RemotePlacementReader::new(DataCenterId::new(local_dc).unwrap(), delegates, limits, clock)
}

fn small(bytes: usize) -> ReadThroughLimits {
    ReadThroughLimits {
        chunk_bytes: NonZeroUsize::new(bytes).unwrap(),
        ..DEFAULT_READ_THROUGH_LIMITS
    }
}

#[tokio::test]
async fn test_serves_verified_bytes_from_a_remote_placement() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"the release archive bytes");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    let reader = reader(
        "home",
        delegates([("east", serving(&content))]),
        DEFAULT_READ_THROUGH_LIMITS,
    );

    let outcome = reader.read_through(&meta, &blobs, &digest).await.unwrap();

    assert_eq!(outcome, ReadThroughOutcome::Served);
    let stored = blobs.open(&digest, None).await.unwrap();
    assert_eq!(stored.collect(u64::MAX).await.unwrap(), content);
}

#[tokio::test]
async fn test_serves_a_blob_drawn_across_several_ranges() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"0123456789abcdef");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    let reader = reader("home", delegates([("east", serving(&content))]), small(4));

    let outcome = reader.read_through(&meta, &blobs, &digest).await.unwrap();

    assert_eq!(outcome, ReadThroughOutcome::Served);
    assert!(blobs.head(&digest).await.unwrap().is_some());
}

#[tokio::test]
async fn test_no_placement_is_unavailable() {
    let (_dir, meta, blobs) = stores();
    let digest = Digest::of(b"never placed anywhere");
    let reader = reader("home", delegates([]), DEFAULT_READ_THROUGH_LIMITS);

    let outcome = reader.read_through(&meta, &blobs, &digest).await.unwrap();

    assert_eq!(outcome, ReadThroughOutcome::Unavailable);
    assert!(blobs.head(&digest).await.unwrap().is_none());
}

#[tokio::test]
async fn test_placement_in_a_data_center_without_a_delegate_is_unavailable() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"held only in an unreachable dc");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "west", "filesystem", "west/a", content.len() as u64);
    let reader = reader(
        "home",
        delegates([("east", serving(&content))]),
        DEFAULT_READ_THROUGH_LIMITS,
    );

    let outcome = reader.read_through(&meta, &blobs, &digest).await.unwrap();

    assert_eq!(outcome, ReadThroughOutcome::Unavailable);
    assert!(blobs.head(&digest).await.unwrap().is_none());
}

#[tokio::test]
async fn test_corrupt_source_commits_no_local_content() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"the bytes the catalog names");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    let corrupt = peer(content.clone(), 0, TransportError::Disconnected, Corruption::Content);
    let reader = reader("home", delegates([("east", corrupt)]), DEFAULT_READ_THROUGH_LIMITS);

    let outcome = reader.read_through(&meta, &blobs, &digest).await.unwrap();

    assert_eq!(outcome, ReadThroughOutcome::Unavailable);
    assert!(blobs.head(&digest).await.unwrap().is_none());
}

#[tokio::test]
async fn test_short_range_source_commits_no_local_content() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"a source that under-delivers a range");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    let short = peer(content.clone(), 0, TransportError::Disconnected, Corruption::Short);
    let reader = reader("home", delegates([("east", short)]), DEFAULT_READ_THROUGH_LIMITS);

    let outcome = reader.read_through(&meta, &blobs, &digest).await.unwrap();

    assert_eq!(outcome, ReadThroughOutcome::Unavailable);
    assert!(blobs.head(&digest).await.unwrap().is_none());
}

#[tokio::test]
async fn test_falls_through_to_a_second_source_when_the_first_loses() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"served by the standby peer");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    seed_verified(&meta, &digest, "west", "filesystem", "west/a", content.len() as u64);
    let down = peer(
        content.clone(),
        usize::MAX,
        TransportError::Disconnected,
        Corruption::None,
    );
    let reader = reader(
        "home",
        delegates([("east", down), ("west", serving(&content))]),
        DEFAULT_READ_THROUGH_LIMITS,
    );

    let outcome = reader.read_through(&meta, &blobs, &digest).await.unwrap();

    assert_eq!(outcome, ReadThroughOutcome::Served);
    assert!(blobs.head(&digest).await.unwrap().is_some());
}

#[tokio::test]
async fn test_terminal_source_gives_up_without_content() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"the peer denies holding it");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    let missing = peer(
        content.clone(),
        usize::MAX,
        TransportError::BlobNotFound {
            digest: digest.as_str().to_owned(),
        },
        Corruption::None,
    );
    let reader = reader("home", delegates([("east", missing)]), DEFAULT_READ_THROUGH_LIMITS);

    let outcome = reader.read_through(&meta, &blobs, &digest).await.unwrap();

    assert_eq!(outcome, ReadThroughOutcome::Unavailable);
    assert!(blobs.head(&digest).await.unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn test_retries_a_transient_failure_then_serves() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"lands on the second attempt");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    let flaky = peer(content.clone(), 1, TransportError::Timeout, Corruption::None);
    let limits = ReadThroughLimits {
        circuit: CircuitConfig {
            trip_after: 5,
            cooldown: Duration::from_secs(30),
        },
        ..DEFAULT_READ_THROUGH_LIMITS
    };
    let reader = reader("home", delegates([("east", flaky)]), limits);

    let outcome = reader.read_through(&meta, &blobs, &digest).await.unwrap();

    assert_eq!(outcome, ReadThroughOutcome::Served);
    assert!(blobs.head(&digest).await.unwrap().is_some());
}

#[tokio::test]
async fn test_open_circuit_skips_a_source_then_recovers_after_cooldown() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"one loss trips it, cooldown clears it");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    let flaky = peer(content.clone(), 1, TransportError::Timeout, Corruption::None);
    let limits = ReadThroughLimits {
        circuit: CircuitConfig {
            trip_after: 1,
            cooldown: Duration::from_secs(45),
        },
        policy: ReconnectPolicy::new(
            Duration::from_millis(1),
            std::num::NonZeroU32::new(2).unwrap(),
            Duration::from_millis(1),
            std::num::NonZeroU32::new(1).unwrap(),
        ),
        ..DEFAULT_READ_THROUGH_LIMITS
    };
    let (ticks, clock) = frozen_clock(0);
    let reader = reader_with_clock("home", delegates([("east", flaky)]), limits, clock);

    let tripped = reader.read_through(&meta, &blobs, &digest).await.unwrap();
    assert_eq!(tripped, ReadThroughOutcome::Unavailable);

    let skipped = reader.read_through(&meta, &blobs, &digest).await.unwrap();
    assert_eq!(skipped, ReadThroughOutcome::Unavailable);
    assert!(blobs.head(&digest).await.unwrap().is_none());

    ticks.store(61, Ordering::SeqCst);
    let recovered = reader.read_through(&meta, &blobs, &digest).await.unwrap();
    assert_eq!(recovered, ReadThroughOutcome::Served);
    assert!(blobs.head(&digest).await.unwrap().is_some());
}

#[tokio::test]
async fn test_fan_out_caps_the_sources_tried() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"the second peer is never reached");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    seed_verified(&meta, &digest, "west", "filesystem", "west/a", content.len() as u64);
    let down = peer(
        content.clone(),
        usize::MAX,
        TransportError::BlobNotFound {
            digest: digest.as_str().to_owned(),
        },
        Corruption::None,
    );
    let limits = ReadThroughLimits {
        max_fanout: NonZeroUsize::new(1).unwrap(),
        ..DEFAULT_READ_THROUGH_LIMITS
    };
    let reader = reader("home", delegates([("east", down), ("west", serving(&content))]), limits);

    let outcome = reader.read_through(&meta, &blobs, &digest).await.unwrap();

    assert_eq!(outcome, ReadThroughOutcome::Unavailable);
    assert!(blobs.head(&digest).await.unwrap().is_none());
}

#[tokio::test]
async fn test_one_source_per_data_center_even_with_several_placements() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"two backends, one datacenter");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    seed_verified(&meta, &digest, "east", "s3", "east/b", content.len() as u64);
    let reader = reader(
        "home",
        delegates([("east", serving(&content))]),
        DEFAULT_READ_THROUGH_LIMITS,
    );

    let outcome = reader.read_through(&meta, &blobs, &digest).await.unwrap();

    assert_eq!(outcome, ReadThroughOutcome::Served);
}

#[tokio::test]
async fn test_serves_through_a_capacity_limited_delegate() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"bounded concurrency still serves");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    let bounded: DcTransport = Arc::new(CapacityLimited::new(
        Peer {
            content: content.clone(),
            fail_first: AtomicUsize::new(0),
            error: TransportError::Disconnected,
            corruption: Corruption::None,
        },
        NonZeroUsize::new(1).unwrap(),
    ));
    let reader = reader("home", delegates([("east", bounded)]), DEFAULT_READ_THROUGH_LIMITS);

    let outcome = reader.read_through(&meta, &blobs, &digest).await.unwrap();

    assert_eq!(outcome, ReadThroughOutcome::Served);
}

#[tokio::test]
async fn test_fill_without_a_reader_is_a_no_op() {
    let (_dir, meta, blobs) = stores();
    let digest = Digest::of(b"no reader installed");

    assert!(fill_from_remote_placement(None, &meta, &blobs, &digest).await.is_none());
    assert!(blobs.head(&digest).await.unwrap().is_none());
}

#[tokio::test]
async fn test_fill_returns_stored_metadata_on_a_served_placement() {
    let (_dir, meta, blobs) = stores();
    let content = Bytes::from_static(b"filled from a peer");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    let reader = reader(
        "home",
        delegates([("east", serving(&content))]),
        DEFAULT_READ_THROUGH_LIMITS,
    );

    let metadata = fill_from_remote_placement(Some(&reader), &meta, &blobs, &digest)
        .await
        .unwrap();

    assert_eq!(metadata.bytes, content.len() as u64);
}

#[tokio::test]
async fn test_fill_returns_none_when_no_source_can_serve() {
    let (_dir, meta, blobs) = stores();
    let digest = Digest::of(b"unplaced");
    let reader = reader("home", delegates([]), DEFAULT_READ_THROUGH_LIMITS);

    assert!(
        fill_from_remote_placement(Some(&reader), &meta, &blobs, &digest)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn test_fill_falls_through_on_a_local_staging_fault() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    let occupied = dir.path().join("occupied");
    std::fs::write(&occupied, b"not a directory").unwrap();
    let blobs = BlobStorage::filesystem(occupied.join("blobs"));
    let content = Bytes::from_static(b"verified but cannot be staged");
    let digest = Digest::of(&content);
    seed_verified(&meta, &digest, "east", "filesystem", "east/a", content.len() as u64);
    let reader = reader(
        "home",
        delegates([("east", serving(&content))]),
        DEFAULT_READ_THROUGH_LIMITS,
    );

    let err = reader.read_through(&meta, &blobs, &digest).await.unwrap_err();

    assert!(matches!(err, ReadThroughError::Blob(_)));
    assert!(
        fill_from_remote_placement(Some(&reader), &meta, &blobs, &digest)
            .await
            .is_none()
    );
}

#[test]
fn test_verified_size_reads_only_a_verified_placement() {
    let digest = Digest::of(b"sized");
    let record = |state| BlobPlacementRecord {
        key: key(&digest, "east", "filesystem", "east/a"),
        state,
        fence: 1,
        generation: 2,
        updated_at_unix: 0,
    };
    assert_eq!(
        verified_size(&record(BlobPlacementState::Verified { size: 42 })),
        Some(42)
    );
    assert_eq!(
        verified_size(&record(BlobPlacementState::Failed {
            class: BlobPlacementFailure::SourceUnavailable,
        })),
        None
    );
}

#[test]
fn test_representative_prefers_a_retryable_failure() {
    let terminal = (0usize, TransportError::BlobNotFound { digest: "d".to_owned() });
    let retryable = (1usize, TransportError::Timeout);
    assert_eq!(representative(&[terminal.clone(), retryable]), &TransportError::Timeout);
    assert_eq!(representative(std::slice::from_ref(&terminal)), &terminal.1);
}

#[test]
fn test_read_through_errors_render() {
    let meta = ReadThroughError::Meta(MetaStore::open_existing("/nonexistent/read-through.redb").unwrap_err());
    let blob = ReadThroughError::Blob(BlobError::not_found(&Digest::of(b"x")));
    assert!(meta.to_string().contains("placements"));
    assert!(blob.to_string().contains("stage"));
}

#[test]
fn test_default_limits_match_the_constant() {
    assert_eq!(ReadThroughLimits::default(), DEFAULT_READ_THROUGH_LIMITS);
    assert_eq!(DEFAULT_READ_THROUGH_LIMITS.policy, DEFAULT_RECONNECT_POLICY);
}

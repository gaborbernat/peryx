use peryx_ha::{ArtifactPlacement, ArtifactSource, ByteAvailability, ReclaimGuard, ReclaimGuardStore as _};
use rstest::rstest;

use crate::blob::BlobStorage;
use crate::meta::MetaStore;
use crate::repair_artifact_placements;
use crate::{ArtifactRepairError, ArtifactRepairFailure, repair_artifact_placements_cancellable};

fn stores() -> (tempfile::TempDir, MetaStore, BlobStorage) {
    let directory = tempfile::tempdir().unwrap();
    let meta = MetaStore::open(directory.path().join("peryx.redb")).unwrap();
    let blobs = BlobStorage::filesystem(directory.path().join("blobs"));
    (directory, meta, blobs)
}

#[tokio::test]
async fn repair_discovers_unprojected_content_as_unknown() {
    let (_directory, meta, blobs) = stores();
    let digest = blobs.put_bytes(b"unprojected").await.unwrap();

    let report = repair_artifact_placements(&meta, &blobs, 1).await.unwrap();

    assert_eq!(report.content.changed, 1);
    assert_eq!(
        meta.get_artifact_placement(digest.as_str()).unwrap(),
        Some(ArtifactPlacement::record(ArtifactSource::Unknown, true))
    );
}

#[tokio::test]
async fn repair_resumes_the_content_scan() {
    let (_directory, meta, blobs) = stores();
    let first = blobs.put_bytes(b"first").await.unwrap();
    let second = blobs.put_bytes(b"second").await.unwrap();

    let first_report = repair_artifact_placements(&meta, &blobs, 1).await.unwrap();
    let second_report = repair_artifact_placements(&meta, &blobs, 1).await.unwrap();

    assert_eq!(first_report.content.scanned, 1);
    assert_eq!(second_report.content.scanned, 1);
    assert_eq!(
        meta.get_artifact_placement(first.as_str()).unwrap(),
        Some(ArtifactPlacement::record(ArtifactSource::Unknown, true))
    );
    assert_eq!(
        meta.get_artifact_placement(second.as_str()).unwrap(),
        Some(ArtifactPlacement::record(ArtifactSource::Unknown, true))
    );
}

#[tokio::test]
async fn repair_resets_content_progress_when_the_backend_changes() {
    let directory = tempfile::tempdir().unwrap();
    let meta = MetaStore::open(directory.path().join("peryx.redb")).unwrap();
    let first_blobs = BlobStorage::filesystem(directory.path().join("first"));
    first_blobs.put_bytes(b"first").await.unwrap();
    first_blobs.put_bytes(b"second").await.unwrap();
    assert!(
        !repair_artifact_placements(&meta, &first_blobs, 1)
            .await
            .unwrap()
            .content
            .eof
    );
    let second_blobs = BlobStorage::filesystem(directory.path().join("second"));
    let digest = second_blobs.put_bytes(b"replacement").await.unwrap();

    let report = repair_artifact_placements(&meta, &second_blobs, 1).await.unwrap();

    assert_eq!(report.content.changed, 1);
    assert_eq!(
        meta.get_artifact_placement(digest.as_str()).unwrap(),
        Some(ArtifactPlacement::record(ArtifactSource::Unknown, true))
    );
}

#[tokio::test]
async fn repair_restarts_a_content_cycle_after_eof() {
    let (_directory, meta, blobs) = stores();
    blobs.put_bytes(b"only").await.unwrap();

    assert!(repair_artifact_placements(&meta, &blobs, 1).await.unwrap().content.eof);
    let report = repair_artifact_placements(&meta, &blobs, 1).await.unwrap();

    assert_eq!(report.content.scanned, 1);
    assert_eq!(report.content.changed, 0);
}

#[tokio::test]
async fn repair_serializes_concurrent_direct_calls() {
    let (_directory, meta, blobs) = stores();
    let first = blobs.put_bytes(b"first").await.unwrap();
    let second = blobs.put_bytes(b"second").await.unwrap();

    let (first_report, second_report) = tokio::join!(
        repair_artifact_placements(&meta, &blobs, 1),
        repair_artifact_placements(&meta, &blobs, 1),
    );

    assert_eq!(
        first_report.unwrap().content.changed + second_report.unwrap().content.changed,
        2
    );
    for digest in [first, second] {
        assert_eq!(
            meta.get_artifact_placement(digest.as_str()).unwrap(),
            Some(ArtifactPlacement::record(ArtifactSource::Unknown, true))
        );
    }
}

#[rstest]
#[case::zero(0)]
#[case::above_limit(peryx_ha::MAX_REPAIR_BATCH + 1)]
#[tokio::test]
async fn repair_rejects_invalid_batch_sizes(#[case] batch: usize) {
    let (_directory, meta, blobs) = stores();

    let error = repair_artifact_placements(&meta, &blobs, batch).await.unwrap_err();

    assert!(matches!(error, ArtifactRepairError::Batch));
}

#[test]
fn repair_failure_delegates_its_display_and_source() {
    let failure = ArtifactRepairFailure {
        report: crate::ArtifactRepairReport::default(),
        error: ArtifactRepairError::Batch,
    };

    assert_eq!(failure.to_string(), ArtifactRepairError::Batch.to_string());
    assert_eq!(
        std::error::Error::source(&failure).unwrap().to_string(),
        ArtifactRepairError::Batch.to_string()
    );
}

#[rstest]
#[case::hosted(ArtifactSource::Hosted, ByteAvailability::Unavailable)]
#[case::proxy(ArtifactSource::Proxy, ByteAvailability::RemoteOnly)]
#[case::generated(ArtifactSource::Generated, ByteAvailability::Unavailable)]
#[case::unknown(ArtifactSource::Unknown, ByteAvailability::Unavailable)]
#[tokio::test]
async fn repair_demotes_a_missing_local_placement(
    #[case] source: ArtifactSource,
    #[case] availability: ByteAvailability,
) {
    let (_directory, meta, blobs) = stores();
    let digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    meta.put_artifact_placement(digest, &ArtifactPlacement::record(source, true))
        .unwrap();

    let report = repair_artifact_placements(&meta, &blobs, 1).await.unwrap();

    assert_eq!(report.placement.changed, 1);
    assert_eq!(
        meta.get_artifact_placement(digest).unwrap(),
        Some(ArtifactPlacement { source, availability })
    );
}

#[tokio::test]
async fn repair_resumes_the_placement_scan() {
    let (_directory, meta, blobs) = stores();
    let digests = ["a".repeat(64), "b".repeat(64)];
    for digest in &digests {
        meta.put_artifact_placement(digest, &ArtifactPlacement::record(ArtifactSource::Hosted, true))
            .unwrap();
    }

    let first = repair_artifact_placements(&meta, &blobs, 1).await.unwrap();
    let second = repair_artifact_placements(&meta, &blobs, 1).await.unwrap();

    assert_eq!(first.placement.changed, 1);
    assert_eq!(second.placement.changed, 1);
    for digest in digests {
        assert_eq!(
            meta.get_artifact_placement(&digest).unwrap(),
            Some(ArtifactPlacement::record(ArtifactSource::Hosted, false))
        );
    }
}

#[tokio::test]
async fn repair_preserves_a_known_source_when_restoring_local_availability() {
    let (_directory, meta, blobs) = stores();
    let digest = blobs.put_bytes(b"hosted").await.unwrap();
    meta.put_artifact_placement(
        digest.as_str(),
        &ArtifactPlacement::record(ArtifactSource::Hosted, false),
    )
    .unwrap();

    let report = repair_artifact_placements(&meta, &blobs, 1).await.unwrap();

    assert_eq!(report.content.changed, 1);
    assert_eq!(
        meta.get_artifact_placement(digest.as_str()).unwrap(),
        Some(ArtifactPlacement::record(ArtifactSource::Hosted, true))
    );
}

#[tokio::test]
async fn repair_does_not_rewrite_a_correct_placement() {
    let (_directory, meta, blobs) = stores();
    let digest = blobs.put_bytes(b"hosted").await.unwrap();
    meta.put_artifact_placement(
        digest.as_str(),
        &ArtifactPlacement::record(ArtifactSource::Hosted, true),
    )
    .unwrap();

    let report = repair_artifact_placements(&meta, &blobs, 1).await.unwrap();

    assert_eq!(report.content.changed + report.placement.changed, 0);
}

#[tokio::test]
async fn repair_rejects_an_invalid_placement_digest_without_demoting_it() {
    let (_directory, meta, blobs) = stores();
    meta.put_artifact_placement("invalid", &ArtifactPlacement::record(ArtifactSource::Hosted, true))
        .unwrap();

    let error = repair_artifact_placements(&meta, &blobs, 1).await.unwrap_err();

    assert!(matches!(error, ArtifactRepairError::InvalidPlacementDigest(digest) if digest == "invalid"));
    assert_eq!(
        meta.get_artifact_placement("invalid").unwrap(),
        Some(ArtifactPlacement::record(ArtifactSource::Hosted, true))
    );
}

#[tokio::test]
async fn repair_skips_content_held_by_a_reclaim_guard() {
    let (_directory, meta, blobs) = stores();
    let digest = blobs.put_bytes(b"guarded").await.unwrap();
    meta.compare_and_arm_reclaim_guards(&[digest.as_str()], 0, 0, ReclaimGuard { expires_at_unix: 10 })
        .unwrap();

    let report = repair_artifact_placements(&meta, &blobs, 1).await.unwrap();

    assert_eq!(report.content.changed, 0);
    assert_eq!(report.content.skipped, 1);
    assert_eq!(meta.get_artifact_placement(digest.as_str()).unwrap(), None);
}

#[tokio::test]
async fn repair_skips_a_missing_placement_held_by_a_reclaim_guard() {
    let (_directory, meta, blobs) = stores();
    let digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    meta.put_artifact_placement(digest, &ArtifactPlacement::record(ArtifactSource::Hosted, true))
        .unwrap();
    meta.compare_and_arm_reclaim_guards(&[digest], 0, 0, ReclaimGuard { expires_at_unix: 10 })
        .unwrap();

    let report = repair_artifact_placements(&meta, &blobs, 1).await.unwrap();

    assert_eq!(report.placement.changed, 0);
    assert_eq!(report.placement.skipped, 1);
    assert_eq!(
        meta.get_artifact_placement(digest).unwrap(),
        Some(ArtifactPlacement::record(ArtifactSource::Hosted, true))
    );
}

#[tokio::test]
async fn repair_cancellation_before_a_page_leaves_both_directions_unchanged() {
    let (_directory, meta, blobs) = stores();
    let digest = blobs.put_bytes(b"cancelled").await.unwrap();
    let cancelled = tokio_util::sync::CancellationToken::new();
    cancelled.cancel();

    let failure = repair_artifact_placements_cancellable(&meta, &blobs, 1, &cancelled)
        .await
        .unwrap_err();

    assert!(matches!(failure.error, ArtifactRepairError::Cancelled));
    assert_eq!(failure.report, crate::ArtifactRepairReport::default());
    assert_eq!(meta.get_artifact_placement(digest.as_str()).unwrap(), None);
}

#[tokio::test]
async fn repair_cancellation_interrupts_ownership_wait() {
    let (_directory, meta, blobs) = stores();
    let _ownership = meta.artifact_repair_ownership().await;
    let cancelled = tokio_util::sync::CancellationToken::new();
    cancelled.cancel();

    let failure = repair_artifact_placements_cancellable(&meta, &blobs, 1, &cancelled)
        .await
        .unwrap_err();

    assert!(matches!(failure.error, ArtifactRepairError::Cancelled));
    assert_eq!(failure.report, crate::ArtifactRepairReport::default());
}

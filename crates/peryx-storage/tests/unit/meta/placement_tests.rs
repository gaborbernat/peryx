//! What a committed placement records, and what a failed record does to the caller.

use peryx_ha::{ArtifactPlacement, ArtifactSource, ReclaimGuard, ReclaimGuardStore as _};
use rstest::rstest;

use crate::meta::fault::initialized;

const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[test]
fn test_a_committed_placement_reads_local_under_its_source() {
    let (store, _inner, _fault) = initialized();

    store.record_committed_placement(DIGEST, ArtifactSource::Proxy);

    assert_eq!(
        store.get_artifact_placement(DIGEST).unwrap(),
        Some(ArtifactPlacement::record(ArtifactSource::Proxy, true))
    );
}

#[test]
fn test_a_committed_placement_refines_unknown_and_keeps_a_known_source() {
    let (store, _inner, _fault) = initialized();
    store
        .put_artifact_placement(DIGEST, &ArtifactPlacement::record(ArtifactSource::Unknown, false))
        .unwrap();

    store.record_committed_placement(DIGEST, ArtifactSource::Hosted);

    assert_eq!(
        store.get_artifact_placement(DIGEST).unwrap(),
        Some(ArtifactPlacement::record(ArtifactSource::Hosted, true))
    );

    store.record_committed_placement(DIGEST, ArtifactSource::Proxy);

    assert_eq!(
        store.get_artifact_placement(DIGEST).unwrap(),
        Some(ArtifactPlacement::record(ArtifactSource::Hosted, true))
    );
}

#[rstest]
#[case::refines_unknown(ArtifactSource::Unknown, ArtifactSource::Proxy)]
#[case::keeps_known(ArtifactSource::Hosted, ArtifactSource::Hosted)]
fn test_marking_an_artifact_local_resolves_its_source(
    #[case] stored: ArtifactSource,
    #[case] expected: ArtifactSource,
) {
    let (store, _inner, _fault) = initialized();
    store
        .put_artifact_placement(DIGEST, &ArtifactPlacement::record(stored, false))
        .unwrap();

    let marked = store.mark_artifact_local(DIGEST, ArtifactSource::Proxy).unwrap();

    assert_eq!(
        (marked, store.get_artifact_placement(DIGEST).unwrap()),
        (
            ArtifactPlacement::record(expected, true),
            Some(ArtifactPlacement::record(expected, true))
        )
    );
}

#[test]
fn test_source_discovery_refines_unknown_without_changing_availability() {
    let (store, _inner, _fault) = initialized();
    store
        .put_artifact_placement(DIGEST, &ArtifactPlacement::record(ArtifactSource::Unknown, false))
        .unwrap();

    store
        .insert_artifact_placement(DIGEST, &ArtifactPlacement::record(ArtifactSource::Proxy, false))
        .unwrap();

    assert_eq!(
        store.get_artifact_placement(DIGEST).unwrap(),
        Some(ArtifactPlacement {
            source: ArtifactSource::Proxy,
            availability: peryx_ha::ByteAvailability::RemoteOnly,
        })
    );
}

#[test]
fn test_a_reclaim_guard_blocks_a_source_aware_placement_writer() {
    for expires_at_unix in [0, 10] {
        let (store, _inner, _fault) = initialized();
        store
            .compare_and_arm_reclaim_guards(&[DIGEST], 0, 0, ReclaimGuard { expires_at_unix })
            .unwrap();

        let error = store
            .put_artifact_placement(DIGEST, &ArtifactPlacement::record(ArtifactSource::Hosted, true))
            .unwrap_err();

        assert!(matches!(error, crate::meta::MetaError::BlobReclaiming { digest } if digest == DIGEST));
        assert_eq!(store.get_artifact_placement(DIGEST).unwrap(), None);
    }
}

/// The bytes are content-addressed and already durable when this runs, so a store that cannot take
/// the row leaves the caller nothing to do about it. Every commit path depends on that: a push, a
/// pull and an import all keep their bytes when the projection write fails.
#[test]
fn test_a_failed_record_leaves_the_caller_alone() {
    let (store, _inner, fault) = initialized();
    fault.arm(0);

    store.record_committed_placement(DIGEST, ArtifactSource::Hosted);

    // Returning at all is the property: the recorder swallows the failure, so reaching this line is
    // what a push, a pull or an import relies on to keep the bytes it already committed.
    assert!(fault.triggered(), "the store write has to have failed");
}

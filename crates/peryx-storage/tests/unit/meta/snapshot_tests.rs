use std::collections::BTreeSet;

use peryx_ha::{ArtifactPlacement, ArtifactSource};

use super::MetaStore;

const FIRST_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SECOND_DIGEST: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[test]
fn test_driver_placement_snapshot_skips_an_empty_request() {
    let directory = tempfile::tempdir().unwrap();
    let store = MetaStore::open(directory.path().join("peryx.redb")).unwrap();
    let _ = store.take_read_transaction_count();

    let snapshot = store.read_driver_placement_snapshot(&[], &BTreeSet::new()).unwrap();

    assert!(snapshot.driver_values.is_empty());
    assert!(snapshot.placements.is_empty());
    assert_eq!(store.take_read_transaction_count(), 0);
}

#[test]
fn test_driver_placement_snapshot_reads_requested_rows_in_one_transaction() {
    let directory = tempfile::tempdir().unwrap();
    let store = MetaStore::open(directory.path().join("peryx.redb")).unwrap();
    store.put_driver_value("first", b"one").unwrap();
    store.put_driver_value("second", b"two").unwrap();
    store
        .put_artifact_placement(FIRST_DIGEST, &ArtifactPlacement::record(ArtifactSource::Proxy, true))
        .unwrap();
    let _ = store.take_read_transaction_count();

    let snapshot = store
        .read_driver_placement_snapshot(
            &["second".to_owned(), "missing".to_owned(), "first".to_owned()],
            &BTreeSet::from([FIRST_DIGEST.to_owned(), SECOND_DIGEST.to_owned()]),
        )
        .unwrap();

    assert_eq!(store.take_read_transaction_count(), 1);
    assert_eq!(
        snapshot.driver_values,
        [Some(b"two".to_vec()), None, Some(b"one".to_vec())]
    );
    assert_eq!(
        snapshot.placements,
        [(
            FIRST_DIGEST.to_owned(),
            ArtifactPlacement::record(ArtifactSource::Proxy, true),
        )]
        .into()
    );
}

#[test]
fn test_driver_placement_snapshot_reads_driver_rows_without_placements() {
    let directory = tempfile::tempdir().unwrap();
    let store = MetaStore::open(directory.path().join("peryx.redb")).unwrap();
    store.put_driver_value("first", b"one").unwrap();

    let snapshot = store
        .read_driver_placement_snapshot(&["first".to_owned()], &BTreeSet::new())
        .unwrap();

    assert_eq!(snapshot.driver_values, [Some(b"one".to_vec())]);
}

#[test]
fn test_driver_placement_snapshot_reads_placements_without_driver_rows() {
    let directory = tempfile::tempdir().unwrap();
    let store = MetaStore::open(directory.path().join("peryx.redb")).unwrap();
    store
        .put_artifact_placement(FIRST_DIGEST, &ArtifactPlacement::record(ArtifactSource::Proxy, true))
        .unwrap();

    let snapshot = store
        .read_driver_placement_snapshot(&[], &BTreeSet::from([FIRST_DIGEST.to_owned()]))
        .unwrap();

    assert_eq!(
        snapshot.placements,
        [(
            FIRST_DIGEST.to_owned(),
            ArtifactPlacement::record(ArtifactSource::Proxy, true),
        )]
        .into()
    );
}

#[test]
fn test_read_transaction_counter_is_shared_by_store_clones() {
    let directory = tempfile::tempdir().unwrap();
    let store = MetaStore::open(directory.path().join("peryx.redb")).unwrap();
    let clone = store.clone();
    let _ = store.take_read_transaction_count();

    clone.get_driver_value("missing").unwrap();

    assert_eq!(store.take_read_transaction_count(), 1);
    assert_eq!(clone.take_read_transaction_count(), 0);
}

use std::collections::BTreeSet;
use std::str::FromStr as _;

use peryx_identity::ArtifactDigest;

use crate::meta::{
    BackendId, BackendLocation, BlobPlacementFailure, BlobPlacementKey, BlobPlacementTransition, DataCenterId,
    DigestReconciliation, MAX_PLACEMENT_RECONCILE_BATCH, MetaStore, PlacementReconcileError, PlacementReconcilePage,
};

const FENCE: u64 = 5;

fn store() -> (tempfile::TempDir, MetaStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    (dir, store)
}

fn digest(suffix: u8) -> ArtifactDigest {
    ArtifactDigest::from_str(&format!("sha256:{suffix:064x}")).unwrap()
}

fn key(suffix: u8, data_center: &str, location: &str) -> BlobPlacementKey {
    BlobPlacementKey {
        digest: digest(suffix),
        backend: BackendId::new("filesystem").unwrap(),
        data_center: DataCenterId::new(data_center).unwrap(),
        location: BackendLocation::new(location).unwrap(),
    }
}

fn dc(name: &str) -> DataCenterId {
    DataCenterId::new(name).unwrap()
}

fn stage(store: &MetaStore, key: &BlobPlacementKey) {
    store
        .apply_blob_placement(key, &BlobPlacementTransition::Stage, FENCE, 0)
        .unwrap();
}

fn verify(store: &MetaStore, key: &BlobPlacementKey) {
    stage(store, key);
    store
        .apply_blob_placement(
            key,
            &BlobPlacementTransition::Verify {
                observed: key.digest.clone(),
                size: 1,
            },
            FENCE,
            0,
        )
        .unwrap();
}

fn policy(dcs: &[&str]) -> BTreeSet<DataCenterId> {
    dcs.iter().map(|name| dc(name)).collect()
}

fn reconcile(store: &MetaStore, target: &[&str]) -> PlacementReconcilePage {
    store
        .reconcile_placement_policy(&policy(target), None, 10, |_| false)
        .unwrap()
}

#[test]
fn test_reconcile_flags_a_target_dc_missing_a_verified_copy() {
    let (_dir, store) = store();
    verify(&store, &key(1, "east", "east/01"));

    let page = reconcile(&store, &["east", "west"]);

    assert_eq!(
        page.reconciliations,
        vec![DigestReconciliation {
            digest: digest(1),
            replicate: vec![dc("west")],
            retire: Vec::new(),
        }]
    );
    assert_eq!(page.scanned, 1);
}

#[test]
fn test_reconcile_lists_replicate_targets_in_policy_order() {
    let (_dir, store) = store();
    verify(&store, &key(1, "east", "east/01"));

    let page = reconcile(&store, &["west", "south", "east"]);

    assert_eq!(page.reconciliations[0].replicate, vec![dc("south"), dc("west")]);
}

#[test]
fn test_reconcile_flags_a_verified_copy_outside_policy_for_retirement() {
    let (_dir, store) = store();
    verify(&store, &key(1, "east", "east/01"));
    verify(&store, &key(1, "west", "west/01"));

    let page = reconcile(&store, &["west"]);

    assert_eq!(
        page.reconciliations,
        vec![DigestReconciliation {
            digest: digest(1),
            replicate: Vec::new(),
            retire: vec![key(1, "east", "east/01")],
        }]
    );
}

#[test]
fn test_reconcile_retires_every_out_of_policy_verified_placement() {
    let (_dir, store) = store();
    verify(&store, &key(1, "east", "east/01"));
    verify(&store, &key(1, "east", "east/02"));
    verify(&store, &key(1, "west", "west/01"));

    let page = reconcile(&store, &["west"]);

    assert_eq!(
        page.reconciliations[0].retire,
        vec![key(1, "east", "east/01"), key(1, "east", "east/02")]
    );
}

#[test]
fn test_reconcile_reports_no_divergence_when_policy_is_satisfied() {
    let (_dir, store) = store();
    verify(&store, &key(1, "east", "east/01"));
    verify(&store, &key(1, "west", "west/01"));

    let page = reconcile(&store, &["east", "west"]);

    assert!(page.reconciliations.is_empty());
    assert_eq!(page.scanned, 1);
}

#[test]
fn test_reconcile_treats_a_failed_placement_as_missing() {
    let (_dir, store) = store();
    let failed = key(1, "east", "east/01");
    stage(&store, &failed);
    store
        .apply_blob_placement(
            &failed,
            &BlobPlacementTransition::Fail {
                class: BlobPlacementFailure::DigestMismatch,
            },
            FENCE,
            0,
        )
        .unwrap();

    let page = reconcile(&store, &["east"]);

    // A failed placement is not verified, so its data center still owes a copy.
    assert_eq!(page.reconciliations[0].replicate, vec![dc("east")]);
}

#[test]
fn test_reconcile_skips_a_withdrawn_digest() {
    let (_dir, store) = store();
    verify(&store, &key(1, "east", "east/01"));

    let page = store
        .reconcile_placement_policy(&policy(&["east", "west"]), None, 10, |candidate| {
            candidate == &digest(1)
        })
        .unwrap();

    // Revoked or reclaimed content is passed over rather than scheduled for a copy, but still counted.
    assert!(page.reconciliations.is_empty());
    assert_eq!(page.scanned, 1);
}

#[test]
fn test_reconcile_is_empty_on_an_empty_ledger() {
    let (_dir, store) = store();

    let page = reconcile(&store, &["east"]);

    assert_eq!(
        page,
        PlacementReconcilePage {
            reconciliations: Vec::new(),
            scanned: 0,
            next_cursor: None,
        }
    );
}

#[test]
fn test_reconcile_paginates_and_resumes_after_the_cursor() {
    let (_dir, store) = store();
    verify(&store, &key(1, "east", "east/01"));
    verify(&store, &key(2, "east", "east/02"));
    verify(&store, &key(3, "east", "east/03"));
    let target = policy(&["east", "west"]);

    let first = store.reconcile_placement_policy(&target, None, 2, |_| false).unwrap();
    let first_digests: Vec<ArtifactDigest> = first.reconciliations.iter().map(|item| item.digest.clone()).collect();
    assert_eq!(first_digests, vec![digest(1), digest(2)]);
    assert_eq!(first.scanned, 2);
    assert_eq!(first.next_cursor, Some(digest(2).canonical()));

    let second = store
        .reconcile_placement_policy(&target, first.next_cursor.as_deref(), 2, |_| false)
        .unwrap();
    let second_digests: Vec<ArtifactDigest> = second.reconciliations.iter().map(|item| item.digest.clone()).collect();
    assert_eq!(second_digests, vec![digest(3)]);
    assert_eq!(second.next_cursor, None);
}

#[test]
fn test_reconcile_rejects_an_out_of_range_limit() {
    let (_dir, store) = store();

    let low = store.reconcile_placement_policy(&policy(&["east"]), None, 0, |_| false);
    assert!(matches!(low, Err(PlacementReconcileError::InvalidLimit)));

    let high = store.reconcile_placement_policy(&policy(&["east"]), None, MAX_PLACEMENT_RECONCILE_BATCH + 1, |_| false);
    assert!(matches!(high, Err(PlacementReconcileError::InvalidLimit)));

    assert_eq!(
        PlacementReconcileError::InvalidLimit.to_string(),
        format!("limit must be between 1 and {MAX_PLACEMENT_RECONCILE_BATCH}")
    );
}

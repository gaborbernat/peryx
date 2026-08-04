use crate::dc_copy::{CopyPlan, plan_dc_copy};
use crate::protocol::{PlacementAvailability, PlacementDescriptor};

fn placement(data_center: &str, availability: PlacementAvailability, generation: u64) -> PlacementDescriptor {
    PlacementDescriptor {
        digest: "sha256:deadbeef".to_owned(),
        backend: "fs".to_owned(),
        data_center: data_center.to_owned(),
        location: format!("/blobs/{data_center}"),
        availability,
        generation,
    }
}

fn targets(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn test_a_pending_placement_is_not_a_source() {
    let placements = [placement("dc-a", PlacementAvailability::Pending, 1)];
    assert_eq!(plan_dc_copy(&placements, &targets(&["dc-b"]), 1), CopyPlan::NoSource);
}

#[test]
fn test_a_stale_generation_placement_is_not_a_source() {
    let placements = [placement("dc-a", PlacementAvailability::Verified, 1)];
    assert_eq!(plan_dc_copy(&placements, &targets(&["dc-b"]), 2), CopyPlan::NoSource);
}

#[test]
fn test_a_failed_placement_is_not_a_source() {
    let placements = [placement("dc-a", PlacementAvailability::Failed, 2)];
    assert_eq!(plan_dc_copy(&placements, &targets(&["dc-b"]), 2), CopyPlan::NoSource);
}

#[test]
fn test_a_target_without_a_current_copy_is_owed() {
    let placements = [placement("dc-a", PlacementAvailability::Verified, 2)];
    assert_eq!(
        plan_dc_copy(&placements, &targets(&["dc-a", "dc-b"]), 2),
        CopyPlan::Targets(vec!["dc-b".to_owned()]),
        "the source datacenter is satisfied while the other is owed a copy"
    );
}

#[test]
fn test_a_target_holding_only_a_stale_copy_is_still_owed() {
    let placements = [
        placement("dc-a", PlacementAvailability::Verified, 2),
        placement("dc-b", PlacementAvailability::Verified, 1),
    ];
    assert_eq!(
        plan_dc_copy(&placements, &targets(&["dc-b"]), 2),
        CopyPlan::Targets(vec!["dc-b".to_owned()]),
        "a target whose only verified copy is a stale generation still needs the current bytes"
    );
}

#[test]
fn test_every_target_holding_a_current_copy_is_complete() {
    let placements = [
        placement("dc-a", PlacementAvailability::Verified, 2),
        placement("dc-b", PlacementAvailability::Verified, 2),
    ];
    assert_eq!(
        plan_dc_copy(&placements, &targets(&["dc-a", "dc-b"]), 2),
        CopyPlan::Complete
    );
}

#[test]
fn test_owed_targets_are_deduplicated_and_ordered() {
    let placements = [placement("dc-x", PlacementAvailability::Verified, 1)];
    assert_eq!(
        plan_dc_copy(&placements, &targets(&["dc-c", "dc-a", "dc-c", "dc-b"]), 1),
        CopyPlan::Targets(vec!["dc-a".to_owned(), "dc-b".to_owned(), "dc-c".to_owned()]),
        "a repeated or unsorted target list plans one stable, deduplicated order"
    );
}

mod worker {
    use std::collections::HashMap;
    use std::num::{NonZeroU64, NonZeroUsize};

    use bytes::Bytes;
    use peryx_storage::blob::{BlobStore, Digest};

    use crate::blob::LoopbackBlobSource;
    use crate::dc_copy::{CopyError, CopyPlan, copy_blob_to_target, run_dc_copy};
    use crate::peer::TransferLimits;

    const CONTENT: &[u8] = b"artifact-bytes";

    fn limits() -> TransferLimits {
        TransferLimits {
            max_operations: NonZeroUsize::new(256).unwrap(),
            max_encoded_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
        }
    }

    fn source_holding(digest: &Digest) -> LoopbackBlobSource {
        LoopbackBlobSource::new(HashMap::from([(digest.clone(), Bytes::from_static(CONTENT))]), limits())
    }

    fn empty_source() -> LoopbackBlobSource {
        LoopbackBlobSource::new(HashMap::new(), limits())
    }

    fn store_in(dir: &std::path::Path, name: &str) -> BlobStore {
        BlobStore::new(dir.join(name))
    }

    fn once() -> NonZeroUsize {
        NonZeroUsize::new(1).unwrap()
    }

    #[tokio::test]
    async fn test_copy_publishes_the_verified_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let digest = Digest::of(CONTENT);
        let target = store_in(dir.path(), "dc-a");

        copy_blob_to_target(&source_holding(&digest), &target, &digest)
            .await
            .unwrap();

        assert_eq!(target.read(&digest).unwrap(), CONTENT);
    }

    #[tokio::test]
    async fn test_copy_reports_a_fetch_error_when_the_source_lacks_the_blob() {
        let dir = tempfile::tempdir().unwrap();
        let digest = Digest::of(CONTENT);
        let target = store_in(dir.path(), "dc-a");

        let error = copy_blob_to_target(&empty_source(), &target, &digest)
            .await
            .unwrap_err();

        assert!(matches!(error, CopyError::Fetch(_)), "{error}");
        assert!(error.to_string().contains("source could not serve"), "{error}");
    }

    #[tokio::test]
    async fn test_copy_reports_a_publish_error_when_the_target_cannot_write() {
        let dir = tempfile::tempdir().unwrap();
        // A file where the store expects its root directory, so publishing the blob cannot create a path.
        let root = dir.path().join("blocked");
        std::fs::write(&root, b"not a directory").unwrap();
        let digest = Digest::of(CONTENT);
        let target = BlobStore::new(&root);

        let error = copy_blob_to_target(&source_holding(&digest), &target, &digest)
            .await
            .unwrap_err();

        assert!(matches!(error, CopyError::Publish(_)), "{error}");
        assert!(error.to_string().contains("target could not publish"), "{error}");
    }

    #[tokio::test]
    async fn test_run_copies_every_owed_target() {
        let dir = tempfile::tempdir().unwrap();
        let digest = Digest::of(CONTENT);
        let stores = HashMap::from([
            ("dc-a".to_owned(), store_in(dir.path(), "dc-a")),
            ("dc-b".to_owned(), store_in(dir.path(), "dc-b")),
        ]);
        let plan = CopyPlan::Targets(vec!["dc-a".to_owned(), "dc-b".to_owned()]);

        let results = run_dc_copy(
            &plan,
            &source_holding(&digest),
            &stores,
            &digest,
            NonZeroUsize::new(2).unwrap(),
        )
        .await;

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].data_center, "dc-a");
        assert_eq!(results[1].data_center, "dc-b");
        assert!(results.iter().all(|target| target.outcome.is_ok()));
        assert_eq!(stores["dc-a"].read(&digest).unwrap(), CONTENT);
        assert_eq!(stores["dc-b"].read(&digest).unwrap(), CONTENT);
    }

    #[tokio::test]
    async fn test_run_reports_an_unconfigured_target() {
        let digest = Digest::of(CONTENT);
        let stores: HashMap<String, BlobStore> = HashMap::new();
        let plan = CopyPlan::Targets(vec!["dc-z".to_owned()]);

        let results = run_dc_copy(&plan, &source_holding(&digest), &stores, &digest, once()).await;

        assert_eq!(results.len(), 1);
        let error = results[0].outcome.as_ref().unwrap_err();
        assert!(
            matches!(error, CopyError::Unconfigured { data_center } if data_center == "dc-z"),
            "{error}"
        );
        assert!(error.to_string().contains("no target store is configured"), "{error}");
    }

    #[tokio::test]
    async fn test_run_does_nothing_when_the_plan_owes_no_copy() {
        let digest = Digest::of(CONTENT);
        let stores: HashMap<String, BlobStore> = HashMap::new();

        let complete = run_dc_copy(&CopyPlan::Complete, &source_holding(&digest), &stores, &digest, once()).await;
        let no_source = run_dc_copy(&CopyPlan::NoSource, &source_holding(&digest), &stores, &digest, once()).await;

        assert!(complete.is_empty());
        assert!(no_source.is_empty());
    }
}

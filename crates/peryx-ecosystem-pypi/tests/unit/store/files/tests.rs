use std::collections::{BTreeMap, BTreeSet};

use peryx_storage::meta::{
    ArtifactOrigin as _, ArtifactPlacement, ArtifactSource, ByteAvailability, DriverPlacementSnapshotError,
};
use peryx_test_support::fault::{backend, faulted};

use super::FileUiReadError;
use super::{
    FILE_PREFIX, FilePublication, FileSource, FileUiLookup, MetaStore, MetadataClaim, ProvenanceSibling,
    PypiArtifactOrigin, read_file_ui_records, split_file_source, split_file_source_key,
};
use crate::store::PypiStore as _;

fn store() -> (tempfile::TempDir, MetaStore) {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    (dir, meta)
}

#[test]
fn test_put_and_get_file_url() {
    let (_dir, meta) = store();
    assert_eq!(meta.get_file_url("pypi", "pkg", "deadbeef").unwrap(), None);
    meta.put_file_url("pypi", "pkg", "deadbeef", "https://files.example/pkg.whl", "pypi")
        .unwrap();
    assert_eq!(
        meta.get_file_url("pypi", "pkg", "deadbeef").unwrap(),
        Some(FileSource {
            url: "https://files.example/pkg.whl".to_owned(),
            source: "pypi".to_owned(),
            size: None,
            upstream: None,
        })
    );
}

#[test]
fn test_file_ui_records_keep_publications_with_one_digest_distinct() {
    let (_dir, meta) = store();
    for (index, url) in [
        ("alpha", "https://alpha.example/pkg.whl"),
        ("beta", "https://beta.example/pkg.whl"),
    ] {
        meta.put_file_url(index, "pkg", "deadbeef", url, index).unwrap();
    }
    let _ = meta.take_read_transaction_count();

    let records = read_file_ui_records(
        &meta,
        &[
            FileUiLookup {
                filename: "alpha.whl",
                source_index: Some("alpha"),
                normalized: "pkg",
                digest: "deadbeef",
            },
            FileUiLookup {
                filename: "beta.whl",
                source_index: Some("beta"),
                normalized: "pkg",
                digest: "deadbeef",
            },
        ],
    )
    .unwrap();

    assert_eq!(meta.take_read_transaction_count(), 1);
    assert_eq!(
        records
            .iter()
            .map(|record| record.source.as_ref().unwrap().url.as_str())
            .collect::<Vec<_>>(),
        ["https://alpha.example/pkg.whl", "https://beta.example/pkg.whl"]
    );
    assert_eq!(records[0].placement, records[1].placement);
}

#[test]
fn test_file_ui_records_do_not_read_a_source_for_a_hosted_file() {
    let (_dir, meta) = store();
    meta.put_driver_value(&format!("{FILE_PREFIX}cached/pkg/deadbeef"), &[0xff])
        .unwrap();

    let records = read_file_ui_records(
        &meta,
        &[FileUiLookup {
            filename: "hosted.whl",
            source_index: None,
            normalized: "pkg",
            digest: "deadbeef",
        }],
    )
    .unwrap();

    assert_eq!(records[0].source, None);
}

#[test]
fn test_file_ui_source_error_names_the_affected_file() {
    let (_dir, meta) = store();
    meta.put_driver_value(&format!("{FILE_PREFIX}cached/pkg/deadbeef"), &[0xff])
        .unwrap();

    let error = read_file_ui_records(
        &meta,
        &[FileUiLookup {
            filename: "broken.whl",
            source_index: Some("cached"),
            normalized: "pkg",
            digest: "deadbeef",
        }],
    )
    .unwrap_err();

    assert!(error.to_string().contains("broken.whl"), "{error}");
}

#[test]
fn test_file_ui_placement_error_redacts_the_stored_value() {
    let (directory, meta) = store();
    meta.put_artifact_placement("deadbeef", &ArtifactPlacement::record(ArtifactSource::Proxy, false))
        .unwrap();
    drop(meta);
    let database = redb::Database::open(directory.path().join("peryx.redb")).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(redb::TableDefinition::<&str, &[u8]>::new("artifact_placement"))
        .unwrap()
        .insert(
            "deadbeef",
            br#"{"source":"credential-like-marker","availability":"local"}"#.as_slice(),
        )
        .unwrap();
    write.commit().unwrap();
    drop(database);
    let meta = MetaStore::open_existing(directory.path().join("peryx.redb")).unwrap();

    let error = read_file_ui_records(
        &meta,
        &[FileUiLookup {
            filename: "broken.whl",
            source_index: None,
            normalized: "pkg",
            digest: "deadbeef",
        }],
    )
    .unwrap_err();
    let message = error.to_string();

    assert!(message.contains("broken.whl"), "{message}");
    assert!(!message.contains("credential-like-marker"), "{message}");
}

#[test]
fn test_file_ui_snapshot_failures_name_the_affected_file() {
    let (backend, fault) = backend();
    let meta = MetaStore::open_backend(faulted(&backend, &fault)).unwrap();
    let digest = "f".repeat(64);
    for index in 0..128 {
        let filler = format!("{index:064x}");
        meta.put_file_url("cached", "pkg", &filler, "https://files.example/filler.whl", "cached")
            .unwrap();
        meta.put_artifact_placement(&filler, &ArtifactPlacement::record(ArtifactSource::Proxy, false))
            .unwrap();
    }
    meta.put_file_url("cached", "pkg", &digest, "https://files.example/pkg.whl", "cached")
        .unwrap();
    meta.put_artifact_placement(&digest, &ArtifactPlacement::record(ArtifactSource::Proxy, false))
        .unwrap();
    drop(meta);
    let lookups = [FileUiLookup {
        filename: "broken.whl",
        source_index: Some("cached"),
        normalized: "pkg",
        digest: &digest,
    }];
    let mut failures = BTreeSet::new();

    for fail_after in 0..512 {
        let meta = MetaStore::reopen_backend(faulted(&backend, &fault)).unwrap();
        fault.arm(fail_after);
        let result = read_file_ui_records(&meta, &lookups);
        let triggered = fault.triggered();
        fault.disable();
        assert!(triggered);
        let error = result.unwrap_err();
        assert!(!matches!(error, FileUiReadError::Source { .. }));
        if let FileUiReadError::Snapshot { filename, source } = &error {
            assert_eq!(filename, "broken.whl");
            if matches!(source.as_ref(), DriverPlacementSnapshotError::Driver { .. }) {
                failures.insert("driver");
            }
            if matches!(source.as_ref(), DriverPlacementSnapshotError::Snapshot(_)) {
                failures.insert("snapshot");
            }
        }
        if let FileUiReadError::Placement { filename, .. } = &error {
            assert_eq!(filename, "broken.whl");
            failures.insert("placement");
        }
        if failures.len() == 3 {
            break;
        }
    }

    assert_eq!(failures, BTreeSet::from(["driver", "placement", "snapshot"]));
}

#[test]
fn test_split_file_source_key_rejects_a_key_with_an_empty_segment() {
    assert_eq!(split_file_source_key("index//sha"), None);
}

#[test]
fn test_origin_maps_to_the_neutral_source() {
    assert_eq!(PypiArtifactOrigin::Upload.artifact_source(), ArtifactSource::Hosted);
    assert_eq!(PypiArtifactOrigin::Cached.artifact_source(), ArtifactSource::Proxy);
}

#[test]
fn test_recording_a_cached_locator_projects_a_remote_only_placement() {
    let (_dir, meta) = store();
    meta.initialize_distributed_state().unwrap();
    meta.put_file_url("pypi", "pkg", "deadbeef", "https://files.example/pkg.whl", "pypi")
        .unwrap();
    assert_eq!(
        meta.get_artifact_placement("deadbeef").unwrap().unwrap().availability,
        ByteAvailability::RemoteOnly
    );
}

#[test]
fn test_file_source_without_size_keeps_routed_upstream() {
    assert_eq!(
        split_file_source("source", "https://files.example/pkg.whl\npypi\n\nmirror").unwrap(),
        FileSource {
            url: "https://files.example/pkg.whl".to_owned(),
            source: "pypi".to_owned(),
            size: None,
            upstream: Some("mirror".to_owned()),
        }
    );
}

#[test]
fn test_put_and_get_metadata_roundtrips_the_derived_digest() {
    let (_dir, meta) = store();
    assert_eq!(meta.get_metadata_digest("wheelsha").unwrap(), None);
    meta.put_metadata("wheelsha", "metasha").unwrap();
    assert_eq!(
        meta.get_metadata_digest("wheelsha").unwrap(),
        Some("metasha".to_owned())
    );
}

#[test]
fn test_get_metadata_digests_deduplicates_and_skips_missing_records() {
    let (_dir, meta) = store();
    meta.put_metadata("wheelsha-a", "metasha-a").unwrap();
    meta.put_metadata("wheelsha-b", "metasha-b").unwrap();

    let digests = meta
        .get_metadata_digests(["missing", "wheelsha-b", "wheelsha-a", "wheelsha-b"])
        .unwrap();

    assert_eq!(
        digests,
        BTreeMap::from([
            ("wheelsha-a".to_owned(), "metasha-a".to_owned()),
            ("wheelsha-b".to_owned(), "metasha-b".to_owned()),
        ])
    );
}

#[test]
fn test_get_metadata_digests_rejects_a_malformed_present_record() {
    let (_dir, meta) = store();
    let key = super::metadata_key("wheelsha");
    meta.put_driver_value(&key, &[0xff]).unwrap();

    assert_eq!(
        meta.get_metadata_digests(["wheelsha"]).unwrap_err().to_string(),
        format!("driver record {key:?} is not UTF-8")
    );
}

#[test]
fn test_get_metadata_digests_reads_one_snapshot_before_the_input_commits() {
    let (_dir, meta) = store();
    meta.put_metadata("wheelsha-a", "metasha-before").unwrap();
    meta.put_metadata("wheelsha-b", "metasha-before").unwrap();
    let digests = meta
        .get_metadata_digests(["wheelsha-a", "wheelsha-b"].into_iter().inspect(|_| {
            meta.put_metadata("wheelsha-a", "metasha-after").unwrap();
            meta.put_metadata("wheelsha-b", "metasha-after").unwrap();
        }))
        .unwrap();

    assert_eq!(
        digests,
        BTreeMap::from([
            ("wheelsha-a".to_owned(), "metasha-before".to_owned()),
            ("wheelsha-b".to_owned(), "metasha-before".to_owned()),
        ])
    );
    assert_eq!(
        meta.get_metadata_digest("wheelsha-a").unwrap(),
        Some("metasha-after".to_owned())
    );
    assert_eq!(
        meta.get_metadata_digest("wheelsha-b").unwrap(),
        Some("metasha-after".to_owned())
    );
}

#[test]
fn test_scan_file_urls_visits_each_record() {
    let (_dir, meta) = store();
    meta.put_file_url("pypi", "aa", "aa", "https://files/aa.whl", "pypi")
        .unwrap();
    let mut seen = Vec::new();
    meta.scan_file_urls(|index, normalized, digest, value| {
        seen.push((
            index.to_owned(),
            normalized.to_owned(),
            digest.to_owned(),
            value.to_owned(),
        ));
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    assert_eq!(
        seen,
        vec![(
            "pypi".to_owned(),
            "aa".to_owned(),
            "aa".to_owned(),
            "https://files/aa.whl\npypi".to_owned()
        )]
    );
}

#[test]
fn test_scan_metadata_records_visits_each_record() {
    let (_dir, meta) = store();
    meta.put_metadata("wheelsha", "metasha").unwrap();
    let mut seen = Vec::new();
    meta.scan_metadata_records(|digest, value| {
        seen.push((digest.to_owned(), value.to_owned()));
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    assert_eq!(seen, vec![("wheelsha".to_owned(), "metasha".to_owned())]);
}

fn bundle(provenance_sha256: &str) -> ProvenanceSibling<'_> {
    ProvenanceSibling {
        provenance_sha256,
        size: 16,
    }
}

#[test]
fn test_put_and_get_provenance_roundtrips_the_bundle() {
    let (_dir, meta) = store();
    assert_eq!(
        meta.get_provenance("hosted", "pkg", "wheelsha", "pkg-1.0.whl").unwrap(),
        None
    );
    meta.put_provenance("hosted", "pkg", "wheelsha", "pkg-1.0.whl", bundle("provsha"))
        .unwrap();
    assert_eq!(
        meta.get_provenance("hosted", "pkg", "wheelsha", "pkg-1.0.whl").unwrap(),
        Some(("provsha".to_owned(), 16))
    );
    assert_eq!(
        meta.list_verified_provenance("hosted", "pkg").unwrap(),
        BTreeMap::from([("pkg-1.0.whl".to_owned(), "wheelsha".to_owned())])
    );
}

#[test]
fn test_get_provenance_reads_only_the_publication_it_was_written_for() {
    let (_dir, meta) = store();
    meta.put_provenance("hosted", "pkg", "wheelsha", "pkg-1.0.whl", bundle("provsha"))
        .unwrap();

    assert_eq!(
        meta.get_provenance("other", "pkg", "wheelsha", "pkg-1.0.whl").unwrap(),
        None,
        "a second hosted index publishing the same bytes inherits no bundle"
    );
    assert_eq!(
        meta.get_provenance("hosted", "pkg", "wheelsha", "pkg-2.0.whl").unwrap(),
        None,
        "a second filename over the same bytes inherits no bundle"
    );
}

#[test]
fn test_get_provenance_suppresses_a_legacy_reference() {
    let (_dir, meta) = store();
    let key = super::provenance_key("hosted", "pkg", "wheelsha", "pkg-1.0.whl");
    meta.put_driver_value(&key, b"provsha\n16").unwrap();

    assert_eq!(
        meta.get_provenance("hosted", "pkg", "wheelsha", "pkg-1.0.whl").unwrap(),
        None
    );
    assert!(meta.list_verified_provenance("hosted", "pkg").unwrap().is_empty());
}

#[test]
fn test_list_verified_provenance_rejects_a_malformed_reference() {
    let (_dir, meta) = store();
    let key = super::provenance_key("hosted", "pkg", "wheelsha", "pkg-1.0.whl");
    meta.put_driver_value(&key, b"{").unwrap();

    assert_eq!(
        meta.list_verified_provenance("hosted", "pkg").unwrap_err().to_string(),
        format!("driver record {key:?} does not decode")
    );
}

#[test]
fn test_get_provenance_rejects_a_record_missing_its_size() {
    let (_dir, meta) = store();
    let key = super::provenance_key("hosted", "pkg", "wheelsha", "pkg-1.0.whl");
    meta.put_driver_value(&key, b"provsha").unwrap();

    let error = meta
        .get_provenance("hosted", "pkg", "wheelsha", "pkg-1.0.whl")
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!("driver record {key:?} is missing field \"size\"")
    );
}

#[test]
fn test_get_provenance_rejects_a_record_whose_size_is_not_a_number() {
    let (_dir, meta) = store();
    let key = super::provenance_key("hosted", "pkg", "wheelsha", "pkg-1.0.whl");
    meta.put_driver_value(&key, b"provsha\nhuge").unwrap();

    let error = meta
        .get_provenance("hosted", "pkg", "wheelsha", "pkg-1.0.whl")
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!("driver record {key:?} has invalid integer field \"size\"")
    );
}

#[test]
fn test_get_provenance_rejects_an_unknown_reference_version() {
    let (_dir, meta) = store();
    let key = super::provenance_key("hosted", "pkg", "wheelsha", "pkg-1.0.whl");
    meta.put_driver_value(&key, br#"{"version":2,"sha256":"provsha","size":16}"#)
        .unwrap();

    let error = meta
        .get_provenance("hosted", "pkg", "wheelsha", "pkg-1.0.whl")
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!("driver record {key:?} carries unknown field \"version 2\" and needs a newer peryx")
    );
}

#[test]
fn test_get_provenance_rejects_a_malformed_versioned_reference() {
    let (_dir, meta) = store();
    let key = super::provenance_key("hosted", "pkg", "wheelsha", "pkg-1.0.whl");
    meta.put_driver_value(&key, b"{").unwrap();

    let error = meta
        .get_provenance("hosted", "pkg", "wheelsha", "pkg-1.0.whl")
        .unwrap_err();

    assert_eq!(error.to_string(), format!("driver record {key:?} does not decode"));
}

#[test]
fn test_scan_provenance_records_visits_each_record() {
    let (_dir, meta) = store();
    meta.put_provenance("hosted", "pkg", "good", "pkg-1.0.whl", bundle("provsha"))
        .unwrap();
    let mut seen = Vec::new();
    meta.scan_provenance_records(|key, value| {
        seen.push((key.to_owned(), value.to_owned()));
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0, "hosted/pkg/good/pkg-1.0.whl");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&seen[0].1).unwrap(),
        serde_json::json!({"version": 1, "sha256": "provsha", "size": 16})
    );
}

fn seed_publication(meta: &MetaStore, value: &[u8]) {
    meta.put_driver_value(&super::publication_key("pypi", "pkg", "wheelsha", "pkg-1.0.whl"), value)
        .unwrap();
}

#[test]
fn test_get_file_publication_reads_a_claim_with_its_routed_upstream() {
    let (_dir, meta) = store();
    seed_publication(&meta, b"https://up/pkg.whl.metadata\nmetasha\npypi\nmirror");

    assert_eq!(
        meta.get_file_publication("pypi", "pkg", "wheelsha", "pkg-1.0.whl")
            .unwrap(),
        Some(FilePublication::Claimed(MetadataClaim {
            url: "https://up/pkg.whl.metadata".to_owned(),
            metadata_sha256: "metasha".to_owned(),
            source: "pypi".to_owned(),
            upstream: Some("mirror".to_owned()),
        }))
    );
}

#[test]
fn test_get_file_publication_reads_an_empty_record_as_unclaimed() {
    let (_dir, meta) = store();
    seed_publication(&meta, b"");

    assert_eq!(
        meta.get_file_publication("pypi", "pkg", "wheelsha", "pkg-1.0.whl")
            .unwrap(),
        Some(FilePublication::Unclaimed)
    );
}

#[test]
fn test_get_file_publication_is_absent_for_an_unpublished_file() {
    let (_dir, meta) = store();

    assert_eq!(
        meta.get_file_publication("pypi", "pkg", "wheelsha", "pkg-1.0.whl")
            .unwrap(),
        None
    );
}

#[rstest::rstest]
#[case::without_digest(b"https://up/pkg.whl.metadata", "metadata_sha256")]
#[case::without_source(b"https://up/pkg.whl.metadata\nmetasha", "source")]
#[case::without_upstream(b"https://up/pkg.whl.metadata\nmetasha\npypi", "upstream")]
fn test_get_file_publication_rejects_a_truncated_claim(#[case] value: &[u8], #[case] field: &str) {
    let (_dir, meta) = store();
    seed_publication(&meta, value);

    let err = meta
        .get_file_publication("pypi", "pkg", "wheelsha", "pkg-1.0.whl")
        .unwrap_err();

    assert!(
        matches!(err, peryx_storage::meta::MetaError::DriverRecordMissing { field: missing, .. } if missing == field)
    );
}

#[test]
fn test_get_file_publication_rejects_a_non_utf8_record() {
    let (_dir, meta) = store();
    seed_publication(&meta, &[0xff, 0xfe]);

    assert!(matches!(
        meta.get_file_publication("pypi", "pkg", "wheelsha", "pkg-1.0.whl")
            .unwrap_err(),
        peryx_storage::meta::MetaError::DriverRecordUtf8 { .. }
    ));
}

#[test]
fn test_scan_file_publications_visits_each_record() {
    let (_dir, meta) = store();
    seed_publication(&meta, b"https://up/pkg.whl.metadata\nmetasha\npypi\n");
    let mut seen = Vec::new();

    meta.scan_file_publications(|key, value| {
        seen.push((key.to_owned(), value.to_owned()));
        Ok::<(), std::io::Error>(())
    })
    .unwrap();

    assert_eq!(
        seen,
        vec![(
            "pypi/pkg/wheelsha/pkg-1.0.whl".to_owned(),
            "https://up/pkg.whl.metadata\nmetasha\npypi\n".to_owned()
        )]
    );
}

#[test]
fn test_each_index_resolves_its_own_source_for_one_shared_digest() {
    let (_dir, meta) = store();

    meta.put_file_url("alpha", "flask", "deadbeef", "https://alpha.example/flask.whl", "alpha")
        .unwrap();
    meta.put_file_url("beta", "flask", "deadbeef", "https://beta.example/flask.whl", "beta")
        .unwrap();

    assert_eq!(
        (
            meta.get_file_url("alpha", "flask", "deadbeef").unwrap(),
            meta.get_file_url("beta", "flask", "deadbeef").unwrap(),
        ),
        (
            Some(FileSource {
                url: "https://alpha.example/flask.whl".to_owned(),
                source: "alpha".to_owned(),
                size: None,
                upstream: None,
            }),
            Some(FileSource {
                url: "https://beta.example/flask.whl".to_owned(),
                source: "beta".to_owned(),
                size: None,
                upstream: None,
            }),
        )
    );
}

#[test]
fn test_one_indexes_source_is_invisible_to_another_index() {
    let (_dir, meta) = store();

    meta.put_file_url("alpha", "flask", "deadbeef", "https://alpha.example/flask.whl", "alpha")
        .unwrap();

    assert_eq!(meta.get_file_url("beta", "flask", "deadbeef").unwrap(), None);
}

#[test]
fn test_a_source_row_naming_no_publication_is_dropped_rather_than_given_an_owner() {
    let (_dir, meta) = store();
    meta.put_driver_value(
        &format!("{FILE_PREFIX}deadbeef"),
        b"https://legacy.example/pkg.whl\npypi",
    )
    .unwrap();
    meta.put_file_url("alpha", "flask", "feedface", "https://alpha.example/flask.whl", "alpha")
        .unwrap();

    let dropped = crate::store::drop_legacy_file_sources(&meta).unwrap();

    assert_eq!(dropped, 1);
    assert!(
        meta.get_file_url("alpha", "flask", "feedface").unwrap().is_some(),
        "an owned row survives the sweep"
    );
}

#[test]
fn test_a_source_row_naming_no_publication_is_skipped_rather_than_failing_the_scan() {
    let (_dir, meta) = store();
    meta.put_driver_value(
        &format!("{FILE_PREFIX}deadbeef"),
        b"https://legacy.example/pkg.whl\npypi",
    )
    .unwrap();
    meta.put_file_url("alpha", "flask", "feedface", "https://alpha.example/flask.whl", "alpha")
        .unwrap();

    let mut seen = Vec::new();
    meta.scan_file_urls(|index, normalized, digest, _value| {
        seen.push((index.to_owned(), normalized.to_owned(), digest.to_owned()));
        Ok::<(), std::convert::Infallible>(())
    })
    .unwrap();

    assert_eq!(
        seen,
        [("alpha".to_owned(), "flask".to_owned(), "feedface".to_owned())],
        "the orphan-blob collector never sees a key it would read as a digest"
    );
}

fn seed_file_sources(meta: &MetaStore) {
    for digest in ["deadbeef", "feedface"] {
        meta.put_driver_value(
            &format!("{FILE_PREFIX}{digest}"),
            b"https://legacy.example/pkg.whl\npypi",
        )
        .unwrap();
    }
    meta.put_file_url("alpha", "flask", "cafebabe", "https://alpha.example/flask.whl", "alpha")
        .unwrap();
}

/// The legacy rows the sweep drops and the owned rows it must leave alone.
fn source_rows(meta: &MetaStore) -> (usize, usize) {
    let (mut legacy, mut owned) = (0, 0);
    meta.scan_driver_prefix(FILE_PREFIX, |key, _| {
        if key[FILE_PREFIX.len()..].contains('/') {
            owned += 1;
        } else {
            legacy += 1;
        }
        Ok::<(), peryx_storage::meta::MetaError>(())
    })
    .unwrap();
    (legacy, owned)
}

/// The sweep reads every source row before it removes any, so a scan that fails part way must leave a
/// store that is all-or-nothing. A half-swept store is the shape to rule out: the rows the scan reached
/// are gone and the rows it never saw remain, which the next sweep reports as fewer legacy rows and an
/// operator reads as progress rather than as a sweep that failed. A commit that lands and then reports
/// a failed sync is not that shape, so both ends of the range are accepted and the middle is not.
///
/// A store handle does not survive its own injected failure, so each step rebuilds the pages from a
/// fresh seed and reopens them rather than reusing one handle. 96 seed-sweep-count rounds, all
/// in-memory, in half a second.
#[test]
fn test_a_sweep_whose_scan_fails_drops_no_source_row_on_its_own() {
    let mut failed = 0_u32;
    for fail_after in 0..96 {
        let (pages, fault) = peryx_test_support::fault::backend();
        let meta = MetaStore::open_backend(peryx_test_support::fault::faulted(&pages, &fault)).unwrap();
        seed_file_sources(&meta);
        drop(meta);
        let meta = MetaStore::reopen_backend(peryx_test_support::fault::faulted(&pages, &fault)).unwrap();
        fault.arm(fail_after);
        let dropped = crate::store::drop_legacy_file_sources(&meta);
        fault.disable();
        drop(meta);

        let meta = MetaStore::reopen_backend(peryx_test_support::fault::faulted(&pages, &fault)).unwrap();
        let rows = source_rows(&meta);
        if let Ok(count) = dropped {
            assert_eq!(
                (count, rows),
                (2, (0, 1)),
                "injecting after {fail_after} reads swept part way"
            );
        } else {
            failed += 1;
            assert!(
                matches!(rows, (0 | 2, 1)),
                "injecting after {fail_after} reads left {rows:?} rows"
            );
        }
    }

    assert!(failed > 0, "no injection point reached the scan");
}

use std::collections::BTreeMap;

use peryx_storage::meta::{MetaError, MetaStore};

use super::{ReleaseMetadataLocator, release_metadata_selection, stored_release_metadata_selection};
use crate::store::{Guard, PromotedRelease, PublishedFile, PypiStore as _};

fn store() -> (tempfile::TempDir, MetaStore) {
    let directory = tempfile::tempdir().unwrap();
    let meta = MetaStore::open(directory.path().join("peryx.redb")).unwrap();
    (directory, meta)
}

fn record(filename: &str, uploaded: Option<&str>, artifact: char, metadata: char) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "version": "1.0.0",
        "file": {
            "filename": filename,
            "hashes": {"sha256": artifact.to_string().repeat(64)},
            "upload-time": uploaded,
            "core-metadata": {"sha256": metadata.to_string().repeat(64)}
        }
    }))
    .unwrap()
}

fn trashed_record(filename: &str, uploaded: &str, artifact: char, metadata: char) -> Vec<u8> {
    let mut record: serde_json::Value =
        serde_json::from_slice(&record(filename, Some(uploaded), artifact, metadata)).unwrap();
    record["trashed"] = serde_json::json!({});
    serde_json::to_vec(&record).unwrap()
}

fn promoted_record(filename: &str, version: &str, uploaded: Option<&str>, artifact: char) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "version": version,
        "file": {
            "filename": filename,
            "hashes": {"sha256": artifact.to_string().repeat(64)},
            "upload-time": uploaded,
            "core-metadata": true
        },
        "imports": "Before25"
    }))
    .unwrap()
}

fn promote(meta: &MetaStore, records: &[(String, String, Vec<u8>)]) -> Result<usize, MetaError> {
    meta.promote_files_checked(
        false,
        &PromotedRelease {
            source: "staging",
            index: "hosted",
            normalized: "flask",
            display: "Flask",
            records,
            blob_sizes: &BTreeMap::new(),
            reservations: &BTreeMap::new(),
            submitted_at_unix: 1_767_225_600,
        },
        |_filename, _token, _stored| Ok(Guard::Commit),
    )
}

#[test]
fn test_release_metadata_selection_migrates_once_by_submission_time_then_filename() {
    let (_directory, meta) = store();
    for (filename, uploaded, artifact, metadata) in [
        ("flask-1.0-d.whl", Some("2025-01-01T00:00:00.100Z"), 'd', '4'),
        ("flask-1.0-b.whl", Some("2025-01-01T00:00:00.100Z"), 'b', '2'),
        ("flask-1.0-a.whl", Some("2025-01-01T00:00:00.900Z"), 'a', '1'),
        ("flask-1.0-c.whl", Some("2026-01-01T00:00:00Z"), 'c', '3'),
        ("flask-1.0-e.whl", None, 'e', '5'),
    ] {
        meta.put_driver_value(
            &format!("pypi\0u\0hosted/flask/{filename}"),
            &record(filename, uploaded, artifact, metadata),
        )
        .unwrap();
    }

    let selected = release_metadata_selection(&meta, "hosted", "flask", "1.0")
        .unwrap()
        .unwrap();
    assert_eq!(selected.filename, "flask-1.0-b.whl");
    assert_eq!(selected.artifact_sha256, "b".repeat(64));
    assert_eq!(selected.metadata, ReleaseMetadataLocator::Digest("2".repeat(64)));

    meta.put_driver_value(
        "pypi\0u\0hosted/flask/flask-1.0-earlier.whl",
        &record("flask-1.0-earlier.whl", Some("2024-01-01T00:00:00Z"), 'd', '4'),
    )
    .unwrap();
    assert_eq!(
        release_metadata_selection(&meta, "hosted", "flask", "1.0.0")
            .unwrap()
            .unwrap(),
        selected
    );
}

#[test]
fn test_release_metadata_selection_ignores_trashed_and_unpublished_rows() {
    let (_directory, meta) = store();
    meta.put_driver_value(
        "pypi\0u\0hosted/flask/flask-1.0-trashed.whl",
        &trashed_record("flask-1.0-trashed.whl", "2024-01-01T00:00:00Z", 'a', '1'),
    )
    .unwrap();
    meta.put_driver_value(
        "pypi\0u\0hosted/flask/flask-1.0-unpublished.whl",
        br#"{"version":"1.0","file":null}"#,
    )
    .unwrap();
    meta.put_driver_value(
        "pypi\0u\0hosted/flask/flask-1.0-live.whl",
        &record("flask-1.0-live.whl", Some("2026-01-01T00:00:00Z"), 'c', '3'),
    )
    .unwrap();

    let selected = release_metadata_selection(&meta, "hosted", "flask", "1.0")
        .unwrap()
        .unwrap();
    assert_eq!(selected.filename, "flask-1.0-live.whl");
}

#[test]
fn test_release_metadata_selection_rejects_invalid_versions() {
    let (_directory, meta) = store();

    assert_eq!(
        release_metadata_selection(&meta, "hosted", "flask", "not/a/version").unwrap(),
        None
    );
    assert_eq!(
        stored_release_metadata_selection(&meta, "hosted", "flask", "not/a/version").unwrap(),
        None
    );
    assert_eq!(
        release_metadata_selection(&meta, "hosted", "flask", "1.0").unwrap(),
        None
    );
}

#[test]
fn test_stored_release_metadata_selection_rejects_a_malformed_row() {
    let (_directory, meta) = store();
    meta.put_driver_value("pypi\0b\0hosted/flask/1", b"not json").unwrap();

    assert!(matches!(
        stored_release_metadata_selection(&meta, "hosted", "flask", "1.0"),
        Err(MetaError::DriverRecordMalformed { .. })
    ));
}

#[test]
fn test_release_metadata_selection_orders_missing_submission_times() {
    let (_directory, meta) = store();
    for (filename, uploaded, artifact) in [
        ("flask-1.0-a.whl", None, 'a'),
        ("flask-1.0-b.whl", None, 'b'),
        ("flask-1.0-c.whl", Some("2026-01-01T00:00:00Z"), 'c'),
    ] {
        meta.put_driver_value(
            &format!("pypi\0u\0hosted/flask/{filename}"),
            &record(filename, uploaded, artifact, '1'),
        )
        .unwrap();
    }

    assert_eq!(
        release_metadata_selection(&meta, "hosted", "flask", "1.0")
            .unwrap()
            .unwrap()
            .filename,
        "flask-1.0-c.whl"
    );
}

#[test]
fn test_release_metadata_selection_preserves_generated_metadata_locators() {
    let (_directory, meta) = store();
    for (version, metadata) in [
        ("1.0", serde_json::json!(true)),
        ("2.0", serde_json::json!({"md5": "abc"})),
    ] {
        let filename = format!("flask-{version}.whl");
        meta.put_driver_value(
            &format!("pypi\0u\0hosted/flask/{filename}"),
            &serde_json::to_vec(&serde_json::json!({
                "version": version,
                "file": {
                    "filename": filename,
                    "hashes": {"sha256": "a".repeat(64)},
                    "core-metadata": metadata
                }
            }))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            release_metadata_selection(&meta, "hosted", "flask", version)
                .unwrap()
                .unwrap()
                .metadata,
            ReleaseMetadataLocator::Generated
        );
    }
}

#[test]
fn test_promoted_metadata_selection_scans_existing_rows_once() {
    let (_directory, meta) = store();
    meta.put_driver_value("pypi\0z\0hosted/flask", b"").unwrap();
    for (filename, value) in [
        (
            "flask-1.0-a-trashed.whl",
            trashed_record("flask-1.0-a-trashed.whl", "2020-01-01T00:00:00Z", 'a', '1'),
        ),
        (
            "flask-1.0-b-invalid.whl",
            promoted_record("flask-1.0-b-invalid.whl", "not/a/version", None, 'b'),
        ),
        (
            "flask-1.0-c-other.whl",
            promoted_record("flask-1.0-c-other.whl", "2.0", None, 'c'),
        ),
        ("flask-1.0-d-empty.whl", br#"{"version":"1.0","file":null}"#.to_vec()),
        (
            "flask-1.0-e-existing.whl",
            promoted_record("flask-1.0-e-existing.whl", "1.0", None, 'e'),
        ),
        (
            "flask-1.0-f-existing.whl",
            promoted_record("flask-1.0-f-existing.whl", "1.0", None, 'f'),
        ),
    ] {
        meta.put_driver_value(&format!("pypi\0u\0hosted/flask/{filename}"), &value)
            .unwrap();
    }
    let records = vec![(
        "flask-1.0-new.whl".to_owned(),
        "f".repeat(64),
        promoted_record("flask-1.0-new.whl", "1.0", Some("2026-01-01T00:00:00Z"), 'f'),
    )];

    assert_eq!(promote(&meta, &records).unwrap(), 1);
    assert_eq!(
        stored_release_metadata_selection(&meta, "hosted", "flask", "1.0")
            .unwrap()
            .unwrap()
            .filename,
        "flask-1.0-e-existing.whl"
    );
}

#[test]
fn test_promoted_metadata_selection_keeps_an_existing_choice() {
    let (_directory, meta) = store();
    let first = vec![(
        "flask-1.0-a.whl".to_owned(),
        "a".repeat(64),
        promoted_record("flask-1.0-a.whl", "1.0", None, 'a'),
    )];
    let second = vec![(
        "flask-1.0-b.whl".to_owned(),
        "b".repeat(64),
        promoted_record("flask-1.0-b.whl", "1.0", None, 'b'),
    )];

    promote(&meta, &first).unwrap();
    promote(&meta, &second).unwrap();

    assert_eq!(
        stored_release_metadata_selection(&meta, "hosted", "flask", "1.0")
            .unwrap()
            .unwrap()
            .filename,
        "flask-1.0-a.whl"
    );
}

#[test]
fn test_promoted_metadata_selection_rejects_malformed_state() {
    let (_directory, meta) = store();
    meta.put_driver_value("pypi\0b\0hosted/flask/1", b"not json").unwrap();
    let records = vec![(
        "flask-1.0.whl".to_owned(),
        "a".repeat(64),
        promoted_record("flask-1.0.whl", "1.0", None, 'a'),
    )];

    assert!(matches!(
        promote(&meta, &records),
        Err(MetaError::DriverRecordMalformed { .. })
    ));
}

#[test]
fn test_promoted_metadata_selection_rejects_malformed_upload_rows() {
    let (_directory, meta) = store();
    meta.put_driver_value("pypi\0z\0hosted/flask", b"").unwrap();
    meta.put_driver_value("pypi\0u\0hosted/flask/broken.whl", b"not json")
        .unwrap();
    let records = vec![(
        "flask-1.0.whl".to_owned(),
        "a".repeat(64),
        promoted_record("flask-1.0.whl", "1.0", None, 'a'),
    )];

    assert!(matches!(
        promote(&meta, &records),
        Err(MetaError::DriverRecordMalformed { .. })
    ));
}

#[test]
fn test_promoted_metadata_selection_requires_a_file() {
    let (_directory, meta) = store();
    let records = vec![(
        "flask-1.0.whl".to_owned(),
        "a".repeat(64),
        br#"{"version":"1.0","file":null,"imports":"Before25"}"#.to_vec(),
    )];

    assert!(matches!(
        promote(&meta, &records),
        Err(MetaError::DriverRecordMissing { .. })
    ));
}

#[test]
fn test_published_metadata_selection_rejects_malformed_upload_rows() {
    let (_directory, meta) = store();
    meta.put_driver_value("pypi\0z\0hosted/flask", b"").unwrap();
    meta.put_driver_value("pypi\0u\0hosted/flask/broken.whl", b"not json")
        .unwrap();
    let record = promoted_record("flask-1.0.whl", "1.0", None, 'a');
    let file = PublishedFile {
        index: "hosted",
        normalized: "flask",
        display: "Flask",
        filename: "flask-1.0.whl",
        artifact_sha256: "a",
        artifact_size: 1,
        record: &record,
        version: "1.0",
        submitted_at_unix: 0,
        metadata: None,
        provenance: None,
        quota: None,
    };

    assert!(matches!(
        meta.publish_file_if(false, &file, |_| Ok::<_, MetaError>(Guard::Commit)),
        Err(MetaError::DriverRecordMalformed { .. })
    ));
}

#[test]
fn test_published_metadata_selection_prefers_historical_rows_to_backdated_publications() {
    let (_directory, meta) = store();
    meta.put_driver_value("pypi\0z\0hosted/flask", b"").unwrap();
    meta.put_driver_value(
        "pypi\0u\0hosted/flask/flask-1.0-existing.whl",
        &record("flask-1.0-existing.whl", Some("2026-01-01T00:00:00Z"), 'b', '2'),
    )
    .unwrap();
    let record = promoted_record("flask-1.0-new.whl", "1.0", None, 'a');
    let file = PublishedFile {
        index: "hosted",
        normalized: "flask",
        display: "Flask",
        filename: "flask-1.0-new.whl",
        artifact_sha256: "a",
        artifact_size: 1,
        record: &record,
        version: "1.0",
        submitted_at_unix: 0,
        metadata: None,
        provenance: None,
        quota: None,
    };

    meta.publish_file_if(false, &file, |_| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();

    assert_eq!(
        stored_release_metadata_selection(&meta, "hosted", "flask", "1.0")
            .unwrap()
            .unwrap()
            .filename,
        "flask-1.0-existing.whl"
    );
}

#[test]
fn test_published_metadata_selection_rejects_malformed_state() {
    let (_directory, meta) = store();
    meta.put_driver_value("pypi\0b\0hosted/flask/1", b"not json").unwrap();
    let record = promoted_record("flask-1.0.whl", "1.0", None, 'a');
    let file = PublishedFile {
        index: "hosted",
        normalized: "flask",
        display: "Flask",
        filename: "flask-1.0.whl",
        artifact_sha256: "a",
        artifact_size: 1,
        record: &record,
        version: "1.0",
        submitted_at_unix: 0,
        metadata: None,
        provenance: None,
        quota: None,
    };

    assert!(matches!(
        meta.publish_file_if(false, &file, |_| Ok::<_, MetaError>(Guard::Commit)),
        Err(MetaError::DriverRecordMalformed { .. })
    ));
}

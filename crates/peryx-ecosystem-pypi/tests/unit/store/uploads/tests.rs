use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::mpsc::sync_channel;
use std::thread;

use peryx_storage::meta::{AccountingClass, DriverMutation, JournalEntry, NewQuotaReservation, QuotaLimits};
use rstest::rstest;
use serde_json::json;

use super::{
    FileOverride, Guard, MetaError, MetaStore, MetadataSibling, OverrideMutation, PromotedRelease, ProvenanceSibling,
    PublishError, PublishedFile, PublishedState, UploadMutation, UploadMutationPlan, map_publish_error,
    mutate_uploads_and_overrides, override_key, provenance_key, upload_key,
};
use crate::Yanked;
use crate::store::{PypiStore as _, read_journal_entries};
use crate::upload::UploadStoreError;

const PUBLISHED_RECORD: &[u8] = br#"{"version":"1.0","file":{"filename":"flask-1.0.whl","url":"https://files.invalid/flask-1.0.whl"},"imports":{"V1":{"exclusive":[],"shared":[]}}}"#;

fn store() -> (tempfile::TempDir, MetaStore) {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    (dir, meta)
}

fn uninitialized_store() -> (tempfile::TempDir, MetaStore) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("peryx.redb");
    drop(redb::Database::create(&path).unwrap());
    (directory, MetaStore::open_existing(path).unwrap())
}

fn published() -> PublishedFile<'static> {
    PublishedFile {
        index: "hosted",
        normalized: "flask",
        display: "Flask",
        filename: "flask-1.0.whl",
        artifact_sha256: "artifact-sha",
        artifact_size: 8,
        record: PUBLISHED_RECORD,
        version: "1.0",
        submitted_at_unix: 123,
        metadata: Some(MetadataSibling {
            metadata_sha256: "metadata-sha",
            size: 8,
        }),
        provenance: None,
        quota: None,
    }
}

fn import_record(version: &str, exclusive: &[&str], shared: &[&str]) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "version": version,
        "file": {"filename": "flask-1.0.whl", "url": "https://files.invalid/flask-1.0.whl"},
        "imports": {
            "V1": {
                "exclusive": exclusive.iter().map(|name| json!({"name": name, "private": false})).collect::<Vec<_>>(),
                "shared": shared.iter().map(|name| json!({"name": name, "private": false})).collect::<Vec<_>>(),
            },
        },
    }))
    .unwrap()
}

fn seed_release(meta: &MetaStore, count: usize) {
    let record = import_record("1.0", &["flask"], &[]);
    for position in 0..count {
        meta.put_driver_value(
            &upload_key("hosted", "flask", &format!("flask-1.0-{position:03}.whl")),
            &record,
        )
        .unwrap();
    }
}

fn mark_release_imports_initialized(meta: &MetaStore) {
    meta.put_driver_value(&crate::store::release_imports_init_key("hosted", "flask"), b"")
        .unwrap();
}

fn put_release_import_constraint(meta: &MetaStore, value: &serde_json::Value) {
    mark_release_imports_initialized(meta);
    meta.put_driver_value(
        &crate::store::release_imports_key("hosted", "flask", "1"),
        &serde_json::to_vec(value).unwrap(),
    )
    .unwrap();
}

fn keep_upload(_filename: &str, record: &[u8]) -> Result<UploadMutation, MetaError> {
    serde_json::from_slice::<serde_json::Value>(record)?;
    Ok(UploadMutation::Keep)
}

fn replace_with_published_record(_filename: &str, record: &[u8]) -> Result<UploadMutation, MetaError> {
    serde_json::from_slice::<serde_json::Value>(record)?;
    Ok(UploadMutation::Replace(PUBLISHED_RECORD.to_vec()))
}

#[test]
fn test_publish_rejects_conflicting_release_import_declarations() {
    let (_dir, meta) = store();
    let first = import_record("1.0", &["flask"], &[]);
    let second = import_record("1.0.0", &[], &["flask"]);

    meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0-py3-none-any.whl",
            record: &first,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    )
    .unwrap();
    let result = meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0.tar.gz",
            record: &second,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    );

    assert!(matches!(result, Err(MetaError::DriverPrecondition(message)) if message.contains("exclusive and shared")));
    assert!(
        meta.get_upload("hosted", "flask", "flask-1.0.tar.gz")
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_publish_rejects_inconsistent_release_import_declarations() {
    let (_dir, meta) = store();
    let first = import_record("1.0", &["flask"], &[]);
    let second = import_record("1.0", &["other"], &[]);
    meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0-py3-none-any.whl",
            record: &first,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    )
    .unwrap();

    let result = meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0.tar.gz",
            record: &second,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    );

    assert!(matches!(result, Err(MetaError::DriverPrecondition(message)) if message.contains("are inconsistent")));
}

#[test]
fn test_publish_rejects_mixing_legacy_and_declared_imports() {
    let (_dir, meta) = store();
    let legacy = br#"{"version":"1.0","imports":"Before25"}"#;
    meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0.tar.gz",
            record: legacy,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    )
    .unwrap();
    let declared = import_record("1.0", &["flask"], &[]);

    let result = meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0.whl",
            record: &declared,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    );

    assert!(matches!(result, Err(MetaError::DriverPrecondition(message)) if message.contains("metadata before 2.5")));
}

#[test]
fn test_publish_rejects_replacing_a_release_member_with_conflicting_imports() {
    let (_dir, meta) = store();
    let matching = import_record("1.0", &["flask"], &[]);
    for filename in ["flask-1.0-py3-none-any.whl", "flask-1.0.tar.gz"] {
        meta.publish_file_if(
            false,
            &PublishedFile {
                filename,
                record: &matching,
                ..published()
            },
            |_stored| Ok::<_, MetaError>(Guard::Commit),
        )
        .unwrap();
    }
    let conflicting = import_record("1.0.0", &[], &["flask"]);

    let result = meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0-py3-none-any.whl",
            record: &conflicting,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    );

    assert!(matches!(result, Err(MetaError::DriverPrecondition(message)) if message.contains("exclusive and shared")));
    assert_eq!(
        meta.get_upload("hosted", "flask", "flask-1.0-py3-none-any.whl")
            .unwrap()
            .as_deref(),
        Some(matching.as_slice())
    );
}

#[test]
fn test_deleting_the_last_declaration_drops_its_release_constraint() {
    let (_dir, meta) = store();
    let exclusive = import_record("1.0", &["flask"], &[]);
    let legacy = serde_json::to_vec(&json!({
        "version": "1.0",
        "file": {"filename": "flask-1.0.tar.gz", "url": "https://files.invalid/flask-1.0.tar.gz"},
    }))
    .unwrap();
    let exclusive_filename = "flask-1.0-py3-none-any.whl";
    let legacy_filename = "flask-1.0.tar.gz";
    meta.put_driver_value(&upload_key("hosted", "flask", exclusive_filename), &exclusive)
        .unwrap();
    meta.put_driver_value(&upload_key("hosted", "flask", legacy_filename), &legacy)
        .unwrap();
    crate::store::initialize_release_imports(&meta, "hosted", "flask").unwrap();

    assert!(
        meta.delete_upload(false, "hosted", "flask", exclusive_filename, 123)
            .unwrap()
    );
    let shared = serde_json::from_value(json!({
        "V1": {"exclusive": [], "shared": [{"name": "flask", "private": false}]}
    }))
    .unwrap();
    assert!(
        crate::store::classify_upload_if_unchanged(&meta, "hosted", "flask", legacy_filename, &legacy, shared,)
            .unwrap()
    );

    let record = import_record("1.0", &[], &["flask"]);
    meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0-py3-none-any-2.whl",
            record: &record,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    )
    .unwrap();
}

#[test]
fn test_classifying_legacy_release_members_does_not_admit_each_partial_projection() {
    let (_dir, meta) = store();
    let first = serde_json::to_vec(&json!({
        "version": "1.0",
        "file": {"filename": "flask-1.0-py3-none-any.whl", "url": "https://files.invalid/flask-1.0-py3-none-any.whl"},
    }))
    .unwrap();
    let second = serde_json::to_vec(&json!({
        "version": "1.0",
        "file": {"filename": "flask-1.0.tar.gz", "url": "https://files.invalid/flask-1.0.tar.gz"},
    }))
    .unwrap();
    let first_filename = "flask-1.0-py3-none-any.whl";
    let second_filename = "flask-1.0.tar.gz";
    meta.put_driver_value(&upload_key("hosted", "flask", first_filename), &first)
        .unwrap();
    meta.put_driver_value(&upload_key("hosted", "flask", second_filename), &second)
        .unwrap();
    crate::store::initialize_release_imports(&meta, "hosted", "flask").unwrap();
    let imports: crate::upload::ImportDeclarations = serde_json::from_value(json!({
        "V1": {"exclusive": [{"name": "flask", "private": false}], "shared": []}
    }))
    .unwrap();

    assert!(
        crate::store::classify_upload_if_unchanged(&meta, "hosted", "flask", first_filename, &first, imports.clone(),)
            .unwrap()
    );
    assert!(
        crate::store::classify_upload_if_unchanged(&meta, "hosted", "flask", second_filename, &second, imports,)
            .unwrap()
    );
    let record = import_record("1.0", &["flask"], &[]);
    meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0-py3-none-any-2.whl",
            record: &record,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    )
    .unwrap();
}

#[rstest]
#[case::missing(None, b"expected", false)]
#[case::stale(Some(&br#"{"version":"1.0"}"#[..]), b"different", false)]
#[case::classified(Some(PUBLISHED_RECORD), PUBLISHED_RECORD, false)]
fn test_classify_upload_if_unchanged_resolves_non_writable_rows(
    #[case] stored: Option<&[u8]>,
    #[case] expected: &[u8],
    #[case] changed: bool,
) {
    let (_dir, meta) = store();
    let filename = "flask-1.0.whl";
    if let Some(stored) = stored {
        meta.put_driver_value(&upload_key("hosted", "flask", filename), stored)
            .unwrap();
    }
    crate::store::initialize_release_imports(&meta, "hosted", "flask").unwrap();

    assert_eq!(
        crate::store::classify_upload_if_unchanged(
            &meta,
            "hosted",
            "flask",
            filename,
            expected,
            crate::upload::ImportDeclarations::Before25,
        )
        .unwrap(),
        changed
    );
}

#[test]
fn test_status_rewrite_keeps_a_historical_import_conflict_manageable() {
    let (_dir, meta) = store();
    let exclusive = import_record("1.0", &["flask"], &[]);
    let shared = import_record("1.0", &[], &["flask"]);
    meta.put_driver_value(&upload_key("hosted", "flask", "flask-1.0.whl"), &exclusive)
        .unwrap();
    meta.put_driver_value(&upload_key("hosted", "flask", "flask-1.0.tar.gz"), &shared)
        .unwrap();
    crate::store::initialize_release_imports(&meta, "hosted", "flask").unwrap();

    assert_eq!(
        meta.mutate_uploads(false, "hosted", "flask", "yank", 123, |_filename, record| {
            let mut uploaded = serde_json::from_slice::<crate::upload::Uploaded>(record).unwrap();
            uploaded.file.yanked = Yanked::Yes;
            Ok::<_, MetaError>(UploadMutation::Replace(serde_json::to_vec(&uploaded).unwrap()))
        })
        .unwrap(),
        2
    );
}

#[rstest]
#[case::empty(0)]
#[case::one_page(128)]
#[case::one_page_and_one(129)]
#[case::two_pages(256)]
#[case::two_pages_and_one(257)]
fn test_release_import_initialization_pages_preserve_admission(#[case] count: usize) {
    let (_dir, meta) = store();
    seed_release(&meta, count);

    crate::store::initialize_release_imports(&meta, "hosted", "flask").unwrap();
    crate::store::initialize_release_imports(&meta, "hosted", "flask").unwrap();
    let conflicting = import_record("1.0", &[], &["flask"]);
    let result = meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0-new.whl",
            record: &conflicting,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    );

    if count == 0 {
        assert!(result.unwrap());
    } else {
        assert!(matches!(
            result,
            Err(MetaError::DriverPrecondition(message)) if message.contains("exclusive and shared")
        ));
    }
}

#[test]
fn test_release_import_initialization_resumes_after_a_page_scan_failure() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("peryx.redb");
    let meta = MetaStore::open(&path).unwrap();
    seed_release(&meta, 257);
    meta.fail_driver_prefix_scan_after(129);

    assert!(crate::store::initialize_release_imports(&meta, "hosted", "flask").is_err());
    assert_eq!(meta.current_serial().unwrap(), 1);
    drop(meta);

    let meta = MetaStore::open_existing(path).unwrap();
    crate::store::initialize_release_imports(&meta, "hosted", "flask").unwrap();
    let conflicting = import_record("1.0", &[], &["flask"]);
    let result = meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0-new.whl",
            record: &conflicting,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    );

    assert!(matches!(
        result,
        Err(MetaError::DriverPrecondition(message)) if message.contains("exclusive and shared")
    ));
}

#[test]
fn test_release_import_migration_replays_without_a_warehouse_entry() {
    let (_writer_directory, writer) = store();
    let (_replica_directory, replica) = store();
    let record = import_record("1.0", &["flask"], &[]);
    writer
        .commit_driver_txn(|txn| {
            txn.put("test/release-import-replay", b"seed").unwrap();
            for position in 0..129 {
                txn.put(
                    &upload_key("hosted", "flask", &format!("flask-1.0-{position:03}.whl")),
                    &record,
                )
                .unwrap();
            }
            Ok::<_, MetaError>(((), vec![b"{}".to_vec()]))
        })
        .unwrap();
    writer
        .commit_driver_txn(|txn| {
            txn.remove("test/release-import-replay")?;
            Ok::<_, MetaError>(((), vec![b"{}".to_vec()]))
        })
        .unwrap();
    crate::store::initialize_release_imports(&writer, "hosted", "flask").unwrap();
    assert!(read_journal_entries(&writer, 0, 16).unwrap().entries.is_empty());

    for record in writer.journal_after(0, 16).unwrap() {
        let expected = replica.current_serial().unwrap();
        replica
            .commit_replica_txn(expected, |txn| {
                for mutation in &record.mutations {
                    match mutation {
                        DriverMutation::Put { key, value } => txn.put(key, value)?,
                        DriverMutation::Delete { key } => {
                            txn.remove(key)?;
                        }
                    }
                }
                Ok::<_, MetaError>((
                    (),
                    vec![JournalEntry {
                        payload: record.payload.clone(),
                        mutations: record.mutations.clone(),
                        blobs: record.blobs.clone(),
                    }],
                ))
            })
            .unwrap();
    }
    assert_eq!(replica.current_serial().unwrap(), writer.current_serial().unwrap());

    let conflicting = import_record("1.0", &[], &["flask"]);
    let result = replica.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0-new.whl",
            record: &conflicting,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    );

    assert!(matches!(
        result,
        Err(MetaError::DriverPrecondition(message)) if message.contains("exclusive and shared")
    ));
}

#[rstest]
#[case::invalid_utf8(b"\xff")]
#[case::outside_prefix(b"other")]
fn test_release_import_initialization_rejects_a_corrupt_cursor(#[case] marker: &[u8]) {
    let (_dir, meta) = store();
    meta.put_driver_value(&crate::store::release_imports_init_key("hosted", "flask"), marker)
        .unwrap();

    let error = crate::store::initialize_release_imports(&meta, "hosted", "flask").unwrap_err();

    assert!(
        matches!(error, MetaError::DriverPrecondition(message) if message.contains("corrupt release import initialization"))
    );
}

#[test]
fn test_release_import_initialization_rejects_a_cursor_at_the_upload_prefix() {
    let (_dir, meta) = store();
    let cursor = upload_key("hosted", "flask", "");
    meta.put_driver_value(
        &crate::store::release_imports_init_key("hosted", "flask"),
        cursor.as_bytes(),
    )
    .unwrap();

    let error = crate::store::initialize_release_imports(&meta, "hosted", "flask").unwrap_err();

    assert!(matches!(error, MetaError::DriverPrecondition(message) if message.contains("outside its upload prefix")));
}

#[test]
fn test_release_import_initialization_excludes_trashed_rows() {
    let (_dir, meta) = store();
    meta.put_driver_value(
        &upload_key("hosted", "flask", "flask-1.0.whl"),
        &serde_json::to_vec(&json!({
            "version": "1.0",
            "imports": "Before25",
            "trashed": {"at": 1},
        }))
        .unwrap(),
    )
    .unwrap();

    crate::store::initialize_release_imports(&meta, "hosted", "flask").unwrap();
    let record = import_record("1.0", &["flask"], &[]);
    assert!(
        meta.publish_file_if(
            false,
            &PublishedFile {
                filename: "flask-1.0-new.whl",
                record: &record,
                ..published()
            },
            |_stored| Ok::<_, MetaError>(Guard::Commit),
        )
        .unwrap()
    );
}

#[test]
fn test_upload_admission_rejects_incomplete_initialization() {
    let (_dir, meta) = store();
    meta.put_driver_value(&upload_key("hosted", "flask", "flask-1.0-old.whl"), PUBLISHED_RECORD)
        .unwrap();
    let record = import_record("1.0", &[], &[]);

    meta.commit_driver_txn(|txn| {
        let error = crate::store::admit_upload_row(txn, "hosted", "flask", "flask-1.0-new.whl", &record).unwrap_err();
        assert!(
            matches!(error, crate::store::UploadWriteError::Meta(MetaError::DriverPrecondition(message)) if message.contains("initialization") && message.contains("incomplete"))
        );
        Ok::<_, MetaError>(((), Vec::new()))
    })
    .unwrap();
}

#[test]
fn test_journal_version_falls_back_to_the_distribution_filename() {
    assert_eq!(
        super::journal_version("flask-2.0-py3-none-any.whl", b"invalid"),
        Some("2.0".to_owned())
    );
}

#[rstest]
#[case::trashed(br#"{"version":"1.0","imports":"Before25","trashed":{}}"#, None)]
#[case::invalid_version(
    br#"{"version":"invalid version","imports":"Before25"}"#,
    Some("invalid uploaded release version")
)]
#[case::unclassified(br#"{"version":"1.0"}"#, Some("are incomplete"))]
fn test_upload_admission_handles_projection_boundaries(#[case] record: &[u8], #[case] expected: Option<&str>) {
    let (_dir, meta) = store();
    mark_release_imports_initialized(&meta);

    let result = meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0-new.whl",
            record,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    );

    match expected {
        Some(expected) => {
            assert!(matches!(result, Err(MetaError::DriverPrecondition(message)) if message.contains(expected)));
        }
        None => assert!(result.unwrap()),
    }
}

#[test]
fn test_upload_admission_rejects_unclassified_incumbents() {
    let (_dir, meta) = store();
    meta.put_driver_value(
        &upload_key("hosted", "flask", "flask-1.0-old.whl"),
        br#"{"version":"1.0"}"#,
    )
    .unwrap();
    crate::store::initialize_release_imports(&meta, "hosted", "flask").unwrap();
    let record = import_record("1.0", &[], &[]);

    let result = meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0-new.whl",
            record: &record,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    );

    assert!(matches!(result, Err(MetaError::DriverPrecondition(message)) if message.contains("unclassified artifact")));
}

#[rstest]
#[case::invalid_json(json!("record"), "corrupt release import constraint")]
#[case::zero_bucket(
    json!({"live": 0, "unclassified": 0, "declarations": [{"declaration": "Before25", "count": 0}]}),
    "invalid declaration buckets",
)]
#[case::duplicate_bucket(
    json!({"live": 2, "unclassified": 0, "declarations": [
        {"declaration": "Before25", "count": 1},
        {"declaration": "Before25", "count": 1},
    ]}),
    "invalid declaration buckets",
)]
#[case::declaration_overflow(
    json!({"live": 0, "unclassified": 0, "declarations": [
        {"declaration": "Before25", "count": u64::MAX},
        {"declaration": {"V1": {"exclusive": null, "shared": null}}, "count": 1},
    ]}),
    "declaration count overflowed",
)]
#[case::count_overflow(
    json!({"live": 0, "unclassified": u64::MAX, "declarations": [
        {"declaration": "Before25", "count": 1},
    ]}),
    "count overflowed",
)]
#[case::count_mismatch(
    json!({"live": 2, "unclassified": 0, "declarations": [
        {"declaration": "Before25", "count": 1},
    ]}),
    "counts do not match",
)]
fn test_upload_admission_rejects_corrupt_release_constraints(
    #[case] constraint: serde_json::Value,
    #[case] expected: &str,
) {
    let (_dir, meta) = store();
    put_release_import_constraint(&meta, &constraint);
    let record = import_record("1.0", &[], &[]);

    let result = meta.publish_file_if(
        false,
        &PublishedFile {
            filename: "flask-1.0-new.whl",
            record: &record,
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    );

    assert!(matches!(result, Err(MetaError::DriverPrecondition(message)) if message.contains(expected)));
}

#[test]
fn test_upload_projection_rejects_a_live_count_overflow() {
    let (_dir, meta) = store();
    put_release_import_constraint(
        &meta,
        &json!({"live": u64::MAX, "unclassified": u64::MAX, "declarations": []}),
    );

    let error = meta
        .put_upload("hosted", "flask", "flask-1.0-new.whl", br#"{"version":"1.0"}"#)
        .unwrap_err();

    assert!(matches!(error, MetaError::DriverPrecondition(message) if message.contains("overflowed live count")));
}

#[test]
fn test_upload_projection_rejects_an_invalid_release_version() {
    let (_dir, meta) = store();
    mark_release_imports_initialized(&meta);

    let error = meta
        .put_upload(
            "hosted",
            "flask",
            "flask-invalid.whl",
            br#"{"version":"invalid version","imports":"Before25"}"#,
        )
        .unwrap_err();

    assert!(
        matches!(error, MetaError::DriverPrecondition(message) if message.contains("invalid uploaded release version"))
    );
}

#[rstest]
#[case::invalid_version(
    br#"{"version":"invalid version","imports":"Before25"}"#,
    json!({"live": 0, "unclassified": 0, "declarations": []}),
    "invalid uploaded release version",
)]
#[case::missing_bucket(
    br#"{"version":"1.0","imports":{"V1":{"exclusive":null,"shared":null}}}"#,
    json!({"live": 1, "unclassified": 0, "declarations": [{"declaration": "Before25", "count": 1}]}),
    "misses a declaration bucket",
)]
#[case::unclassified_underflow(
    br#"{"version":"1.0"}"#,
    json!({"live": 1, "unclassified": 0, "declarations": [{"declaration": "Before25", "count": 1}]}),
    "underflowed unclassified count",
)]
#[case::live_underflow(
    br#"{"version":"1.0","imports":"Before25"}"#,
    json!({"live": 0, "unclassified": 0, "declarations": []}),
    "underflowed live count",
)]
fn test_upload_projection_rejects_corrupt_removal_state(
    #[case] stored: &[u8],
    #[case] constraint: serde_json::Value,
    #[case] expected: &str,
) {
    let (_dir, meta) = store();
    meta.put_driver_value(&upload_key("hosted", "flask", "flask-1.0-old.whl"), stored)
        .unwrap();
    put_release_import_constraint(&meta, &constraint);

    let error = meta
        .delete_upload(false, "hosted", "flask", "flask-1.0-old.whl", 1)
        .unwrap_err();

    assert!(matches!(error, MetaError::DriverPrecondition(message) if message.contains(expected)));
}

#[test]
fn test_publish_file_if_commit_writes_record_sibling_project_and_serial() {
    let (_dir, meta) = store();

    let wrote = meta
        .publish_file_if(true, &published(), |stored| {
            assert_eq!(
                stored,
                PublishedState {
                    record: None,
                    provenance: None
                },
                "a first publish sees no prior record"
            );
            Ok::<_, MetaError>(Guard::Commit)
        })
        .unwrap();

    assert!(wrote);
    assert_eq!(
        meta.get_driver_value(&upload_key("hosted", "flask", "flask-1.0.whl"))
            .unwrap()
            .as_deref(),
        Some(PUBLISHED_RECORD)
    );
    assert!(
        meta.get_metadata_digest("artifact-sha").unwrap().is_some(),
        "the sibling row is written"
    );
    assert_eq!(meta.get_project("hosted", "flask").unwrap().as_deref(), Some("Flask"));
    let journal = read_journal_entries(&meta, 0, 2).unwrap().entries;
    assert_eq!(
        journal
            .iter()
            .map(|entry| (
                entry.action.as_str(),
                entry.filename.as_deref(),
                entry.python.as_deref()
            ))
            .collect::<Vec<_>>(),
        [
            ("new-release", None, None),
            // `flask-1.0.whl` carries no wheel tags, so there is no Python value to read off it and
            // the entry reports Warehouse's sdist default rather than inventing one.
            ("add-file", Some("flask-1.0.whl"), Some("source")),
        ]
    );
    assert!(journal.iter().all(|entry| entry.version.as_deref() == Some("1.0")));
    assert!(journal.iter().all(|entry| entry.submitted_at_unix == 123));
}

#[test]
fn test_publish_file_without_an_outbox_writes_no_journal() {
    let (_dir, meta) = store();

    let wrote = meta
        .publish_file_if(false, &published(), |_stored| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();

    assert!(wrote);
    assert_eq!(meta.current_serial().unwrap(), 0);
    assert!(read_journal_entries(&meta, 0, 1).unwrap().entries.is_empty());
}

#[test]
fn test_publish_file_if_commits_quota_with_a_new_record() {
    let (_dir, meta) = store();
    let reservation = reservation(&meta);

    let wrote = meta
        .publish_file_if(
            true,
            &PublishedFile {
                quota: Some(&reservation),
                ..published()
            },
            |_stored| Ok::<_, MetaError>(Guard::Commit),
        )
        .unwrap();

    assert!(wrote);
    assert_eq!(
        meta.quota_resource_usage("hosted", "flask")
            .unwrap()
            .artifact_bytes
            .committed,
        8
    );
}

#[test]
fn test_publish_file_if_releases_quota_for_a_duplicate() {
    let (_dir, meta) = store();
    let commit_if_missing = |stored: PublishedState<'_>| {
        Ok::<_, MetaError>(if stored.record.is_some() {
            Guard::Skip
        } else {
            Guard::Commit
        })
    };
    meta.publish_file_if(true, &published(), commit_if_missing).unwrap();
    let reservation = reservation(&meta);

    let wrote = meta
        .publish_file_if(
            true,
            &PublishedFile {
                quota: Some(&reservation),
                ..published()
            },
            commit_if_missing,
        )
        .unwrap();

    assert!(!wrote);
    assert_eq!(meta.quota_reservation(reservation.id).unwrap(), None);
    assert_eq!(
        meta.quota_resource_usage("hosted", "flask").unwrap().artifact_bytes,
        peryx_storage::meta::QuotaValue::default()
    );
}

#[test]
fn test_publish_file_if_leaves_quota_pending_after_a_guard_error() {
    let (_dir, meta) = store();
    let reservation = reservation(&meta);

    let result = meta.publish_file_if(
        true,
        &PublishedFile {
            quota: Some(&reservation),
            ..published()
        },
        |_stored| Err::<Guard, _>(MetaError::DriverPrecondition("conflict".to_owned())),
    );

    assert!(matches!(result, Err(MetaError::DriverPrecondition(reason)) if reason == "conflict"));
    assert_eq!(
        meta.quota_resource_usage("hosted", "flask")
            .unwrap()
            .artifact_bytes
            .reserved,
        8
    );
}

#[test]
fn test_publish_file_if_rejects_a_used_quota_reservation() {
    let (_dir, meta) = store();
    let reservation = reservation(&meta);
    meta.commit_quota_reservation(reservation.id).unwrap();

    let result = meta.publish_file_if(
        true,
        &PublishedFile {
            quota: Some(&reservation),
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    );

    assert!(matches!(result, Err(MetaError::DriverPrecondition(reason)) if reason.contains("already committed")));
    assert!(
        meta.get_driver_value(&upload_key("hosted", "flask", "flask-1.0.whl"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_publish_file_if_preserves_quota_store_errors() {
    let error =
        map_publish_error::<UploadStoreError>(PublishError::from(MetaError::DriverPrecondition("store".to_owned())));

    assert!(matches!(error, UploadStoreError::Meta(MetaError::DriverPrecondition(reason)) if reason == "store"));
}

#[test]
fn test_publish_file_if_preserves_driver_store_errors() {
    let error = map_publish_error::<MetaError>(PublishError::from(MetaError::DriverPrecondition("store".to_owned())));

    assert!(matches!(error, MetaError::DriverPrecondition(reason) if reason == "store"));
}

fn reservation(meta: &MetaStore) -> peryx_storage::meta::QuotaReservationRecord {
    meta.reserve_resource_quota(
        NewQuotaReservation {
            repository: "hosted",
            resource: Some("flask"),
            group: Some("1.0"),
            digest: "artifact-sha",
            bytes: 8,
            class: AccountingClass::Hosted,
            created_at_unix: 123,
        },
        QuotaLimits::default(),
        Some(8),
    )
    .unwrap()
}

#[test]
fn test_publish_file_if_commit_without_a_metadata_sibling_writes_no_sibling() {
    let (_dir, meta) = store();

    let wrote = meta
        .publish_file_if(
            true,
            &PublishedFile {
                metadata: None,
                ..published()
            },
            |_stored| Ok::<_, MetaError>(Guard::Commit),
        )
        .unwrap();

    assert!(wrote);
    assert!(
        meta.get_metadata_digest("artifact-sha").unwrap().is_none(),
        "a file without metadata records no sibling row"
    );
    assert_eq!(
        meta.get_driver_value(&upload_key("hosted", "flask", "flask-1.0.whl"))
            .unwrap()
            .as_deref(),
        Some(PUBLISHED_RECORD)
    );
}

#[test]
fn test_publish_file_if_records_artifact_and_metadata_blobs() {
    let (_dir, meta) = store();

    meta.publish_file_if(true, &published(), |_stored| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();

    assert_eq!(
        meta.journal_after(0, 2).unwrap().last().unwrap().blobs,
        vec![
            peryx_storage::meta::DriverBlobReference {
                sha256: "artifact-sha".to_owned(),
                size: 8,
            },
            peryx_storage::meta::DriverBlobReference {
                sha256: "metadata-sha".to_owned(),
                size: 8,
            },
        ]
    );
}

#[test]
fn test_publish_file_if_writes_the_provenance_row_and_references_its_blob() {
    let (_dir, meta) = store();

    meta.publish_file_if(
        true,
        &PublishedFile {
            provenance: Some(ProvenanceSibling {
                provenance_sha256: "provenance-sha",
                size: 16,
            }),
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    )
    .unwrap();

    assert_eq!(
        meta.get_provenance("hosted", "flask", "artifact-sha", "flask-1.0.whl")
            .unwrap(),
        Some(("provenance-sha".to_owned(), 16))
    );
    assert!(
        meta.journal_after(0, 2)
            .unwrap()
            .last()
            .unwrap()
            .blobs
            .contains(&peryx_storage::meta::DriverBlobReference {
                sha256: "provenance-sha".to_owned(),
                size: 16,
            }),
        "the provenance blob is recorded so a purge keeps it"
    );
}

#[test]
fn test_publish_file_if_scopes_the_provenance_row_to_the_publishing_index() {
    let (_dir, meta) = store();

    for (index, bundle) in [("hosted", "staging-bundle"), ("mirror", "mirror-bundle")] {
        meta.publish_file_if(
            true,
            &PublishedFile {
                index,
                provenance: Some(ProvenanceSibling {
                    provenance_sha256: bundle,
                    size: 16,
                }),
                ..published()
            },
            |_stored| Ok::<_, MetaError>(Guard::Commit),
        )
        .unwrap();
    }

    assert_eq!(
        meta.get_provenance("hosted", "flask", "artifact-sha", "flask-1.0.whl")
            .unwrap(),
        Some(("staging-bundle".to_owned(), 16)),
        "the second index publishing the same bytes does not replace the first index's bundle"
    );
    assert_eq!(
        meta.get_provenance("mirror", "flask", "artifact-sha", "flask-1.0.whl")
            .unwrap(),
        Some(("mirror-bundle".to_owned(), 16))
    );
}

#[test]
fn test_publish_file_if_shows_the_guard_the_publications_stored_bundle() {
    let (_dir, meta) = store();
    let with_bundle = PublishedFile {
        provenance: Some(ProvenanceSibling {
            provenance_sha256: "bundle-sha",
            size: 16,
        }),
        ..published()
    };
    meta.publish_file_if(true, &with_bundle, |_stored| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();

    meta.publish_file_if(true, &with_bundle, |stored| {
        assert_eq!(
            stored,
            PublishedState {
                record: Some(PUBLISHED_RECORD),
                provenance: Some("bundle-sha"),
            }
        );
        Ok::<_, MetaError>(Guard::Skip)
    })
    .unwrap();
}

#[test]
fn test_publish_file_if_without_provenance_writes_no_provenance_row() {
    let (_dir, meta) = store();

    meta.publish_file_if(true, &published(), |_stored| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();

    assert!(
        meta.get_provenance("hosted", "flask", "artifact-sha", "flask-1.0.whl")
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_publish_file_if_skip_leaves_the_store_unchanged() {
    let (_dir, meta) = store();

    let wrote = meta
        .publish_file_if(true, &published(), |_stored| Ok::<_, MetaError>(Guard::Skip))
        .unwrap();

    assert!(!wrote);
    assert!(
        meta.get_driver_value(&upload_key("hosted", "flask", "flask-1.0.whl"))
            .unwrap()
            .is_none()
    );
    assert_eq!(meta.current_serial().unwrap(), 0, "a skipped publish records no serial");
}

#[test]
fn test_publish_file_if_propagates_a_guard_rejection_without_writing() {
    let (_dir, meta) = store();

    let result = meta.publish_file_if(true, &published(), |_stored| {
        Err::<Guard, _>(MetaError::from(
            serde_json::from_str::<serde_json::Value>("{").unwrap_err(),
        ))
    });

    assert!(result.is_err());
    assert!(
        meta.get_driver_value(&upload_key("hosted", "flask", "flask-1.0.whl"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_promote_files_checked_writes_the_release_project_and_journal() {
    let (_dir, meta) = store();
    let records = vec![(
        "flask-1.0.whl".to_owned(),
        "artifact-sha".to_owned(),
        import_record("1.0", &[], &[]),
    )];
    let blob_sizes = BTreeMap::from([("artifact-sha".to_owned(), 8)]);

    let written = meta
        .promote_files_checked::<MetaError>(
            true,
            &PromotedRelease {
                source: "staging",
                index: "hosted",
                normalized: "flask",
                display: "Flask",
                records: &records,
                blob_sizes: &blob_sizes,
                reservations: &BTreeMap::new(),
                submitted_at_unix: 123,
            },
            |filename, digest, existing| {
                assert_eq!((filename, digest, existing), ("flask-1.0.whl", "artifact-sha", None));
                Ok::<_, MetaError>(Guard::Commit)
            },
        )
        .unwrap();

    assert_eq!(written, 1);
    assert_eq!(
        meta.get_driver_value(&upload_key("hosted", "flask", "flask-1.0.whl"))
            .unwrap()
            .as_deref(),
        Some(import_record("1.0", &[], &[]).as_slice())
    );
    assert_eq!(meta.get_project("hosted", "flask").unwrap().as_deref(), Some("Flask"));
    let batch = meta.journal_after(0, 2).unwrap().pop().unwrap();
    assert_eq!(
        batch.blobs,
        vec![peryx_storage::meta::DriverBlobReference {
            sha256: "artifact-sha".to_owned(),
            size: 8,
        }]
    );
    let journal = read_journal_entries(&meta, 0, 2).unwrap().entries;
    assert_eq!(
        journal
            .iter()
            .map(|entry| (entry.action.as_str(), entry.filename.as_deref()))
            .collect::<Vec<_>>(),
        [("new-release", None), ("add-file", Some("flask-1.0.whl"))]
    );
    assert!(journal.iter().all(|entry| entry.version.as_deref() == Some("1.0")));
    assert!(journal.iter().all(|entry| entry.submitted_at_unix == 123));
}

#[test]
fn test_promote_files_checked_copies_the_source_publications_bundle() {
    let (_dir, meta) = store();
    meta.publish_file_if(
        true,
        &PublishedFile {
            index: "staging",
            provenance: Some(ProvenanceSibling {
                provenance_sha256: "bundle-sha",
                size: 16,
            }),
            ..published()
        },
        |_stored| Ok::<_, MetaError>(Guard::Commit),
    )
    .unwrap();

    promote(&meta, "staging").unwrap();

    assert_eq!(
        meta.get_provenance("prod", "flask", "artifact-sha", "flask-1.0.whl")
            .unwrap(),
        Some(("bundle-sha".to_owned(), 16)),
        "the target publication inherits the bundle it was promoted with"
    );
    assert_eq!(
        meta.get_provenance("staging", "flask", "artifact-sha", "flask-1.0.whl")
            .unwrap(),
        Some(("bundle-sha".to_owned(), 16)),
        "the source publication keeps its own"
    );
    assert!(
        meta.journal_after(0, 2).unwrap()[1]
            .blobs
            .contains(&peryx_storage::meta::DriverBlobReference {
                sha256: "bundle-sha".to_owned(),
                size: 16,
            }),
        "the target holds its own reference so either deletion leaves the other readable"
    );
}

#[test]
fn test_promote_files_checked_writes_no_bundle_when_the_source_published_none() {
    let (_dir, meta) = store();

    promote(&meta, "staging").unwrap();

    assert_eq!(
        meta.get_provenance("prod", "flask", "artifact-sha", "flask-1.0.whl")
            .unwrap(),
        None
    );
}

#[test]
fn test_promote_files_checked_rejects_a_source_bundle_it_cannot_read() {
    let (_dir, meta) = store();
    meta.put_driver_value(
        &provenance_key("staging", "flask", "artifact-sha", "flask-1.0.whl"),
        b"bundle-sha",
    )
    .unwrap();

    let err = promote(&meta, "staging").unwrap_err();

    assert!(matches!(err, MetaError::DriverRecordMissing { field: "size", .. }));
    assert!(
        meta.get_driver_value(&upload_key("prod", "flask", "flask-1.0.whl"))
            .unwrap()
            .is_none(),
        "the promotion is abandoned rather than publishing a target with no readable bundle"
    );
}

fn promote(meta: &MetaStore, source: &str) -> Result<usize, MetaError> {
    let records = vec![(
        "flask-1.0.whl".to_owned(),
        "artifact-sha".to_owned(),
        import_record("1.0", &[], &[]),
    )];
    meta.promote_files_checked::<MetaError>(
        true,
        &PromotedRelease {
            source,
            index: "prod",
            normalized: "flask",
            display: "Flask",
            records: &records,
            blob_sizes: &BTreeMap::from([("artifact-sha".to_owned(), 8)]),
            reservations: &BTreeMap::new(),
            submitted_at_unix: 123,
        },
        |_filename, _digest, _existing| Ok::<_, MetaError>(Guard::Commit),
    )
}

#[test]
fn test_scan_upload_records_visits_each_row() {
    let (_dir, meta) = store();
    meta.put_upload("hosted", "flask", "flask-1.0.whl", b"upload").unwrap();
    let mut seen = Vec::new();
    meta.scan_upload_records(|key, value| {
        seen.push((key.to_owned(), value.to_vec()));
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    assert_eq!(
        seen,
        vec![("hosted/flask/flask-1.0.whl".to_owned(), b"upload".to_vec())]
    );
}

#[test]
fn test_scan_upload_records_propagates_store_errors_after_visiting_healthy_records() {
    let (_valid_directory, valid) = store();
    valid.put_upload("hosted", "flask", "flask-1.0.whl", b"upload").unwrap();
    let (_invalid_directory, invalid) = uninitialized_store();
    let mut seen = 0;
    let mut visit = |_key: &str, _value: &[u8]| {
        seen += 1;
        Ok::<(), std::convert::Infallible>(())
    };
    valid.scan_upload_records(&mut visit).unwrap();

    let error = invalid.scan_upload_records(&mut visit).unwrap_err();

    assert_eq!(seen, 1);
    assert!(matches!(error, peryx_storage::meta::MetaScanError::Store(_)));
}

#[test]
fn test_scan_upload_records_keeps_deleted_row_from_its_snapshot() {
    let (_dir, meta) = store();
    let first = import_record("1.0", &[], &[]);
    let last = import_record("2.0", &[], &[]);
    meta.put_upload("hosted", "flask", "a.whl", &first).unwrap();
    meta.put_upload("hosted", "flask", "z.whl", &last).unwrap();
    let (scan_started_tx, scan_started_rx) = sync_channel(0);
    let (delete_done_tx, delete_done_rx) = sync_channel(0);
    let mut seen = Vec::new();

    thread::scope(|scope| {
        let delete_meta = &meta;
        scope.spawn(move || {
            scan_started_rx.recv().unwrap();
            delete_meta
                .delete_upload(false, "hosted", "flask", "z.whl", 123)
                .unwrap();
            delete_done_tx.send(()).unwrap();
        });
        meta.scan_upload_records(|key, value| {
            seen.push((key.to_owned(), value.to_vec()));
            if key == "hosted/flask/a.whl" {
                scan_started_tx.send(()).unwrap();
                delete_done_rx.recv().unwrap();
            }
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
    });

    assert_eq!(
        seen,
        vec![
            ("hosted/flask/a.whl".to_owned(), first.clone()),
            ("hosted/flask/z.whl".to_owned(), last),
        ]
    );
    assert_eq!(
        meta.list_upload_entries("hosted", "flask").unwrap(),
        vec![("a.whl".to_owned(), first)]
    );
}

#[test]
fn test_scan_override_records_visits_each_record() {
    let (_dir, meta) = store();
    meta.set_override(
        true,
        "hosted",
        "flask",
        "flask-1.0.whl",
        OverrideMutation::Hidden(true),
        123,
    )
    .unwrap();
    let mut seen = Vec::new();
    meta.scan_override_records(|key, value| {
        seen.push((key.to_owned(), value.to_owned()));
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    assert_eq!(
        seen,
        vec![(
            "hosted/flask/flask-1.0.whl".to_owned(),
            r#"{"hidden":true,"yanked":false}"#.to_owned()
        )]
    );
}

#[test]
fn test_list_overrides_reports_a_missing_driver_table() {
    let (_directory, meta) = uninitialized_store();

    assert!(meta.list_overrides("hosted", "flask").is_err());
}

#[test]
fn test_scan_override_records_propagates_store_errors_after_visiting_healthy_records() {
    let (_valid_directory, valid) = store();
    valid
        .set_override(
            true,
            "hosted",
            "flask",
            "flask-1.0.whl",
            OverrideMutation::Hidden(true),
            123,
        )
        .unwrap();
    let (_invalid_directory, invalid) = uninitialized_store();
    let mut seen = 0;
    let mut visit = |_key: &str, _value: &str| {
        seen += 1;
        Ok::<(), std::convert::Infallible>(())
    };
    valid.scan_override_records(&mut visit).unwrap();

    let error = invalid.scan_override_records(&mut visit).unwrap_err();

    assert_eq!(seen, 1);
    assert!(matches!(error, peryx_storage::meta::MetaScanError::Store(_)));
}

#[test]
fn test_scan_override_records_propagates_the_visitor_error() {
    let (_dir, meta) = store();
    meta.set_override(
        true,
        "hosted",
        "flask",
        "flask-1.0.whl",
        OverrideMutation::Hidden(true),
        123,
    )
    .unwrap();

    let error = meta
        .scan_override_records(|_key, _value| Err(std::io::Error::other("stop")))
        .unwrap_err();

    assert_eq!(error.to_string(), "stop");
}

#[test]
fn test_mutate_uploads_journals_the_action_for_each_rewritten_record() {
    let (_dir, meta) = store();
    meta.put_upload("hosted", "flask", "flask-1.0.whl", &import_record("1.0", &[], &[]))
        .unwrap();
    meta.put_upload("hosted", "flask", "flask-2.0.whl", &import_record("2.0", &[], &[]))
        .unwrap();

    let changed = meta
        .mutate_uploads(true, "hosted", "flask", "yank", 123, |filename, _record| {
            Ok::<_, MetaError>(UploadMutation::Replace(import_record(
                if filename.contains("1.0") { "1.0" } else { "2.0" },
                &[],
                &[],
            )))
        })
        .unwrap();

    assert_eq!(changed, 2);
    assert_eq!(
        meta.current_serial().unwrap(),
        2,
        "each rewritten record allocates its own serial"
    );
    assert_eq!(
        read_journal_entries(&meta, 0, 2)
            .unwrap()
            .entries
            .into_iter()
            .map(|entry| (entry.action, entry.version, entry.filename))
            .collect::<Vec<_>>(),
        [
            (
                "yank".to_owned(),
                Some("1.0".to_owned()),
                Some("flask-1.0.whl".to_owned()),
            ),
            (
                "yank".to_owned(),
                Some("2.0".to_owned()),
                Some("flask-2.0.whl".to_owned()),
            ),
        ]
    );
}

#[test]
fn test_mutate_uploads_counts_rewrites_without_an_outbox() {
    let (_dir, meta) = store();
    let record = import_record("1.0", &[], &[]);
    meta.put_upload("hosted", "flask", "flask-1.0.whl", &record).unwrap();

    let changed = meta
        .mutate_uploads(false, "hosted", "flask", "yank", 123, |_filename, _record| {
            Ok::<_, MetaError>(UploadMutation::Replace(record.clone()))
        })
        .unwrap();

    assert_eq!(changed, 1);
    assert_eq!(
        meta.get_driver_value(&upload_key("hosted", "flask", "flask-1.0.whl"))
            .unwrap()
            .as_deref(),
        Some(record.as_slice()),
    );
    assert_eq!(meta.current_serial().unwrap(), 0);
}

#[test]
fn test_mutate_uploads_journals_only_the_removed_record_and_keeps_the_rest() {
    let (_dir, meta) = store();
    meta.put_upload("hosted", "flask", "flask-1.0.whl", &import_record("1.0", &[], &[]))
        .unwrap();
    meta.put_upload("hosted", "flask", "flask-2.0.whl", &import_record("2.0", &[], &[]))
        .unwrap();

    let changed = meta
        .mutate_uploads(true, "hosted", "flask", "delete-file", 123, |filename, _record| {
            Ok::<_, MetaError>(if filename == "flask-1.0.whl" {
                UploadMutation::Delete
            } else {
                UploadMutation::Keep
            })
        })
        .unwrap();

    assert_eq!(changed, 1);
    assert!(
        meta.get_driver_value(&upload_key("hosted", "flask", "flask-1.0.whl"))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        meta.current_serial().unwrap(),
        1,
        "only the removed record is journaled"
    );
    assert_eq!(
        read_journal_entries(&meta, 0, 1).unwrap().entries[0].version.as_deref(),
        Some("1.0")
    );
}

#[rstest::rstest]
#[case::replace("1.0", true, 1)]
#[case::delete("2.0", false, 1)]
#[case::keep("3.0", true, 0)]
fn test_mutate_uploads_applies_each_mutation(
    #[case] version: &str,
    #[case] expected: bool,
    #[case] expected_changes: usize,
) {
    let (_directory, meta) = store();
    let filename = format!("flask-{version}.whl");
    let record = import_record(version, &[], &[]);
    meta.put_upload("hosted", "flask", &filename, &record).unwrap();
    let mutate = |filename: &str, _record: &[u8]| {
        Ok::<_, MetaError>(match filename {
            "flask-1.0.whl" => UploadMutation::Replace(record.clone()),
            "flask-2.0.whl" => UploadMutation::Delete,
            _ => UploadMutation::Keep,
        })
    };

    let changed = meta
        .mutate_uploads(true, "hosted", "flask", "update", 123, mutate)
        .unwrap();

    assert_eq!(changed, expected_changes);
    assert_eq!(
        meta.get_driver_value(&upload_key("hosted", "flask", &filename))
            .unwrap()
            .is_some(),
        expected
    );
}

#[test]
fn test_mutate_uploads_that_keeps_every_record_journals_nothing() {
    let (_dir, meta) = store();
    meta.put_upload("hosted", "flask", "flask-1.0.whl", &import_record("1.0", &[], &[]))
        .unwrap();

    let changed = meta
        .mutate_uploads(true, "hosted", "flask", "yank", 123, keep_upload)
        .unwrap();

    assert_eq!(changed, 0);
    assert_eq!(meta.current_serial().unwrap(), 0, "an all-keep batch records no serial");
}

#[test]
fn test_delete_upload_removes_the_record_and_journals_delete_file() {
    let (_dir, meta) = store();
    meta.put_upload("hosted", "flask", "flask-1.0.whl", PUBLISHED_RECORD)
        .unwrap();

    let existed = meta
        .delete_upload(true, "hosted", "flask", "flask-1.0.whl", 123)
        .unwrap();

    assert!(existed);
    assert!(
        meta.get_driver_value(&upload_key("hosted", "flask", "flask-1.0.whl"))
            .unwrap()
            .is_none()
    );
    assert_eq!(meta.current_serial().unwrap(), 1, "the deletion is journaled");
    assert_eq!(
        read_journal_entries(&meta, 0, 1).unwrap().entries[0].version.as_deref(),
        Some("1.0")
    );
}

#[test]
fn test_delete_upload_releases_only_its_own_publications_bundle() {
    let (_dir, meta) = store();
    for filename in ["flask-1.0.whl", "flask-2.0.whl"] {
        meta.publish_file_if(
            true,
            &PublishedFile {
                filename,
                provenance: Some(ProvenanceSibling {
                    provenance_sha256: "bundle-sha",
                    size: 16,
                }),
                ..published()
            },
            |_stored| Ok::<_, MetaError>(Guard::Commit),
        )
        .unwrap();
    }

    meta.delete_upload(true, "hosted", "flask", "flask-1.0.whl", 123)
        .unwrap();

    assert_eq!(
        meta.get_provenance("hosted", "flask", "artifact-sha", "flask-1.0.whl")
            .unwrap(),
        None,
        "the deleted publication releases its bundle reference"
    );
    assert_eq!(
        meta.get_provenance("hosted", "flask", "artifact-sha", "flask-2.0.whl")
            .unwrap(),
        Some(("bundle-sha".to_owned(), 16)),
        "the surviving publication keeps its own"
    );
}

#[test]
fn test_delete_upload_of_a_missing_record_journals_nothing() {
    let (_dir, meta) = store();

    let existed = meta
        .delete_upload(true, "hosted", "flask", "flask-1.0.whl", 123)
        .unwrap();

    assert!(!existed);
    assert_eq!(meta.current_serial().unwrap(), 0, "a no-op delete records no serial");
}

#[rstest]
#[case::hide(OverrideMutation::Hidden(true), r#"{"hidden":true,"yanked":false}"#)]
#[case::yank(OverrideMutation::Yanked(&Yanked::Yes), r#"{"hidden":false,"yanked":true}"#)]
#[case::yank_with_a_reason(
    OverrideMutation::Yanked(&Yanked::Reason(String::from("CVE-2026-1234"))),
    r#"{"hidden":false,"yanked":"CVE-2026-1234"}"#
)]
fn test_set_override_stores_the_record_and_journals_it(#[case] mutation: OverrideMutation<'_>, #[case] stored: &str) {
    let (_dir, meta) = store();

    let changed = meta
        .set_override(true, "hosted", "flask", "flask-1.0.whl", mutation, 123)
        .unwrap();

    assert!(changed);
    assert_eq!(
        meta.get_driver_value(&override_key("hosted", "flask", "flask-1.0.whl"))
            .unwrap()
            .as_deref(),
        Some(stored.as_bytes())
    );
    assert_eq!(meta.current_serial().unwrap(), 1, "the override is journaled");
    assert_eq!(
        read_journal_entries(&meta, 0, 1).unwrap().entries[0].version.as_deref(),
        Some("1.0")
    );
}

#[test]
fn test_set_override_that_repeats_the_current_value_journals_nothing() {
    let (_dir, meta) = store();
    meta.set_override(
        true,
        "hosted",
        "flask",
        "flask-1.0.whl",
        OverrideMutation::Yanked(&Yanked::Yes),
        123,
    )
    .unwrap();

    let changed = meta
        .set_override(
            true,
            "hosted",
            "flask",
            "flask-1.0.whl",
            OverrideMutation::Yanked(&Yanked::Yes),
            456,
        )
        .unwrap();

    assert!(!changed);
    assert_eq!(
        meta.current_serial().unwrap(),
        1,
        "re-recording an identical override allocates no second serial"
    );
    assert_eq!(
        read_journal_entries(&meta, 0, 1).unwrap().entries[0].submitted_at_unix,
        123
    );
}

#[rstest]
#[case::restore(OverrideMutation::Hidden(true), OverrideMutation::Hidden(false))]
#[case::unyank(OverrideMutation::Yanked(&Yanked::Yes), OverrideMutation::Yanked(&Yanked::No))]
fn test_set_override_removes_a_record_that_imposes_nothing(
    #[case] impose: OverrideMutation<'_>,
    #[case] reverse: OverrideMutation<'_>,
) {
    let (_dir, meta) = store();
    meta.set_override(true, "hosted", "flask", "flask-1.0.whl", impose, 123)
        .unwrap();

    let changed = meta
        .set_override(true, "hosted", "flask", "flask-1.0.whl", reverse, 456)
        .unwrap();

    assert!(changed);
    assert!(
        meta.get_driver_value(&override_key("hosted", "flask", "flask-1.0.whl"))
            .unwrap()
            .is_none()
    );
    assert_eq!(meta.current_serial().unwrap(), 2, "the reversal is journaled");
}

#[test]
fn test_set_override_keeps_the_yank_of_a_hidden_file() {
    let (_dir, meta) = store();
    meta.set_override(
        true,
        "hosted",
        "flask",
        "flask-1.0.whl",
        OverrideMutation::Yanked(&Yanked::Reason(String::from("CVE-2026-1234"))),
        123,
    )
    .unwrap();
    meta.set_override(
        true,
        "hosted",
        "flask",
        "flask-1.0.whl",
        OverrideMutation::Hidden(true),
        124,
    )
    .unwrap();

    meta.set_override(
        true,
        "hosted",
        "flask",
        "flask-1.0.whl",
        OverrideMutation::Hidden(false),
        125,
    )
    .unwrap();

    let stored = meta.list_overrides("hosted", "flask").unwrap();
    assert_eq!(
        stored.get("flask-1.0.whl"),
        Some(&FileOverride {
            hidden: false,
            yanked: Yanked::Reason(String::from("CVE-2026-1234")),
        })
    );
}

#[test]
fn test_set_override_of_an_absent_record_that_changes_nothing_journals_nothing() {
    let (_dir, meta) = store();

    let changed = meta
        .set_override(
            true,
            "hosted",
            "flask",
            "flask-1.0.whl",
            OverrideMutation::Hidden(false),
            123,
        )
        .unwrap();

    assert!(!changed);
    assert_eq!(meta.current_serial().unwrap(), 0, "a no-op reversal records no serial");
}

#[test]
fn test_set_override_refuses_a_record_it_cannot_read_and_journals_nothing() {
    let (_dir, meta) = store();
    let key = override_key("hosted", "flask", "flask-1.0.whl");
    meta.put_driver_value(&key, b"hidden").unwrap();

    let error = meta
        .set_override(
            true,
            "hosted",
            "flask",
            "flask-1.0.whl",
            OverrideMutation::Hidden(true),
            123,
        )
        .unwrap_err();

    assert_eq!(
        (
            error.to_string(),
            meta.current_serial().unwrap(),
            meta.get_driver_value(&key).unwrap()
        ),
        (
            format!("driver record {key:?} does not decode"),
            0,
            Some(b"hidden".to_vec())
        )
    );
}

#[rstest]
#[case::unchanged(Some(r#"{"hidden":true,"yanked":false}"#), OverrideMutation::Hidden(true), 0)]
#[case::missing(None, OverrideMutation::Hidden(false), 0)]
#[case::changed(None, OverrideMutation::Hidden(true), 1)]
fn test_combined_mutation_reports_override_changes(
    #[case] stored: Option<&str>,
    #[case] mutation: OverrideMutation<'_>,
    #[case] expected: usize,
) {
    let (_dir, meta) = store();
    let filename = "flask-1.0.whl";
    meta.put_driver_value(
        &upload_key("hosted", "flask", filename),
        &import_record("1.0", &[], &[]),
    )
    .unwrap();
    crate::store::initialize_release_imports(&meta, "hosted", "flask").unwrap();
    let serial = meta.current_serial().unwrap();
    if let Some(stored) = stored {
        meta.put_driver_value(&override_key("hosted", "flask", filename), stored.as_bytes())
            .unwrap();
    }
    let webhook_calls = Cell::new(0);

    let changed = mutate_uploads_and_overrides(
        &meta,
        UploadMutationPlan {
            outbox: true,
            index: "hosted",
            normalized: "flask",
            action: "mutate",
            submitted_at_unix: 123,
            override_filenames: &[filename.to_owned()],
            override_mutation: mutation,
        },
        || Ok::<_, MetaError>(()),
        |_filename, _record| Ok::<_, MetaError>(None),
        |_| {
            webhook_calls.set(webhook_calls.get() + 1);
            None
        },
    )
    .unwrap();

    assert_eq!(changed, expected);
    assert_eq!(webhook_calls.get(), usize::from(expected > 0));
    assert_eq!(meta.current_serial().unwrap(), serial + expected as u64);
    assert_eq!(meta.next_webhook_event_id().unwrap(), None);
}

#[test]
fn test_mutating_rejects_an_unreadable_legacy_upload_during_initialization() {
    let (_dir, meta) = store();
    meta.put_driver_value(&upload_key("hosted", "flask", "flask-1.0.whl"), b"record")
        .unwrap();

    let error = meta
        .mutate_uploads(false, "hosted", "flask", "yank", 123, replace_with_published_record)
        .unwrap_err();

    assert!(matches!(error, MetaError::DriverPrecondition(message) if message.starts_with("corrupt uploaded record:")));
    assert_eq!(meta.current_serial().unwrap(), 0);
}

#[test]
fn test_mutating_rejects_a_corrupt_initialized_upload() {
    let (_dir, meta) = store();
    let key = upload_key("hosted", "flask", "flask-1.0.whl");
    meta.put_upload("hosted", "flask", "flask-1.0.whl", PUBLISHED_RECORD)
        .unwrap();
    meta.put_driver_value(&key, b"record").unwrap();

    let error = meta
        .mutate_uploads(false, "hosted", "flask", "yank", 123, keep_upload)
        .unwrap_err();

    assert!(matches!(error, MetaError::DriverPrecondition(message) if message.starts_with("corrupt uploaded record:")));
    assert_eq!(meta.get_driver_value(&key).unwrap(), Some(b"record".to_vec()));
}

#[derive(Clone, Copy)]
enum HostedMutation {
    Publish,
    Promote,
    Rewrite,
    Remove,
    Override,
    RewriteWithOverride,
}

fn apply_hosted_mutation(meta: &MetaStore, mutation: HostedMutation) {
    match mutation {
        HostedMutation::Publish => {
            meta.publish_file_if(true, &published(), |_stored| Ok::<_, MetaError>(Guard::Commit))
                .unwrap();
        }
        HostedMutation::Promote => {
            promote(meta, "staging").unwrap();
        }
        HostedMutation::Rewrite => {
            meta.mutate_uploads(true, "hosted", "flask", "yank", 123, replace_with_published_record)
                .unwrap();
        }
        HostedMutation::Remove => {
            meta.delete_upload(true, "hosted", "flask", "flask-1.0.whl", 123)
                .unwrap();
        }
        HostedMutation::Override => {
            meta.set_override(
                true,
                "hosted",
                "flask",
                "flask-1.0.whl",
                OverrideMutation::Hidden(true),
                123,
            )
            .unwrap();
        }
        HostedMutation::RewriteWithOverride => {
            mutate_uploads_and_overrides(
                meta,
                UploadMutationPlan {
                    outbox: true,
                    index: "hosted",
                    normalized: "flask",
                    action: "mutate",
                    submitted_at_unix: 123,
                    override_filenames: &["flask-1.0.whl".to_owned()],
                    override_mutation: OverrideMutation::Hidden(true),
                },
                || Ok::<_, MetaError>(()),
                |_filename, _record| Ok::<_, MetaError>(Some(PUBLISHED_RECORD.to_vec())),
                |_| None,
            )
            .unwrap();
        }
    }
}

fn revisions(meta: &MetaStore) -> [u64; 3] {
    ["hosted", "staging", "prod"].map(|index| meta.policy_input_generation(index).unwrap().repository)
}

#[rstest]
#[case::publish(HostedMutation::Publish, [1, 0, 0])]
#[case::promote(HostedMutation::Promote, [0, 0, 1])]
#[case::rewrite(HostedMutation::Rewrite, [1, 0, 0])]
#[case::remove(HostedMutation::Remove, [1, 0, 0])]
#[case::override_only(HostedMutation::Override, [1, 0, 0])]
#[case::rewrite_with_override(HostedMutation::RewriteWithOverride, [1, 0, 0])]
fn test_hosted_mutation_advances_only_its_own_repository(#[case] mutation: HostedMutation, #[case] expected: [u64; 3]) {
    let (_dir, meta) = store();
    for index in ["hosted", "staging", "prod"] {
        meta.put_upload(index, "flask", "flask-1.0.whl", PUBLISHED_RECORD)
            .unwrap();
    }
    let before = revisions(&meta);

    apply_hosted_mutation(&meta, mutation);

    assert_eq!((before, revisions(&meta)), ([0, 0, 0], expected));
}

fn wheel() -> PublishedFile<'static> {
    PublishedFile {
        filename: "flask-1.0-py3-none-any.whl",
        artifact_sha256: "wheel-sha",
        ..published()
    }
}

fn sdist() -> PublishedFile<'static> {
    PublishedFile {
        filename: "flask-1.0.tar.gz",
        artifact_sha256: "sdist-sha",
        metadata: None,
        ..published()
    }
}

fn journal_actions(meta: &MetaStore) -> Vec<(String, Option<String>, Option<String>)> {
    read_journal_entries(meta, 0, 16)
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| (entry.action, entry.filename, entry.python))
        .collect()
}

#[test]
fn test_publish_file_if_announces_the_release_before_the_file() {
    let (_dir, meta) = store();

    meta.publish_file_if(true, &sdist(), |_| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();
    meta.publish_file_if(true, &wheel(), |_| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();

    assert_eq!(
        journal_actions(&meta),
        [
            ("new-release".to_owned(), None, None),
            (
                "add-file".to_owned(),
                Some("flask-1.0.tar.gz".to_owned()),
                Some("source".to_owned())
            ),
            (
                "add-file".to_owned(),
                Some("flask-1.0-py3-none-any.whl".to_owned()),
                Some("py3".to_owned())
            ),
        ]
    );
}

#[test]
fn test_publish_file_if_announces_a_version_once_however_many_files_it_gains() {
    let (_dir, meta) = store();

    meta.publish_file_if(true, &wheel(), |_| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();
    meta.publish_file_if(true, &sdist(), |_| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();

    assert_eq!(
        journal_actions(&meta)
            .iter()
            .map(|(action, ..)| action.as_str())
            .collect::<Vec<_>>(),
        ["new-release", "add-file", "add-file"]
    );
}

#[test]
fn test_publish_file_if_keeps_a_release_announced_after_its_last_file_is_deleted() {
    let (_dir, meta) = store();
    meta.publish_file_if(true, &published(), |_| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();
    assert!(
        meta.delete_upload(true, "hosted", "flask", "flask-1.0.whl", 123)
            .unwrap()
    );

    meta.publish_file_if(true, &published(), |_| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();

    assert_eq!(
        journal_actions(&meta)
            .iter()
            .map(|(action, ..)| action.as_str())
            .collect::<Vec<_>>(),
        ["new-release", "add-file", "delete-file", "add-file"]
    );
}

#[test]
fn test_publish_file_if_announces_a_release_the_unjournaled_publication_left_unannounced() {
    let (_dir, meta) = store();

    meta.publish_file_if(false, &wheel(), |_| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();
    meta.publish_file_if(true, &sdist(), |_| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();

    assert_eq!(
        journal_actions(&meta)
            .iter()
            .map(|(action, ..)| action.as_str())
            .collect::<Vec<_>>(),
        ["new-release", "add-file"]
    );
}

#[test]
fn test_promote_files_checked_announces_a_release_once_across_its_files() {
    let (_dir, meta) = store();
    let records = vec![
        (
            "flask-1.0.tar.gz".to_owned(),
            "sdist-sha".to_owned(),
            import_record("1.0", &[], &[]),
        ),
        (
            "flask-1.0-py3-none-any.whl".to_owned(),
            "wheel-sha".to_owned(),
            import_record("1.0", &[], &[]),
        ),
    ];

    meta.promote_files_checked::<MetaError>(
        true,
        &PromotedRelease {
            source: "staging",
            index: "hosted",
            normalized: "flask",
            display: "Flask",
            records: &records,
            blob_sizes: &BTreeMap::new(),
            reservations: &BTreeMap::new(),
            submitted_at_unix: 123,
        },
        |_, _, _| Ok::<_, MetaError>(Guard::Commit),
    )
    .unwrap();

    assert_eq!(
        journal_actions(&meta),
        [
            ("new-release".to_owned(), None, None),
            (
                "add-file".to_owned(),
                Some("flask-1.0.tar.gz".to_owned()),
                Some("source".to_owned())
            ),
            (
                "add-file".to_owned(),
                Some("flask-1.0-py3-none-any.whl".to_owned()),
                Some("py3".to_owned())
            ),
        ]
    );
}

#[test]
fn test_promote_files_checked_rejects_a_record_that_names_no_version() {
    let (_dir, meta) = store();
    let records = vec![("README".to_owned(), "readme-sha".to_owned(), b"{}".to_vec())];

    let result = meta.promote_files_checked::<MetaError>(
        true,
        &PromotedRelease {
            source: "staging",
            index: "hosted",
            normalized: "flask",
            display: "Flask",
            records: &records,
            blob_sizes: &BTreeMap::new(),
            reservations: &BTreeMap::new(),
            submitted_at_unix: 123,
        },
        |_, _, _| Ok::<_, MetaError>(Guard::Commit),
    );

    assert!(
        matches!(result, Err(MetaError::DriverPrecondition(message)) if message.starts_with("corrupt uploaded record:"))
    );
    assert!(journal_actions(&meta).is_empty());
}

/// Nothing in production removes an upload record today: the served `DELETE` trashes it and retention
/// only plans. These drive the store API so the project row keeps step with the last record whenever
/// an apply phase does arrive.
#[test]
fn test_removing_the_last_upload_takes_the_projects_row_with_it() {
    let (_dir, meta) = store();
    meta.publish_file_if(true, &published(), |_| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();
    assert_eq!(meta.get_project("hosted", "flask").unwrap().as_deref(), Some("Flask"));

    meta.delete_upload(true, "hosted", "flask", "flask-1.0.whl", 123)
        .unwrap();

    assert_eq!(
        (
            meta.get_project("hosted", "flask").unwrap(),
            meta.list_projects("hosted").unwrap()
        ),
        (None, Vec::new())
    );
}

#[test]
fn test_publishing_after_the_last_upload_went_writes_the_new_spelling() {
    let (_dir, meta) = store();
    meta.publish_file_if(true, &published(), |_| Ok::<_, MetaError>(Guard::Commit))
        .unwrap();
    meta.delete_upload(true, "hosted", "flask", "flask-1.0.whl", 123)
        .unwrap();

    meta.publish_file_if(
        true,
        &PublishedFile {
            display: "FLASK",
            ..published()
        },
        |_| Ok::<_, MetaError>(Guard::Commit),
    )
    .unwrap();

    assert_eq!(meta.get_project("hosted", "flask").unwrap().as_deref(), Some("FLASK"));
}

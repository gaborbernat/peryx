use super::*;

#[test]
fn test_cache_error_converts_to_its_user_message_string() {
    assert_eq!(
        String::from(CacheError::Unavailable),
        "upstream is unavailable and no cached page exists"
    );
}

#[test]
fn test_cache_error_archive_message_is_user_visible() {
    assert_eq!(
        CacheError::Archive(crate::archive::ArchiveError::Unsupported).user_message(),
        "unsupported archive type"
    );
}

#[test]
fn test_freshness_secs_clamps_a_negative_ttl_to_zero() {
    assert_eq!(freshness_secs(-1, None), 0);
}

#[test]
fn test_freshness_secs_clamps_a_negative_granted_lifetime_to_zero() {
    assert_eq!(freshness_secs(10, Some(-5)), 0);
}

#[test]
fn test_freshness_secs_honors_a_shorter_granted_lifetime() {
    assert_eq!(freshness_secs(60, Some(10)), 10);
}

#[test]
fn test_freshness_secs_caps_a_longer_granted_lifetime_at_the_ttl() {
    assert_eq!(freshness_secs(60, Some(120)), 60);
}

#[test]
fn test_cache_error_maps_upload_store_errors() {
    let err = upload::UploadStoreError::Meta(peryx_storage::meta::MetaError::Decode(
        serde_json::from_str::<serde_json::Value>("{").unwrap_err(),
    ));
    assert!(matches!(CacheError::from(err), CacheError::Meta(_)));

    let err = upload::UploadStoreError::Blob(peryx_storage::blob::BlobError::not_found(
        &peryx_storage::blob::Digest::of(b"missing"),
    ));
    assert!(matches!(CacheError::from(err), CacheError::Blob(_)));

    let err = upload::UploadStoreError::Parse(serde_json::from_str::<serde_json::Value>("{").unwrap_err());
    assert!(matches!(CacheError::from(err), CacheError::Parse(_)));
    assert!(matches!(
        CacheError::from(upload::UploadStoreError::FileExists("file".to_owned())),
        CacheError::FileExists(filename) if filename == "file"
    ));
    assert!(matches!(
        CacheError::from(upload::UploadStoreError::ProvenanceMismatch("file".to_owned())),
        CacheError::ProvenanceMismatch(filename) if filename == "file"
    ));
    assert!(matches!(
        CacheError::from(upload::UploadStoreError::ReleaseImports("imports".to_owned())),
        CacheError::ReleaseImports(message) if message == "imports"
    ));
    assert!(matches!(
        CacheError::from(upload::UploadStoreError::ConcurrentChange("retry".to_owned())),
        CacheError::ConcurrentChange(message) if message == "retry"
    ));
    assert!(matches!(
        CacheError::from(upload::UploadStoreError::MissingSha256("file".to_owned())),
        CacheError::MissingSha256(filename) if filename == "file"
    ));
}

#[test]
fn test_cache_error_maps_typed_upload_writes() {
    assert!(matches!(
        CacheError::from(crate::store::UploadWriteError::Meta(
            peryx_storage::meta::MetaError::DriverPrecondition("store".to_owned())
        )),
        CacheError::Meta(_)
    ));
    assert!(matches!(
        CacheError::from(crate::store::UploadWriteError::ReleaseImports("imports".to_owned())),
        CacheError::ReleaseImports(message) if message == "imports"
    ));
    assert_eq!(
        CacheError::ReleaseImports("imports".to_owned()).user_message(),
        "imports"
    );
    assert_eq!(CacheError::ConcurrentChange("retry".to_owned()).user_message(), "retry");
}

#[tokio::test]
async fn test_archive_metadata_task_reports_a_panicking_worker() {
    let task = tokio::task::spawn_blocking(|| -> Result<Option<Vec<u8>>, upload::LegacyMetadataError> {
        panic!("worker failed")
    });

    assert!(matches!(
        mutate::join_archive_metadata(task).await,
        Err(CacheError::Meta(_))
    ));
}

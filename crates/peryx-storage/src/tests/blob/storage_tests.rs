use crate::blob::BlobStorage;

#[test]
fn test_filesystem_backend_id_matches_its_name() {
    let dir = tempfile::tempdir().unwrap();
    let storage = BlobStorage::filesystem(dir.path());
    assert_eq!(storage.backend_id().as_str(), storage.name());
    assert_eq!(storage.backend_id().as_str(), "filesystem");
}

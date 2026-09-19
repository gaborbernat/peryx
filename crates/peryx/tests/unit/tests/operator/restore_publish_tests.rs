use std::path::Path;

use peryx_storage::blob::Digest;
use rstest::rstest;

#[cfg(unix)]
use super::cleanup_restore_failure;
use super::{ensure_blob_copy_matches, ensure_copy_matches, publish, rollback_publish, sibling_path, staging_path};
use crate::operator::ManifestFile;

#[test]
fn test_publish_restores_the_prior_target_when_the_swap_fails() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("data");
    std::fs::create_dir(&target).unwrap();
    std::fs::write(target.join("marker"), b"old").unwrap();
    let missing_staging = staging_path(&target).unwrap();

    let err = publish(&missing_staging, &target).unwrap_err();

    assert!(err.to_string().contains("publish restored data"), "{err}");
    assert_eq!(std::fs::read(target.join("marker")).unwrap(), b"old");
    assert!(!root.path().join("data.restore-old").exists());
}

#[rstest]
#[case::new_target(false)]
#[case::replacement(true)]
fn test_publish_replaces_the_target(#[case] target_exists: bool) {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("data");
    let staging = root.path().join("staging");
    std::fs::create_dir(&staging).unwrap();
    std::fs::write(staging.join("marker"), b"new").unwrap();
    if target_exists {
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("marker"), b"old").unwrap();
    }

    publish(&staging, &target).unwrap();

    assert_eq!(std::fs::read(target.join("marker")).unwrap(), b"new");
    assert!(!root.path().join("data.restore-old").exists());
}

#[cfg(unix)]
#[rstest]
#[case::new_target(false)]
#[case::replacement(true)]
fn test_publish_reports_a_parent_sync_failure(#[case] target_exists: bool) {
    use std::os::unix::fs::PermissionsExt as _;

    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("data");
    let staging = root.path().join("staging");
    std::fs::create_dir(&staging).unwrap();
    std::fs::write(staging.join("marker"), b"new").unwrap();
    if target_exists {
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("marker"), b"old").unwrap();
    }
    let permissions = std::fs::metadata(root.path()).unwrap().permissions();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o333)).unwrap();

    let error = publish(&staging, &target).unwrap_err();

    std::fs::set_permissions(root.path(), permissions).unwrap();
    assert!(
        error.to_string().contains("sync restored data parent directory"),
        "{error:#}"
    );
    assert_eq!(std::fs::read(target.join("marker")).unwrap(), b"new");
    if target_exists {
        assert_eq!(
            std::fs::read(root.path().join("data.restore-old/marker")).unwrap(),
            b"old"
        );
    }
}

#[test]
fn test_rollback_publish_reports_a_failed_rollback() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("data");

    let error = rollback_publish(&root.path().join("missing"), &target, anyhow::anyhow!("publish failed"));

    assert!(format!("{error:#}").contains("restore original target"), "{error:#}");
}

#[cfg(unix)]
#[test]
fn test_cleanup_restore_failure_reports_a_failed_cleanup() {
    use std::os::unix::fs::PermissionsExt as _;

    let root = tempfile::tempdir().unwrap();
    let staging = root.path().join("staging");
    std::fs::create_dir(&staging).unwrap();
    let permissions = std::fs::metadata(root.path()).unwrap().permissions();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o500)).unwrap();

    let error = cleanup_restore_failure(&staging, anyhow::anyhow!("restore failed"));

    std::fs::set_permissions(root.path(), permissions).unwrap();
    assert!(format!("{error:#}").contains("clean restore staging"), "{error:#}");
}

#[rstest]
#[case::digest("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 1)]
#[case::size("0000000000000000000000000000000000000000000000000000000000000000", 2)]
fn test_blob_copy_rejects_changes_after_verification(#[case] sha256: &str, #[case] size_bytes: u64) {
    let digest = Digest::from_hex("0000000000000000000000000000000000000000000000000000000000000000").unwrap();
    let actual = ManifestFile {
        path: "blobs/00/00".to_owned(),
        sha256: sha256.to_owned(),
        size_bytes,
    };

    let error = ensure_blob_copy_matches(&actual, &digest, 1).unwrap_err();

    assert!(error.to_string().contains("changed after verification"), "{error}");
}

/// The metadata and config members are hashed once by verification and again as they are copied;
/// a member that changed in between is refused rather than published, and the refusal names which.
#[rstest]
#[case::digest("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 1)]
#[case::size("0000000000000000000000000000000000000000000000000000000000000000", 2)]
fn test_member_copy_rejects_changes_after_verification(#[case] sha256: &str, #[case] size_bytes: u64) {
    let expected = ManifestFile {
        path: "metadata/peryx.redb".to_owned(),
        sha256: "0".repeat(64),
        size_bytes: 1,
    };
    let actual = ManifestFile {
        path: expected.path.clone(),
        sha256: sha256.to_owned(),
        size_bytes,
    };

    let error = ensure_copy_matches(&actual, &expected, "metadata").unwrap_err();

    assert_eq!(error.to_string(), "backup metadata changed after verification");
}

#[test]
fn test_sibling_path_reports_a_parentless_target() {
    let err = sibling_path(Path::new("/"), ".restore-staging").unwrap_err();

    assert!(err.to_string().contains("no final path component"), "{err}");
}

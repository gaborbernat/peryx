use std::io::ErrorKind;
use std::time::Duration;

use super::discard_stage_with;
use super::remove_pending;
use super::remove_pending_with;

fn denied(_: &std::path::Path, _: &std::path::Path) -> Result<(), std::io::Error> {
    Err(std::io::Error::from(ErrorKind::PermissionDenied))
}

fn close(path: tempfile::TempPath) -> Result<(), std::io::Error> {
    path.close()
}

fn close_denied(_: tempfile::TempPath) -> Result<(), std::io::Error> {
    Err(std::io::Error::from(ErrorKind::PermissionDenied))
}

#[test]
fn test_remove_pending_treats_a_missing_stage_as_removed() {
    let dir = tempfile::tempdir().unwrap();
    assert!(remove_pending(&dir.path().join("absent")).is_ok());
}

#[test]
fn test_remove_pending_backs_off_then_reports_a_persistent_denial() {
    let mut waits = Vec::new();
    let result = remove_pending_with(
        std::path::Path::new("stage"),
        |_| Err(std::io::Error::from(ErrorKind::PermissionDenied)),
        |backoff| waits.push(backoff),
    );

    assert_eq!(result.unwrap_err().kind(), crate::blob::BlobErrorKind::Io);
    assert_eq!(waits, [1, 2, 4, 8, 16, 32].map(Duration::from_millis));
}

#[test]
fn test_discard_stage_falls_back_when_rename_is_denied() {
    let dir = tempfile::tempdir().unwrap();
    let (_file, path) = tempfile::NamedTempFile::new_in(&dir).unwrap().into_parts();
    let original = path.to_path_buf();

    discard_stage_with(path, denied, close).unwrap();

    assert!(!original.exists());

    let (_file, path) = tempfile::NamedTempFile::new_in(&dir).unwrap().into_parts();
    assert_eq!(
        discard_stage_with(path, denied, close_denied).unwrap_err().kind(),
        crate::blob::BlobErrorKind::Io
    );
}

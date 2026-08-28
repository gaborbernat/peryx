use std::io::ErrorKind;

use rstest::rstest;

use super::{lease_lock_available, open_lease};

#[test]
fn test_open_lease_distinguishes_missing_and_invalid_paths() {
    let dir = tempfile::tempdir().unwrap();
    assert!(open_lease(&dir.path().join("missing")).unwrap().is_none());
    let parent = dir.path().join("file");
    std::fs::write(&parent, []).unwrap();
    assert_eq!(
        open_lease(&parent.join("child")).unwrap_err().kind(),
        crate::blob::BlobErrorKind::Io
    );
}

#[rstest]
#[case::available(Ok(()), Ok(true))]
#[case::contended(Err(fs4::TryLockError::WouldBlock), Ok(false))]
#[case::failed(
    Err(fs4::TryLockError::Error(std::io::Error::from(ErrorKind::PermissionDenied))),
    Err(ErrorKind::PermissionDenied)
)]
fn test_lease_lock_result_preserves_each_outcome(
    #[case] result: Result<(), fs4::TryLockError>,
    #[case] expected: Result<bool, ErrorKind>,
) {
    assert_eq!(lease_lock_available(result).map_err(|error| error.kind()), expected);
}

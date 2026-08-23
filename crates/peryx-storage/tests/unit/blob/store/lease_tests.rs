use std::io::ErrorKind;

use rstest::rstest;

use super::lease_lock_available;

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

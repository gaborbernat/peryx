use std::path::Path;

use rstest::rstest;

use crate::operator;

use super::{blob_relpath, valid_backup};

#[rstest]
#[case::non_empty_directory(create_non_empty_directory, "not empty")]
#[case::file(create_file, "exists and is not a directory")]
fn test_restore_retries_occupied_target_with_force(#[case] create: fn(&Path), #[case] expected: &str) {
    let (_source, root, _config, backup, _content_digest, _metadata_digest) = valid_backup();
    let restored = root.path().join("restored");
    create(&restored);

    let err = operator::restore(&backup, &restored, false, &mut Vec::new()).unwrap_err();
    assert!(err.to_string().contains(expected));

    operator::restore(&backup, &restored, true, &mut Vec::new()).unwrap();
    assert_eq!(
        (
            restored.is_dir(),
            restored.join("peryx.redb").is_file(),
            restored.join("blocker").exists()
        ),
        (true, true, false)
    );
}

#[test]
fn test_restore_refuses_backup_with_verification_errors() {
    let (_source, root, _config, backup, content_digest, _metadata_digest) = valid_backup();
    std::fs::remove_file(backup.join(blob_relpath(&content_digest))).unwrap();

    let err = operator::restore(&backup, &root.path().join("restored"), false, &mut Vec::new()).unwrap_err();

    assert!(err.to_string().contains("backup verification failed"));
    assert!(err.to_string().contains("problem\tblob"));
}

#[test]
fn test_restore_warns_when_config_data_dir_changes() {
    let (_source, root, _config, backup, _content_digest, _metadata_digest) = valid_backup();
    let restored = root.path().join("restored");

    let mut out = Vec::new();
    operator::restore(&backup, &restored, false, &mut out).unwrap();

    assert!(String::from_utf8(out).unwrap().contains("warning\tconfig\tdata_dir"));
}

#[test]
fn test_restore_skips_config_warning_when_data_dir_matches() {
    let (_source, _root, config, backup, _content_digest, _metadata_digest) = valid_backup();

    let mut out = Vec::new();
    let err = operator::restore(&backup, &config.data_dir, false, &mut out).unwrap_err();

    assert!(err.to_string().contains("not empty"));
    assert!(!String::from_utf8(out).unwrap().contains("warning\tconfig\tdata_dir"));
}

fn create_non_empty_directory(path: &Path) {
    std::fs::create_dir_all(path).unwrap();
    std::fs::write(path.join("blocker"), b"x").unwrap();
}

fn create_file(path: &Path) {
    std::fs::write(path, b"x").unwrap();
}

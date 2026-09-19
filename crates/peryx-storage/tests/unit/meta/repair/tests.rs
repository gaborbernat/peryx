use super::{MetaError, RepairScan};

#[test]
fn test_is_incomplete_reflects_whether_a_row_was_skipped() {
    let mut scan = RepairScan::default();
    assert!(!scan.is_incomplete());

    let decode = serde_json::from_slice::<serde_json::Value>(b"not json").unwrap_err();
    scan.skip("key", MetaError::Decode(decode));

    assert!(scan.is_incomplete());
    assert_eq!(scan.corrupt().len(), 1);
    assert_eq!(scan.corrupt()[0].key, "key");
}

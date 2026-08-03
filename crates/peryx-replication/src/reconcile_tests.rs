use crate::reconcile::{Disposition, OldEpochOp, classify};

fn op(durably_committed: bool, already_applied: bool, superseded: bool) -> OldEpochOp {
    OldEpochOp {
        durably_committed,
        already_applied,
        superseded,
    }
}

#[test]
fn test_uncommitted_op_fails() {
    assert_eq!(classify(&op(false, false, false)), Disposition::Failed);
}

#[test]
fn test_applied_op_is_already_applied() {
    assert_eq!(classify(&op(true, true, false)), Disposition::AlreadyApplied);
}

#[test]
fn test_superseded_op_is_superseded() {
    assert_eq!(classify(&op(true, false, true)), Disposition::Superseded);
}

#[test]
fn test_durable_unapplied_unsuperseded_op_is_replayable() {
    assert_eq!(classify(&op(true, false, false)), Disposition::Replayable);
}

#[test]
fn test_already_applied_outranks_superseded() {
    assert_eq!(classify(&op(true, true, true)), Disposition::AlreadyApplied);
}

#[test]
fn test_not_committed_outranks_everything() {
    assert_eq!(classify(&op(false, true, true)), Disposition::Failed);
}

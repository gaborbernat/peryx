use crate::meta::{
    MetaStore, OperationClaim, OperationOutcomeError, OperationOutcomeRecord, OperationResult, OperationState,
};

fn store() -> (tempfile::TempDir, MetaStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    (dir, store)
}

fn pending(expiry_unix: Option<i64>, now: i64) -> OperationOutcomeRecord {
    OperationOutcomeRecord {
        state: OperationState::Pending,
        response: Vec::new(),
        expiry_unix,
        updated_at_unix: now,
    }
}

#[test]
fn test_claim_admits_a_fresh_operation() {
    let (_dir, store) = store();

    assert_eq!(
        store.claim_operation("op-1", Some(10), 1).unwrap(),
        OperationClaim::Admitted
    );
    assert_eq!(store.operation_outcome("op-1").unwrap(), Some(pending(Some(10), 1)));
}

#[test]
fn test_a_second_claim_returns_the_existing_record() {
    let (_dir, store) = store();
    store.claim_operation("op-1", Some(10), 1).unwrap();

    // A retry of the same id finds the in-flight record rather than admitting a second mutation.
    assert_eq!(
        store.claim_operation("op-1", Some(999), 2).unwrap(),
        OperationClaim::Existing(pending(Some(10), 1))
    );
}

#[test]
fn test_finalize_stamps_the_terminal_result_and_response() {
    let (_dir, store) = store();
    store.claim_operation("op-1", Some(10), 1).unwrap();

    let record = store
        .finalize_operation("op-1", OperationResult::Published, b"serial-7", 5)
        .unwrap();

    assert_eq!(
        record,
        OperationOutcomeRecord {
            state: OperationState::Published,
            response: b"serial-7".to_vec(),
            expiry_unix: Some(10),
            updated_at_unix: 5,
        }
    );
    assert_eq!(store.operation_outcome("op-1").unwrap(), Some(record));
}

#[test]
fn test_finalize_records_a_failure() {
    let (_dir, store) = store();
    store.claim_operation("op-1", None, 1).unwrap();

    let record = store
        .finalize_operation("op-1", OperationResult::Failed, b"quota exceeded", 5)
        .unwrap();

    assert_eq!(record.state, OperationState::Failed);
    assert_eq!(record.response, b"quota exceeded");
}

#[test]
fn test_retry_after_finalize_replays_the_original_result() {
    let (_dir, store) = store();
    store.claim_operation("op-1", Some(10), 1).unwrap();
    let finalized = store
        .finalize_operation("op-1", OperationResult::Published, b"serial-7", 5)
        .unwrap();

    // The retry sees the finalized record, so it replays the original response and never remutates.
    assert_eq!(
        store.claim_operation("op-1", Some(10), 9).unwrap(),
        OperationClaim::Existing(finalized)
    );
}

#[test]
fn test_finalizing_an_unclaimed_operation_is_rejected() {
    let (_dir, store) = store();

    let error = store
        .finalize_operation("ghost", OperationResult::Published, b"", 1)
        .unwrap_err();

    assert!(matches!(error, OperationOutcomeError::NotAdmitted { operation } if operation == "ghost"));
}

#[test]
fn test_finalizing_a_terminal_operation_is_rejected() {
    let (_dir, store) = store();
    store.claim_operation("op-1", None, 1).unwrap();
    store
        .finalize_operation("op-1", OperationResult::Published, b"first", 5)
        .unwrap();

    let error = store
        .finalize_operation("op-1", OperationResult::Failed, b"second", 6)
        .unwrap_err();

    assert!(matches!(error, OperationOutcomeError::AlreadyFinal { operation } if operation == "op-1"));
}

#[test]
fn test_outcome_is_none_for_an_unknown_operation() {
    let (_dir, store) = store();
    assert_eq!(store.operation_outcome("unknown").unwrap(), None);
}

#[test]
fn test_error_messages_name_the_operation() {
    assert_eq!(
        OperationOutcomeError::NotAdmitted {
            operation: "op-1".to_owned()
        }
        .to_string(),
        "operation op-1 was never admitted"
    );
    assert_eq!(
        OperationOutcomeError::AlreadyFinal {
            operation: "op-1".to_owned()
        }
        .to_string(),
        "operation op-1 is already finalized"
    );
}

#[test]
fn test_state_reports_whether_it_is_terminal() {
    assert!(!OperationState::Pending.is_terminal());
    assert!(OperationState::Published.is_terminal());
    assert!(OperationState::Failed.is_terminal());
}

#[test]
fn test_prune_removes_only_expired_terminal_records() {
    let (_dir, store) = store();
    // Pending: never pruned. Terminal unexpired: kept. Terminal with no expiry: kept. Terminal expired:
    // removed.
    store.claim_operation("pending", Some(10), 1).unwrap();
    store.claim_operation("unexpired", Some(100), 1).unwrap();
    store
        .finalize_operation("unexpired", OperationResult::Published, b"", 2)
        .unwrap();
    store.claim_operation("no-expiry", None, 1).unwrap();
    store
        .finalize_operation("no-expiry", OperationResult::Published, b"", 2)
        .unwrap();
    store.claim_operation("expired", Some(10), 1).unwrap();
    store
        .finalize_operation("expired", OperationResult::Failed, b"", 2)
        .unwrap();

    let pruned = store.prune_operation_outcomes(50, 10).unwrap();

    assert_eq!(pruned, 1);
    assert_eq!(store.operation_outcome("expired").unwrap(), None);
    assert!(store.operation_outcome("pending").unwrap().is_some());
    assert!(store.operation_outcome("unexpired").unwrap().is_some());
    assert!(store.operation_outcome("no-expiry").unwrap().is_some());
}

#[test]
fn test_prune_honors_the_limit() {
    let (_dir, store) = store();
    for id in ["a", "b"] {
        store.claim_operation(id, Some(10), 1).unwrap();
        store
            .finalize_operation(id, OperationResult::Published, b"", 2)
            .unwrap();
    }

    let pruned = store.prune_operation_outcomes(50, 1).unwrap();

    assert_eq!(pruned, 1);
    // One expired record still remains after the bounded pass.
    let remaining = ["a", "b"]
        .iter()
        .filter(|id| store.operation_outcome(id).unwrap().is_some())
        .count();
    assert_eq!(remaining, 1);
}

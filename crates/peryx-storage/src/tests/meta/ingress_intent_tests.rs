use crate::meta::{IntentPhase, IntentStageOutcome, IntentTransition, MetaStore, StagedIntent};

fn store() -> (tempfile::TempDir, MetaStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    (dir, store)
}

fn pending(digest: &str, size: u64, payload: &[u8], now: i64) -> StagedIntent {
    StagedIntent {
        phase: IntentPhase::Pending,
        digest: digest.to_owned(),
        size,
        payload: payload.to_vec(),
        updated_at_unix: now,
    }
}

#[test]
fn test_stage_admits_a_new_intent() {
    let (_dir, store) = store();

    assert_eq!(
        store.stage_intent("key-1", "digest-a", 10, b"intent", 5, 1).unwrap(),
        IntentStageOutcome::Admitted
    );
    assert_eq!(
        store.staged_intent("key-1").unwrap(),
        Some(pending("digest-a", 10, b"intent", 1))
    );
    assert_eq!(store.count_staged_intents().unwrap(), 1);
}

#[test]
fn test_restaging_the_same_content_is_a_duplicate() {
    let (_dir, store) = store();
    store.stage_intent("key-1", "digest-a", 10, b"first", 5, 1).unwrap();

    assert_eq!(
        store.stage_intent("key-1", "digest-a", 10, b"second", 5, 2).unwrap(),
        IntentStageOutcome::Duplicate
    );
    // The first admission stands: neither the payload nor the count changed.
    assert_eq!(
        store.staged_intent("key-1").unwrap(),
        Some(pending("digest-a", 10, b"first", 1))
    );
    assert_eq!(store.count_staged_intents().unwrap(), 1);
}

#[test]
fn test_restaging_a_different_digest_is_a_conflict() {
    let (_dir, store) = store();
    store.stage_intent("key-1", "digest-a", 10, b"first", 5, 1).unwrap();

    assert_eq!(
        store.stage_intent("key-1", "digest-b", 10, b"second", 5, 2).unwrap(),
        IntentStageOutcome::Conflict
    );
    assert_eq!(store.staged_intent("key-1").unwrap().unwrap().digest, "digest-a");
}

#[test]
fn test_restaging_a_different_size_is_a_conflict() {
    let (_dir, store) = store();
    store.stage_intent("key-1", "digest-a", 10, b"first", 5, 1).unwrap();

    assert_eq!(
        store.stage_intent("key-1", "digest-a", 20, b"second", 5, 2).unwrap(),
        IntentStageOutcome::Conflict
    );
}

#[test]
fn test_a_new_key_past_the_limit_is_rejected_but_a_duplicate_is_not() {
    let (_dir, store) = store();
    assert_eq!(
        store.stage_intent("key-1", "digest-a", 10, b"one", 1, 1).unwrap(),
        IntentStageOutcome::Admitted
    );

    assert_eq!(
        store.stage_intent("key-2", "digest-b", 10, b"two", 1, 2).unwrap(),
        IntentStageOutcome::RejectedOverLimit
    );
    // An existing key is deduplicated before the limit, so a retry still resolves past a full buffer.
    assert_eq!(
        store.stage_intent("key-1", "digest-a", 10, b"one", 1, 3).unwrap(),
        IntentStageOutcome::Duplicate
    );
}

#[test]
fn test_advance_moves_the_phase_forward() {
    let (_dir, store) = store();
    store.stage_intent("key-1", "digest-a", 10, b"intent", 5, 1).unwrap();

    assert_eq!(
        store.advance_intent("key-1", IntentPhase::Admitted, 2).unwrap(),
        IntentTransition::Advanced
    );
    let record = store.staged_intent("key-1").unwrap().unwrap();
    assert_eq!(record.phase, IntentPhase::Admitted);
    assert_eq!(record.updated_at_unix, 2);
}

#[test]
fn test_advance_to_expired() {
    let (_dir, store) = store();
    store.stage_intent("key-1", "digest-a", 10, b"intent", 5, 1).unwrap();

    assert_eq!(
        store.advance_intent("key-1", IntentPhase::Expired, 9).unwrap(),
        IntentTransition::Advanced
    );
    assert_eq!(
        store.staged_intent("key-1").unwrap().unwrap().phase,
        IntentPhase::Expired
    );
}

#[test]
fn test_advance_ignores_a_backward_or_equal_transition() {
    let (_dir, store) = store();
    store.stage_intent("key-1", "digest-a", 10, b"intent", 5, 1).unwrap();
    store.advance_intent("key-1", IntentPhase::Admitted, 2).unwrap();

    // Backward: Admitted cannot drop to Pending.
    assert_eq!(
        store.advance_intent("key-1", IntentPhase::Pending, 3).unwrap(),
        IntentTransition::Ignored
    );
    // Equal: re-applying the current phase is a no-op.
    assert_eq!(
        store.advance_intent("key-1", IntentPhase::Admitted, 4).unwrap(),
        IntentTransition::Ignored
    );
    assert_eq!(
        store.staged_intent("key-1").unwrap().unwrap().phase,
        IntentPhase::Admitted
    );
}

#[test]
fn test_advance_ignores_an_unknown_intent() {
    let (_dir, store) = store();

    assert_eq!(
        store.advance_intent("ghost", IntentPhase::Admitted, 1).unwrap(),
        IntentTransition::Ignored
    );
}

#[test]
fn test_staged_intent_is_none_for_an_unknown_key() {
    let (_dir, store) = store();
    assert_eq!(store.staged_intent("unknown").unwrap(), None);
    assert_eq!(store.count_staged_intents().unwrap(), 0);
}

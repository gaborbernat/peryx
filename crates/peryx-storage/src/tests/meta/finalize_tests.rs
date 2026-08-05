use crate::meta::{
    AccountingClass, FinalizeOutcome, IntentPhase, MetaError, MetaStore, NewQuotaReservation, OperationState,
    QuotaError, QuotaReservationRecord, QuotaValue,
};

fn decode_error() -> MetaError {
    MetaError::from(serde_json::from_str::<serde_json::Value>("{").unwrap_err())
}

fn reservation(store: &MetaStore) -> QuotaReservationRecord {
    store
        .reserve_project_quota(
            NewQuotaReservation {
                repository: "hosted",
                project: Some("flask"),
                version: Some("1.0"),
                digest: "digest",
                bytes: 8,
                class: AccountingClass::Hosted,
                created_at_unix: 1,
            },
            8,
            false,
        )
        .unwrap()
}

fn staged(store: &MetaStore) {
    store.stage_intent("intent", "digest", 8, b"payload", 16, 1).unwrap();
}

fn publish(store: &MetaStore, response: &[u8]) -> Result<FinalizeOutcome, MetaError> {
    store.commit_finalized_write("op", "intent", response, Some(100), 2, |driver| {
        driver.put("row", b"value")?;
        Ok::<_, MetaError>(vec![b"{\"action\":\"add\"}".to_vec()])
    })
}

#[test]
fn test_commit_finalized_write_publishes_rows_outcome_and_intent_advance() {
    let (_dir, store) = super::store();
    staged(&store);

    let outcome = publish(&store, b"response").unwrap();

    assert_eq!(outcome, FinalizeOutcome::Published);
    assert_eq!(
        store.get_driver_value("row").unwrap().as_deref(),
        Some(b"value".as_slice())
    );
    assert_eq!(
        store.current_serial().unwrap(),
        1,
        "the journal entry allocates one serial"
    );
    let record = store.operation_outcome("op").unwrap().unwrap();
    assert_eq!(record.state, OperationState::Published);
    assert_eq!(record.response, b"response");
    assert_eq!(record.expiry_unix, Some(100));
    assert_eq!(
        store.staged_intent("intent").unwrap().unwrap().phase,
        IntentPhase::Admitted,
        "the finalize advances the intent out of pending"
    );
}

#[test]
fn test_commit_finalized_write_replays_the_first_result_without_a_second_write() {
    let (_dir, store) = super::store();
    staged(&store);
    publish(&store, b"first").unwrap();

    let replay = store
        .commit_finalized_write("op", "intent", b"second", Some(200), 5, |driver| {
            driver.put("row", b"clobbered")?;
            Ok::<_, MetaError>(vec![b"{\"action\":\"add\"}".to_vec()])
        })
        .unwrap();

    assert_eq!(
        replay,
        FinalizeOutcome::Replayed(store.operation_outcome("op").unwrap().unwrap())
    );
    assert!(
        matches!(&replay, FinalizeOutcome::Replayed(record) if record.response == b"first"),
        "the replay returns the first attempt's response"
    );
    assert_eq!(
        store.get_driver_value("row").unwrap().as_deref(),
        Some(b"value".as_slice()),
        "the replayed body's staged row is discarded"
    );
    assert_eq!(
        store.current_serial().unwrap(),
        1,
        "the replay appends no second journal entry"
    );
}

#[test]
fn test_commit_finalized_write_rolls_back_when_the_body_errors() {
    let (_dir, store) = super::store();
    staged(&store);

    let result = store.commit_finalized_write("op", "intent", b"response", None, 2, |driver| {
        driver.put("row", b"value")?;
        Err::<Vec<Vec<u8>>, _>(decode_error())
    });

    assert!(result.is_err(), "the body's error propagates");
    assert!(store.get_driver_value("row").unwrap().is_none(), "no row is committed");
    assert!(
        store.operation_outcome("op").unwrap().is_none(),
        "no outcome is stamped"
    );
    assert_eq!(store.current_serial().unwrap(), 0);
    assert_eq!(
        store.staged_intent("intent").unwrap().unwrap().phase,
        IntentPhase::Pending,
        "a rejected finalize leaves the intent pending"
    );
}

#[test]
fn test_commit_finalized_write_advances_nothing_for_an_absent_intent() {
    let (_dir, store) = super::store();

    let outcome = publish(&store, b"response").unwrap();

    assert_eq!(outcome, FinalizeOutcome::Published);
    assert!(
        store.staged_intent("intent").unwrap().is_none(),
        "no intent is created for an absent key"
    );
    assert_eq!(
        store.operation_outcome("op").unwrap().unwrap().state,
        OperationState::Published
    );
}

#[test]
fn test_commit_finalized_write_with_quota_commits_the_reservation_on_publish() {
    let (_dir, store) = super::store();
    staged(&store);
    let reservation = reservation(&store);

    let outcome = store
        .commit_finalized_write_with_quota("op", "intent", b"response", Some(100), 2, reservation.id, |driver| {
            driver.put("row", b"value")?;
            Ok::<_, QuotaError>((true, vec![b"{\"action\":\"add\"}".to_vec()]))
        })
        .unwrap();

    assert_eq!(outcome, FinalizeOutcome::Published);
    assert_eq!(
        store
            .quota_project_usage("hosted", "flask")
            .unwrap()
            .file_bytes
            .committed,
        8
    );
    assert_eq!(
        store.staged_intent("intent").unwrap().unwrap().phase,
        IntentPhase::Admitted
    );
}

#[test]
fn test_commit_finalized_write_with_quota_releases_the_reservation_on_skip() {
    let (_dir, store) = super::store();
    staged(&store);
    let reservation = reservation(&store);

    let outcome = store
        .commit_finalized_write_with_quota("op", "intent", b"response", None, 2, reservation.id, |_driver| {
            Ok::<_, QuotaError>((false, Vec::new()))
        })
        .unwrap();

    assert_eq!(
        outcome,
        FinalizeOutcome::Published,
        "a skipped finalize still records a terminal result"
    );
    assert_eq!(
        store.quota_reservation(reservation.id).unwrap(),
        None,
        "the reservation is released"
    );
    assert_eq!(
        store.quota_project_usage("hosted", "flask").unwrap().file_bytes,
        QuotaValue::default()
    );
}

#[test]
fn test_commit_finalized_write_with_quota_replays_without_moving_the_reservation() {
    let (_dir, store) = super::store();
    staged(&store);
    let reservation = reservation(&store);
    store
        .commit_finalized_write_with_quota("op", "intent", b"first", None, 2, reservation.id, |driver| {
            driver.put("row", b"value")?;
            Ok::<_, QuotaError>((true, vec![b"{\"action\":\"add\"}".to_vec()]))
        })
        .unwrap();

    let replay = store
        .commit_finalized_write_with_quota("op", "intent", b"second", None, 5, reservation.id, |driver| {
            driver.put("row", b"clobbered")?;
            Ok::<_, QuotaError>((true, vec![b"{\"action\":\"add\"}".to_vec()]))
        })
        .unwrap();

    assert!(matches!(&replay, FinalizeOutcome::Replayed(record) if record.response == b"first"));
    assert_eq!(
        store
            .quota_project_usage("hosted", "flask")
            .unwrap()
            .file_bytes
            .committed,
        8,
        "the replay does not move the already-committed reservation a second time"
    );
    assert_eq!(store.current_serial().unwrap(), 1);
}

#[test]
fn test_commit_finalized_write_leaves_an_already_admitted_intent() {
    let (_dir, store) = super::store();
    staged(&store);
    store.advance_intent("intent", IntentPhase::Admitted, 3).unwrap();

    let outcome = publish(&store, b"response").unwrap();

    assert_eq!(outcome, FinalizeOutcome::Published);
    let intent = store.staged_intent("intent").unwrap().unwrap();
    assert_eq!(intent.phase, IntentPhase::Admitted);
    assert_eq!(
        intent.updated_at_unix, 3,
        "the finalize does not rewrite a settled intent"
    );
}

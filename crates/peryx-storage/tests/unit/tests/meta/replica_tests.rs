use rstest::rstest;

use crate::meta::{DriverBlobReference, DriverMutation, JournalEntry, MetaError};

fn write_replica(txn: &mut crate::meta::DriverTxn<'_>) -> Result<((), Vec<JournalEntry>), MetaError> {
    txn.put("alpha\0upload", b"record")?;
    txn.put_local("replication\0state", b"1")?;
    Ok((
        (),
        vec![JournalEntry {
            payload: b"event".to_vec(),
            mutations: vec![DriverMutation::Put {
                key: "alpha\0upload".to_owned(),
                value: b"record".to_vec(),
            }],
            blobs: vec![DriverBlobReference {
                sha256: "digest".to_owned(),
                size: 6,
            }],
        }],
    ))
}

#[test]
fn test_replica_txn_copies_rows_journal_and_serial() {
    let (_dir, store) = super::store();

    store.commit_replica_txn(0, write_replica).unwrap();

    assert_eq!(store.current_serial().unwrap(), 1);
    assert_eq!(
        store.get_driver_value("alpha\0upload").unwrap().as_deref(),
        Some(b"record".as_slice())
    );
    assert_eq!(
        store.journal_after(0, 10).unwrap(),
        vec![crate::meta::JournalRecord {
            serial: 1,
            payload: b"event".to_vec(),
            mutations: vec![crate::meta::DriverMutation::Put {
                key: "alpha\0upload".to_owned(),
                value: b"record".to_vec(),
            }],
            blobs: vec![DriverBlobReference {
                sha256: "digest".to_owned(),
                size: 6,
            }],
        }]
    );
}

#[test]
fn test_replica_txn_rejects_a_stale_cursor_without_writes() {
    let (_dir, store) = super::store();
    store.next_serial().unwrap();

    let result = store.commit_replica_txn(0, write_replica);

    assert!(matches!(
        result,
        Err(MetaError::ReplicaSerialConflict { expected: 0, actual: 1 })
    ));
    assert!(store.get_driver_value("alpha\0upload").unwrap().is_none());
    assert!(store.journal_after(1, 10).unwrap().is_empty());
}

#[rstest]
#[case::same(7, 7, (7, 8))]
#[case::rollback(9, 4, (9, 10))]
#[case::forward(3, 12, (3, 12))]
#[case::zero(0, 0, (0, 1))]
fn test_replica_incarnation_survives_clock_changes(
    #[case] first: u64,
    #[case] second: u64,
    #[case] expected: (u64, u64),
) {
    let (dir, store) = super::store();
    let path = dir.path().join("peryx.redb");
    let first = store.next_replica_incarnation(first).unwrap();
    drop(store);

    let second = crate::meta::MetaStore::open_existing(path)
        .unwrap()
        .next_replica_incarnation(second)
        .unwrap();

    assert_eq!((first, second), expected);
}

#[test]
fn test_replica_incarnation_serializes_concurrent_allocations() {
    let (_dir, store) = super::store();
    let mut incarnations = std::thread::scope(|scope| {
        let threads: [_; 16] = std::array::from_fn(|_| {
            let store = store.clone();
            scope.spawn(move || store.next_replica_incarnation(0).unwrap())
        });
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    });
    incarnations.sort_unstable();

    assert_eq!(incarnations, (0..16).collect::<Vec<_>>());
}

#[test]
fn test_replica_incarnation_rejects_counter_overflow() {
    let (_dir, store) = super::store();
    store.next_replica_incarnation(u64::MAX).unwrap();

    assert!(matches!(
        store.next_replica_incarnation(0),
        Err(MetaError::ReplicaIncarnationOverflow)
    ));
}

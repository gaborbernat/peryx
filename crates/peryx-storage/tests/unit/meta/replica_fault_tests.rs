use crate::meta::fault;

#[test]
fn test_replica_incarnation_recovers_after_storage_failures() {
    let mut failures = 0;
    for fail_after in 0..256 {
        let (store, inner, fault) = fault::initialized();
        fault.arm(fail_after);
        if store.next_replica_incarnation(41).is_err() {
            failures += 1;
            fault.disable();
            drop(store);
            let store = fault::reopen(&inner, &fault);
            let recovered = store.next_replica_incarnation(41).unwrap();
            let next = store.next_replica_incarnation(0).unwrap();

            assert!((41..=42).contains(&recovered));
            assert_eq!(next, recovered + 1);
        }
    }
    assert!(failures > 0);
}

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use crate::circuit::{CircuitBreaker, CircuitClock, CircuitConfig, CircuitPermit, DEFAULT_CIRCUIT};

#[test]
fn test_default_config_matches_the_documented_constant() {
    assert_eq!(CircuitConfig::default(), DEFAULT_CIRCUIT);
}

#[test]
fn test_an_unseen_source_is_admitted() {
    assert!(breaker(3, 30).0.admit("dc-a").is_some());
}

#[test]
fn test_a_source_below_the_threshold_stays_available() {
    let (breaker, _) = breaker(3, 30);
    fail(&breaker, "dc-a");
    fail(&breaker, "dc-a");

    assert!(breaker.admit("dc-a").is_some());
}

#[test]
fn test_reaching_the_threshold_trips_the_source_open() {
    let (breaker, clock) = breaker(3, 30);
    clock.store(1, Ordering::SeqCst);
    trip(&breaker, "dc-a", 3);

    assert!(breaker.admit("dc-a").is_none());
    clock.store(30, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_none());
}

#[test]
fn test_concurrent_admission_grants_one_half_open_probe() {
    let (breaker, _) = probe_ready();
    let start = Arc::new(Barrier::new(9));
    let admitted = Arc::new(Barrier::new(9));

    let probes = std::thread::scope(|scope| {
        let handles: [_; 8] = std::array::from_fn(|_| {
            let breaker = breaker.clone();
            let start = Arc::clone(&start);
            let admitted = Arc::clone(&admitted);
            scope.spawn(move || {
                start.wait();
                let permit = breaker.admit("dc-a");
                admitted.wait();
                permit.is_some()
            })
        });
        start.wait();
        admitted.wait();
        handles
            .into_iter()
            .map(|handle| usize::from(handle.join().unwrap()))
            .sum::<usize>()
    });

    assert_eq!(probes, 1);
}

#[test]
fn test_a_successful_probe_closes_the_source() {
    let (breaker, _) = probe_ready();
    admit(&breaker, "dc-a").success();

    let first = breaker.admit("dc-a");
    let second = breaker.admit("dc-a");
    assert_eq!((first.is_some(), second.is_some()), (true, true));
}

#[test]
fn test_a_failed_probe_reopens_the_source_for_a_fresh_cooldown() {
    let (breaker, clock) = probe_ready();
    admit(&breaker, "dc-a").failure();

    assert!(breaker.admit("dc-a").is_none());
    clock.store(60, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_none());
    clock.store(61, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_some());
}

#[test]
fn test_dropping_a_probe_starts_cooldown_at_cancellation() {
    let (breaker, clock) = probe_ready();
    let probe = admit(&breaker, "dc-a");
    clock.store(40, Ordering::SeqCst);
    drop(probe);

    clock.store(69, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_none());
    clock.store(70, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_some());
}

#[test]
fn test_an_expired_probe_starts_a_fresh_cooldown() {
    let (breaker, clock) = probe_ready();
    let probe = admit(&breaker, "dc-a");
    clock.store(41, Ordering::SeqCst);

    assert!(breaker.admit("dc-a").is_none());
    clock.store(70, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_none());
    clock.store(71, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_some());
    drop(probe);
}

#[test]
fn test_an_expired_probe_outcome_starts_a_fresh_cooldown() {
    let (breaker, clock) = probe_ready();
    let probe = admit(&breaker, "dc-a");
    clock.store(41, Ordering::SeqCst);
    probe.success();

    clock.store(70, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_none());
    clock.store(71, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_some());
}

#[test]
fn test_a_late_probe_outcome_cannot_close_the_reopened_source() {
    let (breaker, clock) = probe_ready();
    let probe = admit(&breaker, "dc-a");
    clock.store(41, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_none());
    probe.success();

    clock.store(70, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_none());
    clock.store(71, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_some());
}

#[test]
fn test_a_probe_timeout_and_cooldown_can_elapse_without_observation() {
    let (breaker, clock) = probe_ready();
    let expired = admit(&breaker, "dc-a");
    clock.store(71, Ordering::SeqCst);

    let current = admit(&breaker, "dc-a");
    drop(expired);
    current.success();

    assert!(breaker.admit("dc-a").is_some());
}

#[test]
fn test_dropping_an_expired_probe_keeps_the_timeout_cooldown_deadline() {
    let (breaker, clock) = probe_ready();
    let probe = admit(&breaker, "dc-a");
    clock.store(50, Ordering::SeqCst);
    drop(probe);

    clock.store(70, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_none());
    clock.store(71, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_some());
}

#[test]
fn test_an_expired_probe_cannot_overwrite_a_newer_claim() {
    let (breaker, clock) = probe_ready();
    let expired = admit(&breaker, "dc-a");
    clock.store(41, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_none());
    clock.store(71, Ordering::SeqCst);
    let current = admit(&breaker, "dc-a");

    expired.success();
    assert!(breaker.admit("dc-a").is_none());
    current.failure();
    clock.store(100, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_none());
    clock.store(101, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_some());
}

#[test]
fn test_concurrent_failures_start_cooldown_from_the_latest_outcome() {
    let (breaker, clock) = breaker(1, 30);
    let first = admit(&breaker, "dc-a");
    let second = admit(&breaker, "dc-a");
    first.failure();
    clock.store(10, Ordering::SeqCst);
    second.failure();

    clock.store(39, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_none());
    clock.store(40, Ordering::SeqCst);
    assert!(breaker.admit("dc-a").is_some());
}

#[test]
fn test_a_success_resets_the_failure_count_before_a_trip() {
    let (breaker, _) = breaker(3, 30);
    fail(&breaker, "dc-a");
    fail(&breaker, "dc-a");
    admit(&breaker, "dc-a").success();
    fail(&breaker, "dc-a");
    fail(&breaker, "dc-a");

    assert!(breaker.admit("dc-a").is_some());
}

#[test]
fn test_a_zero_threshold_trips_on_the_first_failure() {
    let (breaker, _) = breaker(0, 30);
    fail(&breaker, "dc-a");

    assert!(breaker.admit("dc-a").is_none());
}

#[test]
fn test_sources_trip_independently() {
    let (breaker, _) = breaker(1, 30);
    fail(&breaker, "dc-a");

    assert!(breaker.admit("dc-a").is_none());
    assert!(breaker.admit("dc-b").is_some());
}

fn probe_ready() -> (CircuitBreaker, Arc<AtomicU64>) {
    let (breaker, clock) = breaker(1, 30);
    clock.store(1, Ordering::SeqCst);
    fail(&breaker, "dc-a");
    clock.store(31, Ordering::SeqCst);
    (breaker, clock)
}

fn breaker(trip_after: u32, cooldown_secs: u64) -> (CircuitBreaker, Arc<AtomicU64>) {
    let clock = Arc::new(AtomicU64::new(0));
    let handle = Arc::clone(&clock);
    let source: CircuitClock = Arc::new(move || Duration::from_secs(handle.load(Ordering::SeqCst)));
    (
        CircuitBreaker::new(
            CircuitConfig {
                trip_after,
                cooldown: Duration::from_secs(cooldown_secs),
                probe_timeout: Duration::from_secs(10),
            },
            source,
        ),
        clock,
    )
}

fn admit(breaker: &CircuitBreaker, source: &str) -> CircuitPermit {
    breaker.admit(source).expect("the source should admit this call")
}

fn fail(breaker: &CircuitBreaker, source: &str) {
    admit(breaker, source).failure();
}

fn trip(breaker: &CircuitBreaker, source: &str, failures: u32) {
    for _ in 0..failures {
        fail(breaker, source);
    }
}

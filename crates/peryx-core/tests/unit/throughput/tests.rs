use std::num::NonZeroU64;
use std::time::Duration;

use super::ThroughputBudget;
use rstest::rstest;

#[test]
fn test_delivered_reports_the_bytes_counted_so_far() {
    let mut budget = ThroughputBudget::new(NonZeroU64::new(1000).unwrap(), Duration::ZERO);
    budget.deliver(500);
    assert_eq!(budget.delivered(), 500);
}

#[rstest]
#[case::exactly_earned(Duration::from_millis(500), false)]
#[case::one_nanosecond_short(Duration::from_nanos(500_000_001), true)]
fn test_is_starved_at_the_earned_boundary(#[case] elapsed: Duration, #[case] expected: bool) {
    let mut budget = ThroughputBudget::new(NonZeroU64::new(1000).unwrap(), Duration::ZERO);
    budget.deliver(500);
    assert_eq!(budget.is_starved(elapsed), expected);
}

#[test]
fn test_is_starved_counts_the_grace_period_before_any_delivery() {
    let budget = ThroughputBudget::new(NonZeroU64::new(1000).unwrap(), Duration::from_secs(2));
    assert!(!budget.is_starved(Duration::from_secs(2)));
    assert!(budget.is_starved(Duration::from_secs(2) + Duration::from_nanos(1)));
}

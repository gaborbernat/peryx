//! The bounded query that reports how complete a replica's distributed analytics picture is.
//!
//! A reader asks for the accepted download totals over a day range, scoped to one repository or across
//! all of them, and wants to know whether the answer covers every producer it should. This folds the
//! converged [`AnalyticsReceiver`] totals into per-day buckets and classifies the range against the
//! expected producers with [`assess`](crate::completeness::assess).
//!
//! Completeness is measured against the cluster's own accepted frontier, not the wall clock. The
//! frontier is the highest sealed day any expected producer has been folded through; a producer that has
//! reached it covers the range, one that trails it leaves the range [`Delayed`](Completeness::Delayed),
//! and one with no accepted frontier at all leaves it [`Unavailable`](Completeness::Unavailable). How
//! stale that frontier is against today is reported separately as the lag, so a quiet day never reads as
//! a hole: an idle producer that has caught up to the frontier is complete, and the lag says how old the
//! newest sealed day is. A historical range whose end sits below the frontier requires only coverage
//! through its own end, so a producer still catching up to today can still be complete for the past.

use std::collections::BTreeMap;

use crate::analytics::{AggregateDelta, AnalyticsReceiver, ProducerId};
use crate::completeness::{Completeness, ProducerCoverage, assess};
use crate::envelope::AuthorityEpoch;

/// A bounded completeness query.
///
/// It carries the inclusive day range, today for the lag reading, and an optional repository scope. The
/// caller resolves and clamps the range and today off its clock before building this, so the assessment
/// stays a pure function of derived positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletenessQuery {
    pub from_day: i64,
    pub to_day: i64,
    pub today: i64,
    /// The index route the totals are confined to, or `None` for a cross-repository query.
    pub repository: Option<String>,
}

/// One producer the operator expects to contribute, and the datacenter it runs in.
///
/// The expected set comes from the configured topology, so a producer that has never delivered a batch
/// is still expected and still marks the range incomplete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedProducer {
    pub producer: ProducerId,
    pub dc: String,
}

/// How one expected producer covers the queried range: its accepted frontier and the verdict for it
/// alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerReport {
    pub producer: ProducerId,
    pub dc: String,
    /// The producer's accepted frontier `(epoch, sealed day)`, or `None` when it has delivered nothing.
    pub accepted: Option<(AuthorityEpoch, u64)>,
    pub state: Completeness,
}

/// One UTC day's converged download and byte totals inside the queried range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DayBucket {
    pub day: i64,
    pub downloads: u64,
    pub bytes: u64,
}

/// The full result of a completeness query: the range verdict, the frontier and lag it was measured
/// against, the per-producer coverage, and the accepted totals folded per day.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletenessReport {
    pub completeness: Completeness,
    /// The highest sealed day any expected producer has been folded through, or `None` when none has.
    pub frontier_day: Option<i64>,
    /// The sealed day the range required coverage through: the frontier capped at the range end, or
    /// `None` when there is no frontier to require.
    pub required_day: Option<i64>,
    /// Days between today and the frontier, so a caller reads how stale the newest sealed day is apart
    /// from whether producers agree on it. `None` when there is no frontier.
    pub lag_days: Option<i64>,
    pub producers: Vec<ProducerReport>,
    pub buckets: Vec<DayBucket>,
    pub totals: AggregateDelta,
}

/// Assess how completely `receiver` covers `query` for the `expected` producers.
///
/// The verdict comes from [`assess`](crate::completeness::assess) over one [`ProducerCoverage`] per
/// expected producer, each required to reach the cluster frontier capped at the range end. An empty
/// expected set is [`Unavailable`](Completeness::Unavailable), the fail-closed answer, since a picture
/// vouched for against zero producers cannot be told apart from one a filter narrowed to nothing.
#[must_use]
pub fn assess_completeness(
    receiver: &AnalyticsReceiver,
    expected: &[ExpectedProducer],
    query: &CompletenessQuery,
) -> CompletenessReport {
    let frontier_day = expected
        .iter()
        .filter_map(|expected| receiver.accepted_frontier(&expected.producer))
        .map(|(_, sequence)| i64::try_from(sequence).unwrap_or(i64::MAX))
        .max();
    let required_day = frontier_day.map(|frontier| frontier.min(query.to_day));
    let lag_days = frontier_day.map(|frontier| query.today - frontier);

    let coverages: Vec<ProducerCoverage> = expected
        .iter()
        .map(|expected| {
            coverage(
                expected.producer.clone(),
                receiver.accepted_frontier(&expected.producer),
                required_day,
            )
        })
        .collect();
    let producers: Vec<ProducerReport> = expected
        .iter()
        .zip(&coverages)
        .map(|(expected, coverage)| ProducerReport {
            producer: expected.producer.clone(),
            dc: expected.dc.clone(),
            accepted: coverage.accepted,
            state: assess(std::slice::from_ref(coverage)),
        })
        .collect();

    let mut folded: BTreeMap<i64, (u64, u64)> = BTreeMap::new();
    let mut totals = AggregateDelta::default();
    for (key, delta) in receiver.accepted_rows() {
        if key.day < query.from_day
            || key.day > query.to_day
            || query.repository.as_deref().is_some_and(|route| key.repository != route)
        {
            continue;
        }
        let bucket = folded.entry(key.day).or_default();
        bucket.0 = bucket.0.saturating_add(delta.downloads);
        bucket.1 = bucket.1.saturating_add(delta.bytes);
        totals = totals.saturating_add(*delta);
    }
    let buckets = folded
        .into_iter()
        .map(|(day, (downloads, bytes))| DayBucket { day, downloads, bytes })
        .collect();

    CompletenessReport {
        completeness: assess(&coverages),
        frontier_day,
        required_day,
        lag_days,
        producers,
        buckets,
        totals,
    }
}

/// Build one producer's coverage: it must reach `required_day` at its own accepted epoch. A producer
/// with no accepted frontier keeps `accepted` `None`, which [`assess`](crate::completeness::assess)
/// reads as [`Unavailable`](Completeness::Unavailable) whatever the required position is.
fn coverage(
    producer: ProducerId,
    accepted: Option<(AuthorityEpoch, u64)>,
    required_day: Option<i64>,
) -> ProducerCoverage {
    let required = match (accepted, required_day) {
        (Some((epoch, _)), Some(day)) => (epoch, u64::try_from(day).unwrap_or(0)),
        _ => (AuthorityEpoch(0), 0),
    };
    ProducerCoverage {
        producer,
        accepted,
        required,
    }
}

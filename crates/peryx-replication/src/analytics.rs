//! Idempotent apply for the additive analytics aggregates a producer node replicates to a replica.
//!
//! A producer folds request-path downloads into low-cardinality daily buckets and ships them as an
//! [`AnalyticsBatch`]: a set of additive `(dimension, downloads, bytes)` rows stamped with the
//! producer interval that emitted them. A replica applies each interval exactly once. The transfer
//! that carries a batch between nodes is a separate concern (issue #506); this boundary is only the
//! schema and the apply, so it stays verifiable without a live peer.
//!
//! Idempotence is the whole point: a duplicate, reordered, delayed, or retried batch must converge to
//! the same accepted totals a single delivery would. [`ApplyState`] keys deduplication on the
//! [`IntervalId`] `(producer, epoch, sequence)` triple, so a replay is recognized and dropped without
//! disturbing a total, and a producer restart under a fresh [`AuthorityEpoch`] is a distinct identity
//! that applies rather than a lost or double-counted interval.
//!
//! Every untrusted input is bounded and every recovery fails closed. A batch wider than
//! [`ApplyLimits::max_rows_per_batch`] is rejected before it folds, deduplication state cannot grow
//! past [`ApplyLimits::max_retained_intervals`] without [`ApplyState::compact`] first releasing the
//! intervals a durable [`Frontier`] covers, and a snapshot written under an unrecognized schema is
//! refused rather than rebuilt from zero, because silently resetting accepted totals or the replay
//! set would double-count the next replay.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::envelope::AuthorityEpoch;

/// The snapshot schema this build writes and is willing to restore.
pub const APPLY_STATE_SCHEMA: u32 = 1;

/// The default apply bounds: a batch is metadata-sized, and retained replay keys stay bounded so a
/// stalled compaction frontier cannot grow the deduplication set without limit.
pub const DEFAULT_APPLY_LIMITS: ApplyLimits = ApplyLimits {
    max_rows_per_batch: 16_384,
    max_retained_intervals: 65_536,
};

/// The stable identity of a node that produces analytics intervals, independent of any address so it
/// survives a relocation or reconnect.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProducerId(pub String);

/// The idempotency key of one producer interval: which producer, which generation, and the interval's
/// monotonic sequence within that generation. A replica accepts each distinct key once.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IntervalId {
    pub producer: ProducerId,
    pub epoch: AuthorityEpoch,
    pub sequence: u64,
}

/// The dimension tuple of one additive aggregate.
///
/// It identifies a repository/project's downloads of one version, routed from one source, on one UTC
/// day. Every field is a bounded server-side label, never a client identity, address, or credential,
/// so the aggregate carries no raw request or actor history.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AggregateKey {
    /// The UTC day, in whole days since the Unix epoch.
    pub day: i64,
    pub repository: String,
    pub project: String,
    /// The distribution version, or empty when the ecosystem reported none.
    pub version: String,
    /// The routed upstream, or empty when the bytes were served from the local store.
    pub source: String,
}

/// The additive totals of one aggregate. Additive means order-free: applying two deltas for the same
/// key in either order yields the same sum, which is what lets a reordered batch converge.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateDelta {
    pub downloads: u64,
    pub bytes: u64,
}

impl AggregateDelta {
    /// Add `other` into `self`, saturating at [`u64::MAX`] so a hostile or corrupt producer total can
    /// never wrap an accepted sum backward.
    #[must_use]
    pub const fn saturating_add(self, other: Self) -> Self {
        Self {
            downloads: self.downloads.saturating_add(other.downloads),
            bytes: self.bytes.saturating_add(other.bytes),
        }
    }
}

/// One additive row of a batch: a dimension and the totals to fold into it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateRow {
    pub key: AggregateKey,
    pub delta: AggregateDelta,
}

/// A producer interval's additive aggregates, stamped with the [`IntervalId`] that makes applying it
/// idempotent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnalyticsBatch {
    pub interval: IntervalId,
    pub rows: Vec<AggregateRow>,
}

/// The bounds a replica enforces on untrusted apply input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyLimits {
    /// The most rows one batch may carry before it is rejected unfolded.
    pub max_rows_per_batch: usize,
    /// The most interval keys the replay set may retain before a fresh interval is refused until
    /// compaction releases room.
    pub max_retained_intervals: usize,
}

impl Default for ApplyLimits {
    fn default() -> Self {
        DEFAULT_APPLY_LIMITS
    }
}

/// Whether a batch folded into the accepted totals or was recognized as an already-applied replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The interval was new; its rows were folded into the accepted totals.
    Applied,
    /// The interval was already accepted; the totals were left unchanged.
    Duplicate,
}

/// A batch a replica refuses because it breaches a bound. Both variants fail closed, leaving the
/// accepted totals and replay set untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ApplyError {
    #[error("analytics batch carries {actual} rows, over the {limit} row apply limit")]
    BatchTooLarge { limit: usize, actual: usize },
    #[error("replay set holds {limit} intervals; compact past the durable frontier before applying more")]
    RetentionFull { limit: usize },
}

/// A snapshot that cannot be trusted to preserve replay protection, so restore refuses it rather than
/// silently rebuilding accepted totals from zero.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("analytics apply snapshot is malformed: {0}")]
    Malformed(#[source] serde_json::Error),
    #[error("analytics apply snapshot schema {found} is not the {expected} this build restores")]
    UnsupportedSchema { expected: u32, found: u32 },
}

/// The highest interval sequence each `(producer, epoch)` has durably passed everywhere.
///
/// Replay protection must outlast the producer, this replica, and the backup. Compaction may release
/// only the intervals this combined frontier covers, because a producer never resends below the
/// sequence it has been acknowledged for, so those keys can no longer be replayed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Frontier {
    acknowledged: BTreeMap<ProducerId, BTreeMap<AuthorityEpoch, u64>>,
}

impl Frontier {
    /// Record that `producer`'s `epoch` is durable through `sequence`, keeping the highest sequence
    /// seen so an out-of-order acknowledgement never retracts coverage.
    pub fn acknowledge(&mut self, producer: ProducerId, epoch: AuthorityEpoch, sequence: u64) {
        let slot = self.acknowledged.entry(producer).or_default().entry(epoch).or_default();
        *slot = (*slot).max(sequence);
    }

    fn covers(&self, interval: &IntervalId) -> bool {
        self.acknowledged
            .get(&interval.producer)
            .and_then(|epochs| epochs.get(&interval.epoch))
            .is_some_and(|&acknowledged| interval.sequence <= acknowledged)
    }
}

/// The persisted serde shape of an [`ApplyState`]: a schema tag guarding the accepted totals and the
/// replay set, so a format change is a deliberate migration rather than a silent misread.
#[derive(Debug, Serialize, Deserialize)]
struct ApplyStateSnapshot {
    schema: u32,
    totals: Vec<AggregateRow>,
    applied: Vec<IntervalId>,
}

/// A replica's converged view: the accepted additive totals plus the replay set that keeps applying a
/// batch idempotent.
#[derive(Debug, Clone)]
pub struct ApplyState {
    totals: BTreeMap<AggregateKey, AggregateDelta>,
    applied: BTreeSet<IntervalId>,
    limits: ApplyLimits,
}

impl ApplyState {
    /// An empty state bounded by `limits`.
    #[must_use]
    pub const fn new(limits: ApplyLimits) -> Self {
        Self {
            totals: BTreeMap::new(),
            applied: BTreeSet::new(),
            limits,
        }
    }

    /// Apply `batch` once. A batch whose interval was already accepted is a
    /// [`ApplyOutcome::Duplicate`] that changes nothing; a new interval folds its rows into the
    /// accepted totals and joins the replay set.
    ///
    /// # Errors
    /// Returns [`ApplyError::BatchTooLarge`] for a batch over the row limit, or
    /// [`ApplyError::RetentionFull`] when a new interval would grow the replay set past its bound
    /// before [`ApplyState::compact`] has released room. Both leave the state unchanged.
    pub fn apply(&mut self, batch: &AnalyticsBatch) -> Result<ApplyOutcome, ApplyError> {
        if batch.rows.len() > self.limits.max_rows_per_batch {
            return Err(ApplyError::BatchTooLarge {
                limit: self.limits.max_rows_per_batch,
                actual: batch.rows.len(),
            });
        }
        if self.applied.contains(&batch.interval) {
            return Ok(ApplyOutcome::Duplicate);
        }
        if self.applied.len() >= self.limits.max_retained_intervals {
            return Err(ApplyError::RetentionFull {
                limit: self.limits.max_retained_intervals,
            });
        }
        for row in &batch.rows {
            let total = self.totals.entry(row.key.clone()).or_default();
            *total = total.saturating_add(row.delta);
        }
        self.applied.insert(batch.interval.clone());
        Ok(ApplyOutcome::Applied)
    }

    /// The accepted total for `key`, or a zero delta for a dimension nothing has folded into.
    #[must_use]
    pub fn total(&self, key: &AggregateKey) -> AggregateDelta {
        self.totals.get(key).copied().unwrap_or_default()
    }

    /// The number of interval keys the replay set currently retains.
    #[must_use]
    pub fn retained_intervals(&self) -> usize {
        self.applied.len()
    }

    /// Drop the replay keys `frontier` covers, bounding deduplication memory once an interval can no
    /// longer be replayed. Accepted totals are never touched, so releasing a key preserves the sum.
    pub fn compact(&mut self, frontier: &Frontier) {
        self.applied.retain(|interval| !frontier.covers(interval));
    }

    /// Serialize the accepted totals and replay set to their JSON snapshot form.
    ///
    /// # Panics
    /// Panics only if serializing to JSON fails, which the field types make unreachable.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let snapshot = ApplyStateSnapshot {
            schema: APPLY_STATE_SCHEMA,
            totals: self
                .totals
                .iter()
                .map(|(key, delta)| AggregateRow {
                    key: key.clone(),
                    delta: *delta,
                })
                .collect(),
            applied: self.applied.iter().cloned().collect(),
        };
        serde_json::to_vec(&snapshot).expect("an apply-state snapshot always serializes to JSON")
    }

    /// Restore a state from its snapshot under `limits`, preserving both the accepted totals and the
    /// replay protection.
    ///
    /// # Errors
    /// Returns [`SnapshotError::Malformed`] for unparseable bytes and [`SnapshotError::UnsupportedSchema`]
    /// for a schema this build does not restore. Restore fails closed rather than rebuild from zero,
    /// because a reset replay set would double-count the next replay.
    pub fn restore(bytes: &[u8], limits: ApplyLimits) -> Result<Self, SnapshotError> {
        let snapshot: ApplyStateSnapshot = serde_json::from_slice(bytes).map_err(SnapshotError::Malformed)?;
        if snapshot.schema != APPLY_STATE_SCHEMA {
            return Err(SnapshotError::UnsupportedSchema {
                expected: APPLY_STATE_SCHEMA,
                found: snapshot.schema,
            });
        }
        Ok(Self {
            totals: snapshot.totals.into_iter().map(|row| (row.key, row.delta)).collect(),
            applied: snapshot.applied.into_iter().collect(),
            limits,
        })
    }
}

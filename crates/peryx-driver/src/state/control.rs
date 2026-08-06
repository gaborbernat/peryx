//! The availability control plane: the administrator-driven membership and transfer commands, and the
//! idempotency, concurrency, audit, and latency wrapping around them.
//!
//! The ownership consensus group changes only through committed Raft commands, never a direct store
//! write. [`MembershipControl`] is that submission seam: the binary implements it over the running Raft
//! node, and this plane wraps it so a control request is idempotent, bounded, and audited without the
//! HTTP surface reaching the consensus internals.
//!
//! [`ControlPlane::execute`] is the whole path. A request that carries an idempotency key it has already
//! committed returns the same [`CommandReceipt`] without resubmitting, so a client that retries across a
//! leader loss reads one committed result rather than minting a second command. A permit bounds the
//! commands in flight, a bounded window retains recent receipts and latencies, and every attempt writes
//! one audit line naming the actor, command, target, result, and committed identity — never the request
//! body, so an address or token never reaches the log.

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::state::Clock;

/// The commands in flight a control plane admits at once. A membership change rewrites the consensus
/// roster, so the surface serializes a small burst rather than letting an operator script fan out an
/// unbounded set of concurrent rewrites at the leader.
const MAX_CONCURRENT_COMMANDS: usize = 4;

/// How many recent committed receipts and command latencies the plane retains. The receipts bound the
/// idempotency window and the latencies bound the reported percentiles; both are recent-history only,
/// never a growing ledger.
const RETAINED: usize = 256;

/// A membership or transfer command an administrator submits to the ownership consensus group.
///
/// The four membership variants rewrite the consensus roster through `OpenRaft`; the two authority
/// variants move or fence an artifact home through a committed ownership command. Every variant commits
/// through the Raft log, so no handler writes the ownership or membership store directly.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlCommand {
    /// Add a datacenter as a non-voting learner that replicates the log without counting toward quorum.
    AddLearner {
        /// The datacenter identity to add, the stable name the roster keys the voter by.
        datacenter: String,
        /// The bare `host:port` the learner's peer RPCs dial.
        address: String,
    },
    /// Promote an existing learner datacenter to a voter that counts toward quorum.
    PromoteVoter {
        /// The learner datacenter to promote.
        datacenter: String,
    },
    /// Remove a voter datacenter from the consensus roster.
    RemoveVoter {
        /// The voter datacenter to remove.
        datacenter: String,
    },
    /// Replace one voter with another in a single roster rewrite: add the incoming datacenter as a
    /// learner and swap it in for the outgoing one.
    ReplaceVoter {
        /// The voter datacenter to remove.
        remove: String,
        /// The datacenter to add in its place.
        datacenter: String,
        /// The bare `host:port` the incoming datacenter's peer RPCs dial.
        address: String,
    },
    /// Move an authority's home to a new datacenter, minting the next authority epoch.
    TransferAuthority {
        /// The authority to move, a project or repository home.
        authority: String,
        /// The datacenter to home it at.
        new_home: String,
    },
    /// Mint the next epoch for an authority without moving its home, fencing every operation left under
    /// the prior epoch. Cancels the outstanding work a superseded holder left behind.
    AdvanceEpoch {
        /// The authority to fence.
        authority: String,
    },
}

impl ControlCommand {
    /// The stable audit name of this command's kind.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::AddLearner { .. } => "add_learner",
            Self::PromoteVoter { .. } => "promote_voter",
            Self::RemoveVoter { .. } => "remove_voter",
            Self::ReplaceVoter { .. } => "replace_voter",
            Self::TransferAuthority { .. } => "transfer_authority",
            Self::AdvanceEpoch { .. } => "advance_epoch",
        }
    }

    /// The command's target for the audit line: the datacenter a membership command names or the
    /// authority a transfer command names. Never an address or credential, so the audit line carries no
    /// request body.
    #[must_use]
    pub fn target(&self) -> &str {
        match self {
            Self::AddLearner { datacenter, .. }
            | Self::PromoteVoter { datacenter }
            | Self::RemoveVoter { datacenter }
            | Self::ReplaceVoter { datacenter, .. } => datacenter,
            Self::TransferAuthority { authority, .. } | Self::AdvanceEpoch { authority } => authority,
        }
    }
}

/// The committed identity of a control command.
///
/// The term and index of the log entry that carried it, and what the group made of it. A client that
/// retries an idempotent request reads back this same receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CommandReceipt {
    /// The leadership term of the committed log entry.
    pub term: u64,
    /// The index of the committed log entry.
    pub index: u64,
    /// What the group made of the command.
    pub outcome: CommandOutcome,
    /// The voter roster, by datacenter name in sorted order, before the command committed. Empty for a
    /// command that does not touch the voter roster, such as a transfer or epoch command.
    pub old_voters: Vec<String>,
    /// The voter roster after the command committed.
    pub new_voters: Vec<String>,
}

/// What a committed control command changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandOutcome {
    /// The command changed the roster or ownership state.
    Committed,
    /// The command committed but left the state unchanged, for example promoting a datacenter already a
    /// voter. Idempotent by construction.
    NoChange,
}

impl CommandOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::NoChange => "no_change",
        }
    }
}

/// Why a control command did not return a receipt.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ControlError {
    /// This node is not the consensus leader, so it cannot commit the command. Carries the leader's
    /// address when the group knows one, for a caller that retries against it.
    #[error("not the consensus leader{}", .leader.as_deref().map(|a| format!("; leader at {a}")).unwrap_or_default())]
    NotLeader {
        /// The current leader's peer address, when known.
        leader: Option<String>,
    },
    /// The group could not commit the command for another reason, for example an unreachable quorum.
    #[error("consensus command did not commit: {0}")]
    Unavailable(String),
    /// The command is invalid against the current state, for example transferring an unassigned authority
    /// or to the home it already holds.
    #[error("invalid command: {0}")]
    Invalid(String),
    /// The plane is already running its bounded set of concurrent commands.
    #[error("too many concurrent availability commands in flight")]
    Overloaded,
}

impl ControlError {
    /// The stable audit name of this failure.
    const fn kind(&self) -> &'static str {
        match self {
            Self::NotLeader { .. } => "not_leader",
            Self::Unavailable(_) => "unavailable",
            Self::Invalid(_) => "invalid",
            Self::Overloaded => "overloaded",
        }
    }
}

/// The consensus submission seam an availability command commits through.
///
/// The binary implements it over the running Raft node; the control plane holds it behind this trait so
/// the HTTP surface, its idempotency, and its audit never reach the consensus internals.
#[async_trait::async_trait]
pub trait MembershipControl: Send + Sync {
    /// Submit `command` to the consensus log and return the receipt of the entry that carried it.
    ///
    /// # Errors
    /// Returns [`ControlError`] when this node is not the leader, the command cannot commit, or the
    /// command is invalid against the current state.
    async fn submit(&self, command: ControlCommand) -> Result<CommandReceipt, ControlError>;
}

/// The next voter roster a discrete membership command produces from the current one.
///
/// Pure set arithmetic so the binary's Raft-facing impl computes the target roster the same way every
/// call: add the incoming voter id, drop the outgoing one, and the result is the membership to commit. A
/// promotion of a datacenter already a voter, or a removal of one absent, yields the current roster
/// unchanged, which commits as [`CommandOutcome::NoChange`].
#[must_use]
pub fn plan_voter_roster(current: &BTreeSet<u64>, add: Option<u64>, remove: Option<u64>) -> BTreeSet<u64> {
    let mut roster = current.clone();
    if let Some(id) = add {
        roster.insert(id);
    }
    if let Some(id) = remove {
        roster.remove(&id);
    }
    roster
}

/// One audited control attempt.
///
/// Names the actor, the command's kind and target, the result, and the committed identity when it
/// committed. It never carries the request body, so an address or token is not logged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditRecord {
    /// The administrator that submitted the command.
    pub actor: String,
    /// The command's kind.
    pub command: &'static str,
    /// The command's datacenter or authority target.
    pub target: String,
    /// The attempt's result: a committed outcome, a replay, or a failure kind.
    pub result: &'static str,
    /// The committed term, present only when the command committed.
    pub term: Option<u64>,
    /// The committed index, present only when the command committed.
    pub index: Option<u64>,
    /// The voter roster before a committed membership command, so an auditor sees the roster transition.
    /// Empty for a failed command or one that does not touch the voter roster.
    pub old_voters: Vec<String>,
    /// The voter roster after a committed membership command.
    pub new_voters: Vec<String>,
}

impl AuditRecord {
    fn committed(actor: &str, command: &ControlCommand, receipt: &CommandReceipt) -> Self {
        Self {
            actor: actor.to_owned(),
            command: command.kind(),
            target: command.target().to_owned(),
            result: receipt.outcome.as_str(),
            term: Some(receipt.term),
            index: Some(receipt.index),
            old_voters: receipt.old_voters.clone(),
            new_voters: receipt.new_voters.clone(),
        }
    }

    fn replayed(actor: &str, command: &ControlCommand, receipt: &CommandReceipt) -> Self {
        Self {
            result: "replayed",
            ..Self::committed(actor, command, receipt)
        }
    }

    fn failed(actor: &str, command: &ControlCommand, error: &ControlError) -> Self {
        Self {
            actor: actor.to_owned(),
            command: command.kind(),
            target: command.target().to_owned(),
            result: error.kind(),
            term: None,
            index: None,
            old_voters: Vec::new(),
            new_voters: Vec::new(),
        }
    }

    fn emit(&self) {
        tracing::info!(
            actor = %self.actor,
            command = self.command,
            target = %self.target,
            result = self.result,
            term = ?self.term,
            index = ?self.index,
            old_voters = ?self.old_voters,
            new_voters = ?self.new_voters,
            "availability control command",
        );
    }
}

/// The recent command latencies reported through the status resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CommandMetrics {
    /// The total commands the plane has completed since start, replays excluded.
    pub completed: u64,
    /// The median command latency over the retained window, in milliseconds.
    pub p50_ms: i64,
    /// The 99th-percentile command latency over the retained window, in milliseconds. Reported so a
    /// latency spike through a leader change is visible on the status resource.
    pub p99_ms: i64,
}

/// The bounded recent history the plane keeps behind one lock: the idempotency receipts and the latency
/// samples, each a recent-history window rather than a growing ledger.
#[derive(Default)]
struct History {
    receipts: VecDeque<(String, CommandReceipt)>,
    latencies: VecDeque<i64>,
}

/// Wraps a [`MembershipControl`] with idempotency, a concurrency bound, audit, and latency reporting.
pub struct ControlPlane {
    control: Arc<dyn MembershipControl>,
    clock: Clock,
    permits: Semaphore,
    retained: usize,
    completed: std::sync::atomic::AtomicU64,
    history: Mutex<History>,
}

impl ControlPlane {
    /// Wrap `control`, reading time from `clock`, with the default concurrency and retention bounds.
    #[must_use]
    pub fn new(control: Arc<dyn MembershipControl>, clock: Clock) -> Self {
        Self::with_limits(control, clock, MAX_CONCURRENT_COMMANDS, RETAINED)
    }

    fn with_limits(control: Arc<dyn MembershipControl>, clock: Clock, concurrency: usize, retained: usize) -> Self {
        Self {
            control,
            clock,
            permits: Semaphore::new(concurrency),
            retained,
            completed: std::sync::atomic::AtomicU64::new(0),
            history: Mutex::new(History::default()),
        }
    }

    /// Run `command` for `actor`, deduplicating on `key` and auditing the attempt.
    ///
    /// A `key` already committed returns its receipt without resubmitting, so a client that retries after
    /// a leader loss reads one committed result. Otherwise the command runs under a concurrency permit,
    /// its latency is recorded, its receipt is retained under `key`, and one audit line names the actor,
    /// command, target, result, and committed identity.
    ///
    /// # Errors
    /// Returns [`ControlError::Overloaded`] when the concurrency bound is saturated, or the
    /// [`ControlError`] the submission produced.
    pub async fn execute(
        &self,
        actor: &str,
        key: Option<&str>,
        command: ControlCommand,
    ) -> Result<CommandReceipt, ControlError> {
        if let Some(key) = key
            && let Some(receipt) = self.replay(key)
        {
            AuditRecord::replayed(actor, &command, &receipt).emit();
            return Ok(receipt);
        }
        let Ok(_permit) = self.permits.try_acquire() else {
            let error = ControlError::Overloaded;
            AuditRecord::failed(actor, &command, &error).emit();
            return Err(error);
        };
        let started = (self.clock)();
        let result = self.control.submit(command.clone()).await;
        self.record(((self.clock)() - started).max(0));
        match &result {
            Ok(receipt) => {
                if let Some(key) = key {
                    self.remember(key, receipt.clone());
                }
                AuditRecord::committed(actor, &command, receipt).emit();
            }
            Err(error) => AuditRecord::failed(actor, &command, error).emit(),
        }
        result
    }

    /// The recent command latencies for the status resource.
    #[must_use]
    pub fn metrics(&self) -> CommandMetrics {
        let history = self.lock();
        CommandMetrics {
            completed: self.completed.load(std::sync::atomic::Ordering::Relaxed),
            p50_ms: percentile(&history.latencies, 50),
            p99_ms: percentile(&history.latencies, 99),
        }
    }

    /// Lock the history, recovering the guard rather than panicking if a prior holder poisoned it: the
    /// guarded data is bounded queues no panic can leave inconsistent.
    fn lock(&self) -> std::sync::MutexGuard<'_, History> {
        self.history.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn replay(&self, key: &str) -> Option<CommandReceipt> {
        self.lock()
            .receipts
            .iter()
            .find(|(retained, _)| retained == key)
            .map(|(_, receipt)| receipt.clone())
    }

    fn remember(&self, key: &str, receipt: CommandReceipt) {
        push_bounded(&mut self.lock().receipts, (key.to_owned(), receipt), self.retained);
    }

    fn record(&self, latency: i64) {
        self.completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        push_bounded(&mut self.lock().latencies, latency, self.retained);
    }
}

/// Push `item` onto `queue`, evicting from the front until it holds at most `cap`, so a recent-history
/// window never grows past its bound.
fn push_bounded<T>(queue: &mut VecDeque<T>, item: T, cap: usize) {
    queue.push_back(item);
    while queue.len() > cap {
        queue.pop_front();
    }
}

/// The nearest-rank `pct`th percentile of `samples`, or zero when the window is empty.
fn percentile(samples: &VecDeque<i64>, pct: usize) -> i64 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted: Vec<i64> = samples.iter().copied().collect();
    sorted.sort_unstable();
    let rank = pct * sorted.len();
    let index = (rank.div_ceil(100)).clamp(1, sorted.len()) - 1;
    sorted[index]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use tokio::sync::Notify;

    use super::{
        AuditRecord, CommandOutcome, CommandReceipt, ControlCommand, ControlError, ControlPlane, MembershipControl,
        percentile, plan_voter_roster,
    };
    use crate::state::Clock;
    use std::collections::{BTreeSet, VecDeque};

    fn receipt(index: u64) -> CommandReceipt {
        CommandReceipt {
            term: 1,
            index,
            outcome: CommandOutcome::Committed,
            old_voters: Vec::new(),
            new_voters: Vec::new(),
        }
    }

    /// A control double returning a scripted result, counting its submissions so a test can prove a
    /// replay never reached it.
    struct ScriptedControl {
        results: Mutex<VecDeque<Result<CommandReceipt, ControlError>>>,
        submissions: Mutex<Vec<ControlCommand>>,
    }

    impl ScriptedControl {
        fn new(results: impl IntoIterator<Item = Result<CommandReceipt, ControlError>>) -> Arc<Self> {
            Arc::new(Self {
                results: Mutex::new(results.into_iter().collect()),
                submissions: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl MembershipControl for ScriptedControl {
        async fn submit(&self, command: ControlCommand) -> Result<CommandReceipt, ControlError> {
            self.submissions.lock().unwrap().push(command);
            self.results
                .lock()
                .unwrap()
                .pop_front()
                .expect("the scripted control ran out of results")
        }
    }

    /// A control double that blocks inside `submit` until released, so a test can hold a permit and drive
    /// the concurrency bound deterministically.
    struct GatedControl {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait::async_trait]
    impl MembershipControl for GatedControl {
        async fn submit(&self, _command: ControlCommand) -> Result<CommandReceipt, ControlError> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(receipt(1))
        }
    }

    fn fixed_clock() -> Clock {
        Arc::new(|| 0)
    }

    /// A clock that returns each scripted reading in turn, holding the last, so a command's start and end
    /// timestamps are deterministic.
    fn scripted_clock(readings: impl IntoIterator<Item = i64>) -> Clock {
        let readings = Mutex::new(readings.into_iter().collect::<VecDeque<_>>());
        Arc::new(move || {
            let mut readings = readings.lock().unwrap();
            let next = readings.pop_front().expect("the scripted clock ran out of readings");
            if readings.is_empty() {
                readings.push_back(next);
            }
            next
        })
    }

    fn transfer() -> ControlCommand {
        ControlCommand::TransferAuthority {
            authority: "proj".to_owned(),
            new_home: "west".to_owned(),
        }
    }

    #[tokio::test]
    async fn test_execute_returns_the_committed_receipt() {
        let control = ScriptedControl::new([Ok(receipt(7))]);
        let plane = ControlPlane::new(control.clone(), fixed_clock());

        let committed = plane.execute("alice", Some("k1"), transfer()).await.unwrap();

        assert_eq!(committed, receipt(7));
        assert_eq!(control.submissions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_a_repeated_key_returns_one_committed_result_without_resubmitting() {
        // The second submission would fail; a replay must return the first receipt instead of reaching it.
        let control = ScriptedControl::new([Ok(receipt(7)), Err(ControlError::Unavailable("gone".to_owned()))]);
        let plane = ControlPlane::new(control.clone(), fixed_clock());

        let first = plane.execute("alice", Some("k1"), transfer()).await.unwrap();
        let replay = plane.execute("alice", Some("k1"), transfer()).await.unwrap();

        assert_eq!(first, replay);
        assert_eq!(
            control.submissions.lock().unwrap().len(),
            1,
            "the replay never reached the control"
        );
    }

    #[tokio::test]
    async fn test_a_replay_survives_a_leader_loss_after_commit() {
        // The command commits, then the node loses leadership; a retry on the same key reads the committed
        // receipt rather than the not-leader error a fresh submission would now hit.
        let control = ScriptedControl::new([Ok(receipt(3)), Err(ControlError::NotLeader { leader: None })]);
        let plane = ControlPlane::new(control, fixed_clock());

        let committed = plane.execute("alice", Some("k1"), transfer()).await.unwrap();
        let after_loss = plane.execute("alice", Some("k1"), transfer()).await.unwrap();

        assert_eq!(committed, after_loss);
    }

    #[tokio::test]
    async fn test_a_keyless_command_never_deduplicates() {
        let control = ScriptedControl::new([Ok(receipt(1)), Ok(receipt(2))]);
        let plane = ControlPlane::new(control.clone(), fixed_clock());

        plane.execute("alice", None, transfer()).await.unwrap();
        plane.execute("alice", None, transfer()).await.unwrap();

        assert_eq!(control.submissions.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_a_failure_is_returned_and_not_cached() {
        let control = ScriptedControl::new([Err(ControlError::Invalid("same home".to_owned())), Ok(receipt(5))]);
        let plane = ControlPlane::new(control, fixed_clock());

        let failed = plane.execute("alice", Some("k1"), transfer()).await;
        let retried = plane.execute("alice", Some("k1"), transfer()).await.unwrap();

        assert_eq!(failed, Err(ControlError::Invalid("same home".to_owned())));
        assert_eq!(retried, receipt(5), "a failure leaves the key open to a later commit");
    }

    #[rstest::rstest]
    #[case::not_leader(ControlError::NotLeader { leader: None })]
    #[case::unavailable(ControlError::Unavailable("gone".to_owned()))]
    #[case::invalid(ControlError::Invalid("same home".to_owned()))]
    #[tokio::test]
    async fn test_each_failure_kind_is_returned_and_audited(#[case] error: ControlError) {
        // Each failure runs the audit path, so its kind is named without leaking the request body.
        let control = ScriptedControl::new([Err(error.clone())]);
        let plane = ControlPlane::new(control, fixed_clock());

        assert_eq!(plane.execute("alice", None, transfer()).await, Err(error));
    }

    #[tokio::test]
    async fn test_the_concurrency_bound_rejects_an_excess_command() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let control = Arc::new(GatedControl {
            entered: entered.clone(),
            release: release.clone(),
        });
        let plane = Arc::new(ControlPlane::with_limits(control, fixed_clock(), 1, 8));

        let held = plane.clone();
        let holding = tokio::spawn(async move { held.execute("alice", None, transfer()).await });
        entered.notified().await;

        let overflow = plane.execute("bob", None, transfer()).await;
        assert_eq!(overflow, Err(ControlError::Overloaded));

        release.notify_one();
        assert_eq!(holding.await.unwrap().unwrap(), receipt(1));
    }

    #[tokio::test]
    async fn test_metrics_report_the_completed_count_and_latency_percentiles() {
        let control = ScriptedControl::new([Ok(receipt(1)), Ok(receipt(2)), Ok(receipt(3)), Ok(receipt(4))]);
        // Each command reads the clock at start and end: latencies 1, 5, 10, 100 ms.
        let plane = ControlPlane::new(control, scripted_clock([0, 1, 0, 5, 0, 10, 0, 100]));

        for _ in 0..4 {
            plane.execute("alice", None, transfer()).await.unwrap();
        }

        let metrics = plane.metrics();
        assert_eq!(metrics.completed, 4);
        assert_eq!(metrics.p50_ms, 5);
        assert_eq!(metrics.p99_ms, 100);
    }

    #[tokio::test]
    async fn test_the_idempotency_window_evicts_the_oldest_receipt() {
        let results = (0..3).map(|index| Ok(receipt(index))).chain([Ok(receipt(99))]);
        let control = ScriptedControl::new(results);
        let plane = ControlPlane::with_limits(control.clone(), fixed_clock(), 4, 2);

        for key in ["k0", "k1", "k2"] {
            plane.execute("alice", Some(key), transfer()).await.unwrap();
        }
        // k0 fell out of the two-slot window, so its retry resubmits rather than replaying.
        plane.execute("alice", Some("k0"), transfer()).await.unwrap();

        assert_eq!(control.submissions.lock().unwrap().len(), 4);
    }

    #[test]
    fn test_percentile_of_an_empty_window_is_zero() {
        assert_eq!(percentile(&VecDeque::new(), 99), 0);
    }

    #[test]
    fn test_plan_voter_roster_adds_and_removes() {
        let current = BTreeSet::from([1, 2, 3]);

        assert_eq!(plan_voter_roster(&current, Some(4), None), BTreeSet::from([1, 2, 3, 4]));
        assert_eq!(plan_voter_roster(&current, None, Some(2)), BTreeSet::from([1, 3]));
        assert_eq!(plan_voter_roster(&current, Some(4), Some(1)), BTreeSet::from([2, 3, 4]));
    }

    #[test]
    fn test_plan_voter_roster_is_a_no_op_for_a_present_add_or_absent_remove() {
        let current = BTreeSet::from([1, 2]);

        assert_eq!(plan_voter_roster(&current, Some(1), Some(9)), current);
    }

    #[test]
    fn test_audit_records_name_the_command_without_the_body() {
        let committed = AuditRecord::committed("alice", &transfer(), &receipt(4));
        assert_eq!(committed.actor, "alice");
        assert_eq!(committed.command, "transfer_authority");
        assert_eq!(committed.target, "proj");
        assert_eq!(committed.result, "committed");
        assert_eq!(committed.term, Some(1));
        assert_eq!(committed.index, Some(4));

        let learner = ControlCommand::AddLearner {
            datacenter: "west".to_owned(),
            address: "west.internal:4460".to_owned(),
        };
        let failed = AuditRecord::failed("alice", &learner, &ControlError::NotLeader { leader: None });
        assert_eq!(failed.command, "add_learner");
        assert_eq!(failed.target, "west");
        assert_eq!(failed.result, "not_leader");
        assert_eq!(failed.term, None);
        let rendered = serde_json::to_string(&failed).unwrap();
        assert!(
            !rendered.contains("west.internal:4460"),
            "the audit record must not carry the request body: {rendered}"
        );
    }

    #[test]
    fn test_a_no_change_receipt_audits_as_no_change() {
        let no_change = CommandReceipt {
            term: 2,
            index: 8,
            outcome: CommandOutcome::NoChange,
            old_voters: Vec::new(),
            new_voters: Vec::new(),
        };
        let record = AuditRecord::committed("alice", &transfer(), &no_change);
        assert_eq!(record.result, "no_change");
    }

    #[test]
    fn test_a_committed_membership_receipt_audits_the_old_and_new_voter_sets() {
        let promote = ControlCommand::PromoteVoter {
            datacenter: "west".to_owned(),
        };
        let receipt = CommandReceipt {
            term: 3,
            index: 12,
            outcome: CommandOutcome::Committed,
            old_voters: vec!["east".to_owned()],
            new_voters: vec!["east".to_owned(), "west".to_owned()],
        };
        let record = AuditRecord::committed("alice", &promote, &receipt);
        assert_eq!(record.old_voters, ["east"]);
        assert_eq!(record.new_voters, ["east", "west"]);

        // A failed command carries no roster transition.
        let failed = AuditRecord::failed("alice", &promote, &ControlError::NotLeader { leader: None });
        assert!(failed.old_voters.is_empty() && failed.new_voters.is_empty());
    }

    #[test]
    fn test_a_replay_audit_names_the_replay_result() {
        let record = AuditRecord::replayed("alice", &transfer(), &receipt(4));
        assert_eq!(record.result, "replayed");
        assert_eq!(record.index, Some(4));
    }

    #[test]
    fn test_command_kind_and_target_cover_every_variant() {
        for (command, kind, target) in [
            (
                ControlCommand::PromoteVoter {
                    datacenter: "west".to_owned(),
                },
                "promote_voter",
                "west",
            ),
            (
                ControlCommand::RemoveVoter {
                    datacenter: "west".to_owned(),
                },
                "remove_voter",
                "west",
            ),
            (
                ControlCommand::ReplaceVoter {
                    remove: "east".to_owned(),
                    datacenter: "west".to_owned(),
                    address: "west.internal:4460".to_owned(),
                },
                "replace_voter",
                "west",
            ),
            (
                ControlCommand::AdvanceEpoch {
                    authority: "proj".to_owned(),
                },
                "advance_epoch",
                "proj",
            ),
        ] {
            assert_eq!(command.kind(), kind);
            assert_eq!(command.target(), target);
        }
    }

    #[test]
    fn test_control_error_messages_name_the_cause() {
        assert_eq!(
            ControlError::NotLeader {
                leader: Some("east.internal:4460".to_owned()),
            }
            .to_string(),
            "not the consensus leader; leader at east.internal:4460",
        );
        assert_eq!(
            ControlError::NotLeader { leader: None }.to_string(),
            "not the consensus leader"
        );
        assert_eq!(
            ControlError::Unavailable("log gone".to_owned()).to_string(),
            "consensus command did not commit: log gone",
        );
        assert_eq!(
            ControlError::Invalid("same home".to_owned()).to_string(),
            "invalid command: same home",
        );
        assert_eq!(
            ControlError::Overloaded.to_string(),
            "too many concurrent availability commands in flight",
        );
    }

    #[test]
    fn test_the_outcome_serializes_to_its_snake_case_name() {
        assert_eq!(
            serde_json::to_string(&CommandOutcome::NoChange).unwrap(),
            "\"no_change\"",
        );
    }
}

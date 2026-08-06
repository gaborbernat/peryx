//! Driving a fenced authority transfer from a plan to a sealed, persisted audit.
//!
//! [`TransferPlan`](peryx_replication::TransferPlan) is the pure decision core: it waits at
//! `AwaitingCatchUp` until the target's applied frontier reaches the barrier, then stands `Ready` for a
//! caller to commit. This module supplies the two things it reaches for from above — the target
//! datacenter's applied frontier and the committed move — without pulling any I/O into the plan itself.
//!
//! [`observe_target`] probes the target's frontier once and folds it into the plan, so a loop above can
//! poll it toward `Ready`. [`commit_transfer`] takes a ready plan, commits the move through the ownership
//! consensus group, reads the epoch that commit minted back from committed state, seals the audit, and
//! persists it. Splitting the two keeps the barrier wait and the single committed move distinct, so a
//! cancel that races the commit resolves against the plan rather than a half-applied move.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use peryx_driver::state::{ControlCommand, ControlError, ControlPlane, OwnershipAuthority};
use peryx_replication::{
    AuthorityEpoch, BatchRequest, DEFAULT_TRANSFER_LIMITS, HttpPeerTransport, PeerTransport, TransferAudit,
    TransferError, TransferPhase, TransferPlan,
};
use peryx_storage::meta::{MetaError, MetaStore, TransferAudit as StoredTransferAudit};

/// How long a frontier probe waits for the target's change-feed to answer before it reads as unreachable.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Reads a datacenter's applied metadata frontier, the highest serial it has durably applied, so the
/// barrier gate can tell when the target has replicated the source's writes.
///
/// The production source probes the target's change-feed; a test double returns a scripted frontier.
#[async_trait::async_trait]
pub trait FrontierSource: Send + Sync {
    /// The highest metadata serial `datacenter` has durably applied, or `None` when it cannot be reached
    /// or has applied nothing yet.
    ///
    /// # Errors
    /// Returns an error when the target cannot be reached or answers unusably.
    async fn applied_frontier(&self, datacenter: &str) -> anyhow::Result<Option<u64>>;
}

/// Reads the committed authority epoch back from ownership state after a move commits.
///
/// A narrow read seam over the ownership consensus group so the commit path derives the epoch the audit
/// records from committed state rather than the receipt.
#[async_trait::async_trait]
pub trait EpochOracle: Send + Sync {
    /// The committed epoch for `authority`, the value the just-committed move minted.
    async fn committed_epoch(&self, authority: &str) -> u64;
}

/// Why driving a transfer did not seal an audit.
#[derive(Debug, thiserror::Error)]
pub enum TransferDriveError {
    /// Reading the target's applied frontier failed.
    #[error("read the target frontier: {0}")]
    Frontier(#[source] anyhow::Error),
    /// The consensus group refused the committed move.
    #[error("commit the transfer: {0}")]
    Commit(#[source] ControlError),
    /// The plan refused the commit: it was cancelled, or the barrier was not met.
    #[error("seal the transfer: {0}")]
    Plan(#[source] TransferError),
    /// Persisting the sealed audit failed.
    #[error("persist the transfer audit: {0}")]
    Persist(#[source] MetaError),
}

/// Probe the target's applied frontier and record it on `plan`, returning where the plan now stands.
///
/// A target that cannot be reached reads as frontier zero, which never advances a plan past its barrier,
/// so an unreachable target leaves the move waiting rather than committing it early.
///
/// # Errors
/// Returns [`TransferDriveError::Frontier`] when the frontier read fails.
pub async fn observe_target(
    plan: &mut TransferPlan,
    frontier: &dyn FrontierSource,
) -> Result<TransferPhase, TransferDriveError> {
    let applied = frontier
        .applied_frontier(&plan.request().target.0)
        .await
        .map_err(TransferDriveError::Frontier)?
        .unwrap_or(0);
    Ok(plan.observe_frontier(applied))
}

/// Commit a ready `plan` through the ownership consensus group, derive the epoch the commit minted from
/// committed state, seal the audit, and persist it.
///
/// The commit rides [`ControlPlane::execute`], so an `Idempotency-Key` deduplicates a retry across a
/// leader loss to one committed move, and the committed log index it returns is the index the audit
/// records. The epoch comes from a read of committed ownership state rather than the receipt, matching the
/// rest of the ownership plane, which surfaces effects through committed reads.
///
/// # Errors
/// Returns [`TransferDriveError`] when the commit is refused, the plan refuses the commit, or persisting
/// the audit fails.
pub async fn commit_transfer(
    plan: &mut TransferPlan,
    control: &ControlPlane,
    ownership: &dyn EpochOracle,
    meta: &MetaStore,
    key: Option<&str>,
) -> Result<TransferAudit, TransferDriveError> {
    // Resolve the plan's standing before reaching for consensus: only a ready plan submits the move, so a
    // cancel that won the race, or a barrier not yet met, never commits a move the plan already refused.
    let (actor, command) = match plan.phase() {
        TransferPhase::Ready => {
            let request = plan.request();
            (
                request.actor.clone(),
                ControlCommand::TransferAuthority {
                    authority: request.authority.0.clone(),
                    new_home: request.target.0.clone(),
                },
            )
        }
        // Already sealed: return the audit the first commit booked without submitting a second move. The
        // epoch and index are ignored for a committed plan, which reads back its existing record.
        TransferPhase::Committed => return plan.commit(AuthorityEpoch(0), 0).map_err(TransferDriveError::Plan),
        TransferPhase::Cancelled => return Err(TransferDriveError::Plan(TransferError::Cancelled)),
        TransferPhase::AwaitingCatchUp => return Err(TransferDriveError::Plan(TransferError::BarrierNotMet)),
    };
    let receipt = control
        .execute(&actor, key, command)
        .await
        .map_err(TransferDriveError::Commit)?;
    let epoch = ownership.committed_epoch(&plan.request().authority.0).await;
    let audit = plan
        .commit(AuthorityEpoch(epoch), receipt.index)
        .map_err(TransferDriveError::Plan)?;
    meta.record_transfer_audit(&stored(&audit))
        .map_err(TransferDriveError::Persist)?;
    Ok(audit)
}

/// The ownership consensus group reads back a committed epoch, so a driver can derive it from committed
/// state after a move without threading it through the receipt.
#[async_trait::async_trait]
impl EpochOracle for Arc<dyn OwnershipAuthority> {
    async fn committed_epoch(&self, authority: &str) -> u64 {
        OwnershipAuthority::committed_epoch(self.as_ref(), authority).await
    }
}

/// Probes a datacenter's change-feed for its applied frontier, resolving the datacenter to a base address
/// from the availability roster.
///
/// The feed's `current_serial` is the highest metadata serial that datacenter has durably applied. A
/// datacenter absent from the roster, or one whose feed cannot be reached or has synced nothing, reads as
/// no frontier so the barrier gate keeps the move waiting rather than committing to a target that has not
/// caught up.
pub struct RosterFrontierSource {
    peers: Vec<(String, String)>,
    token: String,
}

impl RosterFrontierSource {
    /// Build a source over `peers`, each a `(datacenter, base URL)` pair, authenticating every probe with
    /// `token`.
    #[must_use]
    pub fn new(peers: Vec<(String, String)>, token: impl Into<String>) -> Self {
        Self {
            peers,
            token: token.into(),
        }
    }
}

#[async_trait::async_trait]
impl FrontierSource for RosterFrontierSource {
    async fn applied_frontier(&self, datacenter: &str) -> anyhow::Result<Option<u64>> {
        let Some((_, address)) = self.peers.iter().find(|(name, _)| name == datacenter) else {
            return Ok(None);
        };
        let transport = HttpPeerTransport::new(address, self.token.clone(), DEFAULT_TRANSFER_LIMITS, PROBE_TIMEOUT)?;
        let request = BatchRequest {
            after: 0,
            max_operations: NonZeroUsize::new(1).expect("1 is non-zero"),
        };
        // An unreachable target, or one that has synced no source yet, reads as no frontier so the move
        // keeps waiting rather than committing to a target that has not caught up.
        transport
            .fetch_batch(request)
            .await
            .map_or_else(|_| Ok(None), |frame| Ok(Some(frame.frontier().1)))
    }
}

/// Flatten a sealed audit into the primitive record the store persists.
fn stored(audit: &TransferAudit) -> StoredTransferAudit {
    StoredTransferAudit {
        authority: audit.authority.0.clone(),
        source: audit.source.0.clone(),
        target: audit.target.0.clone(),
        actor: audit.actor.clone(),
        reason: audit.reason.clone(),
        barrier: audit.barrier,
        epoch: audit.epoch.0,
        commit_index: audit.commit_index,
    }
}

#[cfg(test)]
mod tests;

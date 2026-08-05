//! The ownership consensus group as the mutation path sees it.
//!
//! Authoritative first-publish home assignment runs through a Raft group, but this neutral crate carries
//! no consensus dependency. The binary implements [`OwnershipAuthority`] over the concrete node and
//! registers it on the [`ServingState`](crate::state::ServingState); a process running no group registers
//! nothing and the mutation path skips the claim.

/// What a first-publish home claim resolved to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HomeClaim {
    /// This publish assigned the authority's home to the local datacenter.
    AssignedHere,
    /// The authority already had a home before this publish; the first winner keeps it.
    AlreadyHomed,
}

/// The committed result of moving an authority's home to a new datacenter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferOutcome {
    /// The datacenter that held the home before the transfer.
    pub from: String,
    /// The datacenter the home moved to.
    pub to: String,
    /// The epoch the transfer minted, which fences the old home's stale-epoch writes.
    pub epoch: u64,
}

/// A snapshot of the ownership consensus group this node observes, for the availability status resource.
///
/// It names voters by their consensus id and datacenter, never their peer address, so the status surface
/// exposes membership without leaking the internal transport topology.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ClusterStatus {
    /// The datacenter of the current leader, or `None` when this node knows of none.
    pub leader: Option<String>,
    /// The current leadership term, the group's monotonic authority epoch.
    pub term: u64,
    /// The datacenters this node's committed membership holds as voters.
    pub voters: Vec<String>,
}

/// Why a home claim could not commit.
#[derive(Debug, thiserror::Error)]
pub enum OwnershipError {
    /// This node is not the group leader, so it cannot commit the claim itself. Carries the leader's
    /// address when the group knows one, for a caller that forwards the claim.
    #[error("not the ownership leader{}", .leader.as_deref().map(|a| format!("; leader at {a}")).unwrap_or_default())]
    NotLeader {
        /// The current leader's advertised address, when known.
        leader: Option<String>,
    },
    /// The group rejected or could not commit the claim for another reason.
    #[error("ownership claim did not commit: {0}")]
    Unavailable(String),
}

/// The ownership consensus group a writer submits home assignments to.
#[async_trait::async_trait]
pub trait OwnershipAuthority: Send + Sync {
    /// Whether `authority` already has a committed home this node has applied.
    ///
    /// A cheap local read that lets a caller skip a redundant claim for an already-homed authority. It is
    /// current on the leader and may lag on a follower, so a stale `false` costs one rejected claim, never
    /// a wrong home.
    async fn has_home(&self, authority: &str) -> bool;

    /// Claim `authority`'s home for the local datacenter on its first publish, reporting whether this
    /// call won it. Idempotent: a repeat publish, or a race another datacenter already won, reports
    /// [`HomeClaim::AlreadyHomed`].
    ///
    /// # Errors
    /// Returns [`OwnershipError`] when the claim cannot commit, for example when this node is not the
    /// leader and cannot reach it.
    async fn claim_home(&self, authority: &str) -> Result<HomeClaim, OwnershipError>;

    /// A snapshot of the group this node observes — leader, term, and voter membership — for the
    /// availability status resource. Read from local metrics, so it is current on the leader and may lag
    /// on a follower.
    fn cluster_status(&self) -> ClusterStatus;

    /// The committed authority epoch for `authority`, or `0` when no committed command has homed it.
    ///
    /// The fence value a writer stamps onto the work it produces: a background job reads it at lease time
    /// so a former holder's stale-epoch write is fenced out once the authority advances. A local read,
    /// current on the leader and possibly behind on a follower.
    async fn committed_epoch(&self, authority: &str) -> u64;

    /// Whether work carrying `presented` under `authority` may still proceed against the committed epoch.
    ///
    /// Admits only the current committed epoch, so a stale epoch below it — a former holder's, after the
    /// authority advanced — is fenced. An unassigned authority (epoch `0`) admits nothing.
    async fn admit_epoch(&self, authority: &str, presented: u64) -> bool;

    /// Move `authority`'s home to `new_home` on the control quorum, minting the next epoch that fences the
    /// old home's stale-epoch writes. Reports the committed [`TransferOutcome`], or `None` when the
    /// authority was unassigned or already homed there, so nothing moved.
    ///
    /// # Errors
    /// [`OwnershipError::NotLeader`] when this node cannot commit — a control minority cannot transfer
    /// authority — or [`OwnershipError::Unavailable`] when the commit otherwise fails.
    async fn transfer_home(&self, authority: &str, new_home: &str) -> Result<Option<TransferOutcome>, OwnershipError>;
}

/// Claim `authority`'s home on its first publish, best effort, when this process runs a group.
///
/// Skips the claim when no group runs or the authority is already homed, so the common repeat-publish
/// case costs one local read and no consensus round. A claim that cannot commit is logged, never
/// surfaced, so a publish is not blocked on consensus reachability; a node that is not the leader logs
/// and leaves the home to a leader-side claim.
pub(super) async fn claim_first_publish_home(group: Option<&std::sync::Arc<dyn OwnershipAuthority>>, authority: &str) {
    let Some(group) = group else { return };
    if group.has_home(authority).await {
        return;
    }
    if let Err(error) = group.claim_home(authority).await {
        tracing::warn!(%error, authority, "first-publish home claim did not commit");
    }
}

/// The committed authority epoch for `authority`, or `0` when this process runs no group.
///
/// The fence a writer stamps onto its work. A process with no group holds no epoch, reported as the
/// unassigned `0` sentinel the placement fence reads as closed.
pub(super) async fn committed_authority_epoch(
    group: Option<&std::sync::Arc<dyn OwnershipAuthority>>,
    authority: &str,
) -> u64 {
    match group {
        Some(group) => group.committed_epoch(authority).await,
        None => 0,
    }
}

/// Whether work carrying `presented` under `authority` may still be written against the committed epoch.
///
/// A process with no group has no authority to supersede its work, so it admits everything; a running
/// group fences any epoch below its committed one.
pub(super) async fn admit_authority_epoch(
    group: Option<&std::sync::Arc<dyn OwnershipAuthority>>,
    authority: &str,
    presented: u64,
) -> bool {
    match group {
        Some(group) => group.admit_epoch(authority, presented).await,
        None => true,
    }
}

/// Move `authority`'s home to `new_home` on the control quorum, or report the group is absent.
///
/// A process with no group cannot commit a transfer, so it returns `Ok(None)` (nothing moved); a running
/// group commits the fenced move and returns the [`TransferOutcome`], or the [`OwnershipError`] the
/// commit failed with — a control minority surfaces as [`OwnershipError::NotLeader`].
///
/// # Errors
/// The [`OwnershipError`] a running group's commit failed with.
pub(super) async fn transfer_authority_home(
    group: Option<&std::sync::Arc<dyn OwnershipAuthority>>,
    authority: &str,
    new_home: &str,
) -> Result<Option<TransferOutcome>, OwnershipError> {
    match group {
        Some(group) => group.transfer_home(authority, new_home).await,
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        ClusterStatus, HomeClaim, OwnershipAuthority, OwnershipError, TransferOutcome, admit_authority_epoch,
        claim_first_publish_home, committed_authority_epoch, transfer_authority_home,
    };

    struct Fake {
        homed: bool,
        claim: Result<HomeClaim, OwnershipError>,
        epoch: u64,
        transfer: Result<Option<TransferOutcome>, OwnershipError>,
    }

    fn clone_ownership_error(error: &OwnershipError) -> OwnershipError {
        match error {
            OwnershipError::NotLeader { leader } => OwnershipError::NotLeader { leader: leader.clone() },
            OwnershipError::Unavailable(reason) => OwnershipError::Unavailable(reason.clone()),
        }
    }

    #[async_trait::async_trait]
    impl OwnershipAuthority for Fake {
        async fn has_home(&self, _authority: &str) -> bool {
            self.homed
        }

        async fn claim_home(&self, _authority: &str) -> Result<HomeClaim, OwnershipError> {
            match &self.claim {
                Ok(outcome) => Ok(*outcome),
                Err(OwnershipError::NotLeader { leader }) => Err(OwnershipError::NotLeader { leader: leader.clone() }),
                Err(OwnershipError::Unavailable(reason)) => Err(OwnershipError::Unavailable(reason.clone())),
            }
        }

        fn cluster_status(&self) -> ClusterStatus {
            ClusterStatus {
                leader: Some("east".to_owned()),
                term: 3,
                voters: vec!["east".to_owned()],
            }
        }

        async fn committed_epoch(&self, _authority: &str) -> u64 {
            self.epoch
        }

        async fn admit_epoch(&self, _authority: &str, presented: u64) -> bool {
            self.epoch != 0 && presented == self.epoch
        }

        async fn transfer_home(
            &self,
            _authority: &str,
            _new_home: &str,
        ) -> Result<Option<TransferOutcome>, OwnershipError> {
            match &self.transfer {
                Ok(outcome) => Ok(outcome.clone()),
                Err(error) => Err(clone_ownership_error(error)),
            }
        }
    }

    fn group(homed: bool, claim: Result<HomeClaim, OwnershipError>) -> Arc<dyn OwnershipAuthority> {
        Arc::new(Fake {
            homed,
            claim,
            epoch: 7,
            transfer: Ok(None),
        })
    }

    fn transferring_group(transfer: Result<Option<TransferOutcome>, OwnershipError>) -> Arc<dyn OwnershipAuthority> {
        Arc::new(Fake {
            homed: true,
            claim: Ok(HomeClaim::AlreadyHomed),
            epoch: 7,
            transfer,
        })
    }

    #[tokio::test]
    async fn test_first_publish_claims_when_unhomed() {
        // An unhomed authority under a running group triggers a claim; the Ok path is the assignment.
        claim_first_publish_home(Some(&group(false, Ok(HomeClaim::AssignedHere))), "proj").await;
    }

    #[tokio::test]
    async fn test_first_publish_skips_an_already_homed_authority() {
        // A homed authority never reaches claim_home, so a claim error here would be a bug if it ran.
        claim_first_publish_home(
            Some(&group(
                true,
                Err(OwnershipError::Unavailable("must not run".to_owned())),
            )),
            "proj",
        )
        .await;
    }

    #[tokio::test]
    async fn test_first_publish_swallows_a_claim_failure() {
        // A claim that cannot commit is logged, not surfaced, so the publish is never blocked.
        claim_first_publish_home(
            Some(&group(false, Err(OwnershipError::NotLeader { leader: None }))),
            "proj",
        )
        .await;
    }

    #[tokio::test]
    async fn test_first_publish_is_a_no_op_without_a_group() {
        claim_first_publish_home(None, "proj").await;
    }

    #[tokio::test]
    async fn test_committed_epoch_reads_the_running_group() {
        let group = group(true, Ok(HomeClaim::AlreadyHomed));
        assert_eq!(committed_authority_epoch(Some(&group), "proj").await, 7);
    }

    #[tokio::test]
    async fn test_committed_epoch_is_the_unassigned_sentinel_without_a_group() {
        assert_eq!(committed_authority_epoch(None, "proj").await, 0);
    }

    #[tokio::test]
    async fn test_admit_epoch_admits_the_committed_epoch_and_fences_a_stale_one() {
        // The Fake commits epoch 7, so the current epoch is admitted and the superseded one below it is not.
        let group = group(true, Ok(HomeClaim::AlreadyHomed));
        assert!(admit_authority_epoch(Some(&group), "proj", 7).await);
        assert!(!admit_authority_epoch(Some(&group), "proj", 6).await);
    }

    #[tokio::test]
    async fn test_admit_epoch_admits_everything_without_a_group() {
        assert!(admit_authority_epoch(None, "proj", 6).await);
    }

    #[tokio::test]
    async fn test_committed_epoch_reports_the_group_epoch() {
        let group = group(false, Ok(HomeClaim::AlreadyHomed));

        assert_eq!(group.committed_epoch("proj").await, 7);
    }

    #[tokio::test]
    async fn test_transfer_commits_the_move_through_the_group() {
        let outcome = TransferOutcome {
            from: "east".to_owned(),
            to: "west".to_owned(),
            epoch: 2,
        };
        let group = transferring_group(Ok(Some(outcome.clone())));

        let moved = transfer_authority_home(Some(&group), "proj", "west").await.expect("commits");
        assert_eq!(moved, Some(outcome));
    }

    #[tokio::test]
    async fn test_transfer_by_a_control_minority_is_not_the_leader() {
        // A minority cannot commit the transfer, so the group forwards to the leader it knows.
        let group = transferring_group(Err(OwnershipError::NotLeader {
            leader: Some("east.internal:4460".to_owned()),
        }));

        let error = transfer_authority_home(Some(&group), "proj", "west").await.unwrap_err();
        assert!(matches!(error, OwnershipError::NotLeader { leader: Some(_) }));
    }

    #[tokio::test]
    async fn test_transfer_without_a_group_moves_nothing() {
        let moved = transfer_authority_home(None, "proj", "west").await.expect("no group moves nothing");
        assert_eq!(moved, None);
    }

    #[test]
    fn test_cluster_status_snapshots_the_group() {
        let status = Fake {
            homed: false,
            claim: Ok(HomeClaim::AlreadyHomed),
            epoch: 0,
            transfer: Ok(None),
        }
        .cluster_status();

        assert_eq!(status.leader, Some("east".to_owned()));
        assert_eq!(status.term, 3);
        assert_eq!(status.voters, vec!["east".to_owned()]);
    }

    #[test]
    fn test_not_leader_names_the_known_leader() {
        let error = OwnershipError::NotLeader {
            leader: Some("east.internal:4460".to_owned()),
        };

        assert_eq!(
            error.to_string(),
            "not the ownership leader; leader at east.internal:4460"
        );
    }

    #[test]
    fn test_not_leader_omits_an_unknown_leader() {
        let error = OwnershipError::NotLeader { leader: None };

        assert_eq!(error.to_string(), "not the ownership leader");
    }

    #[test]
    fn test_unavailable_carries_its_reason() {
        let error = OwnershipError::Unavailable("log store gone".to_owned());

        assert_eq!(error.to_string(), "ownership claim did not commit: log store gone");
    }
}

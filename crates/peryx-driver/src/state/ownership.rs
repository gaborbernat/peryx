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

/// A snapshot of the ownership consensus group this node observes, for the availability status resource.
///
/// It names voters by their consensus id and datacenter, never their peer address, so the status surface
/// exposes membership without leaking the internal transport topology.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ClusterStatus {
    /// The consensus id of the current leader, or `None` when this node knows of none.
    pub leader: Option<u64>,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{ClusterStatus, HomeClaim, OwnershipAuthority, OwnershipError, claim_first_publish_home};

    struct Fake {
        homed: bool,
        claim: Result<HomeClaim, OwnershipError>,
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
                leader: Some(1),
                term: 3,
                voters: vec!["east".to_owned()],
            }
        }
    }

    fn group(homed: bool, claim: Result<HomeClaim, OwnershipError>) -> Arc<dyn OwnershipAuthority> {
        Arc::new(Fake { homed, claim })
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

    #[test]
    fn test_cluster_status_snapshots_the_group() {
        let status = Fake {
            homed: false,
            claim: Ok(HomeClaim::AlreadyHomed),
        }
        .cluster_status();

        assert_eq!(status.leader, Some(1));
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

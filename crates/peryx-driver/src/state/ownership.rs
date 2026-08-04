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
    /// Claim `authority`'s home for the local datacenter on its first publish, reporting whether this
    /// call won it. Idempotent: a repeat publish, or a race another datacenter already won, reports
    /// [`HomeClaim::AlreadyHomed`].
    ///
    /// # Errors
    /// Returns [`OwnershipError`] when the claim cannot commit, for example when this node is not the
    /// leader and cannot reach it.
    async fn claim_home(&self, authority: &str) -> Result<HomeClaim, OwnershipError>;
}

#[cfg(test)]
mod tests {
    use super::OwnershipError;

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

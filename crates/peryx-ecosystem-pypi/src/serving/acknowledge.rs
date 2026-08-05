//! Deciding a hosted `PyPI` write's datacenter acknowledgement and mapping it to the client response.
//!
//! An admitted upload is durable on the ingress node the moment its bytes commit, which proves the
//! artifact's same-datacenter byte durability for a filesystem backend. This folds that local placement
//! receipt into the configured quorum over the datacenter's members and combines it with the metadata
//! decision through [`FilesystemAck::decide`], then maps the outcome to an HTTP response: a proven write
//! succeeds, and one still short of quorum when the deadline expires is reported retry-safe rather than
//! failed, because a durable completion may land after the client stops waiting.
//!
//! Gathering receipts from other datacenter members is the replication layer's, and is not wired yet, so
//! the only receipt available in-request is the local node's. A single-node or single-member-per-DC
//! deployment reaches quorum from it; a larger same-DC quorum stays unproven until that transport lands,
//! and the write is reported retry-safe-unknown rather than durable. The decision itself is pure: its
//! member set, receipts, metadata decision, and deadline are inputs, so a test drives every phase without
//! a clock or a network.

use std::collections::BTreeSet;

use axum::http::StatusCode;
use peryx_core::TopologyConfig;
use peryx_replication::{AckDecision, DcAck, Deadline, DurabilityPolicy, FilesystemAck, ReceiptAck};
use peryx_storage::blob::Digest;

/// The node id attributed to the local placement receipt when no roster names this node.
const STANDALONE_NODE: &str = "local";

/// The same-datacenter member identities a write must reach quorum across: every roster member sharing
/// the local node's datacenter. A rosterless single node yields its own id, so a `Local` or single-member
/// quorum is provable from the local receipt alone.
pub(super) fn same_dc_members(topology: &TopologyConfig) -> BTreeSet<String> {
    let dc = super::admission::ingress_dc(topology);
    let members: BTreeSet<String> = topology
        .members
        .iter()
        .filter(|member| member.dc == dc)
        .map(|member| member.node.clone())
        .collect();
    if members.is_empty() {
        return BTreeSet::from([local_node_id(topology)]);
    }
    members
}

/// The node identity the local placement receipt is attributed to: the configured local node, or the
/// standalone id when no roster names it. Always a member of [`same_dc_members`].
pub(super) fn local_node_id(topology: &TopologyConfig) -> String {
    topology
        .local_node
        .clone()
        .unwrap_or_else(|| STANDALONE_NODE.to_owned())
}

/// The evidence the handler feeds the decision, injected whole in a unit test so no clock or transport is
/// consulted.
pub(super) struct DcAckInputs {
    pub policy: DurabilityPolicy,
    pub members: BTreeSet<String>,
    pub local_node: String,
    pub digest: Digest,
    pub metadata: AckDecision,
    pub deadline: Deadline,
}

/// Fold the single local filesystem receipt into a [`FilesystemAck`] over the same-datacenter members and
/// combine it with the metadata decision under the deadline.
pub(super) fn decide_dc_ack(inputs: &DcAckInputs) -> DcAck {
    let mut ack = FilesystemAck::new(inputs.digest.clone(), inputs.members.clone(), inputs.policy);
    ack.record(ReceiptAck {
        node: inputs.local_node.clone(),
        digest: inputs.digest.clone(),
    });
    ack.decide(inputs.metadata, inputs.deadline)
}

/// The client-facing outcome of a write's durability decision.
pub(super) struct AckResponse {
    /// `200` once the write is proven datacenter-durable, `202` while it stays retry-safe pending.
    pub status: StatusCode,
    /// The body replayed verbatim to a retry that finds the operation already finalized.
    pub body: Vec<u8>,
    /// Whether the operation ledger records a terminal result. Only a proven-durable write finalizes; a
    /// pending or unknown one is left open so a retry re-drives it rather than replaying a false success.
    pub finalize: bool,
}

/// Map a [`DcAck`] phase to the HTTP contract. A proven write is `200` and finalizes; a write still short
/// of quorum when the deadline expires is `202` carrying its retry-safe operation id, and one still
/// within its deadline is `202` pending. Neither pending outcome finalizes.
pub(super) fn ack_response(ack: DcAck, operation: &str) -> AckResponse {
    match ack {
        DcAck::Durable { .. } => AckResponse {
            status: StatusCode::OK,
            body: b"upload accepted".to_vec(),
            finalize: true,
        },
        DcAck::Unknown => AckResponse {
            status: StatusCode::ACCEPTED,
            body: format!("upload accepted; durability pending, retry-safe operation {operation}").into_bytes(),
            finalize: false,
        },
        DcAck::Pending => AckResponse {
            status: StatusCode::ACCEPTED,
            body: b"upload accepted; durability pending".to_vec(),
            finalize: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use peryx_core::{NodeRole, TopologyConfig, TopologyMember, TopologyMode};
    use peryx_storage::blob::BlobDurability;

    use super::*;

    fn digest() -> Digest {
        Digest::of(b"artifact")
    }

    fn member(node: &str, dc: &str, role: NodeRole) -> TopologyMember {
        TopologyMember {
            node: node.to_owned(),
            dc: dc.to_owned(),
            address: format!("{node}.{dc}:8080"),
            role,
        }
    }

    fn topology(local: &str, members: Vec<TopologyMember>) -> TopologyConfig {
        TopologyConfig {
            mode: TopologyMode::Dc,
            group: Some("group".to_owned()),
            local_node: Some(local.to_owned()),
            members,
        }
    }

    fn inputs(policy: DurabilityPolicy, members: &[&str], metadata: AckDecision, deadline: Deadline) -> DcAckInputs {
        DcAckInputs {
            policy,
            members: members.iter().map(|node| (*node).to_owned()).collect(),
            local_node: "a".to_owned(),
            digest: digest(),
            metadata,
            deadline,
        }
    }

    #[test]
    fn test_local_receipt_proves_a_local_quorum_durable() {
        let ack = decide_dc_ack(&inputs(
            DurabilityPolicy::Local,
            &["a"],
            AckDecision::Acknowledged,
            Deadline::Live,
        ));
        assert_eq!(
            ack,
            DcAck::Durable {
                scope: BlobDurability::Filesystem
            }
        );
    }

    #[test]
    fn test_single_same_dc_member_reaches_majority_from_the_local_receipt() {
        let ack = decide_dc_ack(&inputs(
            DurabilityPolicy::Majority,
            &["a"],
            AckDecision::Acknowledged,
            Deadline::Live,
        ));
        assert!(matches!(ack, DcAck::Durable { .. }));
    }

    #[test]
    fn test_multi_member_majority_is_pending_while_the_deadline_is_live() {
        let ack = decide_dc_ack(&inputs(
            DurabilityPolicy::Majority,
            &["a", "b", "c"],
            AckDecision::Acknowledged,
            Deadline::Live,
        ));
        assert_eq!(ack, DcAck::Pending);
    }

    #[test]
    fn test_multi_member_majority_is_unknown_once_the_deadline_expires() {
        let ack = decide_dc_ack(&inputs(
            DurabilityPolicy::Majority,
            &["a", "b", "c"],
            AckDecision::Acknowledged,
            Deadline::Expired,
        ));
        assert_eq!(
            ack,
            DcAck::Unknown,
            "a durable completion may land after the client stops waiting"
        );
    }

    #[test]
    fn test_unacknowledged_metadata_holds_the_write_pending_despite_byte_quorum() {
        let ack = decide_dc_ack(&inputs(
            DurabilityPolicy::Local,
            &["a"],
            AckDecision::NotYetDurable {
                target: 5,
                durable_frontier: 0,
            },
            Deadline::Live,
        ));
        assert_eq!(ack, DcAck::Pending);
    }

    #[test]
    fn test_durable_response_is_a_finalizing_ok() {
        let response = ack_response(
            DcAck::Durable {
                scope: BlobDurability::Filesystem,
            },
            "op-1",
        );
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.body, b"upload accepted");
        assert!(response.finalize);
    }

    #[test]
    fn test_unknown_response_is_a_retry_safe_accepted_carrying_the_operation() {
        let response = ack_response(DcAck::Unknown, "op-1");
        assert_eq!(response.status, StatusCode::ACCEPTED);
        assert!(String::from_utf8_lossy(&response.body).contains("op-1"));
        assert!(!response.finalize, "an unproven write must not replay a false success");
    }

    #[test]
    fn test_pending_response_is_accepted_and_does_not_finalize() {
        let response = ack_response(DcAck::Pending, "op-1");
        assert_eq!(response.status, StatusCode::ACCEPTED);
        assert!(!response.finalize);
    }

    #[test]
    fn test_same_dc_members_filters_by_the_local_datacenter() {
        let topology = topology(
            "a",
            vec![
                member("a", "east", NodeRole::Writer),
                member("b", "east", NodeRole::Replica),
                member("c", "west", NodeRole::Replica),
            ],
        );
        assert_eq!(
            same_dc_members(&topology),
            BTreeSet::from(["a".to_owned(), "b".to_owned()]),
            "a west member never counts toward the east node's same-DC quorum",
        );
        assert_eq!(local_node_id(&topology), "a");
    }

    #[test]
    fn test_a_rosterless_node_is_its_own_sole_member() {
        let topology = TopologyConfig::default();
        assert_eq!(same_dc_members(&topology), BTreeSet::from([STANDALONE_NODE.to_owned()]));
        assert_eq!(local_node_id(&topology), STANDALONE_NODE);
    }
}

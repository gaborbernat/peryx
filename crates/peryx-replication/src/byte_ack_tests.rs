use peryx_storage::blob::Digest;

use crate::byte_ack::{ByteAckDecision, decide_byte_ack};
use crate::readiness::DurabilityPolicy;
use crate::receipt_quorum::ReceiptAck;

fn ack(node: &str, digest: &Digest) -> ReceiptAck {
    ReceiptAck {
        node: node.to_owned(),
        digest: digest.clone(),
    }
}

fn nodes(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn test_acknowledges_once_the_quorum_is_met() {
    let digest = Digest::of(b"artifact");

    let decision = decide_byte_ack(
        &digest,
        &[ack("a", &digest), ack("b", &digest)],
        3,
        DurabilityPolicy::Majority,
    );

    assert_eq!(
        decision,
        ByteAckDecision::Acknowledged {
            nodes: nodes(&["a", "b"])
        }
    );
    assert!(decision.is_acknowledged());
}

#[test]
fn test_pending_reports_how_many_more_receipts_are_needed() {
    let digest = Digest::of(b"artifact");

    let decision = decide_byte_ack(&digest, &[ack("a", &digest)], 3, DurabilityPolicy::Majority);

    assert_eq!(
        decision,
        ByteAckDecision::Pending {
            nodes: nodes(&["a"]),
            remaining: 1,
        }
    );
    assert!(!decision.is_acknowledged());
}

#[test]
fn test_no_receipts_still_need_the_full_quorum() {
    let digest = Digest::of(b"artifact");

    let decision = decide_byte_ack(&digest, &[], 3, DurabilityPolicy::Majority);

    assert_eq!(
        decision,
        ByteAckDecision::Pending {
            nodes: Vec::new(),
            remaining: 2,
        }
    );
}

#[test]
fn test_everywhere_policy_counts_every_remaining_node() {
    let digest = Digest::of(b"artifact");

    let decision = decide_byte_ack(&digest, &[ack("a", &digest)], 3, DurabilityPolicy::Everywhere);

    assert_eq!(
        decision,
        ByteAckDecision::Pending {
            nodes: nodes(&["a"]),
            remaining: 2,
        }
    );
}

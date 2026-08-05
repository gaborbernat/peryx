//! Deciding a client write's datacenter acknowledgement from the metadata and artifact evidence.
//!
//! In `dc` mode an HTTP write returns success only once the selected backend proves durability for both
//! dimensions: the metadata write and the artifact bytes. [`acknowledge`](crate::acknowledge) decides
//! the metadata dimension from a group's journal frontiers; [`decide_byte_ack`](crate::decide_byte_ack)
//! decides a filesystem backend's byte dimension from its receipt quorum. This is the shared decision
//! that combines a dimension from each side under the write's deadline, so one place turns the evidence
//! into the client outcome. It reads the two decisions and the deadline and consults no transport,
//! clock, or storage.
//!
//! The byte evidence carries its own scope: a filesystem backend proves datacenter durability only from
//! a receipt quorum, while a DC-durable object store proves it from its own atomic-put-and-digest
//! acknowledgement. A deadline that expires before both dimensions prove is not a definite failure,
//! because the durable write may have completed after the client stopped waiting, so the outcome is
//! [`DcAck::Unknown`] rather than a false negative a retry would double-apply.

use peryx_storage::blob::BlobDurability;

use crate::ack::AckDecision;
use crate::byte_ack::ByteAckDecision;

/// The artifact-byte durability evidence for the selected backend, which fixes the write's durability
/// scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ByteEvidence {
    /// A filesystem backend's byte durability, decided from a quorum of independent per-node receipts.
    Filesystem(ByteAckDecision),
    /// A DC-durable object store's byte durability: whether its atomic put returned a matching digest.
    ObjectStore { acknowledged: bool },
}

impl ByteEvidence {
    const fn is_durable(&self) -> bool {
        match self {
            Self::Filesystem(decision) => decision.is_acknowledged(),
            Self::ObjectStore { acknowledged } => *acknowledged,
        }
    }

    const fn scope(&self) -> BlobDurability {
        match self {
            Self::Filesystem(_) => BlobDurability::Filesystem,
            Self::ObjectStore { .. } => BlobDurability::ObjectStore,
        }
    }
}

/// Whether the client's write deadline has expired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deadline {
    /// The client is still within its deadline, so a pending dimension may still prove.
    Live,
    /// The client deadline expired before the write proved durable.
    Expired,
}

/// One client write's datacenter acknowledgement outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DcAck {
    /// Both the metadata write and the artifact bytes are datacenter-durable for the backend `scope`;
    /// the client write succeeds.
    Durable { scope: BlobDurability },
    /// The evidence is incomplete but the deadline is live, so the caller keeps waiting.
    Pending,
    /// The deadline expired before the evidence completed. Durable completion may have occurred after
    /// the client stopped waiting, so this is not a definite failure and must not be retried as a fresh
    /// write.
    Unknown,
}

/// Decide a client write's datacenter acknowledgement from its `metadata` and byte `evidence` under the
/// write's `deadline`.
///
/// The write is [`Durable`](DcAck::Durable) only when both the metadata dimension and the backend's byte
/// dimension are acknowledged, and it carries the scope the byte evidence fixes. While either dimension
/// is unproven and the deadline is live the caller keeps waiting ([`Pending`](DcAck::Pending)); once the
/// deadline expires with a dimension unproven the outcome is [`Unknown`](DcAck::Unknown) rather than a
/// failure, because the durable write may have completed after the client stopped waiting.
#[must_use]
pub const fn decide_dc_ack(metadata: AckDecision, evidence: &ByteEvidence, deadline: Deadline) -> DcAck {
    if metadata.is_acknowledged() && evidence.is_durable() {
        DcAck::Durable {
            scope: evidence.scope(),
        }
    } else {
        match deadline {
            Deadline::Live => DcAck::Pending,
            Deadline::Expired => DcAck::Unknown,
        }
    }
}

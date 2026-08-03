//! Primary/replica replication over peryx's ordered storage journal.
//!
//! A primary exposes [`ChangePage`] records and digest-addressed blob streams through [`Primary`].
//! [`Replica`] verifies the serial sequence and every missing blob before committing metadata,
//! copied journal entries, and its resume cursor in one transaction.

mod ack;
mod analytics;
mod authority;
mod backoff;
mod blob;
mod channel;
mod consensus;
mod driver;
mod election;
mod envelope;
mod error;
mod follower;
mod http;
mod ingress_intent;
mod liveness;
mod peer;
mod peer_http;
mod protocol;
mod readiness;
mod reconcile;
mod replica;
pub mod sim;
mod status;
mod versions;
mod visibility;

pub use ack::{AckDecision, acknowledge};
pub use analytics::{
    APPLY_STATE_SCHEMA, AggregateDelta, AggregateKey, AggregateRow, AnalyticsBatch, ApplyError, ApplyLimits,
    ApplyOutcome, ApplyState, DEFAULT_APPLY_LIMITS, Frontier, IntervalId, ProducerId, SnapshotError,
};
pub use authority::{Admission, AuthorityFence, AuthorityKey, CommitOutcome};
pub use backoff::{DEFAULT_RECONNECT_POLICY, RETRY_EXHAUSTED, ReconnectPolicy, Retry};
pub use blob::{BlobRequest, BlobTransport, ByteRange, CapacityLimited, LoopbackBlobSource};
pub use channel::{BoundedChannel, BufferOutcome, ChannelFull, buffer_batch};
pub use consensus::{
    AppendEntries, AppendOutcome, DEFAULT_LOG_LIMITS, LogEntry, LogIndex, LogLimits, MemoryRaftLog, RaftLog,
    RaftLogError, Term,
};
pub use driver::{StepOutcome, advance_once};
pub use election::{ElectionError, NodeId, PersistentState, VoteDecision, VoteReason, VoteRequest};
pub use envelope::{
    AuthorityEpoch, DEFAULT_DECODE_LIMITS, DecodeLimits, EnvelopeError, OperationEnvelope, OperationId, OperationKind,
    SCHEMA_VERSION, SchemaVersion, TraceContext,
};
pub use error::SyncError;
pub use follower::{
    AppendAccepted, AppendReject, AppendRequest, AppendResponse, CommitTracker, receive_append_entries,
};
pub use http::{DEFAULT_MAX_CHANGE_PAGE_SIZE, HttpPrimary, HttpPrimaryError, PrimaryHttpConfigError, primary_router};
pub use ingress_intent::{
    Ecosystem, IngressIntent, IntentKey, IntentLedger, IntentState, StageOutcome, TransitionOutcome,
};
pub use liveness::{
    DEFAULT_DEAD_AFTER, DEFAULT_MAX_HEARTBEAT_BYTES, DEFAULT_SUSPECT_AFTER, HeartbeatReport, LivenessRejection,
    LivenessTracker, PeerHealth, Suspicion, liveness_router,
};
pub use peer::{
    BatchFrame, BatchRequest, DEFAULT_TRANSFER_LIMITS, FrontierSync, LoopbackPeer, LoopbackTransport, PeerFault,
    PeerTransport, TransferLimits, TransportError, drain_to_frontier,
};
pub use peer_http::{HttpPeerError, HttpPeerTransport};
pub use protocol::{
    BlobReference, Change, ChangePage, MetadataMutation, PROTOCOL_VERSION, PlacementAvailability, PlacementDescriptor,
    Primary,
};
pub use readiness::{DurabilityPolicy, GroupReadiness, MemberFrontier, MemberRole, ReadinessBlocker, group_readiness};
pub use reconcile::{Disposition, OldEpochOp, classify};
pub use replica::{Replica, ReplicaState, SyncOutcome};
pub use status::{OperationStatus, WriteRecord};
pub use versions::{
    AvailabilityVersions, Incompatibility, Negotiation, Version, VersionRange, WireKind, accepts_operation_kind,
    feature_activated, negotiate, snapshot_compatible,
};
pub use visibility::{ApplyEffect, ArtifactId, OpOrder, Visibility, VisibilityAction, VisibilityOp, VisibilityState};

#[cfg(test)]
mod ack_tests;
#[cfg(test)]
mod analytics_tests;
#[cfg(test)]
mod authority_tests;
#[cfg(test)]
mod backoff_tests;
#[cfg(test)]
mod blob_tests;
#[cfg(test)]
mod channel_tests;
#[cfg(test)]
mod consensus_tests;
#[cfg(test)]
mod driver_tests;
#[cfg(test)]
mod election_tests;
#[cfg(test)]
mod envelope_tests;
#[cfg(test)]
mod follower_tests;
#[cfg(test)]
mod http_tests;
#[cfg(test)]
mod ingress_intent_tests;
#[cfg(test)]
mod liveness_tests;
#[cfg(test)]
mod peer_http_tests;
#[cfg(test)]
mod peer_tests;
#[cfg(test)]
mod protocol_tests;
#[cfg(test)]
mod readiness_tests;
#[cfg(test)]
mod reconcile_tests;
#[cfg(test)]
mod status_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod versions_tests;
#[cfg(test)]
mod visibility_tests;

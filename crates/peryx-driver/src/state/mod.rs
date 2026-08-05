//! Shared application state and index routing.

mod app;
mod build;
mod caches;
mod control;
mod derived_views;
mod describe;
mod ownership;
mod registry;

pub use app::{AppState, Clock, PrometheusSource, ServingState};
pub use build::{DEFAULT_HOT_CACHE_BYTES, DEFAULT_MAX_STALE_SECS, DEFAULT_TOKEN_TTL_SECS, RuntimeOptions};
pub use control::{
    AuditRecord, CommandMetrics, CommandOutcome, CommandReceipt, ControlCommand, ControlError, ControlPlane,
    MembershipControl, plan_voter_roster,
};
pub use derived_views::{REQUIRED_VIEWS, ReadableFrontier, SEARCH_VIEW, readable_frontier};
pub use describe::{
    HostedDescription, IndexDescription, MemberDescription, SecretDescription, UpstreamDescription,
    UpstreamSourceDescription, describe_index, describe_indexes,
};
pub use ownership::{ClusterStatus, HomeClaim, OwnershipAuthority, OwnershipError, TransferOutcome};
pub use peryx_index::{Index, IndexKind};

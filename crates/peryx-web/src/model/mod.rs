//! Serializable view models shared by the server renderer and the hydrated client.
//!
//! The server builds them from `AppState`; the browser rebuilds them from peryx's own JSON API
//! (`/+status` and the PEP 691 simple endpoints), so both sides render identical pages.

mod analytics;
mod policy_decision;
mod project;
mod search;
mod snapshot;
mod stats;
mod trash;

pub use analytics::{
    AnalyticsFilters, AnalyticsView, UiInterval, UiPackageRow, UiSourceRow, UiTimelineRow, UiUnusedRow, UiUsagePage,
    UiUsageRows, UiVersionRow, format_instant,
};
pub use policy_decision::{PolicyDecisionFilters, UiPolicyDecision, UiPolicyDecisionPage};
pub use project::{
    PlacementLabel, UiArtifactRef, UiArtifactSource, UiByteAvailability, UiFile, UiManifest, UiMember, UiMemberChunk,
    UiProject, UiProjectStatus, UiProjectView, UiRelease, byte_availability_label, file_source_label,
    members_from_listing, projects_from_list,
};
pub use search::{UiSearchPage, UiSearchResult, source_label};
pub use snapshot::{UiEcosystemSummary, UiHosted, UiIndex, UiMetricFamily, UiRecentUpload, UiSnapshot, UiUpstream};
pub use stats::{UiCounters, UiStats, stats_index, stats_project, stats_routes};
pub use trash::{TrashFilters, UiTrashPage, UiTrashRecord};

fn string_at(value: &serde_json::Value, key: &str) -> String {
    value[key].as_str().unwrap_or_default().to_owned()
}

fn u64_at(value: &serde_json::Value, key: &str) -> u64 {
    value[key].as_u64().unwrap_or_default()
}

fn usize_from(value: Option<u64>, default: usize) -> usize {
    value.and_then(|value| usize::try_from(value).ok()).unwrap_or(default)
}

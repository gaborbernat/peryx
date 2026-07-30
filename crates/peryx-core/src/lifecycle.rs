//! Ecosystem-neutral artifact lifecycle records.

use serde::{Deserialize, Serialize};

/// Provenance retained when an artifact is soft-deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrashInfo {
    /// When the artifact was trashed, as a Unix timestamp.
    pub deleted_at_unix: i64,
    /// The token or actor that deleted it, when the request carried an identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    /// The operator's stated reason, when the delete request supplied one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

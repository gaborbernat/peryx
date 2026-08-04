//! The neutral artifact-placement-health view an operator surface renders.
//!
//! An administrator needs to see how much of the store serves locally, how much depends on an upstream,
//! and how much cannot be served at all, without paging every artifact by hand or reading tenant data
//! from a topology view. [`PlacementView`] carries the aggregate every viewer reads and, for an
//! administrator, a bounded page of per-digest rows. The models are pure serde with no I/O, so the same
//! type crosses the server/browser boundary the topology view already established.
//!
//! The aggregate is whole-store; the rows are one capped page in digest order with a cursor to resume,
//! so a large store never turns a render into an unbounded payload.

use serde::{Deserialize, Serialize};

use crate::view::{UiArtifactSource, UiByteAvailability};

/// How the store's byte availability splits across its artifacts, plus the total it sums to. Three
/// counts and a total regardless of store size, so the summary never scales with the object count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PlacementHealth {
    /// Artifacts this instance serves from local storage.
    pub local: u64,
    /// Artifacts with no local bytes that a known upstream can still supply.
    pub remote_only: u64,
    /// Artifacts with no local bytes and no upstream to supply them.
    pub unavailable: u64,
    /// The sum of the three states, so a reader sees the store size the split covers.
    pub total: u64,
}

/// One artifact's placement: its content digest and the two orthogonal dimensions a health view reads.
///
/// Carries no file path, tenant identity, or repository coordinate, so an administrator inspects
/// convergence by digest without the row leaking where the artifact lives or who owns it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementRow {
    pub digest: String,
    pub source: UiArtifactSource,
    pub availability: UiByteAvailability,
}

/// The placement-health view filtered to the caller's class.
///
/// Every admitted caller reads the aggregate and the observation time; only an administrator reads the
/// per-digest `rows` and the `next_cursor` to page them. A withheld page is absent rather than empty, so
/// an operator cannot mistake a filtered view for a converged store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementView {
    /// Unix seconds when the view was taken, so a stale render shows as age rather than health.
    pub captured_at: i64,
    pub health: PlacementHealth,
    /// A bounded page of per-digest rows in digest order, present only for an administrator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<PlacementRow>>,
    /// The digest to resume the page after, present only with `rows` and only when more remain.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{PlacementHealth, PlacementRow, PlacementView};
    use crate::view::{UiArtifactSource, UiByteAvailability};

    #[test]
    fn test_view_round_trips_through_json() {
        let view = PlacementView {
            captured_at: 1_800_000_000,
            health: PlacementHealth {
                local: 3,
                remote_only: 1,
                unavailable: 2,
                total: 6,
            },
            rows: Some(vec![PlacementRow {
                digest: "sha256:aa".to_owned(),
                source: UiArtifactSource::Proxy,
                availability: UiByteAvailability::RemoteOnly,
            }]),
            next_cursor: Some("sha256:aa".to_owned()),
        };
        let encoded = serde_json::to_string(&view).unwrap();
        assert_eq!(serde_json::from_str::<PlacementView>(&encoded).unwrap(), view);
    }

    #[test]
    fn test_operator_view_omits_rows_and_cursor() {
        let view = PlacementView {
            captured_at: 7,
            health: PlacementHealth::default(),
            rows: None,
            next_cursor: None,
        };
        let value: serde_json::Value = serde_json::from_str(&serde_json::to_string(&view).unwrap()).unwrap();
        assert!(value.get("rows").is_none(), "{value}");
        assert!(value.get("next_cursor").is_none(), "{value}");
        assert_eq!(value["health"]["total"], 0);
    }
}

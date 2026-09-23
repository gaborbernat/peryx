use std::collections::{BTreeSet, HashMap};

use peryx_ha::ArtifactPlacement;

use super::{ARTIFACT_PLACEMENT, DRIVER_KV, MetaError, MetaStore, open_optional_table};

#[derive(Debug, Default)]
pub struct DriverPlacementSnapshot {
    pub driver_values: Vec<Option<Vec<u8>>>,
    pub placements: HashMap<String, ArtifactPlacement>,
}

#[derive(Debug, thiserror::Error)]
pub enum DriverPlacementSnapshotError {
    #[error("metadata snapshot could not be read: {0}")]
    Snapshot(#[source] MetaError),
    #[error("driver record {key:?} could not be read: {source}")]
    Driver {
        key: String,
        #[source]
        source: MetaError,
    },
    #[error("artifact placement {digest:?} could not be read: {source}")]
    Placement {
        digest: String,
        #[source]
        source: MetaError,
    },
    #[error("artifact placement {digest:?} is malformed")]
    MalformedPlacement {
        digest: String,
        #[source]
        source: serde_json::Error,
    },
}

impl MetaStore {
    /// Reads exact opaque driver records and deduplicated artifact placements from one snapshot.
    ///
    /// The returned driver values retain `driver_keys` order. A missing driver row has a `None`
    /// entry, while missing placements are absent from the placement map.
    ///
    /// # Errors
    /// Returns a contextual error when the snapshot, a requested row, or a placement value cannot be
    /// read. Placement decode errors do not expose stored bytes.
    pub fn read_driver_placement_snapshot(
        &self,
        driver_keys: &[String],
        placement_digests: &BTreeSet<String>,
    ) -> Result<DriverPlacementSnapshot, DriverPlacementSnapshotError> {
        if driver_keys.is_empty() && placement_digests.is_empty() {
            return Ok(DriverPlacementSnapshot::default());
        }
        let transaction = self
            .db
            .begin_read()
            .map_err(MetaError::from)
            .map_err(DriverPlacementSnapshotError::Snapshot)?;
        let driver = transaction
            .open_table(DRIVER_KV)
            .map_err(MetaError::from)
            .map_err(DriverPlacementSnapshotError::Snapshot)?;
        let mut driver_values = Vec::with_capacity(driver_keys.len());
        for key in driver_keys {
            let value = driver
                .get(key.as_str())
                .map_err(MetaError::from)
                .map_err(|source| DriverPlacementSnapshotError::Driver {
                    key: key.clone(),
                    source,
                })?
                .map(|value| value.value().to_vec());
            driver_values.push(value);
        }
        let Some(table) =
            open_optional_table(&transaction, ARTIFACT_PLACEMENT).map_err(DriverPlacementSnapshotError::Snapshot)?
        else {
            return Ok(DriverPlacementSnapshot {
                driver_values,
                placements: HashMap::new(),
            });
        };
        let mut placements = HashMap::with_capacity(placement_digests.len());
        for digest in placement_digests {
            let Some(value) = table.get(digest.as_str()).map_err(MetaError::from).map_err(|source| {
                DriverPlacementSnapshotError::Placement {
                    digest: digest.clone(),
                    source,
                }
            })?
            else {
                continue;
            };
            let placement = serde_json::from_slice(value.value()).map_err(|source| {
                DriverPlacementSnapshotError::MalformedPlacement {
                    digest: digest.clone(),
                    source,
                }
            })?;
            placements.insert(digest.clone(), placement);
        }
        Ok(DriverPlacementSnapshot {
            driver_values,
            placements,
        })
    }
}

#[cfg(test)]
#[path = "../../tests/unit/meta/snapshot_tests.rs"]
mod snapshot_tests;

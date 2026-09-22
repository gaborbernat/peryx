use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::Bound::{Excluded, Unbounded};

use peryx_ha::ArtifactPlacement;
use redb::ReadableTable as _;
use serde::{Deserialize, Serialize};

use super::placement::advance_artifact_placement_revision;
use super::{
    ARTIFACT_PLACEMENT, ARTIFACT_PLACEMENT_REVISION, ARTIFACT_REPAIR_CURSOR, BLOB_RECLAIM_GUARD, MetaError, MetaStore,
    open_optional_table,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactRepairDirection {
    Content,
    Placement,
}

impl ArtifactRepairDirection {
    const fn key(self) -> &'static str {
        match self {
            Self::Content => "content",
            Self::Placement => "placement",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ArtifactRepairCursor {
    revision: u64,
    cursor: Option<String>,
    #[serde(default)]
    backend: String,
}

impl ArtifactRepairCursor {
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactRepairObservation {
    pub(crate) placement: Option<ArtifactPlacement>,
    revision: u64,
    guarded: bool,
}

#[derive(Debug, Clone)]
pub struct ArtifactRepairChange {
    digest: String,
    observed: ArtifactRepairObservation,
    replacement: ArtifactPlacement,
    requires_existing: bool,
}

impl ArtifactRepairChange {
    pub const fn promote(digest: String, observed: ArtifactRepairObservation, replacement: ArtifactPlacement) -> Self {
        Self {
            digest,
            observed,
            replacement,
            requires_existing: false,
        }
    }

    pub const fn demote(digest: String, observed: ArtifactRepairObservation, replacement: ArtifactPlacement) -> Self {
        Self {
            digest,
            observed,
            replacement,
            requires_existing: true,
        }
    }
}

pub type ArtifactRepairPlacementPage = (Vec<(String, ArtifactRepairObservation)>, Option<String>);

impl MetaStore {
    /// # Errors
    ///
    /// Returns an error when the cursor record cannot be read, encoded, or written.
    pub fn artifact_repair_cursor_for_backend(
        &self,
        direction: ArtifactRepairDirection,
        backend: &str,
    ) -> Result<ArtifactRepairCursor, MetaError> {
        let txn = self.db.begin_write()?;
        let cursor: ArtifactRepairCursor = {
            let table = txn.open_table(ARTIFACT_REPAIR_CURSOR)?;
            table
                .get(direction.key())?
                .map(|value| serde_json::from_slice(value.value()))
                .transpose()?
                .unwrap_or_default()
        };
        if cursor.backend == backend {
            txn.commit()?;
            return Ok(cursor);
        }
        let advanced = Self::advance_artifact_repair_cursor(&txn, direction, &cursor, None, backend)?;
        debug_assert!(advanced);
        txn.commit()?;
        Ok(ArtifactRepairCursor {
            revision: cursor
                .revision
                .checked_add(1)
                .ok_or(MetaError::ArtifactRepairCursorRevisionOverflow)?,
            cursor: None,
            backend: backend.to_owned(),
        })
    }

    /// # Errors
    ///
    /// Returns an error when the cursor record cannot be read, encoded, or written.
    pub fn advance_artifact_repair_cursor(
        txn: &redb::WriteTransaction,
        direction: ArtifactRepairDirection,
        expected: &ArtifactRepairCursor,
        cursor: Option<String>,
        backend: &str,
    ) -> Result<bool, MetaError> {
        let mut table = txn.open_table(ARTIFACT_REPAIR_CURSOR)?;
        let current: ArtifactRepairCursor = table
            .get(direction.key())?
            .map(|value| serde_json::from_slice(value.value()))
            .transpose()?
            .unwrap_or_default();
        if current != *expected {
            return Ok(false);
        }
        let next = ArtifactRepairCursor {
            revision: current
                .revision
                .checked_add(1)
                .ok_or(MetaError::ArtifactRepairCursorRevisionOverflow)?,
            cursor,
            backend: backend.to_owned(),
        };
        table.insert(direction.key(), serde_json::to_vec(&next)?.as_slice())?;
        Ok(true)
    }

    /// # Errors
    ///
    /// Returns an error when a placement, revision, or guard record cannot be read or decoded.
    pub fn artifact_repair_observations(
        &self,
        digests: &[String],
    ) -> Result<HashMap<String, ArtifactRepairObservation>, MetaError> {
        let txn = self.db.begin_read()?;
        let placements = open_optional_table(&txn, ARTIFACT_PLACEMENT)?;
        let revisions = open_optional_table(&txn, ARTIFACT_PLACEMENT_REVISION)?;
        let guards = open_optional_table(&txn, BLOB_RECLAIM_GUARD)?;
        digests
            .iter()
            .map(|digest| {
                let placement = match &placements {
                    Some(table) => table
                        .get(digest.as_str())?
                        .map(|value| serde_json::from_slice(value.value()))
                        .transpose()?,
                    None => None,
                };
                let revision = revisions
                    .as_ref()
                    .map(|table| {
                        table
                            .get(digest.as_str())
                            .map(|value| value.map_or(0, |value| value.value()))
                    })
                    .transpose()?
                    .unwrap_or(0);
                let guarded = guards
                    .as_ref()
                    .map(|table| table.get(digest.as_str()).map(|value| value.is_some()))
                    .transpose()?
                    .unwrap_or(false);
                Ok((
                    digest.clone(),
                    ArtifactRepairObservation {
                        placement,
                        revision,
                        guarded,
                    },
                ))
            })
            .collect()
    }

    /// # Errors
    ///
    /// Returns an error when the placement page, revision, or guard records cannot be read or decoded.
    pub fn artifact_repair_placement_page(
        &self,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> Result<ArtifactRepairPlacementPage, MetaError> {
        let txn = self.db.begin_read()?;
        let Some(placements) = open_optional_table(&txn, ARTIFACT_PLACEMENT)? else {
            return Ok((Vec::new(), None));
        };
        let revisions = open_optional_table(&txn, ARTIFACT_PLACEMENT_REVISION)?;
        let guards = open_optional_table(&txn, BLOB_RECLAIM_GUARD)?;
        let entries = placements.range::<&str>((cursor.map_or(Unbounded, Excluded), Unbounded))?;
        let mut rows: Vec<(String, ArtifactRepairObservation)> = Vec::with_capacity(limit.get());
        let mut next_cursor = None;
        for entry in entries {
            let (digest, value) = entry?;
            if rows.len() == limit.get() {
                next_cursor = rows.last().map(|(digest, _)| digest.clone());
                break;
            }
            let digest = digest.value().to_owned();
            let revision = revisions
                .as_ref()
                .map(|table| {
                    table
                        .get(digest.as_str())
                        .map(|value| value.map_or(0, |value| value.value()))
                })
                .transpose()?
                .unwrap_or(0);
            let guarded = guards
                .as_ref()
                .map(|table| table.get(digest.as_str()).map(|value| value.is_some()))
                .transpose()?
                .unwrap_or(false);
            rows.push((
                digest,
                ArtifactRepairObservation {
                    placement: Some(serde_json::from_slice(value.value())?),
                    revision,
                    guarded,
                },
            ));
        }
        Ok((rows, next_cursor))
    }

    /// # Errors
    ///
    /// Returns an error when a placement update or cursor advance cannot commit.
    pub fn apply_artifact_repair_page(
        &self,
        direction: ArtifactRepairDirection,
        expected_cursor: &ArtifactRepairCursor,
        next_cursor: Option<String>,
        changes: &[ArtifactRepairChange],
    ) -> Result<(usize, usize), MetaError> {
        let txn = self.db.begin_write()?;
        let current_cursor: ArtifactRepairCursor = {
            let table = txn.open_table(ARTIFACT_REPAIR_CURSOR)?;
            table
                .get(direction.key())?
                .map(|value| serde_json::from_slice(value.value()))
                .transpose()?
                .unwrap_or_default()
        };
        if current_cursor != *expected_cursor {
            txn.commit()?;
            return Ok((0, changes.len()));
        }
        let mut applied = 0;
        let mut skipped = 0;
        for change in changes {
            if change.observed.guarded {
                skipped += 1;
                continue;
            }
            let current_revision = {
                let table = txn.open_table(ARTIFACT_PLACEMENT_REVISION)?;
                table.get(change.digest.as_str())?.map_or(0, |value| value.value())
            };
            if current_revision != change.observed.revision {
                skipped += 1;
                continue;
            }
            let current = {
                let table = txn.open_table(ARTIFACT_PLACEMENT)?;
                table
                    .get(change.digest.as_str())?
                    .map(|value| serde_json::from_slice(value.value()))
                    .transpose()?
            };
            if change.requires_existing && current != change.observed.placement {
                skipped += 1;
                continue;
            }
            {
                let mut table = txn.open_table(ARTIFACT_PLACEMENT)?;
                table.insert(
                    change.digest.as_str(),
                    serde_json::to_vec(&change.replacement)?.as_slice(),
                )?;
            }
            advance_artifact_placement_revision(&txn, change.digest.as_str())?;
            applied += 1;
        }
        let advanced = Self::advance_artifact_repair_cursor(
            &txn,
            direction,
            expected_cursor,
            next_cursor,
            &expected_cursor.backend,
        )?;
        debug_assert!(advanced);
        txn.commit()?;
        Ok((applied, skipped))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use peryx_ha::{ArtifactPlacement, ArtifactSource};

    use super::super::fault;
    use super::{ArtifactRepairChange, ArtifactRepairCursor, ArtifactRepairDirection};
    use crate::blob::BlobStorage;
    use crate::meta::{ARTIFACT_PLACEMENT, MetaStore};
    use crate::repair_artifact_placements;

    fn store() -> (tempfile::TempDir, MetaStore) {
        let directory = tempfile::tempdir().unwrap();
        let meta = MetaStore::open(directory.path().join("peryx.redb")).unwrap();
        (directory, meta)
    }

    #[test]
    fn placement_page_resumes_after_its_cursor() {
        let (_directory, meta) = store();
        let digests = ["a".repeat(64), "b".repeat(64)];
        for digest in &digests {
            meta.put_artifact_placement(digest, &ArtifactPlacement::record(ArtifactSource::Hosted, true))
                .unwrap();
        }
        let (_, cursor) = meta.artifact_repair_placement_page(None, NonZeroUsize::MIN).unwrap();

        let (rows, next_cursor) = meta
            .artifact_repair_placement_page(cursor.as_deref(), NonZeroUsize::MIN)
            .unwrap();

        assert_eq!(rows[0].0, digests[1]);
        assert!(next_cursor.is_none());
    }

    #[test]
    fn cursor_advance_rejects_a_stale_revision() {
        let (_directory, meta) = store();
        let stale = ArtifactRepairCursor::default();
        meta.apply_artifact_repair_page(ArtifactRepairDirection::Content, &stale, None, &[])
            .unwrap();
        let txn = meta.db.begin_write().unwrap();

        let advanced = MetaStore::advance_artifact_repair_cursor(
            &txn,
            ArtifactRepairDirection::Content,
            &stale,
            None,
            "filesystem",
        )
        .unwrap();

        assert!(!advanced);
        txn.commit().unwrap();
    }

    #[test]
    fn page_apply_rejects_a_stale_cursor() {
        let (_directory, meta) = store();
        let stale = ArtifactRepairCursor::default();
        meta.apply_artifact_repair_page(ArtifactRepairDirection::Content, &stale, None, &[])
            .unwrap();

        assert_eq!(
            meta.apply_artifact_repair_page(ArtifactRepairDirection::Content, &stale, None, &[])
                .unwrap(),
            (0, 0)
        );
    }

    #[test]
    fn page_apply_rejects_a_stale_placement_revision() {
        let (_directory, meta) = store();
        let digest = "a".repeat(64);
        let observed = meta
            .artifact_repair_observations(std::slice::from_ref(&digest))
            .unwrap()
            .remove(&digest)
            .unwrap();
        meta.put_artifact_placement(&digest, &ArtifactPlacement::record(ArtifactSource::Hosted, true))
            .unwrap();
        let cursor = meta
            .artifact_repair_cursor_for_backend(ArtifactRepairDirection::Content, "")
            .unwrap();
        let change = ArtifactRepairChange::promote(
            digest,
            observed,
            ArtifactPlacement::record(ArtifactSource::Unknown, true),
        );

        assert_eq!(
            meta.apply_artifact_repair_page(ArtifactRepairDirection::Content, &cursor, None, &[change])
                .unwrap(),
            (0, 1)
        );
    }

    #[test]
    fn page_apply_rejects_an_observation_before_a_same_value_local_mark() {
        let (_directory, meta) = store();
        let digest = "a".repeat(64);
        let placement = ArtifactPlacement::record(ArtifactSource::Hosted, true);
        meta.put_artifact_placement(&digest, &placement).unwrap();
        let observed = meta
            .artifact_repair_observations(std::slice::from_ref(&digest))
            .unwrap()
            .remove(&digest)
            .unwrap();
        assert_eq!(
            meta.mark_artifact_local(&digest, ArtifactSource::Hosted).unwrap(),
            placement
        );
        let cursor = meta
            .artifact_repair_cursor_for_backend(ArtifactRepairDirection::Placement, "")
            .unwrap();
        let change = ArtifactRepairChange::demote(
            digest,
            observed,
            ArtifactPlacement::record(ArtifactSource::Hosted, false),
        );

        assert_eq!(
            meta.apply_artifact_repair_page(ArtifactRepairDirection::Placement, &cursor, None, &[change])
                .unwrap(),
            (0, 1)
        );
    }

    #[test]
    fn page_apply_rejects_a_changed_placement_value() {
        let (_directory, meta) = store();
        let digest = "a".repeat(64);
        meta.put_artifact_placement(&digest, &ArtifactPlacement::record(ArtifactSource::Hosted, true))
            .unwrap();
        let observed = meta
            .artifact_repair_observations(std::slice::from_ref(&digest))
            .unwrap()
            .remove(&digest)
            .unwrap();
        let txn = meta.db.begin_write().unwrap();
        {
            let mut placements = txn.open_table(ARTIFACT_PLACEMENT).unwrap();
            placements
                .insert(
                    digest.as_str(),
                    serde_json::to_vec(&ArtifactPlacement::record(ArtifactSource::Proxy, true))
                        .unwrap()
                        .as_slice(),
                )
                .unwrap();
        }
        txn.commit().unwrap();
        let cursor = meta
            .artifact_repair_cursor_for_backend(ArtifactRepairDirection::Placement, "")
            .unwrap();
        let change = ArtifactRepairChange::demote(
            digest,
            observed,
            ArtifactPlacement::record(ArtifactSource::Hosted, false),
        );

        assert_eq!(
            meta.apply_artifact_repair_page(ArtifactRepairDirection::Placement, &cursor, None, &[change])
                .unwrap(),
            (0, 1)
        );
    }

    #[tokio::test]
    async fn repair_replays_a_content_page_after_a_metadata_fault() {
        let mut injections = 0;
        for fail_after in 0.. {
            let directory = tempfile::tempdir().unwrap();
            let blobs = BlobStorage::filesystem(directory.path().join("blobs"));
            let digest = blobs.put_bytes(b"replay").await.unwrap();
            let (meta, inner, fault) = fault::initialized();

            fault.arm(fail_after);
            let result = repair_artifact_placements(&meta, &blobs, 1).await;
            if !fault.triggered() {
                assert!(result.is_ok());
                break;
            }
            assert!(result.is_err());
            injections += 1;
            fault.disable();
            drop(meta);
            let meta = fault::reopen(&inner, &fault);

            repair_artifact_placements(&meta, &blobs, 1).await.unwrap();
            assert_eq!(
                meta.get_artifact_placement(digest.as_str()).unwrap(),
                Some(ArtifactPlacement::record(ArtifactSource::Unknown, true))
            );
        }
        assert!(injections > 0);
    }
}

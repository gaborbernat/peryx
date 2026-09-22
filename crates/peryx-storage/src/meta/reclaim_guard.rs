use peryx_ha::{ReclaimGuard, ReclaimGuardArm, ReclaimGuardStore};
use redb::ReadableTable as _;

use super::index::write_reference_revision;
use super::placement::advance_artifact_placement_revision;
use super::{ARTIFACT_PLACEMENT, BLOB_RECLAIM_GUARD, MetaError, MetaStore, open_optional_table};

impl MetaStore {
    /// Arms guards and removes their placement rows in one metadata transaction.
    ///
    /// # Errors
    /// Returns a store error when the transaction cannot be read or committed.
    pub fn compare_and_arm_reclaim_guards_and_remove_placements(
        &self,
        digests: &[&str],
        revision: u64,
        now: i64,
        replacement: ReclaimGuard,
    ) -> Result<ReclaimGuardArm, MetaError> {
        let txn = self.db.begin_write()?;
        let armed = arm_reclaim_guards(&txn, digests, revision, now, replacement)?;
        if let ReclaimGuardArm::Armed(digests) = &armed {
            let mut removed = Vec::new();
            {
                let mut placements = txn.open_table(ARTIFACT_PLACEMENT)?;
                for digest in digests {
                    if placements.remove(digest.as_str())?.is_some() {
                        removed.push(digest.as_str());
                    }
                }
            }
            for digest in removed {
                advance_artifact_placement_revision(&txn, digest)?;
            }
        }
        txn.commit()?;
        Ok(armed)
    }
}

impl ReclaimGuardStore for MetaStore {
    type Error = MetaError;

    fn compare_and_arm_reclaim_guards(
        &self,
        digests: &[&str],
        revision: u64,
        now: i64,
        replacement: ReclaimGuard,
    ) -> Result<ReclaimGuardArm, Self::Error> {
        let txn = self.db.begin_write()?;
        let armed = arm_reclaim_guards(&txn, digests, revision, now, replacement)?;
        txn.commit()?;
        Ok(armed)
    }

    fn compare_and_disarm_reclaim_guard(&self, digest: &str, expected: ReclaimGuard) -> Result<bool, Self::Error> {
        let exists = {
            let txn = self.db.begin_read()?;
            let table = open_optional_table(&txn, BLOB_RECLAIM_GUARD)?;
            table.is_some()
        };
        if !exists {
            return Ok(false);
        }
        let txn = self.db.begin_write()?;
        let removed = {
            let mut table = txn.open_table(BLOB_RECLAIM_GUARD)?;
            let matches = {
                let value = table.get(digest)?;
                value.is_some_and(|value| value.value() == expected.expires_at_unix)
            };
            if matches {
                table.remove(digest)?;
                true
            } else {
                false
            }
        };
        if removed {
            advance_artifact_placement_revision(&txn, digest)?;
        }
        txn.commit()?;
        Ok(removed)
    }

    fn reclaim_guard(&self, digest: &str) -> Result<Option<ReclaimGuard>, Self::Error> {
        let txn = self.db.begin_read()?;
        let Some(table) = open_optional_table(&txn, BLOB_RECLAIM_GUARD)? else {
            return Ok(None);
        };
        let value = table.get(digest)?.map(|value| ReclaimGuard {
            expires_at_unix: value.value(),
        });
        Ok(value)
    }

    fn reclaim_guards(&self) -> Result<Vec<(String, ReclaimGuard)>, Self::Error> {
        let txn = self.db.begin_read()?;
        let Some(table) = open_optional_table(&txn, BLOB_RECLAIM_GUARD)? else {
            return Ok(Vec::new());
        };
        table
            .iter()?
            .map(|entry| {
                let (digest, expires_at) = entry?;
                Ok((
                    digest.value().to_owned(),
                    ReclaimGuard {
                        expires_at_unix: expires_at.value(),
                    },
                ))
            })
            .collect()
    }
}

fn arm_reclaim_guards(
    txn: &redb::WriteTransaction,
    digests: &[&str],
    revision: u64,
    now: i64,
    replacement: ReclaimGuard,
) -> Result<ReclaimGuardArm, MetaError> {
    if write_reference_revision(txn)? != revision {
        return Ok(ReclaimGuardArm::ReferencesMoved);
    }
    let mut armed = Vec::new();
    if !digests.is_empty() {
        let mut table = txn.open_table(BLOB_RECLAIM_GUARD)?;
        for &digest in digests {
            let available = {
                let value = table.get(digest)?;
                value.is_none_or(|value| {
                    ReclaimGuard {
                        expires_at_unix: value.value(),
                    }
                    .is_expired_at(now)
                })
            };
            if available {
                table.insert(digest, replacement.expires_at_unix)?;
                armed.push(digest.to_owned());
            }
        }
    }
    for digest in &armed {
        advance_artifact_placement_revision(txn, digest)?;
    }
    Ok(ReclaimGuardArm::Armed(armed))
}

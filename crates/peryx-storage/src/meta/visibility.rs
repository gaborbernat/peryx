//! The durable snapshot of a replica's replicated artifact visibility.
//!
//! The replication layer folds trash, restore, revoke, and lift operations into an in-memory
//! visibility apply state and encodes it to opaque bytes. This module holds those bytes in one
//! metadata singleton, so a follower recovers every tombstone across a restart or a metadata log
//! compaction without the storage layer needing to understand the encoding.

use super::{MetaError, MetaStore, VISIBILITY_SNAPSHOT, VISIBILITY_SNAPSHOT_KEY};

impl MetaStore {
    /// Read the persisted visibility snapshot, or `None` before the first save.
    ///
    /// # Errors
    /// Returns a store error if the read fails.
    pub fn visibility_snapshot(&self) -> Result<Option<Vec<u8>>, MetaError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(VISIBILITY_SNAPSHOT)?;
        Ok(table.get(VISIBILITY_SNAPSHOT_KEY)?.map(|value| value.value().to_vec()))
    }

    /// Overwrite the persisted visibility snapshot with `snapshot`, replacing any prior one so the
    /// durable copy always reflects the latest compacted apply state.
    ///
    /// # Errors
    /// Returns a store error if the write fails.
    pub fn save_visibility_snapshot(&self, snapshot: &[u8]) -> Result<(), MetaError> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(VISIBILITY_SNAPSHOT)?;
            table.insert(VISIBILITY_SNAPSHOT_KEY, snapshot)?;
        }
        txn.commit()?;
        Ok(())
    }
}

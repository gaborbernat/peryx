//! The durable audit trail of committed authority transfers.
//!
//! The ownership consensus group mints a transfer's fencing epoch and commits the move, but the record of
//! who moved an authority, from where, to where, and why does not survive a restart on its own. This
//! persists the sealed audit so an operator and reconciliation can read a home's full move history after
//! the fact.
//!
//! An authority may move more than once, so the trail is keyed by the authority and the committed log
//! index the move rode, zero-padded so a range scan over one authority's prefix returns its transfers in
//! commit order. A re-commit of the same move writes the same key with the same record, so a retry after a
//! leader loss books one audit line rather than two.

use serde::{Deserialize, Serialize};

use super::{MetaError, MetaStore, TRANSFER_AUDIT};

/// One committed authority transfer: the move an operator ordered and the committed identity that carried
/// it. Every field is a primitive so the store keeps no dependency on the ownership plane's types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferAudit {
    /// The authority that moved, a project or repository home.
    pub authority: String,
    /// The datacenter the authority was homed at before the move.
    pub source: String,
    /// The datacenter the authority moved to.
    pub target: String,
    /// The operator who ordered the transfer.
    pub actor: String,
    /// The operator's stated reason.
    pub reason: String,
    /// The target frontier the move waited for before it committed.
    pub barrier: u64,
    /// The epoch the move minted, which fences the old home's stale-epoch writes.
    pub epoch: u64,
    /// The index of the Raft log entry that committed the move.
    pub commit_index: u64,
}

impl TransferAudit {
    /// The table key: the authority and the zero-padded commit index, so a prefix range over one
    /// authority reads its moves in commit order and a re-commit rewrites the same key.
    fn key(&self) -> String {
        format!("{}\u{0}{:020}", self.authority, self.commit_index)
    }
}

/// The exclusive upper bound of an authority's key range: its prefix with the separator bumped past
/// `\u{0}`, so a range from the prefix up to this covers every commit index under that authority alone.
fn range_end(authority: &str) -> String {
    format!("{authority}\u{1}")
}

impl MetaStore {
    /// Persist a sealed transfer audit, keyed by its authority and commit index.
    ///
    /// # Errors
    /// Returns an error when the store write fails.
    pub fn record_transfer_audit(&self, audit: &TransferAudit) -> Result<(), MetaError> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(TRANSFER_AUDIT)?;
            table.insert(audit.key().as_str(), serde_json::to_vec(audit)?.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// The committed transfers of one authority, in commit-index order, or an empty list when it has never
    /// moved.
    ///
    /// # Errors
    /// Returns an error when the store read fails or a record cannot be decoded.
    pub fn transfer_audits(&self, authority: &str) -> Result<Vec<TransferAudit>, MetaError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(TRANSFER_AUDIT)?;
        let prefix = format!("{authority}\u{0}");
        let mut audits = Vec::new();
        for entry in table.range(prefix.as_str()..range_end(authority).as_str())? {
            let (_, value) = entry?;
            audits.push(serde_json::from_slice(value.value())?);
        }
        Ok(audits)
    }
}

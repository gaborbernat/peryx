//! The ingress DC's durable staging ledger for write intents.
//!
//! The replication layer holds the pure admission state; this persists it so a restart recovers the
//! intents a home DC has yet to finalize. Each intent is keyed by its opaque client-scoped identity and
//! carries the content it binds to — a digest and byte size — so a retried admission of the same key is
//! idempotent: the same content is a duplicate, and different content a conflict that never overwrites
//! the bytes a home DC will finalize. A new key past the retention limit is refused rather than growing
//! the buffer without end, and a lifecycle transition only advances an intent, never moves it back.
//!
//! The staged payload the caller serializes is opaque here, like the neutral driver key-value table: the
//! store reads only the key, the content binding, and the lifecycle phase.

use redb::{ReadableTable as _, ReadableTableMetadata as _};
use serde::{Deserialize, Serialize};

use super::{INGRESS_INTENT, MetaError, MetaStore};

/// Where a staged intent sits in its lifecycle. Declared in advancing order, so the derived ordering
/// ranks `Pending` below `Admitted` below `Expired` and a transition only moves forward.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentPhase {
    /// Admitted at the ingress DC and retained, awaiting home-DC finalization.
    Pending,
    /// The home DC finalized the write; the intent is settled.
    Admitted,
    /// The retention window elapsed before finalization; the intent is eligible for reclamation.
    Expired,
}

/// One durably staged write intent: its lifecycle phase, the content it binds to, and the opaque payload
/// the caller serialized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedIntent {
    pub phase: IntentPhase,
    /// The content digest the intent commits to, used to tell a duplicate retry from a conflict.
    pub digest: String,
    pub size: u64,
    /// The intent the caller serialized, replayed verbatim; the store never interprets it.
    pub payload: Vec<u8>,
    pub updated_at_unix: i64,
}

/// The verdict of staging an intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentStageOutcome {
    /// A new distinct intent was admitted and retained as [`Pending`](IntentPhase::Pending).
    Admitted,
    /// The key was already staged for the same content, so the first admission stands.
    Duplicate,
    /// The key was already staged for different content, so it is refused rather than overwriting it.
    Conflict,
    /// The retention buffer already holds its limit, so a new intent is refused.
    RejectedOverLimit,
}

/// Whether a lifecycle transition advanced a staged intent or was dropped as a replay or a move backward.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentTransition {
    /// The target phase was later than the intent's current phase, so the intent advanced to it.
    Advanced,
    /// The target was the current phase or earlier, or no such intent exists, so the phase stands.
    Ignored,
}

impl MetaStore {
    /// Stage `intent_key` for content `digest`/`size`, retaining `payload` for the finalization path.
    ///
    /// The read, the retention check, and the insert share one write transaction, so two racing
    /// admissions of the same key never both take a slot. A key already staged for the same content is a
    /// [`Duplicate`](IntentStageOutcome::Duplicate); the same key for different content a
    /// [`Conflict`](IntentStageOutcome::Conflict); a new key once the ledger holds `limit` intents a
    /// [`RejectedOverLimit`](IntentStageOutcome::RejectedOverLimit).
    ///
    /// # Errors
    /// Returns a store error when the row cannot be read, encoded, or committed.
    pub fn stage_intent(
        &self,
        intent_key: &str,
        digest: &str,
        size: u64,
        payload: &[u8],
        limit: usize,
        now: i64,
    ) -> Result<IntentStageOutcome, MetaError> {
        let txn = self.db.begin_write()?;
        let outcome;
        {
            let mut table = txn.open_table(INGRESS_INTENT)?;
            let existing = table
                .get(intent_key)?
                .map(|value| serde_json::from_slice::<StagedIntent>(value.value()))
                .transpose()?;
            outcome = match existing {
                Some(record) if record.digest == digest && record.size == size => IntentStageOutcome::Duplicate,
                Some(_) => IntentStageOutcome::Conflict,
                None if table.len()? >= limit as u64 => IntentStageOutcome::RejectedOverLimit,
                None => {
                    let record = StagedIntent {
                        phase: IntentPhase::Pending,
                        digest: digest.to_owned(),
                        size,
                        payload: payload.to_vec(),
                        updated_at_unix: now,
                    };
                    table.insert(intent_key, serde_json::to_vec(&record)?.as_slice())?;
                    IntentStageOutcome::Admitted
                }
            };
        }
        txn.commit()?;
        Ok(outcome)
    }

    /// Advance the intent under `intent_key` to `to`, returning whether it moved.
    ///
    /// The transition applies only when `to` is later than the intent's current phase, so a replayed or
    /// reordered event, or one naming an unknown intent, leaves the lifecycle unchanged.
    ///
    /// # Errors
    /// Returns a store error when the row cannot be read, encoded, or committed.
    pub fn advance_intent(&self, intent_key: &str, to: IntentPhase, now: i64) -> Result<IntentTransition, MetaError> {
        let txn = self.db.begin_write()?;
        let outcome;
        {
            let mut table = txn.open_table(INGRESS_INTENT)?;
            let existing = table
                .get(intent_key)?
                .map(|value| serde_json::from_slice::<StagedIntent>(value.value()))
                .transpose()?;
            outcome = match existing {
                Some(mut record) if to > record.phase => {
                    record.phase = to;
                    record.updated_at_unix = now;
                    table.insert(intent_key, serde_json::to_vec(&record)?.as_slice())?;
                    IntentTransition::Advanced
                }
                _ => IntentTransition::Ignored,
            };
        }
        txn.commit()?;
        Ok(outcome)
    }

    /// Read the intent staged under `intent_key`, or `None` when none is retained.
    ///
    /// # Errors
    /// Returns a store error when the row cannot be read or decoded.
    pub fn staged_intent(&self, intent_key: &str) -> Result<Option<StagedIntent>, MetaError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(INGRESS_INTENT)?;
        Ok(table
            .get(intent_key)?
            .map(|value| serde_json::from_slice(value.value()))
            .transpose()?)
    }

    /// List up to `limit` intents still [`Pending`](IntentPhase::Pending), in key order.
    ///
    /// The drain reads its work through this: key order is the stable resume order, so a re-run after an
    /// interruption picks up at the first intent still pending, the finalized ones having advanced out of
    /// the pending set.
    ///
    /// # Errors
    /// Returns a store error when the table cannot be read or a record decoded.
    pub fn list_pending_intents(&self, limit: usize) -> Result<Vec<(String, StagedIntent)>, MetaError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(INGRESS_INTENT)?;
        let mut pending = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            let record: StagedIntent = serde_json::from_slice(value.value())?;
            if record.phase == IntentPhase::Pending {
                pending.push((key.value().to_owned(), record));
                if pending.len() == limit {
                    break;
                }
            }
        }
        Ok(pending)
    }

    /// How many intents the ledger currently retains, the durable count the retention limit bounds.
    ///
    /// # Errors
    /// Returns a store error when the table cannot be read.
    pub fn count_staged_intents(&self) -> Result<u64, MetaError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(INGRESS_INTENT)?;
        Ok(table.len()?)
    }
}

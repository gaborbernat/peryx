//! The neutral finalize primitive: commit an admitted write's metadata, its outbox journal, its
//! terminal operation outcome, and the advance of its staging intent in one redb transaction.
//!
//! A home DC finalizes an upload another node admitted. The four facts a finalize records — the
//! authoritative rows, the journal entries a replica reconciles, the operation outcome a retry
//! replays, and the intent's advance out of [`Pending`](super::IntentPhase::Pending) — are one fact,
//! so they commit together: a crash between any two would leave a published artifact no replica
//! receives, an outcome a retry cannot find, or an intent a home DC finalizes twice. The outcome
//! check runs in that transaction too, so a duplicate finalize racing the first cannot slip between a
//! separate read and the publish and append a second journal entry for an artifact already published.
//!
//! The ecosystem body stays opaque here, like [`commit_driver_txn`](MetaStore::commit_driver_txn): it
//! stages its own rows and returns the journal entries to record, and this module frames them with the
//! outcome and the intent advance.

use redb::ReadableTable as _;

use super::ingress_intent::{IntentPhase, StagedIntent};
use super::operation_outcome::{OperationOutcomeRecord, OperationState};
use super::{DriverTxn, INGRESS_INTENT, MetaError, MetaStore, OPERATION_OUTCOME};

/// The verdict of a finalize commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalizeOutcome {
    /// The write committed for the first time: its rows, journal, terminal outcome, and intent advance
    /// went in together.
    Published,
    /// A prior attempt already committed a terminal outcome, replayed verbatim with no second write.
    Replayed(OperationOutcomeRecord),
}

/// How a finalize transaction ended, threading the caller's error `E` alongside the internal
/// race-replay signal so both leave the transaction dropped through one error channel.
enum FinalizeFlow<E> {
    /// The caller's body, or a store fault mapped into it, rejected the finalize.
    User(E),
    /// The outcome was already terminal when the finalize hook read it, so the staged rows and journal
    /// are discarded and the stored outcome replayed instead.
    RaceReplay,
}

impl<E: From<MetaError>> From<MetaError> for FinalizeFlow<E> {
    fn from(err: MetaError) -> Self {
        Self::User(E::from(err))
    }
}

impl MetaStore {
    /// Commit an admitted write's `body` rows, its journal, a [`Published`](OperationState::Published)
    /// outcome for `operation` replaying `response`, and the advance of `intent_key` to
    /// [`Admitted`](super::IntentPhase::Admitted), all in one transaction.
    ///
    /// `body` reads and stages the authoritative rows through the [`DriverTxn`] and returns the journal
    /// entries to record, exactly as [`commit_driver_txn`](MetaStore::commit_driver_txn) takes them.
    /// The outcome for `operation` is read inside that transaction: a terminal outcome — the write
    /// already published, by a first attempt or a racing duplicate — discards the staged rows and the
    /// journal and returns [`Replayed`](FinalizeOutcome::Replayed) with the stored record, so the
    /// mutation and its journal entries commit exactly once. `expiry_unix` bounds when the outcome may
    /// be pruned. An absent `intent_key` advances nothing.
    ///
    /// # Errors
    /// Returns the body's error, or a store error mapped into it, if the transaction fails to open,
    /// read, write, or commit.
    pub fn commit_finalized_write<E: From<MetaError>>(
        &self,
        operation: &str,
        intent_key: &str,
        response: &[u8],
        expiry_unix: Option<i64>,
        now: i64,
        body: impl FnOnce(&mut DriverTxn) -> Result<Vec<Vec<u8>>, E>,
    ) -> Result<FinalizeOutcome, E> {
        let committed = self.commit_driver_txn_at(
            None,
            None,
            true,
            |txn, ()| stamp_finalized(txn, operation, intent_key, response, expiry_unix, now),
            |driver| body(driver).map(|journal| ((), journal)).map_err(FinalizeFlow::User),
        );
        match committed {
            Ok(()) => Ok(FinalizeOutcome::Published),
            Err(FinalizeFlow::User(err)) => Err(err),
            Err(FinalizeFlow::RaceReplay) => {
                let record = self
                    .operation_outcome(operation)?
                    .expect("a race-replay observed a terminal outcome that is still stored");
                Ok(FinalizeOutcome::Replayed(record))
            }
        }
    }
}

/// Stamp the terminal outcome and advance the intent inside the open finalize transaction, or signal a
/// race-replay when the outcome is already terminal.
fn stamp_finalized<E: From<MetaError>>(
    txn: &redb::WriteTransaction,
    operation: &str,
    intent_key: &str,
    response: &[u8],
    expiry_unix: Option<i64>,
    now: i64,
) -> Result<(), FinalizeFlow<E>> {
    let mut outcomes = txn.open_table(OPERATION_OUTCOME).map_err(MetaError::from)?;
    let existing = outcomes
        .get(operation)
        .map_err(MetaError::from)?
        .map(|value| serde_json::from_slice::<OperationOutcomeRecord>(value.value()))
        .transpose()
        .map_err(MetaError::from)?;
    if existing.is_some_and(|record| record.state.is_terminal()) {
        return Err(FinalizeFlow::RaceReplay);
    }
    let record = OperationOutcomeRecord {
        state: OperationState::Published,
        response: response.to_vec(),
        expiry_unix,
        updated_at_unix: now,
    };
    let encoded = serde_json::to_vec(&record).map_err(MetaError::from)?;
    outcomes
        .insert(operation, encoded.as_slice())
        .map_err(MetaError::from)?;
    advance_intent_to_admitted(txn, intent_key, now)
}

/// Advance a staged intent to [`Admitted`](super::IntentPhase::Admitted) in the finalize transaction,
/// guarding the move forward the way [`advance_intent`](MetaStore::advance_intent) does so a replay or
/// an absent intent leaves the ledger unchanged.
fn advance_intent_to_admitted<E: From<MetaError>>(
    txn: &redb::WriteTransaction,
    intent_key: &str,
    now: i64,
) -> Result<(), FinalizeFlow<E>> {
    let mut intents = txn.open_table(INGRESS_INTENT).map_err(MetaError::from)?;
    let existing = intents
        .get(intent_key)
        .map_err(MetaError::from)?
        .map(|value| serde_json::from_slice::<StagedIntent>(value.value()))
        .transpose()
        .map_err(MetaError::from)?;
    if let Some(mut record) = existing.filter(|record| IntentPhase::Admitted > record.phase) {
        record.phase = IntentPhase::Admitted;
        record.updated_at_unix = now;
        let encoded = serde_json::to_vec(&record).map_err(MetaError::from)?;
        intents
            .insert(intent_key, encoded.as_slice())
            .map_err(MetaError::from)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "finalize_fault_tests.rs"]
mod fault_tests;

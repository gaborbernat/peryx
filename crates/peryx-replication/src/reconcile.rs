//! Classifying a durable old-epoch operation after authority transfer.
//!
//! Once a newer epoch commits, the [authority fence](crate::AuthorityFence) stops the old home from
//! applying more work, but operations it durably recorded before the transfer still sit in the log and
//! need a disposition. This is that classification: given what the reconciler has derived about one
//! old-epoch operation, [`classify`] returns the single terminal category it falls into, so every
//! operation reaches exactly one outcome.
//!
//! The classification is pure. Deriving the facts in [`OldEpochOp`] from the committed operation
//! record, its epoch, and current metadata, replaying a [`Replayable`](Disposition::Replayable)
//! operation under the new epoch while preserving its original audit identity, and gating cleanup on
//! replica and retention frontiers are the reconciler's wiring, deferred to later work.

/// What the reconciler has derived about one durable old-epoch operation, from the committed operation
/// record, its epoch, and the current metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OldEpochOp {
    /// The operation reached a durable commit before authority transferred. `false` means it was in
    /// flight and never committed, so it has no effect to keep.
    pub durably_committed: bool,
    /// The current committed state already carries this operation's effect, so applying it again would
    /// be a no-op.
    pub already_applied: bool,
    /// A newer committed operation has overwritten this operation's target, so its intent no longer
    /// stands.
    pub superseded: bool,
}

/// The single terminal outcome an old-epoch operation reconciles to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Its effect is already in the committed state; reconciliation is a no-op.
    AlreadyApplied,
    /// Durable and still standing; the reconciler replays it under the new epoch.
    Replayable,
    /// A newer operation has overwritten its target; it is dropped as obsolete.
    Superseded,
    /// It never reached a durable commit; it terminates as failed with nothing to apply.
    Failed,
}

/// Classify `op` into the one terminal [`Disposition`] it reconciles to.
///
/// The precedence is fixed so the outcome is deterministic and single-valued: an operation that never
/// committed fails outright, ahead of every other consideration; an already-applied operation is a
/// no-op even when a later operation also superseded it, because idempotency has already settled its
/// effect; a durable, unapplied operation whose target a newer operation overwrote is superseded; and a
/// durable, unapplied, unsuperseded operation still stands and is replayable.
#[must_use]
pub const fn classify(op: &OldEpochOp) -> Disposition {
    if !op.durably_committed {
        Disposition::Failed
    } else if op.already_applied {
        Disposition::AlreadyApplied
    } else if op.superseded {
        Disposition::Superseded
    } else {
        Disposition::Replayable
    }
}

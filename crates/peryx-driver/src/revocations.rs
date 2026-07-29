//! Transactional digest-revocation operations and the bounded serving-decision cache.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use moka::sync::Cache;
use peryx_events::security::Event;
use peryx_identity::{ArtifactDigest, DigestDecision, RevocationReason, UserId};
use peryx_storage::meta::{
    DigestRevocation, DigestRevocationPage, DigestRevocationQuery, DigestRevocationQueryError, DigestRevocationState,
    LiftRevocationOutcome, MetaError, MetaStore, PutRevocationError, PutRevocationOutcome,
};

const CACHE_CAPACITY: u64 = 16_384;
const CACHE_TTL: Duration = Duration::from_mins(1);

/// Shared digest-revocation operations for HTTP management and future download enforcement.
#[derive(Clone)]
pub struct RevocationService {
    store: MetaStore,
    decisions: Cache<ArtifactDigest, DigestDecision>,
    cache_gate: Arc<RwLock<()>>,
}

impl RevocationService {
    #[must_use]
    pub fn new(store: MetaStore) -> Self {
        Self {
            store,
            decisions: Cache::builder()
                .max_capacity(CACHE_CAPACITY)
                .time_to_live(CACHE_TTL)
                .build(),
            cache_gate: Arc::new(RwLock::new(())),
        }
    }

    /// Decide whether a digest is currently clear or revoked with one indexed store read on a cache
    /// miss. Store errors are returned rather than cached or converted to clear.
    ///
    /// # Errors
    /// Returns a store error when the authoritative row cannot be read.
    pub fn decision(&self, digest: &ArtifactDigest) -> Result<DigestDecision, MetaError> {
        if let Some(decision) = self.decisions.get(digest) {
            return Ok(decision);
        }
        let _guard = self
            .cache_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let decision = self
            .store
            .digest_revocation(digest)?
            .map_or(DigestDecision::Clear, |record| match record.state {
                DigestRevocationState::Active => DigestDecision::Revoked,
                DigestRevocationState::Lifted { .. } => DigestDecision::Clear,
            });
        self.decisions.insert(digest.clone(), decision);
        Ok(decision)
    }

    /// Create, retry, or reopen one revocation and invalidate exactly its cached decision before a
    /// concurrent miss can publish an older read.
    ///
    /// # Errors
    /// Returns a reason conflict or store error from the transaction.
    pub fn put(
        &self,
        digest: &ArtifactDigest,
        reason: &RevocationReason,
        actor: &UserId,
        now: i64,
    ) -> Result<PutRevocationOutcome, PutRevocationError> {
        let guard = self
            .cache_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let outcome = self.store.put_digest_revocation(digest, reason, actor, now)?;
        self.decisions.invalidate(digest);
        drop(guard);
        emit_change(
            "digest_revocation_put",
            digest,
            actor,
            !matches!(outcome, PutRevocationOutcome::Unchanged(_)),
        );
        Ok(outcome)
    }

    /// Lift one revocation and invalidate exactly its cached decision before releasing the mutation
    /// gate.
    ///
    /// # Errors
    /// Returns a store error from the transaction.
    pub fn lift(
        &self,
        digest: &ArtifactDigest,
        actor: &UserId,
        now: i64,
    ) -> Result<Option<LiftRevocationOutcome>, MetaError> {
        let guard = self
            .cache_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let outcome = self.store.lift_digest_revocation(digest, actor, now)?;
        self.decisions.invalidate(digest);
        drop(guard);
        emit_change(
            "digest_revocation_lift",
            digest,
            actor,
            matches!(outcome, Some(LiftRevocationOutcome::Lifted(_))),
        );
        Ok(outcome)
    }

    /// Inspect one current row.
    ///
    /// # Errors
    /// Returns a store error when the row cannot be read.
    pub fn inspect(&self, digest: &ArtifactDigest) -> Result<Option<DigestRevocation>, MetaError> {
        self.store.digest_revocation(digest)
    }

    /// Check whether any active record requires discovery filtering.
    ///
    /// # Errors
    /// Returns a store error when the transactional active index cannot be read.
    pub fn has_active(&self) -> Result<bool, MetaError> {
        self.store.has_active_digest_revocation()
    }

    /// List a bounded page of current rows.
    ///
    /// # Errors
    /// Returns a query or store error.
    pub fn list(&self, query: &DigestRevocationQuery) -> Result<DigestRevocationPage, DigestRevocationQueryError> {
        self.store.query_digest_revocations(query)
    }
}

fn emit_change(action: &'static str, digest: &ArtifactDigest, actor: &UserId, changed: bool) {
    let digest = digest.canonical();
    Event::new(action, "success")
        .actor(Some(actor.as_str()))
        .digest(Some(&digest))
        .changed(changed)
        .emit();
}

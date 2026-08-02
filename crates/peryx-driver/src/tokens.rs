//! The scoped-token lifecycle service shared by the management handlers.
//!
//! It mints and rotates the one-time secret, persists only its verifier, and emits a lifecycle security
//! event that names the actor and token without ever carrying the secret.

use std::collections::BTreeSet;

use peryx_events::security::Event;
use peryx_identity::{Action, GrantScope, TokenId, TokenName, TokenSecret, UserId};
use peryx_storage::meta::{
    MetaError, MetaStore, NewScopedToken, RevokeScopedTokenOutcome, ScopedTokenPage, ScopedTokenQuery,
    ScopedTokenQueryError, ScopedTokenRecord,
};

/// Persistent scoped-token operations over the metadata store.
#[derive(Debug, Clone)]
pub struct TokenService {
    store: MetaStore,
}

/// A token to mint: the reach and actions to grant, validated against the caller's authority before it
/// reaches this service.
#[derive(Debug, Clone)]
pub struct CreateScopedToken {
    pub name: TokenName,
    pub reach: GrantScope,
    pub actions: BTreeSet<Action>,
    pub expires_at: Option<i64>,
    pub created_by: UserId,
}

impl TokenService {
    #[must_use]
    pub const fn new(store: MetaStore) -> Self {
        Self { store }
    }

    /// Mint a token, returning its record and the one-time secret a client must store now.
    ///
    /// # Errors
    /// Returns a store error when the token cannot be persisted.
    pub fn create(&self, request: CreateScopedToken, now: i64) -> Result<(ScopedTokenRecord, TokenSecret), MetaError> {
        let secret = TokenSecret::generate();
        let record = self.store.create_scoped_token(NewScopedToken {
            name: request.name,
            reach: request.reach,
            actions: request.actions,
            expires_at: request.expires_at,
            verifier: secret.verifier(),
            created_by: request.created_by,
            created_at_unix: now,
        })?;
        emit("scoped_token_created", &record.created_by, &record.id);
        Ok((record, secret))
    }

    /// Read one token's metadata, revoked or live.
    ///
    /// # Errors
    /// Returns a store error when the row cannot be read.
    pub fn inspect(&self, id: &TokenId) -> Result<Option<ScopedTokenRecord>, MetaError> {
        self.store.get_scoped_token(id)
    }

    /// List one reach's tokens, paginated.
    ///
    /// # Errors
    /// Returns a query or store error.
    pub fn list(&self, query: &ScopedTokenQuery) -> Result<ScopedTokenPage, ScopedTokenQueryError> {
        self.store.list_scoped_tokens(query)
    }

    /// Rotate a live token's secret, returning the updated record and the new one-time secret. A missing
    /// or revoked token returns `None` with no change.
    ///
    /// # Errors
    /// Returns a store error when the rotation cannot be committed.
    pub fn rotate(&self, id: &TokenId, actor: &UserId) -> Result<Option<(ScopedTokenRecord, TokenSecret)>, MetaError> {
        let secret = TokenSecret::generate();
        let Some(record) = self.store.rotate_scoped_token(id, &secret.verifier())? else {
            return Ok(None);
        };
        emit("scoped_token_rotated", actor, &record.id);
        Ok(Some((record, secret)))
    }

    /// Revoke a token, blocking its next request. Idempotent.
    ///
    /// # Errors
    /// Returns a store error when the revocation cannot be committed.
    pub fn revoke(
        &self,
        id: &TokenId,
        actor: &UserId,
        now: i64,
    ) -> Result<Option<RevokeScopedTokenOutcome>, MetaError> {
        let outcome = self.store.revoke_scoped_token(id, now)?;
        if let Some(RevokeScopedTokenOutcome::Revoked(record)) = &outcome {
            emit("scoped_token_revoked", actor, &record.id);
        }
        Ok(outcome)
    }

    /// Resolve the live token a presented secret authenticates, reading no more than one indexed row and
    /// writing nothing.
    ///
    /// # Errors
    /// Returns a store error when the lookup cannot be read.
    pub fn verify(&self, presented: &TokenSecret, now: i64) -> Result<Option<ScopedTokenRecord>, MetaError> {
        self.store.verify_scoped_token(presented, now)
    }
}

fn emit(action: &'static str, actor: &UserId, id: &TokenId) {
    Event::new(action, "success")
        .actor(Some(actor.as_str()))
        .token_id(id.as_str())
        .emit();
}

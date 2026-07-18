//! The one authorization service HTTP, UI, and CLI-backed operations share to decide a role-based
//! access question.
//!
//! It wraps the persisted role grants ([`MetaStore`]) and the fixed decision model
//! ([`peryx_identity::grants_permit`]). Every decision reads the authoritative grants for the user, so
//! there is no cached copy to invalidate: a revoked grant is absent from the very next decision without
//! a restart. A read is a snapshot-isolated redb transaction and never a write, so authorizing a
//! package download performs no database write.
//!
//! Decisions fail closed. When the grant store cannot be read the answer is [`Decision::Deny`] with
//! [`DenyReason::StorageUnavailable`], never an allow, so a storage fault cannot open access. Each
//! decision emits one bounded security event carrying the user, scope, resource, and outcome — never a
//! credential — for both allowed and denied results.

use peryx_identity::{GrantScope, Resource, Role, RoleGrant, Scope, UserId, grants_permit};
use peryx_storage::meta::{MetaError, MetaStore, RoleGrantStoreError};

/// Role-based authorization over persistent server users.
#[derive(Debug, Clone)]
pub struct AuthorizationService {
    store: MetaStore,
}

/// The outcome of an authorization decision. It has no allow variant that carries a storage error, so
/// a fault can only ever deny.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny(DenyReason),
}

/// Why a decision denied: the user held no covering grant, or the grants could not be read and the
/// decision failed closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenyReason {
    NoGrant,
    StorageUnavailable,
}

impl Decision {
    #[must_use]
    pub const fn is_allowed(self) -> bool {
        matches!(self, Self::Allow)
    }

    const fn result(self) -> &'static str {
        if self.is_allowed() { "allowed" } else { "denied" }
    }

    const fn reason(self) -> &'static str {
        match self {
            Self::Allow => "granted",
            Self::Deny(DenyReason::NoGrant) => "no_grant",
            Self::Deny(DenyReason::StorageUnavailable) => "storage_unavailable",
        }
    }
}

impl AuthorizationService {
    #[must_use]
    pub const fn new(store: MetaStore) -> Self {
        Self { store }
    }

    /// Grant a role to a user over one reach, idempotently.
    ///
    /// # Errors
    /// Returns [`RoleGrantStoreError::UnknownUser`] for an unknown user or a store error when the
    /// grant cannot be committed.
    pub fn grant(&self, user: &UserId, role: Role, scope: GrantScope) -> Result<RoleGrant, RoleGrantStoreError> {
        self.store.grant_role(user, role, scope)
    }

    /// Revoke a role a user held over one reach, reporting whether a binding was present. The next
    /// [`authorize`](Self::authorize) reflects the removal with no restart.
    ///
    /// # Errors
    /// Returns a store error when the revocation cannot be committed.
    pub fn revoke(&self, user: &UserId, role: Role, scope: &GrantScope) -> Result<bool, MetaError> {
        self.store.revoke_role(user, role, scope)
    }

    /// Read every role a user holds.
    ///
    /// # Errors
    /// Returns a store error when the grants cannot be read.
    pub fn grants(&self, user: &UserId) -> Result<Vec<RoleGrant>, MetaError> {
        self.store.user_role_grants(user)
    }

    /// Decide whether `user` may take `scope` on `resource`, failing closed on a storage fault and
    /// emitting one bounded security event for the outcome.
    #[must_use]
    pub fn authorize(&self, user: &UserId, scope: Scope, resource: &Resource) -> Decision {
        let decision = match self.store.user_role_grants(user) {
            Ok(grants) if grants_permit(&grants, scope, resource) => Decision::Allow,
            Ok(_) => Decision::Deny(DenyReason::NoGrant),
            Err(_) => Decision::Deny(DenyReason::StorageUnavailable),
        };
        // Compute the log fields before the macro: as macro arguments they would evaluate only when the
        // callsite is enabled, so a run without a security-log subscriber would never cover them.
        let user = user.as_str();
        let scope = scope.as_str();
        let (resource_kind, resource_name) = resource.fields();
        let result = decision.result();
        let reason = decision.reason();
        tracing::info!(
            target: "peryx::security",
            security_event = true,
            event = "authorization",
            user,
            scope,
            resource_kind,
            resource = resource_name,
            result,
            reason,
            "role authorization decision"
        );
        decision
    }
}

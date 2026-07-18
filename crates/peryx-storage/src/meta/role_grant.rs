use peryx_identity::{GrantScope, Role, RoleGrant, UserId};
use redb::{ReadableDatabase as _, ReadableTable as _, WriteTransaction};

use super::{MetaError, MetaStore, ROLE_GRANT, USER};

/// A rejected role-grant operation.
#[derive(Debug, thiserror::Error)]
pub enum RoleGrantStoreError {
    #[error(transparent)]
    Store(#[from] MetaError),
    #[error("server user {id} does not exist")]
    UnknownUser { id: UserId },
}

impl MetaStore {
    /// Grant a role to a user over one reach, idempotently: re-granting the same role and reach is a
    /// no-op, so a caller need not first check whether the binding exists.
    ///
    /// # Errors
    /// Returns [`RoleGrantStoreError::UnknownUser`] when no server user holds `id`, or a store error
    /// when the transaction cannot commit.
    pub fn grant_role(&self, id: &UserId, role: Role, scope: GrantScope) -> Result<RoleGrant, RoleGrantStoreError> {
        let txn = self.db.begin_write().map_err(MetaError::from)?;
        if !user_exists(&txn, id)? {
            return Err(RoleGrantStoreError::UnknownUser { id: id.clone() });
        }
        let grant = RoleGrant::new(id.clone(), role, scope);
        let key = grant_key_of(&grant.user, grant.role, &grant.scope);
        let bytes = serde_json::to_vec(&grant).map_err(MetaError::from)?;
        txn.open_table(ROLE_GRANT)
            .map_err(MetaError::from)?
            .insert(key.as_str(), bytes.as_slice())
            .map_err(MetaError::from)?;
        txn.commit().map_err(MetaError::from)?;
        Ok(grant)
    }

    /// Remove a role a user held over one reach, reporting whether a binding was present. The next
    /// [`user_role_grants`](Self::user_role_grants) read reflects the removal with no restart.
    ///
    /// # Errors
    /// Returns a store error when the transaction cannot commit.
    pub fn revoke_role(&self, id: &UserId, role: Role, scope: &GrantScope) -> Result<bool, MetaError> {
        let txn = self.db.begin_write()?;
        let removed = txn
            .open_table(ROLE_GRANT)?
            .remove(grant_key_of(id, role, scope).as_str())?
            .is_some();
        txn.commit()?;
        Ok(removed)
    }

    /// Read every role a user holds, the authority an authorization decision runs against. An unknown
    /// user and a user with no grants both read as empty.
    ///
    /// # Errors
    /// Returns a store error when a record cannot be read or decoded.
    pub fn user_role_grants(&self, id: &UserId) -> Result<Vec<RoleGrant>, MetaError> {
        let txn = self.db.begin_read()?;
        let table = match txn.open_table(ROLE_GRANT) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let (start, end) = (format!("{id}/"), format!("{id}0"));
        let mut grants = Vec::new();
        for entry in table.range(start.as_str()..end.as_str())? {
            let (_, value) = entry?;
            grants.push(serde_json::from_slice(value.value())?);
        }
        Ok(grants)
    }
}

fn user_exists(txn: &WriteTransaction, id: &UserId) -> Result<bool, MetaError> {
    Ok(txn.open_table(USER)?.get(id.as_str())?.is_some())
}

fn grant_key_of(id: &UserId, role: Role, scope: &GrantScope) -> String {
    let reach = match scope {
        GrantScope::Server => "server".to_owned(),
        GrantScope::Repository { name } => format!("repository/{name}"),
    };
    format!("{id}/{}/{reach}", role.as_str())
}

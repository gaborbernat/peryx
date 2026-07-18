use peryx_identity::{GrantScope, Role, RoleGrant, ServerUser, UserId, UserName, UserState};
use redb::TableDefinition;

use super::store;
use crate::meta::{MetaError, MetaStore, RoleGrantStoreError};

const RAW_USER: TableDefinition<&str, &[u8]> = TableDefinition::new("server_user");

fn repository(name: &str) -> GrantScope {
    GrantScope::Repository { name: name.to_owned() }
}

fn raw_store(setup: impl FnOnce(&redb::WriteTransaction)) -> (tempfile::TempDir, MetaStore) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("peryx.redb");
    let database = redb::Database::create(&path).unwrap();
    let txn = database.begin_write().unwrap();
    setup(&txn);
    txn.commit().unwrap();
    drop(database);
    (dir, MetaStore::open_existing(path).unwrap())
}

fn persist_user(txn: &redb::WriteTransaction, id: &UserId) {
    let user = ServerUser {
        id: id.clone(),
        name: UserName::new("Alice").unwrap(),
        state: UserState::Active,
        revision: 1,
    };
    let bytes = serde_json::to_vec(&user).unwrap();
    txn.open_table(RAW_USER)
        .unwrap()
        .insert(id.as_str(), bytes.as_slice())
        .unwrap();
}

#[test]
fn test_grants_persist_and_read_back_for_the_user() {
    let (dir, store) = store();
    let alice = store.create_user("Alice").unwrap().id;
    let bob = store.create_user("Bob").unwrap().id;
    let publisher = store
        .grant_role(&alice, Role::RepositoryPublisher, repository("team/api"))
        .unwrap();
    let operator = store.grant_role(&alice, Role::Operator, GrantScope::Server).unwrap();
    store
        .grant_role(&bob, Role::RepositoryReader, repository("team/web"))
        .unwrap();
    drop(store);

    let reopened = MetaStore::open_existing(dir.path().join("peryx.redb")).unwrap();
    let alice_grants = reopened.user_role_grants(&alice).unwrap();
    assert_eq!(alice_grants.len(), 2);
    assert!(alice_grants.contains(&publisher));
    assert!(alice_grants.contains(&operator));
    assert_eq!(
        reopened.user_role_grants(&bob).unwrap(),
        vec![RoleGrant::new(bob, Role::RepositoryReader, repository("team/web"))]
    );
}

#[test]
fn test_regranting_the_same_role_and_reach_is_idempotent() {
    let (_dir, store) = store();
    let alice = store.create_user("Alice").unwrap().id;

    store
        .grant_role(&alice, Role::RepositoryReader, repository("team/api"))
        .unwrap();
    store
        .grant_role(&alice, Role::RepositoryReader, repository("team/api"))
        .unwrap();

    assert_eq!(store.user_role_grants(&alice).unwrap().len(), 1);
}

#[test]
fn test_revoke_removes_only_the_named_binding_and_reports_presence() {
    let (_dir, store) = store();
    let alice = store.create_user("Alice").unwrap().id;
    store
        .grant_role(&alice, Role::RepositoryReader, repository("team/api"))
        .unwrap();
    store.grant_role(&alice, Role::Operator, GrantScope::Server).unwrap();

    assert!(
        store
            .revoke_role(&alice, Role::RepositoryReader, &repository("team/api"))
            .unwrap()
    );
    assert!(
        !store
            .revoke_role(&alice, Role::RepositoryReader, &repository("team/api"))
            .unwrap()
    );

    assert_eq!(
        store.user_role_grants(&alice).unwrap(),
        vec![RoleGrant::new(alice, Role::Operator, GrantScope::Server)]
    );
}

#[test]
fn test_grant_rejects_an_unknown_user() {
    let (_dir, store) = store();
    let missing = UserId::random();

    assert!(matches!(
        store.grant_role(&missing, Role::Administrator, GrantScope::Server),
        Err(RoleGrantStoreError::UnknownUser { id }) if id == missing
    ));
}

#[test]
fn test_grants_read_empty_before_the_table_exists() {
    let (_dir, store) = raw_store(|txn| {
        txn.open_table(RAW_USER).unwrap();
    });

    assert_eq!(store.user_role_grants(&UserId::random()).unwrap(), Vec::new());
}

#[test]
fn test_grant_operations_surface_an_incompatible_table() {
    let alice = UserId::random();
    let (_dir, store) = raw_store(|txn| {
        persist_user(txn, &alice);
        txn.open_table(TableDefinition::<&str, u64>::new("role_grant"))
            .unwrap()
            .insert(alice.as_str(), 1)
            .unwrap();
    });

    assert!(matches!(
        store.grant_role(&alice, Role::Operator, GrantScope::Server),
        Err(RoleGrantStoreError::Store(MetaError::Table(_)))
    ));
    assert!(matches!(store.user_role_grants(&alice), Err(MetaError::Table(_))));
    assert!(matches!(
        store.revoke_role(&alice, Role::Operator, &GrantScope::Server),
        Err(MetaError::Table(_))
    ));
}

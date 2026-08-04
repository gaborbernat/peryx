use std::collections::BTreeMap;
use std::time::Duration;

use openraft::error::{ClientWriteError, Fatal, ForwardToLeader, InitializeError, RaftError};
use openraft::ServerState;
use tempfile::TempDir;

use peryx_storage::raft::RaftLogStore;

use crate::ownership::{DatacenterId, OwnershipCommand, OwnershipEffect, Rejection};
use crate::raft::log_store::RaftLogStoreAdapter;
use crate::raft::network::PeerRaftNetworkFactory;
use crate::raft::{OwnershipResponse, OwnershipStateMachine, PeryxNode, RaftConfig, RaftNode, StartError};
use crate::AuthorityKey;

const RPC_TIMEOUT: Duration = Duration::from_secs(1);

fn peer(dc: &str, addr: &str) -> PeryxNode {
    PeryxNode {
        datacenter: DatacenterId(dc.to_owned()),
        addr: addr.to_owned(),
    }
}

fn seed_roster() -> BTreeMap<u64, PeryxNode> {
    BTreeMap::from([(1, peer("east", "localhost:4460"))])
}

/// Assemble a fresh single-node ownership group over a redb log store in `dir` and return it with the
/// directory guard so the database file outlives the node.
async fn start_node(dir: &TempDir, config: RaftConfig) -> Result<RaftNode, StartError> {
    let store = RaftLogStore::open(dir.path().join("raft.redb")).unwrap();
    RaftNode::start(
        1,
        config,
        "ownership",
        PeerRaftNetworkFactory::new("group-secret", RPC_TIMEOUT),
        RaftLogStoreAdapter::new(store),
        OwnershipStateMachine::default(),
    )
    .await
}

/// A bootstrapped single-node group that has already won its election, ready to accept writes.
async fn leader_node(dir: &TempDir) -> RaftNode {
    let node = start_node(dir, RaftConfig::default()).await.unwrap();
    node.bootstrap(seed_roster()).await.unwrap();
    node.raft()
        .wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "single node elects itself")
        .await
        .unwrap();
    node
}

#[tokio::test]
async fn test_a_bootstrapped_single_node_becomes_its_own_leader() {
    let dir = tempfile::tempdir().unwrap();
    let node = leader_node(&dir).await;

    assert_eq!(node.leader(), Some(peer("east", "localhost:4460")));
    assert_eq!(node.metrics().borrow().current_leader, Some(1));
}

#[tokio::test]
async fn test_the_leader_lookup_skips_a_non_leader_member() {
    let dir = tempfile::tempdir().unwrap();
    let node = leader_node(&dir).await;

    // A learner ordered before the leader forces the roster scan past a non-matching member, so the
    // miss branch is exercised, not only the hit on a lone member.
    node.raft()
        .add_learner(0, peer("west", "localhost:4470"), false)
        .await
        .unwrap();

    assert_eq!(node.leader(), Some(peer("east", "localhost:4460")));
}

#[tokio::test]
async fn test_a_committed_command_returns_its_applied_effect() {
    let dir = tempfile::tempdir().unwrap();
    let node = leader_node(&dir).await;

    let response = node
        .submit(OwnershipCommand::AssignHome {
            authority: AuthorityKey("proj".to_owned()),
            home: DatacenterId("east".to_owned()),
        })
        .await
        .unwrap();

    assert!(matches!(
        response,
        OwnershipResponse::Applied(OwnershipEffect::Assigned { .. })
    ));
    node.raft().shutdown().await.unwrap();
}

#[tokio::test]
async fn test_a_submit_without_a_leader_surfaces_the_error() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_node(&dir, RaftConfig::default()).await.unwrap();

    // An unbootstrapped node has no leader, so the write cannot commit and the error propagates
    // instead of hanging or being swallowed.
    let result = node
        .submit(OwnershipCommand::AdvanceAuthorityEpoch {
            authority: AuthorityKey("proj".to_owned()),
        })
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_bootstrap_is_idempotent_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let node = leader_node(&dir).await;

    // A second bootstrap, as a restarted node would issue, is a no-op rather than a NotAllowed error.
    node.bootstrap(seed_roster()).await.unwrap();
}

#[tokio::test]
async fn test_bootstrap_rejects_a_roster_that_omits_this_node() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_node(&dir, RaftConfig::default()).await.unwrap();

    // A roster this node is not a member of is an operator error, distinct from the benign
    // already-initialized case, so it surfaces rather than silently succeeding.
    let result = node
        .bootstrap(BTreeMap::from([(2, peer("west", "localhost:4470"))]))
        .await;

    assert!(matches!(
        result,
        Err(RaftError::APIError(InitializeError::NotInMembers(_)))
    ));
}

#[tokio::test]
async fn test_a_fresh_node_reports_no_leader() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_node(&dir, RaftConfig::default()).await.unwrap();

    assert_eq!(node.leader(), None);
}

#[tokio::test]
async fn test_an_invalid_tuning_fails_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let inverted = RaftConfig {
        election_timeout_min: Duration::from_millis(600),
        election_timeout_max: Duration::from_millis(300),
        ..RaftConfig::default()
    };

    let result = start_node(&dir, inverted).await;

    assert!(matches!(result, Err(StartError::Config(_))));
}

#[tokio::test]
async fn test_a_corrupt_log_store_fails_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let store = RaftLogStore::open(dir.path().join("raft.redb")).unwrap();
    // openraft reads the persisted vote while constructing the node; an undecodable vote is a fatal
    // storage error rather than a benign empty store.
    store.save_vote(b"not valid json").unwrap();
    let result = RaftNode::start(
        1,
        RaftConfig::default(),
        "ownership",
        PeerRaftNetworkFactory::new("group-secret", RPC_TIMEOUT),
        RaftLogStoreAdapter::new(store),
        OwnershipStateMachine::default(),
    )
    .await;

    assert!(matches!(result, Err(StartError::Fatal(_))));
}

#[tokio::test]
async fn test_forward_target_prefers_the_error_leader_over_local_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let node = leader_node(&dir).await;

    let stamped = peer("west", "localhost:4470");
    let error = RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
        leader_id: Some(2),
        leader_node: Some(stamped.clone()),
    }));

    assert_eq!(node.forward_target(&error), Some(stamped));
}

#[tokio::test]
async fn test_forward_target_falls_back_to_local_metrics_without_a_stamped_leader() {
    let dir = tempfile::tempdir().unwrap();
    let node = leader_node(&dir).await;

    let error = RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
        leader_id: None,
        leader_node: None,
    }));

    assert_eq!(node.forward_target(&error), node.leader());
}

#[tokio::test]
async fn test_forward_target_falls_back_for_a_non_forwarding_error() {
    let dir = tempfile::tempdir().unwrap();
    let node = leader_node(&dir).await;

    // An error that is not a forward-to-leader carries no target, so the lookup falls through to the
    // local metrics view rather than misreading the error.
    let error = RaftError::Fatal(Fatal::Stopped);

    assert_eq!(node.forward_target(&error), node.leader());
}

#[tokio::test]
async fn test_a_reassignment_is_rejected_through_the_shared_state() {
    let dir = tempfile::tempdir().unwrap();
    let node = leader_node(&dir).await;
    let assign = OwnershipCommand::AssignHome {
        authority: AuthorityKey("proj".to_owned()),
        home: DatacenterId("east".to_owned()),
    };
    node.submit(assign.clone()).await.unwrap();

    // The first assignment persists in the shared state machine, so a second one is rejected rather
    // than silently reassigned.
    let response = node.submit(assign).await.unwrap();
    assert_eq!(
        response,
        OwnershipResponse::Applied(OwnershipEffect::Rejected(Rejection::AlreadyAssigned))
    );

    // The app reads applied state through this shared handle behind a linearizable barrier.
    node.raft().ensure_linearizable().await.unwrap();
    let _ = node.state_machine();
}

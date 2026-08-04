use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use peryx_driver::state::{HomeClaim, OwnershipAuthority as _, OwnershipError};
use peryx_replication::DatacenterId;
use peryx_replication::raft::log_store::RaftLogStoreAdapter;
use peryx_replication::raft::network::PeerRaftNetworkFactory;
use peryx_replication::raft::{OwnershipStateMachine, PeryxNode, RaftConfig, RaftNode};
use peryx_storage::raft::RaftLogStore;
use tempfile::TempDir;

use super::{ConsensusPlan, OwnershipGroup, authority, build_roster, voter_id};
use crate::config::{AvailabilityConfig, Config, DcMember, DcMembership, DcRole, ReplicationConfig, SecretSource};

const TOKEN: &str = "group-secret";

fn member(node: &str, dc: &str, address: &str, role: DcRole) -> DcMember {
    DcMember {
        node: node.to_owned(),
        dc: dc.to_owned(),
        address: address.to_owned(),
        role,
    }
}

fn ha_config(dir: &TempDir, membership: Option<DcMembership>, identity: Option<&str>, token: SecretSource) -> Config {
    Config {
        data_dir: dir.path().to_path_buf(),
        writer_identity: identity.map(str::to_owned),
        availability: AvailabilityConfig::Ha(ReplicationConfig::Primary {
            source: "seed".to_owned(),
            token,
        }),
        dc_membership: membership,
        ..Config::default()
    }
}

fn seed_membership() -> DcMembership {
    DcMembership {
        group: "ownership".to_owned(),
        members: vec![
            member("node-a", "east", "http://east.internal:4460", DcRole::Writer),
            member("node-b", "west", "http://west.internal:4460", DcRole::Replica),
        ],
    }
}

fn one_voter(dc: &str, addr: &str) -> BTreeMap<u64, PeryxNode> {
    BTreeMap::from([(
        voter_id(dc),
        PeryxNode {
            datacenter: DatacenterId(dc.to_owned()),
            addr: addr.to_owned(),
        },
    )])
}

/// A plan aimed at `log_path`, bypassing `from_config` so a test can drive `ignite`'s failure arms that
/// a validated configuration never produces.
fn plan_at(log_path: PathBuf, local: u64, roster: BTreeMap<u64, PeryxNode>) -> ConsensusPlan {
    ConsensusPlan {
        local,
        home: DatacenterId("east".to_owned()),
        seed: true,
        roster,
        log_path,
        group: "ownership".to_owned(),
        token: TOKEN.to_owned(),
    }
}

#[test]
fn test_none_mode_builds_no_plan() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        data_dir: dir.path().to_path_buf(),
        ..Config::default()
    };

    assert!(ConsensusPlan::from_config(&config).unwrap().is_none());
}

#[test]
fn test_dc_mode_builds_no_plan() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        data_dir: dir.path().to_path_buf(),
        availability: AvailabilityConfig::Dc(ReplicationConfig::Primary {
            source: "seed".to_owned(),
            token: SecretSource::Literal(TOKEN.to_owned()),
        }),
        ..Config::default()
    };

    assert!(ConsensusPlan::from_config(&config).unwrap().is_none());
}

#[test]
fn test_ha_resolves_the_local_voter_and_full_roster() {
    let dir = tempfile::tempdir().unwrap();
    let config = ha_config(
        &dir,
        Some(seed_membership()),
        Some("node-b"),
        SecretSource::Literal(TOKEN.to_owned()),
    );

    let plan = ConsensusPlan::from_config(&config).unwrap().expect("ha builds a plan");

    assert_eq!(plan.local, voter_id("west"));
    assert_eq!(
        plan.roster,
        BTreeMap::from([
            (
                voter_id("east"),
                PeryxNode {
                    datacenter: DatacenterId("east".to_owned()),
                    addr: "east.internal:4460".to_owned(),
                },
            ),
            (
                voter_id("west"),
                PeryxNode {
                    datacenter: DatacenterId("west".to_owned()),
                    addr: "west.internal:4460".to_owned(),
                },
            ),
        ]),
    );
    assert!(plan.log_path.ends_with("raft/ownership-log.redb"));
    assert_eq!(plan.group, "ownership");
}

#[test]
fn test_ha_without_a_roster_builds_no_plan() {
    let dir = tempfile::tempdir().unwrap();
    let config = ha_config(&dir, None, Some("node-a"), SecretSource::Literal(TOKEN.to_owned()));

    // Ha without a roster keeps the metadata-replication-only posture and forms no consensus group.
    assert!(ConsensusPlan::from_config(&config).unwrap().is_none());
}

#[test]
fn test_ha_reads_the_shared_token_from_a_replica_role() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        data_dir: dir.path().to_path_buf(),
        writer_identity: Some("node-a".to_owned()),
        availability: AvailabilityConfig::Ha(ReplicationConfig::Replica {
            upstream: "http://east.internal:4460/".to_owned(),
            token: SecretSource::Literal(TOKEN.to_owned()),
            poll_interval: std::time::Duration::from_secs(1),
            page_size: std::num::NonZeroUsize::new(100).unwrap(),
        }),
        dc_membership: Some(seed_membership()),
        ..Config::default()
    };

    // A replica-role process in an ha group draws the same peer token as a primary-role one.
    assert!(ConsensusPlan::from_config(&config).unwrap().is_some());
}

#[test]
fn test_ha_without_a_writer_identity_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let config = ha_config(
        &dir,
        Some(seed_membership()),
        None,
        SecretSource::Literal(TOKEN.to_owned()),
    );

    let error = ConsensusPlan::from_config(&config).err().unwrap().to_string();

    assert!(error.contains("writer-identity"), "{error}");
}

#[test]
fn test_ha_with_a_foreign_identity_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let config = ha_config(
        &dir,
        Some(seed_membership()),
        Some("node-z"),
        SecretSource::Literal(TOKEN.to_owned()),
    );

    let error = ConsensusPlan::from_config(&config).err().unwrap().to_string();

    assert!(error.contains("not a member"), "{error}");
}

#[test]
fn test_ha_with_a_non_authority_address_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let membership = DcMembership {
        group: "ownership".to_owned(),
        members: vec![member(
            "node-a",
            "east",
            "http://east.internal:4460/raft",
            DcRole::Writer,
        )],
    };
    let config = ha_config(
        &dir,
        Some(membership),
        Some("node-a"),
        SecretSource::Literal(TOKEN.to_owned()),
    );

    let error = ConsensusPlan::from_config(&config).err().unwrap().to_string();

    assert!(error.contains("bare host:port"), "{error}");
}

#[test]
fn test_ha_with_an_unreadable_token_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let config = ha_config(
        &dir,
        Some(seed_membership()),
        Some("node-a"),
        SecretSource::File(dir.path().join("absent-token")),
    );

    let error = ConsensusPlan::from_config(&config).err().unwrap().to_string();

    assert!(error.contains("peer token"), "{error}");
}

#[test]
fn test_authority_extracts_host_and_port() {
    assert_eq!(authority("http://host.internal:4460").unwrap(), "host.internal:4460");
    assert_eq!(authority("https://host.internal:8443/").unwrap(), "host.internal:8443");
}

#[test]
fn test_authority_rejects_a_non_url() {
    assert!(authority("not a url").is_err());
}

#[test]
fn test_authority_rejects_a_missing_host() {
    assert!(authority("unix:/var/run/peryx.sock").is_err());
}

#[test]
fn test_authority_rejects_a_missing_port() {
    let error = authority("http://host.internal").err().unwrap().to_string();
    assert!(error.contains("explicit `host:port`"), "{error}");
}

#[test]
fn test_authority_rejects_a_path() {
    let error = authority("http://host.internal:4460/raft").err().unwrap().to_string();
    assert!(error.contains("bare host:port"), "{error}");
}

#[test]
fn test_build_roster_rejects_a_voter_id_collision() {
    // Two members sharing a datacenter hash onto one voter id; configuration forbids it, but the roster
    // builder rejects it directly rather than silently dropping a voter.
    let membership = DcMembership {
        group: "ownership".to_owned(),
        members: vec![
            member("node-a", "east", "http://a.internal:4460", DcRole::Writer),
            member("node-b", "east", "http://b.internal:4460", DcRole::Replica),
        ],
    };

    let error = build_roster(&membership).err().unwrap().to_string();

    assert!(error.contains("same consensus voter id"), "{error}");
}

#[test]
fn test_voter_id_is_stable_and_distinct() {
    assert_eq!(voter_id("east"), voter_id("east"));
    assert_ne!(voter_id("east"), voter_id("west"));
}

#[tokio::test]
async fn test_ignite_starts_and_bootstraps_a_single_node_group() {
    let dir = tempfile::tempdir().unwrap();
    let config = ha_config(
        &dir,
        Some(DcMembership {
            group: "ownership".to_owned(),
            members: vec![member("node-a", "east", "http://east.internal:4460", DcRole::Writer)],
        }),
        Some("node-a"),
        SecretSource::Literal(TOKEN.to_owned()),
    );
    let plan = ConsensusPlan::from_config(&config).unwrap().unwrap();

    let node = plan.ignite().await.unwrap();

    // The lone voter elects itself within an election window; poll its own leader view rather than
    // reach for openraft's wait helper, which this crate does not depend on directly.
    let mut leader = None;
    for _ in 0..50 {
        if let Some(found) = node.leader() {
            leader = Some(found);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(leader.map(|node| node.datacenter.0), Some("east".to_owned()));
    assert!(dir.path().join("raft/ownership-log.redb").exists());
}

#[tokio::test]
async fn test_ignite_fails_when_the_log_directory_cannot_be_created() {
    let dir = tempfile::tempdir().unwrap();
    // A file where the `raft` directory should be makes the directory creation fail.
    std::fs::create_dir_all(dir.path()).unwrap();
    std::fs::write(dir.path().join("raft"), b"not a directory").unwrap();
    let plan = plan_at(
        dir.path().join("raft/ownership-log.redb"),
        voter_id("east"),
        one_voter("east", "east.internal:4460"),
    );

    let error = plan.ignite().await.err().unwrap().to_string();

    assert!(error.contains("log directory"), "{error}");
}

#[tokio::test]
async fn test_ignite_fails_when_the_log_store_cannot_open() {
    let dir = tempfile::tempdir().unwrap();
    // A directory where the store file should be makes the redb open fail after the parent exists.
    std::fs::create_dir_all(dir.path().join("raft/ownership-log.redb")).unwrap();
    let plan = plan_at(
        dir.path().join("raft/ownership-log.redb"),
        voter_id("east"),
        one_voter("east", "east.internal:4460"),
    );

    let error = plan.ignite().await.err().unwrap().to_string();

    assert!(error.contains("log store"), "{error}");
}

#[tokio::test]
async fn test_ignite_fails_to_start_on_a_corrupt_store() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("raft/ownership-log.redb");
    std::fs::create_dir_all(log_path.parent().unwrap()).unwrap();
    // A vote the decoder cannot parse is a fatal storage error the node surfaces while starting.
    RaftLogStore::open(&log_path)
        .unwrap()
        .save_vote(b"not valid json")
        .unwrap();
    let plan = plan_at(log_path, voter_id("east"), one_voter("east", "east.internal:4460"));

    let error = plan.ignite().await.err().unwrap().to_string();

    assert!(error.contains("start the ownership consensus node"), "{error}");
}

#[tokio::test]
async fn test_ignite_fails_to_bootstrap_a_roster_without_the_local_node() {
    let dir = tempfile::tempdir().unwrap();
    // A local id absent from the roster is an inconsistent seed; bootstrap rejects it rather than
    // forming a group the node is not part of.
    let plan = plan_at(
        dir.path().join("raft/ownership-log.redb"),
        voter_id("east"),
        one_voter("west", "west.internal:4460"),
    );

    let error = plan.ignite().await.err().unwrap().to_string();

    assert!(error.contains("bootstrap the ownership consensus group"), "{error}");
}

async fn started_node(dir: &TempDir) -> RaftNode {
    let store = RaftLogStore::open(dir.path().join("raft.redb")).unwrap();
    RaftNode::start(
        1,
        RaftConfig::default(),
        "ownership",
        PeerRaftNetworkFactory::new(TOKEN, Duration::from_secs(1)),
        RaftLogStoreAdapter::new(store),
        OwnershipStateMachine::default(),
    )
    .await
    .unwrap()
}

async fn leader_node(dir: &TempDir) -> RaftNode {
    let node = started_node(dir).await;
    node.bootstrap(BTreeMap::from([(
        1,
        PeryxNode {
            datacenter: DatacenterId("east".to_owned()),
            addr: "east.internal:4460".to_owned(),
        },
    )]))
    .await
    .unwrap();
    for _ in 0..50 {
        if node.leader().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    node
}

#[tokio::test]
async fn test_claim_home_assigns_on_first_publish_then_reports_already_homed() {
    let dir = tempfile::tempdir().unwrap();
    let group = OwnershipGroup::new(leader_node(&dir).await, DatacenterId("east".to_owned()));

    assert_eq!(group.claim_home("proj").await.unwrap(), HomeClaim::AssignedHere);
    // The home persists in the group, so a repeat publish, or a race another datacenter won, reads as
    // already homed rather than reassigning.
    assert_eq!(group.claim_home("proj").await.unwrap(), HomeClaim::AlreadyHomed);
}

#[tokio::test]
async fn test_claim_home_without_a_leader_reports_not_leader() {
    let dir = tempfile::tempdir().unwrap();
    // An unbootstrapped node has no leader, so the claim cannot commit here and names no forward target.
    let group = OwnershipGroup::new(started_node(&dir).await, DatacenterId("east".to_owned()));

    assert!(matches!(
        group.claim_home("proj").await,
        Err(OwnershipError::NotLeader { leader: None })
    ));
}

#[tokio::test]
async fn test_claim_home_on_a_stopped_group_is_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let node = leader_node(&dir).await;
    node.raft().shutdown().await.unwrap();
    let group = OwnershipGroup::new(node, DatacenterId("east".to_owned()));

    assert!(matches!(
        group.claim_home("proj").await,
        Err(OwnershipError::Unavailable(_))
    ));
}

#[tokio::test]
async fn test_has_home_reflects_a_committed_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let group = OwnershipGroup::new(leader_node(&dir).await, DatacenterId("east".to_owned()));

    assert!(!group.has_home("proj").await);
    group.claim_home("proj").await.unwrap();
    // client_write returns after the entry applies, so the home reads back locally at once.
    assert!(group.has_home("proj").await);
}

#[tokio::test]
async fn test_cluster_status_reports_the_leader_and_voter_membership() {
    let dir = tempfile::tempdir().unwrap();
    let group = OwnershipGroup::new(leader_node(&dir).await, DatacenterId("east".to_owned()));

    let status = group.cluster_status();

    assert_eq!(status.leader, Some("east".to_owned()));
    assert!(status.term >= 1, "an elected leader holds a nonzero term");
    assert_eq!(status.voters, vec!["east".to_owned()]);
}

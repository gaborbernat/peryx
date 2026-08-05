//! A live-process proof that a three-node `ha` cluster forms.
//!
//! Three real `peryx serve` binaries run the embedded ownership Raft node, exchange peer RPCs over the
//! mounted receive router, reach quorum, elect a leader, and report it on the availability status
//! resource ([#540]). Without the mounted router a voter answers no RPCs and this never reaches a leader.
//!
//! Gated behind the `availability-e2e` feature so the default `cargo test` and the coverage gate skip it;
//! it spawns real binaries and drives them only over HTTP.
//!
//! [#540]: https://github.com/tox-dev/peryx/issues/540

mod harness;

use std::time::Duration;

use harness::{ADMIN_PASSWORD, ADMIN_USER, MemberSpec, Role, Topology};

#[test]
fn test_a_three_node_ha_cluster_forms_and_reports_its_leader() {
    let cluster = Topology::ha(
        "ownership",
        vec![
            MemberSpec::new("node-a", "east", Role::Writer),
            MemberSpec::new("node-b", "west", Role::Replica),
            MemberSpec::new("node-c", "south", Role::Replica),
        ],
    )
    .with_admin()
    .start()
    .expect("the three-node ha cluster starts");

    // A leader emerges only once every node serves the inbound raft RPC router; poll one node's
    // consensus status until it names a leader.
    let node = &cluster.nodes()[0];
    let mut consensus = None;
    for _ in 0..150 {
        if let Some((200, body)) = node.control_get_as(ADMIN_USER, ADMIN_PASSWORD, "/availability/v1/status") {
            let status: serde_json::Value = serde_json::from_str(&body).expect("the status body is JSON");
            if let Some(block) = status.get("consensus")
                && block.get("leader").and_then(serde_json::Value::as_str).is_some()
            {
                consensus = Some(block.clone());
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let consensus = consensus.expect("the ha group elects a leader within the deadline");
    let voters = consensus["voters"].as_array().expect("voters is an array").len();
    assert_eq!(
        voters, 3,
        "the committed membership holds all three voters: {consensus}"
    );
    let leader = consensus["leader"].as_str().expect("a leader datacenter");
    assert!(
        ["east", "west", "south"].contains(&leader),
        "the leader is a group member: {leader}"
    );
}

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

use std::collections::HashMap;
use std::time::{Duration, Instant};

use harness::{ADMIN_PASSWORD, ADMIN_USER, Cluster, MemberSpec, Role, Topology};
use serde_json::Value;

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

    let consensus = await_quorum_leader(&cluster);
    let leader = consensus["leader"].as_str().expect("a quorum-agreed leader datacenter");
    assert!(
        ["east", "west", "south"].contains(&leader),
        "the leader is a group member: {leader}"
    );
    let voters = consensus["voters"].as_array().expect("voters is an array").len();
    assert_eq!(
        voters, 3,
        "the committed membership holds all three voters: {consensus}"
    );
}

/// Wait until a majority of the group agrees on the same leader with all three voters committed, then
/// return that consensus block.
///
/// This polls every node, not one. The seed stands for election alone through the startup window before
/// its peers answer RPCs, inflating its term, and each node's raft metrics lag the group by a heartbeat,
/// so one node reporting no leader does not mean the group has none. Agreement across a quorum is the
/// true "formed" signal and does not flap on a transient single-node view. The budget is generous
/// because a saturated CI runner starves the three real processes' async schedulers, not because the
/// election itself is slow; a formed group converges here on the first poll.
fn await_quorum_leader(cluster: &Cluster) -> Value {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(block) = quorum_leader(cluster) {
            return block;
        }
        assert!(
            Instant::now() < deadline,
            "the ha group did not agree on a leader within the deadline:\n{}",
            cluster.failure_report().render(),
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The consensus block a majority of nodes agree on: the same non-null leader named by a committed
/// membership of three voters. `None` until at least two nodes concur, so a single node's transient or
/// lagging view never trips the wait.
fn quorum_leader(cluster: &Cluster) -> Option<Value> {
    let mut agreed: HashMap<String, (usize, Value)> = HashMap::new();
    for node in cluster.nodes() {
        let Some((200, body)) = node.control_get_as(ADMIN_USER, ADMIN_PASSWORD, "/availability/v1/status") else {
            continue;
        };
        let Ok(status) = serde_json::from_str::<Value>(&body) else {
            continue;
        };
        let Some(block) = status.get("consensus") else {
            continue;
        };
        let Some(leader) = block.get("leader").and_then(Value::as_str) else {
            continue;
        };
        if block
            .get("voters")
            .and_then(Value::as_array)
            .is_some_and(|voters| voters.len() == 3)
        {
            let entry = agreed.entry(leader.to_owned()).or_insert_with(|| (0, block.clone()));
            entry.0 += 1;
        }
    }
    agreed
        .into_values()
        .find(|(count, _)| *count >= 2)
        .map(|(_, block)| block)
}

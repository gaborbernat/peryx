//! Self-tests for the multi-process availability harness: proof that it spawns real peryx binaries,
//! observes them, injects and heals network faults through Toxiproxy, and leaves nothing running.
//!
//! Gated behind the `availability-e2e` feature so the default `cargo test` and the coverage gate skip
//! them: they spawn processes and need the `toxiproxy-server` binary. CI runs them in a dedicated job.

#![cfg(feature = "availability-e2e")]

mod harness;

use std::time::{Duration, Instant};

use harness::{HarnessError, MemberSpec, Node, OwnershipControl, Role, Topology, Toxiproxy};

#[test]
fn test_spawns_a_node_and_reports_ready() {
    let cluster = Topology::single().start().expect("cluster starts");
    let node = &cluster.nodes()[0];
    assert!(node.is_ready(), "node should answer /+status");
    let (code, _) = node.readiness().expect("readiness reachable");
    assert!(code == 200 || code == 503, "unexpected readiness code {code}");
}

#[test]
fn test_observes_a_node_over_arbitrary_http() {
    // The general http_get accessor (and the metrics convenience on top of it) lets a test reach any
    // read endpoint the harness has not named, which the observability test tiers build on.
    let cluster = Topology::single().start().expect("cluster starts");
    let node = &cluster.nodes()[0];
    let (code, body) = node.metrics().expect("metrics reachable");
    assert_eq!(code, 200);
    assert!(!body.is_empty(), "metrics body is empty");
    assert!(
        node.http_get("/+status").is_some(),
        "arbitrary http_get reaches the node"
    );
}

#[test]
fn test_detects_a_child_crash() {
    let mut cluster = Topology::single().start().expect("cluster starts");
    let node = &mut cluster.nodes_mut()[0];
    assert!(node.is_running());

    node.kill();

    assert!(!node.is_running(), "harness should observe the killed child has exited");
    assert!(node.status().is_none(), "a dead node answers no HTTP");
}

#[test]
fn test_detects_a_port_collision() {
    // Hold a port so the node cannot bind it. The harness must surface the failed startup rather than
    // attach to a foreign listener already on that port.
    let held = std::net::TcpListener::bind("127.0.0.1:0").expect("hold a port");
    let taken = held.local_addr().unwrap().port();

    let error = harness::spawn_on_port("collider", taken).expect_err("a taken port must fail startup");

    assert!(
        matches!(error, HarnessError::ExitedEarly { .. } | HarnessError::NotReady { .. }),
        "{error}"
    );
}

#[test]
fn test_reports_a_readiness_failure_with_diagnostics() {
    // A config naming an upload token with no secret is rejected at startup, so the node exits and the
    // harness surfaces the log rather than hanging.
    let error = harness::spawn_with_config(
        "broken",
        "[[index]]\nname = \"hosted\"\nhosted = true\n[[index.access_token]]\nname = \"t\"\nactions = [\"write\"]\n",
    )
    .expect_err("an invalid config must fail startup");

    let rendered = error.to_string();
    assert!(
        rendered.contains("log tail"),
        "diagnostics should include the log: {rendered}"
    );
}

#[test]
fn test_leaves_no_leaked_process() {
    let pid = {
        let mut cluster = Topology::single().start().expect("cluster starts");
        cluster.nodes_mut()[0].pid()
    };
    // The cluster dropped at the block's end; its Drop killed the process group.
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !harness::process_alive(pid),
        "dropping the cluster must leave no peryx process (pid {pid})"
    );
}

#[test]
fn test_toxiproxy_partitions_and_heals_a_link() {
    let cluster = Topology::single().start().expect("cluster starts");
    let node = &cluster.nodes()[0];
    let mut toxiproxy = Toxiproxy::start().expect("toxiproxy starts");
    let proxy = toxiproxy
        .proxy(&format!("127.0.0.1:{}", node.port()))
        .expect("create proxy");

    assert!(
        harness::reachable_through(proxy.endpoint()),
        "a healthy proxy forwards to the node"
    );

    proxy.partition().expect("partition the link");
    assert!(
        !harness::reachable_through(proxy.endpoint()),
        "a partitioned proxy drops the connection"
    );

    proxy.heal().expect("heal the link");
    assert!(
        harness::reachable_through(proxy.endpoint()),
        "a healed proxy forwards again"
    );
}

#[test]
fn test_detects_a_proxy_failure() {
    let mut toxiproxy = Toxiproxy::start().expect("toxiproxy starts");
    assert!(toxiproxy.control_is_up());

    toxiproxy.kill();

    assert!(
        !toxiproxy.control_is_up(),
        "harness should detect the dead proxy server"
    );
}

#[test]
fn test_captures_a_failure_artifact() {
    let cluster = Topology::single().start().expect("cluster starts");
    let report = cluster.failure_report();
    let rendered = report.render();

    assert_eq!(report.nodes.len(), 1);
    assert!(rendered.contains("== node node-a =="), "{rendered}");
    assert!(
        rendered.contains("status:"),
        "artifact should include status: {rendered}"
    );
    assert!(rendered.contains("log:"), "artifact should include the log: {rendered}");
}

#[test]
fn test_validates_a_generated_ha_topology_config() {
    // The embedded ownership node ([#498]) runs but a cluster cannot form yet — the inbound peer-RPC
    // router is unmounted, so bootstrap never reaches quorum. The reachable assertion is that the
    // topology builder generates config peryx accepts: a writer, a replica, the group, and the roster.
    let output = Topology::ha(
        "ownership",
        vec![
            MemberSpec::new("node-a", "east", Role::Writer),
            MemberSpec::new("node-b", "west", Role::Replica),
        ],
    )
    .validate_config()
    .expect("peryx accepts the generated ha config");
    assert!(output.contains("configuration is valid"), "{output}");
    assert!(output.contains("availability: ha"), "{output}");
    assert!(output.contains("2 members"), "{output}");
}

#[test]
fn test_validates_a_generated_dc_topology_config() {
    let output = Topology::dc(
        "region",
        vec![
            MemberSpec::new("primary", "east", Role::Writer),
            MemberSpec::new("replica", "west", Role::Replica),
        ],
    )
    .validate_config()
    .expect("peryx accepts the generated dc config");
    assert!(output.contains("configuration is valid"), "{output}");
}

#[test]
fn test_ownership_controls_are_unavailable_until_the_write_api_lands() {
    let cluster = Topology::single().start().expect("cluster starts");
    assert!(matches!(cluster.leader(), Err(HarnessError::Unsupported(_))));
    assert!(matches!(
        cluster.submit_ownership_write("x"),
        Err(HarnessError::Unsupported(_))
    ));
    assert!(matches!(
        cluster.await_authority_transfer("node-a", Duration::from_secs(1)),
        Err(HarnessError::Unsupported(_))
    ));
}

#[test]
fn test_dc_replica_metrics_exposes_the_replication_series() {
    // A real replica in a dc group follows the writer, so its scrape carries the replication series a
    // `none` node never exports. The harness bootstraps the replica's writer identity offline, which is
    // what lets a replica start read-only at all.
    let cluster = Topology::dc(
        "east",
        vec![
            MemberSpec::new("writer-a", "east-1", Role::Writer),
            MemberSpec::new("replica-b", "east-2", Role::Replica),
        ],
    )
    .start()
    .expect("dc cluster starts");
    let replica = cluster.node("replica-b").expect("the replica is present");

    let (code, body) = replica.metrics().expect("metrics reachable");

    assert_eq!(code, 200);
    assert!(
        body.contains("peryx_replication_"),
        "a dc replica exports the replication series: {body}"
    );
}

#[test]
fn test_ha_replica_metrics_exposes_the_replication_series() {
    let cluster = Topology::ha(
        "global",
        vec![
            MemberSpec::new("writer-east", "east", Role::Writer),
            MemberSpec::new("replica-west", "west", Role::Replica),
        ],
    )
    .start()
    .expect("ha cluster starts");
    let replica = cluster.node("replica-west").expect("the replica is present");

    let (code, body) = replica.metrics().expect("metrics reachable");

    assert_eq!(code, 200);
    assert!(
        body.contains("peryx_replication_"),
        "an ha replica exports the replication series: {body}"
    );
}

fn metric(node: &Node, series: &str) -> u64 {
    let (_, body) = node.metrics().expect("metrics reachable");
    body.lines()
        .find_map(|line| line.strip_prefix(series).and_then(|rest| rest.trim().parse().ok()))
        .unwrap_or(0)
}

/// Poll until the replica records a metadata transport error, asserting it keeps serving read-only the
/// whole time, or fail after a generous deadline. Deterministic in outcome: it waits for the transition
/// rather than sleeping a fixed span and hoping.
fn await_sync_error(node: &Node) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        assert!(
            node.is_ready(),
            "the replica keeps serving read-only through the metadata outage"
        );
        if metric(node, "peryx_replication_sync_errors_total ") > 0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("the replica never recorded a metadata transport loss after the writer died");
}

fn await_caught_up(node: &Node) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if metric(node, "peryx_replication_caught_up ") == 1 {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("the replica did not catch up: {:?}", node.metrics());
}

#[test]
fn test_replica_recovers_metadata_after_a_writer_disconnect() {
    // A real dc replica follows the writer over the bounded metadata transport. Killing the writer
    // disconnects the metadata link: the replica keeps serving read-only and its transport records the
    // loss (disconnect during a batch). Restarting the writer heals the link and the replica catches up
    // again, its durable frontier keeping the re-poll from replaying an already-applied change (response
    // loss after apply).
    let mut cluster = Topology::dc(
        "east",
        vec![
            MemberSpec::new("writer-a", "east-1", Role::Writer),
            MemberSpec::new("replica-b", "east-2", Role::Replica),
        ],
    )
    .start()
    .expect("dc cluster starts");
    let writer = cluster
        .nodes()
        .iter()
        .position(|node| node.identity() == "writer-a")
        .unwrap();
    let replica = cluster
        .nodes()
        .iter()
        .position(|node| node.identity() == "replica-b")
        .unwrap();

    await_caught_up(&cluster.nodes()[replica]);

    cluster.nodes_mut()[writer].kill();
    await_sync_error(&cluster.nodes()[replica]);

    cluster.nodes_mut()[writer]
        .restart()
        .expect("the writer restarts on its port");
    await_caught_up(&cluster.nodes()[replica]);
}

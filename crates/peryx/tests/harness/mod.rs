//! A multi-process availability test harness: spawn real `peryx serve` binaries with isolated stores
//! and a generated datacenter roster, observe them over their public HTTP surface, inject network
//! faults through Toxiproxy, and tear the whole group down without leaking a process.
//!
//! The harness drives production binaries and public APIs only. It never links a peryx crate to reach
//! into private state, so a test asserts through `/+status`, `/+ready`, and `/+availability/topology`
//! the way an operator would. Every spawned process (peryx nodes and `toxiproxy-server`) runs in its own
//! process group and is killed on [`Drop`], so a panicking test leaks nothing.
//!
//! The ownership consensus plane is only partly reachable today: the embedded Raft node ([#498]) runs,
//! but no write or authority endpoint is exposed over HTTP yet, and a multi-node group cannot form
//! because the inbound peer-RPC router is not mounted. So [`OwnershipControl`] is defined but its methods
//! return [`HarnessError::Unsupported`]; the failover test tier fills them once [#540] lands.

#![allow(
    dead_code,
    unused_imports,
    reason = "a reusable harness exposes surface the self-tests do not each exercise"
)]

pub mod toxiproxy;

use std::fmt::Write as _;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use tempfile::TempDir;

pub use toxiproxy::{Proxy, Toxiproxy};

const BIN: &str = env!("CARGO_BIN_EXE_peryx");
const READY_TIMEOUT: Duration = Duration::from_secs(20);
const READY_POLL: Duration = Duration::from_millis(25);
const HTTP_TIMEOUT: Duration = Duration::from_secs(2);

/// A harness failure, distinct from a node's own error, so a self-test can assert why the harness gave
/// up rather than only that it did.
#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[error("node {node:?} did not become ready within {timeout:?}\n--- log tail ---\n{log}")]
    NotReady {
        node: String,
        timeout: Duration,
        log: String,
    },
    #[error("node {node:?} exited during startup with {status}\n--- log tail ---\n{log}")]
    ExitedEarly { node: String, status: String, log: String },
    #[error("toxiproxy: {0}")]
    Toxiproxy(String),
    #[error("peryx rejected the generated config:\n{0}")]
    Config(String),
    #[error("this control is not available yet: {0}")]
    Unsupported(&'static str),
}

/// The availability mode a node runs in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Single node, no replication: the zero-config default.
    None,
    /// A writer and read replicas within one datacenter.
    Dc,
    /// Metadata durability across datacenters, running the embedded ownership Raft node.
    Ha,
}

impl Mode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Dc => "dc",
            Self::Ha => "ha",
        }
    }
}

/// The role a member plays in a `dc` or `ha` group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Writer,
    Replica,
}

impl Role {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Writer => "writer",
            Self::Replica => "replica",
        }
    }
}

/// One member of a topology, before ports are assigned.
#[derive(Debug, Clone)]
pub struct MemberSpec {
    pub node: String,
    pub dc: String,
    pub role: Role,
}

impl MemberSpec {
    #[must_use]
    pub fn new(node: &str, dc: &str, role: Role) -> Self {
        Self {
            node: node.to_owned(),
            dc: dc.to_owned(),
            role,
        }
    }
}

/// A cluster blueprint: the mode, group name, shared peer token, and the member roster.
#[derive(Debug, Clone)]
pub struct Topology {
    mode: Mode,
    group: String,
    token: String,
    members: Vec<MemberSpec>,
}

impl Topology {
    /// A single stand-alone `none`-mode node, the simplest thing the harness can run.
    #[must_use]
    pub fn single() -> Self {
        Self {
            mode: Mode::None,
            group: "solo".to_owned(),
            token: "harness-token".to_owned(),
            members: vec![MemberSpec::new("node-a", "local", Role::Writer)],
        }
    }

    /// An `ha` group over the given members, running the embedded ownership Raft node on each.
    #[must_use]
    pub fn ha(group: &str, members: Vec<MemberSpec>) -> Self {
        Self {
            mode: Mode::Ha,
            group: group.to_owned(),
            token: "harness-token".to_owned(),
            members,
        }
    }

    /// A `dc` group over the given members: one writer and its read replicas within a datacenter.
    #[must_use]
    pub fn dc(group: &str, members: Vec<MemberSpec>) -> Self {
        Self {
            mode: Mode::Dc,
            group: group.to_owned(),
            token: "harness-token".to_owned(),
            members,
        }
    }

    /// Spawn every member and wait until each answers `/+status`.
    ///
    /// # Errors
    /// Returns the first [`HarnessError`] a node reports while coming up.
    pub fn start(&self) -> Result<Cluster, HarnessError> {
        let addresses: Vec<(u16, u16)> = self.members.iter().map(|_| (free_port(), free_port())).collect();
        let roster = self.roster_toml(&addresses);
        let mut nodes = Vec::with_capacity(self.members.len());
        for (member, &(public, control)) in self.members.iter().zip(&addresses) {
            let node = Node::spawn(self, member, public, control, &roster)?;
            nodes.push(node);
        }
        Ok(Cluster { nodes })
    }

    /// Validate the generated config for the first member through `peryx config check`, without spawning
    /// a server or forming a cluster. This proves the topology produces configuration peryx accepts,
    /// which is the reachable assertion while the ownership consensus plane is only partly wired.
    ///
    /// # Errors
    /// [`HarnessError::Config`] with the validator's output when peryx rejects the generated config.
    pub fn validate_config(&self) -> Result<String, HarnessError> {
        let addresses: Vec<(u16, u16)> = self
            .members
            .iter()
            .enumerate()
            .map(|(index, _)| {
                (
                    9000 + 2 * u16::try_from(index).unwrap_or(0),
                    9001 + 2 * u16::try_from(index).unwrap_or(0),
                )
            })
            .collect();
        let roster = self.roster_toml(&addresses);
        let member = self.members.first().expect("a topology has at least one member");
        let dir = TempDir::new().expect("temp dir");
        let config = dir.path().join("peryx.toml");
        std::fs::write(&config, node_config(self, member, addresses[0].1, &roster)).expect("write config");
        let output = Command::new(BIN)
            .args(["config", "check"])
            .arg("--config")
            .arg(&config)
            .arg("--data-dir")
            .arg(dir.path())
            .output()
            .expect("run peryx config check");
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(HarnessError::Config(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }

    /// The `[[availability.member]]` roster block shared by every node, mapping each member to its peer
    /// control port. Empty for a `none`-mode topology.
    fn roster_toml(&self, addresses: &[(u16, u16)]) -> String {
        if self.mode == Mode::None {
            return String::new();
        }
        let mut toml = format!(
            "[availability]\nmode = \"{}\"\ngroup = \"{}\"\n\n\
             [availability.replication]\nrole = \"primary\"\nsource = \"{}\"\ntoken = \"{}\"\n\n",
            self.mode.as_str(),
            self.group,
            self.group,
            self.token,
        );
        for (member, &(_, control)) in self.members.iter().zip(addresses) {
            let _ = write!(
                toml,
                "[[availability.member]]\nnode = \"{}\"\ndc = \"{}\"\naddress = \"http://127.0.0.1:{control}\"\nrole = \"{}\"\n\n",
                member.node,
                member.dc,
                member.role.as_str(),
            );
        }
        toml
    }
}

/// A spawned cluster. Dropping it kills every node's process group and removes its data directory.
pub struct Cluster {
    nodes: Vec<Node>,
}

impl Cluster {
    /// The cluster's nodes, in roster order.
    #[must_use]
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// The cluster's nodes for mutation (kill, restart, wait).
    pub fn nodes_mut(&mut self) -> &mut [Node] {
        &mut self.nodes
    }

    /// A node by its configured identity.
    #[must_use]
    pub fn node(&self, identity: &str) -> Option<&Node> {
        self.nodes.iter().find(|node| node.identity == identity)
    }

    /// A failure artifact for every node: topology, process status, recent log, and pending operations.
    #[must_use]
    pub fn failure_report(&self) -> FailureReport {
        FailureReport {
            nodes: self.nodes.iter().map(Node::snapshot).collect(),
        }
    }
}

impl OwnershipControl for Cluster {}

/// One running `peryx serve` process and the surface a test drives it through.
#[derive(Debug)]
pub struct Node {
    identity: String,
    child: Child,
    port: u16,
    control_port: u16,
    config: PathBuf,
    data: TempDir,
    http: reqwest::blocking::Client,
}

impl Node {
    fn spawn(
        topology: &Topology,
        member: &MemberSpec,
        port: u16,
        control_port: u16,
        roster: &str,
    ) -> Result<Self, HarnessError> {
        let data = TempDir::new().expect("temp data dir");
        let config = data.path().join("peryx.toml");
        std::fs::write(&config, node_config(topology, member, control_port, roster)).expect("write config");
        let http = reqwest::blocking::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .expect("build http client");
        let mut node = Self {
            identity: member.node.clone(),
            child: launch(&config, data.path(), port),
            port,
            control_port,
            config,
            data,
            http,
        };
        node.await_ready()?;
        Ok(node)
    }

    /// Poll `/+status` until this node answers, exits, or the deadline passes.
    ///
    /// # Errors
    /// [`HarnessError::ExitedEarly`] when the child dies first, [`HarnessError::NotReady`] on timeout.
    pub fn await_ready(&mut self) -> Result<(), HarnessError> {
        let deadline = Instant::now() + READY_TIMEOUT;
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait().expect("child status") {
                return Err(HarnessError::ExitedEarly {
                    node: self.identity.clone(),
                    status: status.to_string(),
                    log: self.log_tail(),
                });
            }
            if self.is_ready() {
                return Ok(());
            }
            std::thread::sleep(READY_POLL);
        }
        Err(HarnessError::NotReady {
            node: self.identity.clone(),
            timeout: READY_TIMEOUT,
            log: self.log_tail(),
        })
    }

    /// Spawn a bare `none`-mode node with an explicit port and raw config, for harness self-tests that
    /// force a port collision or an invalid configuration.
    fn start_raw(identity: &str, port: u16, config_toml: String) -> Result<Self, HarnessError> {
        let data = TempDir::new().expect("temp data dir");
        let config = data.path().join("peryx.toml");
        std::fs::write(&config, config_toml).expect("write config");
        let http = reqwest::blocking::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .expect("build http client");
        let mut node = Self {
            identity: identity.to_owned(),
            child: launch(&config, data.path(), port),
            port,
            control_port: 0,
            config,
            data,
            http,
        };
        node.await_ready()?;
        Ok(node)
    }

    /// The node's public HTTP port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// The node process's pid, for a leaked-process assertion.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// The `host:port` peers dial for this node's control plane, the target a Toxiproxy proxy fronts.
    #[must_use]
    pub fn control_endpoint(&self) -> String {
        format!("127.0.0.1:{}", self.control_port)
    }

    /// The node's configured identity.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Whether the process is still running (has not exited).
    #[must_use]
    pub fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Whether `/+status` answers `200` with a peryx body.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.status()
            .is_some_and(|(code, body)| code == 200 && body.contains("\"version\""))
    }

    /// `GET /+status`, or `None` when the node is unreachable.
    #[must_use]
    pub fn status(&self) -> Option<(u16, String)> {
        self.http_get("/+status")
    }

    /// `GET /+ready`, or `None` when the node is unreachable.
    #[must_use]
    pub fn readiness(&self) -> Option<(u16, String)> {
        self.http_get("/+ready")
    }

    /// `GET /+availability/topology`, or `None` when the node is unreachable or serves no topology.
    #[must_use]
    pub fn topology(&self) -> Option<(u16, String)> {
        self.http_get("/+availability/topology")
    }

    /// `GET /metrics`, the Prometheus exposition, or `None` when the node is unreachable.
    #[must_use]
    pub fn metrics(&self) -> Option<(u16, String)> {
        self.http_get("/metrics")
    }

    /// `GET /+availability/placements`, the artifact placement view, or `None` when the node is
    /// unreachable or serves no placement view.
    #[must_use]
    pub fn placements(&self) -> Option<(u16, String)> {
        self.http_get("/+availability/placements")
    }

    /// `GET {path}` against the node's public port, returning the status code and body, or `None` when
    /// the node is unreachable. This is the general accessor the typed observations build on, so a test
    /// can reach any read endpoint without the harness naming it first.
    #[must_use]
    pub fn http_get(&self, path: &str) -> Option<(u16, String)> {
        let response = self
            .http
            .get(format!("http://127.0.0.1:{}{path}", self.port))
            .send()
            .ok()?;
        let code = response.status().as_u16();
        Some((code, response.text().unwrap_or_default()))
    }

    /// Kill the node's process group, so a test can drive a crash or a partition-by-death.
    pub fn kill(&mut self) {
        kill_group(&mut self.child);
    }

    /// Kill and re-spawn the node against the same data directory and port, then wait until it is ready.
    ///
    /// # Errors
    /// The [`HarnessError`] the fresh process reports while coming up.
    pub fn restart(&mut self) -> Result<(), HarnessError> {
        self.kill();
        self.child = launch(&self.config, self.data.path(), self.port);
        self.await_ready()
    }

    /// The last of the node's own log, for failure diagnostics.
    #[must_use]
    pub fn log_tail(&self) -> String {
        let log = std::fs::read_to_string(self.data.path().join("peryx.log")).unwrap_or_default();
        log.lines()
            .rev()
            .take(40)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn snapshot(&self) -> NodeArtifact {
        NodeArtifact {
            identity: self.identity.clone(),
            topology: self.topology().map(|(_, body)| body),
            status: self.status().map(|(_, body)| body),
            log_tail: self.log_tail(),
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        kill_group(&mut self.child);
    }
}

/// A diagnostic bundle for a failed test: one entry per node.
#[derive(Debug)]
pub struct FailureReport {
    pub nodes: Vec<NodeArtifact>,
}

impl FailureReport {
    /// Render the report as text a failing assertion can print.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        for node in &self.nodes {
            let _ = write!(
                out,
                "== node {} ==\ntopology: {}\nstatus: {}\nlog:\n{}\n\n",
                node.identity,
                node.topology.as_deref().unwrap_or("<unreachable>"),
                node.status.as_deref().unwrap_or("<unreachable>"),
                node.log_tail,
            );
        }
        out
    }
}

/// One node's slice of a [`FailureReport`]: its topology and status (the pending-operations surface),
/// plus the tail of its log.
#[derive(Debug)]
pub struct NodeArtifact {
    pub identity: String,
    pub topology: Option<String>,
    pub status: Option<String>,
    pub log_tail: String,
}

/// The ownership-plane controls the failover test tier will use once the write and authority endpoints
/// exist. Every method fails with [`HarnessError::Unsupported`] today: the embedded Raft node runs but
/// exposes nothing to drive or observe over HTTP, so the harness will not fake a result.
pub trait OwnershipControl {
    /// Submit an ownership command to the current leader.
    ///
    /// # Errors
    /// [`HarnessError::Unsupported`] until an ownership write endpoint exists (#540).
    fn submit_ownership_write(&self, _command: &str) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported("ownership write endpoint is blocked on #540"))
    }

    /// The identity of the node currently holding ownership authority.
    ///
    /// # Errors
    /// [`HarnessError::Unsupported`] until authority is exposed over HTTP (#540).
    fn leader(&self) -> Result<Option<String>, HarnessError> {
        Err(HarnessError::Unsupported(
            "leader/authority is not exposed over HTTP yet (#540)",
        ))
    }

    /// Wait until authority leaves `from` within `within`, returning the new holder.
    ///
    /// # Errors
    /// [`HarnessError::Unsupported`] until authority transfer is observable (#540).
    fn await_authority_transfer(&self, _from: &str, _within: Duration) -> Result<String, HarnessError> {
        Err(HarnessError::Unsupported(
            "authority-transfer observation is blocked on #540",
        ))
    }
}

/// Generate one node's full config: a minimal hosted index every node serves, plus the availability and
/// roster blocks for a `dc` or `ha` member.
fn node_config(topology: &Topology, member: &MemberSpec, control_port: u16, roster: &str) -> String {
    // Top-level keys must precede any table, or TOML folds them into the last `[[index]]`.
    let mut config = String::new();
    if topology.mode != Mode::None {
        let _ = writeln!(config, "writer_identity = \"{}\"\n", member.node);
    }
    config.push_str("[[index]]\nname = \"hosted\"\nhosted = true\n\n");
    if topology.mode != Mode::None {
        config.push_str(roster);
        let _ = writeln!(config, "[availability.listener]\nbind = \"127.0.0.1:{control_port}\"");
    }
    config
}

fn launch(config: &std::path::Path, data: &std::path::Path, port: u16) -> Child {
    let log = std::fs::File::create(data.join("peryx.log")).expect("create node log");
    let mut command = Command::new(BIN);
    command
        .arg("serve")
        .args(["--host", "127.0.0.1"])
        .args(["--port", &port.to_string()])
        .arg("--data-dir")
        .arg(data)
        .arg("--config")
        .arg(config)
        .args(["--log-level", "debug"])
        .stdout(log.try_clone().expect("clone log handle"))
        .stderr(log);
    spawn_in_group(&mut command);
    command.spawn().expect("spawn peryx")
}

/// Put a child in its own process group so the harness can signal the whole group, not just the leader.
fn spawn_in_group(command: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        command.process_group(0);
    }
    let _ = command;
}

/// Kill a child's entire process group and reap it, so no descendant leaks.
fn kill_group(child: &mut Child) {
    #[cfg(unix)]
    {
        // The child leads its own group (spawned by `spawn_in_group`), so its pid names the group.
        let group = nix::unistd::Pid::from_raw(i32::try_from(child.id()).expect("pid fits an i32"));
        let _ = nix::sys::signal::killpg(group, nix::sys::signal::Signal::SIGKILL);
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// Grab a free loopback port by binding `:0` and releasing it. A spawned process re-binds it a moment
/// later; each node uses a distinct port so parallel runs stay separate.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral")
        .local_addr()
        .expect("local addr")
        .port()
}

/// Spawn a stand-alone node forced onto `port`, so a self-test can prove the harness detects a port
/// collision instead of hanging or attaching to a foreign server.
///
/// # Errors
/// The [`HarnessError`] the losing process reports while failing to come up.
pub fn spawn_on_port(identity: &str, port: u16) -> Result<Node, HarnessError> {
    Node::start_raw(
        identity,
        port,
        "[[index]]\nname = \"hosted\"\nhosted = true\n".to_owned(),
    )
}

/// Spawn a stand-alone node from a raw config, so a self-test can drive a startup failure.
///
/// # Errors
/// The [`HarnessError`] the process reports while failing to come up.
pub fn spawn_with_config(identity: &str, config_toml: &str) -> Result<Node, HarnessError> {
    Node::start_raw(identity, free_port(), config_toml.to_owned())
}

/// Whether a process with `pid` still exists, for a leaked-process assertion.
#[must_use]
pub fn process_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // Signalling with `None` performs the existence and permission check without delivering one.
        let pid = nix::unistd::Pid::from_raw(i32::try_from(pid).expect("pid fits an i32"));
        nix::sys::signal::kill(pid, None).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

/// Whether a peryx node answers `/+status` through a `host:port` endpoint (a Toxiproxy listen address).
#[must_use]
pub fn reachable_through(endpoint: &str) -> bool {
    reqwest::blocking::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .expect("build http client")
        .get(format!("http://{endpoint}/+status"))
        .send()
        .is_ok_and(|response| response.status().is_success())
}

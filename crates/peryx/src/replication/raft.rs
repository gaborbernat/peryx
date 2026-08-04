//! Stand up and hold the ownership consensus node an `ha` process runs.
//!
//! A single-datacenter `dc` group and single-node `none` run no consensus, so only [`AvailabilityMode::Ha`]
//! builds anything here. [`ConsensusPlan::from_config`] resolves the roster synchronously — validating
//! every member address and deriving each voter's stable id before any runtime starts — so a bad topology
//! fails config assembly rather than a live node. [`ConsensusPlan::ignite`] then opens the durable log
//! store and assembles the [`RaftNode`], seeding a fresh group with an idempotent bootstrap so a restart
//! rejoins the existing one.
//!
//! [`AvailabilityMode::Ha`]: crate::config::AvailabilityMode::Ha

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context as _, bail};
use peryx_replication::DatacenterId;
use peryx_replication::raft::log_store::RaftLogStoreAdapter;
use peryx_replication::raft::network::PeerRaftNetworkFactory;
use peryx_replication::raft::{OwnershipStateMachine, PeryxNode, RaftConfig, RaftNode};
use peryx_storage::raft::RaftLogStore;
use url::Url;

use crate::config::{AvailabilityConfig, Config, DcMembership, ReplicationConfig};

/// The `u64` voter handle `OpenRaft` keys a node by. Derived from the datacenter identity so every node
/// computes the same id for each voter from the shared roster, without a coordination round or a
/// persisted assignment that could drift from the roster.
type VoterId = u64;

/// How long a single peer Raft RPC may run before it is a retryable loss.
const PEER_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Where the ownership log store lives under the data directory, kept stable across restarts so a node
/// recovers its Raft log rather than rejoining empty.
const LOG_STORE_SUBPATH: &str = "raft/ownership-log.redb";

/// The resolved seed for one `ha` node: its own voter id, the roster `OpenRaft` bootstraps from, the
/// durable log path, the group name, and the shared peer credential.
///
/// Built synchronously so every address and roster rule is enforced before a runtime starts; only
/// [`ignite`](Self::ignite) touches the disk or network.
pub(super) struct ConsensusPlan {
    local: VoterId,
    roster: BTreeMap<VoterId, PeryxNode>,
    log_path: PathBuf,
    group: String,
    token: String,
}

impl ConsensusPlan {
    /// Resolve the consensus seed for this process, or `None` when it runs no ownership group.
    ///
    /// A group forms only under `ha` mode with a member roster to name the voters; `none`, `dc`, and an
    /// `ha` process without a roster run no consensus and return `None`. Given a roster, the process must
    /// name itself with a writer identity so it can find its own voter, so an identity that is missing or
    /// absent from the roster is a configuration error caught here.
    ///
    /// # Errors
    /// Returns an error when a rostered `ha` process has no writer identity, when that identity is not a
    /// member, when a member address is not a bare `host:port` authority, when two members collide onto
    /// one voter id, or when the shared peer token cannot be read.
    pub(super) fn from_config(config: &Config) -> anyhow::Result<Option<Self>> {
        let AvailabilityConfig::Ha(replication) = &config.availability else {
            return Ok(None);
        };
        let Some(membership) = config.dc_membership.as_ref() else {
            return Ok(None);
        };
        let identity = config
            .writer_identity
            .as_deref()
            .context("an `ha` consensus roster needs a `writer-identity` to find this node in it")?;
        let roster = build_roster(membership)?;
        let local = voter_id(&local_datacenter(membership, identity)?);
        let (ReplicationConfig::Primary { token, .. } | ReplicationConfig::Replica { token, .. }) = replication;
        let token = token.read().context("read the shared consensus peer token")?;
        Ok(Some(Self {
            local,
            roster,
            log_path: config.data_dir.join(LOG_STORE_SUBPATH),
            group: membership.group.clone(),
            token,
        }))
    }

    /// Open the durable log store, assemble the node over its three adapters, and bootstrap the group.
    ///
    /// The bootstrap is idempotent, so a restarted node rejoins the group it already formed rather than
    /// failing. Only the seed node forms the group; a node added later joins through an operator-driven
    /// membership change on the existing leader.
    ///
    /// # Errors
    /// Returns an error when the log directory or store cannot be opened, the node cannot start, or the
    /// bootstrap fails for an inconsistent roster.
    pub(super) async fn ignite(&self) -> anyhow::Result<RaftNode> {
        // The path is always `data_dir/raft/<file>`, so it has a parent directory to create.
        let parent = self
            .log_path
            .parent()
            .expect("the consensus log path is rooted under the data directory");
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create the consensus log directory {}", parent.display()))?;
        let store = RaftLogStore::open(&self.log_path)
            .with_context(|| format!("open the consensus log store at {}", self.log_path.display()))?;
        let node = RaftNode::start(
            self.local,
            RaftConfig::default(),
            self.group.clone(),
            PeerRaftNetworkFactory::new(self.token.clone(), PEER_RPC_TIMEOUT),
            RaftLogStoreAdapter::new(store),
            OwnershipStateMachine::default(),
        )
        .await
        .context("start the ownership consensus node")?;
        node.bootstrap(self.roster.clone())
            .await
            .context("bootstrap the ownership consensus group")?;
        Ok(node)
    }
}

/// Map each roster member to its voter id and node data, rejecting a non-authority address or an id
/// collision so the group `OpenRaft` bootstraps from is well-formed.
fn build_roster(membership: &DcMembership) -> anyhow::Result<BTreeMap<VoterId, PeryxNode>> {
    let mut roster = BTreeMap::new();
    for member in &membership.members {
        let node = PeryxNode {
            datacenter: DatacenterId(member.dc.clone()),
            addr: authority(&member.address)?,
        };
        if let Some(existing) = roster.insert(voter_id(&member.dc), node) {
            bail!(
                "datacenter {:?} collides with {:?} on the same consensus voter id",
                member.dc,
                existing.datacenter.0
            );
        }
    }
    Ok(roster)
}

/// The datacenter of the member whose identity is this process, so its voter id names the local node.
fn local_datacenter(membership: &DcMembership, identity: &str) -> anyhow::Result<String> {
    membership
        .members
        .iter()
        .find(|member| member.node == identity)
        .map(|member| member.dc.clone())
        .with_context(|| format!("this node's identity {identity:?} is not a member of the roster"))
}

/// The bare `host:port` authority a peer's Raft RPCs dial, extracted from the member's advertised URL.
///
/// The network factory prepends its own scheme to this, so a scheme, path, query, or missing port would
/// misdirect or malform the peer URL. Rejecting them here fails a bad roster at config time rather than
/// as a lazy per-peer unreachable error once the node runs.
fn authority(address: &str) -> anyhow::Result<String> {
    let url = Url::parse(address).with_context(|| format!("member address {address:?} is not a valid URL"))?;
    let host = url
        .host_str()
        .with_context(|| format!("member address {address:?} has no host"))?;
    let port = url
        .port()
        .with_context(|| format!("member address {address:?} needs an explicit `host:port`"))?;
    if url.path() != "/" && !url.path().is_empty() {
        bail!("member address {address:?} must be a bare host:port with no path");
    }
    Ok(format!("{host}:{port}"))
}

/// A stable 64-bit id for a datacenter identity, computed with FNV-1a so it is identical on every node
/// and across restarts and toolchains, unlike the standard hasher's unspecified output.
fn voter_id(datacenter: &str) -> VoterId {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for byte in datacenter.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
#[path = "raft_tests.rs"]
mod raft_tests;

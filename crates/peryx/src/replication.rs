//! Process-level replication configuration and follower scheduling.

mod analytics;
mod availability_metrics;
mod drain;
mod raft;
mod worker;

pub use drain::AuthorityDrainJob;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fmt::Write as _, sync::Mutex};

use anyhow::Context as _;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse as _, Response};
use axum::routing::get;
use axum::{Json, Router};
use peryx_driver::read_through::{DEFAULT_READ_THROUGH_LIMITS, DcTransport, RemotePlacementReader};
use peryx_driver::state::SEARCH_VIEW;
use peryx_driver::{AppState, PrometheusSource};
use peryx_http::handlers::status_authorization;
use peryx_http::response_security::{
    ClassifiedField, FieldClassification, ProtectedCachePolicy, ResponseAuthorization, filter_fields,
};
use peryx_replication::{
    BatchRequest, BlobPlaneReport, BlobSources, CapacityLimited, DEFAULT_DEAD_AFTER, DEFAULT_RECONNECT_POLICY,
    DEFAULT_SUSPECT_AFTER, DEFAULT_TRANSFER_LIMITS, HttpBlobTransport, HttpPeerTransport, LivenessTracker,
    PeerTransport, ReconnectPolicy, Replica, Retry, SyncError, SyncOutcome, TransferLimits, TransportError,
    advance_blob_frontier, liveness_router, primary_router, pull_outstanding,
};
use peryx_storage::blob::BlobStorage;
use peryx_storage::meta::{DataCenterId, MetaStore};
use peryx_upstream::redact_url;
use serde_json::{Value, json};

use crate::config::{AvailabilityConfig, Config, DcMembership, DcRole, ReplicationConfig};
use crate::replication::availability_metrics::AvailabilityMetrics;
use crate::replication::worker::{AvailabilityRuntime, WorkerShared};

#[derive(Clone, Copy)]
enum ReplicaHealthStatus {
    Starting,
    CatchingUp,
    CaughtUp,
    Error,
}

/// Why a replica is unready, kept apart from the transient status so a schema mismatch a restart
/// cannot resolve reads differently from a page a later poll will drain.
#[derive(Clone, Copy)]
enum ReplicaFault {
    None,
    Sync,
    IncompatibleSchema,
}

#[derive(Clone, Copy)]
struct ReplicaObservation {
    status: ReplicaHealthStatus,
    fault: ReplicaFault,
    serial: u64,
    primary_serial: Option<u64>,
    changes: u64,
    errors: u64,
    readable_serial: u64,
    blobs_fetched: u64,
    blobs_pending: u64,
}

struct ReplicaMonitor {
    observation: Mutex<ReplicaObservation>,
}

impl ReplicaMonitor {
    const fn new(serial: u64) -> Self {
        Self {
            observation: Mutex::new(ReplicaObservation {
                status: ReplicaHealthStatus::Starting,
                fault: ReplicaFault::None,
                serial,
                primary_serial: None,
                changes: 0,
                errors: 0,
                readable_serial: 0,
                blobs_fetched: 0,
                blobs_pending: 0,
            }),
        }
    }

    fn record(&self, outcome: SyncOutcome) {
        let mut observation = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        observation.status = if outcome.caught_up() {
            ReplicaHealthStatus::CaughtUp
        } else {
            ReplicaHealthStatus::CatchingUp
        };
        observation.fault = ReplicaFault::None;
        observation.serial = outcome.serial;
        observation.primary_serial = Some(outcome.primary_serial);
        observation.changes = observation
            .changes
            .saturating_add(u64::try_from(outcome.changes).unwrap_or(u64::MAX));
    }

    /// Record the serial a reader may safely serve, the lowest frontier every required derived view
    /// has applied. It trails the committed serial while the search index catches up, so a scrape
    /// shows how far derived views lag the applied metadata.
    fn record_readable(&self, serial: u64) {
        let mut observation = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        observation.readable_serial = serial;
    }

    /// Record what one blob-plane pass made of its outstanding blobs: add the blobs it committed to the
    /// cumulative fetched counter and set the pending gauge to how many it left for a later pass. A backlog
    /// that the plane cannot drain shows as a pending gauge stuck above zero while the readable serial
    /// stalls, so a stuck blob is visible before the readable frontier lags.
    fn record_blobs(&self, report: BlobPlaneReport) {
        let mut observation = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        observation.blobs_fetched = observation
            .blobs_fetched
            .saturating_add(u64::try_from(report.fetched).unwrap_or(u64::MAX));
        observation.blobs_pending = u64::try_from(report.pending).unwrap_or(u64::MAX);
    }

    fn record_error(&self, error: &SyncError) {
        let mut observation = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        observation.status = ReplicaHealthStatus::Error;
        observation.fault = match error {
            SyncError::UnsupportedVersion { .. } => ReplicaFault::IncompatibleSchema,
            _ => ReplicaFault::Sync,
        };
        observation.errors = observation.errors.saturating_add(1);
    }

    fn snapshot(&self) -> ReplicaObservation {
        *self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The reason this replica cannot yet serve at the primary's frontier, or `None` when it is
    /// caught up and error-free. A persistent schema mismatch outranks a transient sync failure,
    /// which outranks ordinary catch-up lag.
    fn readiness_gap(&self) -> Option<&'static str> {
        let observation = self.snapshot();
        match observation.fault {
            ReplicaFault::IncompatibleSchema => Some("incompatible_schema"),
            ReplicaFault::Sync => Some("sync_error"),
            ReplicaFault::None => match observation.status {
                ReplicaHealthStatus::CaughtUp => None,
                ReplicaHealthStatus::Starting | ReplicaHealthStatus::CatchingUp | ReplicaHealthStatus::Error => {
                    Some("frontier_lag")
                }
            },
        }
    }
}

impl PrometheusSource for ReplicaMonitor {
    fn write_metrics(&self, body: &mut String) {
        let observation = *self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let caught_up = u8::from(matches!(observation.status, ReplicaHealthStatus::CaughtUp));
        let _ = write!(
            body,
            "# HELP peryx_replication_caught_up Whether the replica has reached the latest observed primary serial.\n\
             # TYPE peryx_replication_caught_up gauge\n\
             peryx_replication_caught_up {caught_up}\n\
             # HELP peryx_replication_serial Last serial committed by the replica.\n\
             # TYPE peryx_replication_serial gauge\n\
             peryx_replication_serial {}\n\
             # HELP peryx_replication_changes_total Metadata changes committed by the replica.\n\
             # TYPE peryx_replication_changes_total counter\n\
             peryx_replication_changes_total {}\n\
             # HELP peryx_replication_sync_errors_total Replica synchronization failures.\n\
             # TYPE peryx_replication_sync_errors_total counter\n\
             peryx_replication_sync_errors_total {}\n",
            observation.serial, observation.changes, observation.errors
        );
        let _ = write!(
            body,
            "# HELP peryx_replication_readable_serial Highest serial every required derived view has applied.\n\
             # TYPE peryx_replication_readable_serial gauge\n\
             peryx_replication_readable_serial {}\n\
             # HELP peryx_replication_blobs_fetched_total Blobs the dual-plane blob fetch committed.\n\
             # TYPE peryx_replication_blobs_fetched_total counter\n\
             peryx_replication_blobs_fetched_total {}\n\
             # HELP peryx_replication_blobs_pending Blobs the last blob-plane pass left outstanding.\n\
             # TYPE peryx_replication_blobs_pending gauge\n\
             peryx_replication_blobs_pending {}\n",
            observation.readable_serial, observation.blobs_fetched, observation.blobs_pending
        );
        if let Some(primary_serial) = observation.primary_serial {
            let _ = write!(
                body,
                "# HELP peryx_replication_primary_serial Latest serial reported by the primary.\n\
                 # TYPE peryx_replication_primary_serial gauge\n\
                 peryx_replication_primary_serial {primary_serial}\n\
                 # HELP peryx_replication_lag Serial distance between the primary and replica.\n\
                 # TYPE peryx_replication_lag gauge\n\
                 peryx_replication_lag {}\n",
                primary_serial.saturating_sub(observation.serial)
            );
        }
    }
}

/// The replication role a `dc` or `ha` process drives, reported to any caller so an operator can
/// route probes without a credential.
#[derive(Clone, Copy)]
enum AvailabilityRole {
    Primary,
    Replica,
}

impl AvailabilityRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Replica => "replica",
        }
    }
}

/// What a replica exposes about the primary it follows: the shared monitor and the primary's origin
/// with credentials, query, and fragment already removed.
#[derive(Clone)]
struct ReplicaView {
    monitor: Arc<ReplicaMonitor>,
    upstream: String,
}

/// The readiness reason a replica reports when its background worker domain has failed, or `None`
/// when no worker runs or the runtime is still healthy. A panicked task clears the domain's health,
/// so a replica keeps serving reads under its staleness contract while readiness names the fault.
fn worker_reason(workers: Option<&Arc<WorkerShared>>) -> Option<&'static str> {
    workers
        .filter(|workers| !workers.is_healthy())
        .map(|_| "worker_unhealthy")
}

/// The availability surface a `dc` or `ha` process serves its health and readiness resources from.
/// A `none` process holds none of this, so it mounts neither resource.
#[derive(Clone)]
struct AvailabilityNode {
    app: Arc<AppState>,
    mode: &'static str,
    role: AvailabilityRole,
    replica: Option<ReplicaView>,
    /// The writer's view of replica liveness, present only when a `dc`/`ha` primary follows a
    /// configured member roster. It informs routing hints and never gates this node's own readiness.
    liveness: Option<Arc<LivenessTracker>>,
    workers: Option<Arc<WorkerShared>>,
}

impl AvailabilityNode {
    /// Whether this node can serve at its frontier, and every reason it cannot. The list is empty
    /// exactly when the node is ready, so a probe reads `ready` and a human reads the causes. The
    /// blob store carries a mount's crash and replication guarantees, so its reachability gates
    /// readiness; a replica's metadata frontier is validated at startup and tracked below.
    async fn readiness(&self) -> (bool, Vec<&'static str>) {
        let mut reasons = Vec::new();
        if self.app.blobs.health().await.is_err() {
            reasons.push("blob_store");
        }
        if let Some(gap) = self
            .replica
            .as_ref()
            .and_then(|replica| replica.monitor.readiness_gap())
        {
            reasons.push(gap);
        }
        reasons.extend(worker_reason(self.workers.as_ref()));
        (reasons.is_empty(), reasons)
    }

    /// The readiness verdict and a body already filtered to the caller's class: mode, role, and the
    /// verdict for any caller; serials and lag for an operator; the redacted primary origin for an
    /// administrator.
    async fn document(&self, authorization: ResponseAuthorization) -> (bool, serde_json::Map<String, Value>) {
        let (ready, reasons) = self.readiness().await;
        let mut fields = vec![
            ClassifiedField::new("mode", FieldClassification::Public, json!(self.mode)),
            ClassifiedField::new("role", FieldClassification::Public, json!(self.role.as_str())),
            ClassifiedField::new("ready", FieldClassification::Public, json!(ready)),
            ClassifiedField::new("reasons", FieldClassification::Public, json!(reasons)),
        ];
        if let Some(replica) = &self.replica {
            let observation = replica.monitor.snapshot();
            let lag = observation
                .primary_serial
                .map(|primary_serial| primary_serial.saturating_sub(observation.serial));
            fields.extend([
                ClassifiedField::new("serial", FieldClassification::Operator, json!(observation.serial)),
                ClassifiedField::new(
                    "primary_serial",
                    FieldClassification::Operator,
                    json!(observation.primary_serial),
                ),
                ClassifiedField::new("lag", FieldClassification::Operator, json!(lag)),
                ClassifiedField::new(
                    "synced_changes",
                    FieldClassification::Operator,
                    json!(observation.changes),
                ),
                ClassifiedField::new("sync_errors", FieldClassification::Operator, json!(observation.errors)),
                ClassifiedField::new("upstream", FieldClassification::Administrator, json!(replica.upstream)),
            ]);
        } else {
            let serial = self.app.meta.current_serial().unwrap_or(0);
            fields.push(ClassifiedField::new(
                "serial",
                FieldClassification::Operator,
                json!(serial),
            ));
            if let Some(liveness) = &self.liveness {
                fields.push(ClassifiedField::new(
                    "peers",
                    FieldClassification::Operator,
                    json!(liveness.summary(Instant::now())),
                ));
            }
        }
        let body = filter_fields(authorization, fields).expect("public and allowed scopes classify");
        (ready, body)
    }
}

/// `GET /+replication/v1/health`: availability liveness. It answers `200` whenever the process still
/// serves the resource, so a load balancer never restarts a replica that is merely catching up. The
/// body carries the readiness verdict for callers that want one document.
async fn availability_health(State(node): State<AvailabilityNode>, headers: HeaderMap) -> Response {
    let authorization = status_authorization(&node.app, &headers).await;
    let (_ready, body) = node.document(authorization).await;
    availability_response(StatusCode::OK, body)
}

/// `GET /+replication/v1/ready`: availability readiness. It answers `503` for a frontier gap, an
/// incompatible primary schema, or a failed local store, so readiness removes the node from a pool
/// without restarting it.
async fn availability_readiness(State(node): State<AvailabilityNode>, headers: HeaderMap) -> Response {
    let authorization = status_authorization(&node.app, &headers).await;
    let (ready, body) = node.document(authorization).await;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    availability_response(status, body)
}

fn availability_response(status: StatusCode, body: serde_json::Map<String, Value>) -> Response {
    let mut response = (status, Json(Value::Object(body))).into_response();
    ProtectedCachePolicy::NoStore.apply(response.headers_mut());
    response
}

/// How much of the primary's blob traffic one dual-plane fetch pass may hold in flight, and how long a
/// single blob request may run before it is a retryable loss.
const BLOB_FETCH_CONCURRENCY: std::num::NonZeroUsize = std::num::NonZeroUsize::new(8).expect("8 is non-zero");
const BLOB_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

const METADATA_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Install the serving read-through capability when this node knows its own datacenter and has peers to
/// fetch a missed public download from.
///
/// A node reaches a peer's blob-serving endpoint with the same replication token its metadata plane
/// uses, so the capability is available only under `dc`/`ha` with a configured roster. This node's own
/// datacenter comes from its writer identity; a node that does not carry one (a replica) installs no
/// capability yet and serves misses through its ecosystem's upstream path. Peers are keyed by the unique
/// datacenter each roster member runs in, the node's own excluded so it never fetches from itself.
///
/// # Errors
/// Returns an error if the replication token cannot be read or a peer address is not a usable HTTP base.
pub fn install_read_through(config: &Config, state: &Arc<AppState>) -> anyhow::Result<()> {
    let (Some(replication), Some(membership)) = (config.availability.replication(), config.dc_membership.as_ref())
    else {
        return Ok(());
    };
    let Some(local_dc) = local_datacenter(config, membership) else {
        return Ok(());
    };
    let (ReplicationConfig::Primary { token, .. } | ReplicationConfig::Replica { token, .. }) = replication;
    let token = token.read().context("read the replication token for read-through")?;
    let limits = config.read_through.unwrap_or(DEFAULT_READ_THROUGH_LIMITS);
    let transfer = TransferLimits {
        max_operations: TransferLimits::default().max_operations,
        max_encoded_bytes: limits.per_fetch_bytes,
    };
    let mut delegates: HashMap<String, DcTransport> = HashMap::new();
    for member in &membership.members {
        if member.dc == local_dc {
            continue;
        }
        let base = peer_blob_base(&member.address);
        let transport = HttpBlobTransport::new(&base, token.clone(), transfer, BLOB_FETCH_TIMEOUT)
            .with_context(|| format!("build a read-through blob transport for datacenter {}", member.dc))?;
        delegates.insert(
            member.dc.clone(),
            Arc::new(CapacityLimited::new(transport, limits.concurrency)),
        );
    }
    if delegates.is_empty() {
        return Ok(());
    }
    let dc = DataCenterId::new(local_dc.clone()).with_context(|| format!("local datacenter identity {local_dc}"))?;
    let reader = RemotePlacementReader::new(dc, delegates, limits, state.serving.clock.clone());
    state.set_read_through(Arc::new(reader));
    Ok(())
}

/// A peer's blob-serving base URL from its roster address. A bare `host:port` reaches the peer over
/// plaintext HTTP, the availability plane's default; an operator terminating TLS gives a full
/// `https://` address, which is used as written.
fn peer_blob_base(address: &str) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        address.to_owned()
    } else {
        format!("http://{address}")
    }
}

/// This node's own datacenter, read from the roster member its writer identity names. A node with no
/// configured identity (a replica) has none.
fn local_datacenter(config: &Config, membership: &DcMembership) -> Option<String> {
    let identity = config.writer_identity.as_deref()?;
    membership
        .members
        .iter()
        .find(|member| member.node == identity)
        .map(|member| member.dc.clone())
}

struct ReplicaLoop {
    app: Arc<AppState>,
    metadata: HttpPeerTransport,
    policy: ReconnectPolicy,
    meta: MetaStore,
    blobs: BlobStorage,
    page_size: std::num::NonZeroUsize,
    poll_interval: Duration,
    monitor: Arc<ReplicaMonitor>,
    metrics: Arc<AvailabilityMetrics>,
    transport: CapacityLimited<HttpBlobTransport>,
}

fn log_replica_page(outcome: SyncOutcome) {
    tracing::info!(
        changes = outcome.changes,
        serial = outcome.serial,
        primary_serial = outcome.primary_serial,
        "replica page applied"
    );
}

/// Apply the effects of one replicated page: log it, rebuild the derived views the changed keys touch,
/// and advance the readable frontier over the applied serial once every required view reflects it.
///
/// Each ecosystem driver rebuilds its own affected views. When they all succeed the search view frontier
/// advances to `outcome.serial`, so a read the gate held becomes visible only after the view it depends
/// on caught up. When a driver reports a [`ViewBlock`](peryx_driver::state::ViewBlock) the frontier stays
/// where it was — the read stays held — and the lazy full refresh a later search runs recovers it. The
/// mutation-epoch bump keeps that recovery path armed for every ecosystem, the OCI views included. A page
/// with no changes does nothing. This is synchronous and independent of the async sync loop, so a direct
/// test covers it deterministically rather than riding on async scheduling.
pub(crate) fn apply_replicated_page(app: &AppState, outcome: SyncOutcome, changed_keys: &[String]) {
    if outcome.changes == 0 {
        return;
    }
    log_replica_page(outcome);
    app.bump_search_epoch();
    let state = app.serving.as_ref();
    let mut blocked = None;
    for driver in app.drivers() {
        if let Err(block) = driver.apply_replicated_changes(state, changed_keys) {
            blocked = Some(block);
        }
    }
    if let Some(block) = blocked {
        tracing::warn!(
            view = %block.view,
            serial = outcome.serial,
            "held the readable frontier: a required derived view could not rebuild"
        );
    } else if let Err(error) = state.meta.set_view_frontier(SEARCH_VIEW, outcome.serial) {
        tracing::error!(%error, serial = outcome.serial, "advancing the search view frontier failed");
    }
}

/// The delay the replica loop waits after one pass: nothing between pages that still have more to pull,
/// the poll interval once caught up, and a reconnect-policy backoff after a retryable transport loss,
/// falling back to the poll interval once the policy gives up, so a peer that never recovers keeps trying
/// at the base cadence rather than spinning. `attempt` counts consecutive transport losses and resets on
/// any pass that applied.
pub(crate) fn schedule_delay(
    result: &Result<bool, TransportError>,
    attempt: &mut u32,
    policy: &ReconnectPolicy,
    poll_interval: Duration,
) -> Duration {
    match result {
        Ok(caught_up) => {
            *attempt = 0;
            if *caught_up { poll_interval } else { Duration::ZERO }
        }
        Err(error) => {
            *attempt += 1;
            match policy.on_error(error, *attempt) {
                Retry::After(delay) => delay,
                Retry::GiveUp { .. } => poll_interval,
            }
        }
    }
}

impl ReplicaLoop {
    async fn run(self) {
        let mut attempt: u32 = 0;
        loop {
            let result = self.cycle().await;
            let delay = schedule_delay(&result, &mut attempt, &self.policy, self.poll_interval);
            tokio::time::sleep(delay).await;
        }
    }

    /// Drive both planes for one pass. Metadata commits and its search view advances first, so a blob
    /// still in flight never holds up metadata; the blob plane then pulls the tail's outstanding bytes
    /// and moves the blob frontier only over serials whose blobs are all local. The readable frontier
    /// the loop records is the slower of the two views, so reads never outrun the bytes they name. A
    /// blob loss records and retries; the metadata plane keeps advancing regardless.
    ///
    /// Returns `Ok(caught_up)` after a page applies, or `Err` on a retryable metadata transport loss so
    /// the run loop backs off. A page that fails validation or commit records the failure and returns
    /// `Ok(true)`, since re-fetching the same cursor at the poll cadence is the recovery, not a backoff.
    async fn cycle(&self) -> Result<bool, TransportError> {
        let started = Instant::now();
        let replica = Replica::new(&self.meta, self.page_size);
        let after = match replica.state() {
            Ok(state) => state.map_or(0, |state| state.serial),
            Err(error) => return Ok(self.record_metadata_error(&error, started.elapsed())),
        };
        let request = BatchRequest {
            after,
            max_operations: self.page_size,
        };
        let frame = match self.metadata.fetch_batch(request).await {
            Ok(frame) => frame,
            Err(error) => {
                self.record_metadata_error(&SyncError::primary(error.clone()), started.elapsed());
                return Err(error);
            }
        };
        let (outcome, changed_keys) = match replica.apply_page(frame.into_page()) {
            Ok((outcome, changed_keys, _referenced)) => (outcome, changed_keys),
            Err(error) => return Ok(self.record_metadata_error(&error, started.elapsed())),
        };
        let elapsed = started.elapsed();
        apply_replicated_page(&self.app, outcome, &changed_keys);
        match self.pull_blobs().await {
            Ok(report) => self.monitor.record_blobs(report),
            Err(error) => {
                self.monitor.record_error(&error);
                self.metrics.record_error(&error, elapsed);
                tracing::error!(%error, "replica blob plane failed");
            }
        }
        self.monitor.record(outcome);
        let readable = self.app.readable_frontier().map_or(0, |frontier| frontier.serial);
        self.monitor.record_readable(readable);
        self.metrics.record_cycle(outcome, elapsed);
        Ok(outcome.caught_up())
    }

    /// Record a metadata-plane failure on the monitor and the metrics, and report the pass as done so
    /// the loop retries at its poll cadence. Returns `true` for the caught-up slot the cycle result carries.
    fn record_metadata_error(&self, error: &SyncError, elapsed: Duration) -> bool {
        self.monitor.record_error(error);
        self.metrics.record_error(error, elapsed);
        tracing::error!(%error, "replica metadata synchronization failed");
        true
    }

    async fn pull_blobs(&self) -> Result<BlobPlaneReport, SyncError> {
        // A replica draws every blob whole from its upstream primary until placement descriptors ride in
        // the replicated page and name the peers that hold each blob; with no delegates every blob is
        // single-source and takes the whole-blob path.
        let delegates = HashMap::new();
        let sources = BlobSources {
            simple: &self.transport,
            delegates: &delegates,
            local_dc: "",
        };
        let report = pull_outstanding(
            &sources,
            &self.meta,
            &self.blobs,
            self.page_size,
            BLOB_FETCH_CONCURRENCY,
        )
        .await?;
        advance_blob_frontier(&self.meta, &self.blobs, self.page_size).await?;
        Ok(report)
    }
}

/// Track the configured replica members so the writer can age their beacons into routing hints. A
/// process without a member roster, or a roster naming no replica, tracks nothing.
fn primary_liveness(config: &Config) -> Option<Arc<LivenessTracker>> {
    let replicas: Vec<String> = config
        .dc_membership
        .as_ref()?
        .members
        .iter()
        .filter(|member| member.role == DcRole::Replica)
        .map(|member| member.node.clone())
        .collect();
    (!replicas.is_empty()).then(|| {
        Arc::new(LivenessTracker::new(
            replicas,
            DEFAULT_SUSPECT_AFTER,
            DEFAULT_DEAD_AFTER,
        ))
    })
}

/// Replication routes and follower work prepared from one resolved configuration.
pub struct ReplicationRuntime {
    primary: Option<Router>,
    replica: Option<(ReplicaLoop, AvailabilityRuntime)>,
    availability: Option<AvailabilityNode>,
    /// The replica's analytics pull worker, spawned onto the availability runtime alongside the replica
    /// loop. `None` unless this process follows an upstream to pull analytics batches from.
    analytics_puller: Option<analytics::AnalyticsPuller>,
    /// The ownership consensus seed for an `ha` process, resolved synchronously here and ignited into a
    /// running node once the async runtime is up. `None` under `none` and `dc`, which run no group.
    consensus: Option<raft::ConsensusPlan>,
}

/// The running ownership consensus an `ha` process holds for its lifetime.
///
/// Carries the group handle the mutation path submits first-publish claims to, and the peer RPC router
/// the process mounts on its peer-facing listener so remote voters can exchange raft RPCs with this node.
/// Dropping it shuts the raft runtime down.
pub struct Consensus {
    /// The group handle to register on the [`AppState`] for the mutation path.
    pub authority: Arc<dyn peryx_driver::state::OwnershipAuthority>,
    /// The same group as the membership and transfer command seam the administrator control plane submits
    /// through, wrapped once the runtime holds a clock.
    pub control: Arc<dyn peryx_driver::state::MembershipControl>,
    /// The receive-side raft RPC routes to mount on the peer-facing (availability) listener.
    pub peer_router: Router,
}

/// Merge the producer's sealed-batch analytics endpoint into `router` when this node has an identity to
/// stamp its intervals with, leaving `router` untouched otherwise.
fn merge_analytics_endpoint(
    router: Router,
    config: &Config,
    state: &Arc<AppState>,
    token: &str,
) -> anyhow::Result<Router> {
    let Some(identity) = &config.node_identity else {
        return Ok(router);
    };
    let epoch = analytics::resolve_producer_epoch(&state.serving.meta.analytics())
        .context("resolve the analytics producer epoch")?;
    Ok(router.merge(analytics::analytics_router(
        token.to_owned(),
        state.serving.metrics.clone(),
        peryx_replication::ProducerId(identity.clone()),
        epoch,
    )))
}

/// Build the replica's background analytics pull worker, or `None` when this node is not a replica.
fn build_analytics_puller(
    config: &Config,
    state: &Arc<AppState>,
) -> anyhow::Result<Option<analytics::AnalyticsPuller>> {
    let Some(ReplicationConfig::Replica {
        upstream,
        token,
        poll_interval,
        ..
    }) = config.availability.replication()
    else {
        return Ok(None);
    };
    let token = token
        .read()
        .context("read the replica replication token for analytics")?;
    let puller = analytics::AnalyticsPuller::new(upstream, token, state.serving.meta.analytics(), *poll_interval)
        .context("build the analytics pull worker")?;
    Ok(Some(puller))
}

impl ReplicationRuntime {
    /// Prepare the configured replication role without starting background work.
    ///
    /// # Errors
    /// Returns an error if a secret cannot be read, the upstream URL is invalid, or the primary
    /// router rejects its identity or token.
    pub fn new(config: &Config, state: &Arc<AppState>) -> anyhow::Result<Self> {
        let mode = match &config.availability {
            AvailabilityConfig::None => "none",
            AvailabilityConfig::Dc(_) => "dc",
            AvailabilityConfig::Ha(_) => "ha",
        };
        let (primary, replica, availability) = match config.availability.replication() {
            None => (None, None, None),
            Some(ReplicationConfig::Primary { source, token }) => {
                let token = token.read().context("read the primary replication token")?;
                let router = primary_router(
                    source.clone(),
                    token.clone(),
                    state.serving.meta.clone(),
                    state.serving.blobs.clone(),
                )
                .context("build primary replication routes")?;
                let router = merge_analytics_endpoint(router, config, state, &token)?;
                let liveness = primary_liveness(config);
                let router = match &liveness {
                    Some(tracker) => {
                        router.merge(liveness_router(token, tracker.clone()).context("build liveness ingest routes")?)
                    }
                    None => router,
                };
                let node = AvailabilityNode {
                    app: state.clone(),
                    mode,
                    role: AvailabilityRole::Primary,
                    replica: None,
                    liveness,
                    workers: None,
                };
                (Some(router), None, Some(node))
            }
            Some(ReplicationConfig::Replica {
                upstream,
                token,
                poll_interval,
                page_size,
            }) => {
                let token = token.read().context("read the replica replication token")?;
                let metadata =
                    HttpPeerTransport::new(upstream, token.clone(), DEFAULT_TRANSFER_LIMITS, METADATA_FETCH_TIMEOUT)
                        .context("build replica metadata transport")?;
                let blob_transport =
                    HttpBlobTransport::new(upstream, token, TransferLimits::default(), BLOB_FETCH_TIMEOUT)
                        .context("build replica blob transport")?;
                let transport = CapacityLimited::new(blob_transport, BLOB_FETCH_CONCURRENCY);
                let monitor = Arc::new(ReplicaMonitor::new(
                    state.meta.current_serial().context("read the replica serial")?,
                ));
                let metrics = Arc::new(AvailabilityMetrics::default());
                let workers = Arc::new(WorkerShared::for_replica());
                state.register_prometheus(monitor.clone());
                state.register_prometheus(metrics.clone());
                state.register_prometheus(workers.clone());
                let runtime =
                    AvailabilityRuntime::start(workers.clone()).context("build the availability worker runtime")?;
                let node = AvailabilityNode {
                    app: state.clone(),
                    mode,
                    role: AvailabilityRole::Replica,
                    replica: Some(ReplicaView {
                        monitor: monitor.clone(),
                        upstream: redact_url(upstream),
                    }),
                    liveness: None,
                    workers: Some(workers),
                };
                (
                    None,
                    Some((
                        ReplicaLoop {
                            app: state.clone(),
                            metadata,
                            policy: DEFAULT_RECONNECT_POLICY,
                            meta: state.serving.meta.clone(),
                            blobs: state.serving.blobs.clone(),
                            page_size: *page_size,
                            poll_interval: *poll_interval,
                            monitor,
                            metrics,
                            transport,
                        },
                        runtime,
                    )),
                    Some(node),
                )
            }
        };
        // A `dc` or `ha` node resolves each write to a datacenter durability decision, so it exposes the
        // outcome counters; single-node `none` runs no such decision and registers nothing.
        if availability.is_some() {
            state.register_prometheus(state.serving.dc_durability.clone());
        }
        Ok(Self {
            primary,
            replica,
            availability,
            analytics_puller: build_analytics_puller(config, state)?,
            consensus: raft::ConsensusPlan::from_config(config)?,
        })
    }

    /// Ignite the ownership consensus node, when this process runs an `ha` group, returning the running
    /// handle for the caller to hold for the process lifetime. A `none` or `dc` process runs no group and
    /// returns `None`. This is the async step the synchronous [`new`](Self::new) defers: it opens the
    /// durable log store and bootstraps the group.
    ///
    /// # Errors
    /// Returns an error when the consensus log store cannot be opened or the node cannot start or
    /// bootstrap.
    pub async fn ignite_consensus(&self) -> anyhow::Result<Option<Consensus>> {
        match &self.consensus {
            Some(plan) => {
                let node = plan.ignite().await?;
                let peer_router = peryx_replication::raft::network::raft_rpc_router(plan.token(), node.rpc_handler())
                    .context("build the raft peer rpc router")?;
                let group = Arc::new(raft::OwnershipGroup::new(node, plan.home()));
                Ok(Some(Consensus {
                    authority: group.clone(),
                    control: group,
                    peer_router,
                }))
            }
            None => Ok(None),
        }
    }

    /// Whether this process follows a primary and must avoid local writers.
    #[must_use]
    pub const fn is_replica(&self) -> bool {
        self.replica.is_some()
    }

    /// Mount primary routes and the availability health and readiness resources, when configured, on
    /// the process router. A `none` process has no availability surface and mounts neither resource.
    pub fn mount(&self, router: Router) -> Router {
        let router = match &self.primary {
            Some(primary) => router.merge(primary.clone()),
            None => router,
        };
        match &self.availability {
            Some(node) => router.merge(
                Router::new()
                    .route("/+replication/v1/health", get(availability_health))
                    .route("/+replication/v1/ready", get(availability_readiness))
                    .with_state(node.clone()),
            ),
            None => router,
        }
    }

    /// Start the replica loop on its own availability runtime, when configured, returning the
    /// runtime so the caller keeps it alive for the process lifetime. Follower work runs on the
    /// bounded worker pool rather than the foreground request executor.
    #[must_use]
    pub fn start(self) -> Option<AvailabilityRuntime> {
        let (replica, runtime) = self.replica?;
        let _ = runtime.try_spawn(Box::pin(replica.run()));
        if let Some(puller) = self.analytics_puller {
            let _ = runtime.try_spawn(Box::pin(puller.run()));
        }
        Some(runtime)
    }

    #[cfg(test)]
    pub(crate) async fn sync_cycle(&self) -> Option<bool> {
        match &self.replica {
            Some((replica, _)) => Some(replica.cycle().await.unwrap_or(true)),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use peryx_driver::PrometheusSource;
    use peryx_replication::BlobPlaneReport;

    use super::{ReplicaMonitor, WorkerShared, worker_reason};

    #[test]
    fn test_worker_reason_names_only_a_failed_domain() {
        assert_eq!(worker_reason(None), None);
        let healthy = Arc::new(WorkerShared::for_replica());
        assert_eq!(worker_reason(Some(&healthy)), None);
        let failed = Arc::new(WorkerShared::for_replica());
        failed.record_panic();
        assert_eq!(worker_reason(Some(&failed)), Some("worker_unhealthy"));
    }

    #[test]
    fn test_monitor_surfaces_blob_fetched_and_pending_counts() {
        let monitor = ReplicaMonitor::new(0);
        monitor.record_blobs(BlobPlaneReport { fetched: 2, pending: 3 });

        let mut body = String::new();
        monitor.write_metrics(&mut body);
        assert!(body.contains("peryx_replication_blobs_fetched_total 2\n"), "{body}");
        assert!(body.contains("peryx_replication_blobs_pending 3\n"), "{body}");

        // The fetched counter accumulates across passes while the pending gauge takes the latest value.
        monitor.record_blobs(BlobPlaneReport { fetched: 1, pending: 0 });
        let mut body = String::new();
        monitor.write_metrics(&mut body);
        assert!(body.contains("peryx_replication_blobs_fetched_total 3\n"), "{body}");
        assert!(body.contains("peryx_replication_blobs_pending 0\n"), "{body}");
    }
}

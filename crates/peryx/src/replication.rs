//! Process-level replication configuration and follower scheduling.

mod availability_metrics;
mod worker;

use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fmt::Write as _, sync::Mutex};

use anyhow::Context as _;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse as _, Response};
use axum::routing::get;
use axum::{Json, Router};
use peryx_driver::{AppState, PrometheusSource};
use peryx_http::handlers::status_authorization;
use peryx_http::response_security::{
    ClassifiedField, FieldClassification, ProtectedCachePolicy, ResponseAuthorization, filter_fields,
};
use peryx_replication::{
    CapacityLimited, DEFAULT_DEAD_AFTER, DEFAULT_SUSPECT_AFTER, HttpBlobTransport, HttpPrimary, LivenessTracker,
    Replica, SyncError, SyncOutcome, TransferLimits, advance_blob_frontier, liveness_router, primary_router,
    pull_outstanding,
};
use peryx_storage::blob::BlobStorage;
use peryx_storage::meta::MetaStore;
use peryx_upstream::redact_url;
use serde_json::{Value, json};

use crate::config::{AvailabilityConfig, Config, DcRole, ReplicationConfig};
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
    blobs: u64,
    errors: u64,
    readable_serial: u64,
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
                blobs: 0,
                errors: 0,
                readable_serial: 0,
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
        observation.blobs = observation
            .blobs
            .saturating_add(u64::try_from(outcome.blobs).unwrap_or(u64::MAX));
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
             # HELP peryx_replication_blobs_total Blobs fetched by the replica.\n\
             # TYPE peryx_replication_blobs_total counter\n\
             peryx_replication_blobs_total {}\n\
             # HELP peryx_replication_sync_errors_total Replica synchronization failures.\n\
             # TYPE peryx_replication_sync_errors_total counter\n\
             peryx_replication_sync_errors_total {}\n",
            observation.serial, observation.changes, observation.blobs, observation.errors
        );
        let _ = write!(
            body,
            "# HELP peryx_replication_readable_serial Highest serial every required derived view has applied.\n\
             # TYPE peryx_replication_readable_serial gauge\n\
             peryx_replication_readable_serial {}\n",
            observation.readable_serial
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
                ClassifiedField::new("synced_blobs", FieldClassification::Operator, json!(observation.blobs)),
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

/// Which planes a replica cycle drives. A unified replica commits metadata and blobs together in
/// [`Replica::sync_once`]; a dual-plane replica commits metadata first, then pulls whole blobs on an
/// independent frontier so a lagging blob never stalls metadata.
enum ReplicaMode {
    Unified,
    Dual {
        transport: CapacityLimited<HttpBlobTransport>,
    },
}

struct ReplicaLoop {
    app: Arc<AppState>,
    primary: HttpPrimary,
    meta: MetaStore,
    blobs: BlobStorage,
    page_size: std::num::NonZeroUsize,
    poll_interval: Duration,
    monitor: Arc<ReplicaMonitor>,
    metrics: Arc<AvailabilityMetrics>,
    mode: ReplicaMode,
}

fn log_replica_page(outcome: SyncOutcome) {
    tracing::info!(
        changes = outcome.changes,
        blobs = outcome.blobs,
        serial = outcome.serial,
        primary_serial = outcome.primary_serial,
        "replica page applied"
    );
}

/// Apply the effects of one replicated page: log it, refresh the search view, and hand the changed keys
/// to every ecosystem driver so each retires the derived views those keys touch. A page with no changes
/// does nothing. This is synchronous and independent of the async sync loop, so a direct test covers it
/// deterministically rather than riding on async scheduling.
pub(crate) fn apply_replicated_page(app: &AppState, outcome: SyncOutcome, changed_keys: &[String]) {
    if outcome.changes > 0 {
        log_replica_page(outcome);
        app.bump_search_epoch();
        let state = app.serving.as_ref();
        for driver in app.drivers() {
            driver.apply_replicated_changes(state, changed_keys);
        }
    }
}

impl ReplicaLoop {
    async fn run(self) {
        loop {
            if self.cycle().await {
                tokio::time::sleep(self.poll_interval).await;
            }
        }
    }

    async fn cycle(&self) -> bool {
        match &self.mode {
            ReplicaMode::Unified => self.unified_cycle().await,
            ReplicaMode::Dual { transport } => self.dual_cycle(transport).await,
        }
    }

    async fn unified_cycle(&self) -> bool {
        let started = Instant::now();
        let result = Replica::new(&self.meta, &self.blobs, self.page_size)
            .sync_once(&self.primary)
            .await;
        let elapsed = started.elapsed();
        match result {
            Ok((outcome, changed_keys)) => {
                apply_replicated_page(&self.app, outcome, &changed_keys);
                self.monitor.record(outcome);
                let readable = self.app.readable_frontier().map_or(0, |frontier| frontier.serial);
                self.monitor.record_readable(readable);
                self.metrics.record_cycle(outcome, elapsed);
                outcome.caught_up()
            }
            Err(error) => {
                self.monitor.record_error(&error);
                self.metrics.record_error(&error, elapsed);
                tracing::error!(%error, "replica synchronization failed");
                true
            }
        }
    }

    /// Drive both planes for one pass. Metadata commits and its search view advances first, so a blob
    /// still in flight never holds up metadata; the blob plane then pulls the tail's outstanding bytes
    /// and moves the blob frontier only over serials whose blobs are all local. The readable frontier
    /// the loop records is the slower of the two views, so reads never outrun the bytes they name. A
    /// blob loss records and retries — the metadata plane keeps advancing regardless.
    async fn dual_cycle(&self, transport: &CapacityLimited<HttpBlobTransport>) -> bool {
        let started = Instant::now();
        let result = Replica::new(&self.meta, &self.blobs, self.page_size)
            .sync_metadata(&self.primary)
            .await;
        let elapsed = started.elapsed();
        let (outcome, changed_keys) = match result {
            Ok((outcome, changed_keys, _referenced)) => (outcome, changed_keys),
            Err(error) => {
                self.monitor.record_error(&error);
                self.metrics.record_error(&error, elapsed);
                tracing::error!(%error, "replica metadata synchronization failed");
                return true;
            }
        };
        apply_replicated_page(&self.app, outcome, &changed_keys);
        if let Err(error) = self.pull_blobs(transport).await {
            self.monitor.record_error(&error);
            self.metrics.record_error(&error, elapsed);
            tracing::error!(%error, "replica blob plane failed");
        }
        self.monitor.record(outcome);
        let readable = self.app.readable_frontier().map_or(0, |frontier| frontier.serial);
        self.monitor.record_readable(readable);
        self.metrics.record_cycle(outcome, elapsed);
        outcome.caught_up()
    }

    async fn pull_blobs(&self, transport: &CapacityLimited<HttpBlobTransport>) -> Result<(), SyncError> {
        pull_outstanding(
            transport,
            &self.meta,
            &self.blobs,
            self.page_size,
            BLOB_FETCH_CONCURRENCY,
        )
        .await?;
        advance_blob_frontier(&self.meta, &self.blobs, self.page_size).await?;
        Ok(())
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
                dual_plane,
            }) => {
                let token = token.read().context("read the replica replication token")?;
                let replica_mode = if *dual_plane {
                    let transport =
                        HttpBlobTransport::new(upstream, token.clone(), TransferLimits::default(), BLOB_FETCH_TIMEOUT)
                            .context("build replica blob transport")?;
                    ReplicaMode::Dual {
                        transport: CapacityLimited::new(transport, BLOB_FETCH_CONCURRENCY),
                    }
                } else {
                    ReplicaMode::Unified
                };
                let primary = HttpPrimary::new(upstream, token).context("build replica HTTP client")?;
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
                            primary,
                            meta: state.serving.meta.clone(),
                            blobs: state.serving.blobs.clone(),
                            page_size: *page_size,
                            poll_interval: *poll_interval,
                            monitor,
                            metrics,
                            mode: replica_mode,
                        },
                        runtime,
                    )),
                    Some(node),
                )
            }
        };
        Ok(Self {
            primary,
            replica,
            availability,
        })
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
        Some(runtime)
    }

    #[cfg(test)]
    pub(crate) async fn sync_cycle(&self) -> Option<bool> {
        match &self.replica {
            Some((replica, _)) => Some(replica.cycle().await),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{WorkerShared, worker_reason};

    #[test]
    fn test_worker_reason_names_only_a_failed_domain() {
        assert_eq!(worker_reason(None), None);
        let healthy = Arc::new(WorkerShared::for_replica());
        assert_eq!(worker_reason(Some(&healthy)), None);
        let failed = Arc::new(WorkerShared::for_replica());
        failed.record_panic();
        assert_eq!(worker_reason(Some(&failed)), Some("worker_unhealthy"));
    }
}

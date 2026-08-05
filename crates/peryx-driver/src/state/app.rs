//! The shared application state and its request-time index routing.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use peryx_core::{Ecosystem, LexiconRegistry};
use peryx_storage::blob::BlobStorage;
use peryx_storage::meta::MetaStore;
use peryx_upstream::UpstreamRouter;

use peryx_index::{Index, RouteResolver};

use super::describe::{IndexDescription, describe_indexes, describe_upstream_route};
use crate::authz::AuthorizationService;
use crate::jobs::JobAttemptControl;
use crate::rate_limit::{RateLimiter, UpstreamLimits};
use crate::revocations::RevocationService;
use crate::tokens::TokenService;
use crate::users::UserService;
use peryx_events::metrics::Metrics;
use peryx_events::webhook::WebhookRuntime;
use peryx_search::PackageSearch;

/// A source of the current unix time, injectable so cache-freshness logic is deterministic in
/// tests.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// A process-level component that contributes Prometheus text exposition.
pub trait PrometheusSource: Send + Sync {
    /// Append complete metric families to `body`.
    fn write_metrics(&self, body: &mut String);
}

/// Everything a serving handler needs, and nothing about *which* ecosystems are installed.
///
/// An ecosystem driver receives an `Arc<ServingState>`, so it can read the stores, the caches and the
/// configured indexes and spawn background work over them — but it holds no driver registry, so it
/// cannot reach another ecosystem's driver or enumerate them. The registry lives one level up on
/// [`AppState`], which the router and rate limiter hold; a driver reaching for it is a compile error,
/// not a convention.
pub struct ServingState {
    pub meta: MetaStore,
    /// Shared password worker bound and persistent user operations.
    pub users: UserService,
    /// Persistent server-role authorization.
    pub authorization: AuthorizationService,
    /// Digest revocation lifecycle and serving decisions.
    pub revocations: RevocationService,
    /// Scoped API token lifecycle: create, list, inspect, rotate, revoke, and verification.
    pub tokens: TokenService,
    /// Durable attempt state shared by the scheduler and management handlers.
    pub job_attempts: JobAttemptControl,
    pub blobs: BlobStorage,
    /// Fallback freshness for cached simple pages, in seconds: applies only when upstream's
    /// `Cache-Control` granted no usable lifetime.
    pub ttl_secs: i64,
    /// The bound on stale-on-error serving; see [`RuntimeOptions::max_stale_secs`].
    pub max_stale_secs: i64,
    pub clock: Clock,
    pub requests: AtomicU64,
    /// Datacenter durability acknowledgement observability. Every process holds one; only a `dc` or `ha`
    /// node registers it as a Prometheus source, so its series appear only where the decision is real.
    pub dc_durability: Arc<crate::state::DcDurabilityMetrics>,
    /// Whether this process serves as a replica and rejects client mutations.
    pub read_only: bool,
    /// The authority role this node holds, derived from its configured replication role rather than its
    /// read-only posture. A configured primary is a [`Writer`](peryx_core::NodeRole::Writer) even when it
    /// serves read-only, so the topology snapshot agrees with the replication and control surfaces on
    /// which node writes. Single-node `none` mode is a lone writer.
    pub(super) availability_role: peryx_core::NodeRole,
    /// The fixed availability topology this process was configured with, projected per request into a
    /// role-filtered snapshot. Single-node `none` mode holds an empty roster.
    pub(super) availability_topology: peryx_core::TopologyConfig,
    /// Immutable repository-route positions for request dispatch.
    pub(super) route_resolver: RouteResolver,
    pub indexes: Vec<Index>,
    /// The role engine's caches for a cached (proxy) index: the single-flight map, the transformed-page
    /// cache, the negative cache, and the mutation epoch that retires them.
    pub cache: peryx_index::ServingCache,
    /// One live download per blob digest: concurrent cold requests for the same file all tail the
    /// one upstream transfer as it lands instead of waiting for it to finish.
    pub downloads: crate::download::DownloadRegistry,
    /// Off-thread usage aggregation: index → project → file counters for the dashboard.
    pub metrics: Metrics,
    /// Derived package search index, refreshed from storage when the mutation epoch advances.
    pub search: PackageSearch,
    /// The views a read must wait for before this process exposes a serial. A single-node or
    /// metadata-only replica requires only the search view; a replica running the blob plane adds its
    /// blob view so a metadata commit stays hidden until the referenced bytes are present. Held per
    /// instance rather than as a constant because a node that never advances the blob view must not
    /// gate every read on it.
    pub required_views: std::sync::Arc<[&'static str]>,
    /// Per-client HTTP request limits. The bucket cache has a fixed capacity.
    pub rate_limits: RateLimiter,
    /// Per-cached-index upstream fetch gates, keyed by configured index name.
    pub upstream_limits: UpstreamLimits,
    /// Independent gates for mutable artifact metadata, so its latency cannot consume page-fetch slots.
    pub metadata_upstream_limits: UpstreamLimits,
    /// Multi-source routes keyed by cached index name. Legacy cached indexes are absent.
    pub upstream_routes: HashMap<String, UpstreamRouter>,
    /// Signed webhook delivery runtime.
    pub webhooks: WebhookRuntime,
    /// The token realm's signing key, or `None` when no signing key is configured. Without it an
    /// ecosystem's token endpoint cannot mint a JWT, so an OCI index falls back to Basic-only auth and
    /// never challenges with the Bearer scheme.
    pub signer: Option<peryx_identity::Signer>,
    /// How long a token the realm mints stays valid, in seconds.
    pub token_ttl_secs: i64,
    /// CI identity exchange runtime. Absent means the OIDC endpoints stay disabled and no issuer
    /// client or replay state exists.
    pub trusted_publishing: Option<Arc<dyn peryx_identity::IdentityExchange>>,
    /// Named LDAP login services. Authentication routes can select one without knowing its bind mode.
    pub(super) ldap_logins: HashMap<String, Arc<peryx_identity::LdapLoginService<MetaStore>>>,
    /// Per-repository concurrency bound on retention-plan previews, so one repository's full-scan
    /// previews cannot starve the rest.
    pub retention_gates: crate::retention::RetentionGates,
    /// Named browser OIDC login services. The login and callback routes select one by provider ID.
    pub(super) oidc_logins: HashMap<String, Arc<peryx_identity::OidcLoginService<MetaStore>>>,
    /// Seals the browser session and login-handoff cookies. Present only when a token-realm signing key
    /// is configured, since the sealing key derives from it.
    pub(super) session_sealer: Option<Arc<peryx_identity::SessionSealer>>,
    /// The ownership consensus group this writer submits first-publish home claims to, registered once
    /// the async runtime has ignited it. Absent when the process runs no group, so the mutation path
    /// skips the claim.
    pub(super) ownership: std::sync::OnceLock<Arc<dyn crate::state::OwnershipAuthority>>,
    /// The availability control plane the administrator command surface submits membership and transfer
    /// commands through, registered once the runtime ignites the consensus group. Absent when the process
    /// runs no group, so the command surface has nothing to drive.
    pub(super) control: std::sync::OnceLock<Arc<crate::state::ControlPlane>>,
    /// The cross-data-center blob copier the scheduled `DcCopy` job drives, registered once the binary
    /// has resolved its roster and replication token. Absent when the process copies nothing — single
    /// node, no roster, or an object-store backend — so the job runs as a no-op.
    pub(super) cross_dc_copier: std::sync::OnceLock<Arc<dyn crate::jobs::CrossDcCopier>>,
}

/// The whole process state: the serving data every handler needs, plus the driver registry only the
/// router and rate limiter reach.
///
/// Shared as `Arc<AppState>`; it [`Deref`](std::ops::Deref)s to [`ServingState`], so `app.meta` and
/// the rest read through unchanged.
pub struct AppState {
    /// The serving data, separately `Arc`-shared so a driver receives it without the registry and
    /// background tasks can own a clone.
    pub serving: Arc<ServingState>,
    /// The ecosystem serving drivers, one slot per [`Ecosystem`]. A request is dispatched to the driver
    /// of the index it resolved to (or of the absolute prefix it fell under), so several ecosystems
    /// coexist; a slot stays `None` for an ecosystem nobody installed. Each driver's
    /// [`mount`](crate::serving::EcosystemDriver::mount) tells the router and rate limiter how to reach
    /// it, so neither names an ecosystem.
    pub(super) drivers: [Option<Arc<dyn crate::serving::EcosystemDriver>>; Ecosystem::COUNT],
    /// The absolute top-level prefixes of the [`Absolute`](crate::serving::RouteMount::Absolute)-mount
    /// drivers, each paired with its slot, precomputed at registration. The rate limiter classifies a
    /// request through this on every call, so it must not walk every driver and dispatch `mount()`
    /// dynamically: this list holds only the few absolute prefixes, whatever the ecosystem count.
    pub(super) absolute_prefixes: Vec<(&'static str, usize)>,
    /// Each ecosystem's user-facing vocabulary, registered by its driver at install time so surfaces
    /// localize a label by an index's ecosystem without the neutral core naming any ecosystem's words.
    pub(super) lexicons: LexiconRegistry,
    /// The `OpenAPI` document served at `/api-docs/openapi.json`. The binary assembles it from each
    /// ecosystem driver's paths at startup and installs it here, so this neutral crate carries no
    /// format-specific API description, only a minimal stub until the binary sets the real one.
    pub(super) openapi: std::sync::Arc<str>,
    pub(super) prometheus: Mutex<Vec<Arc<dyn PrometheusSource>>>,
}

impl AppState {
    /// Register process metrics that are not owned by an ecosystem driver.
    pub fn register_prometheus(&self, source: Arc<dyn PrometheusSource>) {
        self.prometheus
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(source);
    }

    /// Append every registered process metric family to `body`.
    pub fn write_process_metrics(&self, body: &mut String) {
        for source in self
            .prometheus
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            source.write_metrics(body);
        }
    }
}

impl std::ops::Deref for AppState {
    type Target = ServingState;

    fn deref(&self) -> &ServingState {
        &self.serving
    }
}

impl std::ops::DerefMut for AppState {
    /// Mutable access to the serving state, sound only while its `Arc` is uniquely owned — during
    /// build and install, before any handler holds a clone. The router shares the state afterwards,
    /// so a mutation then is a bug, and this panics rather than silently splitting the state.
    fn deref_mut(&mut self) -> &mut ServingState {
        Arc::get_mut(&mut self.serving).expect("serving state is mutated only before it is served")
    }
}

impl ServingState {
    /// Whether the local stores and process role permit the requested traffic class.
    #[must_use]
    pub async fn is_ready(&self, writes: bool) -> bool {
        self.meta.current_serial().is_ok() && self.blobs.health().await.is_ok() && (!writes || !self.read_only)
    }

    /// The fixed availability topology this process serves a role-filtered snapshot from.
    #[must_use]
    pub const fn availability_topology(&self) -> &peryx_core::TopologyConfig {
        &self.availability_topology
    }

    /// The authority role this node was configured with, from its replication role rather than its
    /// read-only posture, so a read-only primary still reports itself as the writer.
    #[must_use]
    pub const fn availability_role(&self) -> peryx_core::NodeRole {
        self.availability_role
    }

    /// Register the ownership consensus group once the runtime ignites it, so the mutation path can
    /// submit first-publish home claims. Set at most once; a later call is ignored.
    pub fn set_ownership_authority(&self, authority: Arc<dyn crate::state::OwnershipAuthority>) {
        let _ = self.ownership.set(authority);
    }

    /// The ownership consensus group, or `None` when this process runs no group and assigns no homes.
    #[must_use]
    pub fn ownership_authority(&self) -> Option<&Arc<dyn crate::state::OwnershipAuthority>> {
        self.ownership.get()
    }

    /// Register the availability control plane once the runtime ignites the consensus group, so the
    /// administrator command surface can submit membership and transfer commands. Set at most once; a
    /// later call is ignored.
    pub fn set_control_plane(&self, control: Arc<crate::state::ControlPlane>) {
        let _ = self.control.set(control);
    }

    /// The availability control plane, or `None` when this process runs no consensus group and exposes no
    /// command surface.
    #[must_use]
    pub fn control_plane(&self) -> Option<&Arc<crate::state::ControlPlane>> {
        self.control.get()
    }

    /// Register the cross-data-center blob copier the scheduled `DcCopy` job drives. Set at most once; a
    /// later call is ignored.
    pub fn set_cross_dc_copier(&self, copier: Arc<dyn crate::jobs::CrossDcCopier>) {
        let _ = self.cross_dc_copier.set(copier);
    }

    /// The registered cross-data-center blob copier, or `None` when this process copies nothing.
    #[must_use]
    pub fn cross_dc_copier(&self) -> Option<&Arc<dyn crate::jobs::CrossDcCopier>> {
        self.cross_dc_copier.get()
    }

    /// Assign `authority`'s home on its first publish, best effort. A publish path calls this after it
    /// commits a new project or repository; it claims a home only when a group runs and the authority has
    /// none yet, and never blocks the publish on the outcome.
    pub async fn claim_first_publish_home(&self, authority: &str) {
        crate::state::ownership::claim_first_publish_home(self.ownership.get(), authority).await;
    }

    /// The committed authority epoch for `authority`, the fence value a writer stamps onto work it
    /// produces so a stale-epoch write is fenced out. `0` when this process runs no consensus group,
    /// which the placement fence reads as the closed, unassigned sentinel.
    pub async fn committed_authority_epoch(&self, authority: &str) -> u64 {
        crate::state::ownership::committed_authority_epoch(self.ownership.get(), authority).await
    }

    /// Whether background work carrying `presented` under `authority` may still be written, or is fenced
    /// as a stale-epoch writer that the authority superseded. A process running no consensus group has no
    /// authority to supersede its work, so it admits everything.
    pub async fn admit_authority_epoch(&self, authority: &str, presented: u64) -> bool {
        crate::state::ownership::admit_authority_epoch(self.ownership.get(), authority, presented).await
    }

    /// Move `authority`'s home to `new_home` on the control quorum, minting the epoch that fences the old
    /// home. Reports the committed [`TransferOutcome`](crate::state::TransferOutcome), or `None` when this
    /// process runs no group or the move was a no-op. A control minority surfaces as
    /// [`OwnershipError::NotLeader`](crate::state::OwnershipError::NotLeader).
    ///
    /// # Errors
    /// The [`OwnershipError`](crate::state::OwnershipError) the commit failed with.
    pub async fn transfer_authority_home(
        &self,
        authority: &str,
        new_home: &str,
    ) -> Result<Option<crate::state::TransferOutcome>, crate::state::OwnershipError> {
        crate::state::ownership::transfer_authority_home(self.ownership.get(), authority, new_home).await
    }

    /// Find the index whose route is the longest segment-aligned prefix of `path` (which has no
    /// leading slash), and the path remainder after `route/`. Returns `None` if no route matches.
    #[must_use]
    pub fn resolve<'a>(&'a self, path: &'a str) -> Option<(&'a Index, &'a str)> {
        self.resolve_position(path)
            .map(|(position, rest)| (&self.indexes[position], rest))
    }

    /// Like [`Self::resolve`], returning the index position instead of a borrow.
    #[must_use]
    pub fn resolve_position<'a>(&self, path: &'a str) -> Option<(usize, &'a str)> {
        self.route_resolver.resolve(path)
    }

    /// The index at position `pos` (a virtual-index layer or upload target).
    #[must_use]
    pub fn index_at(&self, pos: usize) -> &Index {
        &self.indexes[pos]
    }

    /// Describe every configured index for presentation: kind name, virtual-index layer names, upload
    /// access, and delete policy. Shared by `/+status` and the web UI.
    #[must_use]
    pub fn describe_indexes(&self) -> Vec<IndexDescription> {
        let mut descriptions = describe_indexes(&self.indexes);
        for description in &mut descriptions {
            if let (Some(router), Some(upstream)) = (
                self.upstream_routes.get(&description.name),
                description.upstream.as_mut(),
            ) {
                (upstream.status, upstream.sources) = describe_upstream_route(router);
            }
        }
        descriptions
    }

    /// Find a configured LDAP login service by its operator-defined provider ID.
    #[must_use]
    pub fn ldap_login(&self, provider: &str) -> Option<&peryx_identity::LdapLoginService<MetaStore>> {
        self.ldap_logins.get(provider).map(AsRef::as_ref)
    }

    /// Find a configured browser OIDC login service by its operator-defined provider ID.
    #[must_use]
    pub fn oidc_login(&self, provider: &str) -> Option<&peryx_identity::OidcLoginService<MetaStore>> {
        self.oidc_logins.get(provider).map(AsRef::as_ref)
    }

    /// The configured browser OIDC provider IDs, sorted, for the login surface to list.
    #[must_use]
    pub fn oidc_providers(&self) -> Vec<&str> {
        let mut providers = self.oidc_logins.keys().map(String::as_str).collect::<Vec<_>>();
        providers.sort_unstable();
        providers
    }

    /// The sealer for browser session and login-handoff cookies, present when a token-realm signing key
    /// is configured.
    #[must_use]
    pub fn session_sealer(&self) -> Option<&peryx_identity::SessionSealer> {
        self.session_sealer.as_deref()
    }
}

/// Signed webhook delivery borrows exactly three things from the process — the configured targets,
/// the queue's store, and the clock — and reaches them through this trait rather than the whole state.
impl peryx_events::webhook::WebhookHost for ServingState {
    fn webhooks(&self) -> &WebhookRuntime {
        &self.webhooks
    }

    fn meta(&self) -> &MetaStore {
        &self.meta
    }

    fn now(&self) -> i64 {
        (self.clock)()
    }
}

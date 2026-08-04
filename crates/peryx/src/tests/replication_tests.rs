use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::routing::get as route_get;
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use http_body_util::BodyExt as _;
use peryx_driver::IndexKind as RuntimeIndexKind;
use peryx_driver::state::AppState;
use peryx_identity::{Action, GrantScope, Role};
use peryx_replication::{BLOB_VIEW, ChangePage, SyncOutcome, primary_router};
use peryx_storage::blob::{BlobStore, Digest};
use peryx_storage::meta::MetaStore;
use rstest::rstest;
use tower::ServiceExt as _;

use crate::config::{
    AvailabilityConfig, Config, DcMember, DcMembership, DcRole, IndexKind, ReplicationConfig, SecretSource,
    TokenConfig, UpstreamConfig, UpstreamRoutingConfig, WebhookConfig, WebhookSecret,
};
use crate::replication::ReplicationRuntime;
use crate::server::{build_router, build_state, router_for};

const TOKEN: &str = "replica-secret";
const WRITER_IDENTITY: &str = "writer-a";

struct TestServer {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start(router: Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            url: format!("http://{address}/"),
            task,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn config(dir: &tempfile::TempDir, replication: Option<ReplicationConfig>) -> Config {
    let replica = matches!(replication, Some(ReplicationConfig::Replica { .. }));
    if replica {
        MetaStore::open(dir.path().join("peryx.redb"))
            .unwrap()
            .claim_writer_identity(WRITER_IDENTITY)
            .unwrap();
    }
    Config {
        data_dir: dir.path().to_path_buf(),
        writer_identity: replica.then(|| WRITER_IDENTITY.to_owned()),
        availability: replication.map_or(AvailabilityConfig::None, AvailabilityConfig::Dc),
        ..Config::default()
    }
}

fn replica_config(upstream: &str, page_size: usize) -> ReplicationConfig {
    ReplicationConfig::Replica {
        upstream: upstream.to_owned(),
        token: SecretSource::Literal(TOKEN.to_owned()),
        poll_interval: Duration::from_millis(1),
        page_size: NonZeroUsize::new(page_size).unwrap(),
        dual_plane: false,
    }
}

fn dual_replica_config(upstream: &str, page_size: usize) -> ReplicationConfig {
    let ReplicationConfig::Replica {
        upstream,
        token,
        poll_interval,
        page_size,
        ..
    } = replica_config(upstream, page_size)
    else {
        unreachable!("replica_config builds a replica")
    };
    ReplicationConfig::Replica {
        upstream,
        token,
        poll_interval,
        page_size,
        dual_plane: true,
    }
}

fn primary_config() -> ReplicationConfig {
    ReplicationConfig::Primary {
        source: "primary-a".to_owned(),
        token: SecretSource::Literal(TOKEN.to_owned()),
    }
}

#[tokio::test]
async fn test_build_state_projects_the_configured_dc_topology() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        data_dir: dir.path().to_path_buf(),
        writer_identity: Some(WRITER_IDENTITY.to_owned()),
        availability: AvailabilityConfig::Dc(primary_config()),
        dc_membership: Some(DcMembership {
            group: "east".to_owned(),
            members: vec![
                DcMember {
                    node: WRITER_IDENTITY.to_owned(),
                    dc: "east-1".to_owned(),
                    address: "10.0.0.1:8080".to_owned(),
                    role: DcRole::Writer,
                },
                DcMember {
                    node: "replica-b".to_owned(),
                    dc: "east-2".to_owned(),
                    address: "10.0.0.2:8080".to_owned(),
                    role: DcRole::Replica,
                },
            ],
        }),
        ..Config::default()
    };

    let topology = build_state(&config).unwrap().availability_topology().clone();

    assert_eq!(topology.mode, peryx_core::TopologyMode::Dc);
    assert_eq!(topology.group.as_deref(), Some("east"));
    assert_eq!(topology.local_node.as_deref(), Some(WRITER_IDENTITY));
    let roles = topology
        .members
        .iter()
        .map(|member| (member.node.as_str(), member.role))
        .collect::<Vec<_>>();
    assert_eq!(
        roles,
        vec![
            (WRITER_IDENTITY, peryx_core::NodeRole::Writer),
            ("replica-b", peryx_core::NodeRole::Replica),
        ],
    );
    assert_eq!(topology.members[0].address, "10.0.0.1:8080");
}

#[tokio::test]
async fn test_build_state_derives_the_writer_role_for_a_read_only_primary() {
    let dir = tempfile::tempdir().unwrap();
    MetaStore::open(dir.path().join("peryx.redb"))
        .unwrap()
        .claim_writer_identity(WRITER_IDENTITY)
        .unwrap();
    let config = Config {
        data_dir: dir.path().to_path_buf(),
        writer_identity: Some(WRITER_IDENTITY.to_owned()),
        availability: AvailabilityConfig::Dc(primary_config()),
        read_only: true,
        ..Config::default()
    };

    let state = build_state(&config).unwrap();

    assert!(state.read_only, "the primary is configured read-only");
    assert_eq!(
        state.availability_role(),
        peryx_core::NodeRole::Writer,
        "a read-only primary holds write authority, so the topology self-role agrees with the \
         listener and replication surfaces that read it as the writer",
    );
}

#[tokio::test]
async fn test_build_state_derives_the_replica_role_for_a_configured_replica() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, Some(replica_config("http://primary-a/", 16)));

    let state = build_state(&config).unwrap();

    assert_eq!(state.availability_role(), peryx_core::NodeRole::Replica);
}

fn primary_stores() -> (tempfile::TempDir, MetaStore, BlobStore) {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    let blobs = BlobStore::new(dir.path().join("blobs"));
    (dir, meta, blobs)
}

async fn get(router: &Router, path: &str) -> (StatusCode, Vec<u8>) {
    get_as(router, path, None).await
}

async fn get_as(router: &Router, path: &str, credentials: Option<&str>) -> (StatusCode, Vec<u8>) {
    let mut request = Request::get(path);
    if let Some(credentials) = credentials {
        request = request.header(header::AUTHORIZATION, format!("Basic {}", STANDARD.encode(credentials)));
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    (status, body)
}

async fn document(router: &Router, path: &str, credentials: Option<&str>) -> (StatusCode, serde_json::Value) {
    let (status, body) = get_as(router, path, credentials).await;
    (status, serde_json::from_slice(&body).unwrap())
}

const PASSWORD: &str = "local availability password";

async fn credential(state: &AppState, name: &str, role: Role) -> String {
    let user = state.users.create(name).unwrap();
    state.users.set_password(&user.id, PASSWORD).await.unwrap();
    state.authorization.grant(&user.id, role, GrantScope::Server).unwrap();
    format!("{name}:{PASSWORD}")
}

/// A stand-in primary that always answers `changes` with a page tagged an unsupported protocol
/// version, so a replica polling it records a schema fault rather than a transport failure.
async fn incompatible_primary() -> TestServer {
    let page = ChangePage {
        version: u16::MAX,
        source: "primary-a".to_owned(),
        after: 0,
        current_serial: 1,
        changes: Vec::new(),
    };
    let handler = route_get(move || {
        let page = page.clone();
        async move { Json(page) }
    });
    TestServer::start(Router::new().route("/+replication/v1/changes", handler)).await
}

#[tokio::test]
async fn test_primary_runtime_mounts_authenticated_routes() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, Some(primary_config()));
    let router = build_router(&config).unwrap();

    let response = router
        .oneshot(
            Request::get("/+replication/v1/changes?after=0&limit=10")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let page = serde_json::from_slice::<ChangePage>(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(page.source, "primary-a");
}

#[tokio::test]
async fn test_replica_runtime_drains_available_pages() {
    let (_primary_dir, primary_meta, primary_blobs) = primary_stores();
    primary_meta
        .commit_driver_txn(|_| {
            Ok::<_, peryx_storage::meta::MetaError>(((), vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]))
        })
        .unwrap();
    let server = TestServer::start(primary_router("primary-a", TOKEN, primary_meta, primary_blobs).unwrap()).await;
    let replica_dir = tempfile::tempdir().unwrap();
    let config = config(&replica_dir, Some(replica_config(&server.url, 1)));
    let state = build_state(&config).unwrap();
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();

    assert!(runtime.is_replica());
    let subscriber = tracing_subscriber::fmt().with_writer(std::io::sink).finish();
    let guard = tracing::subscriber::set_default(subscriber);
    assert_eq!(runtime.sync_cycle().await, Some(false));
    drop(guard);

    let router = runtime.mount(router_for(state.clone()));
    let availability = runtime.start().unwrap();
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        if get(&router, "/+replication/v1/ready").await.0 == StatusCode::OK {
            break;
        }
        tokio::select! {
            () = &mut deadline => panic!(
                "replica runtime did not drain pages; current serial is {}",
                state.meta.current_serial().unwrap()
            ),
            () = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    drop(availability);

    assert_eq!(state.meta.journal_after(0, 10).unwrap().len(), 3);
}

#[tokio::test]
async fn test_replica_runtime_copies_primary_metadata() {
    let (_primary_dir, primary_meta, primary_blobs) = primary_stores();
    primary_meta
        .commit_driver_txn(|txn| {
            txn.put("pypi\0upload", b"record")?;
            Ok::<_, peryx_storage::meta::MetaError>(((), vec![b"upload".to_vec()]))
        })
        .unwrap();
    let server = TestServer::start(primary_router("primary-a", TOKEN, primary_meta, primary_blobs).unwrap()).await;
    let replica_dir = tempfile::tempdir().unwrap();
    let config = config(&replica_dir, Some(replica_config(&server.url, 10)));
    let state = build_state(&config).unwrap();
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();

    assert_eq!(runtime.sync_cycle().await, Some(true));
    assert_eq!(
        state.meta.get_driver_value("pypi\0upload").unwrap().as_deref(),
        Some(b"record".as_slice())
    );
}

#[tokio::test]
async fn test_replica_dispatches_applied_keys_to_ecosystem_drivers() {
    let (_primary_dir, primary_meta, primary_blobs) = primary_stores();
    primary_meta
        .commit_driver_txn(|txn| {
            txn.put("pypi\0p\0hosted/flask", b"Flask")?;
            Ok::<_, peryx_storage::meta::MetaError>(((), vec![b"upload".to_vec()]))
        })
        .unwrap();
    let server = TestServer::start(primary_router("primary-a", TOKEN, primary_meta, primary_blobs).unwrap()).await;
    let replica_dir = tempfile::tempdir().unwrap();
    let config = config(&replica_dir, Some(replica_config(&server.url, 10)));
    let state = build_state(&config).unwrap();
    // A cached hosted page whose project the applied marker names, so the driver's invalidation shows.
    let hot = state.hot_key("hosted", "flask", "simple.html");
    state
        .cache
        .store_hot(hot.clone(), axum::body::Bytes::from_static(b"x"), i64::MAX);
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();

    assert_eq!(runtime.sync_cycle().await, Some(true));

    // The replica handed the applied project key to the PyPI driver, which retired flask's pages by
    // advancing their epoch; the OCI driver's default hook ignored the key.
    assert_ne!(
        state.hot_key("hosted", "flask", "simple.html"),
        hot,
        "the replica dispatched the change to the ecosystem driver"
    );
}

#[test]
fn test_apply_replicated_page_dispatches_a_changed_page_to_drivers() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, None);
    let state = build_state(&config).unwrap();
    let hot = state.hot_key("hosted", "flask", "simple.html");
    state
        .cache
        .store_hot(hot.clone(), axum::body::Bytes::from_static(b"x"), i64::MAX);

    // Synchronous and independent of the async sync loop, so this covers the dispatch every run.
    crate::replication::apply_replicated_page(
        &state,
        SyncOutcome {
            changes: 1,
            blobs: 0,
            serial: 1,
            primary_serial: 1,
        },
        &["pypi\u{0}p\u{0}hosted/flask".to_owned()],
    );

    assert_ne!(
        state.hot_key("hosted", "flask", "simple.html"),
        hot,
        "a page with changes reached the PyPI driver, which retired flask's pages",
    );
}

#[test]
fn test_apply_replicated_page_ignores_a_page_with_no_changes() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, None);
    let state = build_state(&config).unwrap();
    let hot = state.hot_key("hosted", "flask", "simple.html");
    state
        .cache
        .store_hot(hot.clone(), axum::body::Bytes::from_static(b"x"), i64::MAX);

    crate::replication::apply_replicated_page(
        &state,
        SyncOutcome {
            changes: 0,
            blobs: 0,
            serial: 0,
            primary_serial: 0,
        },
        &["pypi\u{0}p\u{0}hosted/flask".to_owned()],
    );

    assert_eq!(
        state.hot_key("hosted", "flask", "simple.html"),
        hot,
        "a page with no changes dispatched nothing",
    );
}

#[tokio::test]
async fn test_replica_runtime_copies_primary_blobs() {
    let (_primary_dir, primary_meta, primary_blobs) = primary_stores();
    let digest = primary_blobs.write(b"artifact").unwrap();
    primary_meta
        .commit_driver_txn(|txn| {
            txn.reference_blob(digest.as_str(), 8);
            Ok::<_, peryx_storage::meta::MetaError>(((), vec![b"upload".to_vec()]))
        })
        .unwrap();
    let server = TestServer::start(primary_router("primary-a", TOKEN, primary_meta, primary_blobs).unwrap()).await;
    let replica_dir = tempfile::tempdir().unwrap();
    let config = config(&replica_dir, Some(replica_config(&server.url, 10)));
    let state = build_state(&config).unwrap();
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();

    assert_eq!(runtime.sync_cycle().await, Some(true));
    assert_eq!(state.blobs.read_bytes(&digest, 8).await.unwrap(), b"artifact");
}

#[tokio::test]
async fn test_dual_replica_advances_both_planes_when_the_blob_is_available() {
    let (_primary_dir, primary_meta, primary_blobs) = primary_stores();
    let digest = primary_blobs.write(b"artifact").unwrap();
    primary_meta
        .commit_driver_txn(|txn| {
            txn.reference_blob(digest.as_str(), 8);
            Ok::<_, peryx_storage::meta::MetaError>(((), vec![b"upload".to_vec()]))
        })
        .unwrap();
    let server = TestServer::start(primary_router("primary-a", TOKEN, primary_meta, primary_blobs).unwrap()).await;
    let replica_dir = tempfile::tempdir().unwrap();
    let config = config(&replica_dir, Some(dual_replica_config(&server.url, 10)));
    let state = build_state(&config).unwrap();
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();

    assert_eq!(runtime.sync_cycle().await, Some(true));

    // The metadata plane committed the serial and the blob plane pulled its bytes and advanced its
    // frontier to match, so a reader gated on both views sees the record fully byte-backed.
    assert_eq!(state.meta.current_serial().unwrap(), 1);
    assert_eq!(state.blobs.read_bytes(&digest, 8).await.unwrap(), b"artifact");
    assert_eq!(state.meta.view_frontier(BLOB_VIEW).unwrap(), Some(1));
}

#[tokio::test]
async fn test_dual_replica_advances_metadata_while_a_missing_blob_holds_the_blob_frontier() {
    let (_primary_dir, primary_meta, primary_blobs) = primary_stores();
    // Reference a blob the primary never stored, so serving it 404s and the blob plane cannot advance.
    let digest = Digest::of(b"artifact");
    primary_meta
        .commit_driver_txn(|txn| {
            txn.put("pypi\0upload", b"record")?;
            txn.reference_blob(digest.as_str(), 8);
            Ok::<_, peryx_storage::meta::MetaError>(((), vec![b"upload".to_vec()]))
        })
        .unwrap();
    let server = TestServer::start(primary_router("primary-a", TOKEN, primary_meta, primary_blobs).unwrap()).await;
    let replica_dir = tempfile::tempdir().unwrap();
    let config = config(&replica_dir, Some(dual_replica_config(&server.url, 10)));
    let state = build_state(&config).unwrap();
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();

    let subscriber = tracing_subscriber::fmt().with_writer(std::io::sink).finish();
    let guard = tracing::subscriber::set_default(subscriber);
    assert_eq!(runtime.sync_cycle().await, Some(true));
    drop(guard);

    // The metadata plane committed the record even though its blob never arrived...
    assert_eq!(
        state.meta.get_driver_value("pypi\0upload").unwrap().as_deref(),
        Some(b"record".as_slice())
    );
    assert_eq!(state.meta.current_serial().unwrap(), 1);
    // ...while the blob frontier stays put, so a reader gated on the blob view never sees the serial.
    assert!(state.blobs.head(&digest).await.unwrap().is_none());
    assert_eq!(state.meta.view_frontier(BLOB_VIEW).unwrap(), None);
}

#[tokio::test]
async fn test_dual_replica_heals_the_blob_frontier_after_the_blob_arrives() {
    let (_primary_dir, primary_meta, primary_blobs) = primary_stores();
    let digest = Digest::of(b"artifact");
    primary_meta
        .commit_driver_txn(|txn| {
            txn.reference_blob(digest.as_str(), 8);
            Ok::<_, peryx_storage::meta::MetaError>(((), vec![b"upload".to_vec()]))
        })
        .unwrap();
    let server =
        TestServer::start(primary_router("primary-a", TOKEN, primary_meta, primary_blobs.clone()).unwrap()).await;
    let replica_dir = tempfile::tempdir().unwrap();
    let config = config(&replica_dir, Some(dual_replica_config(&server.url, 10)));
    let state = build_state(&config).unwrap();
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();

    let subscriber = tracing_subscriber::fmt().with_writer(std::io::sink).finish();
    let guard = tracing::subscriber::set_default(subscriber);
    // The first pass commits metadata, but the blob is not on the primary yet, so its frontier holds.
    assert_eq!(runtime.sync_cycle().await, Some(true));
    assert_eq!(state.meta.view_frontier(BLOB_VIEW).unwrap(), None);

    // The blob lands on the primary; the next pass re-derives the outstanding set from the tail, pulls
    // it, and advances the blob frontier with no new metadata to apply.
    assert_eq!(primary_blobs.write(b"artifact").unwrap(), digest);
    assert_eq!(runtime.sync_cycle().await, Some(true));
    drop(guard);

    assert_eq!(state.blobs.read_bytes(&digest, 8).await.unwrap(), b"artifact");
    assert_eq!(state.meta.view_frontier(BLOB_VIEW).unwrap(), Some(1));
}

#[tokio::test]
async fn test_dual_replica_retries_after_a_metadata_sync_error() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    drop(listener);
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, Some(dual_replica_config(&url, 10)));
    let state = build_state(&config).unwrap();
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();

    let subscriber = tracing_subscriber::fmt().with_writer(std::io::sink).finish();
    let guard = tracing::subscriber::set_default(subscriber);
    // The metadata plane cannot reach the primary, so the cycle records the error and asks to retry
    // without advancing either frontier.
    assert_eq!(runtime.sync_cycle().await, Some(true));
    drop(guard);

    assert_eq!(state.meta.current_serial().unwrap(), 0);
    assert_eq!(state.meta.view_frontier(BLOB_VIEW).unwrap(), None);
}

#[tokio::test]
async fn test_dual_replica_requires_the_blob_view() {
    let replica_dir = tempfile::tempdir().unwrap();
    let config = config(&replica_dir, Some(dual_replica_config("https://primary.example/", 10)));
    let state = build_state(&config).unwrap();

    assert_eq!(
        &*state.serving.required_views,
        [peryx_driver::state::SEARCH_VIEW, BLOB_VIEW].as_slice()
    );
}

#[tokio::test]
async fn test_unified_replica_keeps_the_default_required_views() {
    let replica_dir = tempfile::tempdir().unwrap();
    let config = config(&replica_dir, Some(replica_config("https://primary.example/", 10)));
    let state = build_state(&config).unwrap();

    assert_eq!(
        &*state.serving.required_views,
        [peryx_driver::state::SEARCH_VIEW].as_slice()
    );
}

#[tokio::test]
async fn test_replica_runtime_forwards_blobs_to_a_follower() {
    let (_primary_dir, primary_meta, primary_blobs) = primary_stores();
    let digest = primary_blobs.write(b"artifact").unwrap();
    primary_meta
        .commit_driver_txn(|txn| {
            txn.reference_blob(digest.as_str(), 8);
            Ok::<_, peryx_storage::meta::MetaError>(((), vec![b"upload".to_vec()]))
        })
        .unwrap();
    let primary = TestServer::start(primary_router("primary-a", TOKEN, primary_meta, primary_blobs).unwrap()).await;
    let replica_dir = tempfile::tempdir().unwrap();
    let intermediate_config = config(&replica_dir, Some(replica_config(&primary.url, 10)));
    let replica_state = build_state(&intermediate_config).unwrap();
    assert_eq!(
        ReplicationRuntime::new(&intermediate_config, &replica_state)
            .unwrap()
            .sync_cycle()
            .await,
        Some(true)
    );
    let replica = TestServer::start(
        primary_router(
            "replica-b",
            TOKEN,
            replica_state.meta.clone(),
            replica_state.blobs.clone(),
        )
        .unwrap(),
    )
    .await;
    let follower_dir = tempfile::tempdir().unwrap();
    let follower_config = config(&follower_dir, Some(replica_config(&replica.url, 10)));
    let follower_state = build_state(&follower_config).unwrap();

    assert_eq!(
        ReplicationRuntime::new(&follower_config, &follower_state)
            .unwrap()
            .sync_cycle()
            .await,
        Some(true)
    );
    assert_eq!(follower_state.blobs.read_bytes(&digest, 8).await.unwrap(), b"artifact");
}

#[tokio::test]
async fn test_replica_stays_live_but_unready_while_starting() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, Some(replica_config("https://primary.example/", 10)));
    let state = build_state(&config).unwrap();
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();
    let router = runtime.mount(router_for(state));

    let (health_status, health) = document(&router, "/+replication/v1/health", None).await;
    assert_eq!(health_status, StatusCode::OK);
    assert_eq!(
        health,
        serde_json::json!({"mode": "dc", "role": "replica", "ready": false, "reasons": ["frontier_lag"]})
    );

    let (ready_status, ready) = document(&router, "/+replication/v1/ready", None).await;
    assert_eq!(ready_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(ready, health);
}

#[tokio::test]
async fn test_replica_readiness_reports_a_sync_error() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    drop(listener);
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, Some(replica_config(&url, 10)));
    let state = build_state(&config).unwrap();
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();

    assert_eq!(runtime.sync_cycle().await, Some(true));
    let router = runtime.mount(router_for(state));

    assert_eq!(get(&router, "/+replication/v1/health").await.0, StatusCode::OK);
    let (status, ready) = document(&router, "/+replication/v1/ready", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        ready,
        serde_json::json!({"mode": "dc", "role": "replica", "ready": false, "reasons": ["sync_error"]})
    );
    let (_, body) = get(&router, "/metrics").await;
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("peryx_replication_sync_errors_total 1\n"), "{body}");
    assert!(
        body.contains("peryx_availability_sync_errors_total{class=\"transport\"} 1\n"),
        "{body}"
    );
    assert!(body.contains("peryx_availability_sync_cycles_total 1\n"), "{body}");
}

#[tokio::test]
async fn test_replica_readiness_reports_an_incompatible_schema() {
    let primary = incompatible_primary().await;
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, Some(replica_config(&primary.url, 10)));
    let state = build_state(&config).unwrap();
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();

    assert_eq!(runtime.sync_cycle().await, Some(true));
    let router = runtime.mount(router_for(state));

    let (status, ready) = document(&router, "/+replication/v1/ready", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        ready,
        serde_json::json!({"mode": "dc", "role": "replica", "ready": false, "reasons": ["incompatible_schema"]})
    );
    let (_, body) = get(&router, "/metrics").await;
    let body = String::from_utf8(body).unwrap();
    assert!(
        body.contains("peryx_availability_sync_errors_total{class=\"schema\"} 1\n"),
        "{body}"
    );
}

#[tokio::test]
async fn test_replica_readiness_recovers_and_reports_serials_to_operators() {
    let (_primary_dir, primary_meta, primary_blobs) = primary_stores();
    primary_meta
        .commit_driver_txn(|_| {
            Ok::<_, peryx_storage::meta::MetaError>(((), vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]))
        })
        .unwrap();
    let server = TestServer::start(primary_router("primary-a", TOKEN, primary_meta, primary_blobs).unwrap()).await;
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, Some(replica_config(&server.url, 2)));
    let state = build_state(&config).unwrap();
    let operator = credential(&state, "Olivia", Role::Operator).await;
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();
    let router = runtime.mount(router_for(state));

    assert_eq!(runtime.sync_cycle().await, Some(false));
    let (status, ready) = document(&router, "/+replication/v1/ready", Some(&operator)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        ready,
        serde_json::json!({
            "mode": "dc", "role": "replica", "ready": false, "reasons": ["frontier_lag"],
            "serial": 2, "primary_serial": 3, "lag": 1,
            "synced_changes": 2, "synced_blobs": 0, "sync_errors": 0,
        })
    );
    let (_, body) = get(&router, "/metrics").await;
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("peryx_replication_lag 1\n"), "{body}");
    assert!(body.contains("peryx_availability_pending_serials 1\n"), "{body}");
    assert!(body.contains("peryx_availability_sync_cycles_total 1\n"), "{body}");
    assert!(body.contains("peryx_availability_apply_seconds_count 1\n"), "{body}");

    assert_eq!(runtime.sync_cycle().await, Some(true));
    let (status, ready) = document(&router, "/+replication/v1/ready", Some(&operator)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ready,
        serde_json::json!({
            "mode": "dc", "role": "replica", "ready": true, "reasons": [],
            "serial": 3, "primary_serial": 3, "lag": 0,
            "synced_changes": 3, "synced_blobs": 0, "sync_errors": 0,
        })
    );
    let (_, body) = get(&router, "/metrics").await;
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("peryx_replication_lag 0\n"), "{body}");
    // Caught up to the primary at serial 3, yet readability holds at 0: applying the metadata
    // invalidated the search view, so no read is served ahead of it until the index refreshes.
    assert!(body.contains("peryx_replication_readable_serial 0\n"), "{body}");
    assert!(body.contains("peryx_availability_pending_serials 0\n"), "{body}");
    assert!(body.contains("peryx_availability_sync_cycles_total 2\n"), "{body}");
}

#[tokio::test]
async fn test_availability_health_filters_topology_by_caller_class() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(
        &dir,
        Some(replica_config("http://replica:s3cr3t@primary.example:8443/", 10)),
    );
    let state = build_state(&config).unwrap();
    let operator = credential(&state, "Olivia", Role::Operator).await;
    let administrator = credential(&state, "Alice", Role::Administrator).await;
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();
    let router = runtime.mount(router_for(state));

    let (_, public) = document(&router, "/+replication/v1/health", None).await;
    assert_eq!(
        public,
        serde_json::json!({"mode": "dc", "role": "replica", "ready": false, "reasons": ["frontier_lag"]})
    );

    let (_, operator) = document(&router, "/+replication/v1/health", Some(&operator)).await;
    assert!(operator.get("serial").is_some());
    assert!(operator.get("lag").is_some());
    assert!(operator.get("upstream").is_none());

    let (_, administrator) = document(&router, "/+replication/v1/health", Some(&administrator)).await;
    let upstream = administrator["upstream"].as_str().unwrap();
    assert_eq!(upstream, "http://primary.example:8443/");
    assert!(!upstream.contains("replica"));
    assert!(!upstream.contains("s3cr3t"));
}

#[tokio::test]
async fn test_primary_exposes_ready_availability() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, Some(primary_config()));
    let state = build_state(&config).unwrap();
    let administrator = credential(&state, "Alice", Role::Administrator).await;
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();
    let router = runtime.mount(router_for(state));

    let (status, ready) = document(&router, "/+replication/v1/ready", Some(&administrator)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ready,
        serde_json::json!({"mode": "dc", "role": "primary", "ready": true, "reasons": [], "serial": 0})
    );
    assert_eq!(get(&router, "/+replication/v1/health").await.0, StatusCode::OK);
}

#[tokio::test]
async fn test_readiness_reports_a_failed_blob_store_in_ha_mode() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("blobs"), b"not a directory").unwrap();
    let mut config = config(&dir, Some(primary_config()));
    let AvailabilityConfig::Dc(replication) = config.availability else {
        panic!("config helper builds a dc primary");
    };
    config.availability = AvailabilityConfig::Ha(replication);
    let router = build_router(&config).unwrap();

    let (status, ready) = document(&router, "/+replication/v1/ready", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        ready,
        serde_json::json!({"mode": "ha", "role": "primary", "ready": false, "reasons": ["blob_store"]})
    );
}

#[tokio::test]
async fn test_disabled_runtime_mounts_no_routes_or_task() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, None);
    let state = build_state(&config).unwrap();
    let runtime = ReplicationRuntime::new(&config, &state).unwrap();

    assert!(!runtime.is_replica());
    assert_eq!(runtime.sync_cycle().await, None);
    let router = runtime.mount(router_for(state));
    assert!(runtime.start().is_none());
    let response = router
        .clone()
        .oneshot(
            Request::get("/+replication/v1/changes?after=0&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(get(&router, "/+replication/v1/health").await.0, StatusCode::NOT_FOUND);
    assert_eq!(get(&router, "/+replication/v1/ready").await.0, StatusCode::NOT_FOUND);
    let (_, body) = get(&router, "/metrics").await;
    let metrics = String::from_utf8(body).unwrap();
    assert!(!metrics.contains("peryx_replication_"), "{metrics}");
    assert!(!metrics.contains("peryx_availability_worker_"), "{metrics}");
}

#[test]
fn test_replica_runtime_disables_local_writers() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(&dir, Some(replica_config("https://primary.example/", 10)));
    let IndexKind::Cached { routing, .. } = &mut config.indexes[0].kind else {
        panic!("expected the default cached index");
    };
    *routing = UpstreamRoutingConfig {
        upstreams: vec![UpstreamConfig {
            name: "primary".to_owned(),
            url: "https://packages.example/simple/".to_owned(),
            artifact_url: None,
            username: Some("replica".to_owned()),
            password: Some(SecretSource::File("missing-routed-upstream-password".into())),
            token: None,
            credential_exec: None,
            credential_refresh: None,
            tls: crate::config::UpstreamTlsConfig::default(),
        }],
        fallback: true,
        protected: Vec::new(),
        pins: BTreeMap::default(),
    };
    config.indexes[1].tokens.extend([
        TokenConfig {
            name: "reader".to_owned(),
            secret: SecretSource::Literal("reader-secret".to_owned()),
            projects: vec!["*".to_owned()],
            actions: BTreeSet::from([Action::Read, Action::Write]),
            expires_at: None,
        },
        TokenConfig {
            name: "writer".to_owned(),
            secret: SecretSource::File("missing-writer-token".into()),
            projects: vec!["*".to_owned()],
            actions: BTreeSet::from([Action::Write]),
            expires_at: None,
        },
    ]);
    config.indexes[1].webhooks.push(WebhookConfig {
        name: "audit".to_owned(),
        url: "https://hooks.example/audit".to_owned(),
        secret: WebhookSecret::Env("PERYX_TEST_MISSING_REPLICA_WEBHOOK_SECRET".to_owned()),
        events: Vec::new(),
    });

    let state = build_state(&config).unwrap();

    assert!(state.read_only);
    assert!(matches!(
        state.indexes[0].kind,
        RuntimeIndexKind::Cached { offline: true, .. }
    ));
    assert!(state.upstream_routes.is_empty());
    assert!(state.indexes[1].acl.grants_to_anyone(Action::Read));
    assert!(!state.indexes[1].acl.grants_to_anyone(Action::Write));
    assert!(!state.indexes[1].acl.grants_to_anyone(Action::Delete));
    assert!(matches!(
        state.indexes[2].kind,
        RuntimeIndexKind::Virtual { upload: None, .. }
    ));
    assert!(state.webhooks.is_empty());
}

#[rstest]
#[case::primary(ReplicationConfig::Primary {
    source: "primary-a".to_owned(),
    token: SecretSource::File("missing-primary-token".into()),
}, "read the primary replication token")]
#[case::replica(ReplicationConfig::Replica {
    upstream: "https://primary.example/".to_owned(),
    token: SecretSource::File("missing-replica-token".into()),
    poll_interval: Duration::from_secs(1),
    page_size: NonZeroUsize::new(10).unwrap(),
    dual_plane: false,
}, "read the replica replication token")]
fn test_replication_runtime_reports_secret_errors(#[case] replication: ReplicationConfig, #[case] expected: &str) {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, Some(replication));
    let state = build_state(&config).unwrap();

    let Err(error) = ReplicationRuntime::new(&config, &state) else {
        panic!("expected the missing replication token to fail");
    };

    assert!(error.to_string().contains(expected), "{error}");
}

#[test]
fn test_replication_runtime_rejects_an_invalid_upstream_url() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir, Some(replica_config("not a URL", 10)));
    let state = build_state(&config).unwrap();

    let Err(error) = ReplicationRuntime::new(&config, &state) else {
        panic!("expected the invalid upstream URL to fail");
    };

    assert!(error.to_string().contains("build replica HTTP client"), "{error}");
}

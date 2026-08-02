use std::collections::BTreeSet;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use peryx_core::{NodeRole, TopologyConfig, TopologyMember, TopologyMode};
use peryx_driver::authz::AuthorizationService;
use peryx_driver::state::AppState;
use peryx_driver::users::UserService;
use peryx_identity::{GrantScope, PasswordPolicy, Role};
use peryx_storage::meta::MetaStore;
use tower::ServiceExt as _;

const USER_PASSWORD: &str = "local password";

fn member(node: &str, dc: &str, role: NodeRole) -> TopologyMember {
    TopologyMember {
        node: node.to_owned(),
        dc: dc.to_owned(),
        address: format!("{node}:8080"),
        role,
    }
}

fn topology(local_node: Option<&str>) -> TopologyConfig {
    TopologyConfig {
        mode: TopologyMode::Dc,
        group: Some("east".to_owned()),
        members: vec![
            member("writer-a", "east-1", NodeRole::Writer),
            member("replica-b", "east-2", NodeRole::Replica),
        ],
        local_node: local_node.map(str::to_owned),
    }
}

async fn app(read_only: bool, healthy: bool) -> (tempfile::TempDir, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("peryx.redb");
    let meta = MetaStore::open(&path).unwrap();
    let users = UserService::with_password_settings(meta.clone(), PasswordPolicy::new(8, 1, 1).unwrap(), 2);
    let authorization = AuthorizationService::new(meta.clone());
    for (name, role) in [
        ("Alice", Role::Administrator),
        ("Olivia", Role::Operator),
        ("Rita", Role::RepositoryReader),
    ] {
        let user = users.create(name).unwrap();
        users.set_password(&user.id, USER_PASSWORD).await.unwrap();
        let scope = if matches!(role, Role::RepositoryReader) {
            GrantScope::Repository {
                name: "private".to_owned(),
            }
        } else {
            GrantScope::Server
        };
        authorization.grant(&user.id, role, scope).unwrap();
    }
    drop(authorization);
    drop(users);
    // A blob root that is a regular file cannot be opened as a directory, so the health probe fails and
    // the node reports itself unready.
    let blob_root = dir.path().join("blobs");
    if !healthy {
        std::fs::write(&blob_root, b"not a directory").unwrap();
    }
    let blobs = peryx_storage::blob::BlobStore::new(blob_root);
    let mut state = AppState::new(meta.clone(), blobs, 60, Vec::new());
    state.users = UserService::with_password_settings(meta, PasswordPolicy::new(8, 1, 1).unwrap(), 2);
    state.read_only = read_only;
    state.set_availability_topology(topology((!read_only).then_some("writer-a")));
    (dir, Arc::new(state))
}

async fn get(
    state: &Arc<AppState>,
    credential: Option<(&str, &str)>,
) -> (StatusCode, axum::http::HeaderMap, serde_json::Value) {
    let mut request = Request::builder().uri("/+availability/topology");
    if let Some((user, password)) = credential {
        request = request.header(
            header::AUTHORIZATION,
            format!("Basic {}", STANDARD.encode(format!("{user}:{password}"))),
        );
    }
    let response = crate::router(state.clone())
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, headers, serde_json::from_slice(&body).unwrap())
}

fn node<'a>(body: &'a serde_json::Value, identity: &str) -> &'a serde_json::Value {
    body["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["node"] == identity)
        .unwrap()
}

fn node_keys(body: &serde_json::Value, identity: &str) -> BTreeSet<String> {
    node(body, identity).as_object().unwrap().keys().cloned().collect()
}

#[tokio::test]
async fn test_topology_public_reveals_only_the_static_roster() {
    let (_dir, state) = app(false, true).await;

    let (status, headers, body) = get(&state, None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(body["mode"], "dc");
    assert_eq!(body["group"], "east");
    assert_eq!(body["node_count"], 2);
    assert!(body["captured_at"].is_number());
    assert_eq!(body["local"]["role"], "writer");
    assert!(body["local"].get("liveness").is_none(), "{body}");
    assert!(body["local"].get("frontier").is_none(), "{body}");
    let writer = node(&body, "writer-a");
    assert_eq!(writer["local"], true);
    assert_eq!(
        node_keys(&body, "writer-a"),
        BTreeSet::from([
            "node".to_owned(),
            "dc".to_owned(),
            "role".to_owned(),
            "local".to_owned()
        ]),
    );
}

#[tokio::test]
async fn test_topology_repository_token_reads_the_public_roster() {
    let (_dir, state) = app(false, true).await;

    let (status, _, body) = get(&state, Some(("Rita", USER_PASSWORD))).await;

    assert_eq!(status, StatusCode::OK);
    assert!(node(&body, "writer-a").get("liveness").is_none(), "{body}");
}

#[tokio::test]
async fn test_topology_operator_sees_liveness_and_the_local_frontier_only() {
    let (_dir, state) = app(false, true).await;

    let (status, _, body) = get(&state, Some(("Olivia", USER_PASSWORD))).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["local"]["liveness"], "live");
    assert!(body["local"]["frontier"].is_number());
    let writer = node(&body, "writer-a");
    assert_eq!(writer["liveness"], "live");
    assert!(writer["frontier"].is_number());
    assert!(writer.get("address").is_none(), "{body}");
    let peer = node(&body, "replica-b");
    assert_eq!(peer["liveness"], "unknown");
    assert!(peer.get("frontier").is_none(), "a peer frontier is unknown: {body}");
}

#[tokio::test]
async fn test_topology_administrator_sees_the_advertised_addresses() {
    let (_dir, state) = app(false, true).await;

    let (status, _, body) = get(&state, Some(("Alice", USER_PASSWORD))).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(node(&body, "writer-a")["address"], "writer-a:8080");
    assert_eq!(node(&body, "replica-b")["address"], "replica-b:8080");
}

#[tokio::test]
async fn test_topology_reports_unready_when_the_blob_store_is_unhealthy() {
    let (_dir, state) = app(false, false).await;

    let (status, _, body) = get(&state, Some(("Olivia", USER_PASSWORD))).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["local"]["liveness"], "unready");
    assert_eq!(node(&body, "writer-a")["liveness"], "unready");
}

#[tokio::test]
async fn test_topology_replica_marks_no_local_roster_member() {
    let (_dir, state) = app(true, true).await;

    let (status, _, body) = get(&state, Some(("Alice", USER_PASSWORD))).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["local"]["role"], "replica");
    assert!(
        body["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|node| node["local"] == false),
        "a replica does not know its roster identity: {body}",
    );
}

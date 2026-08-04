use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode, header};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use peryx_driver::authz::AuthorizationService;
use peryx_driver::state::AppState;
use peryx_driver::users::UserService;
use peryx_identity::{GrantScope, PasswordPolicy, Role};
use peryx_storage::meta::MetaStore;
use rstest::rstest;
use serde_json::Value;
use tower::ServiceExt as _;

use crate::availability::{AvailabilityPosture, router};
use crate::config::{AvailabilityConfig, ReplicationConfig, SecretSource};

const ADMIN: &str = "Alice";
const OPERATOR: &str = "Olivia";
const PASSWORD: &str = "local password";

async fn app() -> (tempfile::TempDir, Arc<AppState>) {
    build_app(false).await
}

async fn build_app(break_identity_store: bool) -> (tempfile::TempDir, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("peryx.redb");
    let meta = MetaStore::open(&path).unwrap();
    let users = UserService::with_password_settings(meta.clone(), PasswordPolicy::new(8, 1, 1).unwrap(), 2);
    let authorization = AuthorizationService::new(meta.clone());
    for (name, role) in [(ADMIN, Role::Administrator), (OPERATOR, Role::Operator)] {
        let user = users.create(name).unwrap();
        users.set_password(&user.id, PASSWORD).await.unwrap();
        authorization.grant(&user.id, role, GrantScope::Server).unwrap();
    }
    drop(authorization);
    drop(users);
    drop(meta);
    if break_identity_store {
        let database = redb::Database::open(&path).unwrap();
        let transaction = database.begin_write().unwrap();
        transaction
            .delete_table(redb::TableDefinition::<&str, &[u8]>::new("server_user_verifier"))
            .unwrap();
        transaction
            .open_table(redb::TableDefinition::<&str, u64>::new("server_user_verifier"))
            .unwrap();
        transaction.commit().unwrap();
    }
    let meta = MetaStore::open_existing(path).unwrap();
    let blobs = peryx_storage::blob::BlobStore::new(dir.path().join("blobs"));
    let mut state = AppState::new(meta.clone(), blobs, 60, Vec::new());
    state.users = UserService::with_password_settings(meta, PasswordPolicy::new(8, 1, 1).unwrap(), 2);
    (dir, Arc::new(state))
}

fn dc_writer() -> AvailabilityPosture {
    AvailabilityPosture::from_config(&AvailabilityConfig::Dc(ReplicationConfig::Primary {
        source: "primary-a".to_owned(),
        token: SecretSource::Literal("secret".to_owned()),
    }))
    .expect("dc primary posture")
}

fn ha_replica() -> AvailabilityPosture {
    AvailabilityPosture::from_config(&AvailabilityConfig::Ha(ReplicationConfig::Replica {
        upstream: "https://primary.example/".to_owned(),
        token: SecretSource::Literal("secret".to_owned()),
        poll_interval: Duration::from_secs(1),
        page_size: NonZeroUsize::MIN,
    }))
    .expect("ha replica posture")
}

fn basic(user: &str, password: &str) -> String {
    format!("Basic {}", STANDARD.encode(format!("{user}:{password}")))
}

async fn request(
    state: &Arc<AppState>,
    posture: AvailabilityPosture,
    path: &str,
    auth: Option<&str>,
) -> (StatusCode, HeaderMap, Value) {
    let mut builder = Request::builder().uri(path);
    if let Some(value) = auth {
        builder = builder.header(header::AUTHORIZATION, value);
    }
    let response = router(state.clone(), posture)
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, headers, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

#[test]
fn test_posture_absent_for_single_node_none() {
    assert!(AvailabilityPosture::from_config(&AvailabilityConfig::None).is_none());
}

#[tokio::test]
async fn test_status_reports_writer_posture_for_an_administrator() {
    let (_dir, state) = app().await;

    let (status, _, body) = request(
        &state,
        dc_writer(),
        "/availability/v1/status",
        Some(&basic(ADMIN, PASSWORD)),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["protocol_version"], 1);
    assert_eq!(body["mode"], "dc");
    assert_eq!(body["role"], "writer");
    assert_eq!(body["read_only"], false);
}

#[tokio::test]
async fn test_status_is_never_cached() {
    let (_dir, state) = app().await;

    let (status, headers, _) = request(
        &state,
        dc_writer(),
        "/availability/v1/status",
        Some(&basic(ADMIN, PASSWORD)),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers[header::CACHE_CONTROL],
        "no-store",
        "an authenticated posture must not be stored by a shared cache",
    );
}

#[tokio::test]
async fn test_status_reports_replica_role_in_ha_mode() {
    let (_dir, state) = app().await;

    let (status, _, body) = request(
        &state,
        ha_replica(),
        "/availability/v1/status",
        Some(&basic(ADMIN, PASSWORD)),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["mode"], "ha");
    assert_eq!(body["role"], "replica");
}

#[rstest]
#[case::missing(None)]
#[case::not_basic(Some("Bearer some-token".to_owned()))]
#[case::malformed_basic(Some("Basic not+valid+base64+@@".to_owned()))]
#[case::wrong_password(Some(basic(ADMIN, "wrong password")))]
#[tokio::test]
async fn test_status_rejects_missing_or_invalid_credentials(#[case] auth: Option<String>) {
    let (_dir, state) = app().await;

    let (status, headers, _) = request(&state, dc_writer(), "/availability/v1/status", auth.as_deref()).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(headers.contains_key(header::WWW_AUTHENTICATE));
}

#[tokio::test]
async fn test_status_forbids_a_non_administrator() {
    let (_dir, state) = app().await;

    let (status, _, _) = request(
        &state,
        dc_writer(),
        "/availability/v1/status",
        Some(&basic(OPERATOR, PASSWORD)),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_credential_rotation_rejects_the_old_password() {
    let (_dir, state) = app().await;
    let id = state.users.authenticate(ADMIN, PASSWORD).await.unwrap().unwrap();
    state.users.set_password(&id, "rotated password").await.unwrap();

    let (old, _, _) = request(
        &state,
        dc_writer(),
        "/availability/v1/status",
        Some(&basic(ADMIN, PASSWORD)),
    )
    .await;
    let (new, _, _) = request(
        &state,
        dc_writer(),
        "/availability/v1/status",
        Some(&basic(ADMIN, "rotated password")),
    )
    .await;

    assert_eq!(old, StatusCode::UNAUTHORIZED);
    assert_eq!(new, StatusCode::OK);
}

#[tokio::test]
async fn test_revoking_administration_forbids_a_prior_administrator() {
    let (_dir, state) = app().await;
    let id = state.users.authenticate(ADMIN, PASSWORD).await.unwrap().unwrap();
    state
        .authorization
        .revoke(&id, Role::Administrator, &GrantScope::Server)
        .unwrap();

    let (status, _, _) = request(
        &state,
        dc_writer(),
        "/availability/v1/status",
        Some(&basic(ADMIN, PASSWORD)),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_identity_store_failure_is_service_unavailable() {
    let (_dir, state) = build_app(true).await;

    let (status, _, _) = request(
        &state,
        dc_writer(),
        "/availability/v1/status",
        Some(&basic(ADMIN, PASSWORD)),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_unknown_path_is_not_found_without_authenticating() {
    let (_dir, state) = app().await;

    let (status, _, _) = request(&state, dc_writer(), "/availability/v1/unknown", None).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_listener_serves_then_drains_on_shutdown() {
    let (_dir, state) = app().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let signal = shutdown.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state, dc_writer()).into_make_service())
            .with_graceful_shutdown(async move { signal.cancelled().await })
            .await
            .unwrap();
    });

    let client = reqwest::Client::new();
    let url = format!("http://{address}/availability/v1/status");
    let serving = client
        .get(&url)
        .header(header::AUTHORIZATION, basic(ADMIN, PASSWORD))
        .send()
        .await
        .unwrap();
    assert_eq!(serving.status(), StatusCode::OK);

    shutdown.cancel();
    server.await.unwrap();
    assert!(client.get(&url).send().await.is_err());
}

use std::io::Read as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use peryx_upstream::{CredentialFailure, CredentialProvider, CredentialRefresh, UpstreamClient, UpstreamTls};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::Barrier;

use super::*;
use rstest::rstest;

fn header(value: &str) -> HeaderValue {
    HeaderValue::from_str(value).unwrap()
}

/// The default clock names the real wall time, not a placeholder: a token cached against it must
/// expire when the calendar says it does, not on some fixed date a mock could stand in for.
#[test]
fn test_unix_now_reads_the_real_wall_clock() {
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .cast_signed();

    let now = unix_now();

    assert!((now - before).abs() < 10, "unix_now={now} before={before}");
}

#[test]
fn test_parse_bearer_reads_realm_service_scope() {
    let challenge = parse_bearer(&header(
        r#"Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull""#,
    ))
    .unwrap();
    assert_eq!(challenge.realm, "https://auth.docker.io/token");
    assert_eq!(challenge.service.as_deref(), Some("registry.docker.io"));
    assert_eq!(challenge.scope.as_deref(), Some("repository:library/nginx:pull"));
}

#[test]
fn test_parse_bearer_realm_only() {
    let challenge = parse_bearer(&header(r#"bearer realm="https://auth.example/token""#)).unwrap();
    assert_eq!(challenge.realm, "https://auth.example/token");
    assert_eq!(challenge.service, None);
    assert_eq!(challenge.scope, None);
}

#[test]
fn test_parse_bearer_rejects_non_bearer_scheme() {
    assert_eq!(parse_bearer(&header(r#"Basic realm="x""#)), None);
}

#[test]
fn test_parse_bearer_requires_a_realm() {
    assert_eq!(parse_bearer(&header(r#"Bearer service="registry.docker.io""#)), None);
}

#[test]
fn test_parse_bearer_rejects_malformed_parameter() {
    assert_eq!(parse_bearer(&header("Bearer realmnoeq")), None);
}

#[test]
fn test_parse_bearer_ignores_unknown_parameters() {
    let challenge = parse_bearer(&header(
        r#"Bearer realm="https://auth.example/token",error="insufficient_scope""#,
    ))
    .unwrap();
    assert_eq!(challenge.realm, "https://auth.example/token");
}

#[test]
fn test_upstream_error_display() {
    assert_eq!(UpstreamError::Timeout.to_string(), crate::error::TIMEOUT_MESSAGE);
    assert_eq!(
        UpstreamError::Status(StatusCode::NOT_FOUND).to_string(),
        "upstream returned 404 Not Found"
    );
    assert_eq!(UpstreamError::Transport("reset".to_owned()).to_string(), "reset");
    assert_eq!(
        UpstreamError::RateLimited(Some("5".to_owned())).to_string(),
        "upstream rate limit reached"
    );
}

enum ManifestHeadOutcome {
    Metadata(u64),
    Invalid,
    Transport,
}

#[rstest]
#[case::valid(
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "application/vnd.oci.image.manifest.v1+json; charset=utf-8",
    &["7"],
    ManifestHeadOutcome::Metadata(7),
)]
#[case::uppercase_digest(
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeF",
    "application/vnd.oci.image.manifest.v1+json",
    &["7"],
    ManifestHeadOutcome::Invalid,
)]
#[case::wrong_digest_algorithm(
    "sha512:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "application/vnd.oci.image.manifest.v1+json",
    &["7"],
    ManifestHeadOutcome::Invalid,
)]
#[case::wildcard_media_type(
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "application/*",
    &["7"],
    ManifestHeadOutcome::Invalid,
)]
#[case::malformed_media_type(
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "application/vnd/oci",
    &["7"],
    ManifestHeadOutcome::Invalid,
)]
#[case::identical_content_lengths(
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "application/vnd.oci.image.manifest.v1+json",
    &["7", "7"],
    ManifestHeadOutcome::Transport,
)]
#[case::conflicting_content_lengths(
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "application/vnd.oci.image.manifest.v1+json",
    &["7", "9"],
    ManifestHeadOutcome::Transport,
)]
#[tokio::test]
async fn test_manifest_head_validates_upstream_metadata(
    #[case] digest: &str,
    #[case] media_type: &str,
    #[case] content_lengths: &[&str],
    #[case] expected: ManifestHeadOutcome,
) {
    let server = MockServer::start().await;
    let mut response = ResponseTemplate::new(200)
        .insert_header("docker-content-digest", digest)
        .insert_header("content-type", media_type);
    for content_length in content_lengths {
        response = response.append_header("content-length", *content_length);
    }
    Mock::given(method("HEAD"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .respond_with(response)
        .expect(1)
        .mount(&server)
        .await;

    let result = Upstream::new()
        .manifest_head(
            &upstream_client(&format!("{}/", server.uri()), credentials(Auth::None)),
            "library/nginx",
            "latest",
            None,
            &TokenRealms::default(),
        )
        .await;

    match expected {
        ManifestHeadOutcome::Metadata(bytes) => {
            let head = result.unwrap();
            assert_eq!(
                (head.digest, head.media_type, head.bytes),
                (digest.to_owned(), media_type.to_owned(), bytes)
            );
        }
        ManifestHeadOutcome::Invalid => {
            assert!(matches!(result, Err(UpstreamError::InvalidManifestHead)));
        }
        ManifestHeadOutcome::Transport => {
            assert!(matches!(result, Err(UpstreamError::Transport(_))));
        }
    }
}

fn basic(username: &str, password: &str) -> Auth {
    Auth::Basic {
        username: username.to_owned(),
        password: password.to_owned(),
    }
}

fn credentials(auth: Auth) -> CredentialProvider {
    CredentialProvider::fixed(auth)
}

fn basic_header(username: &str, password: &str) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"))
    )
}

fn configured_realms(origins: &[&str]) -> TokenRealms {
    let entries = origins.iter().map(|origin| toml::Value::from(*origin)).collect();
    TokenRealms::parse(&toml::Value::Array(entries)).unwrap()
}

fn upstream_client(base: &str, credentials: CredentialProvider) -> UpstreamClient {
    UpstreamClient::with_credentials_and_tls_for_origin(base, credentials, &UpstreamTls::default(), base, &[]).unwrap()
}

use base64::Engine as _;
use wiremock::matchers::{header as match_header, method, path, query_param};
use wiremock::{Match, Mock, MockServer, Request, ResponseTemplate};

use crate::tests::{ResponseGate, gated_response, observe_pending, response_gate};

/// The matcher leaves the authenticated retry for the success mock.
struct Unauthenticated;
impl Match for Unauthenticated {
    fn matches(&self, request: &Request) -> bool {
        !request.headers.contains_key("authorization")
    }
}

fn challenge(base: &str) -> ResponseTemplate {
    ResponseTemplate::new(401).insert_header(
        "www-authenticate",
        format!(r#"Bearer realm="{base}token",service=reg,scope="repository:library/nginx:pull""#).as_str(),
    )
}

async fn assert_manifest_authenticates(server: &MockServer, base: &str) {
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#))
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(server)
        .await;

    let response = Upstream::new()
        .manifest(
            &upstream_client(base, credentials(Auth::None)),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_manifest_selects_bearer_from_combined_challenges() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(ResponseTemplate::new(401).insert_header(
            "www-authenticate",
            format!(
                r#"Basic realm="login", bEaReR ReAlM="{base}token?aud=a,b",SeRvIcE="reg\"istry",ScOpE="repository:library\/nginx:pull","#
            )
            .as_str(),
        ))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .and(query_param("aud", "a,b"))
        .and(query_param("service", "reg\"istry"))
        .and(query_param("scope", "repository:library/nginx:pull"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let response = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(Auth::None)),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_manifest_selects_bearer_from_repeated_challenge_fields() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(
            ResponseTemplate::new(401)
                .append_header("www-authenticate", r#"Basic realm="login"#)
                .append_header("www-authenticate", format!(r#"Bearer realm="{base}token""#).as_str()),
        )
        .expect(1)
        .mount(&server)
        .await;

    assert_manifest_authenticates(&server, &base).await;
}

#[tokio::test]
async fn test_manifest_selects_bearer_after_malformed_challenge() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(ResponseTemplate::new(401).insert_header(
            "www-authenticate",
            format!(r#"Bearer realmnoeq, Bearer realm="{base}token""#).as_str(),
        ))
        .expect(1)
        .mount(&server)
        .await;

    assert_manifest_authenticates(&server, &base).await;
}

#[tokio::test]
async fn test_fetch_token_rejects_an_oversized_response() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .expect(1)
        .mount(&server)
        .await;
    let oversized = format!(r#"{{"filler":"{}"}}"#, "A".repeat(2 * 1024 * 1024));
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(oversized))
        .mount(&server)
        .await;

    let error = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(Auth::None)),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("exceeds"), "{error}");
}

/// A token response of exactly the size cap is a legitimate response, not an abusive one: the bound
/// rejects a body that exceeds it, not one that just meets it.
#[tokio::test]
async fn test_fetch_token_accepts_a_response_at_exactly_the_size_cap() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .expect(1)
        .mount(&server)
        .await;
    let prefix = r#"{"token":"tok","filler":""#;
    let suffix = "\"}";
    let padding = (1usize << 20) - prefix.len() - suffix.len();
    let body = format!("{prefix}{}{suffix}", "x".repeat(padding));
    assert_eq!(body.len(), 1 << 20);
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let response = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(Auth::None)),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[rstest]
#[case::realm_exact_token("realm=invalid")]
#[case::realm_mixed_quoted(r#"ReAlM="://""#)]
#[case::service_exact_equal_token("service=registry,service=registry")]
#[case::service_mixed_different_quoted(r#"service="first",SeRvIcE="second""#)]
#[case::scope_exact_equal_quoted(r#"scope="repository:app:pull",scope="repository:app:pull""#)]
#[case::scope_mixed_different_token("scope=first,ScOpE=second")]
#[case::extension_exact_equal_quoted(r#"extension="value",extension="value""#)]
#[case::extension_mixed_different_token("extension=first,ExTeNsIoN=second")]
#[tokio::test]
async fn test_manifest_skips_bearer_with_duplicate_parameter(#[case] parameters: &str) {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(ResponseTemplate::new(401).insert_header(
            "www-authenticate",
            format!(r#"Bearer realm="{base}wrong",{parameters}, Bearer realm="{base}token""#).as_str(),
        ))
        .expect(1)
        .mount(&server)
        .await;

    assert_manifest_authenticates(&server, &base).await;
}

#[tokio::test]
async fn test_manifest_selects_bearer_after_duplicate_parameter_field() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(
            ResponseTemplate::new(401)
                .append_header(
                    "www-authenticate",
                    format!(r#"Bearer realm="{base}wrong",realm=invalid"#).as_str(),
                )
                .append_header("www-authenticate", format!(r#"Bearer realm="{base}token""#).as_str()),
        )
        .expect(1)
        .mount(&server)
        .await;

    assert_manifest_authenticates(&server, &base).await;
}

#[rstest]
#[case::missing_name("Bearer =token")]
#[case::garbage_before_a_valid_realm("Bearer =token,realm=foo")]
#[case::unterminated_quote(r#"Bearer realm="https://auth.example/token"#)]
#[case::unterminated_escape(r#"Bearer realm="https://auth.example/token\"#)]
#[case::unterminated_after_escape(r#"Bearer realm="https://auth.example/\token"#)]
#[tokio::test]
async fn test_manifest_rejects_malformed_bearer_parameters(#[case] challenge: &str) {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .respond_with(ResponseTemplate::new(401).insert_header("www-authenticate", challenge))
        .expect(1)
        .mount(&server)
        .await;

    let result = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(Auth::None)),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await;

    assert!(matches!(result, Err(UpstreamError::Status(StatusCode::UNAUTHORIZED))));
}

#[tokio::test]
async fn test_manifest_rejects_an_invalid_bearer_realm() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .respond_with(ResponseTemplate::new(401).insert_header("www-authenticate", r#"Bearer realm="://""#))
        .expect(1)
        .mount(&server)
        .await;

    let result = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(Auth::None)),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await;

    assert!(matches!(result, Err(UpstreamError::Transport(message)) if message.starts_with("invalid bearer realm:")));
}

#[tokio::test]
async fn test_manifest_blocks_a_private_bearer_realm() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("www-authenticate", r#"Bearer realm="http://169.254.169.254/token""#),
        )
        .expect(1)
        .mount(&server)
        .await;

    let error = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(Auth::None)),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap_err();

    assert!(
        error.to_string().contains("169.254.169.254 is not a public address"),
        "{error}"
    );
}

#[rstest]
#[case::loopback_hostname(
    "http://localhost:9/private",
    "host resolves only to non-public addresses; configure `trusted_hosts` to allow it"
)]
#[case::link_local_literal("http://169.254.169.254/private", "169.254.169.254 is not a public address")]
#[case::private_literal("http://10.0.0.1/private", "10.0.0.1 is not a public address")]
#[tokio::test]
async fn test_manifest_blocks_redirects_to_private_destinations(#[case] location: &str, #[case] reason: &str) {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", location))
        .expect(1)
        .mount(&server)
        .await;

    let error = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(Auth::None)),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains(reason), "{error}");
}

#[rstest]
#[case::silent(false)]
#[case::trickled(true)]
#[tokio::test]
async fn test_manifest_has_the_shared_read_deadline(#[case] trickle: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let client = upstream_client(&base, credentials(Auth::None));
    let (request_seen_tx, request_seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (trickle_tx, trickle_rx) = tokio::sync::oneshot::channel();
    let (trickled_tx, trickled_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        let mut request_bytes = [0; 4_096];
        let received = connection.read(&mut request_bytes).await.unwrap();
        assert!(request_bytes[..received].starts_with(b"GET /v2/library/nginx/manifests/latest"));
        request_seen_tx.send(()).unwrap();
        if trickle_rx.await.is_ok() {
            connection.write_all(b"HTTP/1.1 200 OK\r\n").await.unwrap();
            tokio::time::sleep(Duration::from_secs(10)).await;
            connection.write_all(b"content-length: 1\r\n").await.unwrap();
            trickled_tx.send(()).unwrap();
        }
        let _ = release_rx.await;
    });
    let request = tokio::spawn(async move {
        Upstream::new()
            .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
            .await
    });
    request_seen_rx.await.unwrap();
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    if trickle {
        tokio::time::resume();
        trickle_tx.send(()).unwrap();
        trickled_rx.await.unwrap();
        tokio::time::pause();
    } else {
        drop(trickle_tx);
    }
    assert!(!request.is_finished());
    tokio::time::advance((started + Duration::from_secs(30)).saturating_duration_since(tokio::time::Instant::now()))
        .await;

    assert!(matches!(request.await.unwrap(), Err(UpstreamError::Timeout)));
    assert!(
        (Duration::from_secs(30)..=Duration::from_secs(30) + Duration::from_millis(2))
            .contains(&(tokio::time::Instant::now() - started))
    );
    release_tx.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn test_manifest_tls_handshake_has_the_shared_connect_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("https://{}/", listener.local_addr().unwrap());
    let client = upstream_client(&base, credentials(Auth::None));
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        let mut hello = [0; 1];
        connection.read_exact(&mut hello).await.unwrap();
        accepted_tx.send(()).unwrap();
        release_rx.await.unwrap();
    });
    let request = tokio::spawn(async move {
        Upstream::new()
            .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
            .await
    });
    accepted_rx.await.unwrap();
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    tokio::time::advance(Duration::from_secs(11)).await;

    assert!(matches!(request.await.unwrap(), Err(UpstreamError::Timeout)));
    assert_eq!(tokio::time::Instant::now() - started, Duration::from_secs(11));
    release_tx.send(()).unwrap();
    server.await.unwrap();
}

const PROXY_CONNECT_TIMEOUT_CHILD: &str = "PERYX_OCI_PROXY_CONNECT_TIMEOUT_CHILD";

#[tokio::test]
async fn test_manifest_proxy_connect_has_the_shared_connect_deadline() {
    if std::env::var_os(PROXY_CONNECT_TIMEOUT_CHILD).is_some() {
        let client = upstream_client("https://registry.example/", credentials(Auth::None));
        let request = tokio::spawn(async move {
            Upstream::new()
                .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
                .await
        });
        tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::spawn_blocking(|| std::io::stdin().read_exact(&mut [0])),
        )
        .await
        .expect("proxy parent did not acknowledge the child")
        .unwrap()
        .unwrap();
        tokio::time::pause();
        let started = tokio::time::Instant::now();
        tokio::time::advance(Duration::from_secs(10)).await;

        assert!(matches!(request.await.unwrap(), Err(UpstreamError::Timeout)));
        assert!(
            (Duration::from_secs(10)..=Duration::from_secs(10) + Duration::from_millis(2))
                .contains(&(tokio::time::Instant::now() - started))
        );
        return;
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = format!("http://{}", listener.local_addr().unwrap());
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "upstream::tests::test_manifest_proxy_connect_has_the_shared_connect_deadline",
            "--nocapture",
        ])
        .env(PROXY_CONNECT_TIMEOUT_CHILD, "1")
        .env("HTTPS_PROXY", &proxy)
        .env("https_proxy", &proxy)
        .env_remove("HTTP_PROXY")
        .env_remove("http_proxy")
        .env_remove("ALL_PROXY")
        .env_remove("all_proxy")
        .env_remove("NO_PROXY")
        .env_remove("no_proxy")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    let (mut connection, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("proxy child did not connect")
        .unwrap();
    let request = tokio::time::timeout(Duration::from_secs(5), read_request(&mut connection))
        .await
        .expect("proxy child did not send CONNECT");
    assert!(request.starts_with(b"CONNECT registry.example:443 HTTP/1.1\r\n"));
    tokio::time::timeout(Duration::from_secs(5), child.stdin.as_mut().unwrap().write_all(&[1]))
        .await
        .expect("proxy parent did not acknowledge the child")
        .unwrap();

    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
}

#[tokio::test]
async fn test_blob_progress_resets_the_shared_read_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let client = upstream_client(&base, credentials(Auth::None));
    let (request_tx, request_rx) = tokio::sync::oneshot::channel();
    let (chunks_tx, mut chunks_rx) = tokio::sync::mpsc::channel::<(Vec<u8>, tokio::sync::oneshot::Receiver<()>)>(1);
    let peer = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        let _ = read_request(&mut connection).await;
        request_tx.send(()).unwrap();
        connection
            .write_all(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        while let Some((chunk, consumed)) = chunks_rx.recv().await {
            connection
                .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .await
                .unwrap();
            connection.write_all(&chunk).await.unwrap();
            connection.write_all(b"\r\n").await.unwrap();
            consumed.await.unwrap();
        }
        connection.write_all(b"0\r\n\r\n").await.unwrap();
    });
    let pull = tokio::spawn(async move {
        Upstream::new()
            .blob(
                &client,
                "library/nginx",
                "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                &TokenRealms::default(),
            )
            .await
            .unwrap()
    });
    request_rx.await.unwrap();
    let mut response = pull.await.unwrap();
    let mut bytes = Vec::new();
    for chunk in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        let (consumer, pending) = observe_pending(async move {
            let received = response.chunk().await;
            (response, received)
        });
        pending.await.unwrap();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(20)).await;
        tokio::time::resume();
        let (consumed_tx, consumed_rx) = tokio::sync::oneshot::channel();
        chunks_tx.send((chunk.to_vec(), consumed_rx)).await.unwrap();
        let (next_response, received) = consumer.await.unwrap();
        response = next_response;
        bytes.extend_from_slice(&received.unwrap().unwrap());
        consumed_tx.send(()).unwrap();
    }

    assert_eq!(bytes, b"onetwothree");
    drop(chunks_tx);
    assert_eq!(response.chunk().await.unwrap(), None);
    tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_blob_chunk_times_out_after_an_idle_interval() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let client = upstream_client(&base, credentials(Auth::None));
    let (request_tx, request_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::channel::<(Vec<u8>, tokio::sync::oneshot::Receiver<()>)>(1);
    let peer = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        let _ = read_request(&mut connection).await;
        request_tx.send(()).unwrap();
        connection
            .write_all(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        let (chunk, consumed) = chunk_rx.recv().await.unwrap();
        connection
            .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
            .await
            .unwrap();
        connection.write_all(&chunk).await.unwrap();
        connection.write_all(b"\r\n").await.unwrap();
        consumed.await.unwrap();
        let _ = release_rx.await;
    });
    let pull = tokio::spawn(async move {
        Upstream::new()
            .blob(
                &client,
                "library/nginx",
                "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                &TokenRealms::default(),
            )
            .await
            .unwrap()
    });
    request_rx.await.unwrap();
    let mut response = pull.await.unwrap();
    let (consumer, pending) = observe_pending(async move {
        let received = response.chunk().await;
        (response, received)
    });
    pending.await.unwrap();
    let (consumed_tx, consumed_rx) = tokio::sync::oneshot::channel();
    chunk_tx.send((b"one".to_vec(), consumed_rx)).await.unwrap();
    let (next_response, received) = consumer.await.unwrap();
    response = next_response;
    assert_eq!(received.unwrap().unwrap().as_ref(), b"one");
    consumed_tx.send(()).unwrap();

    let (consumer, pending) = observe_pending(async move {
        let received = response.chunk().await;
        (response, received)
    });
    pending.await.unwrap();
    tokio::time::pause();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    tokio::time::advance(Duration::from_secs(30)).await;
    let (response, received) = consumer.await.unwrap();

    assert!(received.unwrap_err().is_timeout());
    assert!(tokio::time::Instant::now() >= deadline);
    assert!(tokio::time::Instant::now() <= deadline + Duration::from_millis(1));
    tokio::time::resume();
    drop(response);
    drop(chunk_tx);
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .unwrap()
        .unwrap();
}

/// A realm the operator did not name receives the token request but not the secret.
async fn assert_token_request_is_anonymous(server: &MockServer, auth: &MockServer, client: &UpstreamClient) {
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&format!("{}/", auth.uri())))
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .and(Unauthenticated)
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#))
        .expect(1)
        .mount(auth)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(server)
        .await;

    let response = Upstream::new()
        .manifest(client, "library/nginx", "latest", &TokenRealms::default())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[rstest]
#[case::basic(Auth::Basic { username: "alice".to_owned(), password: "pw".to_owned() })]
#[case::bearer(Auth::Bearer("configured".to_owned()))]
#[tokio::test]
async fn test_fetch_token_withholds_credentials_from_an_untrusted_realm_origin(#[case] auth: Auth) {
    let server = MockServer::start().await;
    let realm = MockServer::start().await;
    let client = upstream_client(&format!("{}/", server.uri()), credentials(auth));

    assert_token_request_is_anonymous(&server, &realm, &client).await;
}

#[tokio::test]
async fn test_fetch_token_sends_basic_credentials_to_the_upstream_origin() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .and(match_header("authorization", basic_header("alice", "pw").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    // The upstream is reached over cleartext here, which is the case a `localhost` registry serves.
    let response = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(basic("alice", "pw"))),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_fetch_token_sends_basic_credentials_to_a_configured_realm_origin() {
    let server = MockServer::start().await;
    let realm = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&format!("{}/", realm.uri())))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .and(match_header("authorization", basic_header("alice", "pw").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#))
        .expect(1)
        .mount(&realm)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    // Docker Hub's shape: the authorization service answers on an origin of its own.
    let response = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(basic("alice", "pw"))),
            "library/nginx",
            "latest",
            &configured_realms(&[&realm.uri()]),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_fetch_token_names_the_untrusted_realm_origin_it_withheld_from() {
    let server = MockServer::start().await;
    let realm = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&format!("{}/", realm.uri())))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .and(Unauthenticated)
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&realm)
        .await;

    let error = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(basic("alice", "pw"))),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap_err();

    // Only the origin, so the message carries neither the realm path nor the requested scope.
    assert_eq!(
        error.to_string(),
        format!(
            "bearer realm {} is not a trusted token realm for this upstream, so the token request \
             carried no credentials; add it to `token_realms` to authenticate there",
            realm.uri()
        )
    );
}

#[tokio::test]
async fn test_fetch_token_drops_credentials_on_a_redirect_to_another_origin() {
    let server = MockServer::start().await;
    let realm = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .and(match_header("authorization", basic_header("alice", "pw").as_str()))
        .respond_with(ResponseTemplate::new(302).insert_header("location", format!("{}/token", realm.uri()).as_str()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .and(Unauthenticated)
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#))
        .expect(1)
        .mount(&realm)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let response = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(basic("alice", "pw"))),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_fetch_token_keeps_credentials_across_a_redirect_within_the_trusted_origin() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    let credential = basic_header("alice", "pw");
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .and(match_header("authorization", credential.as_str()))
        .respond_with(ResponseTemplate::new(302).insert_header("location", "/auth/token"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/auth/token"))
        .and(match_header("authorization", credential.as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let response = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(basic("alice", "pw"))),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[rstest]
#[case::private_literal("http://169.254.169.254/token", "169.254.169.254 is not a public address")]
#[case::resolves_to_loopback(
    "http://localhost:9/token",
    "host resolves only to non-public addresses; configure `trusted_hosts` to allow it"
)]
#[case::unparseable("http://[", "invalid bearer realm redirect")]
#[tokio::test]
async fn test_fetch_token_rejects_a_realm_redirect_off_the_public_internet(
    #[case] location: &str,
    #[case] reason: &str,
) {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", location))
        .expect(1)
        .mount(&server)
        .await;

    let error = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(basic("alice", "pw"))),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains(reason), "{error}");
}

#[tokio::test]
async fn test_fetch_token_stops_a_realm_that_keeps_redirecting() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(307).insert_header("location", "/token"))
        .expect(4)
        .mount(&server)
        .await;

    let error = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(Auth::None)),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap_err();

    assert_eq!(error.to_string(), "bearer realm redirected more than 3 times");
}

#[tokio::test]
async fn test_fetch_token_reports_a_redirect_without_a_location() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(302))
        .expect(1)
        .mount(&server)
        .await;

    let result = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(Auth::None)),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await;

    assert!(matches!(result, Err(UpstreamError::Status(StatusCode::FOUND))));
}

#[tokio::test]
async fn test_send_does_not_share_a_token_across_providers() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&server)
        .await;

    let upstream = Upstream::new();
    upstream
        .manifest(
            &upstream_client(&base, credentials(basic("alice", "pw"))),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap();
    upstream
        .manifest(
            &upstream_client(&base, credentials(basic("alice", "pw"))),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_token_realm_401_refreshes_the_source_credential_once() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .and(match_header("authorization", basic_header("alice", "old").as_str()))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .and(match_header("authorization", basic_header("alice", "new").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let credentials = CredentialProvider::refreshing(
        basic("alice", "old"),
        CredentialRefresh {
            interval: Duration::from_mins(1),
            on_unauthorized: true,
            failure: CredentialFailure::Fail,
        },
        || async { Ok(basic("alice", "new")) },
    );
    let client = upstream_client(&base, credentials);

    let response = Upstream::new()
        .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_manifest_reports_a_scheduled_credential_refresh_failure() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    let credentials = CredentialProvider::refreshing(
        Auth::None,
        CredentialRefresh {
            interval: Duration::ZERO,
            on_unauthorized: true,
            failure: CredentialFailure::Fail,
        },
        || async { Err(CredentialError::new("source unavailable")) },
    );
    let client = upstream_client(&base, credentials);

    let result = Upstream::new()
        .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
        .await;

    assert!(matches!(result, Err(UpstreamError::Transport(message)) if message == "source unavailable"));
}

#[tokio::test]
async fn test_token_realm_reports_a_source_refresh_failure() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;
    let credentials = CredentialProvider::refreshing(
        Auth::None,
        CredentialRefresh {
            interval: Duration::from_mins(1),
            on_unauthorized: true,
            failure: CredentialFailure::Fail,
        },
        || async { Err(CredentialError::new("source unavailable")) },
    );
    let client = upstream_client(&base, credentials);

    let result = Upstream::new()
        .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
        .await;

    assert!(matches!(result, Err(UpstreamError::Transport(message)) if message == "source unavailable"));
}

#[tokio::test]
async fn test_token_realm_401_stops_when_refresh_is_disabled() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;

    let result = Upstream::new()
        .manifest(
            &upstream_client(&base, credentials(Auth::None)),
            "library/nginx",
            "latest",
            &TokenRealms::default(),
        )
        .await;

    assert!(matches!(result, Err(UpstreamError::Status(StatusCode::UNAUTHORIZED))));
}

#[tokio::test]
async fn test_send_reuses_a_cached_token_for_the_same_credentials() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&server)
        .await;

    let upstream = Upstream::new();
    let client = upstream_client(&base, credentials(basic("alice", "pw1")));
    upstream
        .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
        .await
        .unwrap();
    upstream
        .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_send_discards_a_cached_token_after_credential_refresh() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&server)
        .await;
    let credentials = CredentialProvider::refreshing(
        basic("alice", "pw"),
        CredentialRefresh {
            interval: Duration::ZERO,
            on_unauthorized: false,
            failure: CredentialFailure::Fail,
        },
        || async { Ok(basic("alice", "pw")) },
    );
    let upstream = Upstream::new();
    let client = upstream_client(&base, credentials);

    upstream
        .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
        .await
        .unwrap();
    upstream
        .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
        .await
        .unwrap();
}

/// The barrier makes all pulls contend for one cold token.
async fn concurrent_pulls(
    upstream: &Arc<Upstream>,
    client: &UpstreamClient,
    count: usize,
) -> Vec<Result<StatusCode, UpstreamError>> {
    let barrier = Arc::new(Barrier::new(count));
    let mut pulls = Vec::with_capacity(count);
    for _ in 0..count {
        let upstream = Arc::clone(upstream);
        let client = client.clone();
        let barrier = Arc::clone(&barrier);
        pulls.push(tokio::spawn(async move {
            barrier.wait().await;
            upstream
                .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
                .await
                .map(|response| response.status())
        }));
    }
    let mut outcomes = Vec::with_capacity(count);
    for pull in pulls {
        outcomes.push(pull.await.unwrap());
    }
    outcomes
}

#[tokio::test]
async fn test_token_flight_wait_has_a_deadline() {
    let (_sender, receiver) = broadcast::channel(1);

    assert_eq!(
        wait_for_flight(receiver, Instant::now()).await.unwrap_err().to_string(),
        "upstream request timed out"
    );
}

#[tokio::test]
async fn test_token_flight_retries_when_the_sender_closes() {
    let upstream = Upstream::new();
    let credentials = credentials(basic("alice", "pw"));
    let credential = credentials.credential().await.unwrap();
    let client = upstream_client("https://registry.example/", credentials.clone());
    let cache_key = token_cache_key(
        "https://registry.example/",
        "repository:library/nginx:pull",
        credential.identity().provider(),
    );
    let (sender, _) = broadcast::channel(1);
    upstream.inflight.lock().insert(cache_key.clone(), sender.clone());
    let close = async {
        tokio::task::yield_now().await;
        assert_eq!(sender.receiver_count(), 1);
        upstream.tokens.lock().insert(
            cache_key.clone(),
            credential.identity(),
            "retried".to_owned(),
            i64::MAX,
            0,
        );
        upstream.inflight.lock().remove(&cache_key);
        drop(sender);
    };
    let challenge = Bearer {
        realm: "unreachable".to_owned(),
        service: None,
        scope: None,
    };

    let realms = TokenRealms::default();
    let exchange = TokenExchange {
        challenge: &challenge,
        credentials: &credentials,
        credential: &credential,
        realms: &realms,
    };
    let ((), token) = tokio::join!(
        biased;
        close,
        upstream.acquire_token(&client, &cache_key, None, &exchange),
    );

    assert_eq!(token.unwrap(), "retried");
}

#[tokio::test]
async fn test_token_flight_waiter_returns_the_leader_token() {
    let upstream = Upstream::new();
    let credentials = credentials(basic("alice", "pw"));
    let credential = credentials.credential().await.unwrap();
    let client = upstream_client("https://registry.example/", credentials.clone());
    let cache_key = token_cache_key(
        "https://registry.example/",
        "repository:library/nginx:pull",
        credential.identity().provider(),
    );
    let (sender, _) = broadcast::channel(1);
    upstream.inflight.lock().insert(cache_key.clone(), sender.clone());
    let challenge = Bearer {
        realm: "unreachable".to_owned(),
        service: None,
        scope: None,
    };

    let realms = TokenRealms::default();
    let exchange = TokenExchange {
        challenge: &challenge,
        credentials: &credentials,
        credential: &credential,
        realms: &realms,
    };
    let (token, _) = tokio::join!(
        biased;
        upstream.acquire_token(&client, &cache_key, None, &exchange),
        async { sender.send("shared".to_owned()).unwrap() },
    );

    assert_eq!(token.unwrap(), "shared");
}

#[tokio::test]
async fn test_token_flight_reuses_a_token_cached_after_the_registry_request() {
    let upstream = Upstream::new();
    let credentials = credentials(basic("alice", "pw"));
    let credential = credentials.credential().await.unwrap();
    let base = "https://registry.example/";
    let client = upstream_client(base, credentials.clone());
    let scope = "repository:library/nginx:pull";
    let cache_key = token_cache_key(base, scope, credential.identity().provider());
    upstream.tokens.lock().insert(
        cache_key.clone(),
        credential.identity(),
        "cached".to_owned(),
        i64::MAX,
        0,
    );
    let challenge = Bearer {
        realm: "unreachable".to_owned(),
        service: None,
        scope: None,
    };
    let realms = TokenRealms::default();
    let exchange = TokenExchange {
        challenge: &challenge,
        credentials: &credentials,
        credential: &credential,
        realms: &realms,
    };

    assert_eq!(
        upstream
            .acquire_token(&client, &cache_key, None, &exchange)
            .await
            .unwrap(),
        "cached"
    );
}

#[tokio::test]
async fn test_token_exchange_has_a_deadline() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    let token_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token_base = format!("http://{}/", token_listener.local_addr().unwrap());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&token_base))
        .mount(&server)
        .await;
    let mut upstream = Upstream::new();
    upstream.token_flight_timeout = Duration::from_millis(100);
    let client = upstream_client(&base, credentials(basic("alice", "pw")));
    let manifest = tokio::spawn(async move {
        upstream
            .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
            .await
            .unwrap_err()
            .to_string()
    });
    let _connection = token_listener.accept().await.unwrap();

    assert_eq!(manifest.await.unwrap(), "upstream request timed out");
}

async fn recover_default_token_manifest(
    upstream: Arc<Upstream>,
    client: UpstreamClient,
    listener: &tokio::net::TcpListener,
) -> reqwest::Response {
    tokio::time::timeout(Duration::from_secs(5), async {
        let retry = tokio::spawn(async move {
            upstream
                .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
                .await
        });
        let (mut token, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("healthy token peer did not connect")
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), read_request(&mut token))
            .await
            .expect("healthy token peer did not send headers");
        token
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 15\r\n\r\n{\"token\":\"tok\"}")
            .await
            .unwrap();
        retry.await.unwrap().unwrap()
    })
    .await
    .expect("healthy token recovery did not complete")
}

#[tokio::test]
async fn test_token_exchange_deadline_does_not_reset_for_progress() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    let token_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token_base = format!("http://{}/", token_listener.local_addr().unwrap());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&token_base))
        .mount(&server)
        .await;
    let upstream = Arc::new(Upstream::new());
    let client = upstream_client(&base, credentials(basic("alice", "pw")));
    let started = tokio::time::Instant::now();
    let timed_out = tokio::spawn({
        let upstream = Arc::clone(&upstream);
        let client = client.clone();
        async move {
            upstream
                .manifest(&client, "library/nginx", "latest", &TokenRealms::default())
                .await
        }
    });
    let (token, _) = tokio::time::timeout(Duration::from_secs(5), token_listener.accept())
        .await
        .expect("token peer did not connect")
        .unwrap();
    let (request_tx, request_rx) = tokio::sync::oneshot::channel();
    let (script_tx, mut script_rx) = tokio::sync::mpsc::channel::<(Vec<u8>, tokio::sync::oneshot::Sender<()>)>(1);
    let peer = tokio::spawn(async move {
        let mut token = token;
        let _ = read_request(&mut token).await;
        request_tx.send(()).unwrap();
        while let Some((response, written)) = script_rx.recv().await {
            token.write_all(&response).await.unwrap();
            written.send(()).unwrap();
        }
    });
    tokio::time::timeout(Duration::from_secs(5), request_rx)
        .await
        .expect("token peer did not receive the request")
        .unwrap();
    let token_requested = tokio::time::Instant::now();
    let (written_tx, written_rx) = tokio::sync::oneshot::channel();
    script_tx
        .send((
            b"HTTP/1.1 200 OK\r\ncontent-length: 15\r\n\r\n{\"token\":\"a".to_vec(),
            written_tx,
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), written_rx)
        .await
        .expect("token peer did not write the first progress")
        .unwrap();
    tokio::time::sleep(Duration::from_secs(20)).await;
    let (written_tx, written_rx) = tokio::sync::oneshot::channel();
    script_tx.send((b"b".to_vec(), written_tx)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), written_rx)
        .await
        .expect("token peer did not write the second progress")
        .unwrap();
    assert!(matches!(
        tokio::time::timeout_at(token_requested + Duration::from_secs(35), timed_out)
            .await
            .expect("token exchange did not time out")
            .unwrap(),
        Err(UpstreamError::Timeout)
    ));
    let finished = tokio::time::Instant::now();
    assert!((started + Duration::from_secs(30)..token_requested + Duration::from_secs(50)).contains(&finished));
    drop(script_tx);
    tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .unwrap()
        .unwrap();
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let response = recover_default_token_manifest(Arc::clone(&upstream), client.clone(), &token_listener).await;
    assert_eq!(response.status(), StatusCode::OK);
}

async fn redirect_then_refresh_token(
    listener: &tokio::net::TcpListener,
    old: &str,
    new: &str,
) -> (tokio::net::TcpStream, tokio::time::Instant) {
    let (mut initial, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("initial token peer did not connect")
        .unwrap();
    let request = String::from_utf8(
        tokio::time::timeout(Duration::from_secs(5), read_request(&mut initial))
            .await
            .expect("initial token peer did not receive the request"),
    )
    .unwrap();
    assert!(request.starts_with("GET /token?scope=repository%3Alibrary%2Fnginx%3Apull"));
    assert!(
        request
            .to_ascii_lowercase()
            .contains(&format!("\r\nauthorization: {old}").to_ascii_lowercase())
    );
    let requested = tokio::time::Instant::now();
    tokio::time::sleep(Duration::from_secs(20)).await;
    initial
        .write_all(b"HTTP/1.1 302 Found\r\nlocation: /redirect\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let (mut redirected, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("redirect token peer did not connect")
        .unwrap();
    let request = String::from_utf8(
        tokio::time::timeout(Duration::from_secs(5), read_request(&mut redirected))
            .await
            .expect("redirect token peer did not receive the request"),
    )
    .unwrap();
    assert!(request.starts_with("GET /redirect"));
    assert!(
        request
            .to_ascii_lowercase()
            .contains(&format!("\r\nauthorization: {old}").to_ascii_lowercase())
    );
    redirected
        .write_all(b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let (mut refreshed, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("refreshed token peer did not connect")
        .unwrap();
    let request = String::from_utf8(
        tokio::time::timeout(Duration::from_secs(5), read_request(&mut refreshed))
            .await
            .expect("refreshed token peer did not receive the request"),
    )
    .unwrap();
    assert!(request.starts_with("GET /token?scope=repository%3Alibrary%2Fnginx%3Apull"));
    assert!(
        request
            .to_ascii_lowercase()
            .contains(&format!("\r\nauthorization: {new}").to_ascii_lowercase())
    );
    (refreshed, requested)
}

#[tokio::test]
async fn test_token_exchange_deadline_spans_a_redirect_and_credential_refresh() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    let token_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token_base = format!("http://{}/", token_listener.local_addr().unwrap());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&token_base))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let refreshes = Arc::new(AtomicUsize::new(0));
    let credentials = CredentialProvider::refreshing(
        basic("alice", "old"),
        CredentialRefresh {
            interval: Duration::from_mins(1),
            on_unauthorized: true,
            failure: CredentialFailure::Fail,
        },
        {
            let refreshes = Arc::clone(&refreshes);
            move || {
                let refreshes = Arc::clone(&refreshes);
                async move {
                    refreshes.fetch_add(1, Ordering::SeqCst);
                    Ok(basic("alice", "new"))
                }
            }
        },
    );
    let upstream = Arc::new(Upstream::new());
    let client = upstream_client(&base, credentials);
    let started = tokio::time::Instant::now();
    let timed_out = tokio::spawn({
        let upstream = Arc::clone(&upstream);
        let client = client.clone();
        let token_base = token_base.clone();
        async move {
            upstream
                .manifest(&client, "library/nginx", "latest", &configured_realms(&[&token_base]))
                .await
        }
    });
    let old = basic_header("alice", "old");
    let new = basic_header("alice", "new");
    let (mut refreshed_token, token_requested) = redirect_then_refresh_token(&token_listener, &old, &new).await;
    let deadline = token_requested + Duration::from_secs(30);
    refreshed_token
        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 15\r\nconnection: close\r\n\r\n{\"token\":\"a")
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout_at(deadline + Duration::from_secs(5), timed_out)
            .await
            .expect("redirected token exchange did not time out")
            .unwrap(),
        Err(UpstreamError::Timeout)
    ));
    let finished = tokio::time::Instant::now();
    assert!(finished >= started + Duration::from_secs(30));
    assert!(finished < token_requested + Duration::from_secs(50));
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    drop(refreshed_token);

    let response = tokio::time::timeout(Duration::from_secs(5), async {
        let retry = tokio::spawn({
            let upstream = Arc::clone(&upstream);
            let client = client.clone();
            let token_base = token_base.clone();
            async move {
                upstream
                    .manifest(&client, "library/nginx", "latest", &configured_realms(&[&token_base]))
                    .await
            }
        });
        let (mut healthy, _) = token_listener.accept().await.unwrap();
        let request = String::from_utf8(read_request(&mut healthy).await).unwrap();
        assert!(
            request
                .to_ascii_lowercase()
                .contains(&format!("\r\nauthorization: {new}").to_ascii_lowercase())
        );
        healthy
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 15\r\n\r\n{\"token\":\"tok\"}")
            .await
            .unwrap();
        retry.await.unwrap().unwrap()
    })
    .await
    .expect("healthy redirected token recovery did not complete");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
}

async fn read_request(connection: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut buffer = [0; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = connection.read(&mut buffer).await.unwrap();
        assert_ne!(read, 0, "request ended before its headers");
        request.extend_from_slice(&buffer[..read]);
    }
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_send_coalesces_concurrent_token_exchanges() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .mount(&server)
        .await;
    let (gate, response) = gated_response(ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#));
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(response)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let upstream = Arc::new(Upstream::new());
    let client = upstream_client(&base, credentials(basic("alice", "pw")));
    let pulls = tokio::spawn({
        let upstream = Arc::clone(&upstream);
        let client = client.clone();
        async move { concurrent_pulls(&upstream, &client, 8).await }
    });
    drop(gate.entered().await);
    let outcomes = pulls.await.unwrap();

    for outcome in outcomes {
        assert_eq!(outcome.unwrap(), StatusCode::OK);
    }
    let cached = upstream.tokens.lock().values();
    assert_eq!(cached, ["tok"]);
}

/// The fixture verifies waiter re-election after leader failure.
struct FailThenIssueToken {
    calls: Arc<AtomicUsize>,
    first: ResponseGate,
}
impl wiremock::Respond for FailThenIssueToken {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first.block();
            ResponseTemplate::new(500)
        } else {
            ResponseTemplate::new(200).set_body_string(r#"{"token":"tok"}"#)
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_send_reelects_a_leader_after_a_failed_exchange() {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .mount(&server)
        .await;
    let first = response_gate();
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(FailThenIssueToken {
            calls: Arc::new(AtomicUsize::new(0)),
            first: first.clone(),
        })
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let upstream = Arc::new(Upstream::new());
    let client = upstream_client(&base, credentials(basic("alice", "pw")));
    let pulls = tokio::spawn({
        let upstream = Arc::clone(&upstream);
        let client = client.clone();
        async move { concurrent_pulls(&upstream, &client, 2).await }
    });
    drop(first.entered().await);
    let outcomes = pulls.await.unwrap();

    assert_eq!(
        (
            outcomes.len(),
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Ok(StatusCode::OK)))
                .count(),
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Err(UpstreamError::Status(StatusCode::INTERNAL_SERVER_ERROR))))
                .count(),
        ),
        (2, 1, 1)
    );
}

async fn accept_bearer_challenge(
    registry: &tokio::net::TcpListener,
    realm: &str,
    content_length: usize,
) -> tokio::net::TcpStream {
    let (mut connection, _) = tokio::time::timeout(Duration::from_secs(5), registry.accept())
        .await
        .expect("registry request did not arrive")
        .unwrap();
    let request = tokio::time::timeout(Duration::from_secs(5), read_request(&mut connection))
        .await
        .expect("registry request was incomplete");
    assert!(request.starts_with(b"GET /v2/library/nginx/manifests/latest HTTP/1.1\r\n"));
    connection
        .write_all(
            format!(
                "HTTP/1.1 401 Unauthorized\r\nwww-authenticate: Bearer realm=\"{realm}\",service=reg,scope=\"repository:library/nginx:pull\"\r\ncontent-length: {content_length}\r\nconnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    connection
}

async fn await_follower_drop(follower: &mut tokio::net::TcpStream) {
    let mut eof = [0];
    let read = tokio::time::timeout(Duration::from_secs(5), follower.read(&mut eof))
        .await
        .expect("follower did not drop its incomplete challenge response");
    assert!(matches!(
        read.as_ref().map_err(std::io::Error::kind),
        Ok(0) | Err(std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::ConnectionReset)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn test_send_reelects_a_token_leader_after_cancellation() {
    let registry = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", registry.local_addr().unwrap());
    let token_base = format!("http://{}/", token.local_addr().unwrap());
    let realm = format!("{token_base}token");
    let upstream = Arc::new(Upstream::new());
    let client = upstream_client(&base, credentials(basic("alice", "pw")));
    let manifest = |upstream: Arc<Upstream>, client: UpstreamClient, token_base: String| async move {
        upstream
            .manifest(&client, "library/nginx", "latest", &configured_realms(&[&token_base]))
            .await
    };
    let leader = tokio::spawn(manifest(Arc::clone(&upstream), client.clone(), token_base.clone()));
    let mut initial = accept_bearer_challenge(&registry, &realm, 0).await;
    initial.shutdown().await.unwrap();
    let (mut first_token, _) = tokio::time::timeout(Duration::from_secs(5), token.accept())
        .await
        .expect("leader did not request a token")
        .unwrap();
    let request = tokio::time::timeout(Duration::from_secs(5), read_request(&mut first_token))
        .await
        .expect("leader token request was incomplete");
    assert!(request.starts_with(b"GET /token?"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let waiter = tokio::spawn(manifest(upstream, client, token_base));
    let mut follower = accept_bearer_challenge(&registry, &realm, 1).await;
    await_follower_drop(&mut follower).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), token.accept())
            .await
            .is_err(),
        "follower started a replacement token exchange before the leader was cancelled"
    );
    leader.abort();
    assert!(leader.await.unwrap_err().is_cancelled());
    let _ = first_token
        .write_all(b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
        .await;
    let _ = first_token.shutdown().await;
    drop(follower);
    let (mut replacement, _) = tokio::time::timeout_at(deadline, token.accept())
        .await
        .expect("waiter did not replace the cancelled token leader")
        .unwrap();
    let request = tokio::time::timeout_at(deadline, read_request(&mut replacement))
        .await
        .expect("replacement token request was incomplete");
    assert!(request.starts_with(b"GET /token?"));
    replacement
        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 15\r\nconnection: close\r\n\r\n{\"token\":\"tok\"}")
        .await
        .unwrap();
    replacement.shutdown().await.unwrap();

    let (mut replay, _) = tokio::time::timeout_at(deadline, registry.accept())
        .await
        .expect("waiter did not replay the authenticated registry request")
        .unwrap();
    let request = String::from_utf8(
        tokio::time::timeout_at(deadline, read_request(&mut replay))
            .await
            .expect("authenticated replay was incomplete"),
    )
    .unwrap();
    assert!(
        request
            .to_ascii_lowercase()
            .contains("\r\nauthorization: bearer tok\r\n")
    );
    replay
        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    replay.shutdown().await.unwrap();

    assert_eq!(
        timeout_at(deadline, waiter).await.unwrap().unwrap().unwrap().status(),
        StatusCode::OK
    );
}

/// Pull twice through one client, the second time at `second_pull_at` on the injected clock, and
/// report how many token exchanges the realm saw. One means the first token was still good.
async fn token_exchanges(token_body: &str, second_pull_at: i64) -> usize {
    let server = MockServer::start().await;
    let base = format!("{}/", server.uri());
    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(token_body))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(Unauthenticated)
        .respond_with(challenge(&base))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/library/nginx/manifests/latest"))
        .and(match_header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let now = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let clock = Arc::clone(&now);
    let upstream = Upstream::with_clock(Arc::new(move || clock.load(Ordering::Relaxed)));
    let client = upstream_client(&base, credentials(Auth::None));
    let realms = TokenRealms::default();

    upstream
        .manifest(&client, "library/nginx", "latest", &realms)
        .await
        .unwrap();
    now.store(second_pull_at, Ordering::Relaxed);
    upstream
        .manifest(&client, "library/nginx", "latest", &realms)
        .await
        .unwrap();

    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.url.path() == "/token")
        .count()
}

/// The realm's own fields decide how long a token is reused. The clock starts at the Unix epoch, so
/// an `issued_at` before it is a token already partly spent when it arrived.
#[rstest]
#[case::absent_defaults_to_a_minute(r#"{"token":"tok"}"#, 54, 1)]
#[case::absent_expires(r#"{"token":"tok"}"#, 56, 2)]
#[case::declared(r#"{"token":"tok","expires_in":300}"#, 294, 1)]
#[case::declared_expires(r#"{"token":"tok","expires_in":300}"#, 296, 2)]
#[case::capped_at_an_hour(r#"{"token":"tok","expires_in":86400}"#, 3594, 1)]
#[case::capped_expires(r#"{"token":"tok","expires_in":86400}"#, 3596, 2)]
#[case::malformed_is_not_retained(r#"{"token":"tok","expires_in":"soon"}"#, 0, 2)]
#[case::zero_is_not_retained(r#"{"token":"tok","expires_in":0}"#, 0, 2)]
#[case::negative_is_not_retained(r#"{"token":"tok","expires_in":-30}"#, 0, 2)]
#[case::issued_at_spends_the_lifetime(r#"{"token":"tok","expires_in":300,"issued_at":"1969-12-31T23:55:00Z"}"#, 0, 2)]
#[case::issued_at_spends_part_of_it(r#"{"token":"tok","expires_in":300,"issued_at":"1969-12-31T23:59:00Z"}"#, 234, 1)]
#[case::unreadable_issued_at_starts_at_the_exchange(
    r#"{"token":"tok","expires_in":300,"issued_at":"not-a-date"}"#,
    294,
    1
)]
#[tokio::test]
async fn test_a_token_is_reused_for_the_lifetime_its_realm_declared(
    #[case] token_body: &str,
    #[case] second_pull_at: i64,
    #[case] expected_exchanges: usize,
) {
    assert_eq!(token_exchanges(token_body, second_pull_at).await, expected_exchanges);
}

use axum::http::{Method, StatusCode, header};
use rstest::rstest;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{proxy, proxy_with_settings, send, virtual_stack};
use crate::store;
use crate::{IndexSettings, LibraryPrefix};

async fn mount_tags(server: &MockServer, upstream_repo: &str, body: &'static [u8]) {
    Mock::given(method("GET"))
        .and(path(format!("/v2/{upstream_repo}/tags/list")))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body.to_vec(), "application/json"))
        .mount(server)
        .await;
}

/// A tag-list body of exactly the tags-list ceiling is a legitimate page, not an abusive one: the
/// bound rejects a body that exceeds it, not one that just meets it.
#[tokio::test]
async fn test_proxied_tag_list_accepts_a_body_at_exactly_the_ceiling() {
    const MAX_TAGS_BYTES: usize = 4 * 1024 * 1024;
    let server = MockServer::start().await;
    let prefix = r#"{"name":"app","tags":[],"pad":""#;
    let suffix = "\"}";
    let padding = MAX_TAGS_BYTES - prefix.len() - suffix.len();
    let body = format!("{prefix}{}{suffix}", "x".repeat(padding)).into_bytes();
    assert_eq!(body.len(), MAX_TAGS_BYTES);
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, app) = proxy(&dir, &format!("{}/", server.uri()), false);

    let (status, _, body) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["name"], "hub/app");
}

#[tokio::test]
async fn test_proxied_tag_list_rewrites_name_to_the_client_repository() {
    let server = MockServer::start().await;
    mount_tags(&server, "app", br#"{"name":"app","tags":["v0","v1"]}"#).await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, app) = proxy(&dir, &format!("{}/", server.uri()), false);

    let (status, _, body) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["name"], "hub/app");
    assert_eq!(json["tags"], serde_json::json!(["v0", "v1"]));
}

#[tokio::test]
async fn test_cached_tag_list_serves_the_client_name_without_upstream() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(br#"{"name":"app","tags":["v0"]}"#.to_vec(), "application/json"),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, app) = proxy(&dir, &format!("{}/", server.uri()), false);

    assert_eq!(send(&app, Method::GET, "/v2/hub/app/tags/list").await.0, StatusCode::OK);
    server.reset().await;
    let (status, _, body) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["name"], "hub/app");
    assert_eq!(json["tags"], serde_json::json!(["v0"]));
}

/// A cached tag page is trusted for exactly `ttl_secs`: at that exact age the registry must
/// revalidate against upstream rather than serve the stale page one more time.
#[tokio::test]
async fn test_cached_tag_list_revalidates_exactly_at_the_ttl_boundary() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(br#"{"name":"app","tags":["fresh"]}"#.to_vec(), "application/json"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (state, app) = proxy(&dir, &format!("{}/", server.uri()), false);
    // `proxy` fixes the clock at 1000 and the ttl at 60 seconds, so 940 ages the cached page to
    // exactly the boundary.
    store::set_tag_page(
        &state.serving.meta,
        "hub",
        "app",
        "",
        940,
        None,
        br#"{"name":"app","tags":["cached"]}"#,
    )
    .unwrap();

    let (status, _, body) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["tags"], serde_json::json!(["fresh"]));
}

#[tokio::test]
async fn test_proxied_tag_list_rewrites_name_and_next_link_on_a_page() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .and(query_param("n", "2"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("link", "</v2/app/tags/list?n=2&last=v1>; rel=\"next\"")
                .set_body_raw(br#"{"name":"app","tags":["v0","v1"]}"#.to_vec(), "application/json"),
        )
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, app) = proxy(&dir, &format!("{}/", server.uri()), false);

    let (status, headers, body) = send(&app, Method::GET, "/v2/hub/app/tags/list?n=2").await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["name"], "hub/app");
    assert_eq!(json["tags"], serde_json::json!(["v0", "v1"]));
    assert_eq!(
        headers[header::LINK],
        "</v2/hub/app/tags/list?n=2&last=v1>; rel=\"next\""
    );
}

#[tokio::test]
async fn test_library_prefixed_tag_list_rewrites_upstream_name_to_client_name() {
    let server = MockServer::start().await;
    mount_tags(&server, "library/app", br#"{"name":"library/app","tags":["latest"]}"#).await;
    let dir = tempfile::tempdir().unwrap();
    let settings = IndexSettings {
        library_prefix: LibraryPrefix::Always,
        ..IndexSettings::default()
    };
    let (_state, app) = proxy_with_settings(&dir, &format!("{}/", server.uri()), settings);

    let (status, _, body) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["name"], "hub/app");
    assert_eq!(json["tags"], serde_json::json!(["latest"]));
}

#[tokio::test]
async fn test_virtual_tag_list_names_the_client_repository() {
    let server = MockServer::start().await;
    mount_tags(&server, "app", br#"{"name":"app","tags":["latest"]}"#).await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, app) = virtual_stack(&dir, &format!("{}/", server.uri()));

    let (status, _, body) = send(&app, Method::GET, "/v2/reg/app/tags/list").await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["name"], "reg/app");
    assert!(json["tags"].as_array().unwrap().iter().any(|tag| tag == "latest"));
}

/// A page whose `last` cursor always advances, so each page is cached under its own query and a
/// revisited one cannot masquerade as fresh.
struct EndlessNextPage;

impl wiremock::Respond for EndlessNextPage {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        let last: u64 = request
            .url
            .query_pairs()
            .find_map(|(key, value)| (key == "last").then(|| value.parse().ok()).flatten())
            .unwrap_or(0);
        ResponseTemplate::new(200)
            .insert_header("link", format!("</v2/app/tags/list?last={}>; rel=\"next\"", last + 1))
            .set_body_raw(br#"{"name":"app","tags":[]}"#.to_vec(), "application/json")
    }
}

/// A virtual tag union follows a `next` link across pages by re-issuing the exact query the link
/// names; a query sliced one character short or long would ask upstream for something it never
/// offered.
#[tokio::test]
async fn test_virtual_tag_union_follows_the_exact_next_page_query() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .and(query_param_is_missing("last"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("link", "</v2/app/tags/list?last=b>; rel=\"next\"")
                .set_body_raw(br#"{"name":"app","tags":["a"]}"#.to_vec(), "application/json"),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .and(query_param("last", "b"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(br#"{"name":"app","tags":["b"]}"#.to_vec(), "application/json"),
        )
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, app) = virtual_stack(&dir, &format!("{}/", server.uri()));

    let (status, _, body) = send(&app, Method::GET, "/v2/reg/app/tags/list").await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["tags"], serde_json::json!(["a", "b"]));
}

/// An upstream that never stops linking a next page cannot make a virtual tag union walk forever: the
/// scan stops after exactly `MAX_TAG_PAGES` fetches.
#[tokio::test]
async fn test_virtual_tag_union_pagination_stops_exactly_at_the_page_cap() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .respond_with(EndlessNextPage)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, app) = virtual_stack(&dir, &format!("{}/", server.uri()));

    let (status, ..) = send(&app, Method::GET, "/v2/reg/app/tags/list").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(server.received_requests().await.unwrap().len(), 100);
}

#[rstest]
#[case::null_tags(br#"{"name":"app","tags":null}"#)]
#[case::absent_tags(br#"{"name":"app"}"#)]
#[tokio::test]
async fn test_proxied_tag_list_normalizes_an_empty_tag_set(#[case] body: &'static [u8]) {
    let server = MockServer::start().await;
    mount_tags(&server, "app", body).await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, app) = proxy(&dir, &format!("{}/", server.uri()), false);

    let (status, _, out) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(json["name"], "hub/app");
    assert_eq!(json["tags"], serde_json::json!([]));
}

#[rstest]
#[case::not_an_object(br#"["app"]"#)]
#[case::tags_not_an_array(br#"{"name":"app","tags":"latest"}"#)]
#[case::tags_hold_a_non_string(br#"{"name":"app","tags":[1]}"#)]
#[case::not_json(br"not json")]
#[tokio::test]
async fn test_proxied_tag_list_rejects_a_body_that_is_not_a_tag_list(#[case] body: &'static [u8]) {
    let server = MockServer::start().await;
    mount_tags(&server, "app", body).await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, app) = proxy(&dir, &format!("{}/", server.uri()), false);

    let (status, _, _) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

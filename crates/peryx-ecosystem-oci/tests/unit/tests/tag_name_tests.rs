use axum::http::{Method, StatusCode, header};
use rstest::rstest;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{proxy, proxy_with_clock, proxy_with_settings, send, virtual_stack};
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

struct NextPage {
    terminal: bool,
}

impl wiremock::Respond for NextPage {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        let last: u64 = request
            .url
            .query_pairs()
            .find_map(|(key, value)| (key == "last").then(|| value.parse().ok()).flatten())
            .unwrap_or(0);
        let page = ResponseTemplate::new(200).set_body_raw(br#"{"name":"app","tags":[]}"#.to_vec(), "application/json");
        if self.terminal && last == 99 {
            page
        } else {
            page.insert_header("link", format!("</v2/app/tags/list?last={}>; rel=\"next\"", last + 1))
        }
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

#[rstest]
#[case::next_link(false, StatusCode::BAD_GATEWAY)]
#[case::terminal(true, StatusCode::OK)]
#[tokio::test]
async fn test_virtual_tag_union_handles_the_page_cap(#[case] terminal: bool, #[case] expected: StatusCode) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .respond_with(NextPage { terminal })
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, app) = virtual_stack(&dir, &format!("{}/", server.uri()));

    assert_eq!(send(&app, Method::GET, "/v2/reg/app/tags/list").await.0, expected);
    assert_eq!(server.received_requests().await.unwrap().len(), 100);
}

#[tokio::test]
async fn test_virtual_tag_union_rejects_normalized_cached_cycles_and_recovers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .and(query_param_is_missing("last"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("link", "</v2/app/tags/list?last=a&n=2>; rel=\"next\"")
                .set_body_raw(br#"{"name":"app","tags":["first"]}"#.to_vec(), "application/json"),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .and(query_param("n", "2"))
        .and(query_param("last", "a"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("link", "</v2/app/tags/list?n=2&last=a>; rel=\"next\"")
                .set_body_raw(br#"{"name":"app","tags":["second"]}"#.to_vec(), "application/json"),
        )
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (state, app) = virtual_stack(&dir, &format!("{}/", server.uri()));

    assert_eq!(
        send(&app, Method::GET, "/v2/reg/app/tags/list").await.0,
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        send(&app, Method::GET, "/v2/reg/app/tags/list").await.0,
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    store::set_tag_page(
        &state.serving.meta,
        "hub",
        "app",
        "n=2&last=a",
        1_000,
        None,
        br#"{"name":"app","tags":["second"]}"#,
    )
    .unwrap();
    server.reset().await;

    let (status, _, body) = send(&app, Method::GET, "/v2/reg/app/tags/list").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["tags"],
        serde_json::json!(["first", "second"])
    );
}

#[rstest]
#[case::not_an_object(br#"["app"]"#)]
#[case::positional_array(br#"["app",["v1"]]"#)]
#[case::wrong_name(br#"{"name":"other","tags":["latest"]}"#)]
#[case::absent_tags(br#"{"name":"app"}"#)]
#[case::null_tags(br#"{"name":"app","tags":null}"#)]
#[case::tags_not_an_array(br#"{"name":"app","tags":"latest"}"#)]
#[case::tags_hold_a_non_string(br#"{"name":"app","tags":[1]}"#)]
#[case::hybrid_ordering(br#"{"name":"app","tags":["B","a","A"]}"#)]
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

#[tokio::test]
async fn test_proxied_tag_list_preserves_an_ascii_ordered_upstream_page() {
    let server = MockServer::start().await;
    mount_tags(&server, "app", br#"{"name":"app","tags":["v1","v1","v2"]}"#).await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, app) = proxy(&dir, &format!("{}/", server.uri()), false);

    let (status, _, body) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({"name":"hub/app","tags":["v1","v1","v2"]})
    );
}

#[tokio::test]
async fn test_proxied_tag_list_accepts_a_case_insensitively_ordered_upstream_page() {
    let server = MockServer::start().await;
    mount_tags(&server, "app", br#"{"name":"app","tags":["a","B"]}"#).await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, app) = proxy(&dir, &format!("{}/", server.uri()), false);

    let (status, _, body) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({"name":"hub/app","tags":["a","B"]})
    );
}

#[rstest]
#[case::not_an_object(br#"["app"]"#)]
#[case::positional_array(br#"["app",["v1"]]"#)]
#[case::wrong_name(br#"{"name":"other","tags":["latest"]}"#)]
#[case::absent_tags(br#"{"name":"app"}"#)]
#[case::null_tags(br#"{"name":"app","tags":null}"#)]
#[case::tags_not_an_array(br#"{"name":"app","tags":"latest"}"#)]
#[case::tags_hold_a_non_string(br#"{"name":"app","tags":[1]}"#)]
#[case::hybrid_ordering(br#"{"name":"app","tags":["B","a","A"]}"#)]
#[case::not_json(br"not json")]
#[tokio::test]
async fn test_invalid_cached_tag_list_is_replaced_before_serving(#[case] body: &'static [u8]) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(br#"{"name":"app","tags":["fresh"]}"#.to_vec(), "application/json"),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (state, app) = proxy(&dir, &format!("{}/", server.uri()), false);
    store::set_tag_page(&state.serving.meta, "hub", "app", "", 1_000, None, body).unwrap();

    assert_eq!(send(&app, Method::GET, "/v2/hub/app/tags/list").await.0, StatusCode::OK);
    server.reset().await;
    let (status, _, body) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({"name":"hub/app","tags":["fresh"]})
    );
}

#[rstest]
#[case::zero_limit("n=0&last=a")]
#[case::missing_cursor("n=1")]
#[case::unknown_parameter("ignored=x")]
#[tokio::test]
async fn test_invalid_cached_tag_list_continuation_is_replaced_before_serving(#[case] link: &str) {
    let server = MockServer::start().await;
    let fresh = br#"{"name":"app","tags":["fresh"]}"#;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(fresh.to_vec(), "application/json"))
        .expect(1)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (state, app) = proxy(&dir, &format!("{}/", server.uri()), false);
    store::set_tag_page(
        &state.serving.meta,
        "hub",
        "app",
        "",
        1_000,
        Some(link),
        br#"{"name":"app","tags":["cached"]}"#,
    )
    .unwrap();

    let (status, _, body) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({"name": "hub/app", "tags": ["fresh"]})
    );
    assert_eq!(
        store::tag_page(&state.serving.meta, "hub", "app", "").unwrap(),
        store::TagPageRead::Page((1_000, None, fresh.to_vec()))
    );
}

#[tokio::test]
async fn test_cached_tag_list_with_an_invalid_link_encoding_is_replaced_before_serving() {
    let server = MockServer::start().await;
    let fresh = br#"{"name":"app","tags":["fresh"]}"#;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(fresh.to_vec(), "application/json"))
        .expect(1)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (state, app) = proxy(&dir, &format!("{}/", server.uri()), false);
    state
        .serving
        .meta
        .put_driver_value(
            "oci\0tp\0hub\0app\0",
            &[
                &1_000i64.to_be_bytes()[..],
                &1u32.to_be_bytes()[..],
                &[0xff],
                br#"{"name":"app","tags":["cached"]}"#,
            ]
            .concat(),
        )
        .unwrap();

    let (status, _, body) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({"name": "hub/app", "tags": ["fresh"]})
    );
    assert_eq!(
        store::tag_page(&state.serving.meta, "hub", "app", "").unwrap(),
        store::TagPageRead::Page((1_000, None, fresh.to_vec()))
    );
}

#[tokio::test]
async fn test_malformed_upstream_tag_list_does_not_replace_a_stale_page() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(br#"["app",["v1"]]"#.to_vec(), "application/json"))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (state, app) = proxy_with_clock(&dir, &format!("{}/", server.uri()), std::sync::Arc::new(|| 1_300));
    let link = "</v2/app/tags/list?last=v1>; rel=\"next\"";
    let body = br#"{"name":"app","tags":["v1"]}"#;
    store::set_tag_page(&state.serving.meta, "hub", "app", "", 1_000, Some(link), body).unwrap();

    assert_eq!(
        send(&app, Method::GET, "/v2/hub/app/tags/list").await.0,
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        store::tag_page(&state.serving.meta, "hub", "app", "").unwrap(),
        store::TagPageRead::Page((1_000, Some(link.to_owned()), body.to_vec()))
    );
}

#[tokio::test]
async fn test_stale_clamped_tag_page_without_a_continuation_is_evicted() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (state, app) = proxy_with_clock(&dir, &format!("{}/", server.uri()), std::sync::Arc::new(|| 1_300));
    let tags = (0..1_000).map(|number| format!("tag-{number:04}")).collect::<Vec<_>>();
    store::set_tag_page(
        &state.serving.meta,
        "hub",
        "app",
        "n=1000",
        1_000,
        None,
        serde_json::json!({"name": "app", "tags": tags}).to_string().as_bytes(),
    )
    .unwrap();

    assert_eq!(
        send(&app, Method::GET, "/v2/hub/app/tags/list?n=1001").await.0,
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        store::tag_page(&state.serving.meta, "hub", "app", "n=1000").unwrap(),
        store::TagPageRead::Missing
    );
}

#[tokio::test]
async fn test_truncated_cached_tag_list_is_replaced_before_serving() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(br#"{"name":"app","tags":["fresh"]}"#.to_vec(), "application/json"),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (state, app) = proxy(&dir, &format!("{}/", server.uri()), false);
    state
        .serving
        .meta
        .put_driver_value("oci\0tp\0hub\0app\0", &[0; 7])
        .unwrap();

    assert_eq!(send(&app, Method::GET, "/v2/hub/app/tags/list").await.0, StatusCode::OK);
    server.reset().await;
    let (status, _, body) = send(&app, Method::GET, "/v2/hub/app/tags/list").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({"name":"hub/app","tags":["fresh"]})
    );
}

#[tokio::test]
async fn test_truncated_cached_tag_list_is_evicted_before_a_failed_refetch() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/app/tags/list"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let (state, app) = proxy(&dir, &format!("{}/", server.uri()), false);
    state
        .serving
        .meta
        .put_driver_value("oci\0tp\0hub\0app\0", &[0; 7])
        .unwrap();

    let _ = send(&app, Method::GET, "/v2/hub/app/tags/list").await;

    assert_eq!(
        store::tag_page(&state.serving.meta, "hub", "app", "").unwrap(),
        store::TagPageRead::Missing
    );
}

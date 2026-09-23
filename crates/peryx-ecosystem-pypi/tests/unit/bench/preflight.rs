use super::*;
use std::sync::Arc;

use peryx_bench_core::servers::Server;
use wiremock::matchers::path;
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::super::test_support::{benchmark, http_client};

const SHA256: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[tokio::test]
async fn equivalent_json_and_html_have_the_same_identity() {
    let url = Url::parse("https://fixture.invalid/simple/demo/").unwrap();
    let html = format!(
        "<!doctype html><html><body><a href=\"../../files/demo-1.0-py3-none-any.whl#sha256={SHA256}\">demo-1.0-py3-none-any.whl</a></body></html>"
    );

    assert_eq!(
        candidates_from_response(
            "demo",
            &url,
            "application/vnd.pypi.simple.v1+json",
            page().to_string().as_bytes(),
            &http_client(),
        )
        .await
        .unwrap(),
        candidates_from_response("demo", &url, "text/html", html.as_bytes(), &http_client())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn candidate_without_an_advertised_digest_is_verified_from_its_bytes() {
    let files = MockServer::start().await;
    Mock::given(path("/demo-1.0-py3-none-any.whl"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"demo"))
        .mount(&files)
        .await;
    let page_url = Url::parse(&format!("{}/simple/demo/", files.uri())).unwrap();
    let html = "<a href=\"../../demo-1.0-py3-none-any.whl\">demo-1.0-py3-none-any.whl</a>";

    assert_eq!(
        candidates_from_response("demo", &page_url, "text/html", html.as_bytes(), &http_client())
            .await
            .unwrap(),
        BTreeSet::from([Candidate {
            project: "demo".to_owned(),
            version: "1.0".to_owned(),
            filename: "demo-1.0-py3-none-any.whl".to_owned(),
            sha256: "2a97516c354b68848cdbd8f54a226a0a55b21ed138e207ad6c5cbb9c00aa5aea".to_owned(),
        }])
    );
}

#[tokio::test]
async fn candidate_whose_bytes_cannot_be_fetched_is_named() {
    let files = MockServer::start().await;
    Mock::given(path("/demo-1.0-py3-none-any.whl"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&files)
        .await;
    let page_url = Url::parse(&format!("{}/simple/demo/", files.uri())).unwrap();
    let html = "<a href=\"../../demo-1.0-py3-none-any.whl\">demo-1.0-py3-none-any.whl</a>";

    assert_eq!(
        candidates_from_response("demo", &page_url, "text/html", html.as_bytes(), &http_client())
            .await
            .unwrap_err()
            .to_string(),
        "cannot verify demo-1.0-py3-none-any.whl"
    );
}

#[rstest::rstest]
#[case::missing(&[], &["1.0"], &[])]
#[case::extra(&["1.0", "2.0"], &[], &["2.0"])]
fn a_differing_candidate_set_rejects_the_run(
    #[case] actual: &[&str],
    #[case] missing: &[&str],
    #[case] extra: &[&str],
) {
    assert_eq!(
        verify_candidates("server", &candidates(&["1.0"]), &candidates(actual))
            .unwrap_err()
            .to_string(),
        format!(
            "server resolved a different candidate set: missing {:?}, extra {:?}",
            candidates(missing).iter().collect::<Vec<_>>(),
            candidates(extra).iter().collect::<Vec<_>>()
        )
    );
}

fn candidates(versions: &[&str]) -> BTreeSet<Candidate> {
    versions
        .iter()
        .map(|version| Candidate {
            project: "demo".to_owned(),
            version: (*version).to_owned(),
            filename: format!("demo-{version}-py3-none-any.whl"),
            sha256: SHA256.to_owned(),
        })
        .collect()
}

#[tokio::test]
async fn preflight_accepts_the_recorded_candidate_set() {
    let index = MockServer::start().await;
    Mock::given(path("/simple/demo/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(page().to_string(), SIMPLE_JSON))
        .mount(&index)
        .await;
    let (_directory, context) = benchmark();

    verify(&context, &[server(&index)], &corpus(), &http_client())
        .await
        .unwrap();
}

#[tokio::test]
async fn preflight_reports_an_omitted_project() {
    let index = MockServer::start().await;
    Mock::given(path("/simple/demo/"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&index)
        .await;
    let (_directory, context) = benchmark();

    assert!(
        verify(&context, &[server(&index)], &corpus(), &http_client(),)
            .await
            .unwrap_err()
            .to_string()
            .contains("direct omitted demo")
    );
}

const SIMPLE_JSON: &str = "application/vnd.pypi.simple.v1+json";

fn server(index: &MockServer) -> Server {
    let url = format!("{}/simple/", index.uri());
    Server {
        name: "direct",
        homepage: "https://example.invalid/",
        version: "fixture-v1",
        base_url: Arc::new(move |_| url.clone()),
        probe: Arc::new(str::to_owned),
        command: None,
        setup: None,
        configure: None,
        teardown: None,
    }
}

fn corpus() -> Corpus {
    super::super::corpus::parse(
        &serde_json::json!({
            "schema": 1,
            "pip_version": "26.2.1",
            "python": "3.14.7",
            "platform": "test",
            "roots": ["demo==1.0"],
            "artifacts": [{
                "project": "demo",
                "version": "1.0",
                "filename": "demo-1.0-py3-none-any.whl",
                "url": "https://files.example/demo-1.0-py3-none-any.whl",
                "sha256": SHA256,
                "size": 1,
                "requires_python": null,
                "dependencies": [],
                "requested": true
            }]
        })
        .to_string(),
    )
    .unwrap()
}

fn page() -> serde_json::Value {
    serde_json::json!({
        "meta": {"api-version": "1.1"},
        "name": "demo",
        "versions": ["1.0"],
        "files": [{
            "filename": "demo-1.0-py3-none-any.whl",
            "url": "../../files/demo-1.0-py3-none-any.whl",
            "hashes": {"sha256": SHA256},
            "size": 1
        }]
    })
}

use super::*;
use std::io::Write as _;

use wiremock::matchers::path;
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::super::test_support::http_client;

const METADATA: &[u8] = b"Metadata-Version: 2.4\nName: demo\nVersion: 1.0\nProvides-Extra: example\n\n";

fn artifact(bytes: &[u8], url: &str) -> Artifact {
    Artifact {
        project: "demo".to_owned(),
        version: "1.0".to_owned(),
        filename: "demo-1.0-py3-none-any.whl".to_owned(),
        url: url.to_owned(),
        sha256: hex::encode(Sha256::digest(bytes)),
        size: bytes.len() as u64,
        requires_python: Some(">=3.10".to_owned()),
        dependencies: Vec::new(),
        requested: true,
    }
}

#[test]
fn metadata_is_read_from_the_wheel() {
    let directory = tempfile::tempdir().unwrap();
    let bytes = wheel();
    let path = directory.path().join("demo.whl");
    std::fs::write(&path, &bytes).unwrap();

    assert_eq!(
        wheel_metadata(
            &path,
            &artifact(&bytes, "https://files.invalid/demo-1.0-py3-none-any.whl")
        )
        .unwrap(),
        METADATA
    );
}

#[rstest::rstest]
#[case::two(
    zip_with(&["a-1.0.dist-info/METADATA", "b-1.0.dist-info/METADATA"]),
    "demo-1.0-py3-none-any.whl contains multiple METADATA files"
)]
#[case::none(zip_with(&["demo/__init__.py"]), "demo-1.0-py3-none-any.whl contains no METADATA file")]
#[case::not_a_zip(b"not a wheel".to_vec(), "cannot open demo-1.0-py3-none-any.whl as a wheel")]
fn wheel_without_exactly_one_metadata_is_rejected(#[case] bytes: Vec<u8>, #[case] expected: &str) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("demo.whl");
    std::fs::write(&path, &bytes).unwrap();

    assert_eq!(
        wheel_metadata(
            &path,
            &artifact(&bytes, "https://files.invalid/demo-1.0-py3-none-any.whl")
        )
        .unwrap_err()
        .to_string(),
        expected
    );
}

fn zip_with(names: &[&str]) -> Vec<u8> {
    let mut archive = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for name in names {
        archive
            .start_file(*name, zip::write::SimpleFileOptions::default())
            .unwrap();
        archive.write_all(METADATA).unwrap();
    }
    archive.finish().unwrap().into_inner()
}

#[rstest::rstest]
#[case::matching(b"abc", true)]
#[case::other_digest(b"xyz", false)]
#[case::other_size(b"abcd", false)]
#[tokio::test]
async fn cached_artifact_requires_the_recorded_size_and_digest(#[case] cached: &[u8], #[case] expected: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("artifact");
    tokio::fs::write(&path, cached).await.unwrap();

    assert_eq!(
        verify_file(
            &path,
            &artifact(b"abc", "https://files.invalid/demo-1.0-py3-none-any.whl")
        )
        .await
        .unwrap(),
        expected
    );
}

#[tokio::test]
async fn fixture_hydrates_the_cache_once() {
    let upstream = MockServer::start().await;
    let wheel = wheel();
    Mock::given(path("/demo-1.0-py3-none-any.whl"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(wheel.clone()))
        .expect(1)
        .mount(&upstream)
        .await;
    let corpus = corpus(&format!("{}/demo-1.0-py3-none-any.whl", upstream.uri()), &wheel);
    let directory = tempfile::tempdir().unwrap();
    let client = http_client();
    Fixture::start(&corpus, directory.path(), &client).await.unwrap();
    Fixture::start(&corpus, directory.path(), &client).await.unwrap();
}

#[tokio::test]
async fn fixture_lists_projects_as_json() {
    let (fixture, _directory, client, _corpus, _wheel) = running_fixture().await;

    let projects = client
        .get(&fixture.url)
        .header(header::ACCEPT, "application/vnd.pypi.simple.v1+json")
        .send()
        .await
        .unwrap();

    assert_eq!(
        (
            projects.headers()[header::CONTENT_TYPE].clone(),
            projects.json::<serde_json::Value>().await.unwrap()["projects"].clone()
        ),
        (
            HeaderValue::from_static("application/vnd.pypi.simple.v1+json"),
            serde_json::json!([{"name": "demo"}])
        )
    );
}

#[tokio::test]
async fn fixture_lists_projects_as_html() {
    let (fixture, _directory, client, _corpus, _wheel) = running_fixture().await;

    let projects = client.get(&fixture.url).send().await.unwrap();

    assert_eq!(
        (
            projects.headers()[header::CONTENT_TYPE].clone(),
            projects.text().await.unwrap()
        ),
        (
            HeaderValue::from_static("text/html; charset=utf-8"),
            "<!doctype html><html><body><a href=\"demo/\">demo</a></body></html>".to_owned()
        )
    );
}

#[tokio::test]
async fn fixture_serves_the_wheel_bytes() {
    let (fixture, _directory, client, _corpus, wheel) = running_fixture().await;

    let file_url = file_url(&fixture, &client).await;

    assert_eq!(client.get(file_url).send().await.unwrap().bytes().await.unwrap(), wheel);
}

#[tokio::test]
async fn fixture_serves_the_embedded_metadata() {
    let (fixture, _directory, client, _corpus, _wheel) = running_fixture().await;

    let metadata_url = format!("{}.metadata", file_url(&fixture, &client).await);

    assert_eq!(
        client.get(metadata_url).send().await.unwrap().bytes().await.unwrap(),
        METADATA
    );
}

#[tokio::test]
async fn fixture_escapes_requires_python_in_html() {
    let (fixture, _directory, client, _corpus, _wheel) = running_fixture().await;

    let page = client
        .get(format!("{}demo/", fixture.url))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(page.contains("data-requires-python=\"&gt;=3.10 &amp; &quot;x&quot; &lt; 4\""));
}

#[rstest::rstest]
#[case::project("missing/")]
#[case::file("../files/missing/missing.whl")]
#[tokio::test]
async fn fixture_answers_unknown_paths_with_not_found(#[case] relative: &str) {
    let (fixture, _directory, client, _corpus, _wheel) = running_fixture().await;

    assert_eq!(
        client
            .get(format!("{}{relative}", fixture.url))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn fixture_reports_an_unreadable_cached_wheel() {
    let (fixture, directory, client, corpus, _wheel) = running_fixture().await;
    let artifact = &corpus.artifacts[0];
    tokio::fs::remove_file(directory.path().join("fixture-v1").join(&artifact.sha256))
        .await
        .unwrap();

    assert_eq!(
        client
            .get(format!(
                "{}../files/{}/{}",
                fixture.url, artifact.sha256, artifact.filename
            ))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

async fn file_url(fixture: &Fixture, client: &reqwest::Client) -> url::Url {
    let project_url = url::Url::parse(&format!("{}demo/", fixture.url)).unwrap();
    let detail = client
        .get(project_url.as_str())
        .header(header::ACCEPT, "application/vnd.pypi.simple.v1+json")
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    project_url.join(detail["files"][0]["url"].as_str().unwrap()).unwrap()
}

async fn running_fixture() -> (Fixture, tempfile::TempDir, reqwest::Client, Corpus, Vec<u8>) {
    let upstream = MockServer::start().await;
    let wheel = wheel();
    Mock::given(path("/demo-1.0-py3-none-any.whl"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(wheel.clone()))
        .mount(&upstream)
        .await;
    let corpus = corpus(&format!("{}/demo-1.0-py3-none-any.whl", upstream.uri()), &wheel);
    let directory = tempfile::tempdir().unwrap();
    let client = http_client();
    let fixture = Fixture::start(&corpus, directory.path(), &client).await.unwrap();
    (fixture, directory, client, corpus, wheel)
}

#[tokio::test]
async fn fixture_rejects_artifact_bytes_outside_the_corpus() {
    let upstream = MockServer::start().await;
    Mock::given(path("/demo-1.0-py3-none-any.whl"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"xyz"))
        .mount(&upstream)
        .await;
    let directory = tempfile::tempdir().unwrap();

    assert_eq!(
        Fixture::start(
            &corpus(&format!("{}/demo-1.0-py3-none-any.whl", upstream.uri()), b"abc"),
            directory.path(),
            &http_client()
        )
        .await
        .err()
        .unwrap()
        .to_string(),
        "downloaded artifact does not match demo-1.0-py3-none-any.whl"
    );
    assert!(
        !directory
            .path()
            .join("fixture-v1")
            .join(format!(
                "{}.part",
                artifact(b"abc", "https://files.invalid/demo-1.0-py3-none-any.whl").sha256
            ))
            .exists()
    );
}

#[tokio::test]
async fn fixture_reports_an_upstream_status_failure() {
    let upstream = MockServer::start().await;
    Mock::given(path("/demo-1.0-py3-none-any.whl"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&upstream)
        .await;
    let directory = tempfile::tempdir().unwrap();

    assert!(
        Fixture::start(
            &corpus(&format!("{}/demo-1.0-py3-none-any.whl", upstream.uri()), b"abc"),
            directory.path(),
            &http_client()
        )
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("503")
    );
}

fn corpus(url: &str, bytes: &[u8]) -> Corpus {
    let artifact = artifact(bytes, url);
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
                "url": url,
                "sha256": artifact.sha256,
                "size": artifact.size,
                "requires_python": ">=3.10 & \"x\" < 4",
                "dependencies": [],
                "requested": true
            }]
        })
        .to_string(),
    )
    .unwrap()
}

fn wheel() -> Vec<u8> {
    let mut archive = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    archive
        .start_file(
            "demo/_vendor/example-1.0.dist-info/METADATA",
            zip::write::SimpleFileOptions::default(),
        )
        .unwrap();
    archive
        .write_all(b"Metadata-Version: 2.4\nName: vendored\nVersion: 1.0\n")
        .unwrap();
    archive
        .start_file("demo-1.0.dist-info/METADATA", zip::write::SimpleFileOptions::default())
        .unwrap();
    archive.write_all(METADATA).unwrap();
    archive.finish().unwrap().into_inner()
}

#[tokio::test]
async fn fixture_names_an_unreachable_artifact_url() {
    let closed = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let url = format!("http://{}/demo-1.0-py3-none-any.whl", closed.local_addr().unwrap());
    drop(closed);
    let directory = tempfile::tempdir().unwrap();

    assert_eq!(
        Fixture::start(&corpus(&url, b"abc"), directory.path(), &http_client())
            .await
            .err()
            .unwrap()
            .to_string(),
        format!("cannot download {url}")
    );
}

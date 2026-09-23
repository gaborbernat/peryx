use super::support::*;
use peryx_identity::IndexAcl;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wiremock::{Request as WiremockRequest, Respond};

struct BlockingMetadata {
    body: Vec<u8>,
    entered: Sender<()>,
    release: Mutex<Receiver<()>>,
}

struct InvalidatingDetail {
    body: Vec<u8>,
    state: Arc<AppState>,
}

impl Respond for InvalidatingDetail {
    fn respond(&self, _request: &WiremockRequest) -> ResponseTemplate {
        self.state.serving.invalidate_representations("pypi", "flask");
        ResponseTemplate::new(200)
            .insert_header("cache-control", "max-age=0")
            .set_body_raw(self.body.clone(), "application/vnd.pypi.simple.v1+json")
    }
}

impl Respond for BlockingMetadata {
    fn respond(&self, _request: &WiremockRequest) -> ResponseTemplate {
        self.entered.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .expect("test did not release metadata response");
        ResponseTemplate::new(200).set_body_bytes(self.body.clone())
    }
}

#[tokio::test]
async fn test_legacy_project_json_serves_releases_from_simple_detail() {
    let h = harness().await;
    let wheel_digest = Digest::of(b"wheel");
    let sdist_digest = Digest::of(b"sdist");
    let wheel_url = format!("{}/files/flask-2.0.whl", h.server.uri());
    let sdist_url = format!("{}/files/flask-1.0.tar.gz", h.server.uri());
    let json = format!(
        "{{\"meta\":{{\"api-version\":\"1.4\"}},\"name\":\"flask\",\"versions\":[\"1.0\",\"2.0\"],\
         \"files\":[{{\"filename\":\"flask-2.0-py3-none-any.whl\",\"url\":\"{wheel_url}\",\
         \"hashes\":{{\"sha256\":\"{wheel_digest}\"}},\"requires-python\":\">=3.9\",\
         \"size\":123,\"upload-time\":\"2026-01-01T00:00:00.123456Z\"}},\
         {{\"filename\":\"flask-1.0.tar.gz\",\"url\":\"{sdist_url}\",\
         \"hashes\":{{\"sha256\":\"{sdist_digest}\"}},\"yanked\":\"bad build\",\
         \"size\":456,\"upload-time\":\"2025-12-31T23:59:59Z\"}}]}}",
        wheel_digest = wheel_digest.as_str(),
        sdist_digest = sdist_digest.as_str(),
    );
    Mock::given(method("GET"))
        .and(path("/simple/flask/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(json.into_bytes(), "application/vnd.pypi.simple.v1+json"))
        .mount(&h.server)
        .await;

    let (status, headers, body) = get(&h.state, "/root/pypi/flask/json/", None).await;

    let legacy: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get(header::CONTENT_TYPE).unwrap(), "application/json");
    assert_eq!(
        legacy["info"],
        serde_json::json!({
            "author": "",
            "author_email": "",
            "bugtrack_url": null,
            "classifiers": [],
            "description": "",
            "description_content_type": null,
            "docs_url": null,
            "download_url": "",
            "downloads": {"last_day": -1, "last_month": -1, "last_week": -1},
            "dynamic": [],
            "home_page": "",
            "keywords": "",
            "license": "",
            "license_expression": null,
            "license_files": null,
            "maintainer": "",
            "maintainer_email": "",
            "name": "flask",
            "package_url": "",
            "platform": null,
            "project_url": "",
            "project_urls": {},
            "provides_extra": [],
            "release_url": "",
            "requires_dist": [],
            "requires_python": ">=3.9",
            "summary": "",
            "version": "2.0",
            "yanked": false,
            "yanked_reason": null
        })
    );
    assert_eq!(legacy["urls"], legacy["releases"]["2.0"]);
    assert_eq!(
        legacy["urls"][0],
        serde_json::json!({
            "comment_text": "",
            "digests": {"sha256": wheel_digest.as_str()},
            "downloads": -1,
            "filename": "flask-2.0-py3-none-any.whl",
            "has_sig": false,
            "md5_digest": null,
            "packagetype": "bdist_wheel",
            "python_version": "py3",
            "requires_python": ">=3.9",
            "size": 123,
            "upload_time": "2026-01-01T00:00:00",
            "upload_time_iso_8601": "2026-01-01T00:00:00.123456Z",
            "url": format!("/root/pypi/files/{}/flask-2.0-py3-none-any.whl", wheel_digest.as_str()),
            "yanked": false,
            "yanked_reason": null
        })
    );
    assert_eq!(
        legacy["releases"]["1.0"][0]["url"],
        format!("/root/pypi/files/{}/flask-1.0.tar.gz", sdist_digest.as_str())
    );
    assert_eq!(legacy["vulnerabilities"], serde_json::json!([]));
    assert_eq!(
        legacy["ownership"],
        serde_json::json!({"roles": [], "organization": null})
    );
}

#[tokio::test]
async fn test_legacy_json_populates_info_from_selected_release_metadata() {
    let h = harness().await;
    let wheel = Digest::of(b"wheel");
    let metadata = b"Metadata-Version: 2.4\nName: Flask\nVersion: 2.0\nSummary: Stable build\nRequires-Dist: httpx>=0.27\nClassifier: Framework :: Flask\nLicense-Expression: MIT\nLicense-File: LICENSE\nProject-URL: Docs, https://docs.example\nProvides-Extra: async\nAuthor: Jane\nDescription-Content-Type: text/markdown\n\nLong description";
    let metadata_digest = Digest::of(metadata);
    let file_url = format!("{}/files/flask-2.0.whl", h.server.uri());
    let detail = format!(
        "{{\"meta\":{{\"api-version\":\"1.4\"}},\"name\":\"flask\",\"versions\":[\"1.0\",\"2.0\"],\
         \"files\":[{{\"filename\":\"flask-2.0-py3-none-any.whl\",\"url\":\"{file_url}\",\
         \"hashes\":{{\"sha256\":\"{wheel}\"}},\"size\":5,\"upload-time\":\"2026-01-01T00:00:00Z\",\
         \"core-metadata\":{{\"sha256\":\"{metadata_digest}\"}}}}]}}",
        wheel = wheel.as_str(),
        metadata_digest = metadata_digest.as_str(),
    );
    Mock::given(method("GET"))
        .and(path("/simple/flask/"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(detail.into_bytes(), "application/vnd.pypi.simple.v1+json"),
        )
        .mount(&h.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/files/flask-2.0.whl.metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(metadata))
        .expect(1)
        .mount(&h.server)
        .await;

    let (project_status, _, project_body) = get(&h.state, "/pypi/flask/json", None).await;
    let (release_status, _, release_body) = get(&h.state, "/pypi/flask/2.0/json", None).await;

    assert_eq!(project_status, StatusCode::OK, "{project_body}");
    assert_eq!(release_status, StatusCode::OK, "{release_body}");
    for body in [project_body, release_body] {
        let info = serde_json::from_str::<serde_json::Value>(&body).unwrap()["info"].clone();
        assert_eq!(info["summary"], "Stable build");
        assert_eq!(info["description"], "Long description");
        assert_eq!(info["requires_dist"], serde_json::json!(["httpx>=0.27"]));
        assert_eq!(info["classifiers"], serde_json::json!(["Framework :: Flask"]));
        assert_eq!(info["license_expression"], "MIT");
        assert_eq!(info["license_files"], serde_json::json!(["LICENSE"]));
        assert_eq!(
            info["project_urls"],
            serde_json::json!({"Docs": "https://docs.example"})
        );
        assert_eq!(info["provides_extra"], serde_json::json!(["async"]));
        assert_eq!(info["author"], "Jane");
    }
}

#[tokio::test]
async fn test_legacy_json_generates_selected_metadata_on_first_read() {
    let h = authority_harness().await;
    let metadata =
        b"Metadata-Version: 2.4\nName: peryxpkg\nVersion: 1.0\nSummary: Generated metadata\nRequires-Python: >=3.8\n";
    let wheel = fixture_wheel_with_metadata(metadata);
    let digest = Digest::of(&wheel);
    let filename = "peryxpkg-1.0-py3-none-any.whl";
    let (content_type, body) = multipart_body(&upload_fields(), Some((filename, wheel.as_slice())));
    assert_eq!(
        post_upload(&h.state, "/hosted/", Some(&upload_auth()), &content_type, body).await,
        StatusCode::OK
    );
    let upload_key = format!("pypi\0u\0hosted/peryxpkg/{filename}");
    let mut record: serde_json::Value =
        serde_json::from_slice(&h.state.serving.meta.get_driver_value(&upload_key).unwrap().unwrap()).unwrap();
    record["file"]["core-metadata"] = serde_json::json!(true);
    record["file"]["dist-info-metadata"] = serde_json::json!(true);
    h.state
        .serving
        .meta
        .put_driver_value(&upload_key, &serde_json::to_vec(&record).unwrap())
        .unwrap();
    h.state
        .serving
        .meta
        .delete_driver_value(&format!("pypi\0d\0{}", digest.as_str()))
        .unwrap();
    let selection = crate::store::ReleaseMetadataSelection {
        filename: filename.to_owned(),
        artifact_sha256: digest.as_str().to_owned(),
        metadata: crate::store::ReleaseMetadataLocator::Generated,
    };
    h.state
        .serving
        .meta
        .put_driver_value("pypi\0b\0hosted/peryxpkg/1", &serde_json::to_vec(&selection).unwrap())
        .unwrap();

    let (status, _, body) = get(&h.state, "/hosted/peryxpkg/json", None).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["info"]["summary"],
        "Generated metadata"
    );
}

#[tokio::test]
async fn test_virtual_legacy_json_reads_metadata_from_the_resolved_owner() {
    let first_policy = policy(|_neutral, pypi| pypi.min_release_age_secs = Some(604_800));
    let (h, second) = two_cached_harness(first_policy).await;
    h.clock.store(1_768_003_200, Ordering::Relaxed);
    let digest = Digest::of(b"shared wheel");
    let first_metadata = b"Metadata-Version: 2.4\nName: Flask\nVersion: 1.0\nSummary: Hidden owner\n";
    let second_metadata = b"Metadata-Version: 2.4\nName: Flask\nVersion: 1.0\nSummary: Visible owner\n";
    for (server, uploaded, metadata) in [
        (&h.server, "2026-01-09T00:00:00Z", first_metadata.as_slice()),
        (&second, "2026-01-01T00:00:00Z", second_metadata.as_slice()),
    ] {
        let file_url = format!("{}/files/flask-1.0.whl", server.uri());
        let detail = format!(
            "{{\"meta\":{{\"api-version\":\"1.4\"}},\"name\":\"flask\",\"versions\":[\"1.0\"],\
             \"files\":[{{\"filename\":\"flask-1.0.whl\",\"url\":\"{file_url}\",\
             \"hashes\":{{\"sha256\":\"{digest}\"}},\"size\":5,\"upload-time\":\"{uploaded}\",\
             \"core-metadata\":{{\"sha256\":\"{}\"}}}}]}}",
            Digest::of(metadata).as_str(),
            digest = digest.as_str(),
        );
        Mock::given(method("GET"))
            .and(path("/simple/flask/"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(detail.into_bytes(), "application/vnd.pypi.simple.v1+json"),
            )
            .mount(server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/files/flask-1.0.whl.metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(first_metadata))
        .expect(0)
        .mount(&h.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/files/flask-1.0.whl.metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(second_metadata))
        .expect(1)
        .mount(&second)
        .await;

    let (status, _, body) = get(&h.state, "/combined/flask/json", None).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["info"]["summary"],
        "Visible owner"
    );
}

#[tokio::test]
async fn test_hosted_legacy_json_keeps_first_published_release_metadata() {
    let h = authority_harness().await;
    let first = fixture_wheel_with_build_and_metadata(
        "9",
        b"Metadata-Version: 2.4\nName: peryxpkg\nVersion: 1.0\nSummary: First publication\nRequires-Python: >=3.8\n",
    );
    let second = fixture_wheel_with_metadata(
        b"Metadata-Version: 2.4\nName: peryxpkg\nVersion: 1.0\nSummary: Later publication\nRequires-Python: >=3.8\n",
    );
    for (filename, wheel) in [
        ("peryxpkg-1.0-9-py3-none-any.whl", first.as_slice()),
        ("peryxpkg-1.0-py3-none-any.whl", second.as_slice()),
    ] {
        let (content_type, body) = multipart_body(&upload_fields(), Some((filename, wheel)));
        assert_eq!(
            post_upload(&h.state, "/hosted/", Some(&upload_auth()), &content_type, body).await,
            StatusCode::OK
        );
    }

    let (status, _, body) = get(&h.state, "/hosted/peryxpkg/json", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["info"]["summary"],
        "First publication"
    );

    assert_eq!(
        request(&h.state, "PUT", "/hosted/peryxpkg/1.0/yank", Some(&upload_auth()),).await,
        StatusCode::OK
    );
    let (status, _, body) = get(&h.state, "/hosted/peryxpkg/json", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["info"]["summary"],
        "First publication"
    );
}

#[tokio::test]
async fn test_hosted_legacy_json_rejects_changed_selected_metadata() {
    let h = authority_harness().await;
    let selected = b"Metadata-Version: 2.4\nName: peryxpkg\nVersion: 1.0\nSummary: Selected\nRequires-Python: >=3.8\n";
    let replacement =
        b"Metadata-Version: 2.4\nName: peryxpkg\nVersion: 1.0\nSummary: Replacement\nRequires-Python: >=3.8\n";
    let wheel = fixture_wheel_with_metadata(selected);
    let artifact_digest = Digest::of(&wheel);
    let (content_type, body) = multipart_body(
        &upload_fields(),
        Some(("peryxpkg-1.0-py3-none-any.whl", wheel.as_slice())),
    );
    assert_eq!(
        post_upload(&h.state, "/hosted/", Some(&upload_auth()), &content_type, body).await,
        StatusCode::OK
    );
    let replacement_digest = h.state.serving.blobs.put_bytes(replacement).await.unwrap();
    h.state
        .serving
        .meta
        .put_metadata(artifact_digest.as_str(), replacement_digest.as_str())
        .unwrap();

    let (status, _, body) = get(&h.state, "/hosted/peryxpkg/json", None).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(
        body.contains("selected document does not match its persisted digest"),
        "{body}"
    );
}

#[tokio::test]
async fn test_hosted_legacy_json_rejects_an_invalid_selected_metadata_digest() {
    let h = authority_harness().await;
    let metadata = b"Metadata-Version: 2.4\nName: peryxpkg\nVersion: 1.0\nSummary: Selected\nRequires-Python: >=3.8\n";
    let wheel = fixture_wheel_with_metadata(metadata);
    let filename = "peryxpkg-1.0-py3-none-any.whl";
    let artifact_digest = Digest::of(&wheel);
    let (content_type, body) = multipart_body(&upload_fields(), Some((filename, wheel.as_slice())));
    assert_eq!(
        post_upload(&h.state, "/hosted/", Some(&upload_auth()), &content_type, body).await,
        StatusCode::OK
    );
    let selection = crate::store::ReleaseMetadataSelection {
        filename: filename.to_owned(),
        artifact_sha256: artifact_digest.as_str().to_owned(),
        metadata: crate::store::ReleaseMetadataLocator::Digest("invalid".to_owned()),
    };
    h.state
        .serving
        .meta
        .put_driver_value("pypi\0b\0hosted/peryxpkg/1", &serde_json::to_vec(&selection).unwrap())
        .unwrap();

    let (status, _, body) = get(&h.state, "/hosted/peryxpkg/json", None).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(body.contains("selected metadata digest is invalid"), "{body}");
}

#[tokio::test]
async fn test_hosted_legacy_json_uses_defaults_when_selection_cannot_be_migrated() {
    let h = authority_harness().await;
    let wheel = fixture_wheel_with_metadata(
        b"Metadata-Version: 2.4\nName: peryxpkg\nVersion: 1.0\nSummary: Hidden selection\nRequires-Python: >=3.8\n",
    );
    let filename = "peryxpkg-1.0-py3-none-any.whl";
    let (content_type, body) = multipart_body(&upload_fields(), Some((filename, wheel.as_slice())));
    assert_eq!(
        post_upload(&h.state, "/hosted/", Some(&upload_auth()), &content_type, body).await,
        StatusCode::OK
    );
    let upload_key = format!("pypi\0u\0hosted/peryxpkg/{filename}");
    let mut record: serde_json::Value =
        serde_json::from_slice(&h.state.serving.meta.get_driver_value(&upload_key).unwrap().unwrap()).unwrap();
    record["file"]["hashes"].as_object_mut().unwrap().remove("sha256");
    h.state
        .serving
        .meta
        .put_driver_value(&upload_key, &serde_json::to_vec(&record).unwrap())
        .unwrap();
    h.state
        .serving
        .meta
        .delete_driver_value("pypi\0b\0hosted/peryxpkg/1")
        .unwrap();

    let (status, _, body) = get(&h.state, "/hosted/peryxpkg/json", None).await;

    assert!(status == StatusCode::OK, "{status}: {body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["info"]["summary"],
        ""
    );
}

#[tokio::test]
async fn test_hosted_legacy_json_uses_defaults_when_selection_is_hidden() {
    let h = authority_harness().await;
    let wheel = fixture_wheel_with_metadata(
        b"Metadata-Version: 2.4\nName: peryxpkg\nVersion: 1.0\nSummary: Hidden selection\nRequires-Python: >=3.8\n",
    );
    let (content_type, body) = multipart_body(
        &upload_fields(),
        Some(("peryxpkg-1.0-py3-none-any.whl", wheel.as_slice())),
    );
    assert_eq!(
        post_upload(&h.state, "/hosted/", Some(&upload_auth()), &content_type, body).await,
        StatusCode::OK
    );
    let selection = crate::store::ReleaseMetadataSelection {
        filename: "peryxpkg-1.0-hidden.whl".to_owned(),
        artifact_sha256: "0".repeat(64),
        metadata: crate::store::ReleaseMetadataLocator::Generated,
    };
    h.state
        .serving
        .meta
        .put_driver_value("pypi\0b\0hosted/peryxpkg/1", &serde_json::to_vec(&selection).unwrap())
        .unwrap();

    let (status, _, body) = get(&h.state, "/hosted/peryxpkg/json", None).await;

    assert!(status == StatusCode::OK, "{status}: {body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["info"]["summary"],
        ""
    );
}

#[tokio::test]
async fn test_legacy_json_retries_a_release_changed_during_metadata_fetch() {
    let h = harness().await;
    let metadata = b"Metadata-Version: 2.4\nName: Flask\nVersion: 1.0\nSummary: Racing selection\n";
    let digest = Digest::of(b"wheel");
    let metadata_digest = Digest::of(metadata);
    let file_url = format!("{}/files/flask-1.0.whl", h.server.uri());
    let detail = format!(
        "{{\"meta\":{{\"api-version\":\"1.4\"}},\"name\":\"flask\",\"versions\":[\"1.0\"],\
         \"files\":[{{\"filename\":\"flask-1.0-py3-none-any.whl\",\"url\":\"{file_url}\",\
         \"hashes\":{{\"sha256\":\"{digest}\"}},\"size\":5,\"upload-time\":\"2026-01-01T00:00:00Z\",\
         \"core-metadata\":{{\"sha256\":\"{metadata_digest}\"}}}}]}}",
        digest = digest.as_str(),
        metadata_digest = metadata_digest.as_str(),
    );
    Mock::given(method("GET"))
        .and(path("/simple/flask/"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(detail.into_bytes(), "application/vnd.pypi.simple.v1+json"),
        )
        .mount(&h.server)
        .await;
    let (entered_sender, entered_receiver) = channel();
    let (release_sender, release_receiver) = channel();
    Mock::given(method("GET"))
        .and(path("/files/flask-1.0.whl.metadata"))
        .respond_with(BlockingMetadata {
            body: metadata.to_vec(),
            entered: entered_sender,
            release: Mutex::new(release_receiver),
        })
        .expect(1)
        .mount(&h.server)
        .await;
    let state = h.state.clone();
    let request = tokio::spawn(async move { get(&state, "/pypi/flask/json", None).await });
    tokio::task::spawn_blocking(move || entered_receiver.recv_timeout(Duration::from_secs(2)))
        .await
        .unwrap()
        .expect("metadata request did not enter responder");
    h.state.serving.invalidate_representations("pypi", "flask");
    release_sender.send(()).unwrap();

    let (status, _, body) = request.await.unwrap();

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["info"]["summary"],
        "Racing selection"
    );
}

#[tokio::test]
async fn test_legacy_json_rejects_repeated_changes_during_resolution() {
    let h = harness().await;
    let digest = Digest::of(b"wheel");
    let file_url = format!("{}/files/flask-1.0.whl", h.server.uri());
    let detail = format!(
        "{{\"meta\":{{\"api-version\":\"1.4\"}},\"name\":\"flask\",\"versions\":[\"1.0\"],\
         \"files\":[{{\"filename\":\"flask-1.0.whl\",\"url\":\"{file_url}\",\
         \"hashes\":{{\"sha256\":\"{}\"}},\"size\":5}}]}}",
        digest.as_str(),
    );
    Mock::given(method("GET"))
        .and(path("/simple/flask/"))
        .respond_with(InvalidatingDetail {
            body: detail.into_bytes(),
            state: h.state.clone(),
        })
        .expect(2)
        .mount(&h.server)
        .await;

    let (status, _, body) = get(&h.state, "/pypi/flask/json", None).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        body.contains("selected release changed while reading its metadata"),
        "{body}"
    );
}

#[tokio::test]
async fn test_legacy_json_does_not_fallback_after_selected_metadata_fails() {
    let h = harness().await;
    let first_metadata = b"Metadata-Version: 2.4\nName: other\nVersion: 1.0\nSummary: Wrong project\n";
    let second_metadata = b"Metadata-Version: 2.4\nName: Flask\nVersion: 1.0\nSummary: Fallback\n";
    let first_digest = Digest::of(b"first");
    let second_digest = Digest::of(b"second");
    let first_url = format!("{}/files/flask-1.0-a.whl", h.server.uri());
    let second_url = format!("{}/files/flask-1.0-b.whl", h.server.uri());
    let detail = format!(
        "{{\"meta\":{{\"api-version\":\"1.4\"}},\"name\":\"flask\",\"versions\":[\"1.0\"],\
         \"files\":[{{\"filename\":\"flask-1.0-a.whl\",\"url\":\"{first_url}\",\
         \"hashes\":{{\"sha256\":\"{first_digest}\"}},\"size\":5,\"upload-time\":\"2025-01-01T00:00:00Z\",\
         \"core-metadata\":{{\"sha256\":\"{first_metadata_digest}\"}}}},\
         {{\"filename\":\"flask-1.0-b.whl\",\"url\":\"{second_url}\",\
         \"hashes\":{{\"sha256\":\"{second_digest}\"}},\"size\":6,\"upload-time\":\"2026-01-01T00:00:00Z\",\
         \"core-metadata\":{{\"sha256\":\"{second_metadata_digest}\"}}}}]}}",
        first_digest = first_digest.as_str(),
        second_digest = second_digest.as_str(),
        first_metadata_digest = Digest::of(first_metadata).as_str(),
        second_metadata_digest = Digest::of(second_metadata).as_str(),
    );
    Mock::given(method("GET"))
        .and(path("/simple/flask/"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(detail.into_bytes(), "application/vnd.pypi.simple.v1+json"),
        )
        .mount(&h.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/files/flask-1.0-a.whl.metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(first_metadata))
        .expect(1)
        .mount(&h.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/files/flask-1.0-b.whl.metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(second_metadata))
        .expect(0)
        .mount(&h.server)
        .await;

    let (status, _, body) = get(&h.state, "/pypi/flask/1.0/json", None).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(
        body.contains("selected document names another project or release"),
        "{body}"
    );
}

#[rstest]
#[case::non_utf8(&[0xff], "selected document is not UTF-8")]
#[case::malformed(b"not core metadata", "header line \"not core metadata\" is missing a colon")]
#[tokio::test]
async fn test_legacy_json_rejects_invalid_selected_metadata(#[case] metadata: &[u8], #[case] expected: &str) {
    let h = harness().await;
    let file_url = format!("{}/files/flask-1.0.whl", h.server.uri());
    let detail = format!(
        "{{\"meta\":{{\"api-version\":\"1.4\"}},\"name\":\"flask\",\"versions\":[\"1.0\"],\
         \"files\":[{{\"filename\":\"flask-1.0.whl\",\"url\":\"{file_url}\",\
         \"hashes\":{{\"sha256\":\"{}\"}},\"size\":5,\
         \"core-metadata\":{{\"sha256\":\"{}\"}}}}]}}",
        Digest::of(b"wheel").as_str(),
        Digest::of(metadata).as_str(),
    );
    Mock::given(method("GET"))
        .and(path("/simple/flask/"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(detail.into_bytes(), "application/vnd.pypi.simple.v1+json"),
        )
        .mount(&h.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/files/flask-1.0.whl.metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(metadata))
        .mount(&h.server)
        .await;

    let (status, _, body) = get(&h.state, "/pypi/flask/1.0/json", None).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(body.contains(expected), "{body}");
}

#[tokio::test]
async fn test_legacy_project_json_preserves_upstream_serial() {
    let h = harness().await;
    let digest = Digest::of(b"wheel");
    let file_url = format!("{}/files/flask.whl", h.server.uri());
    Mock::given(method("GET"))
        .and(path("/simple/flask/"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-pypi-last-serial", "42")
                .set_body_raw(
                    detail_json(digest.as_str(), &file_url).into_bytes(),
                    "application/vnd.pypi.simple.v1+json",
                ),
        )
        .mount(&h.server)
        .await;
    mount_metadata(&h.server, &file_url).await;

    let (cold_status, cold_headers, cold_body) = get(&h.state, "/pypi/flask/json", None).await;
    let (hot_status, hot_headers, hot_body) = get(&h.state, "/pypi/flask/json", None).await;

    let cold_legacy: serde_json::Value = serde_json::from_str(&cold_body).unwrap();
    let hot_legacy: serde_json::Value = serde_json::from_str(&hot_body).unwrap();
    assert_eq!(cold_status, StatusCode::OK);
    assert_eq!(cold_headers.get("x-pypi-last-serial").unwrap(), "42");
    assert_eq!(cold_legacy["last_serial"], 42);
    assert_eq!(hot_status, StatusCode::OK);
    assert_eq!(hot_headers.get("x-pypi-last-serial").unwrap(), "42");
    assert_eq!(hot_legacy["last_serial"], 42);
}
#[tokio::test]
async fn test_legacy_release_json_serves_one_version_without_releases() {
    let h = harness().await;
    let digest = Digest::of(b"sdist");
    let file_url = format!("{}/files/flask-1.0.tar.gz", h.server.uri());
    let json = format!(
        "{{\"meta\":{{\"api-version\":\"1.4\"}},\"name\":\"flask\",\"versions\":[\"1.0\",\"2.0\"],\
         \"files\":[{{\"filename\":\"flask-1.0.tar.gz\",\"url\":\"{file_url}\",\
         \"hashes\":{{\"sha256\":\"{digest}\"}},\"yanked\":\"bad build\",\
         \"size\":456,\"upload-time\":\"2025-12-31T23:59:59Z\"}}]}}",
        digest = digest.as_str(),
    );
    Mock::given(method("GET"))
        .and(path("/simple/flask/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(json.into_bytes(), "application/vnd.pypi.simple.v1+json"))
        .mount(&h.server)
        .await;

    let (status, _, body) = get(&h.state, "/root/pypi/flask/1.0/json", None).await;

    let legacy: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(legacy.get("releases"), None);
    assert_eq!(legacy["info"]["version"], "1.0");
    assert_eq!(legacy["info"]["yanked"], true);
    assert_eq!(legacy["info"]["yanked_reason"], "bad build");
    assert_eq!(
        legacy["urls"],
        serde_json::json!([{
            "comment_text": "",
            "digests": {"sha256": digest.as_str()},
            "downloads": -1,
            "filename": "flask-1.0.tar.gz",
            "has_sig": false,
            "md5_digest": null,
            "packagetype": "sdist",
            "python_version": "source",
            "requires_python": null,
            "size": 456,
            "upload_time": "2025-12-31T23:59:59",
            "upload_time_iso_8601": "2025-12-31T23:59:59Z",
            "url": format!("/root/pypi/files/{}/flask-1.0.tar.gz", digest.as_str()),
            "yanked": true,
            "yanked_reason": "bad build"
        }])
    );
}
#[tokio::test]
async fn test_legacy_release_json_unknown_version_is_not_found() {
    let h = harness().await;
    let digest = Digest::of(b"wheel");
    mount_detail(&h.server, digest.as_str(), "http://x/flask-1.0-py3-none-any.whl", None).await;

    let (status, _, body) = get(&h.state, "/pypi/flask/9.9/json", None).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("version Some(\"9.9\") was not found"));
}
#[rstest]
#[case::invalid_project_path("/pypi/%FF/json", "invalid percent-encoded path segment")]
#[case::invalid_versioned_project_path("/pypi/%FF/1.0/json", "invalid percent-encoded path segment")]
#[case::invalid_version_path("/pypi/flask/%FF/json", "invalid percent-encoded path segment")]
#[case::unsafe_project_path("/pypi/flask%2Fbad/json", "invalid project \"flask/bad\"")]
#[case::unsafe_versioned_project_path("/pypi/flask%2Fbad/1.0/json", "invalid project \"flask/bad\"")]
#[case::unsafe_version_path("/pypi/flask/1.0%2Fbad/json", "invalid version \"1.0/bad\"")]
#[tokio::test]
async fn test_legacy_json_rejects_invalid_paths(#[case] uri: &str, #[case] expected: &str) {
    let h = harness().await;
    let (status, _, body) = get(&h.state, uri, None).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains(expected), "{body}");
}
#[tokio::test]
async fn test_legacy_json_missing_project_is_not_found() {
    let h = harness().await;
    Mock::given(method("GET"))
        .and(path("/simple/missing/"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&h.server)
        .await;

    let (status, _, body) = get(&h.state, "/pypi/missing/json", None).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("project \"missing\" was not found on index \"pypi\""));
}
#[tokio::test]
async fn test_legacy_json_unsupported_upstream_content_type_is_bad_gateway() {
    let h = harness().await;
    Mock::given(method("GET"))
        .and(path("/simple/flask/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(b"not an index".to_vec(), "application/octet-stream"))
        .mount(&h.server)
        .await;

    let (status, _, body) = get(&h.state, "/pypi/flask/json", None).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("upstream returned an invalid response"), "{body}");
}
#[tokio::test]
async fn test_legacy_json_forwards_upstream_retry_after() {
    let h = harness().await;
    Mock::given(method("GET"))
        .and(path("/simple/flask/"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "120"))
        .expect(1)
        .mount(&h.server)
        .await;

    let (status, headers, body) = get(&h.state, "/pypi/flask/json", None).await;

    assert_eq!(
        (
            status,
            headers.get(header::RETRY_AFTER).and_then(|value| value.to_str().ok()),
            body.as_str(),
        ),
        (
            StatusCode::TOO_MANY_REQUESTS,
            Some("120"),
            "project detail on index \"pypi\" for project \"flask\": upstream rate limit exceeded",
        )
    );
}
#[tokio::test]
async fn test_legacy_json_unavailable_upstream_is_bad_gateway() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    let blobs = BlobStorage::filesystem(dir.path().join("blobs"));
    let upstream = UpstreamClient::new("http://127.0.0.1:0/simple/").unwrap();
    let state = crate::tests::wired(AppState::new(
        meta,
        blobs,
        60,
        vec![Index {
            name: "pypi".to_owned(),
            route: "pypi".to_owned(),
            ecosystem: crate::ECOSYSTEM,
            kind: IndexKind::Cached {
                client: upstream,
                offline: false,
            },
            policy: Policy::default(),
            acl: IndexAcl::default(),
        }],
    ));

    let (status, _, body) = get(&state, "/pypi/flask/json", None).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("project detail on index \"pypi\" for project \"flask\""));
}
#[tokio::test]
async fn test_legacy_json_is_cached_per_version() {
    let h = harness().await;
    let file_url = format!("{}/files/flask.whl", h.server.uri());
    mount_detail(&h.server, Digest::of(b"wheel-v1").as_str(), &file_url, None).await;

    let (status, _, project) = get(&h.state, "/pypi/flask/json", None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, release) = get(&h.state, "/pypi/flask/1.0/json", None).await;
    assert_eq!(status, StatusCode::OK);
    // The release document is not the project document; one cache key cannot hold both.
    assert_ne!(project, release);

    h.server.reset().await;
    assert_eq!(get(&h.state, "/pypi/flask/json", None).await.2, project);
    assert_eq!(get(&h.state, "/pypi/flask/1.0/json", None).await.2, release);
}
#[tokio::test]
async fn test_proxy_project_list_does_not_fetch_the_upstream_catalog() {
    let h = harness().await;
    Mock::given(method("GET"))
        .and(path("/simple/"))
        .respond_with(ResponseTemplate::new(503))
        .expect(0)
        .mount(&h.server)
        .await;
    let (status, _, body) = get(&h.state, "/pypi/simple/", Some("application/json")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["projects"],
        serde_json::json!([])
    );
}

use std::collections::BTreeSet;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use base64::Engine as _;
use http_body_util::BodyExt as _;
use peryx_ecosystem_pypi::store::PypiStore as _;
use peryx_identity::{GrantScope, Role};
use tower::ServiceExt as _;
use utoipa::openapi::PathsBuilder;

use crate::api::{openapi, openapi_for, openapi_json, openapi_json_for};

// Sorted entries reduce merge conflicts between endpoint additions.
#[test]
fn test_openapi_document_covers_every_endpoint() {
    let spec = serde_json::to_value(openapi()).unwrap();
    let documented: BTreeSet<String> = spec["paths"].as_object().unwrap().keys().cloned().collect();
    let plugin_spec =
        serde_json::to_value(crate::compiled_plugins().openapi_paths(PathsBuilder::new()).build()).unwrap();
    let plugin_paths: BTreeSet<String> = plugin_spec.as_object().unwrap().keys().cloned().collect();
    let core_paths: BTreeSet<String> = documented.difference(&plugin_paths).cloned().collect();
    let expected = BTreeSet::from(
        [
            "/+acl",
            "/+analytics/completeness",
            "/+analytics/sources",
            "/+analytics/timeline",
            "/+analytics/top-resources",
            "/+analytics/unused",
            "/+analytics/groups",
            "/+api",
            "/+availability/operations",
            "/+availability/placements",
            "/+availability/placements/{digest}",
            "/+availability/topology",
            "/+availability/topology/stream",
            "/+grants",
            "/+grants/{id}",
            "/+health",
            "/+jobs/{id}/cancel",
            "/+policy/decisions",
            "/+query",
            "/+quota",
            "/+quota/repository",
            "/+ready",
            "/+repositories",
            "/+repositories/{id}",
            "/+repositories/{id}/disable",
            "/+repositories/{id}/enable",
            "/+retention/export",
            "/+retention/plan",
            "/+revocations",
            "/+revocations/{digest}",
            "/+revocations/{digest}/lift",
            "/+search",
            "/+shadow/candidates",
            "/+stats",
            "/+status",
            "/+tokens",
            "/+tokens/{id}",
            "/+tokens/{id}/rotate",
            "/+trash",
            "/+trash/record",
            "/api-docs/openapi.json",
            "/metrics",
        ]
        .map(str::to_owned),
    );
    assert_eq!(core_paths, expected);
    assert_eq!(spec["info"]["version"], env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn test_shadow_contract_openapi_matches_the_public_handler() {
    let spec = serde_json::to_value(openapi()).unwrap();
    let operation = &spec["paths"]["/+shadow/candidates"]["get"];
    let parameters = operation["parameters"].as_array().unwrap();
    assert_eq!(
        parameters
            .iter()
            .map(|parameter| parameter["name"].as_str().unwrap())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["cursor", "limit", "project", "repository"])
    );

    let directory = tempfile::tempdir().unwrap();
    let state = crate::server::build_state(&crate::config::Config {
        data_dir: directory.path().to_path_buf(),
        ..crate::config::Config::default()
    })
    .unwrap();
    seed_shadow_candidate(&state);
    let user = state.serving.users.create("OpenAPI reader").unwrap();
    state.serving.users.set_password(&user.id, "password").await.unwrap();
    state
        .serving
        .authorization
        .grant(
            &user.id,
            Role::RepositoryReader,
            GrantScope::Repository {
                name: "root-pypi".to_owned(),
            },
        )
        .unwrap();
    let query = parameters
        .iter()
        .filter(|parameter| parameter["required"] == true)
        .map(|parameter| {
            (
                parameter["name"].as_str().unwrap(),
                parameter["example"].as_str().unwrap(),
            )
        });
    let request = Request::builder()
        .uri(format!(
            "/+shadow/candidates?{}",
            url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(query)
                .finish()
        ))
        .header(
            header::AUTHORIZATION,
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode("OpenAPI reader:password")
            ),
        )
        .body(Body::empty())
        .unwrap();
    let response = crate::server::router_for(state).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(
        body["candidates"][0]
            .as_object()
            .unwrap()
            .keys()
            .collect::<BTreeSet<_>>(),
        operation["responses"]["200"]["content"]["application/json"]["example"]["candidates"][0]
            .as_object()
            .unwrap()
            .keys()
            .collect()
    );
}

#[test]
fn test_repository_state_filter_has_a_closed_enum() {
    let spec = serde_json::to_value(openapi()).unwrap();
    let state = spec["paths"]["/+repositories"]["get"]["parameters"]
        .as_array()
        .unwrap()
        .iter()
        .find(|parameter| parameter["name"] == "state")
        .unwrap();

    assert_eq!(
        state["schema"],
        serde_json::json!({"type": "string", "enum": ["enabled", "disabled"]})
    );
}

#[test]
fn test_openapi_json_has_stable_object_order() {
    assert_json_objects_are_sorted(&serde_json::from_str(&openapi_json()).unwrap());
}

#[test]
fn test_none_openapi_omits_distributed_routes() {
    let spec = serde_json::to_value(openapi_for(peryx_ha::AvailabilityResources::None)).unwrap();
    let paths = spec["paths"].as_object().unwrap();

    assert!(!paths.contains_key("/+analytics/completeness"));
    assert!(!paths.keys().any(|path| path.starts_with("/+availability/")));
}

#[test]
fn test_none_openapi_json_matches_the_none_document() {
    let json = openapi_json_for(peryx_ha::AvailabilityResources::None);

    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&json).unwrap(),
        serde_json::to_value(openapi_for(peryx_ha::AvailabilityResources::None)).unwrap()
    );
    assert!(json.ends_with('\n'));
}

fn assert_json_objects_are_sorted(value: &serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => values.iter().for_each(assert_json_objects_are_sorted),
        serde_json::Value::Object(object) => {
            assert!(object.keys().is_sorted(), "object keys are not sorted: {object:?}");
            object.values().for_each(assert_json_objects_are_sorted);
        }
        _ => {}
    }
}

fn seed_shadow_candidate(state: &peryx_driver::AppState) {
    let filename = "acme_pkg-1.0-py3-none-any.whl";
    let uploaded = peryx_ecosystem_pypi::upload::Uploaded {
        version: "1.0".to_owned(),
        file: peryx_ecosystem_pypi::File {
            filename: filename.to_owned(),
            url: format!("https://files.invalid/{filename}"),
            hashes: [("sha256".to_owned(), "1".repeat(64))].into(),
            requires_python: None,
            size: Some(1),
            upload_time: Some("2020-01-01T00:00:00Z".to_owned()),
            yanked: peryx_ecosystem_pypi::Yanked::No,
            core_metadata: peryx_ecosystem_pypi::CoreMetadata::Absent,
            dist_info_metadata: peryx_ecosystem_pypi::CoreMetadata::Absent,
            gpg_sig: None,
            provenance: peryx_ecosystem_pypi::Provenance::Absent,
        },
        trashed: None,
    };
    state
        .serving
        .meta
        .put_upload("hosted", "acme-pkg", filename, &serde_json::to_vec(&uploaded).unwrap())
        .unwrap();
    state
        .serving
        .meta
        .put_project("hosted", "acme-pkg", "acme-pkg")
        .unwrap();
}

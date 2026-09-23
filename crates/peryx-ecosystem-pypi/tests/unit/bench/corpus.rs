use super::*;

fn corpus(artifacts: &serde_json::Value, roots: &serde_json::Value) -> String {
    serde_json::json!({
        "schema": 1,
        "pip_version": "1.0",
        "python": "3.14.7",
        "platform": "test",
        "roots": roots,
        "artifacts": artifacts,
    })
    .to_string()
}

fn artifact(project: &str, version: &str, filename: &str, sha256: &str, requested: bool) -> serde_json::Value {
    serde_json::json!({
        "project": project,
        "version": version,
        "filename": filename,
        "url": format!("https://files.example/{filename}"),
        "sha256": sha256,
        "size": 1,
        "requires_python": ">=3.14",
        "dependencies": [],
        "requested": requested,
    })
}

fn artifact_with(field: &str, value: serde_json::Value) -> serde_json::Value {
    let mut artifact = artifact("demo", "1.0", "demo.whl", &"a".repeat(64), true);
    artifact[field] = value;
    artifact
}

#[cfg(any(
    all(target_os = "macos", target_arch = "aarch64"),
    all(target_os = "linux", target_arch = "x86_64")
))]
#[test]
fn selected_corpus_is_valid() {
    let corpus = selected().unwrap();

    #[cfg(target_os = "macos")]
    let artifacts = 62;
    // torch pulls in its CUDA stack on Linux.
    #[cfg(target_os = "linux")]
    let artifacts = 81;
    assert_eq!(
        (corpus.roots.len(), corpus.artifacts.len(), corpus.candidates().len()),
        (52, artifacts, artifacts)
    );
}

#[test]
fn corpus_normalizes_candidate_identity() {
    let sha256 = "a".repeat(64);
    let parsed = parse(&corpus(
        &serde_json::json!([artifact("Demo_Pkg", "1.0", "demo.whl", &sha256, true)]),
        &serde_json::json!(["demo-pkg==1.0"]),
    ))
    .unwrap();

    assert_eq!(
        parsed.candidates(),
        BTreeSet::from([Candidate {
            project: "demo-pkg".to_owned(),
            version: "1.0".to_owned(),
            filename: "demo.whl".to_owned(),
            sha256,
        }])
    );
}

#[rstest::rstest]
#[case::schema(
    serde_json::json!({
        "schema": 2,
        "pip_version": "1.0",
        "python": "3.14.7",
        "platform": "test",
        "roots": ["demo==1.0"],
        "artifacts": [artifact("demo", "1.0", "demo.whl", &"a".repeat(64), true)],
    })
    .to_string(),
    "unsupported benchmark corpus schema 2"
)]
#[case::empty_versions(
    serde_json::json!({
        "schema": 1,
        "pip_version": "",
        "python": "3.14.7",
        "platform": "test",
        "roots": ["demo==1.0"],
        "artifacts": [artifact("demo", "1.0", "demo.whl", &"a".repeat(64), true)],
    })
    .to_string(),
    "benchmark corpus tool and platform versions must not be empty"
)]
#[case::no_roots(
    corpus(&serde_json::json!([artifact("demo", "1.0", "demo.whl", &"a".repeat(64), true)]), &serde_json::json!([])),
    "benchmark corpus must contain roots and artifacts"
)]
#[case::repeated_project(
    corpus(
        &serde_json::json!([
            artifact("demo", "1.0", "demo.whl", &"a".repeat(64), true),
            artifact("Demo", "1.0", "Demo.whl", &"a".repeat(64), false),
        ]),
        &serde_json::json!(["demo==1.0"]),
    ),
    "benchmark corpus repeats project demo"
)]
#[case::repeated_root(
    corpus(
        &serde_json::json!([artifact("demo", "1.0", "demo.whl", &"a".repeat(64), true)]),
        &serde_json::json!(["demo==1.0", "Demo==1.0"]),
    ),
    "benchmark corpus repeats root demo"
)]
#[case::unrequested_root(
    corpus(
        &serde_json::json!([artifact("demo", "1.0", "demo.whl", &"a".repeat(64), false)]),
        &serde_json::json!(["demo==1.0"]),
    ),
    "benchmark corpus requested artifacts do not match its roots"
)]
#[case::url(
    corpus(
        &serde_json::json!([artifact_with("url", serde_json::json!("https://files.example/other.whl"))]),
        &serde_json::json!(["demo==1.0"]),
    ),
    "benchmark artifact URL does not end with demo.whl"
)]
#[case::empty_metadata(
    corpus(
        &serde_json::json!([artifact_with("requires_python", serde_json::json!(""))]),
        &serde_json::json!(["demo==1.0"]),
    ),
    "benchmark artifact demo.whl has empty metadata"
)]
#[case::floating(
    corpus(
        &serde_json::json!([artifact("demo", "1.0", "demo.whl", &"a".repeat(64), true)]),
        &serde_json::json!(["demo"]),
    ),
    "benchmark root \"demo\" is not exactly pinned"
)]
#[case::missing(
    corpus(
        &serde_json::json!([artifact("other", "1.0", "other.whl", &"a".repeat(64), false)]),
        &serde_json::json!(["demo==1.0"]),
    ),
    "benchmark root \"demo==1.0\" has no artifact"
)]
#[case::version(
    corpus(
        &serde_json::json!([artifact("demo", "2.0", "demo.whl", &"a".repeat(64), true)]),
        &serde_json::json!(["demo==1.0"]),
    ),
    "benchmark root \"demo==1.0\" resolves to demo==2.0"
)]
#[case::digest(
    corpus(
        &serde_json::json!([artifact("demo", "1.0", "demo.whl", "bad", true)]),
        &serde_json::json!(["demo==1.0"]),
    ),
    "benchmark artifact demo.whl has an invalid sha256"
)]
#[case::dependency(
    corpus(
        &serde_json::json!([{
            "project": "demo",
            "version": "1.0",
            "filename": "demo.whl",
            "url": "https://files.example/demo.whl",
            "sha256": "a".repeat(64),
            "size": 1,
            "requires_python": null,
            "dependencies": ["Missing_Dep"],
            "requested": true,
        }]),
        &serde_json::json!(["demo==1.0"]),
    ),
    "benchmark artifact demo.whl depends on Missing_Dep, which the corpus lacks"
)]
#[case::size(
    corpus(
        &serde_json::json!([{
            "project": "demo",
            "version": "1.0",
            "filename": "demo.whl",
            "url": "https://files.example/demo.whl",
            "sha256": "a".repeat(64),
            "size": 0,
            "requires_python": null,
            "dependencies": [],
            "requested": true,
        }]),
        &serde_json::json!(["demo==1.0"]),
    ),
    "benchmark artifact demo.whl has no bytes"
)]
fn invalid_corpus_is_rejected(#[case] raw: String, #[case] expected: &str) {
    assert_eq!(parse(&raw).unwrap_err().to_string(), expected);
}

#[test]
fn corpus_without_a_fleet_root_is_rejected() {
    let parsed = parse(&corpus(
        &serde_json::json!([artifact("demo", "1.0", "demo.whl", &"a".repeat(64), true)]),
        &serde_json::json!(["demo==1.0"]),
    ))
    .unwrap();

    assert_eq!(
        parsed.fleet_root().unwrap_err().to_string(),
        "benchmark corpus has no pinned polars root"
    );
}

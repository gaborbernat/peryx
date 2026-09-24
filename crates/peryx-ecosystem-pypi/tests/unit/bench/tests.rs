use clap::Command;
use peryx_bench_core::context::BenchmarkContext;
use peryx_bench_core::report::load;
use peryx_bench_core::suite::BenchmarkRun;

use super::test_support::http_client;
use super::*;

#[tokio::test]
async fn suite_skips_selected_workloads() {
    let cases: &[(&str, &[&str])] = &[
        ("install", &["install-uv", "install-pip"]),
        ("pip", &["install-pip"]),
        ("throughput", &["throughput"]),
        ("parallel", &["parallel-install"]),
        ("metadata", &["metadata"]),
        ("load", &["load"]),
        ("endpoints", &["endpoints"]),
    ];
    let all = [
        "endpoints",
        "install-pip",
        "install-uv",
        "load",
        "metadata",
        "parallel-install",
        "throughput",
    ];
    for (skip, absent) in cases {
        let directory = tempfile::tempdir().unwrap();
        let report = directory.path().join("report.toml");
        let context = BenchmarkContext::with_scratch("peryx".into(), report.clone(), directory.path().join("scratch"));
        let matches = SUITE.configure(Command::new("bench")).get_matches_from(["bench"]);
        SUITE
            .run(BenchmarkRun {
                context: &context,
                rounds: 0,
                skip: &[(*skip).to_owned()],
                only: "peryx",
                http: &http_client(),
                matches: &matches,
            })
            .await
            .unwrap();
        let tables = load(&report).unwrap().tables;
        let expected = all
            .iter()
            .copied()
            .filter(|name| !absent.contains(name))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            tables
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            expected
        );
    }
}

#[tokio::test]
async fn suite_rejects_unknown_server_selectors() {
    let directory = tempfile::tempdir().unwrap();
    let context = BenchmarkContext::with_scratch(
        "peryx".into(),
        directory.path().join("report.toml"),
        directory.path().join("scratch"),
    );
    let matches = SUITE.configure(Command::new("bench")).get_matches_from(["bench"]);
    let error = SUITE
        .run(BenchmarkRun {
            context: &context,
            rounds: 0,
            skip: &[],
            only: "missing,absent",
            http: &http_client(),
            matches: &matches,
        })
        .await
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("unknown server selectors: missing, absent; valid selectors: "));
    for server in servers::all("https://fixture.invalid/simple/") {
        assert!(error.contains(server.name));
    }
}

#[tokio::test]
async fn a_round_hydrates_the_fixture_and_verifies_every_server_before_timing() {
    let upstream = wiremock::MockServer::start().await;
    let wheel = super::test_support::wheel();
    wiremock::Mock::given(wiremock::matchers::path("/polars-1.0-py3-none-any.whl"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(wheel.clone()))
        .mount(&upstream)
        .await;
    let sha256 = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&wheel));
    let corpus = corpus::parse(
        &serde_json::json!({
            "schema": 1,
            "pip_version": "26.2.1",
            "python": "3.14.7",
            "platform": "test",
            "roots": ["polars==1.0"],
            "artifacts": [{
                "project": "polars",
                "version": "1.0",
                "filename": "polars-1.0-py3-none-any.whl",
                "url": format!("{}/polars-1.0-py3-none-any.whl", upstream.uri()),
                "sha256": sha256,
                "size": wheel.len(),
                "requires_python": null,
                "dependencies": [],
                "requested": true
            }]
        })
        .to_string(),
    )
    .unwrap();
    let (directory, context) = super::test_support::benchmark();
    let skip = ["install", "throughput", "parallel", "metadata", "load", "endpoints"].map(str::to_owned);

    run_suite(&context, &corpus, 1, 1161, &skip, "direct", &http_client())
        .await
        .unwrap();

    let evidence = std::fs::read_to_string(directory.path().join("report.toml"))
        .unwrap()
        .parse::<toml::Table>()
        .unwrap()["evidence"]
        .clone();
    assert_eq!(
        (evidence["schedule"].clone(), evidence["artifacts"].clone()),
        (
            toml::Value::from(vec!["direct:1"]),
            toml::Value::from(vec![format!("polars==1.0 polars-1.0-py3-none-any.whl {sha256}")])
        )
    );
}

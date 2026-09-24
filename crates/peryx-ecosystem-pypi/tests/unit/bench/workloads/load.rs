use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;

use peryx_bench_core::report::load as load_report;
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::super::super::test_support::{
    benchmark, http_client, install_crypto_provider, load_bad_base, load_good_base, server, set_load_bases,
};
use super::*;

#[rstest::rstest]
#[case::server(true, Some(vec![3, 5]))]
#[case::direct(false, None)]
fn requests_align_with_server_costs(#[case] server_ran: bool, #[case] expected: Option<Vec<u64>>) {
    assert_eq!(requests_if_server_ran(server_ran, vec![3, 5]), expected);
}

#[tokio::test]
async fn load_workload_records_successful_and_failed_servers() {
    let good = MockServer::start().await;
    Mock::given(wiremock::matchers::method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("page"))
        .mount(&good)
        .await;
    let bad = MockServer::start().await;
    Mock::given(wiremock::matchers::method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&bad)
        .await;
    set_load_bases(&good, &bad);
    let (directory, context) = benchmark();
    let servers = [
        Server {
            name: "good",
            homepage: "https://example.invalid/",
            version: "1.0.0",
            base_url: Arc::new(load_good_base),
            probe: Arc::new(str::to_owned),
            command: Some(Arc::new(idle_process)),
            setup: None,
            configure: None,
            teardown: None,
        },
        server("bad", load_bad_base),
    ];
    let windows = LoadWindows {
        capacity: Duration::from_millis(250),
        latency: Duration::from_millis(250),
        request: REQUEST_TIMEOUT,
        round: ROUND_TIMEOUT,
        drain: Duration::ZERO,
    };

    load_with_windows(
        &context,
        &servers,
        &[1, 2],
        &Schedule::new(servers.len(), 1, 1161),
        &http_client(),
        &windows,
    )
    .await
    .unwrap();

    let report = load_report(&directory.path().join("report.toml")).unwrap();
    let rows = &report.tables["load"].rows;
    assert_eq!(
        rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
        [
            "1 user: requests/s",
            "1 user: p95 latency",
            "2 users: requests/s",
            "2 users: p95 latency",
            "server CPU per 1k requests",
            "server peak memory",
        ]
    );
    for row in &rows[..4] {
        assert!(row.cells[0].value.is_some());
        assert_eq!(row.cells[1].text, "error");
    }
    assert!(rows[4].cells[0].value.is_some());
}

fn idle_process(_: &BenchmarkContext, _: u16, _: &Path) -> Command {
    #[cfg(unix)]
    let mut command = {
        let mut command = Command::new("sh");
        command.args(["-c", "read value"]);
        command
    };
    #[cfg(windows)]
    let mut command = {
        let mut command = Command::new("cmd");
        command.args(["/C", "set /p value="]);
        command
    };
    command.stdin(Stdio::piped());
    command
}

#[tokio::test]
async fn swarm_and_tail_report_empty_probes() {
    install_crypto_provider();
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let windows = LoadWindows {
        capacity: Duration::from_millis(20),
        latency: Duration::from_millis(20),
        request: REQUEST_TIMEOUT,
        round: ROUND_TIMEOUT,
        drain: Duration::ZERO,
    };
    assert_eq!(
        swarm(&format!("{}/simple/", server.uri()), 1, &windows)
            .await
            .err()
            .unwrap()
            .to_string(),
        "the swarm completed no requests"
    );
    assert_eq!(
        measure_tail(&format!("{}/simple/", server.uri()), 0, 1.0, &windows)
            .await
            .unwrap_err()
            .to_string(),
        "the latency probe recorded no requests"
    );
}

#[tokio::test]
async fn a_request_the_server_never_answers_fails_instead_of_stalling_the_round() {
    install_crypto_provider();
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::method("GET"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_mins(5)))
        .mount(&server)
        .await;
    let windows = LoadWindows {
        capacity: Duration::from_millis(20),
        latency: Duration::from_millis(20),
        request: Duration::from_millis(50),
        round: ROUND_TIMEOUT,
        drain: Duration::ZERO,
    };
    assert_eq!(
        swarm(&format!("{}/simple/", server.uri()), 1, &windows)
            .await
            .err()
            .unwrap()
            .to_string(),
        "the swarm completed no requests"
    );
}

#[tokio::test]
async fn a_round_past_its_deadline_becomes_an_error_cell() {
    let stalled = MockServer::start().await;
    Mock::given(wiremock::matchers::method("GET"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_mins(5)))
        .mount(&stalled)
        .await;
    let base = format!("{}/simple/", stalled.uri());
    let (directory, context) = benchmark();
    let servers = [Server {
        base_url: Arc::new(move |_| base.clone()),
        ..server("stalled", load_bad_base)
    }];
    let windows = LoadWindows {
        capacity: Duration::from_millis(20),
        latency: Duration::from_millis(20),
        request: REQUEST_TIMEOUT,
        round: Duration::from_millis(100),
        drain: Duration::ZERO,
    };

    load_with_windows(
        &context,
        &servers,
        &[1],
        &Schedule::new(servers.len(), 1, 1161),
        &http_client(),
        &windows,
    )
    .await
    .unwrap();

    let report = load_report(&directory.path().join("report.toml")).unwrap();
    assert_eq!(report.tables["load"].rows[0].cells[0].text, "error");
}

#[tokio::test]
async fn each_round_waits_for_the_previous_rounds_sockets_to_expire() {
    let unreachable = MockServer::start().await;
    let base = format!("{}/simple/", unreachable.uri());
    let (_directory, context) = benchmark();
    let servers = [Server {
        base_url: Arc::new(move |_| base.clone()),
        ..server("idle", load_bad_base)
    }];
    let windows = LoadWindows {
        capacity: Duration::from_millis(20),
        latency: Duration::from_millis(20),
        request: REQUEST_TIMEOUT,
        round: ROUND_TIMEOUT,
        drain: Duration::from_millis(300),
    };
    let started = std::time::Instant::now();

    load_with_windows(
        &context,
        &servers,
        &[1],
        &Schedule::new(servers.len(), 2, 1161),
        &http_client(),
        &windows,
    )
    .await
    .unwrap();

    assert!(started.elapsed() >= Duration::from_millis(600));
}

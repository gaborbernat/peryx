use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use peryx_core::PrometheusSource as _;
use peryx_storage::meta::{MetaError, MetaStore};
use tokio::sync::mpsc;

use crate::support::{TestServer, http_contract};
use crate::{
    AvailabilityMetrics, BeaconError, BeaconSender, DEFAULT_BEACON_INTERVAL, HeartbeatError, LivenessTracker,
    liveness_router,
};

const TOKEN: &str = "group-secret";

fn seeded_meta(dir: &tempfile::TempDir, serial: u64) -> MetaStore {
    let meta = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    if serial > 0 {
        let entries: Vec<Vec<u8>> = (0..serial).map(|_| b"beat".to_vec()).collect();
        meta.commit_driver_txn(|_txn| Ok::<((), Vec<Vec<u8>>), MetaError>(((), entries)))
            .unwrap();
    }
    assert_eq!(meta.current_serial().unwrap(), serial);
    meta
}

async fn writer() -> (TestServer, Arc<LivenessTracker>) {
    let tracker = Arc::new(LivenessTracker::new(
        ["replica-a".to_owned()],
        Duration::from_secs(10),
        Duration::from_secs(30),
    ));
    let router = Router::new().nest("/mirror", liveness_router(TOKEN, tracker.clone()).unwrap());
    let mut server = TestServer::start(router).await;
    server.url.push_str("mirror");
    (server, tracker)
}

async fn recording_writer() -> (TestServer, mpsc::UnboundedReceiver<()>) {
    async fn record(State(sender): State<mpsc::UnboundedSender<()>>) -> StatusCode {
        sender.send(()).unwrap();
        StatusCode::NO_CONTENT
    }

    let (sender, receiver) = mpsc::unbounded_channel();
    let router = Router::new()
        .route("/+replication/v1/heartbeat", post(record))
        .with_state(sender);
    (TestServer::start(router).await, receiver)
}

async fn status_writer(status: StatusCode) -> TestServer {
    TestServer::start(Router::new().route("/+replication/v1/heartbeat", post(move || async move { status }))).await
}

fn heartbeat_metrics(metrics: &AvailabilityMetrics) -> String {
    let mut body = String::new();
    metrics.write_metrics(&mut body);
    body
}

fn beacon(upstream: &str) -> (tempfile::TempDir, BeaconSender) {
    let dir = tempfile::tempdir().unwrap();
    let sender = BeaconSender::new(
        upstream,
        TOKEN,
        "replica-a",
        1,
        seeded_meta(&dir, 0),
        DEFAULT_BEACON_INTERVAL,
    )
    .unwrap();
    (dir, sender)
}

#[test]
fn test_configuration_contract() {
    http_contract::assert_configuration(
        |base, token| {
            let dir = tempfile::tempdir().unwrap();
            BeaconSender::new(
                base,
                token,
                "replica-a",
                1,
                seeded_meta(&dir, 0),
                DEFAULT_BEACON_INTERVAL,
            )
            .map(|_| ())
        },
        |error| matches!(error, BeaconError::EmptyToken),
        |error| matches!(error, BeaconError::InvalidBase(_)),
    );
}

#[test]
fn test_new_appends_the_heartbeat_path_to_a_base_without_a_trailing_slash() {
    let dir = tempfile::tempdir().unwrap();
    let beacon = BeaconSender::new(
        "http://writer/api",
        TOKEN,
        "replica-a",
        1,
        seeded_meta(&dir, 0),
        DEFAULT_BEACON_INTERVAL,
    );

    assert!(beacon.is_ok());
}

#[tokio::test]
async fn test_beat_reports_the_current_frontier_and_beacon_position() {
    let (server, tracker) = writer().await;
    let dir = tempfile::tempdir().unwrap();
    let beacon = BeaconSender::new(
        &server.url,
        TOKEN,
        "replica-a",
        7,
        seeded_meta(&dir, 5),
        Duration::from_mins(1),
    )
    .unwrap();

    beacon.beat(3).await.unwrap();

    let now = Instant::now();
    assert_eq!(tracker.applied_frontier("replica-a", now), Some(5));
    let peer = tracker
        .summary(now)
        .into_iter()
        .find(|peer| peer.node == "replica-a")
        .unwrap();
    assert_eq!((peer.incarnation, peer.sequence), (Some(7), Some(3)));
}

#[tokio::test]
async fn test_beat_accepts_success_statuses() {
    for status in [StatusCode::OK, StatusCode::ACCEPTED, StatusCode::NO_CONTENT] {
        let server = status_writer(status).await;
        let (_dir, beacon) = beacon(&server.url);

        assert_eq!(beacon.beat(1).await.map_err(|error| error.to_string()), Ok(()));
    }
}

#[tokio::test]
async fn test_beat_classifies_rejected_statuses() {
    for (status, expected) in [
        (
            StatusCode::UNAUTHORIZED,
            ("authentication", Some(StatusCode::UNAUTHORIZED)),
        ),
        (StatusCode::FORBIDDEN, ("authentication", Some(StatusCode::FORBIDDEN))),
        (StatusCode::CONFLICT, ("stale_incarnation", None)),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ("server", Some(StatusCode::INTERNAL_SERVER_ERROR)),
        ),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            ("server", Some(StatusCode::SERVICE_UNAVAILABLE)),
        ),
        (StatusCode::BAD_REQUEST, ("rejected", Some(StatusCode::BAD_REQUEST))),
    ] {
        let server = status_writer(status).await;
        let (_dir, beacon) = beacon(&server.url);

        let observed = match beacon.beat(1).await.unwrap_err() {
            HeartbeatError::Authentication { status } => ("authentication", Some(status)),
            HeartbeatError::StaleIncarnation => ("stale_incarnation", None),
            HeartbeatError::Server { status } => ("server", Some(status)),
            HeartbeatError::Rejected { status } => ("rejected", Some(status)),
            HeartbeatError::Transport(_) => ("transport", None),
        };
        assert_eq!(observed, expected, "{status}");
    }
}

#[tokio::test]
async fn test_beat_surfaces_and_counts_a_transport_failure() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}/", listener.local_addr().unwrap());
    let close_connection = tokio::spawn(async move {
        let (connection, _) = listener.accept().await.unwrap();
        drop(connection);
    });
    let metrics = Arc::new(AvailabilityMetrics::default());
    let (_dir, beacon) = beacon(&upstream);
    let beacon = beacon.with_metrics(metrics.clone());

    assert!(matches!(beacon.beat(1).await, Err(HeartbeatError::Transport(_))));
    close_connection.await.unwrap();
    assert!(heartbeat_metrics(&metrics).contains("peryx_availability_heartbeat_errors_total{class=\"transport\"} 1\n"));
}

#[tokio::test]
async fn test_beat_counts_repeated_failures_by_bounded_class() {
    let metrics = Arc::new(AvailabilityMetrics::default());
    for (status, class) in [
        (StatusCode::UNAUTHORIZED, "authentication"),
        (StatusCode::CONFLICT, "stale_incarnation"),
        (StatusCode::INTERNAL_SERVER_ERROR, "server"),
        (StatusCode::BAD_REQUEST, "rejected"),
    ] {
        let server = status_writer(status).await;
        let (_dir, beacon) = beacon(&server.url);
        let beacon = beacon.with_metrics(metrics.clone());
        assert!(beacon.beat(1).await.is_err());
        assert!(beacon.beat(2).await.is_err());
        assert!(heartbeat_metrics(&metrics).contains(&format!(
            "peryx_availability_heartbeat_errors_total{{class=\"{class}\"}} 2\n"
        )));
    }
}

#[tokio::test(start_paused = true)]
async fn test_run_beats_each_interval_until_it_is_dropped() {
    let (server, mut beats) = recording_writer().await;
    let dir = tempfile::tempdir().unwrap();
    let interval = Duration::from_secs(5);
    let beacon = BeaconSender::new(&server.url, TOKEN, "replica-a", 1, seeded_meta(&dir, 4), interval).unwrap();
    let running = tokio::spawn(beacon.run());

    beats.recv().await.expect("writer stopped");
    tokio::time::advance(interval).await;
    beats.recv().await.expect("writer stopped");

    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
}

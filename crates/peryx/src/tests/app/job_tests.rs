use peryx_storage::meta::{JobKind, JobOutcome, JobState, NewJobRun};
use rstest::rstest;

use super::*;
use crate::app;
use crate::cli::{JobCommand, JobListArgs, JobShowArgs};

fn list_command() -> JobCommand {
    JobCommand::List(JobListArgs {
        runtime: RuntimeArgs::default(),
    })
}

fn show_command(id: &str) -> JobCommand {
    JobCommand::Show(JobShowArgs {
        runtime: RuntimeArgs::default(),
        id: id.to_owned(),
    })
}

fn run_command(
    repository: &str,
    source: Option<&str>,
    max_projects: usize,
    concurrency: usize,
    timeout_secs: u64,
) -> JobCommand {
    JobCommand::Run {
        runtime: RuntimeArgs::default(),
        repository: repository.to_owned(),
        source: source.map(str::to_owned),
        max_projects,
        concurrency,
        timeout_secs,
    }
}

fn catalog_server() -> (String, std::thread::JoinHandle<()>) {
    use std::io::{Read as _, Write as _};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = std::thread::spawn(move || {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0; 2048];
            let read = socket.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            let body = if request.starts_with("GET /simple/ ") {
                r#"{"meta":{"api-version":"1.4"},"projects":[{"name":"Flask"}]}"#
            } else {
                assert!(request.starts_with("GET /simple/flask/ "), "{request}");
                r#"{"meta":{"api-version":"1.4"},"name":"flask","files":[]}"#
            };
            write!(
                socket,
                "HTTP/1.1 200 OK\r\ncontent-type: application/vnd.pypi.simple.v1+json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    });
    (format!("http://{address}/simple/"), handle)
}

fn start_job(meta: &MetaStore, scope: &str, started_at_unix: i64) -> String {
    meta.start_job_run(NewJobRun {
        kind: JobKind::CacheRefresh,
        scope,
        started_at_unix,
    })
    .unwrap()
}

#[test]
fn test_job_list_prints_newest_first_with_every_state() {
    let (_dir, meta, config) = store_and_config();
    let running = start_job(&meta, "", 10);
    let succeeded = start_job(&meta, "root/pypi", 20);
    meta.finish_job_run(
        &succeeded,
        JobOutcome {
            state: JobState::Succeeded,
            finished_at_unix: 21,
            items_processed: 12,
            items_changed: 3,
            error: None,
        },
    )
    .unwrap();
    let failed = start_job(&meta, "pypi", 30);
    meta.finish_job_run(
        &failed,
        JobOutcome {
            state: JobState::Failed,
            finished_at_unix: 31,
            items_processed: 4,
            items_changed: 1,
            error: Some("upstream unavailable"),
        },
    )
    .unwrap();
    drop(meta);

    let mut out = Vec::new();
    app::job(&config, &list_command(), &mut out).unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        format!(
            "id\tkind\tscope\tstate\tstarted_at_unix\tfinished_at_unix\tprocessed\tchanged\terror\n\
             {failed}\tcache_refresh\tpypi\tfailed\t30\t31\t4\t1\tupstream unavailable\n\
             {succeeded}\tcache_refresh\troot/pypi\tsucceeded\t20\t21\t12\t3\t-\n\
             {running}\tcache_refresh\t-\trunning\t10\t-\t0\t0\t-\n"
        )
    );
}

#[test]
fn test_job_list_empty_prints_header() {
    let (_dir, meta, config) = store_and_config();
    drop(meta);
    let mut out = Vec::new();
    app::job(&config, &list_command(), &mut out).unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "id\tkind\tscope\tstate\tstarted_at_unix\tfinished_at_unix\tprocessed\tchanged\terror\n"
    );
}

#[test]
fn test_job_show_prints_detail() {
    let (_dir, meta, config) = store_and_config();
    let id = start_job(&meta, "root/pypi", 40);
    meta.finish_job_run(
        &id,
        JobOutcome {
            state: JobState::Failed,
            finished_at_unix: 42,
            items_processed: 8,
            items_changed: 2,
            error: Some("timed out"),
        },
    )
    .unwrap();
    drop(meta);

    let mut out = Vec::new();
    app::job(&config, &show_command(&id), &mut out).unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        format!(
            "id\t{id}\nkind\tcache_refresh\nscope\troot/pypi\nstate\tfailed\nstarted_at_unix\t40\n\
             finished_at_unix\t42\nprocessed\t8\nchanged\t2\nerror\ttimed out\n"
        )
    );
}

#[test]
fn test_job_show_rejects_unknown_id() {
    let (_dir, meta, config) = store_and_config();
    drop(meta);
    let err = app::job(&config, &show_command("missing"), &mut Vec::new()).unwrap_err();
    assert!(err.to_string().contains("unknown job run \"missing\""));
}

#[test]
fn test_job_reports_missing_store() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_at(&dir);
    let err = app::job(&config, &list_command(), &mut Vec::new()).unwrap_err();
    assert!(err.to_string().contains("open metadata store"));
}

#[test]
fn test_job_list_propagates_header_write_failure() {
    let (_dir, meta, config) = store_and_config();
    drop(meta);
    let err = app::job(&config, &list_command(), &mut FailImmediately).unwrap_err();
    assert!(err.to_string().contains("write failed"));
}

#[test]
fn test_job_list_propagates_row_write_failure() {
    let (_dir, meta, config) = store_and_config();
    start_job(&meta, "root/pypi", 50);
    drop(meta);
    let err = app::job(
        &config,
        &list_command(),
        &mut FailOnText {
            needle: "cache_refresh",
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("write failed"));
}

#[test]
fn test_job_show_propagates_write_failure() {
    let (_dir, meta, config) = store_and_config();
    let id = start_job(&meta, "root/pypi", 60);
    drop(meta);
    let err = app::job(
        &config,
        &show_command(&id),
        &mut FailOnText {
            needle: "state",
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("write failed"));
}

#[test]
fn test_job_run_executes_the_registered_catalog_job_and_prints_progress() {
    let (upstream, server) = catalog_server();
    let (_dir, meta, mut config) = store_and_config();
    let crate::config::IndexKind::Cached {
        upstream: configured, ..
    } = &mut config.indexes[0].kind
    else {
        panic!("default pypi index is cached");
    };
    *configured = upstream;
    drop(meta);
    let mut out = Vec::new();

    app::job(&config, &run_command("pypi", None, 1, 1, 30), &mut out).unwrap();

    assert_eq!(String::from_utf8(out).unwrap(), "processed\t1\nchanged\t2\n");
    server.join().unwrap();
    let runs = MetaStore::open(config.data_dir.join("peryx.redb"))
        .unwrap()
        .list_job_runs()
        .unwrap();
    assert_eq!(runs[0].kind, JobKind::CatalogSync);
    assert_eq!(runs[0].state, JobState::Succeeded);
    let mut history = Vec::new();
    app::job(&config, &list_command(), &mut history).unwrap();
    assert!(
        String::from_utf8(history)
            .unwrap()
            .contains("\tcatalog_sync\tpypi\tsucceeded\t")
    );
}

#[rstest]
#[case::repository("", None, 1, 1, 1, "repository must not be empty")]
#[case::source("pypi", Some(" "), 1, 1, 1, "source must not be empty")]
#[case::zero_projects("pypi", None, 0, 1, 1, "max-projects must be positive")]
#[case::many_projects("pypi", None, 100_001, 1, 1, "max-projects exceeds the per-run limit")]
#[case::zero_concurrency("pypi", None, 1, 0, 1, "concurrency must be positive")]
#[case::much_concurrency("pypi", None, 1, 33, 1, "concurrency exceeds the per-run limit")]
#[case::zero_timeout("pypi", None, 1, 1, 0, "timeout-secs must be positive")]
#[case::long_timeout("pypi", None, 1, 1, 86_401, "timeout-secs exceeds the per-run limit")]
fn test_job_run_rejects_invalid_limits_before_opening_the_store(
    #[case] repository: &str,
    #[case] source: Option<&str>,
    #[case] max_projects: usize,
    #[case] concurrency: usize,
    #[case] timeout_secs: u64,
    #[case] expected: &str,
) {
    let dir = tempfile::tempdir().unwrap();
    let config = config_at(&dir);

    let error = app::job(
        &config,
        &run_command(repository, source, max_projects, concurrency, timeout_secs),
        &mut Vec::new(),
    )
    .unwrap_err();

    assert_eq!(error.to_string(), expected);
}

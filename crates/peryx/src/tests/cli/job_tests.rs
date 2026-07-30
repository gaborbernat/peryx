use std::path::PathBuf;

use super::parse;
use crate::cli::{Command, JobCommand};

#[test]
fn test_parse_job_list_and_show() {
    let Command::Job(list) = parse(&["peryx", "job", "list", "--data-dir", "/d"]).command else {
        panic!("expected job command");
    };
    assert!(matches!(list, JobCommand::List(_)));
    assert_eq!(list.runtime_args().data_dir, Some(PathBuf::from("/d")));

    let Command::Job(show) = parse(&["peryx", "job", "show", "jr_0000000000000001"]).command else {
        panic!("expected job command");
    };
    let JobCommand::Show(args) = &show else {
        panic!("expected job show");
    };
    assert_eq!(args.id, "jr_0000000000000001");
    assert_eq!(show.runtime_args().data_dir, None);
}

#[test]
fn test_parse_job_run_defaults_and_explicit_limits() {
    let Command::Job(defaults) = parse(&["peryx", "job", "run", "--repository", "pypi"]).command else {
        panic!("expected job command");
    };
    let JobCommand::Run {
        repository,
        source,
        max_projects,
        concurrency,
        timeout_secs,
        ..
    } = defaults
    else {
        panic!("expected job run");
    };
    assert_eq!(repository, "pypi");
    assert_eq!(source, None);
    assert_eq!(max_projects, peryx_driver::jobs::DEFAULT_CATALOG_PROJECTS);
    assert_eq!(concurrency, peryx_driver::jobs::DEFAULT_CATALOG_CONCURRENCY);
    assert_eq!(timeout_secs, peryx_driver::jobs::DEFAULT_CATALOG_TIMEOUT.as_secs());

    let Command::Job(explicit) = parse(&[
        "peryx",
        "job",
        "run",
        "--repository",
        "corp",
        "--source",
        "primary",
        "--max-projects",
        "9",
        "--concurrency",
        "2",
        "--timeout-secs",
        "30",
        "--data-dir",
        "/d",
    ])
    .command
    else {
        panic!("expected job command");
    };
    assert_eq!(explicit.runtime_args().data_dir, Some(PathBuf::from("/d")));
    assert!(matches!(
        explicit,
        JobCommand::Run {
            repository,
            source: Some(source),
            max_projects: 9,
            concurrency: 2,
            timeout_secs: 30,
            ..
        } if repository == "corp" && source == "primary"
    ));
}

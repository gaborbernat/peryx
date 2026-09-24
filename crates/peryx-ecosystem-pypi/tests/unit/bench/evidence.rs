use super::*;

#[test]
fn evidence_records_versions_artifacts_commands_and_schedule() {
    let directory = tempfile::tempdir().unwrap();
    let context = BenchmarkContext::new("peryx".into(), directory.path().join("report.toml"));
    let corpus = super::super::corpus::selected().unwrap();
    let upstream = "http://127.0.0.1:4321/simple/";
    let servers = super::super::servers::all(upstream);

    write(
        &context,
        &servers,
        &corpus,
        &Schedule::new(servers.len(), 2, 7),
        upstream,
    )
    .unwrap();

    let report = std::fs::read_to_string(context.report_path())
        .unwrap()
        .parse::<toml::Table>()
        .unwrap();
    let evidence = report["evidence"].as_table().unwrap();
    let versions = evidence["versions"].as_table().unwrap();
    assert_eq!(evidence["seed"].as_integer(), Some(7));
    assert_eq!(versions["python"].as_str(), Some(corpus.python.as_str()));
    assert_eq!(versions["pip"].as_str(), Some(corpus.pip_version.as_str()));
    assert!(versions["uv"].as_str().unwrap().starts_with("uv "));
    assert_eq!(versions["devpi-client"].as_str(), Some("7.3.0"));
    assert_eq!(versions["pypicloud-python"].as_str(), Some("3.10.21"));
    assert!(!versions["peryx-revision"].as_str().unwrap().is_empty());
    assert_eq!(evidence["artifacts"].as_array().unwrap().len(), corpus.artifacts.len());
    assert_eq!(evidence["schedule"].as_array().unwrap().len(), servers.len() * 2);
    let install = evidence["commands"]["uv install"].as_str().unwrap();
    assert!(install.contains(&corpus.roots[0]));
    assert!(!install.contains("polars=="));
    assert!(
        evidence["commands"]["fleet install"]
            .as_str()
            .unwrap()
            .contains("polars==")
    );
}

#[test]
fn evidence_commands_name_neither_the_fixture_port_nor_the_binary_path() {
    let directory = tempfile::tempdir().unwrap();
    let context = BenchmarkContext::new(
        directory.path().join("target").join("peryx"),
        directory.path().join("report.toml"),
    );
    let upstream = "http://127.0.0.1:4321/simple/";
    let servers = super::super::servers::all(upstream);

    write(
        &context,
        &servers,
        &super::super::corpus::selected().unwrap(),
        &Schedule::new(servers.len(), 1, 7),
        upstream,
    )
    .unwrap();

    let report = std::fs::read_to_string(context.report_path()).unwrap();
    let commands = &report.parse::<toml::Table>().unwrap()["evidence"]["commands"];
    assert_eq!(
        (
            commands["direct"].as_str(),
            commands["peryx"].as_str().map(|command| command.split(' ').next()),
            report.contains("4321") || report.contains(&directory.path().display().to_string()),
        ),
        (Some("GET <fixture>"), Some(Some("\"peryx\"")), false)
    );
}

#[test]
fn evidence_reports_an_unreadable_report() {
    let directory = tempfile::tempdir().unwrap();
    let context = BenchmarkContext::new("peryx".into(), directory.path().to_owned());
    let servers = super::super::servers::all("http://127.0.0.1:4321/simple/");

    let error = write(
        &context,
        &servers,
        &super::super::corpus::selected().unwrap(),
        &Schedule::new(servers.len(), 1, 7),
        "http://127.0.0.1:4321/simple/",
    )
    .unwrap_err();

    assert_eq!(
        error.downcast_ref::<std::io::Error>().map(std::io::Error::kind),
        Some(std::io::ErrorKind::IsADirectory)
    );
}

#[test]
fn evidence_keeps_what_the_report_already_holds() {
    let directory = tempfile::tempdir().unwrap();
    let context = BenchmarkContext::new("peryx".into(), directory.path().join("report.toml"));
    std::fs::write(context.report_path(), "earlier = 1\n").unwrap();
    let servers = super::super::servers::all("http://127.0.0.1:4321/simple/");

    write(
        &context,
        &servers,
        &super::super::corpus::selected().unwrap(),
        &Schedule::new(servers.len(), 1, 7),
        "http://127.0.0.1:4321/simple/",
    )
    .unwrap();

    let report = std::fs::read_to_string(context.report_path())
        .unwrap()
        .parse::<toml::Table>()
        .unwrap();
    assert_eq!(report["earlier"].as_integer(), Some(1));
}

#[test]
fn evidence_rejects_a_malformed_report() {
    let directory = tempfile::tempdir().unwrap();
    let context = BenchmarkContext::new("peryx".into(), directory.path().join("report.toml"));
    std::fs::write(context.report_path(), "not = [toml").unwrap();
    let servers = super::super::servers::all("http://127.0.0.1:4321/simple/");

    assert_eq!(
        write(
            &context,
            &servers,
            &super::super::corpus::selected().unwrap(),
            &Schedule::new(servers.len(), 1, 7),
            "http://127.0.0.1:4321/simple/",
        )
        .unwrap_err()
        .to_string(),
        "existing report is not valid TOML"
    );
}

#[test]
fn command_output_reports_start_failure() {
    assert!(
        command_output("peryx-command-that-does-not-exist", &[])
            .unwrap_err()
            .to_string()
            .contains("cannot run")
    );
}

#[test]
fn command_output_reports_unsuccessful_process() {
    assert!(
        command_output("rustc", &["--peryx-invalid-option"])
            .unwrap_err()
            .to_string()
            .contains("failed")
    );
}

#[test]
fn command_output_rejects_empty_stdout() {
    #[cfg(unix)]
    let result = command_output("true", &[]);
    #[cfg(windows)]
    let result = command_output("cmd", &["/C", "exit", "0"]);

    assert!(result.unwrap_err().to_string().contains("returned no output"));
}

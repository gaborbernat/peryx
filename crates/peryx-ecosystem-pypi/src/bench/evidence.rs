use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use anyhow::{Context as _, bail};
use peryx_bench_core::context::BenchmarkContext;
use peryx_bench_core::servers::Server;
use serde::Serialize;

use super::corpus::Corpus;
use super::schedule::Schedule;

const SCHEMA: u64 = 1;

#[derive(Serialize)]
struct Evidence {
    schema: u64,
    seed: u64,
    platform: String,
    python: String,
    pip: String,
    roots: Vec<String>,
    artifacts: Vec<String>,
    versions: BTreeMap<String, String>,
    commands: BTreeMap<String, String>,
    schedule: Vec<String>,
}

pub(super) fn write(
    context: &BenchmarkContext,
    servers: &[Server],
    corpus: &Corpus,
    schedule: &Schedule,
    upstream: &str,
) -> anyhow::Result<()> {
    let mut report: toml::Table = match std::fs::read_to_string(context.report_path()) {
        Ok(existing) => existing.parse().context("existing report is not valid TOML")?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => toml::Table::new(),
        Err(error) => return Err(error.into()),
    };
    let mut commands = servers
        .iter()
        .map(|server| {
            let command = server.command.as_ref().map_or_else(
                || format!("GET {}", (server.base_url)(0)),
                |build| format!("{:?}", build(context, 0, Path::new("<state>"))),
            );
            // The fixture's port and the binary's path change per run and per machine; the evidence names neither.
            let command = command
                .replace(upstream, "<fixture>")
                .replace(&context.peryx_binary().display().to_string(), "peryx");
            (server.name.to_owned(), command)
        })
        .collect::<BTreeMap<_, _>>();
    let fleet = corpus.fleet_root()?;
    let roots = corpus
        .roots
        .iter()
        .map(String::as_str)
        .filter(|root| *root != fleet)
        .collect::<Vec<_>>()
        .join(" ");
    commands.insert(
        "devpi mirror".to_owned(),
        "devpi use <server>; devpi login root --password ''; devpi index root/pypi mirror_url=<fixture>".to_owned(),
    );
    commands.insert(
        "pip install".to_owned(),
        format!(
            "uv venv --python {} <venv>; uv pip install --python <venv>/bin/python pip=={}; \
             <venv>/bin/pip install --index-url <server> --only-binary :all: {roots}",
            corpus.python, corpus.pip_version
        ),
    );
    commands.insert(
        "uv install".to_owned(),
        format!(
            "uv venv --python {} <venv>; uv pip install --index-url <server> --only-binary :all: {roots}",
            corpus.python
        ),
    );
    commands.insert(
        "fleet install".to_owned(),
        format!(
            "uv venv --python {} <venv>; uv pip install --index-url <server> --only-binary :all: {fleet}",
            corpus.python
        ),
    );
    let mut versions = servers
        .iter()
        .map(|server| (server.name.to_owned(), server.version.to_owned()))
        .collect::<BTreeMap<_, _>>();
    versions.extend(super::servers::component_versions(servers));
    versions.insert("pip".to_owned(), corpus.pip_version.clone());
    versions.insert("python".to_owned(), corpus.python.clone());
    versions.insert("uv".to_owned(), command_output("uv", &["--version"])?);
    if servers.iter().any(|server| server.name == "peryx") {
        versions.insert(
            "peryx-revision".to_owned(),
            command_output("git", &["describe", "--always", "--dirty"])?,
        );
    }
    let evidence = Evidence {
        schema: SCHEMA,
        seed: schedule.seed,
        platform: corpus.platform.clone(),
        python: corpus.python.clone(),
        pip: corpus.pip_version.clone(),
        roots: corpus.roots.clone(),
        artifacts: corpus
            .candidates()
            .into_iter()
            .map(|candidate| {
                format!(
                    "{}=={} {} {}",
                    candidate.project, candidate.version, candidate.filename, candidate.sha256
                )
            })
            .collect(),
        versions,
        commands,
        schedule: schedule
            .entries()
            .iter()
            .map(|entry| format!("{}:{}", servers[entry.server].name, entry.round))
            .collect(),
    };
    report.insert("evidence".to_owned(), toml::Value::try_from(evidence)?);
    context.report_path().parent().map_or(Ok(()), std::fs::create_dir_all)?;
    std::fs::write(context.report_path(), toml::to_string_pretty(&report)?)?;
    Ok(())
}

fn command_output(program: &str, arguments: &[&str]) -> anyhow::Result<String> {
    let output = Command::new(program)
        .args(arguments)
        .output()
        .with_context(|| format!("cannot run {program}"))?;
    if !output.status.success() {
        bail!(
            "{program} {} failed:\n{}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout = String::from_utf8(output.stdout)?.trim().to_owned();
    if stdout.is_empty() {
        bail!("{program} {} returned no output", arguments.join(" "));
    }
    Ok(stdout)
}

#[cfg(test)]
#[path = "../../tests/unit/bench/evidence.rs"]
mod tests;

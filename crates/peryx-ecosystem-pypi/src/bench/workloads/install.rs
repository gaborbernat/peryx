use std::path::Path;
use std::process::Command;
use std::time::Instant;

use anyhow::{Context as _, bail};

use super::{Rounds, report_samples, run_checked};
use crate::bench::schedule::Schedule;
use peryx_bench_core::context::BenchmarkContext;
use peryx_bench_core::report::{Absent, Metric, baseline, complete_row, cost_rows, table};
use peryx_bench_core::servers::Server;
use peryx_bench_core::usage::{Cost, Usage};

/// The install workload: every server, cold then warm, per client, over the scheduled restarts.
///
/// # Errors
/// Returns an error when benchmark setup, server startup, or report publication fails. Install
/// failures are recorded as failed table cells.
pub async fn installs(
    context: &BenchmarkContext,
    servers: &[Server],
    clients: &[&str],
    schedule: &Schedule,
    http: &reqwest::Client,
    input: InstallInput<'_>,
) -> anyhow::Result<()> {
    for client in clients {
        let mut cold: Vec<Vec<f64>> = servers.iter().map(|_| Vec::new()).collect();
        let mut warm: Vec<Vec<f64>> = servers.iter().map(|_| Vec::new()).collect();
        let mut costs = servers.iter().map(|_| Rounds::new()).collect::<Vec<_>>();
        for entry in schedule.entries() {
            let server = &servers[entry.server];
            let scratch = tempfile::tempdir_in(context.scratch())?;
            let state = scratch.path().join("state");
            std::fs::create_dir(&state)?;
            let active = server.start(context, &state, http).await?;
            let usage = Usage::watch(active.pid())?;
            println!("[{client}] {} round {}/{}", server.name, entry.round, schedule.rounds);
            match install_round(
                client,
                &active.url,
                scratch.path(),
                input.packages,
                input.python,
                input.pip_version,
            ) {
                Ok((cold_seconds, warm_seconds)) => {
                    cold[entry.server].push(cold_seconds);
                    warm[entry.server].push(warm_seconds);
                }
                Err(error) => println!("[{client}] {} round {}: failed ({error:#})", server.name, entry.round),
            }
            costs[entry.server].record_cost(usage)?;
        }
        for (index, server) in servers.iter().enumerate() {
            report_samples(&format!("[{client}] {}", server.name), &cold[index], &warm[index]);
        }
        let costs = costs.into_iter().map(Rounds::costs).collect::<Vec<Option<Vec<Cost>>>>();
        let base = baseline(servers);
        let mut rows = vec![
            complete_row(
                "cold cache",
                &cold,
                schedule.rounds,
                base,
                Metric::Seconds,
                Absent::Failed,
            ),
            complete_row(
                "warm cache",
                &warm,
                schedule.rounds,
                base,
                Metric::Seconds,
                Absent::Failed,
            ),
        ];
        rows.extend(cost_rows(servers, &costs));
        context.publish(
            &format!("install-{client}"),
            table(
                &format!("{client}: install the top {} PyPI packages", input.packages.len()),
                servers,
                base,
                rows,
            ),
        )?;
    }
    Ok(())
}

pub struct InstallInput<'a> {
    pub packages: &'a [&'a str],
    pub python: &'a str,
    pub pip_version: &'a str,
}

/// One install round: a cold install (empty cache) then a warm one (the server keeps its cache, the
/// client starts over). Fallible as a unit so a flaky server becomes an error cell, not a run abort.
fn install_round(
    client: &str,
    index_url: &str,
    scratch: &Path,
    packages: &[&str],
    python: &str,
    pip_version: &str,
) -> anyhow::Result<(f64, f64)> {
    let cold = install_once(client, index_url, scratch, packages, python, pip_version)?;
    let warm = install_once(client, index_url, scratch, packages, python, pip_version)?;
    Ok((cold, warm))
}

/// Time one from-scratch install of the workload through `index_url`.
fn install_once(
    client: &str,
    index_url: &str,
    scratch: &Path,
    packages: &[&str],
    python: &str,
    pip_version: &str,
) -> anyhow::Result<f64> {
    let workdir = tempfile::tempdir_in(scratch)?;
    let venv = workdir.path().join("venv");
    run_checked(Command::new("uv").args(["venv", "--python", python]).arg(&venv))?;
    let (setup, install) = install_plan(client, index_url, packages, &venv, workdir.path(), pip_version);
    run_install_plan(index_url, setup, install)
}

/// Both clients pass `--only-binary :all:` so a missing wheel fails the round. A source build would land inside the
/// measured install and swamp the server's share of the time.
fn install_plan(
    client: &str,
    index_url: &str,
    packages: &[&str],
    venv: &Path,
    workdir: &Path,
    pip_version: &str,
) -> (Vec<Command>, Command) {
    if client == "uv" {
        let mut command = Command::new("uv");
        command
            .args(["pip", "install", "--index-url", index_url])
            .args(["--only-binary", ":all:"])
            .args(packages)
            .env("VIRTUAL_ENV", venv)
            .env("UV_CACHE_DIR", workdir.join("client-cache"));
        (Vec::new(), command)
    } else {
        let mut setup = Command::new("uv");
        setup
            .args(["pip", "install", "--python"])
            .arg(venv.join("bin").join("python"))
            .arg(format!("pip=={pip_version}"));
        let mut command = Command::new(venv.join("bin").join("pip"));
        command
            .args(["install", "--no-cache-dir", "--disable-pip-version-check"])
            .args(["--only-binary", ":all:"])
            .args(["--index-url", index_url])
            .args(packages);
        (vec![setup], command)
    }
}

fn run_install_plan(index_url: &str, mut setup: Vec<Command>, mut install: Command) -> anyhow::Result<f64> {
    for command in &mut setup {
        run_checked(command)?;
    }
    let start = Instant::now();
    let output = install.output().context("install client did not start")?;
    let elapsed = start.elapsed().as_secs_f64();
    if !output.status.success() {
        bail!(
            "install via {index_url} failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(elapsed)
}

#[cfg(test)]
#[path = "../../../tests/unit/bench/workloads/install.rs"]
mod tests;

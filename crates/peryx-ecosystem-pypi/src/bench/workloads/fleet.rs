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

/// The CI-fleet workload: ten venvs install polars at once, cold then warm, over `rounds` restarts.
///
/// Each worker gets its own empty uv cache, exactly like ten CI jobs landing on the same runner pool:
/// the server sees ten simultaneous copies of every page and wheel request.
///
/// # Errors
/// Returns an error when a server cannot start; a server failing the fleet is a table cell.
pub async fn fleet(
    context: &BenchmarkContext,
    servers: &[Server],
    schedule: &Schedule,
    http: &reqwest::Client,
    package: &str,
    python: &str,
) -> anyhow::Result<()> {
    fleet_package(context, servers, schedule, http, package, python, 10).await
}

async fn fleet_package(
    context: &BenchmarkContext,
    servers: &[Server],
    schedule: &Schedule,
    http: &reqwest::Client,
    package: &str,
    python: &str,
    workers: usize,
) -> anyhow::Result<()> {
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
        match fleet_round(&active.url, scratch.path(), workers, package, python) {
            Ok((cold_seconds, warm_seconds)) => {
                cold[entry.server].push(cold_seconds);
                warm[entry.server].push(warm_seconds);
            }
            Err(error) => println!("[fleet] {} round {}: failed ({error:#})", server.name, entry.round),
        }
        costs[entry.server].record_cost(usage)?;
    }
    for (index, server) in servers.iter().enumerate() {
        report_samples(&format!("[fleet] {}", server.name), &cold[index], &warm[index]);
    }
    let costs = costs.into_iter().map(Rounds::costs).collect::<Vec<Option<Vec<Cost>>>>();
    let base = baseline(servers);
    let mut rows = vec![
        complete_row(
            &format!("cold cache: {workers} parallel installs"),
            &cold,
            schedule.rounds,
            base,
            Metric::Seconds,
            Absent::Failed,
        ),
        complete_row(
            &format!("warm cache: {workers} parallel installs"),
            &warm,
            schedule.rounds,
            base,
            Metric::Seconds,
            Absent::Failed,
        ),
    ];
    rows.extend(cost_rows(servers, &costs));
    context.publish(
        "parallel-install",
        table(
            &format!("uv: {workers} venvs install {package} at once"),
            servers,
            base,
            rows,
        ),
    )
}

/// One round of the fleet workload: ten cold installs, then ten warm ones against the same server.
fn fleet_round(
    index_url: &str,
    scratch: &Path,
    workers: usize,
    package: &str,
    python: &str,
) -> anyhow::Result<(f64, f64)> {
    let cold = fleet_install(index_url, scratch, workers, package, python)?;
    let warm = fleet_install(index_url, scratch, workers, package, python)?;
    Ok((cold, warm))
}

/// Install the fleet package into `workers` fresh venvs at once; returns wall seconds.
fn fleet_install(index_url: &str, scratch: &Path, workers: usize, package: &str, python: &str) -> anyhow::Result<f64> {
    let rundir = tempfile::tempdir_in(scratch)?;
    let venvs: Vec<_> = (0..workers)
        .map(|index| rundir.path().join(format!("venv-{index}")))
        .collect();
    for venv in &venvs {
        run_checked(Command::new("uv").args(["venv", "--python", python]).arg(venv))?;
    }
    let start = Instant::now();
    let threads: Vec<_> = venvs
        .iter()
        .map(|venv| {
            let venv = venv.clone();
            let index_url = index_url.to_owned();
            let package = package.to_owned();
            std::thread::spawn(move || {
                let output = Command::new("uv")
                    .args(["pip", "install", "--index-url", &index_url])
                    .args(["--only-binary", ":all:", &package])
                    .env("VIRTUAL_ENV", &venv)
                    .env("UV_CACHE_DIR", format!("{}-cache", venv.display()))
                    .output()
                    .context("uv did not start")?;
                if !output.status.success() {
                    bail!(
                        "fleet install via {index_url} failed:\n{}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                Ok(())
            })
        })
        .collect();
    let mut outcome = Ok(());
    for thread in threads {
        let worker = thread.join().expect("fleet worker panicked");
        if outcome.is_ok() {
            outcome = worker;
        }
    }
    outcome?;
    Ok(start.elapsed().as_secs_f64())
}

#[cfg(test)]
#[path = "../../../tests/unit/bench/workloads/fleet.rs"]
mod tests;

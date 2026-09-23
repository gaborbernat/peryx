mod corpus;
mod evidence;
mod fixture;
mod packages;
mod preflight;
mod schedule;
mod servers;
mod workloads;

use anyhow::bail;
use clap::{Arg, Command, value_parser};
use peryx_bench_core::suite::{BenchmarkRun, BenchmarkSuite};

/// A zero-round run starts no fixture but still needs server URLs to validate `--only` and write evidence.
const NO_FIXTURE: &str = "https://fixture.invalid/simple/";

struct PypiBenchmarkSuite;

static SUITE: PypiBenchmarkSuite = PypiBenchmarkSuite;

pub static BENCHMARK_SUITE: &dyn BenchmarkSuite = &SUITE;

#[async_trait::async_trait]
impl BenchmarkSuite for PypiBenchmarkSuite {
    fn name(&self) -> &'static str {
        "pypi"
    }

    fn configure(&self, command: Command) -> Command {
        command.about("Benchmark PyPI package serving").arg(
            Arg::new("seed")
                .long("seed")
                .value_parser(value_parser!(u64).range(1..))
                .default_value("1161")
                .help("Seed for interleaving server-round pairs"),
        )
    }

    async fn run(&self, run: BenchmarkRun<'_>) -> anyhow::Result<()> {
        run_suite(
            run.context,
            &corpus::selected()?,
            run.rounds,
            *run.matches.get_one::<u64>("seed").expect("seed has a default"),
            run.skip,
            run.only,
            run.http,
        )
        .await
    }
}

/// Run the `PyPI` suite: every workload not in `skip`, against every server named in `only`.
///
/// Parts, any of which `--skip` leaves out by name: `install`, `pip` (the pip client inside the
/// install workload; uv still runs), `throughput`, `parallel`, `metadata`, `load`, `endpoints`.
///
/// # Errors
/// Returns an error when a server cannot start or a workload against a healthy server fails.
async fn run_suite(
    context: &peryx_bench_core::context::BenchmarkContext,
    corpus: &corpus::Corpus,
    rounds: usize,
    seed: u64,
    skip: &[String],
    only: &str,
    http: &reqwest::Client,
) -> anyhow::Result<()> {
    println!(
        "[fixture] {} roots and {} candidates for {}",
        corpus.roots.len(),
        corpus.candidates().len(),
        corpus.platform
    );
    let available = servers::all(NO_FIXTURE);
    let requested = (!only.is_empty()).then(|| only.split(',').collect::<Vec<_>>());
    let unknown = requested
        .as_deref()
        .unwrap_or_default()
        .iter()
        .copied()
        .filter(|name| !available.iter().any(|server| server.name == *name))
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        bail!(
            "unknown server selectors: {}; valid selectors: {}",
            unknown.join(", "),
            available
                .iter()
                .map(|server| server.name)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let fixture = if rounds == 0 {
        None
    } else {
        Some(fixture::Fixture::start(corpus, context.scratch(), http).await?)
    };
    let upstream = fixture.as_ref().map_or(NO_FIXTURE, |fixture| fixture.url.as_str());
    let servers: Vec<_> = servers::all(upstream)
        .into_iter()
        .filter(|server| requested.as_ref().is_none_or(|names| names.contains(&server.name)))
        .collect();
    if rounds > 0 {
        preflight::verify(context, &servers, corpus, http).await?;
    }
    let schedule = schedule::Schedule::new(servers.len(), rounds, seed);
    evidence::write(context, &servers, corpus, &schedule, upstream)?;
    let enabled = |part: &str| !skip.iter().any(|skipped| skipped.eq_ignore_ascii_case(part));
    let fleet = corpus.fleet_root()?;
    let install_roots = corpus
        .roots
        .iter()
        .map(String::as_str)
        .filter(|root| *root != fleet)
        .collect::<Vec<_>>();
    if enabled("install") {
        let clients: &[&str] = if enabled("pip") { &["uv", "pip"] } else { &["uv"] };
        workloads::installs(
            context,
            &servers,
            clients,
            &schedule,
            http,
            workloads::InstallInput {
                packages: &install_roots,
                python: &corpus.python,
                pip_version: &corpus.pip_version,
            },
        )
        .await?;
    }
    if enabled("throughput") {
        workloads::throughput(context, &servers, &schedule, http, upstream).await?;
    }
    if enabled("parallel") {
        workloads::fleet(context, &servers, &schedule, http, fleet, &corpus.python).await?;
    }
    if enabled("metadata") {
        workloads::metadata(context, &servers, &schedule, http).await?;
    }
    if enabled("load") {
        workloads::load(context, &servers, &[1, 32], &schedule, http).await?;
    }
    if enabled("endpoints") {
        workloads::endpoints(context, &servers, rounds, http).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/bench/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/unit/bench/workload_tests.rs"]
mod workload_tests;

#[cfg(test)]
#[path = "../../tests/unit/bench/test_support.rs"]
mod test_support;

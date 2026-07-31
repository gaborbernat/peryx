//! Durable job-run history commands.

use std::io::Write;
use std::num::NonZeroUsize;
use std::time::Duration;

use anyhow::{Context as _, ensure};
use peryx_driver::jobs::{
    CatalogSyncParameters, JobLimits, JobScheduler, MAX_CATALOG_CONCURRENCY, MAX_CATALOG_PROJECTS_PER_RUN,
    MAX_CATALOG_TIMEOUT, ScheduledJob, scheduled_job,
};
use peryx_storage::meta::{JobRunQuery, MetaStore};

use crate::cli::JobCommand;
use crate::config::Config;

/// List or show durable job-run history.
///
/// # Errors
/// Returns an error if the metadata store cannot be opened or read, the job run is unknown, or
/// output fails.
pub fn job(config: &Config, command: &JobCommand, out: &mut dyn Write) -> anyhow::Result<()> {
    match command {
        JobCommand::List(_) => job_list(&open_store(config)?, out),
        JobCommand::Show(args) => job_show(&open_store(config)?, &args.id, out),
        JobCommand::Run {
            repository,
            source,
            max_projects,
            concurrency,
            timeout_secs,
            ..
        } => run_catalog_sync(
            config,
            repository,
            source.as_deref(),
            *max_projects,
            *concurrency,
            *timeout_secs,
            out,
        ),
    }
}

fn open_store(config: &Config) -> anyhow::Result<MetaStore> {
    let path = config.data_dir.join("peryx.redb");
    MetaStore::open_existing(&path).with_context(|| format!("open metadata store {}", path.display()))
}

fn run_catalog_sync(
    config: &Config,
    repository: &str,
    source: Option<&str>,
    max_projects: usize,
    concurrency: usize,
    timeout_secs: u64,
    out: &mut dyn Write,
) -> anyhow::Result<()> {
    ensure!(!repository.trim().is_empty(), "repository must not be empty");
    ensure!(
        source.is_none_or(|source| !source.trim().is_empty()),
        "source must not be empty"
    );
    ensure!(
        max_projects <= MAX_CATALOG_PROJECTS_PER_RUN,
        "max-projects exceeds the per-run limit"
    );
    ensure!(
        concurrency <= MAX_CATALOG_CONCURRENCY,
        "concurrency exceeds the per-run limit"
    );
    ensure!(
        timeout_secs <= MAX_CATALOG_TIMEOUT.as_secs(),
        "timeout-secs exceeds the per-run limit"
    );
    let max_projects = NonZeroUsize::new(max_projects).context("max-projects must be positive")?;
    let concurrency = NonZeroUsize::new(concurrency).context("concurrency must be positive")?;
    ensure!(timeout_secs > 0, "timeout-secs must be positive");
    let parameters = CatalogSyncParameters {
        repository: repository.to_owned(),
        source: source.map(str::to_owned),
        max_projects,
        concurrency,
        timeout: Duration::from_secs(timeout_secs),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let report = runtime.block_on(async {
        let state = crate::server::build_state(config)?;
        let scheduler = JobScheduler::new(state.serving.clone(), JobLimits::node_local());
        let job = scheduled_job(&state, &ScheduledJob::CatalogSync(parameters)).map_err(anyhow::Error::msg)?;
        let result = scheduler.run(job).await.map_err(anyhow::Error::msg);
        scheduler.shutdown().await;
        result
    })?;
    writeln!(out, "processed\t{}", report.processed)?;
    writeln!(out, "changed\t{}", report.changed)?;
    Ok(())
}

fn job_list(store: &MetaStore, out: &mut dyn Write) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *out, &store.query_job_runs(&JobRunQuery::default())?)?;
    writeln!(out)?;
    Ok(())
}

fn job_show(store: &MetaStore, id: &str, out: &mut dyn Write) -> anyhow::Result<()> {
    let run = store
        .get_job_run(id)?
        .with_context(|| format!("unknown job run {id:?}"))?;
    serde_json::to_writer(&mut *out, &run)?;
    writeln!(out)?;
    Ok(())
}

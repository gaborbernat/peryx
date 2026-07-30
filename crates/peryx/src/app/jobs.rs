//! Durable job-run history commands.

use std::io::Write;
use std::num::NonZeroUsize;
use std::time::Duration;

use anyhow::{Context as _, ensure};
use peryx_driver::jobs::{
    CatalogSyncParameters, JobLimits, JobScheduler, MAX_CATALOG_CONCURRENCY, MAX_CATALOG_PROJECTS_PER_RUN,
    MAX_CATALOG_TIMEOUT, ScheduledJob, scheduled_job,
};
use peryx_storage::meta::{JobKind, JobRunRecord, JobState, MetaStore};

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
    writeln!(
        out,
        "id\tkind\tscope\tstate\tstarted_at_unix\tfinished_at_unix\tprocessed\tchanged\terror"
    )?;
    for run in store.list_job_runs()? {
        writeln!(
            out,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            run.id,
            job_kind(run.kind),
            optional_text(&run.scope),
            job_state(run.state),
            run.started_at_unix,
            optional_number(run.finished_at_unix),
            run.items_processed,
            run.items_changed,
            run.error.as_deref().map_or("-", optional_text),
        )?;
    }
    Ok(())
}

fn job_show(store: &MetaStore, id: &str, out: &mut dyn Write) -> anyhow::Result<()> {
    let run = store
        .get_job_run(id)?
        .with_context(|| format!("unknown job run {id:?}"))?;
    write_job(&run, out)
}

fn write_job(run: &JobRunRecord, out: &mut dyn Write) -> anyhow::Result<()> {
    writeln!(out, "id\t{}", run.id)?;
    writeln!(out, "kind\t{}", job_kind(run.kind))?;
    writeln!(out, "scope\t{}", optional_text(&run.scope))?;
    writeln!(out, "state\t{}", job_state(run.state))?;
    writeln!(out, "started_at_unix\t{}", run.started_at_unix)?;
    writeln!(out, "finished_at_unix\t{}", optional_number(run.finished_at_unix))?;
    writeln!(out, "processed\t{}", run.items_processed)?;
    writeln!(out, "changed\t{}", run.items_changed)?;
    writeln!(out, "error\t{}", run.error.as_deref().map_or("-", optional_text))?;
    Ok(())
}

const fn job_kind(kind: JobKind) -> &'static str {
    match kind {
        JobKind::CacheRefresh => "cache_refresh",
        JobKind::CatalogSync => "catalog_sync",
    }
}

const fn job_state(state: JobState) -> &'static str {
    match state {
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
    }
}

const fn optional_text(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn optional_number(value: Option<i64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

//! The bounded worker pool that runs [`NodeJob`]s under global, per-kind, and per-repository limits.
//!
//! A submission is admitted only when a queue slot is free and no run with the same conflict key is
//! already in flight, so two conflicting repository jobs never overlap while independent repositories
//! run together. Admitted work spawns onto the Tokio runtime and acquires a global permit (the worker
//! bound), then a per-kind and a per-repository permit, before it runs. Shutdown signals cooperative
//! cancellation and then waits out a grace period before it returns.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures_util::FutureExt as _;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use peryx_storage::meta::{JobOutcome, MetaError, NewJobRun};

use super::attempts::{CancelJobRun, JobAttemptError};
use super::metrics::{JobMetrics, Outcome, Reject};
use super::{JobContext, JobFailure, JobReport, NodeJob};
use crate::state::ServingState;

/// The bounds a [`JobScheduler`] runs under.
#[derive(Debug, Clone, Copy)]
pub struct JobLimits {
    /// Jobs allowed to run at once across every kind and repository.
    pub workers: NonZeroUsize,
    /// Admitted-but-unfinished jobs allowed to wait; a submission past this is rejected.
    pub queue: NonZeroUsize,
    /// Jobs of one kind allowed to run at once.
    pub per_kind: NonZeroUsize,
    /// Jobs acting on one repository allowed to run at once.
    pub per_repository: NonZeroUsize,
    /// How long [`shutdown`](JobScheduler::shutdown) waits for cancelled work before returning.
    pub shutdown_grace: Duration,
}

impl JobLimits {
    /// The defaults for a single node's maintenance: a handful of workers, a deep queue that absorbs a
    /// full sweep's fan-out, one run per repository so a repository never sweeps itself twice at once,
    /// and a shutdown grace that lets an in-flight sweep unwind.
    #[must_use]
    pub const fn node_local() -> Self {
        const fn nz(value: usize) -> NonZeroUsize {
            NonZeroUsize::new(value).expect("literal is non-zero")
        }
        Self {
            workers: nz(4),
            queue: nz(128),
            per_kind: nz(4),
            per_repository: nz(1),
            shutdown_grace: Duration::from_secs(30),
        }
    }
}

/// What became of a [`submit`](JobScheduler::submit) call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Submit {
    /// Admitted; the job is queued or running.
    Queued,
    /// A run with the same kind and scope is already in flight, so this one was dropped.
    Conflict,
    /// The queue is full; the job was dropped rather than made to wait unbounded.
    QueueFull,
    /// The scheduler is shutting down and accepts no new work.
    ShuttingDown,
}

/// A set of permits keyed by an arbitrary string, each with the same capacity.
///
/// The node-local scheduler keys these by job kind and by ecosystem, both bounded sets, so the map
/// stays small; it is not sized for an unbounded key space.
struct KeyedLimiter {
    capacity: usize,
    permits: Mutex<HashMap<String, Arc<Semaphore>>>,
}

impl KeyedLimiter {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity: capacity.get(),
            permits: Mutex::new(HashMap::new()),
        }
    }

    async fn acquire(&self, key: &str) -> OwnedSemaphorePermit {
        let semaphore = {
            let mut permits = self.permits.lock().unwrap_or_else(PoisonError::into_inner);
            permits
                .entry(key.to_owned())
                .or_insert_with(|| Arc::new(Semaphore::new(self.capacity)))
                .clone()
        };
        semaphore.acquire_owned().await.expect("keyed semaphore stays open")
    }
}

/// The state a scheduler shares with each admitted job it spawns.
struct Shared {
    state: Arc<ServingState>,
    workers: Arc<Semaphore>,
    queue: Arc<Semaphore>,
    per_kind: KeyedLimiter,
    per_repository: KeyedLimiter,
    inflight: Mutex<HashSet<String>>,
    metrics: Arc<JobMetrics>,
    cancel: CancellationToken,
}

impl Shared {
    fn lock_inflight(&self) -> MutexGuard<'_, HashSet<String>> {
        self.inflight.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A node-local job scheduler: submit typed jobs, and it runs them under the configured bounds.
pub struct JobScheduler {
    shared: Arc<Shared>,
    tracker: TaskTracker,
    grace: Duration,
}

impl JobScheduler {
    /// Build a scheduler over `state`'s stores and clock with the given `limits`.
    #[must_use]
    pub fn new(state: Arc<ServingState>, limits: JobLimits) -> Self {
        let shared = Shared {
            state,
            workers: Arc::new(Semaphore::new(limits.workers.get())),
            queue: Arc::new(Semaphore::new(limits.queue.get())),
            per_kind: KeyedLimiter::new(limits.per_kind),
            per_repository: KeyedLimiter::new(limits.per_repository),
            inflight: Mutex::new(HashSet::new()),
            metrics: Arc::new(JobMetrics::default()),
            cancel: CancellationToken::new(),
        };
        Self {
            shared: Arc::new(shared),
            tracker: TaskTracker::new(),
            grace: limits.shutdown_grace,
        }
    }

    /// The lifecycle counters, to register as a process metric source.
    #[must_use]
    pub fn metrics(&self) -> Arc<JobMetrics> {
        self.shared.metrics.clone()
    }

    /// Admit `job` for execution, or report why it was refused.
    ///
    /// Refusal is a normal outcome, not an error: a duplicate is a [`Conflict`](Submit::Conflict), a
    /// saturated queue is [`QueueFull`](Submit::QueueFull), and a draining scheduler is
    /// [`ShuttingDown`](Submit::ShuttingDown). Only an admitted job spawns.
    pub fn submit(&self, job: Arc<dyn NodeJob>) -> Submit {
        self.admit(job, None)
    }

    /// Admit one job and wait for its persisted result.
    ///
    /// # Panics
    /// Panics if an admitted worker exits without sending its result, which violates the scheduler's
    /// completion invariant.
    ///
    /// # Errors
    /// Returns a user-visible message when admission is refused or execution fails.
    pub async fn run(&self, job: Arc<dyn NodeJob>) -> Result<JobReport, String> {
        let (sender, receiver) = oneshot::channel();
        match self.admit(job, Some(sender)) {
            Submit::Queued => receiver.await.expect("an admitted job sends its result"),
            Submit::Conflict => Err("a matching node-local job is already running".to_owned()),
            Submit::QueueFull => Err("the node-local job queue is full".to_owned()),
            Submit::ShuttingDown => Err("the node-local job scheduler is shutting down".to_owned()),
        }
    }

    /// Signal one running durable attempt without affecting other work.
    ///
    /// # Errors
    /// Returns a store error if the ID is not active and its durable record cannot be inspected.
    pub fn cancel_job_run(&self, id: &str) -> Result<CancelJobRun, MetaError> {
        self.shared.state.job_attempts.cancel(id)
    }

    fn admit(&self, job: Arc<dyn NodeJob>, completion: Option<oneshot::Sender<Result<JobReport, String>>>) -> Submit {
        let kind = job.kind();
        if self.shared.cancel.is_cancelled() {
            return Submit::ShuttingDown;
        }
        let key = conflict_key(kind, job.scope());
        if !self.shared.lock_inflight().insert(key.clone()) {
            self.shared.metrics.rejected(kind, Reject::Conflict);
            return Submit::Conflict;
        }
        let Ok(slot) = self.shared.queue.clone().try_acquire_owned() else {
            self.shared.lock_inflight().remove(&key);
            self.shared.metrics.rejected(kind, Reject::QueueFull);
            return Submit::QueueFull;
        };
        self.tracker
            .spawn(run_admitted(self.shared.clone(), job, key, slot, completion));
        Submit::Queued
    }

    /// Stop accepting work, signal cooperative cancellation, and wait for running jobs to unwind,
    /// returning once they finish or the grace period elapses, whichever comes first.
    pub async fn shutdown(&self) {
        self.shared.cancel.cancel();
        self.tracker.close();
        if timeout(self.grace, self.tracker.wait()).await.is_err() {
            tracing::warn!("node-local jobs did not finish within the shutdown grace period");
        }
    }
}

async fn run_admitted(
    shared: Arc<Shared>,
    job: Arc<dyn NodeJob>,
    key: String,
    slot: OwnedSemaphorePermit,
    completion: Option<oneshot::Sender<Result<JobReport, String>>>,
) {
    let _slot = slot;
    let _worker = shared
        .workers
        .clone()
        .acquire_owned()
        .await
        .expect("worker semaphore stays open");
    let _kind = shared.per_kind.acquire(job.kind()).await;
    let _repository = shared.per_repository.acquire(job.scope()).await;
    let result = execute(job.as_ref(), &shared, &shared.cancel.child_token()).await;
    shared.lock_inflight().remove(&key);
    if let Some(completion) = completion {
        let _ = completion.send(result.map_err(|error| error.to_string()));
    }
}

fn conflict_key(kind: &str, scope: &str) -> String {
    format!("{kind}\u{0}{scope}")
}

async fn execute(job: &dyn NodeJob, shared: &Shared, cancel: &CancellationToken) -> Result<JobReport, JobError> {
    let kind = job.kind();
    shared.metrics.started(kind);
    let (outcome, result) = if cancel.is_cancelled() {
        (
            Outcome::Cancelled,
            Err(JobError::Job(JobFailure::new(
                "cancelled",
                "node-local job cancelled before it started",
            ))),
        )
    } else {
        let (result, outcome) = run_persisted(job, shared, cancel).await;
        if outcome != Outcome::Cancelled {
            let scope = job.scope();
            match &result {
                Ok(report) => tracing::info!(kind, scope, ?report, "node-local job finished"),
                Err(error) => tracing::error!(kind, scope, %error, "node-local job failed"),
            }
        }
        (outcome, result)
    };
    shared.metrics.finished(kind, outcome);
    result
}

async fn run_persisted(
    job: &dyn NodeJob,
    shared: &Shared,
    cancel: &CancellationToken,
) -> (Result<JobReport, JobError>, Outcome) {
    let run = match job.persist_as() {
        Some(kind) => match shared.state.job_attempts.start(
            NewJobRun {
                kind,
                scope: job.scope(),
                repository: job.repository(),
                started_at_unix: (shared.state.clock)(),
            },
            cancel.clone(),
        ) {
            Ok(id) => Some(id),
            Err(error) => return (Err(error.into()), Outcome::Failed),
        },
        None => None,
    };
    // Snapshot the authority epoch as the lease's fence: a run that outlives an authority transfer keeps
    // writing under the epoch it started with, which the newer holder fences out.
    let fence = match job.repository() {
        Some(repository) => shared.state.committed_authority_epoch(repository).await,
        None => 0,
    };
    let context = JobContext {
        state: shared.state.clone(),
        cancel: cancel.clone(),
        fence,
    };
    let (mut result, panicked) = AssertUnwindSafe(job.run(&context)).catch_unwind().await.map_or_else(
        |_| (Err(JobFailure::new("job_panic", "node-local job panicked")), true),
        |result| (result, false),
    );
    // Fence stale-epoch work: a run that leased a real authority epoch but whose authority advanced
    // while it ran wrote under a superseded epoch, so its success is rejected rather than counted. A
    // node-wide job (fence 0) and a process with no group hold no epoch and are never fenced.
    if fence != 0 && result.is_ok() {
        let repository = job.repository().expect("a nonzero fence is taken from a repository");
        if !shared.state.admit_authority_epoch(repository, fence).await {
            result = Err(JobFailure::new(
                "authority_fenced",
                "a newer authority epoch superseded this run",
            ));
        }
    }
    let cancelled = cancel.is_cancelled() && !panicked;
    let outcome = if panicked {
        Outcome::Failed
    } else if cancelled {
        Outcome::Cancelled
    } else if result.is_ok() {
        Outcome::Succeeded
    } else {
        Outcome::Failed
    };
    if let Some(id) = run {
        let finished_at_unix = (shared.state.clock)();
        let error = result.as_ref().err().map(ToString::to_string);
        let persisted = if outcome == Outcome::Cancelled {
            let report = result.as_ref().ok().copied().unwrap_or_default();
            JobOutcome::cancelled(finished_at_unix, report.processed, report.changed)
        } else if let Ok(report) = &result {
            JobOutcome::succeeded(finished_at_unix, report.processed, report.changed)
        } else {
            JobOutcome::failed(
                finished_at_unix,
                0,
                0,
                error.as_deref().expect("failed job carries an error"),
            )
        };
        if let Err(error) = shared.state.job_attempts.finish(&id, persisted) {
            return (Err(error.into()), Outcome::Failed);
        }
    }
    (result.map_err(JobError::from), outcome)
}

#[derive(Debug, thiserror::Error)]
enum JobError {
    #[error("{0}")]
    Job(#[from] JobFailure),
    #[error(transparent)]
    Attempt(#[from] JobAttemptError),
}

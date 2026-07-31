//! The timer that submits registered node-local jobs from configured schedules.
//!
//! One bounded timer drives every schedule from a single min-heap keyed by next-due instant, so a
//! large schedule set costs one heap pop per fire rather than a scan of every entry on each tick. The
//! timer keeps no durable state: a restart recomputes each schedule's next run one interval after
//! startup and never replays the occurrences missed while the process was down.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{
    CACHE_MAINTENANCE, CatalogSyncParameters, JobHistoryCleanup, JobScheduler, MAINTENANCE_INTERVAL, scheduled_job,
    submit_maintenance,
};
use crate::state::AppState;

/// A registered node-local job kind a schedule can name.
///
/// Each kind expands into the concrete [`NodeJob`](super::NodeJob)s to run when it fires: cache
/// maintenance fans out one per installed ecosystem driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduledJob {
    /// Reclaim idle process resources and revalidate stale cached pages, per ecosystem.
    CacheMaintenance,
    /// Refresh one remote project catalog and a bounded set of its project metadata.
    CatalogSync(CatalogSyncParameters),
}

impl ScheduledJob {
    /// The stable label this kind carries in configuration and logs.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CacheMaintenance => CACHE_MAINTENANCE,
            Self::CatalogSync(_) => "catalog_sync",
        }
    }

    fn submit(&self, app: &AppState, scheduler: &JobScheduler) {
        match self {
            Self::CacheMaintenance => submit_maintenance(app, scheduler),
            Self::CatalogSync(_) => match scheduled_job(app, self) {
                Ok(job) => {
                    scheduler.submit(job);
                }
                Err(error) => {
                    let job = self.as_str();
                    tracing::error!(job, %error, "scheduled job rejected");
                }
            },
        }
    }
}

/// One configured schedule: a job kind and the interval between its runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    pub job: ScheduledJob,
    pub interval: Duration,
}

/// Run each schedule on its interval until `cancel` fires, submitting due jobs through `scheduler`.
///
/// An internal history cleanup runs immediately and once per maintenance interval even with no
/// configured schedule. Each fire reschedules one interval on. A fire that wakes past its due instant,
/// from a slow tick or a clock advanced across several intervals, reschedules from the wake instant,
/// so missed occurrences collapse into the next run rather than replaying as a backlog. When the
/// scheduler refuses a submission because the same job is still running, the timer counts that
/// skipped tick in the scheduler's metrics and moves on to the next fire.
pub async fn run_schedules(
    app: Arc<AppState>,
    scheduler: Arc<JobScheduler>,
    plan: Vec<Schedule>,
    cancel: CancellationToken,
) {
    let start = Instant::now();
    let cleanup = plan.len();
    let mut due: BinaryHeap<Reverse<(Instant, usize)>> = plan
        .iter()
        .enumerate()
        .map(|(index, schedule)| Reverse((start + schedule.interval, index)))
        .chain(std::iter::once(Reverse((start, cleanup))))
        .collect();
    while let Some(Reverse((at, index))) = due.pop() {
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep_until(at) => {}
        }
        let interval = if index == cleanup {
            scheduler.submit(Arc::new(JobHistoryCleanup));
            MAINTENANCE_INTERVAL
        } else {
            let schedule = &plan[index];
            let job = schedule.job.as_str();
            tracing::debug!(job, "schedule fired");
            schedule.job.submit(&app, &scheduler);
            schedule.interval
        };
        due.push(Reverse((reschedule(at, Instant::now(), interval), index)));
    }
}

/// The next fire for a schedule that just ran at due instant `at`, given the wake instant `woke`.
///
/// One interval past the due instant holds a steady cadence. When the fire woke past its due instant,
/// from a slow tick or a clock advanced across intervals, the next run is one interval past the wake
/// instant instead, so a long gap yields a single run rather than a replayed backlog.
pub(super) fn reschedule(at: Instant, woke: Instant, interval: Duration) -> Instant {
    let next = at + interval;
    if next <= woke { woke + interval } else { next }
}

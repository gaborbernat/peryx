use std::sync::{Arc, Barrier, Mutex};

use peryx_storage::meta::{
    FinishJobRun, JobKind, JobOutcome, JobRunRecord, JobRunStoreError, JobState, MetaError, MetaStore, NewJobRun,
};
use tokio_util::sync::CancellationToken;

use super::*;

fn store() -> (tempfile::TempDir, MetaStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    (dir, store)
}

fn run() -> NewJobRun<'static> {
    NewJobRun {
        kind: JobKind::new("cache_refresh").unwrap(),
        scope: "alpha",
        repository: None,
        started_at_unix: 100,
    }
}

#[test]
fn test_start_and_finish_owns_the_cancellation_token() {
    let (_dir, store) = store();
    let control = JobAttemptControl::new(store);
    let cancel = CancellationToken::new();
    let id = control.start(run(), cancel.clone()).unwrap();

    assert_eq!(control.cancel(&id).unwrap(), CancelJobRun::Requested);
    assert!(cancel.is_cancelled());
    assert_eq!(
        control.finish(&id, JobOutcome::succeeded(110, 2, 1)).unwrap().state,
        JobState::Succeeded
    );
    assert_eq!(control.cancel(&id).unwrap(), CancelJobRun::Finished);
}

struct ControlledFinishStore {
    store: MetaStore,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
    result: Mutex<Option<Result<FinishJobRun, MetaError>>>,
}

impl JobAttemptStore for ControlledFinishStore {
    fn start_job_run(&self, run: NewJobRun<'_>) -> Result<String, JobRunStoreError> {
        self.store.start_job_run(run)
    }

    fn finish_job_run(&self, _id: &str, _outcome: JobOutcome<'_>) -> Result<FinishJobRun, MetaError> {
        self.entered.wait();
        self.release.wait();
        self.result.lock().unwrap().take().unwrap()
    }

    fn get_job_run(&self, id: &str) -> Result<Option<JobRunRecord>, MetaError> {
        self.store.get_job_run(id)
    }

    fn recover_interrupted_job_runs(&self, recovered_at_unix: i64) -> Result<usize, MetaError> {
        self.store.recover_interrupted_job_runs(recovered_at_unix)
    }
}

struct ControlledFinish {
    _dir: tempfile::TempDir,
    control: Arc<JobAttemptControl>,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
    id: String,
}

fn controlled_finish(result: Result<FinishJobRun, MetaError>) -> ControlledFinish {
    let (dir, store) = store();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let control = Arc::new(JobAttemptControl::with_store(Arc::new(ControlledFinishStore {
        store,
        entered: entered.clone(),
        release: release.clone(),
        result: Mutex::new(Some(result)),
    })));
    let id = control.start(run(), CancellationToken::new()).unwrap();
    ControlledFinish {
        _dir: dir,
        control,
        entered,
        release,
        id,
    }
}

#[test]
fn test_missing_record_releases_the_active_attempt() {
    let controlled = controlled_finish(Ok(FinishJobRun::Missing));
    let finishing = {
        let control = controlled.control.clone();
        let id = controlled.id.clone();
        std::thread::spawn(move || control.finish(&id, JobOutcome::failed(110, 0, 0, "missing")))
    };
    controlled.entered.wait();

    assert_eq!(
        controlled.control.cancel(&controlled.id).unwrap(),
        CancelJobRun::Unavailable
    );
    controlled.release.wait();
    assert!(matches!(finishing.join().unwrap(), Err(JobAttemptError::Missing)));
}

#[test]
fn test_store_error_releases_the_active_attempt() {
    let controlled = controlled_finish(Err(MetaError::DriverPrecondition("finish unavailable".to_owned())));
    let finishing = {
        let control = controlled.control.clone();
        let id = controlled.id.clone();
        std::thread::spawn(move || control.finish(&id, JobOutcome::failed(110, 0, 0, "failure")))
    };
    controlled.entered.wait();

    assert_eq!(
        controlled.control.cancel(&controlled.id).unwrap(),
        CancelJobRun::Unavailable
    );
    controlled.release.wait();
    let error = finishing.join().unwrap().unwrap_err();
    assert!(matches!(error, JobAttemptError::Store(_)));
    assert_eq!(error.to_string(), "driver precondition failed: finish unavailable");
}

#[test]
fn test_external_finish_releases_the_active_attempt() {
    let (_dir, store) = store();
    let control = JobAttemptControl::new(store.clone());
    let id = control.start(run(), CancellationToken::new()).unwrap();
    assert!(matches!(
        store.finish_job_run(&id, JobOutcome::succeeded(105, 0, 0)).unwrap(),
        FinishJobRun::Finished(_)
    ));

    assert!(matches!(
        control.finish(&id, JobOutcome::failed(110, 0, 0, "late")),
        Err(JobAttemptError::AlreadyFinished)
    ));
    assert_eq!(control.cancel(&id).unwrap(), CancelJobRun::Finished);
}

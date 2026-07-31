use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use mockall::mock;
use redb::backends::InMemoryBackend;
use redb::{Database, StorageBackend as _};
use rstest::rstest;

use super::*;

mock! {
    #[derive(Debug)]
    Backend {}

    impl redb::StorageBackend for Backend {
        fn len(&self) -> io::Result<u64>;
        fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()>;
        fn set_len(&self, len: u64) -> io::Result<()>;
        fn sync_data(&self) -> io::Result<()>;
        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()>;
    }
}

#[derive(Debug)]
struct Fault(AtomicI64);

impl Fault {
    fn disabled() -> Self {
        Self(AtomicI64::new(-1))
    }

    fn arm(&self, after: i64) {
        self.0.store(after, Ordering::SeqCst);
    }

    fn disable(&self) {
        self.arm(-1);
    }

    fn pass(&self) -> io::Result<()> {
        self.0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| match remaining {
                -1 => Some(-1),
                0 => None,
                _ => Some(remaining - 1),
            })
            .map(drop)
            .map_err(|_| io::Error::other("injected storage failure"))
    }
}

fn backend(inner: Arc<InMemoryBackend>, fault: Arc<Fault>) -> MockBackend {
    let mut backend = MockBackend::new();
    let storage = inner.clone();
    let fail = fault.clone();
    backend
        .expect_len()
        .returning(move || fail.pass().and_then(|()| storage.len()));
    let storage = inner.clone();
    let fail = fault.clone();
    backend
        .expect_read()
        .returning(move |offset, out| fail.pass().and_then(|()| storage.read(offset, out)));
    let storage = inner.clone();
    let fail = fault.clone();
    backend
        .expect_set_len()
        .returning(move |len| fail.pass().and_then(|()| storage.set_len(len)));
    let storage = inner.clone();
    let fail = fault.clone();
    backend
        .expect_sync_data()
        .returning(move || fail.pass().and_then(|()| storage.sync_data()));
    backend
        .expect_write()
        .returning(move |offset, data| fault.pass().and_then(|()| inner.write(offset, data)));
    backend
}

fn open_store(inner: Arc<InMemoryBackend>, fault: Arc<Fault>, initialize: bool) -> MetaStore {
    // Keep tree pages crossing the mocked boundary after a fault is armed.
    let database = Database::builder()
        .set_cache_size(0)
        .create_with_backend(backend(inner, fault))
        .unwrap();
    if initialize {
        let write = database.begin_write().unwrap();
        write.open_table(SERIAL).unwrap();
        write.open_table(JOB_RUN).unwrap();
        write.commit().unwrap();
    }
    MetaStore {
        db: Arc::new(crate::meta::MetaDatabase::ReadWrite(database)),
    }
}

fn seeded_store() -> (MetaStore, Arc<InMemoryBackend>, Arc<Fault>, String) {
    let inner = Arc::new(InMemoryBackend::new());
    let fault = Arc::new(Fault::disabled());
    let store = open_store(inner.clone(), fault.clone(), true);
    let running = store
        .start_job_run(NewJobRun {
            kind: JobKind::CatalogSync,
            scope: "running",
            repository: Some("hosted"),
            started_at_unix: 1,
        })
        .unwrap();
    for serial in 1..=8 {
        let id = store
            .start_job_run(NewJobRun {
                kind: JobKind::CacheRefresh,
                scope: &format!("finished-{serial}"),
                repository: Some("hosted"),
                started_at_unix: serial + 1,
            })
            .unwrap();
        store
            .finish_job_run(&id, JobOutcome::failed(serial + 2, 1, 0, &"x".repeat(MAX_ERROR_BYTES)))
            .unwrap();
    }
    (store, inner, fault, running)
}

#[derive(Debug, Clone, Copy)]
enum Operation {
    Start,
    Finish,
    Get,
    List,
    Query,
    Recover,
    Prune,
}

fn invoke(store: &MetaStore, running: &str, operation: Operation) -> bool {
    match operation {
        Operation::Start => matches!(
            store.start_job_run(NewJobRun {
                kind: JobKind::CacheRefresh,
                scope: "new",
                repository: Some("hosted"),
                started_at_unix: 30,
            }),
            Err(JobRunStoreError::Store(_))
        ),
        Operation::Finish => store.finish_job_run(running, JobOutcome::succeeded(30, 1, 1)).is_err(),
        Operation::Get => store.get_job_run(running).is_err(),
        Operation::List => store.list_job_runs().is_err(),
        Operation::Query => matches!(
            store.query_job_runs(&JobRunQuery {
                cursor: Some(running.to_owned()),
                limit: 8,
            }),
            Err(JobRunQueryError::Store(_))
        ),
        Operation::Recover => store.recover_interrupted_job_runs(30).is_err(),
        Operation::Prune => store.prune_job_runs_batch(0).is_err(),
    }
}

fn assert_atomic_result(operation: Operation, running: &str, expected: &[JobRunRecord], actual: &[JobRunRecord]) {
    if actual == expected {
        return;
    }
    match operation {
        Operation::Start => {
            assert_eq!(actual.len(), expected.len() + 1);
            assert_eq!((actual[0].scope.as_str(), actual[0].state), ("new", JobState::Running));
            assert_eq!(&actual[1..], expected);
        }
        Operation::Finish => {
            assert_eq!(actual.len(), expected.len());
            assert_eq!(
                actual.last().map(|record| (record.id.as_str(), record.state)),
                Some((running, JobState::Succeeded))
            );
            assert_eq!(&actual[..actual.len() - 1], &expected[..expected.len() - 1]);
        }
        Operation::Recover => {
            assert_eq!(actual.len(), expected.len());
            assert_eq!(
                actual.last().map(|record| (record.id.as_str(), record.state)),
                Some((running, JobState::Failed))
            );
            assert_eq!(&actual[..actual.len() - 1], &expected[..expected.len() - 1]);
        }
        Operation::Prune => {
            assert_eq!(actual.len(), expected.len() - JOB_RETENTION_BATCH);
            assert_eq!(&actual[..4], &expected[..4]);
            assert_eq!(actual.last(), expected.last());
        }
        Operation::Get | Operation::List | Operation::Query => panic!("read failure changed durable state"),
    }
}

#[rstest]
#[case::start(Operation::Start)]
#[case::finish(Operation::Finish)]
#[case::get(Operation::Get)]
#[case::list(Operation::List)]
#[case::query(Operation::Query)]
#[case::recover(Operation::Recover)]
#[case::prune(Operation::Prune)]
fn test_job_run_operations_remain_atomic_after_backend_failures(#[case] operation: Operation) {
    let mut failures = 0;
    for fail_after in 0..64 {
        let (store, inner, fault, running) = seeded_store();
        let expected = store.list_job_runs().unwrap();
        drop(store);
        let store = open_store(inner.clone(), fault.clone(), false);
        fault.arm(fail_after);
        if invoke(&store, &running, operation) {
            failures += 1;
            fault.disable();
            drop(store);
            let actual = open_store(inner, fault, false).list_job_runs().unwrap();
            assert_atomic_result(operation, &running, &expected, &actual);
        }
    }
    assert!(failures > 0);
}

#[rstest]
#[case::start(Operation::Start)]
#[case::finish(Operation::Finish)]
#[case::recover(Operation::Recover)]
#[case::prune(Operation::Prune)]
fn test_job_run_writes_reject_a_poisoned_backend(#[case] operation: Operation) {
    let (store, inner, fault, running) = seeded_store();
    drop(store);
    let store = open_store(inner, fault.clone(), false);
    fault.arm(0);
    assert!(store.get_job_run(&running).is_err());
    fault.disable();

    assert!(invoke(&store, &running, operation));
}

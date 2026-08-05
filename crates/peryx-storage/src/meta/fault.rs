//! A shared redb fault-injection backend for meta-store tests.
//!
//! A [`mockall`] mock [`redb::StorageBackend`] wraps an [`InMemoryBackend`] and consults a [`Fault`]
//! countdown before each operation, so a chosen backend call fails deterministically. Reopening a
//! store over the same backend with a zeroed page cache forces every read through the mock, so the
//! read-path error arms fire too, not only writes.
//!
//! Each store's fault tests open only the tables that store touches by passing an initializer to
//! [`create`]; the counterpart [`reopen`] wraps the populated backend without reinitializing it.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use mockall::mock;
use redb::backends::InMemoryBackend;
use redb::{Database, StorageBackend as _, WriteTransaction};

use super::{MetaDatabase, MetaStore};

mock! {
    #[derive(Debug)]
    pub Backend {}

    impl redb::StorageBackend for Backend {
        fn len(&self) -> io::Result<u64>;
        fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()>;
        fn set_len(&self, len: u64) -> io::Result<()>;
        fn sync_data(&self) -> io::Result<()>;
        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()>;
    }
}

/// A countdown that fails the nth backend operation after it is armed.
#[derive(Debug)]
pub struct Fault(AtomicI64);

impl Fault {
    fn disabled() -> Self {
        Self(AtomicI64::new(-1))
    }

    /// Fail the operation `after` further backend calls; `0` fails the next one.
    pub fn arm(&self, after: i64) {
        self.0.store(after, Ordering::SeqCst);
    }

    /// Stop injecting failures.
    pub fn disable(&self) {
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

fn mock(inner: Arc<InMemoryBackend>, fault: Arc<Fault>) -> MockBackend {
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

fn database(inner: &Arc<InMemoryBackend>, fault: &Arc<Fault>) -> Database {
    // A zeroed cache keeps tree pages crossing the mocked boundary after a fault is armed, so reads
    // reach the mock instead of an in-process page copy.
    Database::builder()
        .set_cache_size(0)
        .create_with_backend(mock(inner.clone(), fault.clone()))
        .unwrap()
}

/// A fresh mock backend and its disarmed fault handle.
pub fn backend() -> (Arc<InMemoryBackend>, Arc<Fault>) {
    (Arc::new(InMemoryBackend::new()), Arc::new(Fault::disabled()))
}

/// Open a store over `inner`, initializing the tables `init` opens. Keep the fault disabled here so
/// initialization and any seeding run cleanly.
pub fn create(
    inner: &Arc<InMemoryBackend>,
    fault: &Arc<Fault>,
    init: impl FnOnce(&WriteTransaction) -> Result<(), redb::TableError>,
) -> MetaStore {
    let database = database(inner, fault);
    let write = database.begin_write().unwrap();
    init(&write).unwrap();
    write.commit().unwrap();
    MetaStore {
        db: Arc::new(MetaDatabase::ReadWrite(database)),
    }
}

/// Reopen a store over an already-populated `inner` without touching its tables, so an armed fault
/// reaches the cache-free read path.
pub fn reopen(inner: &Arc<InMemoryBackend>, fault: &Arc<Fault>) -> MetaStore {
    MetaStore {
        db: Arc::new(MetaDatabase::ReadWrite(database(inner, fault))),
    }
}

/// Overwrite `key` in a `<&str, &[u8]>` table with raw `bytes`, so a store's decode arm meets a
/// malformed record without a backend fault.
pub fn corrupt(store: &MetaStore, table: redb::TableDefinition<'_, &str, &[u8]>, key: &str, bytes: &[u8]) {
    let MetaDatabase::ReadWrite(db) = &*store.db else {
        panic!("corrupt needs a read-write store");
    };
    let write = db.begin_write().unwrap();
    write.open_table(table).unwrap().insert(key, bytes).unwrap();
    write.commit().unwrap();
}

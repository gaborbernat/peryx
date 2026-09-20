//! Deterministic redb fault injection for metadata-store tests.
//!
//! A [`Fault`] counts backend operations and can fail a chosen operation without sleeping, racing,
//! or faking the store. Pair [`faulted`] with a store constructor that accepts a redb backend, such
//! as `MetaStore::open_backend`.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use redb::backends::InMemoryBackend;

/// Hand-written to avoid uncovered macro-generated branches.
#[derive(Debug)]
pub struct FaultBackend {
    inner: Arc<InMemoryBackend>,
    fault: Arc<Fault>,
}

impl redb::StorageBackend for FaultBackend {
    fn len(&self) -> io::Result<u64> {
        self.fault.pass().and_then(|()| self.inner.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.fault.pass().and_then(|()| self.inner.read(offset, out))
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.fault.pass().and_then(|()| self.inner.set_len(len))
    }

    fn sync_data(&self) -> io::Result<()> {
        self.fault.pass().and_then(|()| self.inner.sync_data())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.fault.pass().and_then(|()| self.inner.write(offset, data))
    }
}

#[derive(Debug)]
pub struct Fault {
    remaining: AtomicUsize,
    triggered: AtomicBool,
}

impl Fault {
    const DISABLED: usize = usize::MAX;
    const INJECTED: usize = Self::DISABLED - 1;
    const ONCE: usize = 1 << (usize::BITS - 1);

    const fn disabled() -> Self {
        Self {
            remaining: AtomicUsize::new(Self::DISABLED),
            triggered: AtomicBool::new(false),
        }
    }

    /// Fails every backend operation after the next `after` of them succeed.
    pub fn arm(&self, after: usize) {
        self.arm_with(after);
    }

    /// Fails one backend operation after the next `after` of them succeed.
    pub fn arm_once(&self, after: usize) {
        self.arm_with(Self::ONCE | after);
    }

    pub fn disable(&self) {
        self.remaining.store(Self::DISABLED, Ordering::SeqCst);
        self.triggered.store(false, Ordering::SeqCst);
    }

    #[must_use]
    pub fn triggered(&self) -> bool {
        self.triggered.load(Ordering::SeqCst)
    }

    fn arm_with(&self, after: usize) {
        self.triggered.store(false, Ordering::SeqCst);
        self.remaining.store(after, Ordering::SeqCst);
    }

    fn pass(&self) -> io::Result<()> {
        let previous = self
            .remaining
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| match remaining {
                Self::DISABLED | Self::INJECTED => None,
                Self::ONCE => Some(Self::DISABLED),
                remaining if remaining & Self::ONCE != 0 => Some(remaining - 1),
                0 => Some(Self::INJECTED),
                _ => Some(remaining - 1),
            })
            .unwrap_or_else(|state| state);
        if matches!(previous, 0 | Self::ONCE | Self::INJECTED) {
            self.triggered.store(true, Ordering::SeqCst);
            Err(io::Error::other("injected storage failure"))
        } else {
            Ok(())
        }
    }
}

/// A disarmed fault and the in-memory pages it guards, both retained so a test can reopen the same
/// bytes after arming.
#[must_use]
pub fn backend() -> (Arc<InMemoryBackend>, Arc<Fault>) {
    (Arc::new(InMemoryBackend::new()), Arc::new(Fault::disabled()))
}

#[must_use]
pub fn faulted(inner: &Arc<InMemoryBackend>, fault: &Arc<Fault>) -> FaultBackend {
    FaultBackend {
        inner: inner.clone(),
        fault: fault.clone(),
    }
}

//! Deterministic redb fault injection for metadata-store tests.
//!
//! A [`Fault`] counts backend operations and can fail a chosen operation without sleeping, racing,
//! or faking the store. Pair [`faulted`] with a store constructor that accepts a redb backend, such
//! as `MetaStore::open_backend`.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use redb::backends::InMemoryBackend;

/// Hand-written to avoid uncovered macro-generated branches.
#[derive(Debug)]
pub struct FaultBackend {
    inner: Arc<InMemoryBackend>,
    fault: Arc<Fault>,
}

/// Stops one completed backend read until its paired release drops, so a test can commit through a
/// second handle after redb has released the read's locks.
#[derive(Debug)]
pub struct ReadGateBackend {
    inner: Arc<InMemoryBackend>,
    gate: Arc<ReadGate>,
}

impl redb::StorageBackend for ReadGateBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)?;
        self.gate.wait();
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }
}

#[derive(Debug, Default)]
pub struct ReadGate {
    state: Mutex<ReadGateState>,
    arrived: Condvar,
    released: Condvar,
}

#[derive(Debug, Default)]
struct ReadGateState {
    armed: bool,
    arrived: bool,
    released: bool,
}

impl ReadGate {
    /// Arms the next successful backend read.
    ///
    /// # Panics
    /// Panics if a caller arms an armed or completed gate, or if its mutex is poisoned.
    pub fn arm(&self) {
        let mut state = self.state.lock().expect("read gate is never poisoned");
        assert!(!state.armed && !state.arrived, "read gate is single-use");
        state.armed = !state.released;
    }

    /// Returns whether one delegated backend read reaches its controlled boundary before `timeout`.
    ///
    /// # Panics
    /// Panics if the gate mutex is poisoned.
    #[must_use]
    pub fn wait_for_arrival(&self, timeout: Duration) -> bool {
        let (state, _) = self
            .arrived
            .wait_timeout_while(
                self.state.lock().expect("read gate is never poisoned"),
                timeout,
                |state| !state.arrived && !state.released,
            )
            .expect("read gate is never poisoned");
        let arrived = state.arrived;
        drop(state);
        arrived
    }

    fn wait(&self) {
        let mut state = self.state.lock().expect("read gate is never poisoned");
        if !state.armed {
            return;
        }
        state.armed = false;
        state.arrived = true;
        self.arrived.notify_all();
        state = self
            .released
            .wait_while(state, |state| !state.released)
            .expect("read gate is never poisoned");
        drop(state);
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("read gate is never poisoned");
        state.released = true;
        state.armed = false;
        drop(state);
        self.arrived.notify_all();
        self.released.notify_all();
    }
}

#[must_use]
pub struct ReadRelease(Arc<ReadGate>);

impl Drop for ReadRelease {
    fn drop(&mut self) {
        self.0.release();
    }
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

/// Wraps one in-memory backend with a disarmed read gate and its RAII release.
pub fn gated_reads(inner: &Arc<InMemoryBackend>) -> (ReadGateBackend, Arc<ReadGate>, ReadRelease) {
    let gate = Arc::new(ReadGate::default());
    (
        ReadGateBackend {
            inner: inner.clone(),
            gate: gate.clone(),
        },
        gate.clone(),
        ReadRelease(gate),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use redb::StorageBackend as _;

    use super::{InMemoryBackend, ReadGateBackend};

    fn controller_read_ends(backend: ReadGateBackend) -> [u8; 1] {
        let (done, received) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut byte = [0];
            backend.read(0, &mut byte).unwrap();
            done.send(byte).unwrap();
        });
        let byte = received.recv_timeout(Duration::from_secs(1)).unwrap();
        worker.join().unwrap();
        byte
    }

    fn seeded_backend() -> std::sync::Arc<InMemoryBackend> {
        let (backend, _fault) = super::backend();
        backend.set_len(1).unwrap();
        backend.write(0, &[7]).unwrap();
        backend
    }

    #[test]
    fn test_read_gate_controller_releases_before_arm() {
        let pages = seeded_backend();
        let (backend, gate, release) = super::gated_reads(&pages);
        drop(release);

        gate.arm();

        assert!(!gate.wait_for_arrival(Duration::ZERO));
        assert_eq!(controller_read_ends(backend), [7]);
    }

    #[test]
    fn test_read_gate_controller_releases_before_arrival() {
        let pages = seeded_backend();
        let (backend, gate, release) = super::gated_reads(&pages);
        gate.arm();
        drop(release);

        assert!(!gate.wait_for_arrival(Duration::ZERO));
        assert_eq!(controller_read_ends(backend), [7]);
    }
}

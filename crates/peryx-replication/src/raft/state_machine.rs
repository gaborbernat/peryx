//! The `OpenRaft` state machine that applies committed ownership log entries.
//!
//! [`OwnershipStateMachine`] binds the pure [`OwnershipState`] to `OpenRaft`'s
//! [`RaftStateMachine`] contract. `OpenRaft` hands it the committed entries in order, and it folds each
//! [`Normal`](openraft::EntryPayload::Normal) entry's [`OwnershipCommand`](crate::OwnershipCommand)
//! through [`OwnershipState::apply`], returning the [`OwnershipEffect`](crate::OwnershipEffect) wrapped
//! as an [`OwnershipResponse`]. The blank entry a new leader commits and the membership entries `OpenRaft`
//! manages carry no command; it records the membership and reports them as
//! [`NonMutating`](OwnershipResponse::NonMutating).
//!
//! Snapshots ride on the pure state's own format: [`OwnershipState::snapshot`] produces the bytes and
//! [`OwnershipState::restore`] rebuilds from them, so the same one-based, zero-reserved epoch invariant
//! the state enforces survives a snapshot install. The state lives behind an `Arc<Mutex<_>>` so the
//! snapshot builder shares one view with the applier.

use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{
    AnyError, Entry, EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use tokio::sync::Mutex;

use crate::ownership::OwnershipState;
use crate::raft::{OwnershipResponse, PeryxNode, TypeConfig};

/// The `u64` voter handle `OpenRaft` keys nodes by. See [`crate::raft`] for why it is not the datacenter
/// identity.
type NodeId = u64;

/// The replicated ownership state machine driven by `OpenRaft`.
///
/// Cloning shares the underlying state, so the snapshot builder returned by
/// [`get_snapshot_builder`](RaftStateMachine::get_snapshot_builder) reads the same state the applier
/// writes.
#[derive(Debug, Clone, Default)]
pub struct OwnershipStateMachine {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Default)]
struct Inner {
    state: OwnershipState,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, PeryxNode>,
    snapshots_built: u64,
    current_snapshot: Option<StoredSnapshot>,
}

/// The most recent snapshot this machine built or installed, kept so
/// [`get_current_snapshot`](RaftStateMachine::get_current_snapshot) can return it.
#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, PeryxNode>,
    data: Vec<u8>,
}

impl RaftSnapshotBuilder<TypeConfig> for OwnershipStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        let data = inner.state.snapshot();
        inner.snapshots_built += 1;
        let last_index = inner.last_applied.map_or(0, |log_id| log_id.index);
        let snapshot_id = format!("{last_index}-{}", inner.snapshots_built);
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            snapshot_id,
        };
        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        drop(inner);
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for OwnershipStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, PeryxNode>), StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<OwnershipResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.lock().await;
        let mut responses = Vec::new();
        for entry in entries {
            inner.last_applied = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => OwnershipResponse::NonMutating,
                EntryPayload::Normal(command) => OwnershipResponse::Applied(inner.state.apply(&command)),
                EntryPayload::Membership(membership) => {
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    OwnershipResponse::NonMutating
                }
            };
            responses.push(response);
        }
        drop(inner);
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, PeryxNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let state = OwnershipState::restore(&data)
            .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), AnyError::new(&error)))?;
        let mut inner = self.inner.lock().await;
        inner.state = state;
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        drop(inner);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        Ok(inner.current_snapshot.as_ref().map(|stored| Snapshot {
            meta: stored.meta.clone(),
            snapshot: Box::new(Cursor::new(stored.data.clone())),
        }))
    }
}

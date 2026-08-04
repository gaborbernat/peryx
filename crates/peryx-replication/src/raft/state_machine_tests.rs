use std::collections::BTreeSet;

use openraft::storage::RaftStateMachine;
use openraft::testing::log_id;
use openraft::{Entry, EntryPayload, Membership, RaftSnapshotBuilder, Snapshot};

use crate::AuthorityEpoch;
use crate::ownership::{DatacenterId, OwnershipCommand, OwnershipEffect};
use crate::raft::{OwnershipResponse, OwnershipStateMachine, TypeConfig};

fn key(name: &str) -> crate::AuthorityKey {
    crate::AuthorityKey(name.to_owned())
}

fn assign(authority: &str, home: &str) -> OwnershipCommand {
    OwnershipCommand::AssignHome {
        authority: key(authority),
        home: DatacenterId(home.to_owned()),
    }
}

fn advance(authority: &str) -> OwnershipCommand {
    OwnershipCommand::AdvanceAuthorityEpoch {
        authority: key(authority),
    }
}

fn normal(index: u64, command: OwnershipCommand) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(1, 0, index),
        payload: EntryPayload::Normal(command),
    }
}

fn blank(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(1, 0, index),
        payload: EntryPayload::Blank,
    }
}

fn membership_entry(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(1, 0, index),
        payload: EntryPayload::Membership(Membership::new(vec![BTreeSet::from([0])], ())),
    }
}

#[tokio::test]
async fn test_apply_folds_normal_commands_through_one_state_in_order() {
    let mut machine = OwnershipStateMachine::default();

    let responses = machine
        .apply(vec![normal(1, assign("proj", "east")), normal(2, advance("proj"))])
        .await
        .unwrap();

    assert_eq!(
        responses,
        vec![
            OwnershipResponse::Applied(OwnershipEffect::Assigned {
                epoch: AuthorityEpoch(1)
            }),
            OwnershipResponse::Applied(OwnershipEffect::EpochAdvanced {
                epoch: AuthorityEpoch(2)
            }),
        ]
    );
}

#[tokio::test]
async fn test_apply_records_the_last_applied_log_id() {
    let mut machine = OwnershipStateMachine::default();

    machine.apply(vec![normal(1, assign("proj", "east"))]).await.unwrap();

    let (last_applied, _membership) = machine.applied_state().await.unwrap();
    assert_eq!(last_applied, Some(log_id(1, 0, 1)));
}

#[tokio::test]
async fn test_a_blank_entry_applies_as_non_mutating() {
    let mut machine = OwnershipStateMachine::default();

    let responses = machine.apply(vec![blank(1)]).await.unwrap();

    assert_eq!(responses, vec![OwnershipResponse::NonMutating]);
    let (last_applied, _) = machine.applied_state().await.unwrap();
    assert_eq!(last_applied, Some(log_id(1, 0, 1)));
}

#[tokio::test]
async fn test_a_membership_entry_records_membership_without_mutating_ownership() {
    let mut machine = OwnershipStateMachine::default();

    let responses = machine.apply(vec![membership_entry(4)]).await.unwrap();

    assert_eq!(responses, vec![OwnershipResponse::NonMutating]);
    let (_, membership) = machine.applied_state().await.unwrap();
    assert_eq!(membership.log_id(), &Some(log_id(1, 0, 4)));
    assert_eq!(membership.membership(), &Membership::new(vec![BTreeSet::from([0])], ()));
}

#[tokio::test]
async fn test_applied_state_starts_empty() {
    let mut machine = OwnershipStateMachine::default();

    let (last_applied, membership) = machine.applied_state().await.unwrap();

    assert_eq!(last_applied, None);
    assert_eq!(membership.log_id(), &None);
}

#[tokio::test]
async fn test_get_current_snapshot_is_none_before_any_build() {
    let mut machine = OwnershipStateMachine::default();

    assert!(machine.get_current_snapshot().await.unwrap().is_none());
}

#[tokio::test]
async fn test_build_snapshot_on_an_empty_machine_carries_no_log_id() {
    let mut machine = OwnershipStateMachine::default();

    let snapshot = machine.get_snapshot_builder().await.build_snapshot().await.unwrap();

    assert_eq!(snapshot.meta.last_log_id, None);
    assert_eq!(snapshot.meta.snapshot_id, "0-1");
}

#[tokio::test]
async fn test_build_snapshot_captures_the_applied_log_id_and_is_retrievable() {
    let mut machine = OwnershipStateMachine::default();
    machine.apply(vec![normal(7, assign("proj", "east"))]).await.unwrap();

    let built = machine.get_snapshot_builder().await.build_snapshot().await.unwrap();

    assert_eq!(built.meta.last_log_id, Some(log_id(1, 0, 7)));
    let current = machine.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(current.meta, built.meta);
}

#[tokio::test]
async fn test_begin_receiving_snapshot_hands_back_an_empty_buffer() {
    let mut machine = OwnershipStateMachine::default();

    let buffer = machine.begin_receiving_snapshot().await.unwrap();

    assert!(buffer.into_inner().is_empty());
}

#[tokio::test]
async fn test_installing_a_snapshot_restores_state_onto_a_fresh_machine() {
    let mut source = OwnershipStateMachine::default();
    source
        .apply(vec![normal(1, assign("proj", "east")), normal(2, advance("proj"))])
        .await
        .unwrap();
    let Snapshot { meta, snapshot } = source.get_snapshot_builder().await.build_snapshot().await.unwrap();

    let mut restored = OwnershipStateMachine::default();
    restored.install_snapshot(&meta, snapshot).await.unwrap();

    let (last_applied, _) = restored.applied_state().await.unwrap();
    assert_eq!(last_applied, Some(log_id(1, 0, 2)));
    // The restored state carries the assigned authority: advancing it lands epoch three, which only
    // holds if the assign and the first advance survived the snapshot round-trip.
    let responses = restored.apply(vec![normal(3, advance("proj"))]).await.unwrap();
    assert_eq!(
        responses,
        vec![OwnershipResponse::Applied(OwnershipEffect::EpochAdvanced {
            epoch: AuthorityEpoch(3)
        })]
    );
}

#[tokio::test]
async fn test_installing_a_snapshot_makes_it_the_current_snapshot() {
    let mut source = OwnershipStateMachine::default();
    source.apply(vec![normal(1, assign("proj", "east"))]).await.unwrap();
    let Snapshot { meta, snapshot } = source.get_snapshot_builder().await.build_snapshot().await.unwrap();

    let mut restored = OwnershipStateMachine::default();
    restored.install_snapshot(&meta, snapshot).await.unwrap();

    assert_eq!(restored.get_current_snapshot().await.unwrap().unwrap().meta, meta);
}

#[tokio::test]
async fn test_installing_a_corrupt_snapshot_fails_closed() {
    let mut source = OwnershipStateMachine::default();
    let Snapshot { meta, .. } = source.get_snapshot_builder().await.build_snapshot().await.unwrap();

    let mut machine = OwnershipStateMachine::default();
    let result = machine
        .install_snapshot(&meta, Box::new(std::io::Cursor::new(b"not a snapshot".to_vec())))
        .await;

    assert!(result.is_err());
}

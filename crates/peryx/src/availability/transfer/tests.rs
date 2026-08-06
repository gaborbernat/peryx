use std::sync::{Arc, Mutex};

use peryx_driver::state::{
    CommandOutcome, CommandReceipt, ControlCommand, ControlError, ControlPlane, MembershipControl,
};
use peryx_replication::{AuthorityKey, DatacenterId, TransferPhase, TransferPlan, TransferRequest};
use peryx_storage::meta::MetaStore;

use super::{EpochOracle, FrontierSource, TransferDriveError, commit_transfer, observe_target};

const BARRIER: u64 = 5;

fn request() -> TransferRequest {
    TransferRequest {
        authority: AuthorityKey("proj".to_owned()),
        source: DatacenterId("east".to_owned()),
        target: DatacenterId("west".to_owned()),
        actor: "alice".to_owned(),
        reason: "drain east".to_owned(),
        barrier: BARRIER,
    }
}

/// A frontier source returning one scripted answer for the target.
struct ScriptedFrontier(Mutex<Vec<anyhow::Result<Option<u64>>>>);

impl ScriptedFrontier {
    fn new(answers: impl IntoIterator<Item = anyhow::Result<Option<u64>>>) -> Self {
        let mut answers: Vec<_> = answers.into_iter().collect();
        answers.reverse();
        Self(Mutex::new(answers))
    }
}

#[async_trait::async_trait]
impl FrontierSource for ScriptedFrontier {
    async fn applied_frontier(&self, _datacenter: &str) -> anyhow::Result<Option<u64>> {
        self.0
            .lock()
            .unwrap()
            .pop()
            .expect("the scripted frontier ran out of answers")
    }
}

/// An epoch oracle returning a fixed committed epoch.
struct FixedEpoch(u64);

#[async_trait::async_trait]
impl EpochOracle for FixedEpoch {
    async fn committed_epoch(&self, _authority: &str) -> u64 {
        self.0
    }
}

/// A membership control returning one scripted result and recording its submissions.
struct ScriptedControl {
    result: Mutex<Option<Result<CommandReceipt, ControlError>>>,
    submissions: Mutex<Vec<ControlCommand>>,
}

impl ScriptedControl {
    fn new(result: Result<CommandReceipt, ControlError>) -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(Some(result)),
            submissions: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait::async_trait]
impl MembershipControl for ScriptedControl {
    async fn submit(&self, command: ControlCommand) -> Result<CommandReceipt, ControlError> {
        self.submissions.lock().unwrap().push(command);
        self.result
            .lock()
            .unwrap()
            .take()
            .expect("the scripted control was submitted twice")
    }
}

fn receipt(index: u64) -> CommandReceipt {
    CommandReceipt {
        term: 1,
        index,
        outcome: CommandOutcome::Committed,
        old_voters: Vec::new(),
        new_voters: Vec::new(),
    }
}

fn control(result: Result<CommandReceipt, ControlError>) -> (Arc<ScriptedControl>, ControlPlane) {
    let scripted = ScriptedControl::new(result);
    let plane = ControlPlane::new(scripted.clone(), Arc::new(|| 0));
    (scripted, plane)
}

fn meta() -> (tempfile::TempDir, MetaStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    (dir, store)
}

#[tokio::test]
async fn test_observe_target_waits_below_the_barrier_then_readies_at_it() {
    let mut plan = TransferPlan::plan(request());
    let frontier = ScriptedFrontier::new([Ok(Some(BARRIER - 1)), Ok(Some(BARRIER))]);

    assert_eq!(
        observe_target(&mut plan, &frontier).await.unwrap(),
        TransferPhase::AwaitingCatchUp
    );
    assert_eq!(
        observe_target(&mut plan, &frontier).await.unwrap(),
        TransferPhase::Ready
    );
}

#[tokio::test]
async fn test_observe_target_treats_an_unreachable_target_as_frontier_zero() {
    let mut plan = TransferPlan::plan(request());
    let frontier = ScriptedFrontier::new([Ok(None)]);

    // A target that answers with no frontier never reaches the barrier, so the move keeps waiting.
    assert_eq!(
        observe_target(&mut plan, &frontier).await.unwrap(),
        TransferPhase::AwaitingCatchUp
    );
}

#[tokio::test]
async fn test_observe_target_surfaces_a_frontier_read_error() {
    let mut plan = TransferPlan::plan(request());
    let frontier = ScriptedFrontier::new([Err(anyhow::anyhow!("unreachable"))]);

    let error = observe_target(&mut plan, &frontier).await.unwrap_err();
    assert!(matches!(error, TransferDriveError::Frontier(_)));
}

#[tokio::test]
async fn test_commit_transfer_commits_derives_the_epoch_and_persists_the_audit() {
    let mut plan = TransferPlan::plan(request());
    assert_eq!(plan.observe_frontier(BARRIER), TransferPhase::Ready);
    let (scripted, plane) = control(Ok(receipt(9)));
    let (_dir, store) = meta();

    let audit = commit_transfer(&mut plan, &plane, &FixedEpoch(3), &store, Some("k1"))
        .await
        .unwrap();

    // The audit carries the committed log index and the epoch read back from committed state.
    assert_eq!((audit.epoch.0, audit.commit_index), (3, 9));
    assert_eq!(audit.authority.0, "proj");
    assert_eq!(audit.target.0, "west");
    // The move was submitted once as a transfer command, and the audit is durable.
    assert_eq!(scripted.submissions.lock().unwrap().len(), 1);
    let persisted = store.transfer_audits("proj").unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(
        (
            persisted[0].epoch,
            persisted[0].commit_index,
            persisted[0].reason.as_str()
        ),
        (3, 9, "drain east")
    );
}

#[tokio::test]
async fn test_commit_transfer_surfaces_a_refused_commit_without_persisting() {
    let mut plan = TransferPlan::plan(request());
    plan.observe_frontier(BARRIER);
    let (_scripted, plane) = control(Err(ControlError::NotLeader { leader: None }));
    let (_dir, store) = meta();

    let error = commit_transfer(&mut plan, &plane, &FixedEpoch(3), &store, None)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        TransferDriveError::Commit(ControlError::NotLeader { .. })
    ));
    assert!(store.transfer_audits("proj").unwrap().is_empty());
}

#[tokio::test]
async fn test_commit_transfer_refuses_a_plan_that_has_not_reached_the_barrier() {
    let mut plan = TransferPlan::plan(request());
    let (scripted, plane) = control(Ok(receipt(9)));
    let (_dir, store) = meta();

    let error = commit_transfer(&mut plan, &plane, &FixedEpoch(3), &store, None)
        .await
        .unwrap_err();

    // A plan still awaiting catch-up never reaches consensus, so no move commits.
    assert!(matches!(
        error,
        TransferDriveError::Plan(peryx_replication::TransferError::BarrierNotMet)
    ));
    assert!(scripted.submissions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_commit_transfer_refuses_a_cancelled_plan_without_committing() {
    let mut plan = TransferPlan::plan(request());
    plan.observe_frontier(BARRIER);
    plan.cancel().unwrap();
    let (scripted, plane) = control(Ok(receipt(9)));
    let (_dir, store) = meta();

    let error = commit_transfer(&mut plan, &plane, &FixedEpoch(3), &store, None)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        TransferDriveError::Plan(peryx_replication::TransferError::Cancelled)
    ));
    assert!(scripted.submissions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_commit_transfer_replays_a_committed_plan_without_recommitting() {
    let mut plan = TransferPlan::plan(request());
    plan.observe_frontier(BARRIER);
    let (scripted, plane) = control(Ok(receipt(9)));
    let (_dir, store) = meta();

    let first = commit_transfer(&mut plan, &plane, &FixedEpoch(3), &store, None)
        .await
        .unwrap();
    // A second drive of the same committed plan reads back the sealed audit without a second submission.
    let replay = commit_transfer(&mut plan, &plane, &FixedEpoch(3), &store, None)
        .await
        .unwrap();

    assert_eq!(first, replay);
    assert_eq!(scripted.submissions.lock().unwrap().len(), 1);
}

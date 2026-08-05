# HANDOFF — #539 Transfer authority and drain retained writes after home failure

Branch `feat/authority-transfer-drain-539`, based on `upstream/main` (204cac59, includes #916 fence).
This is a checkpoint: the transfer-commit half is built and tested; the drain half, benchmarks, and docs
remain. Everything below is turnkey — file paths are absolute-relative to the worktree root.

## Rebase note (do first)

When #926 (`fix(consensus): disable openraft debug-assertions`) merges, rebase onto the new main. It adds
one block to root `Cargo.toml`:

```toml
[profile.dev.package.openraft]
debug-assertions = false
```

Any availability-e2e harness test added here (the live failover test below) will hit the openraft
`following_handler` `debug_assert` abort under CI load without that override. If you add the harness test
before #926 merges, add the same Cargo.toml block on this branch (trivial conflict on rebase).

## What is committed and green

Two commits (`559cdd72`, `ad1952f1`). Workspace compiles (`cargo test --all-features --workspace
--no-run` clean). All new tests pass.

1. **Pure failover policy** — `crates/peryx-replication/src/failover.rs` (+ `failover_tests.rs`, 10
   tests). `FailoverPolicy::select(home: Suspicion, candidates: &[Candidate]) -> Failover`. `Failover` is
   `Transfer(DatacenterId) | NoCandidate | Hold`. Rules: holds unless the home is `Suspicion::Dead`
   (suspicion never moves authority — the #539 constraint); for a dead home takes the first
   `Suspicion::Alive` candidate in a bounded pass (`max_candidates`), deterministic on caller order.
   Exported in `lib.rs` (`pub use failover::{Candidate, Failover, FailoverPolicy};`), module + test module
   declared alphabetically.

2. **Transfer-commit wiring** — `OwnershipCommand::RecordTransfer` existed with no submitter; now wired:
   - `crates/peryx-driver/src/state/ownership.rs`: new `TransferOutcome { from, to, epoch }` type; trait
     method `async fn transfer_home(&self, authority, new_home) -> Result<Option<TransferOutcome>,
     OwnershipError>`; free helper `transfer_authority_home(group, authority, new_home)` (Ok(None) with no
     group). Fake double extended + 3 tests (commit, control-minority NotLeader, no-group).
   - `crates/peryx-driver/src/state/app.rs`: `ServingState::transfer_authority_home` delegate.
   - `crates/peryx-driver/src/state/mod.rs`: re-exports `TransferOutcome`.
   - `crates/peryx/src/replication/raft.rs`: `OwnershipGroup::transfer_home` submits `RecordTransfer`,
     maps `Transferred{from,to,epoch}` → `Some`, rejection → `Ok(None)`, `ForwardToLeader` → `NotLeader`
     (control minority cannot transfer), else `Unavailable`.
   - `crates/peryx/src/replication/raft_tests.rs`: 5 tests (moves+advances epoch+fences old, unassigned
     no-op, same-home no-op, control-minority NotLeader, stopped-group Unavailable).
   - The other 4 `OwnershipAuthority` doubles got `transfer_home` returning `Ok(None)` (E0046 cascade):
     `availability_listener_tests.rs` FixedGroup, pypi + oci `RecordingAuthority`, `jobs/tests.rs`
     MutableEpoch.

3. **Harness un-stub** — `crates/peryx/tests/harness/mod.rs`: `OwnershipControl::leader` and
   `await_authority_transfer` are now real (quorum-poll `consensus.leader` from
   `/availability/v1/status`); `submit_ownership_write` stays blocked on #540 (no write endpoint). Added
   `HarnessError::NoTransfer`. Compiles under `--features availability-e2e`.

## Remaining work (to Close #539)

### A. AuthorityDrain job — the "drain retained writes" half

The retained-write ledger is #538: pure `crates/peryx-replication/src/ingress_intent.rs` (`IntentLedger`,
`IngressIntent`, `IntentState::{Pending,Admitted,Expired}`, `advance`) and a durable meta counterpart
`crates/peryx-storage/src/meta/ingress_intent.rs` (read it — mirror its record shape). #537 classifier is
`crates/peryx-replication/src/reconcile.rs` (`classify(&OldEpochOp) -> Disposition`).

Build an ordered, resumable, fence-protected drain that, after a transfer commits the new epoch,
finalizes the old home's `Pending` intents into LOCAL metadata at the new home (`Pending → Admitted`),
classified via `reconcile::classify`, in a bounded batch loop:

- Add `JobKind::AuthorityDrain` to `crates/peryx-storage/src/meta/job.rs:20-29`.
- Implement a `NodeJob` (pattern: `crates/peryx-ecosystem-pypi/src/catalog_job.rs`) with
  `repository() = Some(authority)` + `persist_as() = Some(JobKind::AuthorityDrain)` so the scheduler
  leases the fence and the fence gate at `crates/peryx-driver/src/jobs/scheduler.rs:310-321` rejects a
  drain whose epoch was superseded mid-run (deterministic test: advance the epoch during the run → run
  returns `authority_fenced`, mirroring #916's `jobs/tests.rs` AdvancingJob test).
- Order + resumability: drain intents in a stable key order; each finalize is idempotent
  (`IntentLedger::advance` only moves forward), so a re-run after interruption resumes. Bound batch size
  (mirror `transfer_attempt.rs` `compact_transfer_attempts` / `RETENTION_BATCH`).
- Register via a `ScheduledJob` variant (`crates/peryx-driver/src/jobs/timer.rs:27-49`) or trigger it from
  the transfer path; wire an `app/jobs.rs` CLI entry if the other jobs have one.
- Deterministic test (in-process, no cross-DC network — coordinator confirmed): stage N `Pending`
  intents, commit a transfer, run the drain, assert all `Admitted` in local metadata and one outcome per
  intent; assert order; assert resume after a mid-drain interruption; assert the fence gate rejects a
  stale-epoch drain.

The cross-DC "publish to a remote home" network arm has no producer surface yet (like #522's harness arm
gated on the unlanded placement producer) — note it as a #923-style follow-up; the in-process finalize
proves the acceptance.

### B. Live harness failover test — `crates/peryx/tests/cluster.rs` (availability-e2e)

Form a 3-node `ha` cluster, record `cluster.leader()`, kill that datacenter's node, then
`cluster.await_authority_transfer(&old_leader, Duration::from_secs(N))` → assert a new datacenter took
authority. Needs the #926 openraft override (see rebase note). Mirror the quorum-wait style already in
`cluster.rs`.

### C. Benchmarks — `crates/peryx-bench/` (see existing benches for the harness)

Report: failover RTO, drain throughput, disk use, unaffected-DC p99. CodSpeed-gated (`peryx-bench` is
coverage-excluded). Keep them deterministic (fixed inputs); the standard no-LTO bench profile.

### D. Docs — `site/content/`

Document failure thresholds, candidate selection, failover RTO, drain behavior, and operator recovery.
Format via the pre-commit `mdformat` hook (not plain mdformat — mermaid shortcodes). Find the HA docs
section under `site/content/core/` or `site/content/ecosystems/`; follow neighboring page front-matter.

## Acceptance checklist (issue #539)

- [x] Transfer selects an eligible target and commits a fenced new epoch (failover.rs + transfer_home).
- [x] A control minority cannot transfer authority (ForwardToLeader → NotLeader, tested).
- [ ] Ordered drain of retained intents, resumable after interruption (A).
- [ ] Home loss at each transfer boundary yields one home and one outcome per operation (A test).
- [ ] Benchmarks: failover RTO, drain throughput, disk use, unaffected-DC p99 (C).
- [ ] Docs (D).
- [x] Harness `await_authority_transfer` un-stubbed (leader-failover observation); live test = B.

## Standing rules for finishing

Canonical build: `bash /Volumes/OWC/cargo-target/.cargo-serial.sh <cargo cmd>`. Full pre-commit incl lint
(`prek run --all-files`) + `cargo test --all-features --workspace --no-run` before every push. 100% x86
coverage (functions AND lines) — the macOS number lies; the x86 gate is authoritative. commit/pr/no-slop
skills. One complete PR, `Closes #539`. Delete this file before finalizing the PR.

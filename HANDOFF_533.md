# Handoff — #533 Finalize admitted uploads at the home DC

Branch `feat/finalize-home-dc-533` off `upstream/main` @ `204cac59`. Delete this file before opening the PR.

## CONTINUATION STATUS (2026-08-05) — storage foundation DONE, PyPI finalize remaining

Coordinator confirmed: **scope = PyPI-only** (OCI has no ingress-staging intent; file an OCI-finalize
follow-up that first needs an OCI equivalent of #530, reference it in the PR body). **Fork (b) confirmed**:
`finalize_admitted_upload(state, intent_key, descriptor)` with descriptor-as-input (the descriptor's
transport is #541 routing, excluded from #533; tests call the driver/state entry point directly).
**Quota confirmed**: admission (`serving/admission.rs`) does NOT touch quota; quota is reserved pre-admit
in `serving/post.rs::project_quota_reservation` and committed at publish — so finalize commits it via the
quota-threaded storage path.

### Done + committed (all tests pass; storage layer complete)

Both finalize methods take a `FinalizedWrite<'_> { operation, intent_key, response, expiry_unix, now }`
param object (exported from `meta`) instead of positional args:

- `922e1a45` (+ FinalizedWrite refactor) — neutral primitive `MetaStore::commit_finalized_write<E: From<MetaError>>(write: FinalizedWrite<'_>, body: FnOnce(&mut DriverTxn) -> Result<Vec<Vec<u8>>, E>) -> Result<FinalizeOutcome, E>` in `crates/peryx-storage/src/meta/finalize.rs`. Atomic metadata+journal+outcome+intent-advance via `commit_driver_txn_at`'s finalize hook; in-txn terminal guard → `RaceReplay` → re-read → `FinalizeOutcome::Replayed`. `FinalizeOutcome::{Published, Replayed(OperationOutcomeRecord)}` exported from `meta`. Fault tests `meta/finalize_fault_tests.rs` (redb backend seam, fail-at-each-offset, exactly-once serial). Public tests `tests/meta/finalize_tests.rs`.
- `177fc933` — quota-threaded `MetaStore::commit_finalized_write_with_quota<E: From<MetaError> + From<QuotaError>>(write: FinalizedWrite<'_>, reservation: Uuid, body: FnOnce(&mut DriverTxn) -> Result<(bool, Vec<Vec<u8>>), E>) -> Result<FinalizeOutcome, E>` in `crates/peryx-storage/src/meta/quota.rs`. `body`'s bool = published; commits the reservation when published, releases it on skip, discards the move on replay. Shares `stamp_finalized` + `resolve_finalize` (both `pub(super)` in finalize.rs) with the plain path.

Storage `commit_finalized_write*` is the whole of acceptance criteria 2/3/4 at the store layer. Do NOT rebuild it.

### Remaining: the PyPI finalize (new `crates/peryx-ecosystem-pypi/src/serving/finalize.rs`) — every API mapped

Descriptor (what #541 routing supplies; tests construct it) must carry enough to build a `store::PublishedFile`
(`store/uploads.rs:33`) plus the authz principal: `{ hosted_index_name, normalized, display, filename,
artifact_sha256, artifact_size, record_bytes, version, submitted_at_unix, metadata_sibling{url,sha,size,source},
provenance_sibling?{sha,size}, quota_reservation_id?, principal }`.

`finalize_admitted_upload(state, intent_key, descriptor)` control flow:
1. `state.meta.staged_intent(intent_key)?` → `None` ⇒ `NotStaged`. Decode the intent payload — expose a
   `pub(crate) fn decode_intent(&StagedIntent) -> IngressIntent` (or make fields `pub(crate)`) from
   `serving/admission.rs` (the `IngressIntent` struct + `operation = "{intent_key}:{digest}"`). `op = intent.operation`.
2. Idempotent replay: `state.meta.operation_outcome(&op)?` terminal ⇒ replay (`Published`→same response, `Failed`→same error). No work.
3. **Fence** (acceptance: stale epoch fails before publish): `let e = state.committed_authority_epoch(&intent.authority).await; if !state.admit_authority_epoch(&intent.authority, e).await { fail(op, Fenced) }`. (Presents committed epoch; #541 carries the request epoch. Triggers on unassigned authority = committed 0 with a group.)
4. **Validate before publish** — each failure `fail(op, …)` (stamp a Failed outcome, return the error, no publish):
   - checksum: `intent.digest == descriptor.artifact_sha256 && intent.size == descriptor.artifact_size`.
   - placement: `state.meta.get_artifact_placement(&intent.digest)?.is_some()` (`meta/placement.rs:263`).
   - authz (permissions may have changed): resolve the hosted `Index` from `state`, `peryx_identity::authorize(&descriptor.principal, &hosted.acl, Some(&descriptor.normalized), Action::Write)` (`peryx-identity/src/acl.rs:20`).
   - policy: mirror `serving/post.rs::upload_policy_response` / `PolicyAction::Upload` against the current `index.policy`.
5. **Publish atomically**: build `PublishedFile` from the descriptor; body = `store::uploads::publish_file_in_txn(txn, &file, upload_conflict_guard)` (already a free `fn` at `store/uploads.rs:150`, returns `(bool wrote, Vec<Vec<u8>>)` — perfect for `commit_finalized_write_with_quota`'s body; for the unmetered case use plain `commit_finalized_write` and drop the bool). `response_bytes` = the serialized client upload response so a retry replays it.
6. Return `Published`/`Replayed`; the primitive advanced the intent to `Admitted`.

`fail(op, reason)`: stamp a terminal `Failed` outcome so retries replay it. Simplest: `claim_operation(op)` then
`finalize_operation(op, OperationResult::Failed, response, now)` (`meta/operation_outcome.rs`), or add a one-txn
`fail_operation` helper. A failed finalize leaves the intent `Pending` (reclaimed by expiry).

Driver-state entry point: a thin `pub` fn (in `crates/peryx-driver/src/state/` or the pypi serving layer) tests call
directly, like `apply_replicated_page` — NOT an HTTP route (routing is #541).

Docs: `site/content/core/` (availability finalization: states, validation errors, retries, visibility, remote
placement) + `site/content/ecosystems/pypi/reference/uploads.md`. Format via the pre-commit mdformat hook.

### Test harness notes (for 100% x86, deterministic)
- Fence test: inject a mock `OwnershipAuthority` via `AppState::set_ownership_authority(Arc<dyn OwnershipAuthority>)`
  (`state/app.rs:217`) returning committed epoch 0 with a group ⇒ `admit_authority_epoch` false ⇒ Fenced.
- Fault tests at each durable boundary: the finalize commit is one `commit_driver_txn_at` txn — the storage-layer
  fault suite already proves atomicity; at the PyPI layer drive validation-failure + replay + published paths and
  assert one terminal outcome + one journal entry (acceptance criteria 3 + 4).
- Verify coverage on x86 (`coverage-lcov`, cross-compile+qemu per the `peryx-coverage-arch-gap` memory), never macOS.

### Build/PR rules
Canonical `bash /Volumes/OWC/cargo-target/.cargo-serial.sh cargo <cmd> --manifest-path <this worktree>/Cargo.toml`
(note: pass the literal `cargo`). Full pre-commit incl clippy + mdformat before every push. commit skill + pr skill
(`--body-file`, no footer, run through no-slop). `crates/peryx/src/replication.rs` is a network-lane conflict
hotspot — route rebases to the coordinator. Delete this file before the PR.

---


## Verification (done)

- **Not gated on #541.** #533's constraints explicitly "Exclude ... protocol routing, artifact copying, and authority transfer." Routing is #541; #533 is the finalize *mechanism* #541 later wires. Buildable now.
- Deps present: #530 staging merged (#917); #916 fence merged (`204cac59`). Ownership/epoch source is wired to `AppState` (see below).
- Unowned: no open PR / remote branch for #533.

## The primitives that already exist (reuse, don't rebuild)

1. **Staged intent ledger (#530)** — `crates/peryx-storage/src/meta/ingress_intent.rs`:
   - `StagedIntent { phase: IntentPhase, digest, size, payload: Vec<u8>, updated_at_unix }`, `IntentPhase::{Pending, Admitted, Expired}` (advancing order).
   - `stage_intent(key,digest,size,payload,limit,now) -> IntentStageOutcome`, `advance_intent(key, to, now) -> IntentTransition`, `staged_intent(key) -> Option<StagedIntent>`, `count_staged_intents()`.
   - Table const `INGRESS_INTENT` (in `meta/mod.rs`).
   - The PyPI intent payload is `serving/admission.rs::IngressIntent { tenant, authority, digest, size, ingress_dc, operation }` (private). `intent_key = "pypi:{tenant}:{authority}:{filename}"`, `operation = "{intent_key}:{digest}"`.

2. **Idempotency ledger** — `crates/peryx-storage/src/meta/operation_outcome.rs`:
   - `OperationState::{Pending,Published,Failed}` (`is_terminal()`), `OperationResult::{Published,Failed}`, `OperationOutcomeRecord { state, response: Vec<u8>, expiry_unix, updated_at_unix }`.
   - `claim_operation`, `finalize_operation`, `operation_outcome`, `prune_operation_outcomes`. Table const `OPERATION_OUTCOME`.
   - NOTE: `finalize_operation` uses its OWN txn — NOT atomic with metadata. #533 needs the terminal outcome committed IN the metadata txn (see design), so do not call `finalize_operation` on the publish path; stamp the row inside the finalize txn.

3. **Authority-epoch fence (#916/#536)** — `crates/peryx-replication/src/authority.rs` (`AuthorityFence`, `AuthorityKey`, `Admission::{Admit,Fenced}`) + ownership SM `crates/peryx-replication/src/{ownership.rs,raft/state_machine.rs}`. Wired to runtime via `crates/peryx-driver/src/state/ownership.rs` `OwnershipAuthority` trait, registered on `AppState` (`state/app.rs`):
   - `AppState::committed_authority_epoch(authority: &str) -> u64` (async) — 0 = unassigned/no group.
   - `AppState::admit_authority_epoch(authority, presented: u64) -> bool` (async) — true when no group; else only the committed epoch admits (0 admits nothing).
   - `AppState::claim_first_publish_home(authority)` — #541's job; #533 only READS `has_home`/`committed_epoch`.

4. **Outbox = the journal.** `commit_driver_txn(|txn: &mut DriverTxn| -> Result<(T, Vec<journal_bytes>), E>)` writes DRIVER_KV rows AND appends the returned journal entries (one serial each) in ONE redb write txn (`meta/index.rs:203`). Replicas reconcile journal entries. Atomic "metadata + outbox" = this.

5. **Atomic multi-table hook.** `MetaStore::commit_driver_txn_at(expected_serial, catalog_gen, durable, finalize: FnOnce(&redb::WriteTransaction,&T)->Result<(),E>, body) ` (`meta/index.rs:242`). The `finalize` hook runs AFTER the journal append, BEFORE `txn.commit()`, with the raw `&WriteTransaction` — open `OPERATION_OUTCOME` + `INGRESS_INTENT` there to stamp the outcome and advance the intent in the SAME txn. This is the key to atomicity.

6. **PyPI publish row-writer** — `crates/peryx-ecosystem-pypi/src/store/uploads.rs::publish_file_if` (writes upload row + project marker + journal via its own `commit_driver_txn`). Refactor the row-staging core into a `DriverTxn`-body helper so the finalize path reuses it (don't nest txns).

## Design to build

### A. Storage primitive (new: `crates/peryx-storage/src/meta/finalize.rs`)

```rust
pub enum FinalizeOutcome { Published, Replayed(OperationOutcomeRecord) }

pub fn commit_finalized_write<E: From<MetaError>>(
    &self, operation: &str, intent_key: &str, response: &[u8], expiry_unix: Option<i64>, now: i64,
    body: impl FnOnce(&mut DriverTxn) -> Result<Vec<Vec<u8>>, E>,
) -> Result<FinalizeOutcome, E>
```
- Fast replay: `operation_outcome(operation)?` terminal → `Replayed(record)` (no txn).
- Else `commit_driver_txn_at(None,None,true, finalize_hook, body_wrapper)` with a private `enum FinalizeFlow<E>{User(E),RaceReplay}` (`impl<E:From<MetaError>> From<MetaError> for FinalizeFlow<E>`):
  - body_wrapper: run user `body` (stages DRIVER_KV rows), return `((), journal)`.
  - finalize_hook: open `OPERATION_OUTCOME`; if row terminal → `Err(RaceReplay)` (drops txn, discards metadata → no double-publish/double-journal); else insert `Published{response,expiry}` and advance `INGRESS_INTENT[intent_key]` to `Admitted` (guard `to > phase`).
  - Map result: `Ok`→`Published`; `Err(RaceReplay)`→re-read→`Replayed`; `Err(User(e))`→`e`.
- Tests (peryx-storage): publish-then-replay returns same response w/o second journal; concurrent duplicate → one journal entry; **fault at each boundary** using the redb StorageBackend fault seam (see `crates/peryx-storage/src/tests/` + `job_fault_tests.rs` for the injection pattern) — crash before commit → nothing written, retry re-runs to same terminal; crash "after" → terminal replay. Assert serial advanced exactly once.

### B. PyPI finalize (`store/uploads.rs` + `serving/`)

`finalize_admitted_upload(state, intent_key) -> FinalizeResult`:
1. `staged_intent(intent_key)?` → decode `IngressIntent` (make the struct `pub(crate)` or add a decoder). Absent → NotStaged.
2. **Fence:** `state.admit_authority_epoch(&intent.authority, state.committed_authority_epoch(&intent.authority).await).await` — reject (Fenced) when unassigned/stale ⇒ acceptance "stale epochs ... fail before publication". (Present the committed epoch; #541 will carry the request epoch.)
3. **Validation before publication** (acceptance #1): authorization (re-authorize the tenant/authority against current ACL — permissions may have changed), policy (`index.policy` admission), placement/bytes present (`get_artifact_placement`/blob head for `intent.digest`), checksum (staged `digest`/`size` vs the stored blob). Any failure → stamp `Failed` outcome (via `finalize_operation` OR a Failed variant of the primitive) and return the protocol error; do NOT publish.
4. **Publish atomically:** `meta.commit_finalized_write(operation, intent_key, response_bytes, expiry, now, |driver| { stage upload row + project marker; Ok(journal_entries) })` reusing the extracted `publish_file_if` row-writer. `response_bytes` = the serialized client upload response so a retry replays it.
5. Idempotent: replay returns the same response; intent advanced to `Admitted`.

Acceptance mapping: #1 fence+validation before publish; #2 replay via operation outcome; #3 fault tests around the single commit boundary; #4 metadata+journal in one txn (primitive); #5 docs.

### C. OCI finalize (`crates/peryx-ecosystem-oci/src/registry/manifests/write.rs` + `outbox.rs`)

OCI already journals via `outbox.rs::record` + `commit_driver_txn`. Mirror B: a fenced idempotent finalize of a staged manifest push using `commit_finalized_write` with an `OciMutation` journal entry. VERIFY whether OCI has an ingress-staging intent yet (grep `stage_intent` in oci) — #530 may have been PyPI-only. If OCI staging does NOT exist, #533's OCI half is just the fenced-idempotent-atomic finalize wrapper over the existing manifest publish (no intent to consume), or scope OCI to the neutral primitive + PyPI and confirm with coordinator whether OCI finalize is in-scope for #533 vs a follow-up. Coordinator framed #533 as the finalize mechanism; the PyPI path is the primary acceptance surface.

### D. Config / wiring

- Local DC: `serving/admission.rs::ingress_dc(topology)`. Home lookup is #541; #533 uses `committed_epoch != 0` / `has_home` as "authority assigned".
- The finalize trigger for #533 is an internal driver/state entry point (NOT an HTTP route — routing is #541). A deterministic test calls it directly (like `apply_replicated_page` tests), so it doesn't ride async scheduling.

## Files to touch

`crates/peryx-storage/src/meta/{finalize.rs (new), mod.rs (module + table already exist)}`, `crates/peryx-ecosystem-pypi/src/{store/uploads.rs, serving/mod.rs or a new serving/finalize.rs, serving/admission.rs (expose IngressIntent)}`, `crates/peryx-ecosystem-oci/src/registry/manifests/write.rs` (+ `outbox.rs`), driver state entry point in `crates/peryx-driver/src/state/`. Docs: `site/content/core/` (availability finalization) + `site/content/ecosystems/pypi/reference/uploads.md`.

## Existing tests to extend

`crates/peryx-ecosystem-pypi/src/serving/admission.rs::tests`, `crates/peryx-storage/src/meta/{ingress_intent.rs,operation_outcome.rs}` inline tests, the redb fault seam in `crates/peryx-storage/src/tests/` + `meta/job_fault_tests.rs`, PyPI upload tests `crates/peryx-ecosystem-pypi/src/tests/http/upload.rs`.

## Working rules

Canonical `bash /Volumes/OWC/cargo-target/.cargo-serial.sh <cargo cmd>` targeting THIS worktree manifest (`--manifest-path /Volumes/OWC/git/peryx/peryx-finalize-uploads-533/Cargo.toml`). 100% x86 functions+lines. Full pre-commit incl clippy+mdformat. One complete PR that Closes #533. `crates/peryx/src/replication.rs` is a conflict-hotspot with the network lane — route rebase to the coordinator.

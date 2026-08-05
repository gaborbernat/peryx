# Handoff — #510 Apply PyPI derived views before replica visibility

Branch `feat/pypi-view-application-510` off `upstream/main` @ `fb5ab0c9`. No implementation committed yet — this is a verified foundation + precise plan so a fresh agent can build the single complete PR. Delete this file before opening the PR.

## Status of verification (done)

- #510 is completable: only dependency #509 is CLOSED (merged as PR #797 `feat(pypi): gate replica reads on the derived-view frontier`). The derived-view frontier infra exists in `crates/peryx-driver/src/state/derived_views.rs`.
- Unowned: no open PR, no remote branch, no in-flight ecosystem-lane branch touching the pypi view files.
- Only file overlap with an in-flight PR is my own #917 (`crates/peryx-ecosystem-pypi/src/serving/{post.rs,mod.rs,admission.rs}`). #510's only edit inside that set is the `apply_replicated_changes` body in `serving/mod.rs` (lines ~474-483) — keep that edit small and localized; the conflict-resolver rebases #917.

## The problem #510 solves

The #509/#797 frontier machinery is read-gate-only: `readable_frontier = min(authority_serial, each required view's durable frontier)`, and PyPI reads above it return 404 (`serving/get.rs:46 holds_below_readable_frontier`). But nothing on the replica apply path rebuilds a view and advances its frontier. Today `SEARCH_VIEW` advances only lazily, globally, on a `/+search` read (`crates/peryx-search/src/engine.rs:304 ensure_current` → `set_view_frontier(SEARCH_VIEW, current_serial())`), reindexing every project. So a replica exposes metadata whose search view is stale until someone searches, and a replicated per-file yank retires nothing. #510 makes the apply path rebuild the affected `(index, normalized project)` views and advance the frontier before visibility, scoped, crash-safe, holding + reporting on failure.

## Key APIs (inherited)

- `crates/peryx-storage/src/meta/frontier.rs`: `set_view_frontier(view,&str, serial:u64) -> Result<u64,MetaError>` (durable, monotonic), `view_frontier`, `view_frontiers`.
- `crates/peryx-driver/src/state/derived_views.rs`: `REQUIRED_VIEWS = &[SEARCH_VIEW]`, `ReadableFrontier { serial, blocking: Option<String> }`, `readable_frontier(authority, frontiers, required)`.
- `crates/peryx-driver/src/state/caches.rs`: `invalidate_project(&self, project)` (hot-cache epoch + `bump_search_epoch`), `bump_search_epoch`, `readable_frontier()`.
- Apply seam: `crates/peryx/src/replication.rs:401 apply_replicated_page(app, outcome, changed_keys)` → `bump_search_epoch()` + per-driver `EcosystemDriver::apply_replicated_changes(state, changed_keys)` (`crates/peryx-driver/src/serving.rs:370` default no-op; PyPI impl `crates/peryx-ecosystem-pypi/src/serving/mod.rs:474-483` maps keys via `project_of_key` → `invalidate_project`). `outcome.serial` is the applied authority serial.
- Search engine: `crates/peryx-search/src/engine.rs` — `SearchFields { route, normalized, index, ... }` (schema, `search_schema()` ~line 490; `route`/`normalized`/`index` are `STRING` = indexed exact terms). `document(&PackageDocument) -> TantivyDocument` (line 422). `write(&[PackageDocument])` (310) and `rebuild(...)` (168) both `delete_all_documents()` then re-add — GLOBAL. `PackageIndexer::documents(ctx) -> Vec<PackageDocument>` (`indexer.rs:16`) is the only derivation, GLOBAL, "replacing the current index contents". `PypiIndexer::documents` (`search_pypi.rs:30`) scans every index/project.

## Design (build this)

1. **Scoped search-index update (foundation).**
   - Schema: add a `key` field to `SearchFields` = `STRING | STORED`, value `format!("{route}\0{normalized}")`, set in `document()`. Schema change → `engine.open` already discards+rebuilds a mismatched index, so existing indexes rebuild once (acceptable; add a note).
   - Engine: add `pub fn update_project(&self, docs: &[PackageDocument], key: &str) -> Result<(), SearchError>` that `writer.delete_term(Term::from_field_text(fields.key, key))`, adds `docs`, `commit()`, `reader.reload()`. Idempotent (delete+add). Does NOT touch the frontier (caller advances it after all affected projects rebuild).
   - Indexer trait: add `fn project_documents(&self, ctx, index:&str, normalized:&str) -> Result<Vec<PackageDocument>, SearchError>` (default: filter `documents()` — but override in PyPI to derive only that project's docs from its stored records for scoping/bounded memory). `CompositeIndexer` dispatches to the matching ecosystem.
   - `search_pypi.rs`: implement `project_documents` reading only the one project's upload/index records (reuse the per-project record readers in `store/projects.rs`/`store/uploads.rs`), returning that project's `PackageDocument`(s).

2. **Apply-side rebuild-then-advance seam.**
   - Change `EcosystemDriver::apply_replicated_changes` to return `Result<(), ViewBlock>` (or similar) where `ViewBlock` names the blocking view. On the PyPI impl: for each affected `(index, normalized)`, `update_project(project_documents(...), key)` + `invalidate_project`. If any required-view update errors, return the block WITHOUT advancing.
   - In `apply_replicated_page`: after all drivers succeed, `state.meta.set_view_frontier(SEARCH_VIEW, outcome.serial)`. If a driver returned a block, do NOT advance (frontier holds at the prior value; `ReadableFrontier.blocking` already names the view) and log/emit the blocking view. Keep `bump_search_epoch()` for the primary/read path compatibility, or remove if the scoped path fully replaces it — verify search-read tests.
   - Crash-safe: `set_view_frontier` is durable+monotonic; scoped delete+add is idempotent; on restart the replica re-applies from its cursor and re-runs the scoped update (same result). Confirm no partial visibility (frontier only advances after commit+reload).

3. **`project_of_key` per-file scoping fix** (`crates/peryx-ecosystem-pypi/src/store/mod.rs:157`). Today an upload-prefixed key (`pypi\0f\0…` per-file yank/unyank/delete) maps to no project, so a replicated per-file yank invalidates + reindexes nothing. Map the file key back to its `(index, normalized)` (the upload record carries them) so the affected project's view rebuilds. This is the correctness fix the coordinator wants included.

## Acceptance to satisfy (all, one PR)

- upload/yank/restore/delete update all affected PyPI views before visibility (integration test on the replica apply path: apply a page, assert the search view frontier advanced to the applied serial and the affected project's doc reflects the change, before a read is served).
- crash recovery reaches the same responses as uninterrupted apply (reopen store mid-way; re-apply is idempotent).
- an operation for one project does not rebuild unrelated projects/indexes (assert only the affected `key` term is deleted/re-added; extend `tests/http/frontier.rs:126 test_apply_replicated_changes_retires_only_the_changed_projects` to also assert scoped search update + frontier advance).
- a failed required view update holds the readable frontier and reports the blocking view (inject an indexer failure for one project → frontier does not advance, `ReadableFrontier.blocking == SEARCH_VIEW`).
- bounded memory / chunked (scoped-per-project is inherently bounded; document it).
- Document PyPI view-application ordering + failure-holds behavior (site docs under `site/content/ecosystems/pypi/` and/or the replication/availability reference).

## Likely files

`crates/peryx-search/src/{engine.rs, indexer.rs}`, `crates/peryx-ecosystem-pypi/src/{search_pypi.rs, serving/mod.rs (small), store/mod.rs, store/projects.rs}`, `crates/peryx-driver/src/serving.rs`, `crates/peryx/src/replication.rs`, tests in `crates/peryx-ecosystem-pypi/src/tests/http/frontier.rs`, `crates/peryx-driver/src/tests/state_tests.rs`, `crates/peryx-search/src/tests/*`.

## Existing tests to extend (not duplicate)

`tests/http/frontier.rs` (`test_apply_replicated_changes_retires_only_the_changed_projects`, `test_replica_serves_a_hosted_page_once_the_search_view_catches_up`), `state_tests.rs` (lagging/caught-up search view), `search/tests/frontier_tests.rs`, `driver/jobs/tests.rs` (SearchRebuildJob chunked/cancel/failure).

## Working rules

Canonical `bash /Volumes/OWC/cargo-target/.cargo-serial.sh <cargo cmd>` for all builds/tests/coverage. 100% x86 functions+lines (line-level `DA:,0` gate — avoid `let-else{panic}` in tests; use total helpers). Full pre-commit incl clippy+mdformat before push. One complete PR that Closes #510; route rebases to the coordinator.

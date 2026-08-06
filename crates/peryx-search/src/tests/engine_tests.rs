use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use peryx_core::LexiconRegistry;

use super::Stores;
use crate::context::IndexerCtx;
use crate::engine::{RebuildOutcome, RebuildProgress};
use crate::{PackageDocument, PackageIndexer, PackageSearch, PackageSource, SEARCH_VIEW, SearchError, SearchParams};

/// A test indexer whose document set the test mutates between refreshes, so a rebuild can be shown to
/// publish content a lazy refresh would not yet pick up.
struct NamedDocs(Arc<Mutex<Vec<String>>>);

impl PackageIndexer for NamedDocs {
    fn documents(&self, _ctx: &IndexerCtx<'_>) -> Result<Vec<PackageDocument>, SearchError> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|name| PackageDocument {
                display_name: name.clone(),
                normalized_name: name.clone(),
                route: "root".to_owned(),
                index: "root".to_owned(),
                ecosystem: "pypi".to_owned(),
                source: PackageSource::Cached,
                available_locally: false,
                summary: None,
                text: name.clone(),
            })
            .collect())
    }
}

/// Counts each derive and, when asked, advances the store serial as it derives, so a rebuild's off-lock
/// snapshot is seen to race a concurrent mutation and re-derive under the lock.
struct CountingDocs {
    calls: Arc<AtomicUsize>,
    advance_serial: bool,
    names: Vec<&'static str>,
}

impl PackageIndexer for CountingDocs {
    fn documents(&self, ctx: &IndexerCtx<'_>) -> Result<Vec<PackageDocument>, SearchError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.advance_serial {
            ctx.meta.next_serial().unwrap();
        }
        Ok(self.names.iter().map(|name| pypi_doc(name, name)).collect())
    }
}

fn total(search: &PackageSearch, stores: &Stores, lexicons: &LexiconRegistry) -> usize {
    search
        .search(&stores.ctx(lexicons), SearchParams::default())
        .unwrap()
        .total
}

fn no_cancel(_: RebuildProgress) -> ControlFlow<()> {
    ControlFlow::Continue(())
}

#[test]
fn test_open_rebuilds_when_the_on_disk_schema_changed() {
    let dir = tempfile::tempdir().unwrap();
    // Leave an index a prior peryx built with a different schema.
    let mut legacy = tantivy::schema::Schema::builder();
    legacy.add_text_field("legacy", tantivy::schema::TEXT);
    tantivy::Index::builder()
        .schema(legacy.build())
        .create_in_dir(dir.path())
        .expect("create the legacy index");
    // Opening discards the mismatched index and rebuilds in place instead of failing startup.
    crate::PackageSearch::open(dir.path()).expect("open rebuilds a mismatched index");
}

#[test]
fn test_rebuild_publishes_new_documents_without_an_epoch_bump() {
    let dir = tempfile::tempdir().unwrap();
    let stores = Stores::open(&dir);
    let lexicons = LexiconRegistry::default();
    let names = Arc::new(Mutex::new(vec!["x".to_owned()]));
    let mut search = PackageSearch::in_memory();
    search.add_indexer(Arc::new(NamedDocs(names.clone())));
    assert_eq!(total(&search, &stores, &lexicons), 1);

    // A lazy refresh only reacts to an epoch bump; with none, the eager rebuild is what publishes.
    *names.lock().unwrap() = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
    let outcome = search
        .rebuild(&stores.indexer_ctx(), NonZeroUsize::new(2).unwrap(), &mut no_cancel)
        .unwrap();

    assert_eq!(
        outcome,
        RebuildOutcome::Published {
            documents: 3,
            commits: 2
        }
    );
    assert_eq!(total(&search, &stores, &lexicons), 3);
}

#[test]
fn test_rebuild_to_an_empty_index_commits_once() {
    let dir = tempfile::tempdir().unwrap();
    let stores = Stores::open(&dir);
    let lexicons = LexiconRegistry::default();
    let names = Arc::new(Mutex::new(vec!["a".to_owned()]));
    let mut search = PackageSearch::in_memory();
    search.add_indexer(Arc::new(NamedDocs(names.clone())));
    assert_eq!(total(&search, &stores, &lexicons), 1);

    names.lock().unwrap().clear();
    let outcome = search
        .rebuild(&stores.indexer_ctx(), NonZeroUsize::new(4).unwrap(), &mut no_cancel)
        .unwrap();

    assert_eq!(
        outcome,
        RebuildOutcome::Published {
            documents: 0,
            commits: 1
        }
    );
    assert_eq!(total(&search, &stores, &lexicons), 0);
}

#[test]
fn test_rebuild_cancelled_before_the_first_chunk_keeps_the_served_index() {
    let dir = tempfile::tempdir().unwrap();
    let stores = Stores::open(&dir);
    let lexicons = LexiconRegistry::default();
    let names = Arc::new(Mutex::new(vec!["x".to_owned(), "y".to_owned()]));
    let mut search = PackageSearch::in_memory();
    search.add_indexer(Arc::new(NamedDocs(names.clone())));
    assert_eq!(total(&search, &stores, &lexicons), 2);

    *names.lock().unwrap() = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
    let outcome = search
        .rebuild(&stores.indexer_ctx(), NonZeroUsize::new(1).unwrap(), &mut |_| {
            ControlFlow::Break(())
        })
        .unwrap();

    assert_eq!(outcome, RebuildOutcome::Aborted { documents: 0 });
    assert_eq!(total(&search, &stores, &lexicons), 2);
}

#[test]
fn test_rebuild_cancelled_after_a_chunk_does_not_expose_partial_results() {
    let dir = tempfile::tempdir().unwrap();
    let stores = Stores::open(&dir);
    let lexicons = LexiconRegistry::default();
    let names = Arc::new(Mutex::new(vec!["x".to_owned(), "y".to_owned()]));
    let mut search = PackageSearch::in_memory();
    search.add_indexer(Arc::new(NamedDocs(names.clone())));
    assert_eq!(total(&search, &stores, &lexicons), 2);

    // Commit one chunk, then cancel: the reader must still serve the two prior documents, never the
    // single partially committed one.
    *names.lock().unwrap() = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
    let mut chunks = 0;
    let outcome = search
        .rebuild(&stores.indexer_ctx(), NonZeroUsize::new(1).unwrap(), &mut |_| {
            chunks += 1;
            if chunks > 1 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .unwrap();

    assert_eq!(outcome, RebuildOutcome::Aborted { documents: 1 });
    assert_eq!(total(&search, &stores, &lexicons), 2);
}

#[test]
fn test_on_disk_rebuild_marks_then_clears_the_in_flight_marker() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("search");
    let marker = Path::new(&path).with_extension("rebuilding");
    let stores = Stores::open(&dir);
    let lexicons = LexiconRegistry::default();
    let names = Arc::new(Mutex::new(vec!["a".to_owned(), "b".to_owned()]));
    let mut search = PackageSearch::open(&path).unwrap();
    search.add_indexer(Arc::new(NamedDocs(names)));

    let mut seen_marker = false;
    let outcome = search
        .rebuild(&stores.indexer_ctx(), NonZeroUsize::new(1).unwrap(), &mut |_| {
            seen_marker |= marker.exists();
            ControlFlow::Continue(())
        })
        .unwrap();

    assert_eq!(
        outcome,
        RebuildOutcome::Published {
            documents: 2,
            commits: 2
        }
    );
    assert!(seen_marker, "the marker records an in-flight on-disk rebuild");
    assert!(!marker.exists(), "a published rebuild clears its marker");
    assert_eq!(total(&search, &stores, &lexicons), 2);
}

#[test]
fn test_interrupted_on_disk_rebuild_leaves_a_marker_that_open_discards() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("search");
    let marker = Path::new(&path).with_extension("rebuilding");
    let stores = Stores::open(&dir);
    let names = Arc::new(Mutex::new(vec!["a".to_owned()]));
    let mut search = PackageSearch::open(&path).unwrap();
    search.add_indexer(Arc::new(NamedDocs(names)));

    let outcome = search
        .rebuild(&stores.indexer_ctx(), NonZeroUsize::new(1).unwrap(), &mut |_| {
            ControlFlow::Break(())
        })
        .unwrap();
    assert_eq!(outcome, RebuildOutcome::Aborted { documents: 0 });
    assert!(marker.exists(), "an interrupted rebuild leaves its marker");

    drop(search);
    PackageSearch::open(&path).unwrap();
    assert!(
        !marker.exists(),
        "reopening discards the partial index and clears the marker"
    );
}

#[test]
fn test_search_during_a_rebuild_serves_the_prior_index() {
    let dir = tempfile::tempdir().unwrap();
    let stores = Stores::open(&dir);
    let lexicons = LexiconRegistry::default();
    let names = Arc::new(Mutex::new(vec!["x".to_owned(), "y".to_owned()]));
    let mut search = PackageSearch::in_memory();
    search.add_indexer(Arc::new(NamedDocs(names.clone())));
    assert_eq!(total(&search, &stores, &lexicons), 2);

    // A search issued while the rebuild holds the lock skips the refresh and serves the two prior
    // documents rather than blocking or seeing the half-built three.
    *names.lock().unwrap() = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
    let served = std::cell::Cell::new(None);
    search
        .rebuild(&stores.indexer_ctx(), NonZeroUsize::new(1).unwrap(), &mut |_| {
            if served.get().is_none() {
                served.set(Some(
                    search
                        .search(&stores.ctx(&lexicons), SearchParams::default())
                        .unwrap()
                        .total,
                ));
            }
            ControlFlow::Continue(())
        })
        .unwrap();

    assert_eq!(served.get(), Some(2));
    assert_eq!(total(&search, &stores, &lexicons), 3);
}

#[test]
fn test_rebuild_derives_once_off_lock_when_no_mutation_races() {
    let dir = tempfile::tempdir().unwrap();
    let stores = Stores::open(&dir);
    let lexicons = LexiconRegistry::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut search = PackageSearch::in_memory();
    search.add_indexer(Arc::new(CountingDocs {
        calls: calls.clone(),
        advance_serial: false,
        names: vec!["a", "b"],
    }));
    // A non-empty serial to publish, so the persisted frontier proves the snapshot's serial rather than
    // the empty default an unwritten store shares with it.
    stores.meta.next_serial().unwrap();
    stores.meta.next_serial().unwrap();

    search
        .rebuild(&stores.indexer_ctx(), NonZeroUsize::new(4).unwrap(), &mut no_cancel)
        .unwrap();

    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "the derive ran once, off the writer lock"
    );
    assert_eq!(
        stores.meta.view_frontier(SEARCH_VIEW).unwrap(),
        Some(2),
        "the off-lock snapshot's serial is the one published"
    );
    assert_eq!(total(&search, &stores, &lexicons), 2);
}

#[test]
fn test_rebuild_re_derives_under_lock_when_a_mutation_advances_the_serial() {
    let dir = tempfile::tempdir().unwrap();
    let stores = Stores::open(&dir);
    let lexicons = LexiconRegistry::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut search = PackageSearch::in_memory();
    search.add_indexer(Arc::new(CountingDocs {
        calls: calls.clone(),
        advance_serial: true,
        names: vec!["a", "b", "c"],
    }));

    search
        .rebuild(&stores.indexer_ctx(), NonZeroUsize::new(4).unwrap(), &mut no_cancel)
        .unwrap();

    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "the off-lock snapshot raced a serial bump, so the derive re-ran once under the lock"
    );
    assert_eq!(
        stores.meta.view_frontier(SEARCH_VIEW).unwrap(),
        Some(1),
        "the re-derived snapshot's serial is the one published"
    );
    assert_eq!(total(&search, &stores, &lexicons), 3);
}

fn pypi_doc(name: &str, text: &str) -> PackageDocument {
    PackageDocument {
        display_name: name.to_owned(),
        normalized_name: name.to_owned(),
        route: "root".to_owned(),
        index: "root".to_owned(),
        ecosystem: "pypi".to_owned(),
        source: PackageSource::Cached,
        available_locally: false,
        summary: None,
        text: text.to_owned(),
    }
}

fn hits(search: &PackageSearch, stores: &Stores, lexicons: &LexiconRegistry, query: &str) -> usize {
    search
        .search(
            &stores.ctx(lexicons),
            SearchParams {
                query: query.to_owned(),
                ..SearchParams::default()
            },
        )
        .unwrap()
        .total
}

#[test]
fn test_update_project_replaces_only_the_named_project() {
    let dir = tempfile::tempdir().unwrap();
    let stores = Stores::open(&dir);
    let lexicons = LexiconRegistry::default();
    let mut search = PackageSearch::in_memory();
    search.add_indexer(Arc::new(NamedDocs(Arc::new(Mutex::new(vec![
        "alpha".to_owned(),
        "beta".to_owned(),
    ])))));
    // Build the index once; a scoped update never bumps the epoch, so it alone changes what follows.
    assert_eq!(total(&search, &stores, &lexicons), 2);

    search
        .update_project(
            &[pypi_doc("alpha", "alpha renamed")],
            &crate::project_key("root", "alpha"),
        )
        .unwrap();

    assert_eq!(
        hits(&search, &stores, &lexicons, "renamed"),
        1,
        "alpha reflects its new text"
    );
    assert_eq!(hits(&search, &stores, &lexicons, "beta"), 1, "beta is untouched");
    assert_eq!(total(&search, &stores, &lexicons), 2, "no project was added or dropped");
}

#[test]
fn test_update_project_retires_a_project_given_no_documents() {
    let dir = tempfile::tempdir().unwrap();
    let stores = Stores::open(&dir);
    let lexicons = LexiconRegistry::default();
    let mut search = PackageSearch::in_memory();
    search.add_indexer(Arc::new(NamedDocs(Arc::new(Mutex::new(vec![
        "alpha".to_owned(),
        "beta".to_owned(),
    ])))));
    assert_eq!(total(&search, &stores, &lexicons), 2);

    search
        .update_project(&[], &crate::project_key("root", "alpha"))
        .unwrap();

    assert_eq!(hits(&search, &stores, &lexicons, "alpha"), 0, "alpha was retired");
    assert_eq!(total(&search, &stores, &lexicons), 1, "only beta remains");
}

#[test]
fn test_update_project_is_idempotent_across_a_repeated_apply() {
    let dir = tempfile::tempdir().unwrap();
    let stores = Stores::open(&dir);
    let lexicons = LexiconRegistry::default();
    let mut search = PackageSearch::in_memory();
    search.add_indexer(Arc::new(NamedDocs(Arc::new(Mutex::new(vec!["alpha".to_owned()])))));
    assert_eq!(total(&search, &stores, &lexicons), 1);

    // Re-running the same delete-then-add, as a crash-recovery replay does, reaches the same index.
    for _ in 0..2 {
        search
            .update_project(&[pypi_doc("alpha", "alpha")], &crate::project_key("root", "alpha"))
            .unwrap();
    }

    assert_eq!(
        total(&search, &stores, &lexicons),
        1,
        "the project is present exactly once"
    );
}

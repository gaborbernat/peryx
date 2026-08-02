use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::{Arc, Mutex};

use peryx_core::LexiconRegistry;

use super::Stores;
use crate::context::IndexerCtx;
use crate::engine::{RebuildOutcome, RebuildProgress};
use crate::{PackageDocument, PackageIndexer, PackageSearch, PackageSource, SearchError, SearchParams};

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

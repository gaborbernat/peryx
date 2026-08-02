//! The ecosystem-neutral tantivy index: schema, tokenizers, and query execution.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tantivy::collector::{Count, TopDocs};
use tantivy::directory::MmapDirectory;
use tantivy::query::{AllQuery, BooleanQuery, EmptyQuery, Query, RegexQuery, TermQuery};
use tantivy::schema::document::{TantivyDocument, Value as _};
use tantivy::schema::{FAST, Field, IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{LowerCaser, NgramTokenizer, TextAnalyzer, TokenizerManager};
use tantivy::{Index as TantivyIndex, IndexReader, Order, Term};

use crate::access::{SearchAccess, SearchAccessPattern};
use crate::context::{IndexerCtx, SearchCtx};
use crate::error::SearchError;
use crate::indexer::{CompositeIndexer, PackageDocument, PackageIndexer, default_indexer};
use crate::params::{PackageSource, SearchParams};
use crate::response::{SearchResponse, SearchResult};

const SUBSTRING_TOKENIZER: &str = "peryx_substring";
const MIN_NGRAM: usize = 2;
const MAX_NGRAM: usize = 12;
const RAW_REGEX_BYTES: usize = 32 * 1024;
const WRITER_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const REGEX_SPECIALS: &str = "\\.+*?()|[]{}^$";

pub struct PackageSearch {
    index: TantivyIndex,
    reader: IndexReader,
    fields: SearchFields,
    indexer: Arc<dyn PackageIndexer>,
    epoch: AtomicU64,
    indexed_epoch: Mutex<Option<u64>>,
    rebuild_lock: Mutex<()>,
    /// The on-disk index directory, or `None` for an in-memory index. An eager rebuild uses it to
    /// mark an in-flight rebuild so a restart that interrupts one discards the partial index.
    home: Option<PathBuf>,
}

/// How far an eager [`rebuild`](PackageSearch::rebuild) has progressed, reported once per committed
/// chunk so a caller can surface operator progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildProgress {
    /// Documents committed to the new index so far.
    pub indexed: u64,
    /// Documents the rebuild will commit in total.
    pub total: u64,
}

/// How an eager [`rebuild`](PackageSearch::rebuild) ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebuildOutcome {
    /// The rebuilt index replaced the served one; `documents` were published across `commits` chunks.
    Published { documents: u64, commits: u64 },
    /// The caller cancelled before publication; the served index kept its prior contents. `documents`
    /// counts the chunks committed before the abort, which a restart or the next lazy refresh discards.
    Aborted { documents: u64 },
}

impl PackageSearch {
    /// Build an in-memory package search index.
    ///
    /// # Panics
    /// Panics only if the static schema or tokenizer constants are invalid.
    #[must_use]
    pub fn in_memory() -> Self {
        let (schema, fields) = search_schema();
        Self::from_index(
            TantivyIndex::builder()
                .schema(schema)
                .tokenizers(tokenizers())
                .create_in_ram()
                .expect("search schema and tokenizer constants are valid"),
            fields,
            None,
        )
        .expect("in-memory package search reader opens")
    }

    /// Open or create the on-disk package search index.
    ///
    /// The index is a cache derived from the metadata store, so an index left by an earlier peryx
    /// whose schema no longer matches is discarded and rebuilt rather than failing startup. It
    /// repopulates as pages and tags are served.
    ///
    /// # Errors
    /// Returns an error if the directory cannot be created or read, or Tantivy cannot open the index
    /// for a reason other than a schema change.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SearchError> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;
        if rebuild_marker(path).exists() {
            tracing::warn!(path = %path.display(), "search index rebuild was interrupted; discarding the partial index");
            reset_dir(path)?;
            std::fs::remove_file(rebuild_marker(path))?;
        }
        let (schema, fields) = search_schema();
        let index = match open_index(path, &schema) {
            Err(SearchError::Tantivy(tantivy::TantivyError::SchemaError(_))) => {
                tracing::warn!(path = %path.display(), "search index schema changed; rebuilding it");
                reset_dir(path)?;
                open_index(path, &schema)?
            }
            result => result?,
        };
        Self::from_index(index, fields, Some(path.to_path_buf()))
    }

    fn from_index(index: TantivyIndex, fields: SearchFields, home: Option<PathBuf>) -> Result<Self, SearchError> {
        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()?;
        Ok(Self {
            index,
            reader,
            fields,
            indexer: default_indexer(),
            epoch: AtomicU64::new(0),
            indexed_epoch: Mutex::new(None),
            rebuild_lock: Mutex::new(()),
            home,
        })
    }

    /// Add another ecosystem's indexer, keeping any already installed. A second ecosystem composes its
    /// documents with the first rather than replacing them, so a mixed deployment searches every index.
    pub fn add_indexer(&mut self, indexer: Arc<dyn PackageIndexer>) {
        let current = std::mem::replace(&mut self.indexer, default_indexer());
        self.indexer = Arc::new(CompositeIndexer(vec![current, indexer]));
    }

    /// Call after committing a mutation that changes searchable documents.
    pub fn bump_epoch(&self) {
        self.epoch.fetch_add(1, Ordering::Relaxed);
    }

    /// Rebuild the whole index from authoritative metadata, committing in chunks and publishing the
    /// result atomically.
    ///
    /// Unlike the lazy refresh a search triggers, this is an eager operator recovery path for when the
    /// derived index falls behind: it re-derives every document, adds them to the served index in
    /// `chunk` batches so peak writer memory stays bounded, and reloads the reader only once every
    /// chunk has committed. Concurrent searches keep serving the prior complete index until that final
    /// reload publishes the rebuilt one, so no partial state is ever visible. On disk, a marker records
    /// the in-flight rebuild, so a restart that interrupts one discards the partial index and starts
    /// over rather than serving it.
    ///
    /// `observe` is called before each chunk with the running progress; returning
    /// [`ControlFlow::Break`] cancels the rebuild, leaving the served index untouched.
    ///
    /// # Errors
    /// Returns a search error if the documents cannot be derived, the writer cannot commit, or the
    /// in-flight marker cannot be written.
    ///
    /// # Panics
    /// Panics if the rebuild lock was poisoned by a prior panic while rebuilding.
    pub fn rebuild(
        &self,
        ctx: &IndexerCtx<'_>,
        chunk: NonZeroUsize,
        observe: &mut dyn FnMut(RebuildProgress) -> ControlFlow<()>,
    ) -> Result<RebuildOutcome, SearchError> {
        let _guard = self.rebuild_lock.lock().expect("search rebuild lock");
        let epoch = self.epoch.load(Ordering::Relaxed);
        let documents = self.indexer.documents(ctx)?;
        let total = documents.len() as u64;
        self.mark_rebuilding()?;
        let mut writer = self
            .index
            .writer_with_num_threads::<TantivyDocument>(1, WRITER_MEMORY_BYTES)?;
        writer.delete_all_documents()?;
        let mut indexed = 0_u64;
        let mut commits = 0_u64;
        for slice in documents.chunks(chunk.get()) {
            if observe(RebuildProgress { indexed, total }).is_break() {
                return Ok(RebuildOutcome::Aborted { documents: indexed });
            }
            for package in slice {
                writer.add_document(self.document(package))?;
            }
            writer.commit()?;
            commits += 1;
            indexed += slice.len() as u64;
        }
        if commits == 0 {
            writer.commit()?;
            commits = 1;
        }
        let _ = observe(RebuildProgress { indexed, total });
        self.reader.reload()?;
        self.clear_rebuilding();
        *self.indexed_epoch.lock().expect("search epoch lock") = Some(epoch);
        Ok(RebuildOutcome::Published {
            documents: indexed,
            commits,
        })
    }

    /// Record that an on-disk rebuild is in flight, so an interrupted rebuild is discarded on restart.
    fn mark_rebuilding(&self) -> Result<(), SearchError> {
        if let Some(home) = &self.home {
            std::fs::write(rebuild_marker(home), [])?;
        }
        Ok(())
    }

    /// Clear the in-flight marker after a rebuild publishes. Removal is best-effort: a marker left
    /// behind only makes the next restart rebuild an already-complete index, never serve a partial one.
    fn clear_rebuilding(&self) {
        if let Some(home) = &self.home {
            let _ = std::fs::remove_file(rebuild_marker(home));
        }
    }

    /// Search cached package documents.
    ///
    /// # Errors
    /// Returns an error if the derived index cannot refresh or the query is invalid.
    pub fn search(&self, ctx: &SearchCtx<'_>, params: SearchParams) -> Result<SearchResponse, SearchError> {
        self.search_with_access(ctx, params, None)
    }

    /// Apply access inside the query so totals and pages contain only readable resources.
    ///
    /// # Errors
    /// Returns an error if the derived index cannot refresh or the query is invalid.
    pub fn search_authorized(
        &self,
        ctx: &SearchCtx<'_>,
        params: SearchParams,
        access: &SearchAccess,
    ) -> Result<SearchResponse, SearchError> {
        self.search_with_access(ctx, params, Some(access))
    }

    fn search_with_access(
        &self,
        ctx: &SearchCtx<'_>,
        params: SearchParams,
        access: Option<&SearchAccess>,
    ) -> Result<SearchResponse, SearchError> {
        self.ensure_current(ctx)?;
        let query = self.query(&params, access)?;
        let searcher = self.reader.searcher();
        let offset = params.offset();
        let top_docs = TopDocs::with_limit(params.page_size)
            .and_offset(offset)
            .order_by_string_fast_field("sort", Order::Asc);
        let total = searcher.search(&*query, &Count)?;
        let results = searcher
            .search(&*query, &top_docs)?
            .into_iter()
            .map(|(_sort, address)| {
                searcher.doc::<TantivyDocument>(address).map(|doc| {
                    let mut result = self.result_from_doc(&doc);
                    let ecosystem = result.ecosystem.parse().unwrap_or_default();
                    ctx.lexicon(ecosystem).search_noun.clone_into(&mut result.type_label);
                    result
                })
            })
            .collect::<tantivy::Result<Vec<_>>>()?;
        Ok(SearchResponse {
            query: params.query,
            route: params.route,
            source_type: params.source,
            page: params.page,
            page_size: params.page_size,
            total,
            results,
        })
    }

    fn ensure_current(&self, ctx: &SearchCtx<'_>) -> Result<(), SearchError> {
        // A held lock means an eager rebuild or another refresh is running; serve the current reader
        // rather than block or race a second writer. An eager rebuild leaves the reader on the prior
        // complete index until it publishes, so this serves complete results, never partial ones.
        let Ok(_guard) = self.rebuild_lock.try_lock() else {
            return Ok(());
        };
        let epoch = self.epoch.load(Ordering::Relaxed);
        if self
            .indexed_epoch
            .lock()
            .expect("search epoch lock")
            .is_none_or(|indexed| indexed != epoch)
        {
            self.write(&self.indexer.documents(&ctx.indexer)?)?;
            *self.indexed_epoch.lock().expect("search epoch lock") = Some(epoch);
        }
        Ok(())
    }

    /// Replace the whole index with `documents`, then make them searchable.
    fn write(&self, documents: &[PackageDocument]) -> Result<(), SearchError> {
        let mut writer = self
            .index
            .writer_with_num_threads::<TantivyDocument>(1, WRITER_MEMORY_BYTES)?;
        writer.delete_all_documents()?;
        for package in documents {
            writer.add_document(self.document(package))?;
        }
        writer.commit()?;
        self.reader.reload()?;
        Ok(())
    }

    fn query(&self, params: &SearchParams, access: Option<&SearchAccess>) -> Result<Box<dyn Query>, SearchError> {
        let mut queries = vec![self.text_query(params.query.trim())?];
        if let Some(source) = params.source.package_source() {
            queries.push(Box::new(TermQuery::new(
                Term::from_field_text(self.fields.source, source.as_str()),
                IndexRecordOption::Basic,
            )));
        }
        if let Some(route) = &params.route {
            queries.push(Box::new(TermQuery::new(
                Term::from_field_text(self.fields.route, route),
                IndexRecordOption::Basic,
            )));
        }
        if let Some(access) = access {
            queries.push(self.access_query(access)?);
        }
        Ok(if queries.len() == 1 {
            queries.pop().expect("query exists")
        } else {
            Box::new(BooleanQuery::intersection(queries))
        })
    }

    fn access_query(&self, access: &SearchAccess) -> Result<Box<dyn Query>, SearchError> {
        let mut queries = access
            .patterns
            .iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|SearchAccessPattern { route, glob }| {
                let route_query = Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.route, route),
                    IndexRecordOption::Basic,
                )) as Box<dyn Query>;
                RegexQuery::from_pattern(&glob_regex(glob), self.fields.normalized).map(|project_query| {
                    Box::new(BooleanQuery::intersection(vec![route_query, Box::new(project_query)])) as Box<dyn Query>
                })
            })
            .collect::<tantivy::Result<Vec<Box<dyn Query>>>>()?;
        Ok(match queries.len() {
            0 => Box::new(EmptyQuery),
            1 => queries.pop().expect("query exists"),
            _ => Box::new(BooleanQuery::union(queries)),
        })
    }

    fn text_query(&self, query: &str) -> Result<Box<dyn Query>, SearchError> {
        if query.is_empty() {
            return Ok(Box::new(AllQuery));
        }
        if let Some(pattern) = query.strip_prefix("re:") {
            if pattern.is_empty() {
                return Ok(Box::new(AllQuery));
            }
            return Ok(Box::new(RegexQuery::from_pattern(
                &format!(".*{}.*", fold_lowercase(pattern)),
                self.fields.raw,
            )?));
        }
        let query = fold_lowercase(query);
        let terms = query_terms(&query);
        if terms.is_empty() {
            let pattern = format!(".*{}.*", escape_regex(&query));
            return Ok(Box::new(RegexQuery::from_pattern(&pattern, self.fields.raw)?));
        }
        let queries = terms
            .into_iter()
            .map(|term| {
                Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.search, &term),
                    IndexRecordOption::Basic,
                )) as Box<dyn Query>
            })
            .collect();
        Ok(Box::new(BooleanQuery::intersection(queries)))
    }

    fn result_from_doc(&self, doc: &TantivyDocument) -> SearchResult {
        SearchResult {
            display_name: stored_text(doc, self.fields.display),
            normalized_name: stored_text(doc, self.fields.normalized),
            route: stored_text(doc, self.fields.route),
            index: stored_text(doc, self.fields.index),
            ecosystem: stored_text(doc, self.fields.ecosystem),
            type_label: String::new(),
            source_type: PackageSource::from_value(&stored_text(doc, self.fields.source))
                .unwrap_or(PackageSource::Cached),
            summary: non_empty_string(stored_text(doc, self.fields.summary)),
        }
    }

    fn document(&self, package: &PackageDocument) -> TantivyDocument {
        let sort = format!(
            "{}\u{0}{}\u{0}{}",
            package.display_name.to_ascii_lowercase(),
            package.route,
            package.normalized_name
        );
        let mut doc = TantivyDocument::new();
        doc.add_text(self.fields.route, &package.route);
        doc.add_text(self.fields.normalized, &package.normalized_name);
        doc.add_text(self.fields.display, &package.display_name);
        doc.add_text(self.fields.source, package.source.as_str());
        doc.add_text(self.fields.index, &package.index);
        doc.add_text(self.fields.ecosystem, &package.ecosystem);
        doc.add_text(self.fields.summary, package.summary.as_deref().unwrap_or_default());
        doc.add_text(self.fields.sort, sort);
        doc.add_text(self.fields.search, &package.text);
        doc.add_text(
            self.fields.raw,
            truncate_to_chars(&fold_lowercase(&package.text), RAW_REGEX_BYTES),
        );
        doc
    }
}

#[derive(Clone, Copy)]
struct SearchFields {
    route: Field,
    normalized: Field,
    display: Field,
    source: Field,
    index: Field,
    ecosystem: Field,
    summary: Field,
    sort: Field,
    search: Field,
    raw: Field,
}

fn open_index(path: &Path, schema: &Schema) -> Result<TantivyIndex, SearchError> {
    Ok(TantivyIndex::builder()
        .schema(schema.clone())
        .tokenizers(tokenizers())
        .open_or_create(MmapDirectory::open(path)?)?)
}

/// Discard the on-disk index so a fresh one builds in its place. Drops the directory with whatever it
/// holds, then recreates it empty.
fn reset_dir(path: &Path) -> std::io::Result<()> {
    std::fs::remove_dir_all(path)?;
    std::fs::create_dir_all(path)
}

/// The sibling file that marks an in-flight rebuild of the index at `path`. It sits beside the index
/// directory rather than inside it so tantivy's own file management never touches it.
fn rebuild_marker(path: &Path) -> PathBuf {
    path.with_extension("rebuilding")
}

fn search_schema() -> (Schema, SearchFields) {
    let mut builder = Schema::builder();
    let stored = TextOptions::default().set_stored();
    let exact = STRING | STORED;
    let sort = STRING | FAST | STORED;
    let search = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(SUBSTRING_TOKENIZER)
            .set_index_option(IndexRecordOption::Basic)
            .set_fieldnorms(false),
    );
    let raw = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_index_option(IndexRecordOption::Basic)
            .set_fieldnorms(false),
    );
    let fields = SearchFields {
        route: builder.add_text_field("route", exact.clone()),
        normalized: builder.add_text_field("normalized", exact.clone()),
        display: builder.add_text_field("display", stored.clone()),
        source: builder.add_text_field("source", exact),
        index: builder.add_text_field("index", stored.clone()),
        ecosystem: builder.add_text_field("ecosystem", stored.clone()),
        summary: builder.add_text_field("summary", stored),
        sort: builder.add_text_field("sort", sort),
        search: builder.add_text_field("search", search),
        raw: builder.add_text_field("raw", raw),
    };
    (builder.build(), fields)
}

fn tokenizers() -> TokenizerManager {
    let manager = TokenizerManager::default();
    let tokenizer = TextAnalyzer::builder(
        NgramTokenizer::all_ngrams(MIN_NGRAM, MAX_NGRAM).expect("ngram tokenizer constants are valid"),
    )
    .filter(LowerCaser)
    .build();
    manager.register(SUBSTRING_TOKENIZER, tokenizer);
    manager
}

/// Fold to lowercase the way the substring index does, so an accented or non-Latin query matches the
/// text it indexed. Tantivy's `LowerCaser` maps each character through `char::to_lowercase`; matching
/// it needs the same per-character fold, not `to_ascii_lowercase` (which leaves non-ASCII letters
/// uppercase) nor `str::to_lowercase` (which special-cases final sigma).
fn fold_lowercase(value: &str) -> String {
    value.chars().flat_map(char::to_lowercase).collect()
}

fn query_terms(query: &str) -> Vec<String> {
    let chars: Vec<char> = query.chars().collect();
    match chars.len() {
        0 | 1 => Vec::new(),
        len if len <= MAX_NGRAM => vec![query.to_owned()],
        len => (0..=len - MAX_NGRAM)
            .map(|start| chars[start..start + MAX_NGRAM].iter().collect::<String>())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }
}

fn stored_text(doc: &TantivyDocument, field: Field) -> String {
    doc.get_first(field)
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_owned()
}

fn non_empty_string(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

#[must_use]
pub fn truncate_to_chars(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn escape_regex(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    push_escaped_regex(&mut escaped, value);
    escaped
}

fn glob_regex(value: &str) -> String {
    let mut pattern = String::with_capacity(value.len());
    let mut parts = value.split('*');
    push_escaped_regex(&mut pattern, parts.next().unwrap_or_default());
    for part in parts {
        pattern.push_str(".*");
        push_escaped_regex(&mut pattern, part);
    }
    pattern
}

fn push_escaped_regex(pattern: &mut String, value: &str) {
    for char in value.chars() {
        if REGEX_SPECIALS.contains(char) {
            pattern.push('\\');
        }
        pattern.push(char);
    }
}

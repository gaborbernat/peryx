//! The `AppState` cache accessors, delegating to the role engine's serving cache with the process
//! clock supplied.

use bytes::Bytes;
use peryx_storage::meta::MetaError;

use super::app::ServingState;
use super::derived_views::{
    REQUIRED_VIEWS, ReadableFrontier, SEARCH_VIEW, readable_frontier as compute_readable_frontier,
};

impl ServingState {
    /// A hot-cache entry that is still within its freshness window; expired entries miss.
    #[must_use]
    pub fn hot_fresh(&self, key: &str) -> Option<Bytes> {
        self.cache.hot_fresh(key, (self.clock)())
    }

    /// A fresh hot-cache entry and the source revision attached by its driver.
    #[must_use]
    pub fn hot_fresh_versioned(&self, key: &str) -> Option<(Bytes, Option<u64>)> {
        self.cache.hot_fresh_versioned(key, (self.clock)())
    }

    /// The hot-cache key for one representation of a page as served on `route` right now.
    #[must_use]
    pub fn hot_key(&self, route: &str, project: &str, variant: &str) -> String {
        self.cache.hot_key(route, project, variant)
    }

    /// Whether a remembered upstream miss is still inside its injected-clock expiry.
    #[must_use]
    pub fn negative_fresh(&self, key: &str) -> bool {
        self.cache.negative_fresh(key, (self.clock)())
    }

    /// Remember an upstream miss for `ttl_secs` according to the injected clock.
    pub fn remember_negative(&self, key: String, ttl_secs: i64) {
        self.cache.remember_negative(key, (self.clock)() + ttl_secs);
    }

    /// Invalidate one project's rendered pages and the search documents after a `PyPI` mutation.
    pub fn invalidate_project(&self, project: &str) {
        self.cache.invalidate_hot(project);
        self.bump_search_epoch();
    }

    /// Preserve rendered page caches across `OCI` tag mutations.
    pub fn bump_search_epoch(&self) {
        self.search.bump_epoch();
    }

    /// Persist the durable frontier a required derived `view` has applied, reporting the resulting
    /// frontier, which never moves backward.
    ///
    /// A replica's apply path calls it once a view has rebuilt to `serial`, so the readable frontier
    /// advances only behind proven, crash-safe view state — never after merely enqueueing the work.
    ///
    /// # Errors
    /// Returns a store error if the write fails.
    pub fn advance_view_frontier(&self, view: &str, serial: u64) -> Result<u64, MetaError> {
        self.meta.set_view_frontier(view, serial)
    }

    /// Persist the search index's applied frontier from the live engine, so a restart proves how far
    /// the on-disk index reflects the store. Returns the durable frontier.
    ///
    /// # Errors
    /// Returns a store error if the write fails.
    pub fn persist_search_frontier(&self) -> Result<u64, MetaError> {
        self.advance_view_frontier(SEARCH_VIEW, self.search.indexed_frontier().unwrap_or(0))
    }

    /// The highest metadata serial a replica may expose and the view holding it back, from the
    /// authoritative serial and every required view's durable frontier. Metadata above it stays
    /// hidden until the lagging view catches up, so a read never mixes new metadata with a stale view.
    ///
    /// # Errors
    /// Returns a store error if either read fails.
    pub fn readable_frontier(&self) -> Result<ReadableFrontier, MetaError> {
        let authority = self.meta.current_serial()?;
        let frontiers = self.meta.view_frontiers()?;
        Ok(compute_readable_frontier(authority, &frontiers, REQUIRED_VIEWS))
    }
}

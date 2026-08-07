use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UiSearchPage {
    pub query: String,
    pub source_type: String,
    pub availability: String,
    pub page: usize,
    pub page_size: usize,
    pub total: usize,
    pub results: Vec<UiSearchResult>,
}

/// The `/+search` success body, whose required fields reject an absent or wrong-typed value so a
/// schema mismatch surfaces as an error rather than an empty index. Mirrors the server's
/// `SearchResponse`; only fields the API marks optional carry a serde default.
#[derive(Deserialize)]
struct WireSearchPage {
    query: String,
    #[serde(rename = "type")]
    source_type: String,
    availability: String,
    page: usize,
    page_size: usize,
    total: usize,
    results: Vec<WireSearchResult>,
}

#[derive(Deserialize)]
struct WireSearchResult {
    display_name: String,
    normalized_name: String,
    route: String,
    index: String,
    ecosystem: String,
    type_label: String,
    #[serde(rename = "type")]
    source_type: String,
    available: bool,
    summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiSearchResult {
    pub display_name: String,
    pub normalized_name: String,
    pub route: String,
    pub index: String,
    pub ecosystem: String,
    /// This ecosystem's word for the result (`package`, `image`), filled server-side from the lexicon.
    pub type_label: String,
    pub source_type: String,
    /// Whether this package's bytes can be served from local storage right now.
    pub available: bool,
    pub summary: Option<String>,
}

impl UiSearchPage {
    /// Build a search page from a successful `/+search` response body.
    ///
    /// # Errors
    /// Returns a user-visible message when the body does not match the search wire contract, so a
    /// server/client schema mismatch is reported instead of rendering as an empty index.
    pub fn from_search(value: &serde_json::Value) -> Result<Self, String> {
        let wire = WireSearchPage::deserialize(value).map_err(|err| format!("malformed search response: {err}"))?;
        Ok(Self {
            query: wire.query,
            source_type: wire.source_type,
            availability: wire.availability,
            page: wire.page,
            page_size: wire.page_size,
            total: wire.total,
            results: wire
                .results
                .into_iter()
                .map(|result| UiSearchResult {
                    display_name: result.display_name,
                    normalized_name: result.normalized_name,
                    route: result.route,
                    index: result.index,
                    ecosystem: result.ecosystem,
                    type_label: result.type_label,
                    source_type: result.source_type,
                    available: result.available,
                    summary: result.summary,
                })
                .collect(),
        })
    }

    /// The 1-based inclusive `(start, end)` row interval this page shows in its summary, or `None`
    /// when the page holds no rows. A page requested past the last result carries a nonzero total
    /// yet an empty vector, and its start would otherwise run beyond both the end and the total.
    #[must_use]
    pub fn shown_range(&self) -> Option<(usize, usize)> {
        let last = self.results.len().checked_sub(1)?;
        let start = self.page.saturating_sub(1).saturating_mul(self.page_size) + 1;
        Some((start, self.total.min(start + last)))
    }
}

impl UiSearchResult {
    #[must_use]
    pub fn source_label(&self) -> &'static str {
        source_label(&self.source_type)
    }
}

#[must_use]
pub fn source_label(source_type: &str) -> &'static str {
    match source_type {
        "uploaded" => "Uploaded",
        "override" => "Override",
        _ => "Cached",
    }
}

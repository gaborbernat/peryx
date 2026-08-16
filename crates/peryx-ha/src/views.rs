use peryx_core::{BlobPlacementView, OperationsView, PlacementView};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailabilityPageQuery {
    pub cursor: Option<String>,
    pub limit: usize,
    pub include_rows: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PlacementViewError {
    #[error("placement limit out of range")]
    InvalidLimit,
    #[error("placement health unavailable")]
    HealthRead,
    #[error("placement rows unavailable")]
    RowsRead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BlobPlacementViewError {
    #[error("invalid artifact digest")]
    InvalidDigest,
    #[error("blob placement unavailable")]
    Read,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum OperationsViewError {
    #[error("operation limit out of range")]
    InvalidLimit,
    #[error("operation health unavailable")]
    HealthRead,
    #[error("operation rows unavailable")]
    RowsRead,
}

pub trait AvailabilityViewReader: Send + Sync {
    /// # Errors
    ///
    /// Returns an error when the query is invalid or placement data cannot be read.
    fn placement_view(&self, query: AvailabilityPageQuery) -> Result<PlacementView, PlacementViewError>;

    /// # Errors
    ///
    /// Returns an error when the digest is invalid or placement data cannot be read.
    fn blob_placement_view(&self, digest: &str) -> Result<BlobPlacementView, BlobPlacementViewError>;

    /// # Errors
    ///
    /// Returns an error when the query is invalid or operation data cannot be read.
    fn operations_view(&self, query: AvailabilityPageQuery) -> Result<OperationsView, OperationsViewError>;
}

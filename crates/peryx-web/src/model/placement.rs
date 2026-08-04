//! The artifact placement-health view models, shared by the server renderer and the hydrated client.
//!
//! The neutral [`PlacementView`] crosses the server/browser boundary unchanged; the source and byte
//! availability chips reuse the package page's [`file_source_label`](super::file_source_label) and
//! [`byte_availability_label`](super::byte_availability_label), so a placement row and a file row read
//! the same word for the same state.

pub use peryx_core::{PlacementHealth, PlacementRow, PlacementView};

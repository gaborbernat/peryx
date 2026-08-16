use peryx_ha::{BlobPlacementViewError, PlacementViewError};

use super::{blob_placement_error, placement_error};

#[test]
fn placement_errors_are_user_facing() {
    for (error, message) in [
        (PlacementViewError::InvalidLimit, "The placement page limit is invalid."),
        (PlacementViewError::HealthRead, "Placement health could not be read."),
        (PlacementViewError::RowsRead, "Placement rows could not be read."),
    ] {
        assert_eq!(placement_error(error), message);
    }
}

#[test]
fn blob_placement_errors_are_user_facing() {
    for (error, message) in [
        (
            BlobPlacementViewError::InvalidDigest,
            "That is not a valid artifact digest.",
        ),
        (BlobPlacementViewError::Read, "Blob placement could not be read."),
    ] {
        assert_eq!(blob_placement_error(error), message);
    }
}

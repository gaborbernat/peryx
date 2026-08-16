use peryx_ha::OperationsViewError;

use super::operation_error;

#[test]
fn operation_errors_are_user_facing() {
    for (error, message) in [
        (
            OperationsViewError::InvalidLimit,
            "The operation page limit is invalid.",
        ),
        (OperationsViewError::HealthRead, "Operation health could not be read."),
        (OperationsViewError::RowsRead, "Operation rows could not be read."),
    ] {
        assert_eq!(operation_error(error), message);
    }
}

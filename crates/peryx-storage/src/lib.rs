mod artifact_repair;
mod availability;
pub mod blob;
pub mod meta;

pub use artifact_repair::{
    ArtifactRepairDirectionReport, ArtifactRepairError, ArtifactRepairFailure, ArtifactRepairReport,
    repair_artifact_placements, repair_artifact_placements_cancellable,
};

#[cfg(test)]
#[path = "../tests/unit/artifact_repair_tests.rs"]
mod artifact_repair_tests;

#[cfg(test)]
#[path = "../tests/unit/tests/mod.rs"]
mod tests;

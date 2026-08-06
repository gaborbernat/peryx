//! The availability control plane a `dc` or `ha` node exposes on a dedicated, authenticated listener.

mod dc_copy;
mod listener;
mod placement_reconcile;
mod transfer;

pub use dc_copy::CrossDcBlobCopier;
pub use listener::{AVAILABILITY_PROTOCOL_VERSION, AvailabilityPosture, router};
pub use placement_reconcile::FilesystemPlacementReconciler;
pub use transfer::{EpochOracle, FrontierSource, TransferDriveError, commit_transfer, observe_target};

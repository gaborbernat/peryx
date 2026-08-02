//! The availability control plane a `dc` or `ha` node exposes on a dedicated, authenticated listener.

mod listener;

pub use listener::{AVAILABILITY_PROTOCOL_VERSION, AvailabilityPosture, router};

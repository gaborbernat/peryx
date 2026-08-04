//! The Raft consensus persistence for peryx's replication runtime.
//!
//! This holds the durable, type-independent core of the `openraft` log store. The thin adapter that
//! implements `openraft`'s `RaftLogStorage` over these bytes lands with the `RaftTypeConfig` contract.

mod log_store;

pub use log_store::{RaftLogError, RaftLogStore, StoredEntry, StoredSnapshot};

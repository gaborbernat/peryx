//! The `OpenRaft` `RaftNetwork` adapter for the ownership consensus group: sends append-entries, vote,
//! and install-snapshot RPCs to peer voters.
//!
//! The network lane fills this module with the `RaftNetwork<TypeConfig>` implementation and its client
//! and factory, dialing each peer at its [`PeryxNode`](crate::raft::PeryxNode) address. The foundation
//! declares the module so the `raft` tree is owned in one place and the adapter only adds its
//! implementation here.

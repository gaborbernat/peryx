//! The `OpenRaft` `RaftLogStorage` adapter for the ownership consensus group: persists the Raft log
//! entries, the vote, and the snapshot in durable storage.
//!
//! The storage lane fills this module with the `RaftLogStorage<TypeConfig>` implementation over
//! `peryx-storage`'s log store, keying serialized `Entry` blobs by log index. The foundation declares
//! the module so the `raft` tree is owned in one place and the adapter only adds its implementation
//! here.

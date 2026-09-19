//! Content-addressed blob storage with atomic publication at `sha256/ab/cd/<digest>`.

use std::path::Path;

mod backend;
mod chunked;
mod durability;
mod error;
mod range;
mod s3;
mod stage;
mod storage;
mod store;

pub use backend::{
    BlobBackend, BlobCapabilities, BlobLease, BlobRead, BlobReadBody, BlobStaged, BlobSupport, BlobTail, BlobWrite,
};
pub use chunked::{CHUNK_BYTES, ChunkedDigest, ChunkedDigestBuilder};
pub use durability::{DurabilityCapabilities, DurabilityShortfall, PlacementReceipt, Publication};
pub use error::{BlobError, BlobErrorContext, BlobErrorKind, BlobOperation, BlobScanError};
pub use peryx_core::{BlobDurability, BlobMetadata, Digest, DurabilityRequirement, WriteEvidence};
pub use range::{RangeRequest, parse_range};
pub use s3::{S3Addressing, S3Backend, S3Client, S3Config, S3ConfigError, S3Error, S3Settings};
pub use stage::StageUsage;
pub use storage::{BlobBlocking, BlobStorage};
pub use store::{BlobDigestPage, BlobEntry, BlobStore, PendingBlob, StagedBlob};

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Sync a directory tree from its leaves to its root.
///
/// # Errors
/// Returns the failure to read, open, or flush a directory in the tree.
pub fn sync_tree(path: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(path)? {
        let child = entry?.path();
        if child.is_dir() {
            sync_tree(&child)?;
        }
    }
    sync_dir(path)
}

/// Sync the directory that holds an entry.
///
/// # Errors
/// Returns the failure to open or flush the parent directory, or [`std::io::ErrorKind::InvalidInput`]
/// when `path` names no entry in a directory.
pub fn sync_parent(path: &Path) -> std::io::Result<()> {
    match path.parent() {
        // A bare file name is an entry in the working directory, which is what has to be flushed.
        Some(parent) if parent.as_os_str().is_empty() => sync_dir(Path::new(".")),
        Some(parent) => sync_dir(parent),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} has no parent directory to flush", path.display()),
        )),
    }
}

/// Every directory `create_dir_all` creates is itself an unflushed entry in its own parent, so a fresh
/// fan-out only becomes durable once each new level is flushed from the leaf toward the first level that
/// already existed.
///
/// # Errors
/// Returns the first creation, open, or flush failure.
fn create_dir_durable(dir: &Path) -> std::io::Result<()> {
    let missing = dir
        .ancestors()
        .take_while(|level| !level.as_os_str().is_empty() && !level.exists())
        .count();
    std::fs::create_dir_all(dir)?;
    dir.ancestors().take(missing).try_for_each(sync_parent)
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

/// Windows exposes no directory flush — `FlushFileBuffers` rejects a handle opened with backup semantics
/// — and NTFS instead orders the rename in the metadata log its recovery pass replays.
#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/blob/sweep_tests.rs"]
mod sweep_tests;

#[cfg(test)]
#[path = "../../tests/unit/blob/sync_tests.rs"]
mod sync_tests;

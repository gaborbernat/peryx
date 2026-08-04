//! Drive a multi-source ranged fetch of one blob, the MODE-B counterpart to
//! [`fetch_missing`](crate::blob_fetch::fetch_missing).
//!
//! Where [`fetch_missing`](crate::blob_fetch::fetch_missing) drives one transport over the whole missing
//! set as whole-blob fetches, this pulls ONE blob as byte ranges across an ordered list of sources. For
//! each range it tries the sources in order, falling to the next on any loss so one stale or wrong
//! `Verified` peer cannot block a fetch another peer can serve, adapts each fetched range into a
//! [`BlobPiece`](crate::blob_reassembly::BlobPiece) through [`blob_piece`](crate::blob_piece::blob_piece),
//! and reassembles the whole under [`reassemble_verified`](crate::blob_reassembly::reassemble_verified).
//! The whole-blob digest check is the safety net that makes falling through safe. It commits nothing: the
//! caller commits the returned bytes, as it does for [`fetch_missing`](crate::blob_fetch::fetch_missing).

use bytes::Bytes;
use peryx_storage::blob::Digest;

use crate::blob::{BlobRequest, BlobTransport, ByteRange};
use crate::blob_piece::{PieceError, blob_piece};
use crate::blob_reassembly::{ReassemblyError, reassemble_verified};
use crate::peer::TransportError;

/// Why a multi-source ranged blob pull could not produce the verified whole.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullError {
    /// Every source failed a range. `failures` carries each source's index and its error in source
    /// order, so a caller can see which `Verified` peers failed and how without the pull having blocked
    /// on any one of them.
    Exhausted {
        range: ByteRange,
        failures: Vec<(usize, TransportError)>,
    },
    /// A fetched body was not the length its range declared.
    Piece(PieceError),
    /// The reassembled whole blob failed a structural check or digest verification.
    Reassembly(ReassemblyError),
}

/// Pull `digest` as `ranges` across `sources` in order and return the verified whole blob.
///
/// Each range is fetched from the first source that answers, falling to the next on any
/// [`TransportError`]; a stale placement plan or a wrong `Verified` peer does not block the fetch when
/// another source has the blob. Running out of sources for a range is a [`PullError::Exhausted`] carrying
/// each source's failure. Each fetched range becomes a [`BlobPiece`](crate::blob_reassembly::BlobPiece)
/// through [`blob_piece`], which fails closed when a source returns the wrong number of bytes, and
/// [`reassemble_verified`] tiles the pieces and digest-verifies the whole against `expected` — the check
/// that keeps falling through safe. Nothing is committed; the caller commits the returned bytes.
///
/// # Errors
/// [`PullError`] when every source is exhausted for a range, a fetched range is the wrong length, or the
/// reassembled blob does not verify against `expected`.
pub async fn pull_ranged<T: BlobTransport>(
    sources: &[&T],
    digest: &Digest,
    ranges: &[ByteRange],
    total_length: usize,
    expected: &Digest,
) -> Result<Bytes, PullError> {
    let mut pieces = Vec::with_capacity(ranges.len());
    for &range in ranges {
        let bytes = fetch_range(sources, digest, range).await?;
        pieces.push(blob_piece(range.offset as u64, range.length as u64, bytes).map_err(PullError::Piece)?);
    }
    reassemble_verified(&pieces, total_length, expected).map_err(PullError::Reassembly)
}

/// The byte span one ranged request covers, so a large blob is drawn in bounded pieces rather than a
/// single request that must buffer the whole body before it can be checked.
const RANGED_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// Pull the whole blob `digest` of `total_length` bytes across `sources` as fixed-size ranges and return
/// the verified whole.
///
/// This is the per-blob driver: it tiles `[0, total_length)` into [`RANGED_CHUNK_BYTES`] ranges and drives
/// [`pull_ranged`] over `sources`, so each range falls through to the next source on a loss and the whole
/// is digest-verified against `digest` before any byte is returned. `total_length` bounds the reassembly
/// pre-allocation, so the caller MUST pass the size from its own trusted metadata rather than a peer
/// advertisement. Nothing is committed; the caller commits the returned bytes.
///
/// # Errors
/// [`PullError`] when every source is exhausted for a range, a fetched range is the wrong length, or the
/// reassembled blob does not verify against `digest`.
pub async fn pull_ranged_blob<T: BlobTransport>(
    sources: &[&T],
    digest: &Digest,
    total_length: usize,
) -> Result<Bytes, PullError> {
    let ranges = chunk_ranges(total_length, RANGED_CHUNK_BYTES);
    pull_ranged(sources, digest, &ranges, total_length, digest).await
}

/// Tile `[0, total_length)` into consecutive `chunk`-byte ranges, the last one short. A zero-length blob
/// yields no ranges, which [`reassemble_verified`] accepts as the empty blob.
pub fn chunk_ranges(total_length: usize, chunk: usize) -> Vec<ByteRange> {
    let mut ranges = Vec::with_capacity(total_length.div_ceil(chunk.max(1)));
    let mut offset = 0;
    while offset < total_length {
        let length = chunk.min(total_length - offset);
        ranges.push(ByteRange { offset, length });
        offset += length;
    }
    ranges
}

async fn fetch_range<T: BlobTransport>(
    sources: &[&T],
    digest: &Digest,
    range: ByteRange,
) -> Result<Vec<u8>, PullError> {
    let mut failures = Vec::new();
    for (index, source) in sources.iter().enumerate() {
        match source
            .fetch_blob(BlobRequest {
                digest: digest.clone(),
                range: Some(range),
            })
            .await
        {
            Ok(bytes) => return Ok(bytes),
            Err(error) => failures.push((index, error)),
        }
    }
    Err(PullError::Exhausted { range, failures })
}

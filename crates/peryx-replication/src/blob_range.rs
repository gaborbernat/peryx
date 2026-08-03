//! Parse an HTTP `Range` header for the blob-serve endpoint, the server counterpart to the ranged
//! [`BlobTransport`](crate::blob) fetch.
//!
//! [`parse_range`] maps a request's `Range` value and the blob's total size to one of three answers a
//! handler acts on: serve the whole blob, serve one byte range, or reject as unsatisfiable. It follows
//! [RFC 7233](https://www.rfc-editor.org/rfc/rfc7233): an absent header, an unknown range unit, a
//! multi-range set, or a malformed spec is ignored and the whole blob is served, because a server that
//! cannot honor a `Range` answers `200` with the full representation rather than failing. Only a
//! well-formed single `bytes=` range that names bytes outside the blob is unsatisfiable, the `416`
//! signal.

use std::ops::Range;

/// What a handler should serve for a request's `Range` header against a blob of a known size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeRequest {
    /// Serve the whole blob (`200`): no `Range`, an unrecognized one, or one to ignore per RFC 7233.
    Whole,
    /// Serve this end-exclusive byte range (`206`).
    Partial(Range<u64>),
    /// The `Range` is well formed but names bytes the blob does not have (`416`).
    Unsatisfiable,
}

/// Resolve a request's `Range` header against a blob of `total` bytes.
///
/// An absent header, a non-`bytes=` unit, a comma-joined multi-range, or a syntactically malformed spec
/// all resolve to [`RangeRequest::Whole`]: RFC 7233 requires a server to ignore a `Range` it cannot
/// apply and serve the full representation. A single `bytes=` range resolves to [`RangeRequest::Partial`]
/// with its end clamped to the blob, or to [`RangeRequest::Unsatisfiable`] when the first byte is at or
/// past the end, or the last byte precedes the first. A suffix longer than the blob names the whole
/// blob, since RFC 7233 uses the entire representation when it is shorter than the requested suffix.
#[must_use]
pub fn parse_range(header: Option<&str>, total: u64) -> RangeRequest {
    let Some(spec) = header.and_then(|value| value.strip_prefix("bytes=")) else {
        return RangeRequest::Whole;
    };
    let spec = spec.trim();
    let Some((start, end)) = spec.split_once('-').filter(|_| !spec.contains(',')) else {
        return RangeRequest::Whole;
    };
    match (start.trim(), end.trim()) {
        ("", "") => RangeRequest::Whole,
        ("", suffix) => match suffix.parse::<u64>() {
            Ok(0) => RangeRequest::Unsatisfiable,
            Ok(length) => RangeRequest::Partial(total.saturating_sub(length)..total),
            Err(_) => RangeRequest::Whole,
        },
        (first, "") => match first.parse::<u64>() {
            Ok(first) if first < total => RangeRequest::Partial(first..total),
            Ok(_) => RangeRequest::Unsatisfiable,
            Err(_) => RangeRequest::Whole,
        },
        (first, last) => match (first.parse::<u64>(), last.parse::<u64>()) {
            (Ok(first), Ok(last)) if first > last || first >= total => RangeRequest::Unsatisfiable,
            (Ok(first), Ok(last)) => RangeRequest::Partial(first..last.min(total - 1) + 1),
            _ => RangeRequest::Whole,
        },
    }
}

//! A durable, deterministic Raft log: the append/read/truncate primitive that the election, apply,
//! and snapshot increments of an embedded Raft node (issue #498) build on.
//!
//! The log is the one piece of Raft that carries a hard safety obligation without needing a cluster,
//! a clock, or a network: entries are numbered by a contiguous 1-based [`LogIndex`] and stamped with a
//! never-decreasing [`Term`], and once a follower has persisted an entry it must not lose or silently
//! rewrite it. [`RaftLog`] captures that contract, and [`MemoryRaftLog`] is one concrete backend that
//! keeps the invariants in memory so the property is testable in isolation, ahead of a durable store.
//!
//! [`RaftLog::append`] is the receiver half of `AppendEntries` (Raft §5.3). It rejects, fail-closed,
//! every request that would break an invariant rather than accept it: a gap past the log end, a
//! previous-term that disagrees with what is stored, a non-contiguous or term-regressing batch, or an
//! oversized batch or payload. When a new entry conflicts with a stored one at the same index it
//! truncates that suffix and adopts the new tail, and a batch already present is a no-op, so a
//! duplicated or retried request converges to the same log a single delivery would.
//!
//! [`RaftLog::snapshot`] and [`MemoryRaftLog::restore`] persist and reload the log through a
//! length-framed binary format. Restore re-validates the framing and the index and term invariants
//! and stops with a specific [`RaftLogError`] on corrupt or truncated control storage, rather than
//! rebuilding a shorter or reordered log that would silently drop committed history.

use std::ops::Range;

/// The magic prefix of the persisted log format, ASCII `PRXL`.
const MAGIC: u32 = 0x5052_584c;

/// The persisted log format revision this build writes and is willing to restore.
const FORMAT_VERSION: u8 = 1;

/// The default log bounds: a batch and a single command stay metadata-sized so an untrusted append
/// cannot force an unbounded allocation before its invariants are even checked.
pub const DEFAULT_LOG_LIMITS: LogLimits = LogLimits {
    max_entries_per_append: 1024,
    max_payload_len: 1 << 20,
};

/// A Raft term: a monotonically non-decreasing election epoch. The empty log sits at [`Term::ZERO`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Term(pub u64);

impl Term {
    /// The term of the sentinel that precedes the first entry.
    pub const ZERO: Self = Self(0);
}

/// A 1-based position in the log. Index `0` is the sentinel before the first entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogIndex(pub u64);

impl LogIndex {
    /// The sentinel position before the first entry.
    pub const ZERO: Self = Self(0);
}

/// One log entry: an opaque, bounded command stamped with the term and index it was assigned.
///
/// The command bytes are deliberately uninterpreted here; package metadata and artifact bytes stay
/// out of the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub payload: Vec<u8>,
}

/// A receiver-side `AppendEntries` request: the `(prev_index, prev_term)` the leader believes the
/// follower already holds, followed by the entries to add after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntries {
    pub prev_index: LogIndex,
    pub prev_term: Term,
    pub entries: Vec<LogEntry>,
}

/// What an accepted [`RaftLog::append`] changed: how many entries it added, how many conflicting
/// entries it dropped first, and where the log now ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendOutcome {
    pub appended: u64,
    pub truncated: u64,
    pub last_index: LogIndex,
}

/// The bounds an append is held to before it is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogLimits {
    pub max_entries_per_append: usize,
    pub max_payload_len: usize,
}

/// A rejected append, truncate, or restore. Every variant leaves the log unchanged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RaftLogError {
    #[error("append carries {actual} entries; the limit is {limit}")]
    BatchTooLarge { limit: usize, actual: usize },
    #[error("entry at index {index} carries {actual} payload bytes; the limit is {limit}")]
    PayloadTooLarge { index: u64, limit: usize, actual: usize },
    #[error("append follows index {prev_index} but the log ends at {last_index}")]
    MissingPrev { prev_index: u64, last_index: u64 },
    #[error("append expected term {expected} at index {index} but the log holds {actual}")]
    PrevTermMismatch { index: u64, expected: u64, actual: u64 },
    #[error("entry {position} has index {actual}; a contiguous log expected {expected}")]
    NonContiguous {
        position: usize,
        expected: u64,
        actual: u64,
    },
    #[error("entry at index {index} has term {actual} below the running term {previous}")]
    TermRegression { index: u64, previous: u64, actual: u64 },
    #[error("cannot truncate from index {from}; the log ends at {last_index}")]
    TruncateOutOfRange { from: u64, last_index: u64 },
    #[error("persisted log ends inside its {section}: has {actual} bytes, needs {needed}")]
    Truncated {
        section: &'static str,
        needed: usize,
        actual: usize,
    },
    #[error("persisted log has magic {actual:#010x}; expected {expected:#010x}")]
    BadMagic { expected: u32, actual: u32 },
    #[error("persisted log has format version {actual}; this build reads {expected}")]
    UnsupportedVersion { expected: u8, actual: u8 },
    #[error("persisted log has {trailing} unexpected trailing bytes")]
    TrailingBytes { trailing: usize },
    #[error("persisted entry {position} breaks the {invariant} invariant")]
    CorruptSequence { position: usize, invariant: &'static str },
}

/// A persistent Raft log: an append-only, index-numbered, term-stamped record with fail-closed
/// conflict handling and a persist/restore boundary.
pub trait RaftLog {
    /// The index of the last stored entry, or [`LogIndex::ZERO`] when the log is empty.
    fn last_index(&self) -> LogIndex;

    /// The term of the last stored entry, or [`Term::ZERO`] when the log is empty.
    fn last_term(&self) -> Term;

    /// The term stored at `index`, [`Term::ZERO`] for the sentinel index `0`, or `None` past the end.
    fn term_at(&self, index: LogIndex) -> Option<Term>;

    /// The stored entries whose 1-based index falls in `range`, clamped to what the log holds.
    fn entries(&self, range: Range<u64>) -> Vec<LogEntry>;

    /// Apply an `AppendEntries` request, truncating a conflicting suffix and adopting the new tail.
    ///
    /// # Errors
    /// Returns [`RaftLogError::BatchTooLarge`] or [`RaftLogError::PayloadTooLarge`] for a batch or
    /// command over its bound, [`RaftLogError::MissingPrev`] or [`RaftLogError::PrevTermMismatch`]
    /// when the request's predecessor does not match the stored log, and
    /// [`RaftLogError::NonContiguous`] or [`RaftLogError::TermRegression`] for a batch whose indices
    /// skip or whose terms move backwards.
    fn append(&mut self, request: &AppendEntries) -> Result<AppendOutcome, RaftLogError>;

    /// Drop every entry from `from` onward.
    ///
    /// # Errors
    /// Returns [`RaftLogError::TruncateOutOfRange`] when `from` is the sentinel `0` or past the end.
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), RaftLogError>;

    /// Serialize the whole log to the length-framed persistence format.
    fn snapshot(&self) -> Vec<u8>;
}

/// A deterministic in-memory [`RaftLog`]. Entries live in a contiguous vector whose position `i`
/// always holds index `i + 1`, so the index and term invariants are structural.
#[derive(Debug)]
pub struct MemoryRaftLog {
    entries: Vec<LogEntry>,
    limits: LogLimits,
}

impl MemoryRaftLog {
    /// An empty log bounded by `limits`.
    #[must_use]
    pub const fn new(limits: LogLimits) -> Self {
        Self {
            entries: Vec::new(),
            limits,
        }
    }

    /// Restore a log from [`RaftLog::snapshot`] bytes.
    ///
    /// Restore checks the header magic and version, the length framing, and the contiguous-index and
    /// monotonic-term invariants of every decoded entry. `limits` bounds later [`RaftLog::append`]
    /// calls; it does not cap the decoded entry count or payload sizes, which the input length bounds.
    ///
    /// # Errors
    /// Returns [`RaftLogError::Truncated`] when the bytes end inside a field,
    /// [`RaftLogError::BadMagic`] or [`RaftLogError::UnsupportedVersion`] for an unrecognized header,
    /// [`RaftLogError::CorruptSequence`] when a restored entry breaks the index or term invariant, and
    /// [`RaftLogError::TrailingBytes`] when bytes remain after the last entry.
    pub fn restore(bytes: &[u8], limits: LogLimits) -> Result<Self, RaftLogError> {
        let mut cursor = Cursor::new(bytes);
        let magic = cursor.take_u32("magic")?;
        if magic != MAGIC {
            return Err(RaftLogError::BadMagic {
                expected: MAGIC,
                actual: magic,
            });
        }
        let version = cursor.take_u8("version")?;
        if version != FORMAT_VERSION {
            return Err(RaftLogError::UnsupportedVersion {
                expected: FORMAT_VERSION,
                actual: version,
            });
        }
        let count = cursor.take_u64("entry count")?;
        let mut entries = Vec::new();
        let mut running_term = 0;
        for position in 0..count {
            let term = cursor.take_u64("entry term")?;
            let index = cursor.take_u64("entry index")?;
            let payload_len = cursor.take_u64("payload length")?;
            let payload = cursor.take(as_usize(payload_len), "payload")?.to_vec();
            if index != position + 1 {
                return Err(RaftLogError::CorruptSequence {
                    position: as_usize(position),
                    invariant: "contiguous index",
                });
            }
            if term < running_term {
                return Err(RaftLogError::CorruptSequence {
                    position: as_usize(position),
                    invariant: "monotonic term",
                });
            }
            running_term = term;
            entries.push(LogEntry {
                term: Term(term),
                index: LogIndex(index),
                payload,
            });
        }
        let trailing = cursor.remaining();
        if trailing != 0 {
            return Err(RaftLogError::TrailingBytes { trailing });
        }
        Ok(Self { entries, limits })
    }
}

impl RaftLog for MemoryRaftLog {
    fn last_index(&self) -> LogIndex {
        LogIndex(as_u64(self.entries.len()))
    }

    fn last_term(&self) -> Term {
        self.entries.last().map_or(Term::ZERO, |entry| entry.term)
    }

    fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == LogIndex::ZERO {
            return Some(Term::ZERO);
        }
        if index.0 > as_u64(self.entries.len()) {
            return None;
        }
        Some(self.entries[as_usize(index.0 - 1)].term)
    }

    fn entries(&self, range: Range<u64>) -> Vec<LogEntry> {
        let start = range.start.max(1);
        let end = range.end.min(as_u64(self.entries.len()) + 1);
        if start >= end {
            return Vec::new();
        }
        self.entries[as_usize(start - 1)..as_usize(end - 1)].to_vec()
    }

    fn append(&mut self, request: &AppendEntries) -> Result<AppendOutcome, RaftLogError> {
        if request.entries.len() > self.limits.max_entries_per_append {
            return Err(RaftLogError::BatchTooLarge {
                limit: self.limits.max_entries_per_append,
                actual: request.entries.len(),
            });
        }
        let last_index = self.last_index();
        if request.prev_index > last_index {
            return Err(RaftLogError::MissingPrev {
                prev_index: request.prev_index.0,
                last_index: last_index.0,
            });
        }
        let stored_prev_term = if request.prev_index == LogIndex::ZERO {
            Term::ZERO
        } else {
            self.entries[as_usize(request.prev_index.0 - 1)].term
        };
        if stored_prev_term != request.prev_term {
            return Err(RaftLogError::PrevTermMismatch {
                index: request.prev_index.0,
                expected: stored_prev_term.0,
                actual: request.prev_term.0,
            });
        }
        let mut running_term = request.prev_term.0;
        for (position, entry) in request.entries.iter().enumerate() {
            if entry.payload.len() > self.limits.max_payload_len {
                return Err(RaftLogError::PayloadTooLarge {
                    index: entry.index.0,
                    limit: self.limits.max_payload_len,
                    actual: entry.payload.len(),
                });
            }
            let expected_index = request.prev_index.0 + 1 + as_u64(position);
            if entry.index.0 != expected_index {
                return Err(RaftLogError::NonContiguous {
                    position,
                    expected: expected_index,
                    actual: entry.index.0,
                });
            }
            if entry.term.0 < running_term {
                return Err(RaftLogError::TermRegression {
                    index: entry.index.0,
                    previous: running_term,
                    actual: entry.term.0,
                });
            }
            running_term = entry.term.0;
        }
        let mut appended = 0;
        let mut truncated = 0;
        for entry in &request.entries {
            let position = as_usize(entry.index.0 - 1);
            if position < self.entries.len() {
                if self.entries[position].term == entry.term {
                    continue;
                }
                truncated += as_u64(self.entries.len() - position);
                self.entries.truncate(position);
            }
            self.entries.push(entry.clone());
            appended += 1;
        }
        Ok(AppendOutcome {
            appended,
            truncated,
            last_index: self.last_index(),
        })
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), RaftLogError> {
        let last = as_u64(self.entries.len());
        if from.0 == 0 || from.0 > last {
            return Err(RaftLogError::TruncateOutOfRange {
                from: from.0,
                last_index: last,
            });
        }
        self.entries.truncate(as_usize(from.0 - 1));
        Ok(())
    }

    fn snapshot(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.push(FORMAT_VERSION);
        out.extend_from_slice(&as_u64(self.entries.len()).to_be_bytes());
        for entry in &self.entries {
            out.extend_from_slice(&entry.term.0.to_be_bytes());
            out.extend_from_slice(&entry.index.0.to_be_bytes());
            out.extend_from_slice(&as_u64(entry.payload.len()).to_be_bytes());
            out.extend_from_slice(&entry.payload);
        }
        out
    }
}

fn as_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// A forward-only reader over the persisted format that fails closed the moment a field runs past the
/// available bytes.
struct Cursor<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    const fn remaining(&self) -> usize {
        self.data.len() - self.position
    }

    fn take(&mut self, count: usize, section: &'static str) -> Result<&'a [u8], RaftLogError> {
        if self.remaining() < count {
            return Err(RaftLogError::Truncated {
                section,
                needed: count,
                actual: self.remaining(),
            });
        }
        let slice = &self.data[self.position..self.position + count];
        self.position += count;
        Ok(slice)
    }

    fn take_u8(&mut self, section: &'static str) -> Result<u8, RaftLogError> {
        Ok(self.take(1, section)?[0])
    }

    fn take_u32(&mut self, section: &'static str) -> Result<u32, RaftLogError> {
        let bytes = self.take(4, section)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn take_u64(&mut self, section: &'static str) -> Result<u64, RaftLogError> {
        let bytes = self.take(8, section)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

//! The persistent term and vote a Raft node must remember across restarts, and the `RequestVote`
//! decision built on it (issue #498). Where [`crate::consensus`] owns the durable log, this owns the
//! other half of Raft's persisted state: the current election [`Term`] and the one candidate the node
//! backed in that term.
//!
//! [`PersistentState`] enforces the two safety rules a single node can uphold without a cluster, a
//! clock, or a network. Its term never decreases: [`PersistentState::observe_term`] refuses a
//! regression fail-closed rather than adopt a stale epoch, and stepping into a higher term forgets the
//! previous term's vote so a node grants at most one vote per term. [`PersistentState::request_vote`]
//! is the receiver half of `RequestVote` (Raft §5.4.1): it grants only when the candidate's term is
//! not stale, the node has not already promised this term to a different candidate, and the
//! candidate's log is at least as up to date as the local [`RaftLog`] — last-log term first, then
//! index. Every failing check denies the vote; nothing is granted by omission.
//!
//! [`PersistentState::snapshot`] and [`PersistentState::restore`] carry the term and vote through a
//! length-framed binary format. Restore re-validates the header and framing and stops on a specific
//! [`ElectionError`] for corrupt control storage rather than resurrecting a blank or half-read state
//! that would let the node vote twice in a term it had already spent.

use std::cmp::Ordering;

use crate::consensus::{LogIndex, RaftLog, Term};

/// The magic prefix of the persisted term/vote format, ASCII `PRXV`.
const MAGIC: u32 = 0x5052_5856;

/// The persisted term/vote format revision this build writes and is willing to restore.
const FORMAT_VERSION: u8 = 1;

/// The persisted tag that precedes an absent vote.
const VOTE_NONE: u8 = 0;

/// The persisted tag that precedes a recorded vote.
const VOTE_SOME: u8 = 1;

/// Identifies a voter in the cluster. A node records the [`NodeId`] of the candidate it backed so it
/// can tell a retry from that candidate (grant again) from a rival's competing request (deny).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

/// A candidate's `RequestVote` call: the term it is standing in and the tail of its log, so the
/// receiver can judge whether that log is at least as up to date as its own (Raft §5.4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteRequest {
    pub candidate: NodeId,
    pub term: Term,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// Why [`PersistentState::request_vote`] granted or denied a vote. Exactly one variant means granted;
/// the rest name the safety check that refused it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteReason {
    /// The candidate's term is not stale, the vote was free, and its log was up to date.
    Granted,
    /// The candidate's term is below the node's current term.
    StaleTerm,
    /// The node already granted this term to a different candidate.
    AlreadyVoted,
    /// The candidate's log is behind the node's own by the §5.4.1 comparison.
    LogBehind,
}

/// The reply to a [`VoteRequest`]: the term the node now holds and the [`VoteReason`] for its answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteDecision {
    pub term: Term,
    pub reason: VoteReason,
}

impl VoteDecision {
    /// Whether the node granted its vote.
    #[must_use]
    pub const fn granted(&self) -> bool {
        matches!(self.reason, VoteReason::Granted)
    }
}

/// A rejected [`PersistentState::observe_term`] or [`PersistentState::restore`]. Every variant leaves
/// the state unchanged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ElectionError {
    #[error("term moves from {current} down to {proposed}; a term never decreases")]
    TermRegression { current: u64, proposed: u64 },
    #[error("persisted state ends inside its {section}: has {actual} bytes, needs {needed}")]
    Truncated {
        section: &'static str,
        needed: usize,
        actual: usize,
    },
    #[error("persisted state has magic {actual:#010x}; expected {expected:#010x}")]
    BadMagic { expected: u32, actual: u32 },
    #[error("persisted state has format version {actual}; this build reads {expected}")]
    UnsupportedVersion { expected: u8, actual: u8 },
    #[error("persisted state has vote tag {actual}; expected {none} (none) or {some} (some)")]
    BadVoteTag { actual: u8, none: u8, some: u8 },
    #[error("persisted state has {trailing} unexpected trailing bytes")]
    TrailingBytes { trailing: usize },
}

/// The durable election state of one Raft node: the current [`Term`] and the candidate, if any, it
/// voted for in that term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentState {
    current_term: Term,
    voted_for: Option<NodeId>,
}

impl Default for PersistentState {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistentState {
    /// A fresh node: term zero with no vote cast.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            current_term: Term::ZERO,
            voted_for: None,
        }
    }

    /// The term the node is currently standing in.
    #[must_use]
    pub const fn current_term(&self) -> Term {
        self.current_term
    }

    /// The candidate the node backed in the current term, or `None` if its vote is still free.
    #[must_use]
    pub const fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// Adopt `term` if it advances the node's epoch, reporting whether it moved.
    ///
    /// Stepping into a higher term forgets the previous term's vote, so the node can vote once in the
    /// new term. An equal term is a no-op.
    ///
    /// # Errors
    /// Returns [`ElectionError::TermRegression`] when `term` is below the current term; a term never
    /// moves backwards.
    pub fn observe_term(&mut self, term: Term) -> Result<bool, ElectionError> {
        match term.cmp(&self.current_term) {
            Ordering::Less => Err(ElectionError::TermRegression {
                current: self.current_term.0,
                proposed: term.0,
            }),
            Ordering::Equal => Ok(false),
            Ordering::Greater => {
                self.adopt_term(term);
                Ok(true)
            }
        }
    }

    const fn adopt_term(&mut self, term: Term) {
        self.current_term = term;
        self.voted_for = None;
    }

    /// Decide a candidate's `RequestVote` against this node's term, prior vote, and `log`.
    ///
    /// A candidate in a newer term first advances the node's term (forgetting an earlier vote). The
    /// node then grants only a free or already-promised-to-this-candidate vote whose log is at least
    /// as up to date as its own; a stale term, a vote already spent on a rival, or a shorter log each
    /// denies. A granted vote is recorded, so a repeat from the same candidate stays granted and a
    /// rival in the same term is refused.
    pub fn request_vote(&mut self, request: &VoteRequest, log: &impl RaftLog) -> VoteDecision {
        if request.term < self.current_term {
            return self.deny(VoteReason::StaleTerm);
        }
        if request.term > self.current_term {
            self.adopt_term(request.term);
        }
        if matches!(self.voted_for, Some(backed) if backed != request.candidate) {
            return self.deny(VoteReason::AlreadyVoted);
        }
        if !candidate_log_is_current(request, log) {
            return self.deny(VoteReason::LogBehind);
        }
        self.voted_for = Some(request.candidate);
        VoteDecision {
            term: self.current_term,
            reason: VoteReason::Granted,
        }
    }

    const fn deny(&self, reason: VoteReason) -> VoteDecision {
        VoteDecision {
            term: self.current_term,
            reason,
        }
    }

    /// Serialize the term and vote to the length-framed persistence format.
    #[must_use]
    pub fn snapshot(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.push(FORMAT_VERSION);
        out.extend_from_slice(&self.current_term.0.to_be_bytes());
        match self.voted_for {
            Some(candidate) => {
                out.push(VOTE_SOME);
                out.extend_from_slice(&candidate.0.to_be_bytes());
            }
            None => out.push(VOTE_NONE),
        }
        out
    }

    /// Restore term and vote from [`PersistentState::snapshot`] bytes.
    ///
    /// # Errors
    /// Returns [`ElectionError::Truncated`] when the bytes end inside a field,
    /// [`ElectionError::BadMagic`] or [`ElectionError::UnsupportedVersion`] for an unrecognized header,
    /// [`ElectionError::BadVoteTag`] when the vote tag is neither present nor absent, and
    /// [`ElectionError::TrailingBytes`] when bytes remain after the state.
    pub fn restore(bytes: &[u8]) -> Result<Self, ElectionError> {
        let mut cursor = Cursor::new(bytes);
        let magic = cursor.take_u32("magic")?;
        if magic != MAGIC {
            return Err(ElectionError::BadMagic {
                expected: MAGIC,
                actual: magic,
            });
        }
        let version = cursor.take_u8("version")?;
        if version != FORMAT_VERSION {
            return Err(ElectionError::UnsupportedVersion {
                expected: FORMAT_VERSION,
                actual: version,
            });
        }
        let current_term = Term(cursor.take_u64("current term")?);
        let voted_for = match cursor.take_u8("vote tag")? {
            VOTE_NONE => None,
            VOTE_SOME => Some(NodeId(cursor.take_u64("voted candidate")?)),
            actual => {
                return Err(ElectionError::BadVoteTag {
                    actual,
                    none: VOTE_NONE,
                    some: VOTE_SOME,
                });
            }
        };
        let trailing = cursor.remaining();
        if trailing != 0 {
            return Err(ElectionError::TrailingBytes { trailing });
        }
        Ok(Self {
            current_term,
            voted_for,
        })
    }
}

/// Whether the candidate's log tail is at least as up to date as the receiver's, by Raft §5.4.1: the
/// later last-log term wins, and an equal term falls back to the longer log.
fn candidate_log_is_current(request: &VoteRequest, log: &impl RaftLog) -> bool {
    match request.last_log_term.cmp(&log.last_term()) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => request.last_log_index >= log.last_index(),
    }
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

    fn take(&mut self, count: usize, section: &'static str) -> Result<&'a [u8], ElectionError> {
        if self.remaining() < count {
            return Err(ElectionError::Truncated {
                section,
                needed: count,
                actual: self.remaining(),
            });
        }
        let slice = &self.data[self.position..self.position + count];
        self.position += count;
        Ok(slice)
    }

    fn take_u8(&mut self, section: &'static str) -> Result<u8, ElectionError> {
        Ok(self.take(1, section)?[0])
    }

    fn take_u32(&mut self, section: &'static str) -> Result<u32, ElectionError> {
        let bytes = self.take(4, section)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn take_u64(&mut self, section: &'static str) -> Result<u64, ElectionError> {
        let bytes = self.take(8, section)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

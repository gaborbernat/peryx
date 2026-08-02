use crate::consensus::{AppendEntries, DEFAULT_LOG_LIMITS, LogEntry, LogIndex, MemoryRaftLog, RaftLog, Term};
use crate::election::{ElectionError, NodeId, PersistentState, VoteReason, VoteRequest};

fn log_ending(term: u64, index: u64) -> MemoryRaftLog {
    let mut log = MemoryRaftLog::new(DEFAULT_LOG_LIMITS);
    if index > 0 {
        let entries = (1..=index)
            .map(|position| LogEntry {
                term: Term(term),
                index: LogIndex(position),
                payload: b"x".to_vec(),
            })
            .collect();
        log.append(&AppendEntries {
            prev_index: LogIndex(0),
            prev_term: Term(0),
            entries,
        })
        .unwrap();
    }
    log
}

fn vote(candidate: u64, term: u64, last_log_index: u64, last_log_term: u64) -> VoteRequest {
    VoteRequest {
        candidate: NodeId(candidate),
        term: Term(term),
        last_log_index: LogIndex(last_log_index),
        last_log_term: Term(last_log_term),
    }
}

#[test]
fn test_new_state_carries_no_term_and_no_vote() {
    let state = PersistentState::new();

    assert_eq!(state.current_term(), Term::ZERO);
    assert_eq!(state.voted_for(), None);
    assert_eq!(PersistentState::default(), state);
}

#[test]
fn test_observe_higher_term_advances_and_clears_the_vote() {
    let mut state = PersistentState::new();
    let granted = state.request_vote(&vote(7, 4, 0, 0), &MemoryRaftLog::new(DEFAULT_LOG_LIMITS));
    assert!(granted.granted());

    let moved = state.observe_term(Term(9)).unwrap();

    assert!(moved);
    assert_eq!(state.current_term(), Term(9));
    assert_eq!(state.voted_for(), None);
}

#[test]
fn test_observe_equal_term_keeps_the_vote() {
    let mut state = PersistentState::new();
    state.request_vote(&vote(7, 4, 0, 0), &MemoryRaftLog::new(DEFAULT_LOG_LIMITS));

    let moved = state.observe_term(Term(4)).unwrap();

    assert!(!moved);
    assert_eq!(state.current_term(), Term(4));
    assert_eq!(state.voted_for(), Some(NodeId(7)));
}

#[test]
fn test_observe_lower_term_is_refused_and_changes_nothing() {
    let mut state = PersistentState::new();
    state.observe_term(Term(5)).unwrap();

    let error = state.observe_term(Term(3)).unwrap_err();

    assert_eq!(
        error,
        ElectionError::TermRegression {
            current: 5,
            proposed: 3
        }
    );
    assert!(error.to_string().contains("never decreases"));
    assert_eq!(state.current_term(), Term(5));
}

#[test]
fn test_request_vote_grants_a_free_up_to_date_candidate() {
    let mut state = PersistentState::new();

    let decision = state.request_vote(&vote(2, 1, 0, 0), &MemoryRaftLog::new(DEFAULT_LOG_LIMITS));

    assert!(decision.granted());
    assert_eq!(decision.reason, VoteReason::Granted);
    assert_eq!(decision.term, Term(1));
    assert_eq!(state.current_term(), Term(1));
    assert_eq!(state.voted_for(), Some(NodeId(2)));
}

#[test]
fn test_request_vote_advances_the_term_before_granting() {
    let mut state = PersistentState::new();
    state.observe_term(Term(2)).unwrap();

    let decision = state.request_vote(&vote(3, 5, 0, 0), &MemoryRaftLog::new(DEFAULT_LOG_LIMITS));

    assert!(decision.granted());
    assert_eq!(state.current_term(), Term(5));
    assert_eq!(state.voted_for(), Some(NodeId(3)));
}

#[test]
fn test_request_vote_repeats_stay_granted_for_the_same_candidate() {
    let mut state = PersistentState::new();
    let log = MemoryRaftLog::new(DEFAULT_LOG_LIMITS);
    state.request_vote(&vote(4, 2, 0, 0), &log);

    let decision = state.request_vote(&vote(4, 2, 0, 0), &log);

    assert!(decision.granted());
    assert_eq!(state.voted_for(), Some(NodeId(4)));
}

#[test]
fn test_request_vote_denies_a_stale_term() {
    let mut state = PersistentState::new();
    state.observe_term(Term(5)).unwrap();

    let decision = state.request_vote(&vote(2, 3, 0, 0), &MemoryRaftLog::new(DEFAULT_LOG_LIMITS));

    assert!(!decision.granted());
    assert_eq!(decision.reason, VoteReason::StaleTerm);
    assert_eq!(decision.term, Term(5));
    assert_eq!(state.voted_for(), None);
}

#[test]
fn test_request_vote_denies_a_second_candidate_in_the_same_term() {
    let mut state = PersistentState::new();
    let log = MemoryRaftLog::new(DEFAULT_LOG_LIMITS);
    state.request_vote(&vote(1, 3, 0, 0), &log);

    let decision = state.request_vote(&vote(2, 3, 0, 0), &log);

    assert!(!decision.granted());
    assert_eq!(decision.reason, VoteReason::AlreadyVoted);
    assert_eq!(state.voted_for(), Some(NodeId(1)));
}

#[test]
fn test_request_vote_denies_a_candidate_whose_last_log_term_is_behind() {
    let mut state = PersistentState::new();
    let log = log_ending(3, 2);

    let decision = state.request_vote(&vote(2, 5, 9, 2), &log);

    assert!(!decision.granted());
    assert_eq!(decision.reason, VoteReason::LogBehind);
    assert_eq!(state.current_term(), Term(5));
    assert_eq!(state.voted_for(), None);
}

#[test]
fn test_request_vote_denies_a_candidate_with_a_shorter_log_of_equal_term() {
    let mut state = PersistentState::new();
    let log = log_ending(3, 5);

    let decision = state.request_vote(&vote(2, 5, 4, 3), &log);

    assert!(!decision.granted());
    assert_eq!(decision.reason, VoteReason::LogBehind);
}

#[test]
fn test_request_vote_grants_a_longer_log_of_equal_term() {
    let mut state = PersistentState::new();
    let log = log_ending(3, 2);

    let decision = state.request_vote(&vote(2, 5, 5, 3), &log);

    assert!(decision.granted());
    assert_eq!(state.voted_for(), Some(NodeId(2)));
}

#[test]
fn test_request_vote_grants_a_higher_log_term_despite_a_shorter_index() {
    let mut state = PersistentState::new();
    let log = log_ending(3, 9);

    let decision = state.request_vote(&vote(2, 5, 1, 4), &log);

    assert!(decision.granted());
}

#[test]
fn test_snapshot_round_trips_a_cast_vote() {
    let mut state = PersistentState::new();
    state.request_vote(&vote(6, 8, 0, 0), &MemoryRaftLog::new(DEFAULT_LOG_LIMITS));

    let restored = PersistentState::restore(&state.snapshot()).unwrap();

    assert_eq!(restored, state);
    assert_eq!(restored.voted_for(), Some(NodeId(6)));
}

#[test]
fn test_snapshot_round_trips_a_free_vote() {
    let mut state = PersistentState::new();
    state.observe_term(Term(4)).unwrap();

    let restored = PersistentState::restore(&state.snapshot()).unwrap();

    assert_eq!(restored, state);
    assert_eq!(restored.voted_for(), None);
}

#[test]
fn test_restore_rejects_a_short_header() {
    let error = PersistentState::restore(&[0u8; 2]).unwrap_err();

    assert_eq!(
        error,
        ElectionError::Truncated {
            section: "magic",
            needed: 4,
            actual: 2,
        }
    );
    assert!(error.to_string().contains("magic"));
}

#[test]
fn test_restore_rejects_bad_magic() {
    let mut bytes = PersistentState::new().snapshot();
    bytes[0] ^= 0xff;

    let error = PersistentState::restore(&bytes).unwrap_err();

    assert!(matches!(error, ElectionError::BadMagic { .. }));
    assert!(error.to_string().contains("magic"));
}

#[test]
fn test_restore_rejects_an_unsupported_version() {
    let mut bytes = PersistentState::new().snapshot();
    bytes[4] = 9;

    let error = PersistentState::restore(&bytes).unwrap_err();

    assert_eq!(error, ElectionError::UnsupportedVersion { expected: 1, actual: 9 });
    assert!(error.to_string().contains("format version 9"));
}

#[test]
fn test_restore_rejects_a_bad_vote_tag() {
    let mut bytes = PersistentState::new().snapshot();
    *bytes.last_mut().unwrap() = 2;

    let error = PersistentState::restore(&bytes).unwrap_err();

    assert_eq!(
        error,
        ElectionError::BadVoteTag {
            actual: 2,
            none: 0,
            some: 1
        }
    );
    assert!(error.to_string().contains("vote tag 2"));
}

#[test]
fn test_restore_rejects_a_truncated_vote_body() {
    let mut state = PersistentState::new();
    state.request_vote(&vote(6, 8, 0, 0), &MemoryRaftLog::new(DEFAULT_LOG_LIMITS));
    let bytes = state.snapshot();

    let error = PersistentState::restore(&bytes[..bytes.len() - 1]).unwrap_err();

    assert!(matches!(
        error,
        ElectionError::Truncated {
            section: "voted candidate",
            ..
        }
    ));
}

#[test]
fn test_restore_rejects_trailing_bytes() {
    let mut bytes = PersistentState::new().snapshot();
    bytes.push(0);

    let error = PersistentState::restore(&bytes).unwrap_err();

    assert_eq!(error, ElectionError::TrailingBytes { trailing: 1 });
    assert!(error.to_string().contains("trailing bytes"));
}

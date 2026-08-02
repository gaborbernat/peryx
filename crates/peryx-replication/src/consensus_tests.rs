use crate::{
    AppendEntries, AppendOutcome, DEFAULT_LOG_LIMITS, LogEntry, LogIndex, LogLimits, MemoryRaftLog, RaftLog,
    RaftLogError, Term,
};

fn entry(term: u64, index: u64, payload: &[u8]) -> LogEntry {
    LogEntry {
        term: Term(term),
        index: LogIndex(index),
        payload: payload.to_vec(),
    }
}

fn req(prev_index: u64, prev_term: u64, entries: Vec<LogEntry>) -> AppendEntries {
    AppendEntries {
        prev_index: LogIndex(prev_index),
        prev_term: Term(prev_term),
        entries,
    }
}

fn seeded() -> MemoryRaftLog {
    let mut log = MemoryRaftLog::new(DEFAULT_LOG_LIMITS);
    log.append(&req(
        0,
        0,
        vec![entry(1, 1, b"a"), entry(1, 2, b"b"), entry(2, 3, b"c")],
    ))
    .unwrap();
    log
}

#[test]
fn test_new_log_is_empty_at_the_sentinel() {
    let log = MemoryRaftLog::new(DEFAULT_LOG_LIMITS);

    assert_eq!(log.last_index(), LogIndex::ZERO);
    assert_eq!(log.last_term(), Term::ZERO);
    assert_eq!(log.term_at(LogIndex::ZERO), Some(Term::ZERO));
    assert_eq!(log.term_at(LogIndex(1)), None);
    assert!(log.entries(1..9).is_empty());
}

#[test]
fn test_append_onto_empty_log_reads_back_the_entries() {
    let mut log = MemoryRaftLog::new(DEFAULT_LOG_LIMITS);

    let outcome = log
        .append(&req(0, 0, vec![entry(1, 1, b"a"), entry(1, 2, b"b")]))
        .unwrap();

    assert_eq!(
        outcome,
        AppendOutcome {
            appended: 2,
            truncated: 0,
            last_index: LogIndex(2),
        }
    );
    assert_eq!(log.last_index(), LogIndex(2));
    assert_eq!(log.last_term(), Term(1));
    assert_eq!(log.entries(1..3), vec![entry(1, 1, b"a"), entry(1, 2, b"b")]);
}

#[test]
fn test_append_after_a_predecessor_extends_the_tail() {
    let mut log = seeded();

    let outcome = log.append(&req(3, 2, vec![entry(4, 4, b"d")])).unwrap();

    assert_eq!(outcome.appended, 1);
    assert_eq!(outcome.truncated, 0);
    assert_eq!(log.last_index(), LogIndex(4));
    assert_eq!(log.last_term(), Term(4));
}

#[test]
fn test_append_empty_batch_is_a_heartbeat_that_changes_nothing() {
    let mut log = seeded();

    let outcome = log.append(&req(3, 2, Vec::new())).unwrap();

    assert_eq!(
        outcome,
        AppendOutcome {
            appended: 0,
            truncated: 0,
            last_index: LogIndex(3),
        }
    );
    assert_eq!(log.last_index(), LogIndex(3));
}

#[test]
fn test_replayed_batch_is_idempotent() {
    let mut log = seeded();

    let outcome = log
        .append(&req(1, 1, vec![entry(1, 2, b"b"), entry(2, 3, b"c")]))
        .unwrap();

    assert_eq!(outcome.appended, 0);
    assert_eq!(outcome.truncated, 0);
    assert_eq!(log.last_index(), LogIndex(3));
    assert_eq!(
        log.entries(1..4),
        vec![entry(1, 1, b"a"), entry(1, 2, b"b"), entry(2, 3, b"c")]
    );
}

#[test]
fn test_conflicting_entry_truncates_the_suffix_and_adopts_the_new_tail() {
    let mut log = seeded();

    let outcome = log
        .append(&req(1, 1, vec![entry(3, 2, b"x"), entry(3, 3, b"y")]))
        .unwrap();

    assert_eq!(outcome.appended, 2);
    assert_eq!(outcome.truncated, 2);
    assert_eq!(
        log.entries(1..4),
        vec![entry(1, 1, b"a"), entry(3, 2, b"x"), entry(3, 3, b"y")]
    );
    assert_eq!(log.last_term(), Term(3));
}

#[test]
fn test_append_over_the_batch_limit_is_rejected() {
    let mut log = MemoryRaftLog::new(LogLimits {
        max_entries_per_append: 1,
        max_payload_len: 1024,
    });

    let error = log
        .append(&req(0, 0, vec![entry(1, 1, b"a"), entry(1, 2, b"b")]))
        .unwrap_err();

    assert_eq!(error, RaftLogError::BatchTooLarge { limit: 1, actual: 2 });
    assert!(error.to_string().contains("2 entries"));
    assert_eq!(log.last_index(), LogIndex::ZERO);
}

#[test]
fn test_append_over_the_payload_limit_is_rejected() {
    let mut log = MemoryRaftLog::new(LogLimits {
        max_entries_per_append: 8,
        max_payload_len: 2,
    });

    let error = log.append(&req(0, 0, vec![entry(1, 1, b"too big")])).unwrap_err();

    assert_eq!(
        error,
        RaftLogError::PayloadTooLarge {
            index: 1,
            limit: 2,
            actual: 7,
        }
    );
    assert!(error.to_string().contains("payload bytes"));
    assert_eq!(log.last_index(), LogIndex::ZERO);
}

#[test]
fn test_append_past_the_log_end_is_rejected() {
    let mut log = seeded();

    let error = log.append(&req(9, 2, vec![entry(2, 10, b"z")])).unwrap_err();

    assert_eq!(
        error,
        RaftLogError::MissingPrev {
            prev_index: 9,
            last_index: 3,
        }
    );
    assert!(error.to_string().contains("ends at 3"));
}

#[test]
fn test_append_with_a_disagreeing_prev_term_is_rejected() {
    let mut log = seeded();

    let error = log.append(&req(3, 9, vec![entry(9, 4, b"z")])).unwrap_err();

    assert_eq!(
        error,
        RaftLogError::PrevTermMismatch {
            index: 3,
            expected: 2,
            actual: 9,
        }
    );
    assert!(error.to_string().contains("expected term 2"));
    assert_eq!(log.last_index(), LogIndex(3));
}

#[test]
fn test_append_with_a_non_contiguous_index_is_rejected() {
    let mut log = seeded();

    let error = log.append(&req(3, 2, vec![entry(3, 5, b"gap")])).unwrap_err();

    assert_eq!(
        error,
        RaftLogError::NonContiguous {
            position: 0,
            expected: 4,
            actual: 5,
        }
    );
    assert!(error.to_string().contains("contiguous log expected 4"));
}

#[test]
fn test_append_with_a_regressing_term_is_rejected() {
    let mut log = seeded();

    let error = log.append(&req(3, 2, vec![entry(1, 4, b"old")])).unwrap_err();

    assert_eq!(
        error,
        RaftLogError::TermRegression {
            index: 4,
            previous: 2,
            actual: 1,
        }
    );
    assert!(error.to_string().contains("below the running term 2"));
    assert_eq!(log.last_index(), LogIndex(3));
}

#[test]
fn test_entries_clamps_a_low_start_to_the_first_index() {
    let log = seeded();

    assert_eq!(log.entries(0..2), vec![entry(1, 1, b"a")]);
}

#[test]
fn test_entries_clamps_a_high_end_to_the_last_index() {
    let log = seeded();

    assert_eq!(log.entries(2..99), vec![entry(1, 2, b"b"), entry(2, 3, b"c")]);
}

#[test]
fn test_entries_beyond_the_log_is_empty() {
    let log = seeded();

    assert!(log.entries(5..9).is_empty());
    assert!(log.entries(3..3).is_empty());
}

#[test]
fn test_term_at_reads_the_sentinel_stored_and_missing_positions() {
    let log = seeded();

    assert_eq!(log.term_at(LogIndex::ZERO), Some(Term::ZERO));
    assert_eq!(log.term_at(LogIndex(2)), Some(Term(1)));
    assert_eq!(log.term_at(LogIndex(3)), Some(Term(2)));
    assert_eq!(log.term_at(LogIndex(4)), None);
}

#[test]
fn test_truncate_suffix_drops_from_the_given_index() {
    let mut log = seeded();

    log.truncate_suffix(LogIndex(2)).unwrap();

    assert_eq!(log.last_index(), LogIndex(1));
    assert_eq!(log.entries(1..9), vec![entry(1, 1, b"a")]);
}

#[test]
fn test_truncate_suffix_rejects_the_sentinel() {
    let mut log = seeded();

    let error = log.truncate_suffix(LogIndex::ZERO).unwrap_err();

    assert_eq!(error, RaftLogError::TruncateOutOfRange { from: 0, last_index: 3 });
    assert!(error.to_string().contains("ends at 3"));
    assert_eq!(log.last_index(), LogIndex(3));
}

#[test]
fn test_truncate_suffix_rejects_an_index_past_the_end() {
    let mut log = seeded();

    let error = log.truncate_suffix(LogIndex(9)).unwrap_err();

    assert_eq!(error, RaftLogError::TruncateOutOfRange { from: 9, last_index: 3 });
    assert_eq!(log.last_index(), LogIndex(3));
}

#[test]
fn test_snapshot_round_trips_through_restore() {
    let log = seeded();

    let restored = MemoryRaftLog::restore(&log.snapshot(), DEFAULT_LOG_LIMITS).unwrap();

    assert_eq!(restored.last_index(), LogIndex(3));
    assert_eq!(restored.last_term(), Term(2));
    assert_eq!(
        restored.entries(1..4),
        vec![entry(1, 1, b"a"), entry(1, 2, b"b"), entry(2, 3, b"c")]
    );
}

#[test]
fn test_snapshot_of_an_empty_log_round_trips() {
    let log = MemoryRaftLog::new(DEFAULT_LOG_LIMITS);

    let restored = MemoryRaftLog::restore(&log.snapshot(), DEFAULT_LOG_LIMITS).unwrap();

    assert_eq!(restored.last_index(), LogIndex::ZERO);
    assert!(restored.entries(1..9).is_empty());
}

#[test]
fn test_restore_rejects_a_truncated_header() {
    let error = MemoryRaftLog::restore(&[0u8; 2], DEFAULT_LOG_LIMITS).unwrap_err();

    assert_eq!(
        error,
        RaftLogError::Truncated {
            section: "magic",
            needed: 4,
            actual: 2,
        }
    );
    assert!(error.to_string().contains("magic"));
}

#[test]
fn test_restore_rejects_a_truncated_version() {
    let mut bytes = 0x5052_584c_u32.to_be_bytes().to_vec();
    bytes.truncate(4);

    let error = MemoryRaftLog::restore(&bytes, DEFAULT_LOG_LIMITS).unwrap_err();

    assert_eq!(
        error,
        RaftLogError::Truncated {
            section: "version",
            needed: 1,
            actual: 0,
        }
    );
}

#[test]
fn test_restore_rejects_a_truncated_count() {
    let mut bytes = 0x5052_584c_u32.to_be_bytes().to_vec();
    bytes.push(1);

    let error = MemoryRaftLog::restore(&bytes, DEFAULT_LOG_LIMITS).unwrap_err();

    assert_eq!(
        error,
        RaftLogError::Truncated {
            section: "entry count",
            needed: 8,
            actual: 0,
        }
    );
}

#[test]
fn test_restore_rejects_a_truncated_entry_and_payload() {
    let full = seeded().snapshot();

    let error = MemoryRaftLog::restore(&full[..full.len() - 1], DEFAULT_LOG_LIMITS).unwrap_err();

    assert!(matches!(error, RaftLogError::Truncated { section: "payload", .. }));
}

#[test]
fn test_restore_rejects_bad_magic() {
    let mut bytes = seeded().snapshot();
    bytes[0] = 0;

    let error = MemoryRaftLog::restore(&bytes, DEFAULT_LOG_LIMITS).unwrap_err();

    assert!(matches!(
        error,
        RaftLogError::BadMagic {
            expected: 0x5052_584c,
            ..
        }
    ));
    assert!(error.to_string().contains("magic"));
}

#[test]
fn test_restore_rejects_an_unknown_version() {
    let mut bytes = seeded().snapshot();
    bytes[4] = 9;

    let error = MemoryRaftLog::restore(&bytes, DEFAULT_LOG_LIMITS).unwrap_err();

    assert_eq!(error, RaftLogError::UnsupportedVersion { expected: 1, actual: 9 });
    assert!(error.to_string().contains("format version 9"));
}

#[test]
fn test_restore_rejects_a_non_contiguous_persisted_index() {
    let mut bytes = 0x5052_584c_u32.to_be_bytes().to_vec();
    bytes.push(1);
    bytes.extend_from_slice(&1u64.to_be_bytes());
    bytes.extend_from_slice(&1u64.to_be_bytes());
    bytes.extend_from_slice(&7u64.to_be_bytes());
    bytes.extend_from_slice(&0u64.to_be_bytes());

    let error = MemoryRaftLog::restore(&bytes, DEFAULT_LOG_LIMITS).unwrap_err();

    assert_eq!(
        error,
        RaftLogError::CorruptSequence {
            position: 0,
            invariant: "contiguous index",
        }
    );
    assert!(error.to_string().contains("contiguous index"));
}

#[test]
fn test_restore_rejects_a_regressing_persisted_term() {
    let mut bytes = 0x5052_584c_u32.to_be_bytes().to_vec();
    bytes.push(1);
    bytes.extend_from_slice(&2u64.to_be_bytes());
    for (term, index) in [(5u64, 1u64), (2, 2)] {
        bytes.extend_from_slice(&term.to_be_bytes());
        bytes.extend_from_slice(&index.to_be_bytes());
        bytes.extend_from_slice(&0u64.to_be_bytes());
    }

    let error = MemoryRaftLog::restore(&bytes, DEFAULT_LOG_LIMITS).unwrap_err();

    assert_eq!(
        error,
        RaftLogError::CorruptSequence {
            position: 1,
            invariant: "monotonic term",
        }
    );
    assert!(error.to_string().contains("monotonic term"));
}

#[test]
fn test_restore_rejects_trailing_bytes() {
    let mut bytes = seeded().snapshot();
    bytes.push(0);

    let error = MemoryRaftLog::restore(&bytes, DEFAULT_LOG_LIMITS).unwrap_err();

    assert_eq!(error, RaftLogError::TrailingBytes { trailing: 1 });
    assert!(error.to_string().contains("trailing bytes"));
}

fn drive_through_trait<L: RaftLog>(log: &mut L) -> AppendOutcome {
    log.append(&req(
        log.last_index().0,
        log.last_term().0,
        vec![entry(7, log.last_index().0 + 1, b"z")],
    ))
    .unwrap()
}

#[test]
fn test_raft_log_trait_object_drives_the_backend() {
    let mut log = seeded();

    let outcome = drive_through_trait(&mut log);

    assert_eq!(outcome.last_index, LogIndex(4));
    assert_eq!(log.last_term(), Term(7));
    let persisted = (&log as &dyn RaftLog).snapshot();
    assert_eq!(
        MemoryRaftLog::restore(&persisted, DEFAULT_LOG_LIMITS)
            .unwrap()
            .last_index(),
        LogIndex(4)
    );
}

#[test]
fn test_consensus_types_expose_debug_clone_and_eq() {
    let entries = vec![entry(1, 1, b"a")];
    let request = req(0, 0, entries.clone());
    let outcome = AppendOutcome {
        appended: 1,
        truncated: 0,
        last_index: LogIndex(1),
    };

    assert_eq!(request.clone(), request);
    assert_eq!(entries[0].clone(), entries[0]);
    assert_eq!(outcome, outcome);
    assert_eq!(DEFAULT_LOG_LIMITS.clone(), DEFAULT_LOG_LIMITS);
    assert!(!format!("{request:?}").is_empty());
    assert!(!format!("{outcome:?}").is_empty());
    assert!(!format!("{:?}", entries[0]).is_empty());
    assert!(!format!("{:?}", Term(1)).is_empty());
    assert!(!format!("{:?}", LogIndex(1)).is_empty());
    assert!(!format!("{DEFAULT_LOG_LIMITS:?}").is_empty());
    assert!(!format!("{:?}", seeded()).is_empty());
}

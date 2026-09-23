use std::num::NonZeroUsize;
use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use peryx_identity::{ArtifactDigest, RevocationReason, UserId};
use rstest::rstest;

use crate::meta::checkpoint_transfer::{CheckpointCursor, CheckpointInstallError, CheckpointStageError, ROW_TAG};
use crate::meta::fault::initialized;
use crate::meta::{CheckpointIdentity, CheckpointManifest, MetaError, MetaStore};

const CURSOR_KEY: &str = "replication\u{0}state";
const CURSOR_VALUE: &[u8] = br#"{"source":"primary-a","serial":9}"#;

fn identity() -> CheckpointIdentity {
    CheckpointIdentity {
        source: "primary-a".to_owned(),
        protocol_version: 1,
        schema_version: 7,
    }
}

fn store() -> MetaStore {
    let (store, _pages, _fault) = initialized();
    store
}

fn stepped_store() -> (MetaStore, Arc<AtomicI64>) {
    let now = Arc::new(AtomicI64::new(0));
    let ticks = Arc::clone(&now);
    (store().with_clock(Arc::new(move || ticks.load(Ordering::Relaxed))), now)
}

fn commit(store: &MetaStore, body: impl FnOnce(&mut crate::meta::DriverTxn) -> Result<(), MetaError>) {
    store
        .commit_driver_txn(|txn| body(txn).map(|()| ((), vec![b"{}".to_vec()])))
        .unwrap();
}

/// A writer holding rows, a revocation and a blob reference, so a transfer carries all three sections.
fn published(rows: usize) -> (MetaStore, CheckpointManifest) {
    let store = store();
    for index in 0..rows {
        commit(&store, |txn| {
            txn.put(&format!("pypi\u{0}p\u{0}hosted/pkg{index:04}"), b"display")
        });
    }
    let digest = ArtifactDigest::from_str(&format!("sha256:{:064x}", 1)).unwrap();
    store
        .put_digest_revocation(
            &digest,
            &RevocationReason::new("incident").unwrap(),
            &UserId::random(),
            10,
        )
        .unwrap();
    commit(&store, |txn| {
        txn.reference_blob("f00d", 42);
        Ok(())
    });
    let manifest = store.publish_checkpoint(identity()).unwrap();
    (store, manifest)
}

/// Streams a whole checkpoint through chunks the way a transfer does, returning what was staged.
fn transfer(from: &MetaStore, to: &MetaStore, manifest: &CheckpointManifest, budget: usize) -> u64 {
    to.begin_checkpoint_transfer(manifest).unwrap();
    let mut cursor = CheckpointCursor::start();
    let mut offset = 0;
    loop {
        let chunk = from.checkpoint_chunk(&cursor, budget).unwrap();
        if !chunk.bytes.is_empty() {
            let staged = to
                .stage_checkpoint_chunk(manifest, offset, &chunk.bytes, &chunk.next.token())
                .unwrap()
                .unwrap();
            offset = staged.received;
        }
        if chunk.next == CheckpointCursor::Done {
            return offset;
        }
        cursor = chunk.next;
    }
}

#[test]
fn test_a_chunked_transfer_carries_exactly_what_the_manifest_declares() {
    let (writer, manifest) = published(40);
    let replica = store();

    let received = transfer(&writer, &replica, &manifest, 512);

    assert_eq!(received, manifest.bytes);
}

/// The window is row-aligned, so the chunk count follows the budget rather than the section layout.
#[test]
fn test_a_smaller_budget_splits_the_same_bytes_into_more_chunks() {
    let (writer, _manifest) = published(40);

    let mut counts = Vec::new();
    for budget in [256, 4096] {
        let mut cursor = CheckpointCursor::start();
        let mut chunks = 0;
        loop {
            let chunk = writer.checkpoint_chunk(&cursor, budget).unwrap();
            chunks += 1;
            if chunk.next == CheckpointCursor::Done {
                break;
            }
            cursor = chunk.next;
        }
        counts.push(chunks);
    }

    assert!(counts[0] > counts[1], "{counts:?}");
}

#[test]
fn test_an_installed_checkpoint_replaces_state_and_stands_at_its_serial() {
    let (writer, manifest) = published(4);
    let replica = store();
    commit(&replica, |txn| txn.put("stale\u{0}row", b"from a life before"));
    transfer(&writer, &replica, &manifest, 512);

    let installed = replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap();

    assert_eq!(installed, manifest);
    assert_eq!(replica.current_serial().unwrap(), manifest.serial);
    assert_eq!(replica.get_driver_value("stale\u{0}row").unwrap(), None);
    assert_eq!(
        replica.get_driver_value(CURSOR_KEY).unwrap().as_deref(),
        Some(CURSOR_VALUE)
    );
    assert_eq!(replica.checkpoint().unwrap(), writer.checkpoint().unwrap());
}

/// The install is what a replica's own fold has to agree with afterwards, so the installed rows are
/// compared against the writer's live rows rather than against the encoding they arrived in.
#[test]
fn test_an_installed_checkpoint_holds_the_rows_the_writer_replicated() {
    let (writer, manifest) = published(3);
    let replica = store();
    transfer(&writer, &replica, &manifest, 4096);

    replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap();

    for index in 0..3 {
        let key = format!("pypi\u{0}p\u{0}hosted/pkg{index:04}");
        assert_eq!(
            replica.get_driver_value(&key).unwrap(),
            writer.get_driver_value(&key).unwrap(),
            "{key}"
        );
    }
    assert!(replica.has_active_digest_revocation().unwrap());
}

#[test]
fn test_a_checkpoint_installed_without_a_generation_is_republished_at_generation_one() {
    let (writer, published) = published(2);
    let manifest = CheckpointManifest {
        generation: 0,
        ..published
    };
    let replica = store();
    transfer(&writer, &replica, &manifest, 4096);
    replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap();

    let republished = replica.publish_checkpoint(identity()).unwrap();

    assert_eq!(
        republished,
        CheckpointManifest {
            generation: 1,
            ..manifest
        }
    );
}

fn recovering_replica(blobs: u64) -> (MetaStore, CheckpointManifest) {
    let writer = store();
    for index in 1..=blobs {
        commit(&writer, |txn| {
            txn.reference_blob(&format!("{index:064x}"), index);
            Ok(())
        });
    }
    let manifest = writer.publish_checkpoint(identity()).unwrap();
    let replica = store();
    replica
        .set_view_frontier(peryx_ha::AVAILABILITY_BLOB_VIEW, 999)
        .unwrap();
    transfer(&writer, &replica, &manifest, 4096);
    replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap();
    (replica, manifest)
}

#[test]
fn test_checkpoint_blob_recovery_advances_only_after_the_last_bounded_page() {
    let (replica, manifest) = recovering_replica(3);
    let limit = NonZeroUsize::new(2).unwrap();

    let first = replica.checkpoint_blob_recovery_page(limit).unwrap().unwrap();

    assert!(
        replica
            .advance_checkpoint_blob_recovery("wrong", first.next.as_deref(), peryx_ha::AVAILABILITY_BLOB_VIEW, 0)
            .is_err()
    );

    assert_eq!(first.references.len(), 2);
    assert!(first.next.is_some());
    assert_eq!(
        replica.view_frontier(peryx_ha::AVAILABILITY_BLOB_VIEW).unwrap(),
        Some(manifest.serial - 1)
    );
    replica
        .advance_checkpoint_blob_recovery(
            &first.after,
            first.next.as_deref(),
            peryx_ha::AVAILABILITY_BLOB_VIEW,
            first.serial,
        )
        .unwrap();
    let second = replica.checkpoint_blob_recovery_page(limit).unwrap().unwrap();
    assert_eq!(second.references.len(), 1);
    replica
        .advance_checkpoint_blob_recovery(
            &second.after,
            second.next.as_deref(),
            peryx_ha::AVAILABILITY_BLOB_VIEW,
            second.serial,
        )
        .unwrap();

    assert_eq!(replica.checkpoint_blob_recovery_page(limit).unwrap(), None);
    assert_eq!(
        replica.view_frontier(peryx_ha::AVAILABILITY_BLOB_VIEW).unwrap(),
        Some(manifest.serial)
    );
}

#[test]
fn test_checkpoint_blob_recovery_ends_on_an_exactly_full_page() {
    let (replica, _manifest) = recovering_replica(2);

    let page = replica
        .checkpoint_blob_recovery_page(NonZeroUsize::new(2).unwrap())
        .unwrap()
        .unwrap();

    assert_eq!(page.references.len(), 2);
    assert_eq!(page.next, None);
}

#[test]
fn test_checkpoint_blob_recovery_rejects_a_marker_without_a_manifest() {
    let store = store();
    let txn = store.db.begin_write().unwrap();
    txn.open_table(crate::meta::CHECKPOINT_BLOB_RECOVERY)
        .unwrap()
        .insert(super::BLOB_RECOVERY_KEY, "")
        .unwrap();
    txn.commit().unwrap();

    let error = store.checkpoint_blob_recovery_page(NonZeroUsize::MIN).unwrap_err();

    assert_eq!(
        error.to_string(),
        "checkpoint blob references are invalid: blob recovery has no checkpoint manifest"
    );
}

#[test]
fn test_generation_reads_reject_a_stale_generation_before_and_during_chunking() {
    let (writer, manifest) = published(1);
    let stale = manifest.generation + 1;

    assert!(
        writer
            .checkpoint_chunk_at_generation(stale, &CheckpointCursor::start(), 512)
            .is_err()
    );
    assert!(
        writer
            .checkpoint_chunk_from_generation(&CheckpointCursor::start(), 512, Some(stale))
            .is_err()
    );
}

#[test]
fn test_an_interrupted_transfer_resumes_from_what_it_staged() {
    let (writer, manifest) = published(20);
    let replica = store();
    replica.begin_checkpoint_transfer(&manifest).unwrap();
    let first = writer.checkpoint_chunk(&CheckpointCursor::start(), 256).unwrap();
    let staged = replica
        .stage_checkpoint_chunk(&manifest, 0, &first.bytes, &first.next.token())
        .unwrap()
        .unwrap();
    assert!(staged.received < manifest.bytes);

    let resumed = replica.staged_checkpoint().unwrap().unwrap();
    let mut offset = resumed.received;
    // The cursor comes back from the store rather than from the interrupted run, which is what a
    // restarted process has to work from.
    let mut cursor = CheckpointCursor::from_token(&resumed.cursor).unwrap();
    loop {
        let chunk = writer.checkpoint_chunk(&cursor, 256).unwrap();
        if !chunk.bytes.is_empty() {
            offset = replica
                .stage_checkpoint_chunk(&manifest, offset, &chunk.bytes, &chunk.next.token())
                .unwrap()
                .unwrap()
                .received;
        }
        if chunk.next == CheckpointCursor::Done {
            break;
        }
        cursor = chunk.next;
    }

    assert_eq!(
        (resumed.manifest, resumed.cursor),
        (manifest.clone(), first.next.token())
    );
    assert_eq!(
        replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap(),
        manifest
    );
}

#[test]
fn test_a_chunk_that_does_not_continue_the_transfer_is_refused() {
    let (writer, manifest) = published(20);
    let replica = store();
    replica.begin_checkpoint_transfer(&manifest).unwrap();
    let first = writer.checkpoint_chunk(&CheckpointCursor::start(), 256).unwrap();
    let staged = replica
        .stage_checkpoint_chunk(&manifest, 0, &first.bytes, &first.next.token())
        .unwrap()
        .unwrap();

    let refused = replica
        .stage_checkpoint_chunk(&manifest, 0, &first.bytes, &first.next.token())
        .unwrap();

    assert_eq!(
        refused,
        Err(CheckpointStageError::OutOfOrder {
            offset: 0,
            received: staged.received,
        })
    );
}

/// An install runs against what arrived, so a state that no longer hashes to its manifest is refused
/// with the live state untouched.
#[test]
fn test_a_corrupted_transfer_is_rejected_and_leaves_live_state_alone() {
    let (writer, manifest) = published(4);
    let replica = store();
    commit(&replica, |txn| txn.put("live\u{0}row", b"still here"));
    let before = replica.current_serial().unwrap();
    replica.begin_checkpoint_transfer(&manifest).unwrap();
    let mut cursor = CheckpointCursor::start();
    let mut offset = 0;
    loop {
        let chunk = writer.checkpoint_chunk(&cursor, 64).unwrap();
        let mut bytes = chunk.bytes;
        if chunk.next == CheckpointCursor::Done && !bytes.is_empty() {
            let last = bytes.len() - 1;
            bytes[last] ^= 0xff;
        }
        if !bytes.is_empty() {
            offset = replica
                .stage_checkpoint_chunk(&manifest, offset, &bytes, &chunk.next.token())
                .unwrap()
                .unwrap()
                .received;
        }
        if chunk.next == CheckpointCursor::Done {
            break;
        }
        cursor = chunk.next;
    }

    let refused = replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap_err();

    assert!(matches!(refused, CheckpointInstallError::Verify(_)), "{refused:?}");
    assert!(refused.to_string().contains("digest"), "{refused}");
    assert_eq!(
        replica.get_driver_value("live\u{0}row").unwrap().as_deref(),
        Some(&b"still here"[..])
    );
    assert_eq!(replica.current_serial().unwrap(), before);
}

#[test]
fn test_a_truncated_transfer_is_rejected_before_it_is_hashed() {
    let (writer, manifest) = published(4);
    let replica = store();
    replica.begin_checkpoint_transfer(&manifest).unwrap();
    let first = writer.checkpoint_chunk(&CheckpointCursor::start(), 128).unwrap();
    let staged = replica
        .stage_checkpoint_chunk(&manifest, 0, &first.bytes, &first.next.token())
        .unwrap()
        .unwrap();

    let refused = replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap_err();

    let expected = CheckpointInstallError::Incomplete {
        received: staged.received,
        declared: manifest.bytes,
    };
    assert_eq!(refused.to_string(), expected.to_string());
}

#[test]
fn test_an_install_without_a_staged_transfer_is_refused() {
    let replica = store();

    let refused = replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap_err();

    assert!(matches!(refused, CheckpointInstallError::NotStaged), "{refused:?}");
}

#[test]
fn test_a_cursor_survives_the_token_it_travels_as() {
    let keys = [
        CheckpointCursor::start(),
        CheckpointCursor::Rows {
            after: Some("pypi\u{0}p\u{0}hosted/flask".to_owned()),
        },
        CheckpointCursor::Revocations { after: None },
        CheckpointCursor::Revocations {
            after: Some("sha256:beef".to_owned()),
        },
        CheckpointCursor::Blobs { after: None },
        CheckpointCursor::Blobs {
            after: Some("f00d".to_owned()),
        },
        CheckpointCursor::Done,
    ];

    for cursor in keys {
        assert_eq!(
            CheckpointCursor::from_token(&cursor.token()),
            Some(cursor.clone()),
            "{cursor:?}"
        );
    }
    assert_eq!(CheckpointCursor::from_token("zzz"), None);
    assert_eq!(CheckpointCursor::from_token("r:nothex"), None);
}

#[test]
fn test_a_transfer_pin_keeps_publication_on_one_generation() {
    let (writer, now) = stepped_store();
    commit(&writer, |txn| txn.put("row/one", b"one"));
    let first_manifest = writer.publish_checkpoint(identity()).unwrap();
    writer
        .checkpoint_chunk_at_generation(first_manifest.generation, &CheckpointCursor::start(), 1)
        .unwrap();
    commit(&writer, |txn| txn.put("row/two", b"two"));

    let pinned = writer.publish_checkpoint(identity()).unwrap();

    assert_eq!(pinned, first_manifest);
    now.store(31, Ordering::Relaxed);
    let next = writer.publish_checkpoint(identity()).unwrap();
    assert_eq!(next.generation, first_manifest.generation + 1);
    assert!(next.serial > first_manifest.serial);
}

#[test]
fn test_each_chunk_renews_the_generation_lease() {
    let (writer, now) = stepped_store();
    commit(&writer, |txn| txn.put("row/one", b"one"));
    let manifest = writer.publish_checkpoint(identity()).unwrap();
    writer
        .checkpoint_chunk_at_generation(manifest.generation, &CheckpointCursor::start(), 1)
        .unwrap();
    now.store(20, Ordering::Relaxed);
    writer
        .checkpoint_chunk_at_generation(manifest.generation, &CheckpointCursor::start(), 1)
        .unwrap();
    now.store(40, Ordering::Relaxed);

    assert_eq!(writer.publish_checkpoint(identity()).unwrap(), manifest);
}

#[test]
fn test_a_generation_expires_at_the_maximum_transfer_lifetime() {
    let (writer, now) = stepped_store();
    commit(&writer, |txn| txn.put("row/one", b"one"));
    let manifest = writer.publish_checkpoint(identity()).unwrap();
    writer
        .checkpoint_chunk_at_generation(manifest.generation, &CheckpointCursor::start(), 1)
        .unwrap();
    for tick in [20, 100, 200, 299] {
        now.store(tick, Ordering::Relaxed);
        writer
            .checkpoint_chunk_at_generation(manifest.generation, &CheckpointCursor::start(), 1)
            .unwrap();
    }
    now.store(300, Ordering::Relaxed);

    let expired = writer
        .checkpoint_chunk_at_generation(manifest.generation, &CheckpointCursor::start(), 1)
        .unwrap_err();

    assert_eq!(
        expired.to_string(),
        format!(
            "checkpoint generation {} exceeded its transfer lifetime",
            manifest.generation
        )
    );
    let replacement = writer.publish_checkpoint(identity()).unwrap();
    assert_eq!(
        (replacement.generation, replacement.serial),
        (manifest.generation + 1, manifest.serial)
    );
    writer
        .checkpoint_chunk_at_generation(replacement.generation, &CheckpointCursor::start(), 1)
        .unwrap();
}

#[test]
fn test_a_journal_floor_starts_at_the_first_serial() {
    let store = store();
    assert_eq!(store.journal_floor().unwrap(), Some(1));

    commit(&store, |txn| txn.put("a", b"1"));
    commit(&store, |txn| txn.put("b", b"2"));

    assert_eq!(store.journal_floor().unwrap(), Some(1));
}

/// A store that has published nothing serves an empty transfer rather than failing, which is what a
/// reader gets before the first publication.
#[test]
fn test_a_store_with_no_published_checkpoint_serves_an_empty_transfer() {
    let store = store();

    let chunk = store.checkpoint_chunk(&CheckpointCursor::start(), 512).unwrap();

    assert_eq!((chunk.bytes, chunk.next), (Vec::new(), CheckpointCursor::Done));
}

#[test]
fn test_a_finished_cursor_asks_for_nothing_more() {
    let (writer, _manifest) = published(4);

    let chunk = writer.checkpoint_chunk(&CheckpointCursor::Done, 512).unwrap();

    assert_eq!((chunk.bytes, chunk.next), (Vec::new(), CheckpointCursor::Done));
}

/// The blob section fills a window the same way the rows do, so a checkpoint whose blobs alone exceed
/// the budget still travels.
#[test]
fn test_a_blob_section_larger_than_the_budget_is_split() {
    let store = store();
    for index in 0..40 {
        commit(&store, |txn| {
            txn.reference_blob(&format!("{index:064x}"), index);
            Ok(())
        });
    }
    let manifest = store.publish_checkpoint(identity()).unwrap();

    let chunk = store.checkpoint_chunk(&CheckpointCursor::start(), 256).unwrap();

    assert!(
        matches!(chunk.next, CheckpointCursor::Blobs { after: Some(_) }),
        "{:?}",
        chunk.next
    );
    assert!((chunk.bytes.len() as u64) < manifest.bytes);
}

#[test]
fn test_a_chunk_past_the_declared_length_is_refused() {
    let (writer, manifest) = published(4);
    let replica = store();
    replica.begin_checkpoint_transfer(&manifest).unwrap();
    let whole = writer.checkpoint_chunk(&CheckpointCursor::start(), 1 << 20).unwrap();

    let refused = replica
        .stage_checkpoint_chunk(&manifest, 0, &[whole.bytes.clone(), whole.bytes].concat(), "done")
        .unwrap();

    assert!(
        matches!(refused, Err(CheckpointStageError::Overrun { .. })),
        "{refused:?}"
    );
}

/// The counts are checked before the digest, so a manifest that disagrees about what it names fails on
/// the count rather than on a hash over bytes that were never in question.
#[test]
fn test_a_manifest_disagreeing_about_its_counts_is_refused_before_the_digest() {
    let (writer, manifest) = published(4);
    let replica = store();
    let claimed = CheckpointManifest {
        rows: manifest.rows + 1,
        ..manifest
    };
    replica.begin_checkpoint_transfer(&claimed).unwrap();
    let whole = writer.checkpoint_chunk(&CheckpointCursor::start(), 1 << 20).unwrap();
    replica
        .stage_checkpoint_chunk(&claimed, 0, &whole.bytes, "done")
        .unwrap()
        .unwrap();

    let refused = replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap_err();

    assert!(matches!(refused, CheckpointInstallError::Verify(_)), "{refused:?}");
    assert!(refused.to_string().contains("rows"), "{refused}");
}

#[test]
fn test_discarding_a_transfer_leaves_nothing_staged() {
    let (writer, manifest) = published(4);
    let replica = store();
    transfer(&writer, &replica, &manifest, 512);

    replica.discard_staged_checkpoint().unwrap();

    assert_eq!(replica.staged_checkpoint().unwrap(), None);
}

/// A window boundary can land inside an entry, so the decoder has to hold the part it has and finish it
/// with the bytes that follow. Feeding the same encoding one byte at a time is the worst such split.
#[test]
fn test_the_decoder_reads_entries_split_across_the_bytes_it_is_fed() {
    let (writer, manifest) = published(3);
    let whole = writer
        .checkpoint_chunk(&CheckpointCursor::start(), 1 << 20)
        .unwrap()
        .bytes;
    let replica = store();
    replica.begin_checkpoint_transfer(&manifest).unwrap();

    let mut offset = 0;
    for byte in &whole {
        offset = replica
            .stage_checkpoint_chunk(&manifest, offset, std::slice::from_ref(byte), "done")
            .unwrap()
            .unwrap()
            .received;
    }

    assert_eq!(
        replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap(),
        manifest
    );
}

/// Bytes that stop part-way through an entry are not a checkpoint, whatever their length says.
#[test]
fn test_bytes_that_end_inside_an_entry_are_refused_as_malformed() {
    let writer = store();
    commit(&writer, |txn| txn.put("pypi\u{0}p\u{0}hosted/pkg", b"display"));
    let manifest = writer.publish_checkpoint(identity()).unwrap();
    let whole = writer
        .checkpoint_chunk(&CheckpointCursor::start(), 1 << 20)
        .unwrap()
        .bytes;
    let replica = store();
    let truncated = CheckpointManifest {
        bytes: whole.len() as u64 - 1,
        ..manifest
    };
    replica.begin_checkpoint_transfer(&truncated).unwrap();
    replica
        .stage_checkpoint_chunk(&truncated, 0, &whole[..whole.len() - 1], "done")
        .unwrap()
        .unwrap();

    let refused = replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap_err();

    assert!(
        matches!(refused, CheckpointInstallError::Malformed { .. }),
        "{refused:?}"
    );
}

/// One entry of the canonical encoding: a tag, then a length-prefixed key and value.
fn entry(tag: u8, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut frame = vec![tag];
    for field in [key, value] {
        frame.extend_from_slice(&(field.len() as u64).to_le_bytes());
        frame.extend_from_slice(field);
    }
    frame
}

/// The encoder writes every key as UTF-8 and every revocation as its own JSON, so an entry that
/// arrives otherwise is damage. Refusing the install is what keeps a row from landing under a key the
/// reader guessed at, and a revocation the reader could not parse from being dropped silently.
#[rstest]
#[case::key_is_not_utf8(entry(b'r', &[0xff, 0xfe], b"row"))]
#[case::revocation_is_not_json(entry(b'v', b"sha256:abc", b"not json"))]
fn test_a_damaged_entry_is_refused_as_malformed(#[case] frame: Vec<u8>) {
    let replica = store();
    let manifest = CheckpointManifest {
        generation: 1,
        identity: identity(),
        serial: 1,
        rows: 0,
        revocations: 0,
        blobs: 0,
        bytes: frame.len() as u64,
        digest: String::new(),
    };
    replica.begin_checkpoint_transfer(&manifest).unwrap();
    replica
        .stage_checkpoint_chunk(&manifest, 0, &frame, "done")
        .unwrap()
        .unwrap();

    let refused = replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap_err();

    assert!(
        matches!(refused, CheckpointInstallError::Malformed { offset: 0 }),
        "{refused:?}"
    );
}

/// A tag the encoding never writes is damage rather than a section this reader does not know.
#[test]
fn test_an_unknown_entry_tag_is_refused_as_malformed() {
    let replica = store();
    let manifest = CheckpointManifest {
        generation: 1,
        identity: identity(),
        serial: 1,
        rows: 0,
        revocations: 0,
        blobs: 0,
        bytes: 1,
        digest: String::new(),
    };
    replica.begin_checkpoint_transfer(&manifest).unwrap();
    replica
        .stage_checkpoint_chunk(&manifest, 0, b"z", "done")
        .unwrap()
        .unwrap();

    let refused = replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap_err();

    assert!(
        matches!(refused, CheckpointInstallError::Malformed { offset: 0 }),
        "{refused:?}"
    );
}

/// A source with no replicated rows publishes a checkpoint that encodes to nothing, and a transfer of
/// it stages no window at all. Installing that is still an install: the replica ends at the source's
/// serial holding the empty state rather than refusing a transfer that carried everything it had.
#[test]
fn test_a_checkpoint_that_encodes_to_nothing_still_installs() {
    let writer = store();
    commit(&writer, |_| Ok(()));
    let manifest = writer.publish_checkpoint(identity()).unwrap();
    assert_eq!(manifest.bytes, 0);
    let replica = store();
    commit(&replica, |txn| txn.put("stale\u{0}row", b"from a life before"));
    replica.begin_checkpoint_transfer(&manifest).unwrap();

    let installed = replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap();

    assert_eq!(installed, manifest);
    assert_eq!(replica.current_serial().unwrap(), manifest.serial);
    assert_eq!(replica.get_driver_value("stale\u{0}row").unwrap(), None);
}

/// The offset a malformed error reports must track the exact bytes already decoded, so an operator
/// diagnosing corruption is pointed at the damaged entry rather than the start of the transfer.
#[test]
fn test_a_malformed_offset_counts_the_entries_decoded_before_it() {
    let first = entry(ROW_TAG, b"key1", b"value1");
    let second = entry(ROW_TAG, b"key2", b"value2");
    let expected_offset = (first.len() + second.len()) as u64;
    let mut frame = first;
    frame.extend_from_slice(&second);
    frame.push(b'z');
    let replica = store();
    let manifest = CheckpointManifest {
        generation: 1,
        identity: identity(),
        serial: 1,
        rows: 0,
        revocations: 0,
        blobs: 0,
        bytes: frame.len() as u64,
        digest: String::new(),
    };
    replica.begin_checkpoint_transfer(&manifest).unwrap();
    replica
        .stage_checkpoint_chunk(&manifest, 0, &frame, "done")
        .unwrap()
        .unwrap();

    let refused = replica.install_staged_checkpoint(CURSOR_KEY, CURSOR_VALUE).unwrap_err();

    assert!(
        matches!(refused, CheckpointInstallError::Malformed { offset } if offset == expected_offset),
        "{refused:?}"
    );
}

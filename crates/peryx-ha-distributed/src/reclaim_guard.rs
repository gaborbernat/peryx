use std::collections::BTreeSet;
use std::convert::Infallible;
use std::path::PathBuf;

use peryx_ha::{ReclaimGuard, ReclaimGuardArm, ReclaimGuardStore};
use peryx_storage::blob::{BlobStorage, Digest};
use peryx_storage::meta::{MetaError, MetaStore};

pub const RECLAIM_GUARD_LEASE_SECS: i64 = 300;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanBlob {
    pub digest: String,
    pub bytes: u64,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanPurgeReport {
    pub blobs: Vec<OrphanBlob>,
    pub bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum OrphanPurgeError {
    #[error("scan metadata blob references: {0}")]
    References(String),
    #[error("{operation}: {reason}")]
    Blob { operation: &'static str, reason: String },
    #[error(transparent)]
    Store(#[from] MetaError),
}

/// # Errors
/// Returns metadata, blob, or reference-scan failures.
pub fn purge_orphaned_blobs(
    meta: &MetaStore,
    blobs: &BlobStorage,
    confirmed: bool,
    now: i64,
    scan_references: impl FnMut() -> Result<BTreeSet<String>, String>,
) -> Result<OrphanPurgeReport, OrphanPurgeError> {
    purge_orphaned_blobs_with_delete(meta, blobs, confirmed, now, scan_references, |digest| {
        blobs
            .blocking()
            .delete(digest)
            .map(|_| ())
            .map_err(|error| error.to_string())
    })
}

fn purge_orphaned_blobs_with_delete(
    meta: &MetaStore,
    blobs: &BlobStorage,
    confirmed: bool,
    now: i64,
    mut scan_references: impl FnMut() -> Result<BTreeSet<String>, String>,
    mut delete: impl FnMut(&Digest) -> Result<(), String>,
) -> Result<OrphanPurgeReport, OrphanPurgeError> {
    let _ownership = confirmed.then(|| meta.reclaim_ownership());
    let live_digests = scan_references().map_err(OrphanPurgeError::References)?;
    let candidates = orphan_candidates(blobs, &live_digests)?;
    if !confirmed {
        let live_digests = scan_references().map_err(OrphanPurgeError::References)?;
        return Ok(report(
            candidates
                .into_iter()
                .filter(|candidate| !live_digests.contains(candidate.digest.as_str())),
        ));
    }

    release_expired_guards(meta, now)?;
    let guard = ReclaimGuard {
        expires_at_unix: now.saturating_add(RECLAIM_GUARD_LEASE_SECS),
    };
    // A reference commit between the scan and the arm moves the revision, which retires the scan
    // rather than guarding a digest that is no longer orphaned.
    let armed = loop {
        let revision = meta.reference_revision()?;
        let live_digests = scan_references().map_err(OrphanPurgeError::References)?;
        let digests = candidates
            .iter()
            .filter(|candidate| !live_digests.contains(candidate.digest.as_str()))
            .map(|candidate| candidate.digest.as_str())
            .collect::<Vec<_>>();
        let ReclaimGuardArm::Armed(armed) =
            meta.compare_and_arm_reclaim_guards_and_remove_placements(&digests, revision, now, guard)?
        else {
            continue;
        };
        break armed.into_iter().collect::<BTreeSet<_>>();
    };
    let selected = candidates
        .into_iter()
        .filter(|candidate| armed.contains(candidate.digest.as_str()))
        .collect::<Vec<_>>();
    for candidate in &selected {
        if let Err(reason) = delete(&candidate.digest) {
            return Err(OrphanPurgeError::Blob {
                operation: "delete orphaned blob",
                reason,
            });
        }
        meta.compare_and_disarm_reclaim_guard(candidate.digest.as_str(), guard)?;
    }
    Ok(report(selected))
}

/// A lapsed lease proves its collector no longer holds the blob, whether or not the bytes survived
/// the purge that armed it, so the row goes regardless of what the store reports.
fn release_expired_guards(meta: &MetaStore, now: i64) -> Result<(), OrphanPurgeError> {
    for (digest, guard) in meta.reclaim_guards()? {
        if guard.is_expired_at(now) {
            meta.compare_and_disarm_reclaim_guard(&digest, guard)?;
        }
    }
    Ok(())
}

fn orphan_candidates(blobs: &BlobStorage, referenced: &BTreeSet<String>) -> Result<Vec<Candidate>, OrphanPurgeError> {
    let mut candidates = Vec::new();
    blobs
        .blocking()
        .visit(|entry| {
            if let Some(digest) = entry.digest
                && !referenced.contains(digest.as_str())
            {
                candidates.push(Candidate {
                    digest,
                    bytes: entry.bytes,
                    path: entry.path,
                });
            }
            Ok::<(), Infallible>(())
        })
        .map_err(|error| OrphanPurgeError::Blob {
            operation: "scan orphaned blob files",
            reason: error.to_string(),
        })?;
    Ok(candidates)
}

fn report(candidates: impl IntoIterator<Item = Candidate>) -> OrphanPurgeReport {
    let blobs = candidates
        .into_iter()
        .map(|candidate| OrphanBlob {
            digest: candidate.digest.as_str().to_owned(),
            bytes: candidate.bytes,
            path: candidate.path,
        })
        .collect::<Vec<_>>();
    OrphanPurgeReport {
        bytes: blobs.iter().map(|blob| blob.bytes).sum(),
        blobs,
    }
}

struct Candidate {
    digest: Digest,
    bytes: u64,
    path: PathBuf,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::{Arc, Barrier};

    use peryx_storage::repair_artifact_placements;

    use super::*;

    #[test]
    fn active_purge_keeps_an_expired_guard_through_deletion() {
        let directory = tempfile::tempdir().unwrap();
        let ticks = Arc::new(AtomicI64::new(10));
        let clock = Arc::clone(&ticks);
        let meta = MetaStore::open(directory.path().join("peryx.redb"))
            .unwrap()
            .with_clock(Arc::new(move || clock.load(Ordering::Relaxed)));
        let blobs = BlobStorage::filesystem(directory.path().join("blobs"));
        let orphan = blobs.blocking().put_bytes(b"orphan").unwrap();
        let paused = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        std::thread::scope(|scope| {
            let purge_meta = &meta;
            let purge_blobs = &blobs;
            let pause_delete = Arc::clone(&paused);
            let resume_delete = Arc::clone(&resume);
            let purge = scope.spawn(move || {
                purge_orphaned_blobs_with_delete(
                    purge_meta,
                    purge_blobs,
                    true,
                    10,
                    || Ok(BTreeSet::new()),
                    |digest| {
                        pause_delete.wait();
                        resume_delete.wait();
                        purge_blobs.blocking().delete(digest).unwrap();
                        Ok(())
                    },
                )
            });
            paused.wait();

            ticks.store(10 + RECLAIM_GUARD_LEASE_SECS, Ordering::Relaxed);
            let error = meta
                .commit_driver_txn(|txn| {
                    txn.reference_blob(orphan.as_str(), 6);
                    Ok::<_, MetaError>(((), vec![b"{}".to_vec()]))
                })
                .unwrap_err();
            let repair = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(repair_artifact_placements(&meta, &blobs, 1))
                .unwrap();

            assert!(matches!(error, MetaError::BlobReclaiming { digest } if digest == orphan.as_str()));
            assert_eq!(repair.content.skipped, 1);
            assert_eq!(meta.get_artifact_placement(orphan.as_str()).unwrap(), None);
            resume.wait();
            assert_eq!(purge.join().unwrap().unwrap().blobs.len(), 1);
        });

        assert!(blobs.blocking().head(&orphan).unwrap().is_none());
        assert_eq!(meta.reclaim_guard(orphan.as_str()).unwrap(), None);
        assert_eq!(meta.get_artifact_placement(orphan.as_str()).unwrap(), None);
    }
}

//! Serving a published checkpoint in chunks, and installing one a replica received.
//!
//! A checkpoint is the whole replicated state, which reaches a gigabyte on a mirror with a million
//! rows, so neither side may hold one whole. The writer encodes it a bounded window at a time from the
//! tables [`publish_checkpoint`](super::MetaStore::publish_checkpoint) wrote, and the receiver stages
//! what arrives in the store rather than in memory. Staging survives a restart, so an install
//! interrupted at a chunk boundary resumes instead of starting over.
//!
//! The window is row-aligned and the writer names where the next one begins, because a byte offset
//! alone would make the writer re-encode from the start to find it, which is quadratic over the
//! transfer. The offset travels beside that name so the receiver can refuse a chunk that does not
//! continue what it already holds, and so `manifest.bytes` bounds progress.
//!
//! Nothing here removes a journal row or advances a floor.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::ops::Bound::{Excluded, Unbounded};

use redb::ReadableTable as _;
use sha2::{Digest as _, Sha256};

use super::checkpoint::{CheckpointManifest, CheckpointVerifyError};
use super::error::MetaError;
use super::journal::DriverBlobReference;
use super::revocation::DigestRevocation;
use super::{
    CHECKPOINT_BLOB, CHECKPOINT_BLOB_RECOVERY, CHECKPOINT_META, CHECKPOINT_PIN, CHECKPOINT_REVOCATION, CHECKPOINT_ROW,
    CHECKPOINT_STAGING, CHECKPOINT_STAGING_META, DERIVED_VIEW_FRONTIER, DRIVER_KV, JOURNAL, JOURNAL_BLOBS,
    JOURNAL_MUTATIONS, MetaStore, SERIAL, SERIAL_KEY, open_optional_table,
};

/// Names the single staged-manifest row, which one transfer replaces whole.
const STAGED_MANIFEST_KEY: &str = "staged";

/// The tag each canonical entry opens with, in the order the encoding lays them out.
const ROW_TAG: u8 = b'r';
const REVOCATION_TAG: u8 = b'v';
const BLOB_TAG: u8 = b'b';
pub(super) const PIN_KEY: &str = "active";
const PIN_LEASE_SECS: i64 = 30;
const PIN_MAX_LIFETIME_SECS: i64 = 300;
const BLOB_RECOVERY_KEY: &str = "cursor";

#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct CheckpointPin {
    pub generation: u64,
    pub started_at: i64,
    pub expires_at: i64,
}

/// Where the next chunk of a checkpoint transfer begins.
///
/// A cursor names an entry rather than a byte, so the writer seeks to it through the table's own
/// ordering. `Done` is what a receiver reads as the end of the transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointCursor {
    Rows { after: Option<String> },
    Revocations { after: Option<String> },
    Blobs { after: Option<String> },
    Done,
}

impl CheckpointCursor {
    /// The cursor a transfer opens with.
    #[must_use]
    pub const fn start() -> Self {
        Self::Rows { after: None }
    }

    /// A cursor as a token safe to carry in a URL, with the key hex-encoded because a driver key holds
    /// arbitrary bytes.
    #[must_use]
    pub fn token(&self) -> String {
        let (tag, after) = match self {
            Self::Rows { after } => ('r', after),
            Self::Revocations { after } => ('v', after),
            Self::Blobs { after } => ('b', after),
            Self::Done => return "done".to_owned(),
        };
        after
            .as_ref()
            .map_or_else(|| format!("{tag}"), |key| format!("{tag}:{}", hex::encode(key)))
    }

    #[must_use]
    pub fn generation_token(&self, generation: u64) -> String {
        format!("g{generation}:{}", self.token())
    }

    /// Reads back a token, or `None` when it names no position this writer can resume from.
    #[must_use]
    pub fn from_token(token: &str) -> Option<Self> {
        if token == "done" {
            return Some(Self::Done);
        }
        let (tag, after) = match token.split_once(':') {
            Some((tag, key)) => {
                let decoded = String::from_utf8(hex::decode(key).ok()?).ok()?;
                (tag, Some(decoded))
            }
            None => (token, None),
        };
        match tag {
            "r" => Some(Self::Rows { after }),
            "v" => Some(Self::Revocations { after }),
            "b" => Some(Self::Blobs { after }),
            _ => None,
        }
    }

    #[must_use]
    pub fn from_generation_token(token: &str) -> Option<(u64, Self)> {
        let (generation, cursor) = token.strip_prefix('g')?.split_once(':')?;
        Some((generation.parse().ok()?, Self::from_token(cursor)?))
    }
}

/// One window of a checkpoint's canonical encoding, and where the window after it begins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointChunk {
    pub bytes: Vec<u8>,
    pub next: CheckpointCursor,
}

/// What a receiver holds part-way through a transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedCheckpoint {
    pub manifest: CheckpointManifest,
    /// Canonical bytes staged so far, which is where the next chunk has to start.
    pub received: u64,
    /// The token naming where the writer continues, so a restart resumes rather than refetching.
    pub cursor: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointBlobPage {
    pub serial: u64,
    pub after: String,
    pub references: Vec<DriverBlobReference>,
    pub next: Option<String>,
}

/// What the staging row holds beside the bytes.
#[derive(serde::Serialize, serde::Deserialize)]
struct StagedHeader {
    manifest: CheckpointManifest,
    cursor: String,
}

/// Why a staged checkpoint cannot become the live state.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointInstallError {
    #[error("no checkpoint is staged")]
    NotStaged,
    #[error("staged checkpoint holds {received} of {declared} bytes")]
    Incomplete { received: u64, declared: u64 },
    #[error(transparent)]
    Verify(#[from] CheckpointVerifyError),
    #[error("staged checkpoint is not canonical at byte {offset}")]
    Malformed { offset: u64 },
    #[error(transparent)]
    Store(#[from] MetaError),
}

/// Why a chunk cannot join what is already staged.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CheckpointStageError {
    #[error("chunk starts at {offset} and the staged transfer holds {received} bytes")]
    OutOfOrder { offset: u64, received: u64 },
    #[error("chunk carries {received} bytes past the {declared} its manifest declares")]
    Overrun { received: u64, declared: u64 },
}

impl MetaStore {
    /// Returns the next bounded page of checkpoint blobs an installed replica must recover.
    ///
    /// # Errors
    /// Returns a store or manifest decode error.
    pub fn checkpoint_blob_recovery_page(&self, limit: NonZeroUsize) -> Result<Option<CheckpointBlobPage>, MetaError> {
        let txn = self.db.begin_read()?;
        let Some(recovery) = open_optional_table(&txn, CHECKPOINT_BLOB_RECOVERY)? else {
            return Ok(None);
        };
        let Some(after) = recovery.get(BLOB_RECOVERY_KEY)?.map(|value| value.value().to_owned()) else {
            return Ok(None);
        };
        let manifest = txn
            .open_table(CHECKPOINT_META)?
            .get(super::checkpoint::MANIFEST_KEY)?
            .map(|value| serde_json::from_slice::<CheckpointManifest>(value.value()))
            .transpose()?
            .ok_or_else(|| MetaError::CheckpointReferences("blob recovery has no checkpoint manifest".to_owned()))?;
        let table = txn.open_table(CHECKPOINT_BLOB)?;
        let mut references = table
            .range::<&str>((bound((!after.is_empty()).then_some(after.as_str())), Unbounded))?
            .take(limit.get().saturating_add(1))
            .map(|entry| {
                entry.map(|(digest, size)| DriverBlobReference {
                    sha256: digest.value().to_owned(),
                    size: size.value(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let next = (references.len() > limit.get()).then(|| references[limit.get() - 1].sha256.clone());
        references.truncate(limit.get());
        Ok(Some(CheckpointBlobPage {
            serial: manifest.serial,
            after,
            references,
            next,
        }))
    }

    /// Commits one recovered checkpoint-blob page and advances `view` only after the final page.
    ///
    /// # Errors
    /// Returns a store error or a precondition failure when another worker moved the cursor.
    pub fn advance_checkpoint_blob_recovery(
        &self,
        expected_after: &str,
        next: Option<&str>,
        view: &str,
        serial: u64,
    ) -> Result<(), MetaError> {
        let txn = self.db.begin_write()?;
        let mut recovery = txn.open_table(CHECKPOINT_BLOB_RECOVERY)?;
        let actual = recovery.get(BLOB_RECOVERY_KEY)?.map(|cursor| cursor.value().to_owned());
        if actual.as_deref() != Some(expected_after) {
            return Err(MetaError::DriverPrecondition(
                "checkpoint blob recovery cursor moved".to_owned(),
            ));
        }
        if let Some(next) = next {
            recovery.insert(BLOB_RECOVERY_KEY, next)?;
        } else {
            recovery.remove(BLOB_RECOVERY_KEY)?;
            txn.open_table(DERIVED_VIEW_FRONTIER)?.insert(view, serial)?;
        }
        drop(recovery);
        txn.commit()?;
        Ok(())
    }

    /// Renews the bounded lease for `generation` and serves one stable chunk.
    ///
    /// # Errors
    /// Returns a store error, or rejects stale and over-lifetime generations.
    pub fn checkpoint_chunk_at_generation(
        &self,
        generation: u64,
        cursor: &CheckpointCursor,
        budget: usize,
    ) -> Result<CheckpointChunk, MetaError> {
        let now = (self.clock)();
        let txn = self.db.begin_write()?;
        let current = txn
            .open_table(CHECKPOINT_META)?
            .get(super::checkpoint::MANIFEST_KEY)?
            .map(|value| serde_json::from_slice::<CheckpointManifest>(value.value()))
            .transpose()?
            .map_or(0, |manifest| manifest.generation);
        if generation != current {
            return Err(MetaError::StaleCheckpointGeneration {
                requested: generation,
                current,
            });
        }
        let previous = txn
            .open_table(CHECKPOINT_PIN)?
            .get(PIN_KEY)?
            .map(|value| serde_json::from_slice::<CheckpointPin>(value.value()))
            .transpose()?;
        let started_at = previous
            .filter(|pin| pin.generation == generation)
            .map_or(now, |pin| pin.started_at);
        let deadline = started_at.saturating_add(PIN_MAX_LIFETIME_SECS);
        if now >= deadline {
            return Err(MetaError::CheckpointGenerationExpired { generation });
        }
        let pin = CheckpointPin {
            generation,
            started_at,
            expires_at: now.saturating_add(PIN_LEASE_SECS).min(deadline),
        };
        txn.open_table(CHECKPOINT_PIN)?
            .insert(PIN_KEY, serde_json::to_vec(&pin)?.as_slice())?;
        txn.commit()?;
        self.checkpoint_chunk_from_generation(cursor, budget, Some(generation))
    }

    /// Encodes at most `budget` bytes of the published checkpoint from `cursor`.
    ///
    /// The window ends on an entry boundary, so one entry larger than the budget is still served whole
    /// rather than stalling the transfer.
    ///
    /// # Errors
    /// Returns a store error if the read fails, or a decode error for a malformed stored revocation.
    pub fn checkpoint_chunk(&self, cursor: &CheckpointCursor, budget: usize) -> Result<CheckpointChunk, MetaError> {
        self.checkpoint_chunk_from_generation(cursor, budget, None)
    }

    fn checkpoint_chunk_from_generation(
        &self,
        cursor: &CheckpointCursor,
        budget: usize,
        generation: Option<u64>,
    ) -> Result<CheckpointChunk, MetaError> {
        let txn = self.db.begin_read()?;
        if let Some(generation) = generation {
            let current = txn
                .open_table(CHECKPOINT_META)?
                .get(super::checkpoint::MANIFEST_KEY)?
                .map(|value| serde_json::from_slice::<CheckpointManifest>(value.value()))
                .transpose()?
                .map_or(0, |manifest| manifest.generation);
            if generation != current {
                return Err(MetaError::StaleCheckpointGeneration {
                    requested: generation,
                    current,
                });
            }
        }
        let mut bytes = Vec::new();
        let mut cursor = cursor.clone();
        loop {
            cursor = match cursor {
                CheckpointCursor::Done => return Ok(CheckpointChunk { bytes, next: cursor }),
                CheckpointCursor::Rows { after } => {
                    let mut stopped = None;
                    if let Some(table) = open_optional_table(&txn, CHECKPOINT_ROW)? {
                        for entry in table.range::<&str>((bound(after.as_deref()), Unbounded))? {
                            let (key, value) = entry?;
                            push_field(&mut bytes, ROW_TAG, key.value().as_bytes());
                            push_bytes(&mut bytes, value.value());
                            if bytes.len() >= budget {
                                stopped = Some(key.value().to_owned());
                                break;
                            }
                        }
                    }
                    match stopped {
                        Some(after) => {
                            return Ok(CheckpointChunk {
                                bytes,
                                next: CheckpointCursor::Rows { after: Some(after) },
                            });
                        }
                        None => CheckpointCursor::Revocations { after: None },
                    }
                }
                CheckpointCursor::Revocations { after } => {
                    let mut stopped = None;
                    if let Some(table) = open_optional_table(&txn, CHECKPOINT_REVOCATION)? {
                        for entry in table.range::<&str>((bound(after.as_deref()), Unbounded))? {
                            let (digest, record) = entry?;
                            push_field(&mut bytes, REVOCATION_TAG, digest.value().as_bytes());
                            push_bytes(&mut bytes, record.value());
                            if bytes.len() >= budget {
                                stopped = Some(digest.value().to_owned());
                                break;
                            }
                        }
                    }
                    match stopped {
                        Some(after) => {
                            return Ok(CheckpointChunk {
                                bytes,
                                next: CheckpointCursor::Revocations { after: Some(after) },
                            });
                        }
                        None => CheckpointCursor::Blobs { after: None },
                    }
                }
                CheckpointCursor::Blobs { after } => {
                    let mut stopped = None;
                    if let Some(table) = open_optional_table(&txn, CHECKPOINT_BLOB)? {
                        for entry in table.range::<&str>((bound(after.as_deref()), Unbounded))? {
                            let (sha256, size) = entry?;
                            push_field(&mut bytes, BLOB_TAG, sha256.value().as_bytes());
                            bytes.extend_from_slice(&size.value().to_le_bytes());
                            if bytes.len() >= budget {
                                stopped = Some(sha256.value().to_owned());
                                break;
                            }
                        }
                    }
                    return Ok(CheckpointChunk {
                        next: stopped.map_or(CheckpointCursor::Done, |after| CheckpointCursor::Blobs {
                            after: Some(after),
                        }),
                        bytes,
                    });
                }
            };
        }
    }

    /// Opens a transfer for `manifest`, dropping whatever a previous one staged.
    ///
    /// A transfer is recorded before its first byte arrives, so a checkpoint that encodes to nothing is
    /// still a transfer that can be installed, and so restarting one is a single explicit step rather
    /// than a condition inferred at each chunk.
    ///
    /// # Errors
    /// Returns a store error if the write fails.
    pub fn begin_checkpoint_transfer(&self, manifest: &CheckpointManifest) -> Result<(), MetaError> {
        let txn = self.db.begin_write()?;
        txn.delete_table(CHECKPOINT_STAGING)?;
        {
            let header = StagedHeader {
                manifest: manifest.clone(),
                cursor: CheckpointCursor::start().generation_token(manifest.generation),
            };
            txn.open_table(CHECKPOINT_STAGING_META)?
                .insert(STAGED_MANIFEST_KEY, serde_json::to_vec(&header)?.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Appends `bytes` at `offset` to the staged transfer for `manifest`.
    ///
    /// A chunk that does not continue what is staged is refused rather than written at a gap, and one
    /// that would carry the transfer past the length its manifest declares is refused with it.
    ///
    /// # Errors
    /// Returns a stage error when the chunk does not continue the transfer, or a store error.
    pub fn stage_checkpoint_chunk(
        &self,
        manifest: &CheckpointManifest,
        offset: u64,
        bytes: &[u8],
        cursor: &str,
    ) -> Result<Result<StagedCheckpoint, CheckpointStageError>, MetaError> {
        let staged = self.staged_checkpoint()?.filter(|staged| &staged.manifest == manifest);
        let received = staged.map_or(0, |staged| staged.received);
        if received != offset {
            return Ok(Err(CheckpointStageError::OutOfOrder { offset, received }));
        }
        let total = received.saturating_add(bytes.len() as u64);
        if total > manifest.bytes {
            return Ok(Err(CheckpointStageError::Overrun {
                received: total,
                declared: manifest.bytes,
            }));
        }
        let txn = self.db.begin_write()?;
        {
            txn.open_table(CHECKPOINT_STAGING)?.insert(offset, bytes)?;
            let header = StagedHeader {
                manifest: manifest.clone(),
                cursor: cursor.to_owned(),
            };
            txn.open_table(CHECKPOINT_STAGING_META)?
                .insert(STAGED_MANIFEST_KEY, serde_json::to_vec(&header)?.as_slice())?;
        }
        txn.commit()?;
        Ok(Ok(StagedCheckpoint {
            manifest: manifest.clone(),
            received: total,
            cursor: cursor.to_owned(),
        }))
    }

    /// What a transfer has staged, or `None` when none is in progress.
    ///
    /// # Errors
    /// Returns a store error if the read fails, or a decode error for a malformed staged manifest.
    pub fn staged_checkpoint(&self) -> Result<Option<StagedCheckpoint>, MetaError> {
        let txn = self.db.begin_read()?;
        let header = open_optional_table(&txn, CHECKPOINT_STAGING_META)?
            .and_then(|table| table.get(STAGED_MANIFEST_KEY).transpose())
            .transpose()?
            .map(|value| serde_json::from_slice::<StagedHeader>(value.value()))
            .transpose()?;
        let Some(header) = header else {
            return Ok(None);
        };
        let mut received = 0;
        if let Some(table) = open_optional_table(&txn, CHECKPOINT_STAGING)? {
            for entry in table.iter()? {
                let (_, bytes) = entry?;
                received += bytes.value().len() as u64;
            }
        }
        Ok(Some(StagedCheckpoint {
            manifest: header.manifest,
            received,
            cursor: header.cursor,
        }))
    }

    /// Verifies the staged checkpoint and makes it this node's replicated state.
    ///
    /// Verification streams the staged bytes rather than holding them, and it runs before anything is
    /// replaced, so a corrupt or truncated transfer leaves the previous state and cursor untouched. The
    /// replacement is one transaction: the driver rows, the revocations, the journal it supersedes, the
    /// serial the state stands at, and `cursor_key` all land together or none of them do.
    ///
    /// This is a replacement rather than a merge. A replica installing a checkpoint has fallen off the
    /// retained history, so what it held is a prefix of nothing it can prove; the folded state is the
    /// whole account of what replicated through that serial. Node-local rows go with it, which is what
    /// their writers already tolerate, since each is derived from the replicated rows beside it.
    ///
    /// # Errors
    /// Returns [`CheckpointInstallError`] when nothing is staged, the transfer is short, the bytes are
    /// not canonical, or the manifest disagrees with what arrived.
    pub fn install_staged_checkpoint(
        &self,
        cursor_key: &str,
        cursor_value: &[u8],
    ) -> Result<CheckpointManifest, CheckpointInstallError> {
        let staged = self.staged_checkpoint()?.ok_or(CheckpointInstallError::NotStaged)?;
        let manifest = staged.manifest;
        if staged.received != manifest.bytes {
            return Err(CheckpointInstallError::Incomplete {
                received: staged.received,
                declared: manifest.bytes,
            });
        }
        let decoded = self.decode_staged()?;
        let counted = [
            ("rows", manifest.rows, decoded.rows.len() as u64),
            ("revocations", manifest.revocations, decoded.revocations.len() as u64),
            ("blobs", manifest.blobs, decoded.blobs.len() as u64),
        ];
        for (unit, declared, actual) in counted {
            if declared != actual {
                return Err(CheckpointVerifyError::Truncated { unit, declared, actual }.into());
            }
        }
        if decoded.digest != manifest.digest {
            return Err(CheckpointVerifyError::Digest {
                declared: manifest.digest,
                actual: decoded.digest,
            }
            .into());
        }
        self.replace_state(&decoded, &manifest, cursor_key, cursor_value)?;
        self.discard_staged_checkpoint()?;
        Ok(manifest)
    }

    /// Swaps the decoded state in for whatever this node held, in one transaction.
    ///
    /// The state lands twice: as the live rows this node serves, and as the checkpoint it has published.
    /// Publishing what it installed is what keeps a later local publication honest, since that folds the
    /// journal onto the published checkpoint and this node's journal now starts after the install.
    fn replace_state(
        &self,
        decoded: &DecodedCheckpoint,
        manifest: &CheckpointManifest,
        cursor_key: &str,
        cursor_value: &[u8],
    ) -> Result<(), MetaError> {
        let serial = manifest.serial;
        let txn = self.db.begin_write()?;
        txn.delete_table(DRIVER_KV)?;
        txn.delete_table(JOURNAL)?;
        txn.delete_table(JOURNAL_MUTATIONS)?;
        txn.delete_table(JOURNAL_BLOBS)?;
        txn.delete_table(CHECKPOINT_ROW)?;
        txn.delete_table(CHECKPOINT_REVOCATION)?;
        txn.delete_table(CHECKPOINT_BLOB)?;
        {
            let mut rows = txn.open_table(DRIVER_KV)?;
            let mut published = txn.open_table(CHECKPOINT_ROW)?;
            for (key, value) in &decoded.rows {
                rows.insert(key.as_str(), value.as_slice())?;
                published.insert(key.as_str(), value.as_slice())?;
            }
            rows.insert(cursor_key, cursor_value)?;
            drop(rows);
            drop(published);
            super::revocation::replace_digest_revocations(&txn, &decoded.revocations)?;
            let mut revocations = txn.open_table(CHECKPOINT_REVOCATION)?;
            for (digest, record) in &decoded.revocations {
                revocations.insert(digest.as_str(), serde_json::to_vec(record)?.as_slice())?;
            }
            drop(revocations);
            let mut blobs = txn.open_table(CHECKPOINT_BLOB)?;
            for blob in &decoded.blobs {
                blobs.insert(blob.sha256.as_str(), blob.size)?;
            }
            drop(blobs);
            let published = serde_json::to_vec(manifest)?;
            let mut names = txn.open_table(CHECKPOINT_META)?;
            names.insert(super::checkpoint::MANIFEST_KEY, published.as_slice())?;
            drop(names);
            txn.open_table(super::JOURNAL_RETENTION)?
                .insert(super::journal::RETAINED_FLOOR_KEY, serial.saturating_add(1))?;
            txn.open_table(CHECKPOINT_BLOB_RECOVERY)?
                .insert(BLOB_RECOVERY_KEY, "")?;
            txn.open_table(DERIVED_VIEW_FRONTIER)?
                .insert(peryx_ha::AVAILABILITY_BLOB_VIEW, serial.saturating_sub(1))?;
            txn.open_table(SERIAL)?.insert(SERIAL_KEY, serial)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Reads the staged bytes back into the state they encode, hashing as it goes.
    fn decode_staged(&self) -> Result<DecodedCheckpoint, CheckpointInstallError> {
        let txn = self.db.begin_read().map_err(MetaError::from)?;
        let mut decoder = CanonicalDecoder::default();
        if let Some(table) = open_optional_table(&txn, CHECKPOINT_STAGING)? {
            for entry in table.iter().map_err(MetaError::from)? {
                let (_, bytes) = entry.map_err(MetaError::from)?;
                decoder.push(bytes.value())?;
            }
        }
        decoder.finish()
    }

    /// Drops a staged transfer, which is what a receiver does when it restarts one from the beginning.
    ///
    /// # Errors
    /// Returns a store error if the write fails.
    pub fn discard_staged_checkpoint(&self) -> Result<(), MetaError> {
        let txn = self.db.begin_write()?;
        txn.delete_table(CHECKPOINT_STAGING)?;
        txn.delete_table(CHECKPOINT_STAGING_META)?;
        txn.commit()?;
        Ok(())
    }
}

/// The state a staged transfer encodes, with the digest over the bytes it was read from.
struct DecodedCheckpoint {
    rows: BTreeMap<String, Vec<u8>>,
    revocations: BTreeMap<String, DigestRevocation>,
    blobs: BTreeSet<DriverBlobReference>,
    digest: String,
}

/// Reads canonical entries out of a byte stream that arrives in arbitrary pieces.
///
/// It holds only the entry it is part-way through, so a transfer larger than memory decodes the same
/// way a small one does. The hash covers every byte fed in, in order, which is the same span
/// [`CheckpointState::canonical`](super::CheckpointState::canonical) covers on the writer.
#[derive(Default)]
struct CanonicalDecoder {
    buffer: Vec<u8>,
    consumed: u64,
    hasher: Sha256,
    rows: BTreeMap<String, Vec<u8>>,
    revocations: BTreeMap<String, DigestRevocation>,
    blobs: BTreeSet<DriverBlobReference>,
}

impl CanonicalDecoder {
    fn push(&mut self, bytes: &[u8]) -> Result<(), CheckpointInstallError> {
        self.hasher.update(bytes);
        self.buffer.extend_from_slice(bytes);
        while self.take_entry()? {}
        Ok(())
    }

    /// Decodes one entry when the buffer holds a whole one, reporting whether it did.
    fn take_entry(&mut self) -> Result<bool, CheckpointInstallError> {
        let Some((&tag, rest)) = self.buffer.split_first() else {
            return Ok(false);
        };
        let Some((key, rest)) = take_field(rest) else {
            return Ok(false);
        };
        let (value, rest) = match tag {
            ROW_TAG | REVOCATION_TAG => match take_field(rest) {
                Some((value, rest)) => (value.to_vec(), rest),
                None => return Ok(false),
            },
            BLOB_TAG => match rest.split_first_chunk::<8>() {
                Some((size, rest)) => (size.to_vec(), rest),
                None => return Ok(false),
            },
            _ => return Err(self.malformed()),
        };
        let key = String::from_utf8(key.to_vec()).map_err(|_| self.malformed())?;
        match tag {
            ROW_TAG => {
                self.rows.insert(key, value);
            }
            REVOCATION_TAG => {
                let record = serde_json::from_slice(&value).map_err(|_| self.malformed())?;
                self.revocations.insert(key, record);
            }
            _ => {
                let size = u64::from_le_bytes(value.as_slice().try_into().expect("a blob size is eight bytes"));
                self.blobs.insert(DriverBlobReference { sha256: key, size });
            }
        }
        let remaining = rest.len();
        self.consumed += (self.buffer.len() - remaining) as u64;
        self.buffer.drain(..self.buffer.len() - remaining);
        Ok(true)
    }

    const fn malformed(&self) -> CheckpointInstallError {
        CheckpointInstallError::Malformed { offset: self.consumed }
    }

    fn finish(self) -> Result<DecodedCheckpoint, CheckpointInstallError> {
        if !self.buffer.is_empty() {
            return Err(CheckpointInstallError::Malformed { offset: self.consumed });
        }
        Ok(DecodedCheckpoint {
            rows: self.rows,
            revocations: self.revocations,
            blobs: self.blobs,
            digest: hex::encode(self.hasher.finalize()),
        })
    }
}

/// Splits off one length-prefixed field, or reports that the buffer does not hold a whole one yet.
fn take_field(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let (length, rest) = bytes.split_first_chunk::<8>()?;
    let length = usize::try_from(u64::from_le_bytes(*length)).ok()?;
    (rest.len() >= length).then(|| rest.split_at(length))
}

const fn bound(after: Option<&str>) -> std::ops::Bound<&str> {
    match after {
        Some(key) => Excluded(key),
        None => Unbounded,
    }
}

fn push_field(encoded: &mut Vec<u8>, tag: u8, key: &[u8]) {
    encoded.push(tag);
    push_bytes(encoded, key);
}

fn push_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) {
    encoded.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    encoded.extend_from_slice(bytes);
}

#[cfg(test)]
#[path = "../../tests/unit/meta/checkpoint/transfer_tests.rs"]
mod transfer_tests;

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use peryx_storage::meta::{DriverTxn, MetaError, MetaStore};
use serde::{Deserialize, Serialize};

use super::{RELEASE_METADATA_PREFIX, UPLOAD_PREFIX};
use crate::CoreMetadata;
use crate::version::canonical_release;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReleaseMetadataLocator {
    Absent,
    Generated,
    Digest(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseMetadataSelection {
    pub filename: String,
    pub artifact_sha256: String,
    pub metadata: ReleaseMetadataLocator,
}

struct Candidate {
    submitted_at: Option<time::OffsetDateTime>,
    selection: ReleaseMetadataSelection,
}

#[derive(Deserialize)]
struct MetadataRecord {
    version: String,
    #[serde(default)]
    file: Option<MetadataFile>,
    #[serde(default)]
    trashed: Option<serde::de::IgnoredAny>,
}

#[derive(Deserialize)]
struct MetadataFile {
    filename: String,
    #[serde(default)]
    hashes: BTreeMap<String, String>,
    #[serde(rename = "upload-time", default)]
    upload_time: Option<String>,
    #[serde(rename = "core-metadata", default)]
    core_metadata: CoreMetadata,
    #[serde(rename = "dist-info-metadata", default)]
    dist_info_metadata: CoreMetadata,
}

impl MetadataFile {
    const fn metadata(&self) -> &CoreMetadata {
        if self.core_metadata.is_absent() {
            &self.dist_info_metadata
        } else {
            &self.core_metadata
        }
    }
}

impl Candidate {
    fn from_record(record: MetadataRecord) -> Option<Self> {
        let file = record.file?;
        let submitted_at = file
            .upload_time
            .as_deref()
            .and_then(|value| time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok());
        let artifact_sha256 = file.hashes.get("sha256")?.to_owned();
        let metadata = metadata_locator(file.metadata());
        Some(Self {
            selection: ReleaseMetadataSelection {
                filename: file.filename,
                artifact_sha256,
                metadata,
            },
            submitted_at,
        })
    }

    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.submitted_at, &other.submitted_at) {
            (Some(left), Some(right)) => left.cmp(right),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
        .then_with(|| self.selection.filename.cmp(&other.selection.filename))
    }
}

/// Return the persisted selection, migrating an existing hosted release when needed.
///
/// # Errors
/// Returns a store or record-decoding error when the selection cannot be read or migrated.
pub fn release_metadata_selection(
    meta: &MetaStore,
    index: &str,
    normalized: &str,
    version: &str,
) -> Result<Option<ReleaseMetadataSelection>, MetaError> {
    let Some(release) = canonical_release(version) else {
        return Ok(None);
    };
    meta.commit_driver_txn(|txn| {
        let (selection, created) = ensure_selection(txn, index, normalized, &release, None, &BTreeSet::new())?;
        Ok((selection, created.then(|| b"{}".to_vec()).into_iter().collect()))
    })
}

/// Return a persisted selection without migrating the release.
///
/// # Errors
/// Returns a store or record-decoding error when the selection cannot be read.
pub fn stored_release_metadata_selection(
    meta: &MetaStore,
    index: &str,
    normalized: &str,
    version: &str,
) -> Result<Option<ReleaseMetadataSelection>, MetaError> {
    let Some(release) = canonical_release(version) else {
        return Ok(None);
    };
    let key = release_metadata_key(index, normalized, &release);
    meta.get_driver_value(&key)?
        .map(|raw| {
            serde_json::from_slice(&raw).map_err(|source| MetaError::DriverRecordMalformed {
                key: key.clone(),
                source,
            })
        })
        .transpose()
}

pub(super) fn select_published_file(
    txn: &mut DriverTxn,
    index: &str,
    normalized: &str,
    version: &str,
    candidate: ReleaseMetadataSelection,
    submitted_at_unix: i64,
) -> Result<(), MetaError> {
    let release = canonical_release(version).expect("admitted uploads have valid release versions");
    let new_filenames = BTreeSet::from([candidate.filename.clone()]);
    ensure_selection(
        txn,
        index,
        normalized,
        &release,
        Some(Candidate {
            submitted_at: time::OffsetDateTime::from_unix_timestamp(submitted_at_unix).ok(),
            selection: candidate,
        }),
        &new_filenames,
    )?;
    Ok(())
}

pub(super) fn select_promoted_files(
    txn: &mut DriverTxn,
    index: &str,
    normalized: &str,
    candidates: impl IntoIterator<Item = (String, ReleaseMetadataSelection)>,
    submitted_at_unix: i64,
    new_filenames: &BTreeSet<String>,
) -> Result<(), MetaError> {
    let submitted_at = time::OffsetDateTime::from_unix_timestamp(submitted_at_unix).ok();
    let mut selected = BTreeMap::<String, Candidate>::new();
    for (version, selection) in candidates {
        let release = canonical_release(&version).expect("admitted uploads have valid release versions");
        let candidate = Candidate {
            submitted_at,
            selection,
        };
        if selected
            .get(&release)
            .is_none_or(|current| candidate.cmp(current).is_lt())
        {
            selected.insert(release, candidate);
        }
    }
    let mut missing = BTreeMap::new();
    for (release, candidate) in selected {
        let key = release_metadata_key(index, normalized, &release);
        if let Some(raw) = txn.get(&key)? {
            serde_json::from_slice::<ReleaseMetadataSelection>(&raw)
                .map_err(|source| MetaError::DriverRecordMalformed { key, source })?;
        } else {
            missing.insert(release, candidate);
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    let mut existing = BTreeMap::<String, Candidate>::new();
    let prefix = format!("{UPLOAD_PREFIX}{index}/{normalized}/");
    txn.scan_prefix(&prefix, |stored_key, raw| -> Result<ControlFlow<()>, MetaError> {
        let record: MetadataRecord =
            serde_json::from_slice(raw).map_err(|source| MetaError::DriverRecordMalformed {
                key: stored_key.to_owned(),
                source,
            })?;
        if record.trashed.is_some() {
            return Ok(ControlFlow::Continue(()));
        }
        let Some(release) = canonical_release(&record.version) else {
            return Ok(ControlFlow::Continue(()));
        };
        if !missing.contains_key(&release) {
            return Ok(ControlFlow::Continue(()));
        }
        let Some(file) = record.file.as_ref() else {
            return Ok(ControlFlow::Continue(()));
        };
        if new_filenames.contains(&file.filename) {
            return Ok(ControlFlow::Continue(()));
        }
        let Some(candidate) = Candidate::from_record(record) else {
            return Ok(ControlFlow::Continue(()));
        };
        if existing
            .get(&release)
            .is_none_or(|current| candidate.cmp(current).is_lt())
        {
            existing.insert(release, candidate);
        }
        Ok(ControlFlow::Continue(()))
    })?;
    for (release, candidate) in missing {
        let key = release_metadata_key(index, normalized, &release);
        let candidate = existing.remove(&release).unwrap_or(candidate);
        txn.put(&key, &serde_json::to_vec(&candidate.selection)?)?;
    }
    Ok(())
}

fn ensure_selection(
    txn: &mut DriverTxn,
    index: &str,
    normalized: &str,
    release: &str,
    candidate: Option<Candidate>,
    new_filenames: &BTreeSet<String>,
) -> Result<(Option<ReleaseMetadataSelection>, bool), MetaError> {
    let key = release_metadata_key(index, normalized, release);
    if let Some(raw) = txn.get(&key)? {
        return serde_json::from_slice(&raw)
            .map(Some)
            .map(|selection| (selection, false))
            .map_err(|source| MetaError::DriverRecordMalformed { key, source });
    }
    let mut selected = None;
    let prefix = format!("{UPLOAD_PREFIX}{index}/{normalized}/");
    txn.scan_prefix(&prefix, |stored_key, raw| -> Result<ControlFlow<()>, MetaError> {
        let record: MetadataRecord =
            serde_json::from_slice(raw).map_err(|source| MetaError::DriverRecordMalformed {
                key: stored_key.to_owned(),
                source,
            })?;
        if record.trashed.is_some() || canonical_release(&record.version).as_deref() != Some(release) {
            return Ok(ControlFlow::Continue(()));
        }
        let Some(file) = record.file.as_ref() else {
            return Ok(ControlFlow::Continue(()));
        };
        if new_filenames.contains(&file.filename) {
            return Ok(ControlFlow::Continue(()));
        }
        let Some(candidate) = Candidate::from_record(record) else {
            return Ok(ControlFlow::Continue(()));
        };
        if selected.as_ref().is_none_or(|current| candidate.cmp(current).is_lt()) {
            selected = Some(candidate);
        }
        Ok(ControlFlow::Continue(()))
    })?;
    let selected = selected.or(candidate);
    let Some(selected) = selected else {
        return Ok((None, false));
    };
    txn.put(&key, &serde_json::to_vec(&selected.selection)?)?;
    Ok((Some(selected.selection), true))
}

pub(super) fn selection_from_publication(
    filename: &str,
    artifact_sha256: &str,
    metadata: &CoreMetadata,
) -> ReleaseMetadataSelection {
    ReleaseMetadataSelection {
        filename: filename.to_owned(),
        artifact_sha256: artifact_sha256.to_owned(),
        metadata: metadata_locator(metadata),
    }
}

pub(super) fn selection_from_metadata_digest(
    filename: &str,
    artifact_sha256: &str,
    metadata_sha256: Option<&str>,
) -> ReleaseMetadataSelection {
    ReleaseMetadataSelection {
        filename: filename.to_owned(),
        artifact_sha256: artifact_sha256.to_owned(),
        metadata: metadata_sha256.map_or(ReleaseMetadataLocator::Absent, |digest| {
            ReleaseMetadataLocator::Digest(digest.to_owned())
        }),
    }
}

pub(super) fn selection_from_promoted_record(
    filename: &str,
    artifact_sha256: &str,
    record: &[u8],
) -> Result<(String, ReleaseMetadataSelection), MetaError> {
    let promoted: MetadataRecord = serde_json::from_slice(record)?;
    let file = promoted.file.ok_or_else(|| MetaError::DriverRecordMissing {
        key: filename.to_owned(),
        field: "file",
    })?;
    let selection = selection_from_publication(filename, artifact_sha256, file.metadata());
    Ok((promoted.version, selection))
}

fn metadata_locator(metadata: &CoreMetadata) -> ReleaseMetadataLocator {
    match metadata {
        CoreMetadata::Absent => ReleaseMetadataLocator::Absent,
        CoreMetadata::Available => ReleaseMetadataLocator::Generated,
        CoreMetadata::Hashes(hashes) => hashes
            .get("sha256")
            .cloned()
            .map_or(ReleaseMetadataLocator::Generated, ReleaseMetadataLocator::Digest),
    }
}

fn release_metadata_key(index: &str, normalized: &str, release: &str) -> String {
    format!("{RELEASE_METADATA_PREFIX}{index}/{normalized}/{release}")
}

#[cfg(test)]
#[path = "../../tests/unit/store/release_metadata/tests.rs"]
mod tests;

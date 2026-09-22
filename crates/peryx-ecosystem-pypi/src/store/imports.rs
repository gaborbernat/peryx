use std::collections::BTreeSet;

use peryx_storage::meta::{DriverTxn, MetaError};
use serde::{Deserialize, Serialize};

use super::{UPLOAD_PREFIX, UploadWriteError, release_imports_init_key, release_imports_key};
use crate::canonical_release;
use crate::upload::{ImportDeclarations, ImportEntry};

const INITIALIZATION_PAGE_SIZE: usize = 128;

#[derive(Default, Serialize, Deserialize)]
struct ReleaseImports {
    live: u64,
    unclassified: u64,
    declarations: Vec<DeclarationBucket>,
}

#[derive(Serialize, Deserialize)]
struct DeclarationBucket {
    declaration: ImportDeclarations,
    count: u64,
}

#[derive(Deserialize, PartialEq, Eq)]
struct ImportProjection {
    version: String,
    #[serde(default)]
    imports: Option<ImportDeclarations>,
    #[serde(default)]
    trashed: Option<serde_json::Value>,
}

pub fn initialize_release_imports(
    meta: &peryx_storage::meta::MetaStore,
    index: &str,
    normalized: &str,
) -> Result<(), MetaError> {
    while initialize_release_imports_page(meta, index, normalized)? {}
    Ok(())
}

pub fn release_imports_initialized(
    meta: &peryx_storage::meta::MetaStore,
    index: &str,
    normalized: &str,
) -> Result<bool, MetaError> {
    Ok(meta
        .get_driver_value(&release_imports_init_key(index, normalized))?
        .is_some_and(|marker| marker.is_empty()))
}

pub fn initialize_release_imports_page(
    meta: &peryx_storage::meta::MetaStore,
    index: &str,
    normalized: &str,
) -> Result<bool, MetaError> {
    meta.commit_driver_txn(|txn| {
        let initialized = release_imports_init_key(index, normalized);
        let prefix = format!("{UPLOAD_PREFIX}{index}/{normalized}/");
        let cursor = match txn.get(&initialized)? {
            Some(marker) if marker.is_empty() => return Ok((false, Vec::new())),
            Some(marker) => {
                let cursor = String::from_utf8(marker).map_err(|error| {
                    MetaError::DriverPrecondition(format!(
                        "corrupt release import initialization {initialized:?}: {error}"
                    ))
                })?;
                if !cursor.starts_with(&prefix) || cursor.len() == prefix.len() {
                    return Err(MetaError::DriverPrecondition(format!(
                        "corrupt release import initialization {initialized:?}: cursor is outside its upload prefix"
                    )));
                }
                Some(cursor)
            }
            None => None,
        };
        let mut entries = txn.prefix_after_limited(&prefix, cursor.as_deref(), INITIALIZATION_PAGE_SIZE + 1)?;
        let complete = entries.len() <= INITIALIZATION_PAGE_SIZE;
        if !complete {
            entries.pop();
        }
        if entries.is_empty() && cursor.is_none() {
            return Ok((false, Vec::new()));
        }
        for (_, record) in &entries {
            if let Some(upload) = parse_initialization_upload(record)? {
                add_record(txn, index, normalized, &upload)?;
            }
        }
        if complete {
            txn.put(&initialized, b"")?;
        } else {
            let cursor = entries.last().expect("full initialization page").0.clone();
            txn.put(&initialized, cursor.as_bytes())?;
        }
        Ok((!complete, vec![b"{}".to_vec()]))
    })
}

pub(super) fn ensure_release_imports(txn: &mut DriverTxn, index: &str, normalized: &str) -> Result<(), MetaError> {
    let initialized = release_imports_init_key(index, normalized);
    if txn.get(&initialized)?.is_some_and(|marker| marker.is_empty()) {
        return Ok(());
    }
    let prefix = format!("{UPLOAD_PREFIX}{index}/{normalized}/");
    if txn.prefix_after_limited(&prefix, None, 1)?.is_empty() {
        return txn.put(&initialized, b"");
    }
    Err(MetaError::DriverPrecondition(format!(
        "release import initialization for {index:?}/{normalized:?} is incomplete"
    )))
}

pub(super) fn check_release_imports(
    txn: &DriverTxn,
    index: &str,
    normalized: &str,
    previous: Option<&[u8]>,
    record: &[u8],
) -> Result<(), UploadWriteError> {
    let Some(upload) = parse_upload(record)? else {
        return Ok(());
    };
    let Some(release) = canonical_release(&upload.version) else {
        return Err(
            MetaError::DriverPrecondition(format!("invalid uploaded release version {:?}", upload.version)).into(),
        );
    };
    if let Some(previous) = previous
        && let Some(previous) = parse_upload(previous)?
        && canonical_release(&previous.version).as_deref() == Some(&release)
    {
        if previous.imports == upload.imports {
            return Ok(());
        }
        let key = release_imports_key(index, normalized, &release);
        let mut state = load(txn, &key)?;
        remove_upload(&mut state, &key, &previous)?;
        return check_candidate(&state, &release, upload.imports.as_ref());
    }
    let state = load(txn, &release_imports_key(index, normalized, &release))?;
    check_candidate(&state, &release, upload.imports.as_ref())
}

fn check_candidate(
    state: &ReleaseImports,
    release: &str,
    candidate: Option<&ImportDeclarations>,
) -> Result<(), UploadWriteError> {
    if state.unclassified > 0 {
        return Err(UploadWriteError::ReleaseImports(format!(
            "release import declarations for {release:?} are incomplete: {} unclassified artifact(s)",
            state.unclassified
        )));
    }
    let Some(candidate) = candidate else {
        return Err(UploadWriteError::ReleaseImports(format!(
            "release import declarations for {release:?} are incomplete"
        )));
    };
    for bucket in &state.declarations {
        ensure_compatible(release, &bucket.declaration, candidate)?;
    }
    Ok(())
}

pub(super) fn validate_upload_projection(record: &[u8]) -> Result<(), MetaError> {
    parse_upload(record).map(|_| ())
}

pub(super) fn update_release_imports(
    txn: &mut DriverTxn,
    index: &str,
    normalized: &str,
    previous: Option<&[u8]>,
    record: &[u8],
) -> Result<(), MetaError> {
    if let Some(previous) = previous
        && let Some(previous) = parse_upload(previous)?
    {
        remove_record(txn, index, normalized, &previous)?;
    }
    if let Some(upload) = parse_candidate(record) {
        add_record(txn, index, normalized, &upload)?;
    }
    Ok(())
}

fn add_record(txn: &mut DriverTxn, index: &str, normalized: &str, upload: &ImportProjection) -> Result<(), MetaError> {
    let Some(release) = canonical_release(&upload.version) else {
        return Err(MetaError::DriverPrecondition(format!(
            "invalid uploaded release version {:?}",
            upload.version
        )));
    };
    let key = release_imports_key(index, normalized, &release);
    let mut state = load(txn, &key)?;
    state.live = state.live.checked_add(1).ok_or_else(|| {
        MetaError::DriverPrecondition(format!("release import constraint {key:?} overflowed live count"))
    })?;
    match &upload.imports {
        Some(imports) => {
            if let Some(bucket) = state
                .declarations
                .iter_mut()
                .find(|bucket| bucket.declaration == *imports)
            {
                bucket.count += 1;
            } else {
                state.declarations.push(DeclarationBucket {
                    declaration: imports.clone(),
                    count: 1,
                });
            }
        }
        None => {
            state.unclassified += 1;
        }
    }
    store(txn, &key, &state)
}

fn remove_record(
    txn: &mut DriverTxn,
    index: &str,
    normalized: &str,
    upload: &ImportProjection,
) -> Result<(), MetaError> {
    let Some(release) = canonical_release(&upload.version) else {
        return Err(MetaError::DriverPrecondition(format!(
            "invalid uploaded release version {:?}",
            upload.version
        )));
    };
    let key = release_imports_key(index, normalized, &release);
    let mut state = load(txn, &key)?;
    remove_upload(&mut state, &key, upload)?;
    if state.live == 0 {
        txn.remove(&key)?;
    } else {
        store(txn, &key, &state)?;
    }
    Ok(())
}

fn load(txn: &DriverTxn, key: &str) -> Result<ReleaseImports, MetaError> {
    let state = txn.get(key)?.map_or_else(
        || Ok(ReleaseImports::default()),
        |record| {
            serde_json::from_slice(&record).map_err(|error| {
                MetaError::DriverPrecondition(format!("corrupt release import constraint {key:?}: {error}"))
            })
        },
    )?;
    validate(&state, key)?;
    Ok(state)
}

fn validate(state: &ReleaseImports, key: &str) -> Result<(), MetaError> {
    let declared = state
        .declarations
        .iter()
        .enumerate()
        .try_fold(0_u64, |total, (position, bucket)| {
            if bucket.count == 0
                || state.declarations[..position]
                    .iter()
                    .any(|previous| previous.declaration == bucket.declaration)
            {
                return Err(MetaError::DriverPrecondition(format!(
                    "corrupt release import constraint {key:?}: invalid declaration buckets"
                )));
            }
            total.checked_add(bucket.count).ok_or_else(|| {
                MetaError::DriverPrecondition(format!(
                    "corrupt release import constraint {key:?}: declaration count overflowed"
                ))
            })
        })?;
    if state.live
        != state.unclassified.checked_add(declared).ok_or_else(|| {
            MetaError::DriverPrecondition(format!("corrupt release import constraint {key:?}: count overflowed"))
        })?
    {
        return Err(MetaError::DriverPrecondition(format!(
            "corrupt release import constraint {key:?}: counts do not match live rows"
        )));
    }
    Ok(())
}

fn store(txn: &mut DriverTxn, key: &str, state: &ReleaseImports) -> Result<(), MetaError> {
    txn.put(
        key,
        &serde_json::to_vec(state).expect("release import constraints serialize"),
    )
}

fn parse_initialization_upload(record: &[u8]) -> Result<Option<ImportProjection>, MetaError> {
    let upload: ImportProjection = serde_json::from_slice(record)
        .map_err(|error| MetaError::DriverPrecondition(format!("corrupt uploaded record: {error}")))?;
    Ok((upload.trashed.is_none()).then_some(upload))
}

fn parse_upload(record: &[u8]) -> Result<Option<ImportProjection>, MetaError> {
    let upload: ImportProjection = serde_json::from_slice(record)
        .map_err(|error| MetaError::DriverPrecondition(format!("corrupt uploaded record: {error}")))?;
    Ok((upload.trashed.is_none()).then_some(upload))
}

fn parse_candidate(record: &[u8]) -> Option<ImportProjection> {
    parse_upload(record).unwrap_or_default()
}

fn remove_upload(state: &mut ReleaseImports, key: &str, upload: &ImportProjection) -> Result<(), MetaError> {
    state.live = state.live.checked_sub(1).ok_or_else(|| {
        MetaError::DriverPrecondition(format!("release import constraint {key:?} underflowed live count"))
    })?;
    if let Some(imports) = &upload.imports {
        let position = state
            .declarations
            .iter()
            .position(|bucket| bucket.declaration == *imports)
            .ok_or_else(|| {
                MetaError::DriverPrecondition(format!("release import constraint {key:?} misses a declaration bucket"))
            })?;
        let bucket = &mut state.declarations[position];
        bucket.count -= 1;
        if bucket.count == 0 {
            state.declarations.remove(position);
        }
    } else {
        state.unclassified = state.unclassified.checked_sub(1).ok_or_else(|| {
            MetaError::DriverPrecondition(format!(
                "release import constraint {key:?} underflowed unclassified count"
            ))
        })?;
    }
    Ok(())
}

fn ensure_compatible(
    release: &str,
    existing: &ImportDeclarations,
    candidate: &ImportDeclarations,
) -> Result<(), UploadWriteError> {
    let (
        ImportDeclarations::V1 { exclusive, shared },
        ImportDeclarations::V1 {
            exclusive: candidate_exclusive,
            shared: candidate_shared,
        },
    ) = (existing, candidate)
    else {
        return (existing == candidate).then_some(()).ok_or_else(|| {
            UploadWriteError::ReleaseImports(format!(
                "release import declarations for {release:?} are incomplete because an artifact uses metadata before 2.5"
            ))
        });
    };
    if names(exclusive.as_ref()).is_disjoint(&names(candidate_shared.as_ref()))
        && names(shared.as_ref()).is_disjoint(&names(candidate_exclusive.as_ref()))
    {
        return (exclusive == candidate_exclusive && shared == candidate_shared)
            .then_some(())
            .ok_or_else(|| {
                UploadWriteError::ReleaseImports(format!(
                    "release import declarations for {release:?} are inconsistent"
                ))
            });
    }
    Err(UploadWriteError::ReleaseImports(format!(
        "release import declarations for {release:?} declare an import as both exclusive and shared"
    )))
}

fn names(entries: Option<&BTreeSet<ImportEntry>>) -> BTreeSet<&str> {
    entries.into_iter().flatten().map(|entry| entry.name.as_str()).collect()
}

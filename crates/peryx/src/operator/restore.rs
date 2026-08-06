//! Restoring a verified backup into a data directory.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context as _, bail};
use peryx_storage::blob::Digest;
use peryx_storage::meta::MetaStore;

use super::verify::check_backup;
use super::{Access, BackupManifest, backup_blob_path, copy_hashed, is_empty_dir, read_manifest};
use crate::config::{self, Config};

/// Restore a backup into a data directory.
///
/// # Errors
/// Returns an error if the backup fails verification, the target is unsafe, or files cannot be
/// copied.
pub fn restore(backup: &Path, data_dir: &Path, force: bool, out: &mut dyn Write) -> anyhow::Result<()> {
    let started = Instant::now();
    let manifest = read_manifest(backup)?;
    let mut verification = Vec::new();
    let check = check_backup(backup, &manifest, &mut verification)?;
    if check.problems != 0 {
        bail!(
            "backup verification failed with {problems} problem(s): {}",
            String::from_utf8_lossy(&verification),
            problems = check.problems,
        );
    }
    warn_config_mismatch(backup, &manifest, data_dir, out)?;
    guard_target_identity(&manifest, data_dir, out)?;
    prepare_restore_dir(data_dir, force)?;
    copy_hashed(
        &backup.join(&manifest.metadata.path),
        &data_dir.join("peryx.redb"),
        "peryx.redb",
        Access::Private,
    )
    .context("restore metadata store")?;
    copy_hashed(
        &backup.join(&manifest.config.path),
        &data_dir.join("config.toml"),
        "config.toml",
        Access::Private,
    )
    .context("restore config snapshot")?;
    for (digest, entry) in check.blobs {
        let digest = Digest::from_hex(&digest).context("backup blob index contained an invalid digest")?;
        copy_hashed(
            &backup.join(&entry.path),
            &backup_blob_path(data_dir, &digest),
            &entry.path,
            Access::Shared,
        )
        .context(format!("restore blob {}", digest.as_str()))?;
    }
    writeln!(out, "restored\t{}", data_dir.display())?;
    let count = manifest.blob_index.count;
    let blob_bytes = manifest.blob_index.blob_bytes;
    writeln!(out, "blobs\t{count}\t{blob_bytes}")?;
    let bytes = restored_bytes(&manifest);
    let elapsed_ms = started.elapsed().as_millis();
    writeln!(out, "bytes\t{bytes}")?;
    writeln!(out, "elapsed_ms\t{elapsed_ms}")?;
    Ok(())
}

/// The total bytes a restore reads from the backup: the metadata snapshot, the config snapshot, the blob
/// index, and every referenced blob. An operator reads it against the elapsed time to size the recovery.
const fn restored_bytes(manifest: &BackupManifest) -> u64 {
    manifest
        .metadata
        .size_bytes
        .saturating_add(manifest.config.size_bytes)
        .saturating_add(manifest.blob_index.file.size_bytes)
        .saturating_add(manifest.blob_index.blob_bytes)
}

/// Refuse a restore that would adopt one node's recovery point under a different node's identity, and
/// warn when it would roll a node back over control state it has already advanced past.
///
/// A target with no metadata store is a fresh recovery and passes. When the target already holds one,
/// its claimed writer identity must match the backup's: restoring node `b`'s state onto node `a`'s
/// directory would give two nodes the same identity, a split brain no `--force` should wave through, so
/// this rejects it regardless of `force`. A same-identity target sitting at a control serial ahead of
/// the backup is a genuine rollback; that is the operator's call under `--force`, so it warns rather than
/// refuses. The prepared-directory step still enforces the empty-target rule for the non-forced path.
///
/// # Errors
/// Returns an error when the target belongs to a different node, or its identity or serial cannot be
/// read.
fn guard_target_identity(manifest: &BackupManifest, data_dir: &Path, out: &mut dyn Write) -> anyhow::Result<()> {
    let target = data_dir.join("peryx.redb");
    if !target.is_file() {
        return Ok(());
    }
    let Ok(meta) = MetaStore::open_existing_read_only(&target) else {
        return Ok(());
    };
    let existing = meta.writer_identity().context("read restore target writer identity")?;
    if let (Some(backup), Some(existing)) = (manifest.availability.writer_identity.as_deref(), existing.as_deref())
        && backup != existing
    {
        bail!(
            "refusing to restore node {backup} onto a directory claimed by node {existing}; \
             clear the target or restore {existing}'s own backup"
        );
    }
    let target_serial = meta.current_serial().context("read restore target control serial")?;
    let frontier = manifest.availability.metadata_frontier;
    if target_serial > frontier {
        writeln!(
            out,
            "warning\trestore\trollback\ttarget at serial {target_serial}, backup at {frontier}"
        )?;
    }
    Ok(())
}

fn warn_config_mismatch(
    backup: &Path,
    manifest: &BackupManifest,
    data_dir: &Path,
    out: &mut dyn Write,
) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(backup.join(&manifest.config.path))
        .context(format!("read backup config {}", manifest.config.path))?;
    let backup_config = Config::default()
        .apply(config::from_toml(PathBuf::from(&manifest.config.path), &text)?)
        .context("parse backup config snapshot")?;
    if backup_config.data_dir == data_dir {
        return Ok(());
    }
    let backup_dir = backup_config.data_dir.display();
    let restore_dir = data_dir.display();
    let message = format!("warning\tconfig\tdata_dir\tbackup={backup_dir}\trestore={restore_dir}\n");
    out.write_all(message.as_bytes())?;
    Ok(())
}

fn prepare_restore_dir(data_dir: &Path, force: bool) -> anyhow::Result<()> {
    if data_dir.exists() {
        if data_dir.is_dir() {
            if is_empty_dir(data_dir)? {
                return Ok(());
            }
            if !force {
                bail!(
                    "restore target {} is not empty; pass --force to replace it",
                    data_dir.display()
                );
            }
            std::fs::remove_dir_all(data_dir).context(format!("remove {}", data_dir.display()))?;
        } else {
            if !force {
                bail!(
                    "restore target {} exists and is not a directory; pass --force to replace it",
                    data_dir.display()
                );
            }
            std::fs::remove_file(data_dir).context(format!("remove {}", data_dir.display()))?;
        }
    }
    std::fs::create_dir_all(data_dir).context(format!("create restore target {}", data_dir.display()))
}

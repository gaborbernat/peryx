//! `PyPI` distribution-archive validation and metadata extraction.
//!
//! The wheel and sdist correctness checks run at upload time, and the PEP 658 `METADATA`/`PKG-INFO`
//! sidecar extraction, layer the format-specific rules on the ecosystem-neutral archive engine in
//! `peryx-storage` that the web UI's file browser drives for every ecosystem.

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};

mod sdist;
mod wheel;

pub use peryx_storage::archive::*;

pub use sdist::{sdist_metadata_path, validate_sdist_path, validate_zip_sdist_path};
pub use wheel::{
    MAX_WHEEL_METADATA_BYTES, validate_wheel_path, wheel_metadata, wheel_metadata_member_path, wheel_metadata_path,
};

/// What one validation pass over a distribution archive yields.
///
/// Upload validation owns the `License-File` rejection, so the walk that already lists the members
/// reports the declared paths it did not find rather than failing on them.
#[derive(Debug)]
pub struct ValidatedArchive {
    pub metadata: Vec<u8>,
    pub missing_license_files: Vec<String>,
}

/// Whether an archive member is a file or a directory, kept per normalized name so a path spelled as
/// both can be rejected. Links share the file slot; they carry a distinct path and cannot alias a
/// directory here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryKind {
    File,
    Directory,
}

/// Explain why a normalized member path repeats. A ZIP permits duplicate names and validation and
/// consumption can then pick different bytes for one path, so any second occurrence is rejected: a
/// duplicated file, a duplicated directory, or a name spelled as both a file and a directory.
fn duplicate_member_message(name: &str, first: EntryKind, second: EntryKind) -> String {
    match (first, second) {
        (EntryKind::File, EntryKind::File) => format!("duplicate file member {name:?}"),
        (EntryKind::Directory, EntryKind::Directory) => format!("duplicate directory member {name:?}"),
        _ => format!("member {name:?} is both a file and a directory"),
    }
}

const CENTRAL_DIRECTORY_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x01, 0x02];
const CENTRAL_DIRECTORY_HEADER_LEN: usize = 46;

/// Reject a ZIP whose central directory names any normalized path twice.
///
/// [`zip::ZipArchive`] folds the central directory into a name-keyed map, so its `by_index` walk
/// never reveals a duplicate: the last record silently wins. A ZIP reader in an installer may pick a
/// different record for the same name, so validation and consumption could hash and extract
/// different bytes. This walks the raw records `zip` already located to catch the ambiguity the
/// deduplicated view hides. `invalid` wraps the failure with the wheel or sdist prefix.
fn reject_duplicate_zip_members<R: Read + Seek>(
    reader: &mut R,
    directory_start: u64,
    invalid: impl Fn(String) -> ArchiveError,
) -> Result<(), ArchiveError> {
    reader.seek(SeekFrom::Start(directory_start)).map_err(read_error)?;
    let mut header = [0_u8; CENTRAL_DIRECTORY_HEADER_LEN];
    let mut seen: BTreeMap<Vec<u8>, EntryKind> = BTreeMap::new();
    while read_central_directory_header(reader, &mut header)? {
        let name_len = usize::from(u16::from_le_bytes([header[28], header[29]]));
        let extra_len = i64::from(u16::from_le_bytes([header[30], header[31]]));
        let comment_len = i64::from(u16::from_le_bytes([header[32], header[33]]));
        let mut name = vec![0_u8; name_len];
        reader.read_exact(&mut name).map_err(read_error)?;
        reader
            .seek(SeekFrom::Current(extra_len + comment_len))
            .map_err(read_error)?;
        let is_dir = name.last() == Some(&b'/');
        if is_dir {
            name.pop();
        }
        let kind = if is_dir { EntryKind::Directory } else { EntryKind::File };
        if let Some(first) = seen.insert(name.clone(), kind) {
            return Err(invalid(duplicate_member_message(
                &String::from_utf8_lossy(&name),
                first,
                kind,
            )));
        }
    }
    Ok(())
}

/// Read the next 46-byte central-directory header, returning `false` once the walk reaches the
/// end-of-central-directory record that follows the last entry.
fn read_central_directory_header<R: Read>(
    reader: &mut R,
    header: &mut [u8; CENTRAL_DIRECTORY_HEADER_LEN],
) -> Result<bool, ArchiveError> {
    reader.read_exact(&mut header[..4]).map_err(read_error)?;
    if header[..4] != CENTRAL_DIRECTORY_SIGNATURE {
        return Ok(false);
    }
    reader.read_exact(&mut header[4..]).map_err(read_error)?;
    Ok(true)
}

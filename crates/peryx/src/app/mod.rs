//! Command actions that do not touch global state.

use anyhow::{Context as _, bail};
use peryx_storage::blob::BlobStorage;
use peryx_storage::meta::MetaStore;

use crate::config::{BlobStorageConfig, Config};

mod bootstrap;
mod cache;
mod config;
mod fsck;
mod indexes;
mod jobs;
mod policy;
mod purge;
mod quota;
mod retention;
mod revocation;
mod secret;

pub use bootstrap::bootstrap_administrator;
pub use cache::cache;
pub use config::config_check;
pub use indexes::{config_snippet, index, init, init_data_dir};
pub use jobs::job;
pub use policy::policy;
pub(crate) use purge::referenced_blob_digests;
pub use quota::quota;
pub use retention::retention;
pub use revocation::revocation;

/// Reject an offline command that reads or writes the local filesystem blob store when the
/// repository points its blobs at an object store, before the command can mutate metadata or report
/// success against bytes the running server keeps elsewhere.
///
/// # Errors
/// Returns an error when the configured blob backend is not the local filesystem.
pub(crate) fn reject_object_store_blob(config: &Config, command: &str) -> anyhow::Result<()> {
    match config.blob {
        BlobStorageConfig::Filesystem => Ok(()),
        BlobStorageConfig::S3(_) => bail!(
            "{command} is only supported on the filesystem blob backend, but this repository is configured for \
             S3; run it against a filesystem-backed repository"
        ),
    }
}

struct CacheStores {
    meta: MetaStore,
    blobs: BlobStorage,
}

impl CacheStores {
    fn open(config: &Config) -> anyhow::Result<Self> {
        Ok(Self {
            meta: MetaStore::open_existing(config.data_dir.join("peryx.redb"))
                .with_context(|| format!("open metadata store {}", config.data_dir.join("peryx.redb").display()))?,
            blobs: BlobStorage::filesystem(config.data_dir.join("blobs")),
        })
    }
}

fn index_names(config: &Config) -> Vec<&str> {
    let mut names = config
        .indexes
        .iter()
        .map(|index| index.name.as_str())
        .collect::<Vec<_>>();
    names.sort_by_key(|name| std::cmp::Reverse(name.len()));
    names
}

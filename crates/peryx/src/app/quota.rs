//! Quota status reads over the local store: a table of every repository, or one repository as JSON.
//!
//! Both resolve the configured indexes so a repository's limits come from its policy, and read the
//! committed and reserved counters the store maintains rather than scanning artifacts. `list` prints a
//! tab-separated row per repository; `inspect` prints one repository's full status as JSON. Neither
//! writes metadata.

use std::io::Write;

use anyhow::Context as _;
use peryx_driver::quota::repository_quota;

use super::CacheStores;
use crate::cli::{QuotaCommand, QuotaInspectArgs};
use crate::config::Config;
use crate::server;

/// Run a quota command against the configured store.
///
/// # Errors
/// Returns an error if the configured indexes cannot be built, the metadata store cannot be read, the
/// named index is unknown, or output fails.
pub fn quota(config: &Config, command: &QuotaCommand, out: &mut dyn Write) -> anyhow::Result<()> {
    let stores = CacheStores::open(config)?;
    let indexes = server::build_indexes(&config.indexes, &config.auth, config.offline)?;
    match command {
        QuotaCommand::List(_) => list(&stores, &indexes, out),
        QuotaCommand::Inspect(args) => inspect(&stores, &indexes, args, out),
    }
}

fn list(stores: &CacheStores, indexes: &[peryx_driver::Index], out: &mut dyn Write) -> anyhow::Result<()> {
    writeln!(
        out,
        "repository\tecosystem\tused_bytes\treserved_bytes\tbyte_limit\tremaining_bytes\tprojects\tproject_limit\taudit"
    )?;
    for index in indexes {
        let usage = read_usage(stores, &index.name)?;
        let status = repository_quota(index, &usage);
        writeln!(
            out,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            status.repository,
            status.ecosystem,
            status.accounted_bytes.committed,
            status.accounted_bytes.reserved,
            optional(status.accounted_bytes.limit),
            optional(status.accounted_bytes.remaining),
            status.projects.committed,
            optional(status.projects.limit),
            status.limits.audit,
        )?;
    }
    Ok(())
}

fn inspect(
    stores: &CacheStores,
    indexes: &[peryx_driver::Index],
    args: &QuotaInspectArgs,
    out: &mut dyn Write,
) -> anyhow::Result<()> {
    let index = indexes
        .iter()
        .find(|index| index.name == args.index)
        .with_context(|| format!("unknown index {:?}", args.index))?;
    let usage = read_usage(stores, &index.name)?;
    let status = repository_quota(index, &usage);
    writeln!(out, "{}", serde_json::to_string_pretty(&status)?)?;
    Ok(())
}

fn read_usage(stores: &CacheStores, name: &str) -> anyhow::Result<peryx_storage::meta::QuotaUsage> {
    stores
        .meta
        .quota_usage(name)
        .with_context(|| format!("read quota counters for {name:?}"))
}

fn optional(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

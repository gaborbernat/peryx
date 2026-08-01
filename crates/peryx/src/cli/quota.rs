//! Quota status commands. Both read the local store directly and derive limits from each index's
//! policy, the same status the HTTP surface reports.

use clap::{Args, Subcommand};

use super::RuntimeArgs;

/// Report configured limits and committed and reserved use per repository.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum QuotaCommand {
    /// List every repository's quota as a table.
    List(QuotaListArgs),
    /// Inspect one repository's quota as JSON.
    Inspect(QuotaInspectArgs),
}

impl QuotaCommand {
    #[must_use]
    pub const fn runtime_args(&self) -> &RuntimeArgs {
        match self {
            Self::List(args) => &args.runtime,
            Self::Inspect(args) => &args.runtime,
        }
    }
}

/// Options for the repository quota table.
#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct QuotaListArgs {
    #[command(flatten)]
    pub runtime: RuntimeArgs,
}

/// Options for a single repository's quota.
#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct QuotaInspectArgs {
    #[command(flatten)]
    pub runtime: RuntimeArgs,

    /// Index name to inspect.
    #[arg(long)]
    pub index: String,
}

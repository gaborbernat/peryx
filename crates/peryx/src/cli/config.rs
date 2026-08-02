//! The `config` command group: validate a resolved configuration before a restart.

use clap::{Args, Subcommand};

use super::RuntimeArgs;

/// Inspect a resolved configuration.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum ConfigCommand {
    /// Resolve the configuration from every source and report whether the server would accept it,
    /// without opening the data directory, binding a socket, or reaching an upstream.
    Check(ConfigCheckArgs),
}

impl ConfigCommand {
    #[must_use]
    pub const fn runtime_args(&self) -> &RuntimeArgs {
        match self {
            Self::Check(args) => &args.runtime,
        }
    }
}

/// Options for `peryx config check`.
#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct ConfigCheckArgs {
    #[command(flatten)]
    pub runtime: RuntimeArgs,
}

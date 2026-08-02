//! The `config check` command: preflight a resolved configuration.

use std::io::Write;

use crate::config::{Config, TlsConfig};
use crate::server;

/// Run `peryx config check`: report whether the server would accept this configuration.
///
/// # Errors
/// Returns the configuration error the server would hit while assembling its state, or an output
/// error while writing the summary.
pub fn config_check(config: &Config, out: &mut dyn Write) -> anyhow::Result<()> {
    server::check_config(config)?;
    writeln!(out, "configuration is valid")?;
    let scheme = match &config.tls {
        None => "http",
        Some(TlsConfig::Manual { .. }) => "https",
        Some(TlsConfig::Acme(_)) => "https+acme",
    };
    writeln!(out, "  listen: {scheme}://{}:{}", config.host, config.port)?;
    let count = config.indexes.len();
    let plural = if count == 1 { "" } else { "es" };
    writeln!(out, "  indexes: {count} configured index{plural}")?;
    Ok(())
}

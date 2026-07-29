//! First-administrator bootstrap without starting request-serving state.

use std::io::{Read, Write};

use anyhow::{Context as _, bail};
use peryx_driver::users::UserService;
use peryx_events::security::Event;
use peryx_storage::meta::MetaStore;

use crate::cli::BootstrapAdministratorArgs;
use crate::config::Config;

const MAX_PASSWORD_BYTES: usize = 1_048_576;
const MAX_PASSWORD_CHARACTERS: usize = 1_024;
const MIN_PASSWORD_CHARACTERS: usize = 15;

/// Create the first administrator from a bounded secret input and print its stable identity.
///
/// # Errors
/// Returns an error for invalid secret input, a read-only runtime, password derivation failure, an
/// existing administrator, a name conflict, a metadata failure, or an output failure.
pub fn bootstrap_administrator(
    config: &Config,
    args: &BootstrapAdministratorArgs,
    stdin: &mut dyn Read,
    out: &mut dyn Write,
) -> anyhow::Result<()> {
    if config.read_only {
        bail!("cannot bootstrap an administrator in read-only mode");
    }
    super::init_data_dir(&config.data_dir)
        .with_context(|| format!("initialize data directory {}", config.data_dir.display()))?;
    let password = if let Some(path) = &args.password_file {
        read_password(&mut std::fs::File::open(path).with_context(|| format!("open password file {}", path.display()))?)
            .context(format!("read password file {}", path.display()))?
    } else {
        read_password(stdin).context("read password from standard input")?
    };
    let characters = password.chars().count();
    if characters < MIN_PASSWORD_CHARACTERS {
        bail!("administrator password must contain at least {MIN_PASSWORD_CHARACTERS} characters");
    }
    if characters > MAX_PASSWORD_CHARACTERS {
        bail!("administrator password must contain at most {MAX_PASSWORD_CHARACTERS} characters");
    }
    let path = config.data_dir.join("peryx.redb");
    let store = MetaStore::open(&path).with_context(|| format!("open metadata store {}", path.display()))?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let user = runtime.block_on(UserService::new(store).bootstrap_administrator(&args.display_name, &password))?;
    Event::new("administrator_bootstrap", "success").emit();
    writeln!(out, "administrator\t{}\t{}", user.id, user.name.display())?;
    Ok(())
}

fn read_password(input: &mut dyn Read) -> anyhow::Result<String> {
    let mut bytes = Vec::new();
    input.take((MAX_PASSWORD_BYTES + 1) as u64).read_to_end(&mut bytes)?;
    if bytes.len() > MAX_PASSWORD_BYTES {
        bail!("password input exceeds the {MAX_PASSWORD_BYTES}-byte limit");
    }
    if bytes.ends_with(b"\r\n") {
        bytes.truncate(bytes.len() - 2);
    } else if bytes.ends_with(b"\n") {
        bytes.pop();
    }
    String::from_utf8(bytes).context("password input must be UTF-8")
}

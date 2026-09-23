use std::process::Command;
use std::sync::Arc;

use anyhow::{Context as _, anyhow};

use peryx_bench_core::servers::Server;

const UVX_ENV: &str = "PERYX_BENCH_UVX";
const BENCH_PYTHON: &str = "3.14.7";
const DEVPI_SERVER: &str = "devpi-server==6.20.3";
const DEVPI_CLIENT: &str = "devpi-client==7.3.0";
const PROXPI: &str = "proxpi==1.3.0";
const GUNICORN: &str = "gunicorn==26.2.0";
const PYPISERVER: &str = "pypiserver[passlib]==2.4.2";
const PYPICLOUD: &str = "pypicloud==1.3.12";
const PYPICLOUD_PYTHON: &str = "3.10.21";
const SQLALCHEMY: &str = "sqlalchemy==1.4.54";
const WAITRESS: &str = "waitress==3.0.2";

/// Every party the tables compare, `direct` being the no-proxy baseline.
#[must_use]
pub fn all(upstream: &str) -> Vec<Server> {
    vec![
        peryx(upstream),
        direct(upstream),
        devpi(upstream),
        proxpi(upstream),
        pypiserver(upstream),
        pypicloud(upstream),
    ]
}

pub(super) fn component_versions(servers: &[Server]) -> std::collections::BTreeMap<String, String> {
    let selected = servers
        .iter()
        .map(|server| server.name)
        .collect::<std::collections::BTreeSet<_>>();
    [
        ("devpi", "devpi-client", pinned(DEVPI_CLIENT)),
        ("proxpi", "gunicorn", pinned(GUNICORN)),
        ("pypicloud", "pypicloud-python", PYPICLOUD_PYTHON),
        ("pypicloud", "sqlalchemy", pinned(SQLALCHEMY)),
        ("pypicloud", "waitress", pinned(WAITRESS)),
    ]
    .into_iter()
    .filter(|(server, _, _)| selected.contains(server))
    .map(|(_, component, version)| (component.to_owned(), version.to_owned()))
    .collect()
}

fn pinned(requirement: &'static str) -> &'static str {
    requirement
        .split_once("==")
        .expect("benchmark requirements are pinned")
        .1
}

fn peryx(upstream: &str) -> Server {
    let upstream = upstream.to_owned();
    Server {
        name: "peryx",
        homepage: "https://peryx.readthedocs.io/",
        version: env!("CARGO_PKG_VERSION"),
        base_url: Arc::new(|port| format!("http://127.0.0.1:{port}/root/pypi/simple/")),
        probe: Arc::new(str::to_owned),
        command: Some(Arc::new(|context, port, state| {
            let mut command = Command::new(context.peryx_binary());
            command
                .arg("serve")
                .args(["--host", "127.0.0.1"])
                .args(["--port", &port.to_string()])
                .arg("--data-dir")
                .arg(state)
                .arg("--config")
                .arg(state.join("peryx.toml"));
            command
        })),
        setup: Some(Arc::new(move |_port, state| {
            std::fs::write(
                state.join("peryx.toml"),
                format!(
                    "[[index]]\nname = \"pypi\"\necosystem = \"pypi\"\n\
                     [[index.upstream]]\nname = \"fixture\"\nurl = \"{upstream}\"\n\
                     trusted_hosts = [\"127.0.0.1\"]\n\n\
                     [[index]]\nname = \"root-pypi\"\nroute = \"root/pypi\"\n\
                     ecosystem = \"pypi\"\nlayers = [\"pypi\"]\n"
                ),
            )
            .context("peryx.toml")
        })),
        configure: None,
        teardown: None,
    }
}

fn direct(upstream: &str) -> Server {
    let upstream = upstream.to_owned();
    Server {
        name: "direct",
        homepage: "https://pypi.org/",
        version: "fixture-v1",
        base_url: Arc::new(move |_port| upstream.clone()),
        probe: Arc::new(str::to_owned),
        command: None,
        setup: None,
        configure: None,
        teardown: None,
    }
}

fn devpi(upstream: &str) -> Server {
    let upstream = upstream.to_owned();
    Server {
        name: "devpi",
        homepage: "https://devpi.net/docs/",
        version: pinned(DEVPI_SERVER),
        base_url: Arc::new(|port| format!("http://127.0.0.1:{port}/root/pypi/+simple/")),
        probe: Arc::new(str::to_owned),
        command: Some(Arc::new(|_context, port, state| {
            let mut command = Command::new(uvx_program());
            command
                .args(["--python", BENCH_PYTHON, "--from", DEVPI_SERVER, "devpi-server"])
                .arg("--serverdir")
                .arg(state)
                .args(["--port", &port.to_string()]);
            command
        })),
        setup: Some(Arc::new(|_port, state| {
            let output = Command::new(uvx_program())
                .args([
                    "--python",
                    BENCH_PYTHON,
                    "--from",
                    DEVPI_SERVER,
                    "devpi-init",
                    "--serverdir",
                ])
                .arg(state)
                .output()
                .context("devpi-init did not start")?;
            check_devpi_init(&output)
        })),
        configure: Some(Arc::new(move |base, state| configure_devpi(base, state, &upstream))),
        teardown: None,
    }
}

fn check_devpi_init(output: &std::process::Output) -> anyhow::Result<()> {
    if output.status.success() {
        Ok(())
    } else {
        Err(anyhow!(
            "devpi-init failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn configure_devpi(base: &str, state: &std::path::Path, upstream: &str) -> anyhow::Result<()> {
    let server = base.split("root/pypi/").next().context("devpi base URL has no index")?;
    let mirror = format!("mirror_url={upstream}");
    for arguments in [
        &["use", server][..],
        &["login", "root", "--password", ""],
        &["index", "root/pypi", &mirror],
    ] {
        let output = Command::new(uvx_program())
            .args(["--python", BENCH_PYTHON, "--from", DEVPI_CLIENT, "devpi", "--clientdir"])
            .arg(state.join("client"))
            .args(arguments)
            .output()
            .context("devpi configuration did not start")?;
        if !output.status.success() {
            return Err(anyhow!(
                "devpi configuration failed:\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    Ok(())
}

fn proxpi(upstream: &str) -> Server {
    let upstream = upstream.to_owned();
    Server {
        name: "proxpi",
        homepage: "https://github.com/EpicWink/proxpi",
        version: pinned(PROXPI),
        base_url: Arc::new(|port| format!("http://127.0.0.1:{port}/index/")),
        probe: Arc::new(str::to_owned),
        command: Some(Arc::new(move |_context, port, state| {
            let mut command = Command::new(uvx_program());
            command
                .args([
                    "--python",
                    BENCH_PYTHON,
                    "--from",
                    PROXPI,
                    "--with",
                    GUNICORN,
                    "gunicorn",
                ])
                .args(["--bind", &format!("127.0.0.1:{port}")])
                .args(["--workers", "4", "proxpi.server:app"])
                .env("PROXPI_INDEX_URL", &upstream)
                // Each gunicorn worker keeps a private download map, so a shared PROXPI_CACHE_DIR lets two workers
                // write the same file at once and serve it corrupt. A per-round TMPDIR keeps each worker's own
                // mkdtemp cache, on the scratch volume, removed with the round.
                .env("TMPDIR", state);
            command
        })),
        setup: None,
        configure: None,
        teardown: None,
    }
}

fn pypiserver(upstream: &str) -> Server {
    let upstream = upstream.to_owned();
    Server {
        name: "pypiserver",
        homepage: "https://github.com/pypiserver/pypiserver",
        version: pinned(PYPISERVER),
        base_url: Arc::new(|port| format!("http://127.0.0.1:{port}/simple/")),
        probe: Arc::new(str::to_owned),
        command: Some(Arc::new(move |_context, port, state| {
            let mut command = Command::new(uvx_program());
            command
                .args(["--python", BENCH_PYTHON, "--from", PYPISERVER, "pypi-server", "run"])
                .args(["-p", &port.to_string()])
                .args(["--fallback-url", &upstream])
                .args(["-P", ".", "-a", "."])
                .arg(state);
            command
        })),
        setup: None,
        configure: None,
        teardown: None,
    }
}

fn pypicloud(upstream: &str) -> Server {
    let upstream = upstream.to_owned();
    Server {
        name: "pypicloud",
        homepage: "https://pypicloud.readthedocs.io/",
        version: pinned(PYPICLOUD),
        base_url: Arc::new(|port| format!("http://127.0.0.1:{port}/simple/")),
        probe: Arc::new(str::to_owned),
        command: Some(Arc::new(|_context, _port, state| {
            let mut command = Command::new(uvx_program());
            command
                .args(["--python", PYPICLOUD_PYTHON, "--from", PYPICLOUD])
                .args(["--with", SQLALCHEMY, "--with", WAITRESS, "pserve"])
                .arg(state.join("pypicloud.ini"));
            command
        })),
        setup: Some(Arc::new(move |port, state| {
            // pypicloud's `fallback = cache` mode is the closest analog to a read-through cache.
            let fallback_base = upstream
                .strip_suffix("/simple/")
                .context("pypicloud upstream is not a Simple API URL")?;
            let ini = format!(
                "[app:main]\n\
                     use = egg:pypicloud\n\
                     pyramid.reload_templates = False\n\
                     pypi.fallback = cache\n\
                     pypi.fallback_base_url = {fallback_base}\n\
                     pypi.default_read = everyone\n\
                     pypi.cache_update = everyone\n\
                     pypi.storage = file\n\
                     storage.dir = {packages}\n\
                     db.url = sqlite:///{db}\n\
                     session.encrypt_key = {zeros}\n\
                     session.validate_key = {zeros}\n\
                     auth.admins =\n\
                     \n\
                     [server:main]\n\
                     use = egg:waitress#main\n\
                     host = 127.0.0.1\n\
                     port = {port}\n\
                     threads = 8\n",
                packages = state.join("packages").display(),
                db = state.join("db.sqlite").display(),
                zeros = "0".repeat(64),
            );
            std::fs::write(state.join("pypicloud.ini"), ini).context("pypicloud.ini")
        })),
        configure: None,
        teardown: None,
    }
}

fn uvx_program() -> std::ffi::OsString {
    std::env::var_os(UVX_ENV).unwrap_or_else(|| "uvx".into())
}

#[cfg(test)]
#[path = "../../tests/unit/bench/workloads/servers.rs"]
mod tests;

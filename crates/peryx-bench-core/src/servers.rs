//! The concrete servers under test and their index-URL shapes are per-ecosystem definitions; this
//! module only spawns, health-checks, and tears them down.

use std::ops::Range;
#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::{Context as _, bail};

use crate::context::BenchmarkContext;

#[cfg(test)]
#[path = "../tests/unit/servers.rs"]
mod tests;

/// How long a server gets to answer its first request (uvx may resolve an environment first).
const START_TIMEOUT: Duration = Duration::from_mins(3);

/// Override these deadlines for competitors that need setup before they can answer.
#[derive(Clone, Copy)]
pub struct StartupPolicy {
    pub timeout: Duration,
    pub request_timeout: Duration,
    pub poll_interval: Duration,
}

impl Default for StartupPolicy {
    fn default() -> Self {
        Self {
            timeout: START_TIMEOUT,
            request_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_millis(300),
        }
    }
}

/// # Errors
/// Returns an error when reqwest cannot build the client.
pub fn http_client() -> anyhow::Result<reqwest::Client> {
    client_with_timeouts(CONNECT_TIMEOUT, READ_TIMEOUT)
}

/// A loopback connect that takes longer than this is a server that will not answer.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// The timer restarts with every body chunk, so a large wheel can stream for minutes while a connection that stays
/// silent this long fails.
const READ_TIMEOUT: Duration = Duration::from_mins(1);

fn client_with_timeouts(connect: Duration, read: Duration) -> anyhow::Result<reqwest::Client> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    Ok(reqwest::Client::builder()
        .connect_timeout(connect)
        .read_timeout(read)
        .build()?)
}

pub type BaseUrl = Arc<dyn Fn(u16) -> String + Send + Sync>;
pub type Probe = Arc<dyn Fn(&str) -> String + Send + Sync>;
pub type ServerCommand = Arc<dyn Fn(&BenchmarkContext, u16, &Path) -> Command + Send + Sync>;
pub type ServerSetup = Arc<dyn Fn(u16, &Path) -> anyhow::Result<()> + Send + Sync>;
pub type ServerConfigure = Arc<dyn Fn(&str, &Path) -> anyhow::Result<()> + Send + Sync>;

/// One index server under test; every field is filled in by a per-ecosystem definition.
pub struct Server {
    pub name: &'static str,
    pub homepage: &'static str,
    pub version: &'static str,
    pub base_url: BaseUrl,
    /// The readiness URL derived from the base, hit until any HTTP status answers.
    pub probe: Probe,
    pub command: Option<ServerCommand>,
    pub setup: Option<ServerSetup>,
    pub configure: Option<ServerConfigure>,
    /// Teardown after the spawned process is killed, keyed by port. A container competitor detaches
    /// from the process that launched it, so killing that process is not enough; this removes it.
    pub teardown: Option<fn(u16)>,
}

/// A started server: where to reach it and the process behind it (none for direct).
pub struct Active {
    pub url: String,
    process: Option<Child>,
    log: Option<PathBuf>,
    probe_url: String,
    port: u16,
    teardown: Option<fn(u16)>,
}

impl Active {
    pub fn pid(&self) -> Option<u32> {
        self.process.as_ref().map(Child::id)
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        if let Some(mut process) = self.process.take() {
            // gunicorn forks workers, and a `uvx` shim execs its payload: killing the direct child
            // orphans the rest, which then linger holding CPU and skewing every later measurement.
            // The child leads its own process group (see `start`), so signal the whole group.
            let _ = kill_process_group(&process);
            let _ = process.kill();
            let _ = process.wait();
        }
        if let Some(teardown) = self.teardown {
            teardown(self.port);
        }
    }
}

// Shelling out to `kill -KILL -<pgid>` took the whole GitHub-hosted runner down with the group after
// every cold build, three runs out of three; the syscall reaches the group and nothing else.
fn kill_process_group(process: &Child) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let group = rustix::process::Pid::from_child(process);
        rustix::process::kill_process_group(group, rustix::process::Signal::KILL)?;
    }
    #[cfg(not(unix))]
    let _ = process;
    Ok(())
}

impl Server {
    /// Start this server against `state` and wait until it answers.
    ///
    /// # Errors
    /// Returns an error when the server exits early or never becomes ready; includes its log tail.
    pub async fn start(
        &self,
        context: &BenchmarkContext,
        state: &Path,
        client: &reqwest::Client,
    ) -> anyhow::Result<Active> {
        self.start_with_policy(context, state, client, StartupPolicy::default())
            .await
    }

    /// # Errors
    /// Returns an error when setup, process startup, or readiness fails.
    pub async fn start_with_policy(
        &self,
        context: &BenchmarkContext,
        state: &Path,
        client: &reqwest::Client,
        policy: StartupPolicy,
    ) -> anyhow::Result<Active> {
        let port = free_port()?;
        let url = (self.base_url)(port);
        let probe_url = (self.probe)(&url);
        let Some(command) = &self.command else {
            return Ok(Active {
                url,
                process: None,
                log: None,
                probe_url,
                port,
                teardown: None,
            });
        };
        if let Some(setup) = &self.setup {
            setup(port, state)?;
        }
        let log = state.join("server.log");
        let sink = std::fs::File::create(&log)?;
        let mut spawned = command(context, port, state);
        spawned.stdout(Stdio::from(sink.try_clone()?)).stderr(Stdio::from(sink));
        // Lead a fresh process group so teardown can reap forked workers along with the parent.
        #[cfg(unix)]
        spawned.process_group(0);
        let process = spawned
            .spawn()
            .with_context(|| format!("{} did not start", self.name))?;
        let mut active = Active {
            url,
            process: Some(process),
            log: Some(log),
            probe_url,
            port,
            teardown: self.teardown,
        };
        active.wait_ready(client, policy).await.with_context(|| {
            let tail = active
                .log
                .as_ref()
                .and_then(|log| std::fs::read_to_string(log).ok())
                .unwrap_or_default();
            format!("{}; server log tail:\n{}", self.name, last_chars(&tail, 2000))
        })?;
        if let Some(configure) = &self.configure {
            configure(&active.url, state)?;
        }
        Ok(active)
    }
}

impl Active {
    async fn wait_ready(&mut self, client: &reqwest::Client, policy: StartupPolicy) -> anyhow::Result<()> {
        let probe = self.probe_url.clone();
        let polling = async {
            loop {
                self.ensure_running()?;
                // Any HTTP status means the server is up and routing; only transport errors retry.
                if client.get(&probe).timeout(policy.request_timeout).send().await.is_ok() {
                    return anyhow::Ok(());
                }
                self.ensure_running()?;
                tokio::time::sleep(policy.poll_interval).await;
            }
        };
        let outcome = tokio::time::timeout(policy.timeout, polling).await;
        if let Ok(ready) = outcome {
            return ready;
        }
        self.ensure_running()?;
        bail!("server never answered at {probe}")
    }

    fn ensure_running(&mut self) -> anyhow::Result<()> {
        if let Some(process) = self.process.as_mut()
            && let Some(status) = process.try_wait()?
        {
            bail!("server exited early with {status}");
        }
        Ok(())
    }
}

/// Outgoing connections draw source ports from the ephemeral range (from 32768 on Linux, 49152 on macOS and Windows),
/// so a probed port below it stays free until the server binds it. The range also avoids peryx's 20000-29999 test band.
const SERVER_PORTS: Range<u16> = 10_000..20_000;

/// Seeded from the pid, scattered by a large odd multiplier: nextest gives consecutive tests consecutive pids, which
/// would otherwise start probing at adjacent ports and hand out the same one.
static NEXT_PORT: LazyLock<AtomicUsize> =
    LazyLock::new(|| AtomicUsize::new((std::process::id() as usize).wrapping_mul(0x9E37_79B9)));

fn free_port() -> anyhow::Result<u16> {
    free_port_in(SERVER_PORTS)
}

fn free_port_in(ports: Range<u16>) -> anyhow::Result<u16> {
    // Every probe claims its own cursor value, so no two callers are ever handed the same port.
    (0..ports.len())
        .map(|_| {
            ports.start
                + u16::try_from(NEXT_PORT.fetch_add(1, Ordering::Relaxed) % ports.len())
                    .expect("the offset fits the range")
        })
        .find(|&port| std::net::TcpListener::bind(("127.0.0.1", port)).is_ok())
        .with_context(|| format!("no free port in {ports:?}"))
}

fn last_chars(text: &str, count: usize) -> &str {
    let start = text.len().saturating_sub(count);
    let boundary = (start..text.len())
        .find(|&index| text.is_char_boundary(index))
        .unwrap_or(0);
    &text[boundary..]
}

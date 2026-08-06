//! A minimal Toxiproxy client and a managed `toxiproxy-server` process, so a test can put a controllable
//! proxy in front of a node's socket and inject network faults.
//!
//! The harness owns the `toxiproxy-server` lifecycle the way it owns the peryx nodes: [`Toxiproxy::start`]
//! spawns one on its own control port and [`Drop`] kills it, so a run leaks no proxy process. The client
//! talks to the server's HTTP API (create and delete proxies, disable and re-enable a link, add a
//! latency toxic) over `reqwest::blocking`, keeping the harness synchronous.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::json;

use super::{HarnessError, free_port, kill_group, spawn_in_group};

const CONTROL_HOST: &str = "127.0.0.1";
const START_TIMEOUT: Duration = Duration::from_secs(10);

/// A running `toxiproxy-server` and a client bound to its control API.
pub struct Toxiproxy {
    server: Child,
    control: String,
    http: reqwest::blocking::Client,
    next: u32,
}

impl Toxiproxy {
    /// Spawn `toxiproxy-server` on a free control port and wait until its API answers.
    ///
    /// # Errors
    /// Returns [`HarnessError::Toxiproxy`] when the binary is absent or its API does not come up within
    /// the start deadline.
    pub fn start() -> Result<Self, HarnessError> {
        let control_port = free_port();
        let control = format!("http://{CONTROL_HOST}:{control_port}");
        let mut command = Command::new("toxiproxy-server");
        command
            .args(["-host", CONTROL_HOST, "-port", &control_port.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        spawn_in_group(&mut command);
        let server = command
            .spawn()
            .map_err(|error| HarnessError::Toxiproxy(format!("spawn toxiproxy-server (is it installed?): {error}")))?;
        let http = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .map_err(|error| HarnessError::Toxiproxy(format!("build http client: {error}")))?;
        let toxiproxy = Self {
            server,
            control,
            http,
            next: 0,
        };
        toxiproxy.await_control()?;
        Ok(toxiproxy)
    }

    fn await_control(&self) -> Result<(), HarnessError> {
        let deadline = Instant::now() + START_TIMEOUT;
        while Instant::now() < deadline {
            if self.http.get(format!("{}/version", self.control)).send().is_ok() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        Err(HarnessError::Toxiproxy("control API never came up".to_owned()))
    }

    /// Create a proxy that listens on a fresh loopback port and forwards to `upstream` (a `host:port`).
    ///
    /// # Errors
    /// Returns [`HarnessError::Toxiproxy`] when the server rejects the proxy.
    pub fn proxy(&mut self, upstream: &str) -> Result<Proxy, HarnessError> {
        self.next += 1;
        let name = format!("peryx-{}", self.next);
        let listen = format!("{CONTROL_HOST}:{}", free_port());
        self.post(
            "/proxies",
            &json!({ "name": name, "listen": listen, "upstream": upstream, "enabled": true }),
        )?;
        Ok(Proxy {
            listen,
            name,
            control: self.control.clone(),
            http: self.http.clone(),
        })
    }

    fn post(&self, path: &str, body: &serde_json::Value) -> Result<(), HarnessError> {
        let response = self
            .http
            .post(format!("{}{path}", self.control))
            .json(body)
            .send()
            .map_err(|error| HarnessError::Toxiproxy(format!("POST {path}: {error}")))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(HarnessError::Toxiproxy(format!(
                "POST {path} returned {}",
                response.status()
            )))
        }
    }

    /// Whether the control API still answers, so a test can prove it detects a dead proxy server.
    #[must_use]
    pub fn control_is_up(&self) -> bool {
        self.http.get(format!("{}/version", self.control)).send().is_ok()
    }

    /// Kill the proxy server, so a test can observe a proxy failure.
    pub fn kill(&mut self) {
        kill_group(&mut self.server);
    }
}

impl Drop for Toxiproxy {
    fn drop(&mut self) {
        kill_group(&mut self.server);
    }
}

/// One proxy in front of a node's socket. Clients dial [`endpoint`](Proxy::endpoint); the harness cuts or
/// slows the link through the control API.
pub struct Proxy {
    listen: String,
    name: String,
    control: String,
    http: reqwest::blocking::Client,
}

impl Proxy {
    /// The `host:port` a client connects to instead of the node directly.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.listen
    }

    /// Cut the link: the proxy stops forwarding, so a connection through it fails. This is a partition.
    ///
    /// # Errors
    /// Returns [`HarnessError::Toxiproxy`] when the control call fails.
    pub fn partition(&self) -> Result<(), HarnessError> {
        self.set_enabled(false)
    }

    /// Restore the link after a [`partition`](Proxy::partition).
    ///
    /// # Errors
    /// Returns [`HarnessError::Toxiproxy`] when the control call fails.
    pub fn heal(&self) -> Result<(), HarnessError> {
        self.set_enabled(true)
    }

    /// Add `delay` of latency in both directions, so a test can pause a link without cutting it.
    ///
    /// # Errors
    /// Returns [`HarnessError::Toxiproxy`] when the control call fails.
    pub fn pause(&self, delay: Duration) -> Result<(), HarnessError> {
        let response = self
            .http
            .post(format!("{}/proxies/{}/toxics", self.control, self.name))
            .json(&json!({
                "name": "pause", "type": "latency", "stream": "downstream",
                "attributes": { "latency": u64::try_from(delay.as_millis()).unwrap_or(u64::MAX) },
            }))
            .send()
            .map_err(|error| HarnessError::Toxiproxy(format!("add latency toxic: {error}")))?;
        response
            .status()
            .is_success()
            .then_some(())
            .ok_or_else(|| HarnessError::Toxiproxy(format!("add latency toxic returned {}", response.status())))
    }

    /// Remove the latency toxic a [`pause`](Proxy::pause) added, so the link runs at full speed again.
    ///
    /// # Errors
    /// Returns [`HarnessError::Toxiproxy`] when the control call fails.
    pub fn resume(&self) -> Result<(), HarnessError> {
        let response = self
            .http
            .delete(format!("{}/proxies/{}/toxics/pause", self.control, self.name))
            .send()
            .map_err(|error| HarnessError::Toxiproxy(format!("remove latency toxic: {error}")))?;
        response
            .status()
            .is_success()
            .then_some(())
            .ok_or_else(|| HarnessError::Toxiproxy(format!("remove latency toxic returned {}", response.status())))
    }

    fn set_enabled(&self, enabled: bool) -> Result<(), HarnessError> {
        let response = self
            .http
            .post(format!("{}/proxies/{}", self.control, self.name))
            .json(&json!({ "enabled": enabled }))
            .send()
            .map_err(|error| HarnessError::Toxiproxy(format!("toggle proxy: {error}")))?;
        response
            .status()
            .is_success()
            .then_some(())
            .ok_or_else(|| HarnessError::Toxiproxy(format!("toggle proxy returned {}", response.status())))
    }
}

//! Outbound destination policy for URLs an upstream index controls.
//!
//! A Simple API response chooses the URLs peryx fetches for artifacts, core metadata, and
//! provenance. Left unchecked, a hostile index can steer those requests at loopback, private, or
//! cloud-metadata addresses (SSRF). [`OutboundGuard`] rejects any host that does not resolve to a
//! global address, so the connection never happens. Two operator-controlled origins stay reachable:
//! the configured upstream itself (its host is trusted automatically) and an explicit allowlist of
//! private artifact servers.
//!
//! Enforcement runs at two layers because a hostname check before DNS is open to rebinding. IP
//! literals are validated on the [`Url`] before the request leaves ([`OutboundGuard::check_url`]);
//! hostnames are validated on the address the resolver returns, which is the address the connection
//! uses ([`OutboundGuard`] as a [`Resolve`]).

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use url::{Host, Url};

use super::UpstreamError;

/// Decides which hosts peryx may connect to when following an upstream-controlled URL.
#[derive(Clone)]
pub struct OutboundGuard {
    trusted: Arc<HashSet<String>>,
    inner: Arc<dyn Resolve>,
}

impl std::fmt::Debug for OutboundGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OutboundGuard")
            .field("trusted", &self.trusted)
            .finish_non_exhaustive()
    }
}

impl OutboundGuard {
    /// Trust `base`'s host (the operator configured it) plus every entry in `trusted_hosts`, and
    /// resolve everything else through the system resolver.
    pub fn new<I, S>(base: &Url, trusted_hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::with_resolver(base, trusted_hosts, Arc::new(SystemResolver))
    }

    pub fn with_resolver<I, S>(base: &Url, trusted_hosts: I, inner: Arc<dyn Resolve>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut trusted = HashSet::new();
        if let Some(host) = base.host() {
            trusted.insert(canonical_host(&host));
        }
        for entry in trusted_hosts {
            if let Some(host) = canonical_entry(entry.as_ref()) {
                trusted.insert(host);
            }
        }
        Self {
            trusted: Arc::new(trusted),
            inner,
        }
    }

    /// Reject `url` before the request leaves: the scheme must be HTTP or HTTPS, and an IP-literal
    /// host must be trusted or global. Hostnames pass here and are checked once resolved.
    ///
    /// # Errors
    /// Returns [`UpstreamError::BlockedDestination`] when the scheme or the literal address is not
    /// allowed.
    pub fn check_url(&self, url: &Url) -> Result<(), UpstreamError> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(UpstreamError::BlockedDestination {
                reason: format!("scheme {:?} is not http or https", url.scheme()),
            });
        }
        let host = url.host();
        let Some(ip) = literal_ip(host.as_ref()) else {
            return Ok(());
        };
        if self.is_trusted(host.as_ref()) || is_global_ip(ip) {
            Ok(())
        } else {
            Err(UpstreamError::BlockedDestination {
                reason: format!("{ip} is not a public address"),
            })
        }
    }

    fn is_trusted(&self, host: Option<&Host<&str>>) -> bool {
        host.is_some_and(|host| self.trusted.contains(&canonical_host(host)))
    }
}

impl Resolve for OutboundGuard {
    fn resolve(&self, name: Name) -> Resolving {
        let trusted = self.trusted.contains(&name.as_str().to_ascii_lowercase());
        let inner = self.inner.resolve(name);
        Box::pin(async move {
            let kept = retain_allowed(trusted, inner.await?);
            if kept.is_empty() {
                Err("upstream host resolves only to non-public addresses".into())
            } else {
                Ok(Box::new(kept.into_iter()) as Addrs)
            }
        })
    }
}

fn retain_allowed(trusted: bool, addrs: Addrs) -> Vec<SocketAddr> {
    addrs.filter(|addr| trusted || is_global_ip(addr.ip())).collect()
}

const fn literal_ip(host: Option<&Host<&str>>) -> Option<IpAddr> {
    match host {
        Some(Host::Ipv4(ip)) => Some(IpAddr::V4(*ip)),
        Some(Host::Ipv6(ip)) => Some(IpAddr::V6(*ip)),
        Some(Host::Domain(_)) | None => None,
    }
}

fn canonical_host<S: AsRef<str>>(host: &Host<S>) -> String {
    match host {
        Host::Domain(name) => name.as_ref().to_ascii_lowercase(),
        Host::Ipv4(ip) => ip.to_string(),
        Host::Ipv6(ip) => ip.to_string(),
    }
}

fn canonical_entry(entry: &str) -> Option<String> {
    let entry = entry.trim();
    if entry.is_empty() {
        return None;
    }
    let inner = entry
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(entry);
    if let Ok(ip) = inner.parse::<IpAddr>() {
        return Some(ip.to_string());
    }
    Some(entry.to_ascii_lowercase())
}

pub const fn is_global_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_global_ipv4(ip),
        IpAddr::V6(ip) => is_global_ipv6(ip),
    }
}

const fn is_global_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    if a == 0 {
        return false;
    }
    if ip.is_private() {
        return false;
    }
    if is_shared_ipv4(ip) {
        return false;
    }
    if ip.is_loopback() {
        return false;
    }
    if ip.is_link_local() {
        return false;
    }
    if ip.is_multicast() {
        return false;
    }
    if ip.is_broadcast() {
        return false;
    }
    if ip.is_documentation() {
        return false;
    }
    if is_benchmarking_ipv4(ip) {
        return false;
    }
    if a == 192 && b == 0 {
        return false;
    }
    a < 240
}

const fn is_shared_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    a == 100 && (b & 0b1100_0000 == 0b0100_0000)
}

const fn is_benchmarking_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    a == 198 && (b & 0xfe == 18)
}

const fn is_global_ipv6(ip: Ipv6Addr) -> bool {
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return is_global_ipv4(mapped);
    }
    if ip.is_unspecified() {
        return false;
    }
    if ip.is_loopback() {
        return false;
    }
    let first = ip.segments()[0];
    if first & 0xff00 == 0xff00 {
        return false;
    }
    if first & 0xffc0 == 0xfe80 {
        return false;
    }
    if first & 0xfe00 == 0xfc00 {
        return false;
    }
    !is_documentation_ipv6(ip)
}

const fn is_documentation_ipv6(ip: Ipv6Addr) -> bool {
    let [first, second, ..] = ip.segments();
    first == 0x2001 && second == 0xdb8
}

struct SystemResolver;

impl Resolve for SystemResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let addrs = tokio::net::lookup_host((host, 0)).await?;
            Ok(Box::new(addrs) as Addrs)
        })
    }
}

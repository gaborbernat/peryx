//! Link classification for rendered owner content.

use url::{ParseError, Url};

pub(crate) const EXTERNAL_LINK_REL: &str = "external nofollow noopener noreferrer";

pub(crate) struct LinkDestination {
    pub(crate) href: String,
    pub(crate) rel: Option<&'static str>,
}

/// Reject characters the browser URL parser strips before it determines the scheme.
pub(crate) fn link_destination(href: String) -> Option<LinkDestination> {
    if href
        .chars()
        .any(|character| character.is_control() || character.is_whitespace())
    {
        return None;
    }
    let rel = if is_network_path_reference(&href) {
        Some(EXTERNAL_LINK_REL)
    } else {
        match Url::parse(&href) {
            Ok(url) if matches!(url.scheme(), "http" | "https") => Some(EXTERNAL_LINK_REL),
            Ok(url) if url.scheme() == "mailto" => None,
            Err(ParseError::RelativeUrlWithoutBase) => None,
            _ => return None,
        }
    };
    Some(LinkDestination { href, rel })
}

/// A `//host/path` network-path reference has no scheme, so `Url::parse` rejects it as relative even
/// though a browser resolves it to an off-host HTTP or HTTPS URL. Classify it as external so it never
/// passes as a same-origin route.
fn is_network_path_reference(target: &str) -> bool {
    target.starts_with("//")
}

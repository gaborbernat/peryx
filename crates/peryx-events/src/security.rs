//! Structured security events for index actions and server-role decisions.

use http::{HeaderMap, header};
use peryx_identity::{Identity, Principal, Scope, UserId};

const UNKNOWN: &str = "unknown";

/// A bounded reason for denying a role-based authorization decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationDenial {
    NoGrant,
    StorageUnavailable,
}

impl AuthorizationDenial {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NoGrant => "no_grant",
            Self::StorageUnavailable => "storage_unavailable",
        }
    }
}

/// Record a denied role authorization without accepting a resource path or query string.
pub fn authorization_denied(user: &UserId, scope: Scope, denial: AuthorizationDenial) {
    let user = user.as_str();
    let scope = scope.as_str();
    let reason = denial.as_str();
    tracing::info!(
        target: "peryx::security",
        security_event = true,
        event = "authorization",
        user,
        scope,
        result = "denied",
        reason,
        "role authorization denied"
    );
}

pub struct Event<'a> {
    action: &'static str,
    result: &'static str,
    actor: Option<&'a str>,
    publisher_id: Option<&'a str>,
    token_id: Option<&'a str>,
    index: Option<&'a str>,
    source_index: Option<&'a str>,
    hosted_index: Option<&'a str>,
    project: Option<&'a str>,
    version: Option<&'a str>,
    filename: Option<&'a str>,
    digest: Option<&'a str>,
    count: usize,
    changed: bool,
    reason: Option<&'a str>,
    request_id: Option<&'a str>,
    user_agent: Option<&'a str>,
}

impl<'a> Event<'a> {
    #[must_use]
    pub const fn new(action: &'static str, result: &'static str) -> Self {
        Self {
            action,
            result,
            actor: None,
            publisher_id: None,
            token_id: None,
            index: None,
            source_index: None,
            hosted_index: None,
            project: None,
            version: None,
            filename: None,
            digest: None,
            count: 0,
            changed: false,
            reason: None,
            request_id: None,
            user_agent: None,
        }
    }

    #[must_use]
    pub const fn actor(mut self, actor: Option<&'a str>) -> Self {
        self.actor = actor;
        self
    }

    #[must_use]
    pub const fn publisher_id(mut self, publisher_id: &'a str) -> Self {
        self.publisher_id = Some(publisher_id);
        self
    }

    #[must_use]
    pub const fn token_id(mut self, token_id: &'a str) -> Self {
        self.token_id = Some(token_id);
        self
    }

    #[must_use]
    pub const fn index(mut self, index: &'a str) -> Self {
        self.index = Some(index);
        self
    }

    #[must_use]
    pub const fn source_index(mut self, source_index: &'a str) -> Self {
        self.source_index = Some(source_index);
        self
    }

    #[must_use]
    pub const fn hosted_index(mut self, hosted_index: &'a str) -> Self {
        self.hosted_index = Some(hosted_index);
        self
    }

    #[must_use]
    pub const fn project(mut self, project: Option<&'a str>) -> Self {
        self.project = project;
        self
    }

    #[must_use]
    pub const fn version(mut self, version: Option<&'a str>) -> Self {
        self.version = version;
        self
    }

    #[must_use]
    pub const fn filename(mut self, filename: Option<&'a str>) -> Self {
        self.filename = filename;
        self
    }

    #[must_use]
    pub const fn digest(mut self, digest: Option<&'a str>) -> Self {
        self.digest = digest;
        self
    }

    #[must_use]
    pub const fn count(mut self, count: usize) -> Self {
        self.count = count;
        self
    }

    #[must_use]
    pub const fn changed(mut self, changed: bool) -> Self {
        self.changed = changed;
        self
    }

    #[must_use]
    pub const fn reason(mut self, reason: Option<&'a str>) -> Self {
        self.reason = reason;
        self
    }

    #[must_use]
    pub fn request(mut self, headers: &'a HeaderMap) -> Self {
        self.request_id = request_id(headers);
        self.user_agent = user_agent(headers);
        self
    }

    pub fn emit(&self) {
        let actor = text(self.actor);
        let publisher_id = text(self.publisher_id);
        let token_id = text(self.token_id);
        let index = text(self.index);
        let source_index = text(self.source_index);
        let hosted_index = text(self.hosted_index);
        let project = text(self.project);
        let version = text(self.version);
        let filename = text(self.filename);
        let digest = text(self.digest);
        let reason = text(self.reason);
        let request_id = text(self.request_id);
        let user_agent = text(self.user_agent);
        tracing::info!(
            target: "peryx::security",
            security_event = true,
            event = "index_action",
            action = self.action,
            result = self.result,
            actor,
            publisher_id,
            token_id,
            index,
            source_index,
            hosted_index,
            project,
            version,
            filename,
            digest,
            count = self.count,
            changed = self.changed,
            reason,
            request_id,
            user_agent,
            "index security event"
        );
    }
}

/// Who to record an action against, from the identity the index's ACL already resolved.
///
/// The username a Basic credential carried names the actor whether or not it authenticated, so a
/// refused push still records who tried. A bearer credential carries no username; there the actor is
/// the principal the token names.
#[must_use]
pub fn actor(identity: &Identity) -> Option<String> {
    if let Some(user) = &identity.user {
        return Some(if user.is_empty() {
            UNKNOWN.to_owned()
        } else {
            user.clone()
        });
    }
    match &identity.principal {
        Principal::Named { subject } => Some(subject.clone()),
        Principal::Anonymous => None,
    }
}

fn request_id(headers: &HeaderMap) -> Option<&str> {
    header_str(headers, "x-request-id")
}

fn user_agent(headers: &HeaderMap) -> Option<&str> {
    header_str(headers, header::USER_AGENT.as_str())
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

fn text(value: Option<&str>) -> &str {
    value.unwrap_or("")
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Seek as _};
    use std::sync::Mutex;

    use peryx_identity::{Identity, Principal};
    use rstest::rstest;

    use super::AuthorizationDenial;

    fn presenting(user: &str) -> Identity {
        Identity {
            principal: Principal::Anonymous,
            user: Some(user.to_owned()),
        }
    }

    #[test]
    fn test_actor_uses_the_presented_username() {
        assert_eq!(super::actor(&presenting("alice")).as_deref(), Some("alice"));
    }

    #[test]
    fn test_actor_calls_an_empty_username_unknown() {
        assert_eq!(super::actor(&presenting("")).as_deref(), Some("unknown"));
    }

    #[test]
    fn test_actor_falls_back_to_the_principal_when_no_username_was_presented() {
        let bearer = Identity {
            principal: Principal::Named {
                subject: "ci".to_owned(),
            },
            user: None,
        };
        assert_eq!(super::actor(&bearer).as_deref(), Some("ci"));
    }

    #[test]
    fn test_actor_is_none_for_an_anonymous_request() {
        let anonymous = Identity {
            principal: Principal::Anonymous,
            user: None,
        };
        assert_eq!(super::actor(&anonymous), None);
    }

    #[rstest]
    #[case::no_grant(AuthorizationDenial::NoGrant, "no_grant")]
    #[case::storage_unavailable(AuthorizationDenial::StorageUnavailable, "storage_unavailable")]
    fn test_authorization_denial_event_contains_only_bounded_context(
        #[case] denial: AuthorizationDenial,
        #[case] expected_reason: &str,
    ) {
        let mut capture = tempfile::tempfile().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_writer(Mutex::new(capture.try_clone().unwrap()))
            .finish();
        let user = peryx_identity::UserId::random();

        tracing::subscriber::with_default(subscriber, || {
            super::authorization_denied(&user, peryx_identity::Scope::OperatorRead, denial);
        });

        capture.rewind().unwrap();
        let mut text = String::new();
        capture.read_to_string(&mut text).unwrap();
        let event: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(
            event["fields"],
            serde_json::json!({
                "message": "role authorization denied",
                "security_event": true,
                "event": "authorization",
                "user": user.as_str(),
                "scope": "operator:read",
                "result": "denied",
                "reason": expected_reason,
            })
        );
    }
}

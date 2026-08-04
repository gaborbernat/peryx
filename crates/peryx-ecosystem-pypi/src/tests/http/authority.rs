//! The publish path routes a first publish through the project's home authority: it claims the
//! normalized project's home on the first stored file, reads an existing home without a redundant claim,
//! and never lets a claim that cannot commit block the publish.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use peryx_driver::state::{HomeClaim, OwnershipAuthority, OwnershipError};

use super::support::*;

/// A stand-in ownership group that records which authorities the publish path checks and claims, so a
/// test asserts the routing without a real consensus round. When `fail_claim` is set, a claim is
/// attempted but cannot commit, standing in for an unreachable leader.
struct RecordingAuthority {
    homed: HashSet<String>,
    fail_claim: bool,
    checked: Mutex<Vec<String>>,
    claimed: Mutex<Vec<String>>,
}

impl RecordingAuthority {
    /// A group that homes no authority yet and wins every claim.
    fn unhomed() -> Arc<Self> {
        Self::new(HashSet::new(), false)
    }

    /// A group that already homed `authority`, so the publish path finds a home and claims nothing.
    fn already_homed(authority: &str) -> Arc<Self> {
        Self::new(HashSet::from([authority.to_owned()]), false)
    }

    /// A group that homes no authority yet but cannot commit a claim, so a claim is attempted and fails.
    fn failing() -> Arc<Self> {
        Self::new(HashSet::new(), true)
    }

    fn new(homed: HashSet<String>, fail_claim: bool) -> Arc<Self> {
        Arc::new(Self {
            homed,
            fail_claim,
            checked: Mutex::new(Vec::new()),
            claimed: Mutex::new(Vec::new()),
        })
    }

    fn checked(&self) -> Vec<String> {
        self.checked.lock().unwrap().clone()
    }

    fn claimed(&self) -> Vec<String> {
        self.claimed.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl OwnershipAuthority for RecordingAuthority {
    async fn has_home(&self, authority: &str) -> bool {
        self.checked.lock().unwrap().push(authority.to_owned());
        self.homed.contains(authority)
    }

    async fn claim_home(&self, authority: &str) -> Result<HomeClaim, OwnershipError> {
        self.claimed.lock().unwrap().push(authority.to_owned());
        if self.fail_claim {
            Err(OwnershipError::Unavailable("ownership group unreachable".to_owned()))
        } else {
            Ok(HomeClaim::AssignedHere)
        }
    }
}

/// Publish the sdist fixture (project `peryxpkg`) to the hosted index and return the response.
async fn publish(state: &std::sync::Arc<peryx_driver::AppState>) -> (StatusCode, String) {
    let (content_type, body) = multipart_body(&[], Some(("peryxpkg-1.0.tar.gz", &fixture_sdist())));
    post_upload_with_headers_response(
        state,
        "/root/pypi/",
        Some(&upload_auth()),
        &content_type,
        &[
            ("host", "peryx.test"),
            ("origin", "http://peryx.test"),
            ("x-peryx-csrf", "http://peryx.test"),
        ],
        body,
    )
    .await
}

#[tokio::test]
async fn test_first_publish_claims_the_projects_home() {
    let h = harness().await;
    let group = RecordingAuthority::unhomed();
    h.state.set_ownership_authority(group.clone());

    let (status, body) = publish(&h.state).await;

    assert_eq!((status, body.as_str()), (StatusCode::OK, "upload accepted"));
    assert_eq!(group.checked(), ["peryxpkg"], "the path reads the home before claiming");
    assert_eq!(
        group.claimed(),
        ["peryxpkg"],
        "the first stored file claims the normalized project's home",
    );
    // The claimed key is the normalized project key the store uses, so name variants route to one home.
    assert_eq!(h.state.meta.list_upload_entries("hosted", "peryxpkg").unwrap().len(), 1);
}

#[tokio::test]
async fn test_repeat_publish_of_a_homed_project_makes_no_claim() {
    let h = harness().await;
    let group = RecordingAuthority::already_homed("peryxpkg");
    h.state.set_ownership_authority(group.clone());

    let (status, _) = publish(&h.state).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(group.checked(), ["peryxpkg"], "the path reads the existing home");
    assert!(
        group.claimed().is_empty(),
        "an already-homed project costs one local read and no consensus round",
    );
}

#[tokio::test]
async fn test_a_home_claim_that_cannot_commit_does_not_block_the_publish() {
    let h = harness().await;
    let group = RecordingAuthority::failing();
    h.state.set_ownership_authority(group.clone());

    let (status, body) = publish(&h.state).await;

    assert_eq!(
        (status, body.as_str()),
        (StatusCode::OK, "upload accepted"),
        "a claim that cannot commit is logged, never surfaced, and never blocks the publish",
    );
    assert_eq!(group.claimed(), ["peryxpkg"], "the claim was attempted");
    assert_eq!(h.state.meta.list_upload_entries("hosted", "peryxpkg").unwrap().len(), 1);
}

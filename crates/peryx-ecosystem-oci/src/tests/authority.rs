//! The manifest push path routes a first publish through the repository's home authority: the first
//! push claims the repository's home, a later push finds the home already set and claims nothing, and a
//! claim that cannot commit never blocks the push.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode};
use peryx_driver::state::{HomeClaim, OwnershipAuthority, OwnershipError};

use super::{auth, hosted_writable, send_body};

const TOKEN: &str = "s3cret";
const MANIFEST_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
const MANIFEST: &[u8] = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}"#;

/// A stand-in ownership group that records which authorities the push path checks and claims. A won
/// claim homes the authority, so a repeat push over the same group finds the home already set. When
/// `fail_claim` is set, a claim is attempted but cannot commit, standing in for an unreachable leader.
struct RecordingAuthority {
    fail_claim: bool,
    homed: Mutex<HashSet<String>>,
    checked: Mutex<Vec<String>>,
    claimed: Mutex<Vec<String>>,
}

impl RecordingAuthority {
    fn unhomed() -> Arc<Self> {
        Self::new(false)
    }

    fn failing() -> Arc<Self> {
        Self::new(true)
    }

    fn new(fail_claim: bool) -> Arc<Self> {
        Arc::new(Self {
            fail_claim,
            homed: Mutex::new(HashSet::new()),
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
        self.homed.lock().unwrap().contains(authority)
    }

    async fn claim_home(&self, authority: &str) -> Result<HomeClaim, OwnershipError> {
        self.claimed.lock().unwrap().push(authority.to_owned());
        if self.fail_claim {
            Err(OwnershipError::Unavailable("ownership group unreachable".to_owned()))
        } else {
            self.homed.lock().unwrap().insert(authority.to_owned());
            Ok(HomeClaim::AssignedHere)
        }
    }
}

/// Push the fixture manifest to `store/app` under `reference` and return the response status.
async fn push(app: &axum::Router, reference: &str) -> StatusCode {
    let (status, _, _) = send_body(
        app,
        Method::PUT,
        &format!("/v2/store/app/manifests/{reference}"),
        &[("authorization", &auth(TOKEN)), ("content-type", MANIFEST_TYPE)],
        MANIFEST.to_vec(),
    )
    .await;
    status
}

#[tokio::test]
async fn test_first_manifest_push_claims_the_repositorys_home() {
    let dir = tempfile::tempdir().unwrap();
    let (state, app) = hosted_writable(&dir, TOKEN);
    let group = RecordingAuthority::unhomed();
    state.set_ownership_authority(group.clone());

    assert_eq!(push(&app, "v1").await, StatusCode::CREATED);

    assert_eq!(group.checked(), ["app"], "the path reads the home before claiming");
    assert_eq!(group.claimed(), ["app"], "the first push claims the repository's home");
}

#[tokio::test]
async fn test_repeat_manifest_push_makes_no_second_claim() {
    let dir = tempfile::tempdir().unwrap();
    let (state, app) = hosted_writable(&dir, TOKEN);
    let group = RecordingAuthority::unhomed();
    state.set_ownership_authority(group.clone());

    assert_eq!(push(&app, "v1").await, StatusCode::CREATED);
    assert_eq!(push(&app, "v2").await, StatusCode::CREATED);

    assert_eq!(group.checked(), ["app", "app"], "each push reads the home");
    assert_eq!(
        group.claimed(),
        ["app"],
        "only the first push claims; a homed repository costs no second consensus round",
    );
}

#[tokio::test]
async fn test_a_home_claim_that_cannot_commit_does_not_block_the_push() {
    let dir = tempfile::tempdir().unwrap();
    let (state, app) = hosted_writable(&dir, TOKEN);
    let group = RecordingAuthority::failing();
    state.set_ownership_authority(group.clone());

    assert_eq!(
        push(&app, "v1").await,
        StatusCode::CREATED,
        "a claim that cannot commit is logged, never surfaced, and never blocks the push",
    );
    assert_eq!(group.claimed(), ["app"], "the claim was attempted");
}

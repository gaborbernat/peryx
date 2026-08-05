//! Durable ingress admission for legacy `PyPI` uploads.
//!
//! A client uploads to whichever datacenter it reaches. Before an upload is stored, a durable write
//! intent binds it to the tenant, the ecosystem authority key, the digest, the size, the ingress DC, and
//! an operation id, and the configured backend must be able to prove same-datacenter durability. The
//! intent gives a retried upload one identity: an identical resend resolves the same intent instead of
//! staging its bytes twice, and a different-content resend of the same filename is refused as it is on
//! publication.
//!
//! Publication, home assignment, and cross-DC replication stay out of admission; they run downstream once
//! the ingress DC holds the upload durably.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use peryx_core::TopologyConfig;
use peryx_storage::blob::{DurabilityCapabilities, DurabilityRequirement, DurabilityShortfall};
use peryx_storage::meta::{IntentStageOutcome, MetaError, MetaStore};
use serde::{Deserialize, Serialize};

/// The most un-finalized intents an ingress node stages before it sheds load. Bounds the admission
/// backlog so a stalled home DC cannot let staged uploads grow without limit.
pub(super) const MAX_STAGED_INTENTS: usize = 65_536;

/// The datacenter id recorded for a single-node deployment that configures no roster.
const STANDALONE_DC: &str = "local";

/// The identity of an upload offered for ingress admission. The bytes are already staged; these fields
/// bind the durable intent that holds the upload near the client.
pub(super) struct AdmissionRequest<'a> {
    pub tenant: &'a str,
    pub authority: &'a str,
    pub filename: &'a str,
    pub digest: &'a str,
    pub size: u64,
    pub ingress_dc: &'a str,
}

/// The identity a staged intent binds, serialized as the intent payload so a recovered intent names the
/// tenant, authority, artifact, ingress DC, and operation id it was admitted for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct IngressIntent {
    tenant: String,
    authority: String,
    digest: String,
    size: u64,
    ingress_dc: String,
    operation: String,
}

/// The result of admitting an upload for durable ingress staging.
pub(super) enum Admission {
    /// The upload is durably staged and may proceed to storage.
    Admitted,
    /// The upload is refused: a conflicting resend, a full backlog, an unsupported backend, or a staging
    /// failure. Carries the response to return unchanged.
    Reject(Response),
}

/// Admit `request` for durable ingress staging into `meta`, allowing at most `capacity` un-finalized
/// intents and requiring `durability` to prove same-datacenter durability. The intent binds the upload's
/// identity so an identical resend deduplicates and a different-content resend of the same filename is
/// refused.
pub(super) fn admit(
    meta: &MetaStore,
    durability: DurabilityCapabilities,
    capacity: usize,
    request: &AdmissionRequest<'_>,
    now: i64,
) -> Admission {
    admit_staged(meta, durability, capacity, request, now).unwrap_or_else(|err| Admission::Reject(staging_failed(&err)))
}

fn admit_staged(
    meta: &MetaStore,
    durability: DurabilityCapabilities,
    capacity: usize,
    request: &AdmissionRequest<'_>,
    now: i64,
) -> Result<Admission, MetaError> {
    if let Err(reject) = durability_gate(durability) {
        return Ok(reject);
    }
    let key = intent_key(request.tenant, request.authority, request.filename);
    let payload = serde_json::to_vec(&IngressIntent {
        tenant: request.tenant.to_owned(),
        authority: request.authority.to_owned(),
        digest: request.digest.to_owned(),
        size: request.size,
        ingress_dc: request.ingress_dc.to_owned(),
        operation: format!("{key}:{}", request.digest),
    })
    .expect("an ingress intent serializes");
    let outcome = meta.stage_intent(&key, request.digest, request.size, &payload, capacity, now)?;
    Ok(match stage_gate(outcome, request.filename) {
        Ok(()) => Admission::Admitted,
        Err(reject) => reject,
    })
}

/// Same-datacenter durability must be provable before an upload is admitted: the ingress backend has to
/// commit race-safe, integrity-checked writes so a staged artifact cannot be silently clobbered or
/// corrupted before the home DC finalizes it.
fn durability_gate(durability: DurabilityCapabilities) -> Result<(), Admission> {
    durability
        .check(DurabilityRequirement::REPLICATED)
        .map_err(|shortfall| Admission::Reject(unsupported_durability(shortfall)))
}

/// Map the intent-ledger outcome to an admission decision: a fresh or identical intent proceeds, a
/// different-content resend of the same filename is refused with the file-conflict error publication
/// already returns, and a full backlog sheds load.
fn stage_gate(outcome: IntentStageOutcome, filename: &str) -> Result<(), Admission> {
    match outcome {
        IntentStageOutcome::Admitted | IntentStageOutcome::Duplicate => Ok(()),
        IntentStageOutcome::Conflict => Err(Admission::Reject(conflicting_content(filename))),
        IntentStageOutcome::RejectedOverLimit => Err(Admission::Reject(backlog_full())),
    }
}

/// The intent key binds an upload to its file identity within a tenant and authority, so an identical
/// resend deduplicates and a different-content resend of the same filename conflicts.
fn intent_key(tenant: &str, authority: &str, filename: &str) -> String {
    format!("pypi:{tenant}:{authority}:{filename}")
}

/// The datacenter this node stages into, read from the configured roster; a rosterless single node
/// stages under [`STANDALONE_DC`].
pub(super) fn ingress_dc(topology: &TopologyConfig) -> String {
    topology
        .local_node
        .as_deref()
        .and_then(|node| topology.members.iter().find(|member| member.node == node))
        .map_or_else(|| STANDALONE_DC.to_owned(), |member| member.dc.clone())
}

/// A different-content resend of a taken filename is refused with the response publication already
/// returns, so admission preserves the client-visible upload error.
fn conflicting_content(filename: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        format!("File already exists: {filename:?} has different content; use a different filename"),
    )
        .into_response()
}

fn backlog_full() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "ingress admission backlog is full").into_response()
}

fn unsupported_durability(shortfall: DurabilityShortfall) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        format!("same-datacenter durability unavailable: {shortfall}"),
    )
        .into_response()
}

fn staging_failed(err: &MetaError) -> Response {
    tracing::error!(error = ?err, "ingress admission staging failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "ingress admission failed").into_response()
}

#[cfg(test)]
mod tests {
    use peryx_core::{NodeRole, TopologyConfig, TopologyMember, TopologyMode};
    use peryx_storage::blob::DurabilityCapabilities;
    use peryx_storage::meta::MetaStore;
    use redb::TableDefinition;

    use super::*;

    fn meta(dir: &tempfile::TempDir) -> MetaStore {
        MetaStore::open(dir.path().join("meta.redb")).unwrap()
    }

    fn request<'a>(filename: &'a str, digest: &'a str) -> AdmissionRequest<'a> {
        AdmissionRequest {
            tenant: "root/hosted",
            authority: "flask",
            filename,
            digest,
            size: 11,
            ingress_dc: "dc-a",
        }
    }

    fn reject_status(admission: Admission) -> Option<StatusCode> {
        match admission {
            Admission::Reject(response) => Some(response.status()),
            Admission::Admitted => None,
        }
    }

    #[test]
    fn test_admit_stages_a_fresh_intent_bound_to_its_identity() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta(&dir);
        let request = request("flask-1.0.whl", "aa");

        assert!(matches!(
            admit(
                &meta,
                DurabilityCapabilities::FILESYSTEM,
                MAX_STAGED_INTENTS,
                &request,
                10
            ),
            Admission::Admitted
        ));

        let staged = meta
            .staged_intent("pypi:root/hosted:flask:flask-1.0.whl")
            .unwrap()
            .unwrap();
        assert_eq!((staged.digest.as_str(), staged.size), ("aa", 11));
        let intent: IngressIntent = serde_json::from_slice(&staged.payload).unwrap();
        assert_eq!(intent.ingress_dc, "dc-a");
        assert_eq!(intent.operation, "pypi:root/hosted:flask:flask-1.0.whl:aa");
    }

    #[test]
    fn test_admit_accepts_an_object_store_that_proves_durability() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta(&dir);

        let admission = admit(
            &meta,
            DurabilityCapabilities::object_store(true, true),
            MAX_STAGED_INTENTS,
            &request("flask-1.0.whl", "aa"),
            10,
        );

        assert_eq!(reject_status(admission), None);
        assert_eq!(meta.count_staged_intents().unwrap(), 1);
    }

    #[test]
    fn test_admit_deduplicates_an_identical_resend() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta(&dir);
        let request = request("flask-1.0.whl", "aa");

        assert!(matches!(
            admit(
                &meta,
                DurabilityCapabilities::FILESYSTEM,
                MAX_STAGED_INTENTS,
                &request,
                10
            ),
            Admission::Admitted
        ));
        assert!(matches!(
            admit(
                &meta,
                DurabilityCapabilities::FILESYSTEM,
                MAX_STAGED_INTENTS,
                &request,
                20
            ),
            Admission::Admitted
        ));

        assert_eq!(meta.count_staged_intents().unwrap(), 1);
    }

    #[test]
    fn test_admit_refuses_different_content_for_a_taken_filename() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta(&dir);
        admit(
            &meta,
            DurabilityCapabilities::FILESYSTEM,
            MAX_STAGED_INTENTS,
            &request("flask-1.0.whl", "aa"),
            10,
        );

        let admission = admit(
            &meta,
            DurabilityCapabilities::FILESYSTEM,
            MAX_STAGED_INTENTS,
            &request("flask-1.0.whl", "bb"),
            20,
        );

        assert_eq!(reject_status(admission), Some(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn test_admit_sheds_load_when_the_backlog_is_full() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta(&dir);
        admit(
            &meta,
            DurabilityCapabilities::FILESYSTEM,
            1,
            &request("flask-1.0.whl", "aa"),
            10,
        );

        let admission = admit(
            &meta,
            DurabilityCapabilities::FILESYSTEM,
            1,
            &request("click-8.0.whl", "bb"),
            20,
        );

        assert_eq!(reject_status(admission), Some(StatusCode::SERVICE_UNAVAILABLE));
    }

    #[test]
    fn test_admit_refuses_a_backend_that_cannot_prove_durability() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta(&dir);

        let admission = admit(
            &meta,
            DurabilityCapabilities::object_store(false, false),
            MAX_STAGED_INTENTS,
            &request("flask-1.0.whl", "aa"),
            10,
        );

        assert_eq!(reject_status(admission), Some(StatusCode::SERVICE_UNAVAILABLE));
        assert_eq!(meta.count_staged_intents().unwrap(), 0);
    }

    #[test]
    fn test_admit_surfaces_a_store_fault_as_internal_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.redb");
        {
            let db = redb::Database::create(&path).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn
                    .open_table(TableDefinition::<&str, &[u8]>::new("ingress_intent"))
                    .unwrap();
                table
                    .insert("pypi:root/hosted:flask:flask-1.0.whl", b"not json".as_slice())
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        let meta = MetaStore::open(&path).unwrap();

        let admission = admit(
            &meta,
            DurabilityCapabilities::FILESYSTEM,
            MAX_STAGED_INTENTS,
            &request("flask-1.0.whl", "aa"),
            10,
        );

        assert_eq!(reject_status(admission), Some(StatusCode::INTERNAL_SERVER_ERROR));
    }

    #[test]
    fn test_ingress_dc_reads_the_local_roster_member() {
        let topology = TopologyConfig {
            mode: TopologyMode::Dc,
            group: Some("group".to_owned()),
            members: vec![TopologyMember {
                node: "node-1".to_owned(),
                dc: "dc-west".to_owned(),
                address: "node-1.internal".to_owned(),
                role: NodeRole::Writer,
            }],
            local_node: Some("node-1".to_owned()),
        };

        assert_eq!(ingress_dc(&topology), "dc-west");
    }

    #[test]
    fn test_ingress_dc_falls_back_for_a_rosterless_node() {
        assert_eq!(ingress_dc(&TopologyConfig::default()), STANDALONE_DC);
    }
}

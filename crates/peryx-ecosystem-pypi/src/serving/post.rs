//! The multipart upload handler: authorization, policy and status checks, and storage.
#![allow(
    clippy::result_large_err,
    reason = "handler helpers carry an axum Response as their error; boxing it everywhere adds noise"
)]

use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::extract::Multipart;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use peryx_driver::not_found;
use peryx_driver::state::ServingState;
use peryx_events::metrics::Event;
use peryx_events::webhook::{WebhookEvent, WebhookEventKind};
use peryx_identity::Action;
use peryx_index::Index;
use peryx_policy::{PolicyAction, PolicyDenial};

use crate::cache::{self, CacheError};
use crate::policy::{PypiPolicy, REQUIRED_ATTESTATION_AUDIT_RULE};
use crate::quota::{self, Admission, PendingQuota};
use crate::upload::{self, UploadError};
use crate::{PackageName, ProjectStatus, normalize_name};

use super::admission;
use super::response::{CacheContext, cache_error_response, policy_denial_response};
use super::upload_form::{collect_form, upload_error_message, upload_error_response};
use super::{authorize, identify, request_id, upload_target};

/// `POST /{route}/`, the legacy multipart upload API, used unchanged by twine and `uv publish`.
pub async fn pypi_dispatch_post(
    state: Arc<ServingState>,
    path: String,
    headers: HeaderMap,
    multipart: Multipart,
) -> Response {
    state.requests.fetch_add(1, Ordering::Relaxed);
    let browser = match browser_upload(&headers) {
        Ok(browser) => browser,
        Err(response) => return response,
    };
    let Some((index, rest)) = state.resolve(&path) else {
        return not_found();
    };
    let identity = identify(&state, index, &headers);
    let actor = peryx_events::security::actor(&identity);
    if !rest.is_empty() {
        security_upload_event(&headers, actor.as_deref(), &index.route, None, "denied")
            .reason(Some("upload path must target an index root"))
            .emit();
        return not_found();
    }
    let Some(hosted) = upload_target(&state, index) else {
        security_upload_event(&headers, actor.as_deref(), &index.route, None, "denied")
            .reason(Some("index does not accept uploads"))
            .emit();
        return (StatusCode::METHOD_NOT_ALLOWED, "index does not accept uploads").into_response();
    };
    if let Err(response) = authorize(&index.route, hosted, &identity, None, Action::Write, &headers) {
        return response;
    }
    let response = accept_upload(
        UploadContext {
            state: &state,
            index,
            hosted,
            identity: &identity,
            headers: &headers,
            actor: actor.as_deref(),
            browser,
        },
        multipart,
    )
    .await;
    if browser && response.status().is_server_error() {
        (response.status(), "upload storage failed").into_response()
    } else {
        response
    }
}

const BROWSER_CSRF_HEADER: &str = "x-peryx-csrf";

fn browser_upload(headers: &HeaderMap) -> Result<bool, Response> {
    let origin = headers.get(header::ORIGIN);
    let csrf = headers.get(BROWSER_CSRF_HEADER);
    if origin.is_none() && csrf.is_none() {
        return Ok(false);
    }
    let valid = origin.zip(csrf).is_some_and(|(origin, csrf)| {
        origin == csrf
            && headers.get("sec-fetch-site").is_none_or(|site| site == "same-origin")
            && browser_origin_matches_host(origin, headers.get(header::HOST))
    });
    if valid {
        Ok(true)
    } else {
        Err((StatusCode::FORBIDDEN, "browser upload rejected").into_response())
    }
}

fn browser_origin_matches_host(origin: &axum::http::HeaderValue, host: Option<&axum::http::HeaderValue>) -> bool {
    let Some((origin, host)) = origin.to_str().ok().zip(host.and_then(|host| host.to_str().ok())) else {
        return false;
    };
    let Ok(origin) = url::Url::parse(origin) else {
        return false;
    };
    let Ok(host) = host.parse::<axum::http::uri::Authority>() else {
        return false;
    };
    let host_name = host.host().trim_start_matches('[').trim_end_matches(']');
    matches!(origin.scheme(), "http" | "https")
        && origin
            .host_str()
            .is_some_and(|origin| origin.eq_ignore_ascii_case(host_name))
        && host.port_u16().map_or_else(
            || origin.port().is_none(),
            |port| origin.port_or_known_default() == Some(port),
        )
}

struct UploadContext<'a> {
    state: &'a Arc<ServingState>,
    index: &'a Index,
    hosted: &'a Index,
    identity: &'a super::UploadIdentity,
    headers: &'a HeaderMap,
    actor: Option<&'a str>,
    browser: bool,
}

async fn accept_upload(context: UploadContext<'_>, multipart: Multipart) -> Response {
    let UploadContext {
        state,
        index,
        hosted,
        identity,
        headers,
        actor,
        browser,
    } = context;
    let max_file_size = [index.policy.max_file_size(), hosted.policy.max_file_size()]
        .into_iter()
        .flatten()
        .min();
    let (form, staged) = match collect_form(multipart, &state.blobs, max_file_size, browser).await {
        Ok(form) => form,
        Err(response) => {
            security_upload_event(headers, actor, &index.route, Some(&hosted.name), "failure")
                .reason(Some("multipart body rejected"))
                .emit();
            return response;
        }
    };
    let Some(staged) = staged else {
        let err = UploadError::Missing("content");
        let (_, reason) = upload_error_message(&err);
        security_upload_event(headers, actor, &index.route, Some(&hosted.name), "denied")
            .project(form.name.as_deref().map(normalize_name).as_deref())
            .version(form.version.as_deref())
            .reason(Some(&reason))
            .emit();
        return upload_error_response(&err);
    };
    let form_project = form.name.as_deref().map(normalize_name);
    let form_version = form.version.clone();
    let form_filename = form.filename.clone();
    let upload_time_unix = (state.clock)();
    let prepared = match upload::prepare(form, staged, &index.route, upload_time_unix) {
        Ok(prepared) => prepared,
        Err(err) => {
            let (_, reason) = upload_error_message(&err);
            security_upload_event(headers, actor, &index.route, Some(&hosted.name), "denied")
                .project(form_project.as_deref())
                .version(form_version.as_deref())
                .filename(form_filename.as_deref())
                .reason(Some(&reason))
                .emit();
            return upload_error_response(&err);
        }
    };
    let project = prepared.normalized.clone();
    if let Err(response) = authorize(&index.route, hosted, identity, Some(&project), Action::Write, headers) {
        return response;
    }
    let version = prepared.record.version.clone();
    let filename = prepared.filename.clone();
    let digest = prepared.digest.as_str().to_owned();
    let audit = UploadAudit {
        headers,
        actor: actor.map(str::to_owned),
        request_id: request_id(headers),
        created_at_unix: upload_time_unix,
        index: &index.name,
        route: &index.route,
        hosted: &hosted.name,
        project: &project,
        version: &version,
        filename: &filename,
        digest: &digest,
    };
    if let Some(block) = upload_policy_response(index, &prepared, &audit) {
        return block;
    }
    if hosted.name != index.name
        && let Some(block) = upload_policy_response(hosted, &prepared, &audit)
    {
        return block;
    }
    let quota = match project_quota_reservation(state, index, hosted, &prepared, &project, &filename) {
        Ok(quota) => quota,
        Err(block) => {
            emit_upload_status_event(&audit, &block);
            return block.response;
        }
    };
    if let Some(block) = upload_status_response(
        cache::project_status(state, index, &project).await,
        &index.route,
        &project,
    ) {
        emit_upload_status_event(&audit, &block);
        return block.response;
    }
    admit_and_store(state, &hosted.name, &index.route, &project, prepared, quota, &audit).await
}

/// Durably admit the upload into the ingress datacenter, then store it. Admission stages a durable write
/// intent and proves same-DC durability before storage, so an accepted upload survives a restart near the
/// client; a refused admission returns its response unchanged and nothing is stored.
async fn admit_and_store(
    state: &Arc<ServingState>,
    hosted_name: &str,
    route: &str,
    project: &str,
    prepared: upload::PreparedUpload,
    quota: Option<PendingQuota>,
    audit: &UploadAudit<'_>,
) -> Response {
    let request = admission::AdmissionRequest {
        tenant: route,
        authority: project,
        filename: &prepared.filename,
        digest: prepared.digest.as_str(),
        size: prepared
            .record
            .file
            .size
            .expect("a prepared upload carries its byte size"),
        ingress_dc: &admission::ingress_dc(state.availability_topology()),
    };
    if let admission::Admission::Reject(response) = admission::admit(
        &state.meta,
        state.blobs.durability(),
        admission::MAX_STAGED_INTENTS,
        &request,
        (state.clock)(),
    ) {
        emit_admission_rejection(audit);
        return response;
    }
    let stored = cache::store_upload(state, hosted_name, prepared, quota).await;
    // The first stored file publishes the project, so it assigns the project's home datacenter through
    // the ownership group; a later file finds a home already set and only reads it. The project routes
    // through its canonical authority key — its PEP 503 normalized name — so every name variant homes
    // under one authority.
    if matches!(&stored, Ok(true)) {
        state
            .claim_first_publish_home(&crate::name::authority_key(project))
            .await;
    }
    upload_store_response(state, audit, stored)
}

fn emit_admission_rejection(audit: &UploadAudit<'_>) {
    security_upload_event(
        audit.headers,
        audit.actor.as_deref(),
        audit.route,
        Some(audit.hosted),
        "denied",
    )
    .project(Some(audit.project))
    .version(Some(audit.version))
    .filename(Some(audit.filename))
    .digest(Some(audit.digest))
    .reason(Some("ingress admission rejected"))
    .emit();
}

fn project_quota_reservation(
    state: &Arc<ServingState>,
    index: &Index,
    hosted: &Index,
    prepared: &upload::PreparedUpload,
    project: &str,
    filename: &str,
) -> Result<Option<PendingQuota>, UploadStatusBlock> {
    let Some((limit, audit)) = effective_project_quota(index, hosted) else {
        return Ok(None);
    };
    let exists = cache::upload_exists(state, &hosted.name, project, filename);
    if upload_quota_result(exists, &index.route, project)? {
        return Ok(None);
    }
    let incoming = prepared
        .record
        .file
        .size
        .expect("a prepared upload carries its byte size");
    let package = PackageName::new(project);
    let request = quota::quota_reservation(
        &hosted.name,
        &package,
        Some(prepared.record.version.as_str()),
        prepared.digest.as_str(),
        incoming,
        peryx_storage::meta::AccountingClass::Hosted,
        (state.clock)(),
    );
    let admission = quota::admit_upload(&state.meta, request, limit, audit);
    let admission = upload_quota_result(admission, &index.route, project)?;
    match admission {
        Admission::Reserved(reservation) => {
            quota::record_decision(state, hosted, project, false);
            Ok(Some(reservation))
        }
        Admission::Rejected { total } => {
            quota::record_decision(state, hosted, project, true);
            Err(upload_quota_denial(limit, project, filename, total))
        }
    }
}

fn effective_project_quota(index: &Index, hosted: &Index) -> Option<(u64, bool)> {
    match (
        index.policy.max_project_size(),
        (hosted.name != index.name)
            .then(|| hosted.policy.max_project_size())
            .flatten(),
    ) {
        (Some(index_limit), Some(hosted_limit)) => Some((
            index_limit.min(hosted_limit),
            index.policy.quota_audit() && hosted.policy.quota_audit(),
        )),
        (Some(limit), None) => Some((limit, index.policy.quota_audit())),
        (None, Some(limit)) => Some((limit, hosted.policy.quota_audit())),
        (None, None) => None,
    }
}

fn upload_policy_response(
    index: &Index,
    prepared: &upload::PreparedUpload,
    audit: &UploadAudit<'_>,
) -> Option<Response> {
    let Err(denial) = index.policy.check_upload(
        PolicyAction::Upload,
        &prepared.normalized,
        &prepared.record.file,
        &prepared.attestation_predicate_types,
    ) else {
        return None;
    };
    // An audit-mode attestation requirement records the unmet rule but does not reject the upload; the
    // decision is already persisted, so the handler emits the observation and lets the file publish.
    let audit_only = denial.rule == REQUIRED_ATTESTATION_AUDIT_RULE;
    security_upload_event(
        audit.headers,
        audit.actor.as_deref(),
        audit.route,
        Some(audit.hosted),
        if audit_only { "audit" } else { "denied" },
    )
    .project(Some(audit.project))
    .version(Some(audit.version))
    .filename(Some(audit.filename))
    .digest(Some(audit.digest))
    .reason(Some(&denial.reason))
    .emit();
    (!audit_only).then(|| policy_denial_response(&denial))
}

struct UploadAudit<'a> {
    headers: &'a HeaderMap,
    actor: Option<String>,
    request_id: Option<String>,
    created_at_unix: i64,
    index: &'a str,
    route: &'a str,
    hosted: &'a str,
    project: &'a str,
    version: &'a str,
    filename: &'a str,
    digest: &'a str,
}

fn upload_store_response(
    state: &Arc<ServingState>,
    audit: &UploadAudit<'_>,
    result: Result<bool, CacheError>,
) -> Response {
    match result {
        Ok(stored) => {
            if stored {
                state.metrics.record(Event::Upload {
                    route: audit.route.to_owned(),
                    project: audit.project.to_owned(),
                });
                peryx_events::webhook::emit(
                    state.clone(),
                    &WebhookEvent {
                        kind: WebhookEventKind::Upload,
                        created_at_unix: audit.created_at_unix,
                        index: audit.index.to_owned(),
                        route: audit.route.to_owned(),
                        hosted_index: audit.hosted.to_owned(),
                        project: audit.project.to_owned(),
                        version: Some(audit.version.to_owned()),
                        filename: Some(audit.filename.to_owned()),
                        digest: Some(audit.digest.to_owned()),
                        count: 1,
                        actor: audit.actor.clone(),
                        request_id: audit.request_id.clone(),
                    },
                );
            }
            security_upload_event(
                audit.headers,
                audit.actor.as_deref(),
                audit.route,
                Some(audit.hosted),
                if stored { "success" } else { "noop" },
            )
            .project(Some(audit.project))
            .version(Some(audit.version))
            .filename(Some(audit.filename))
            .digest(Some(audit.digest))
            .count(usize::from(stored))
            .reason((!stored).then_some("same content already stored"))
            .emit();
            (StatusCode::OK, "upload accepted").into_response()
        }
        Err(CacheError::FileExists(filename)) => {
            security_upload_event(
                audit.headers,
                audit.actor.as_deref(),
                audit.route,
                Some(audit.hosted),
                "denied",
            )
            .project(Some(audit.project))
            .version(Some(audit.version))
            .filename(Some(&filename))
            .digest(Some(audit.digest))
            .reason(Some("file exists with different content"))
            .emit();
            (
                StatusCode::BAD_REQUEST,
                format!("File already exists: {filename:?} has different content; use a different filename"),
            )
                .into_response()
        }
        Err(err) => {
            let reason = err.user_message();
            security_upload_event(
                audit.headers,
                audit.actor.as_deref(),
                audit.route,
                Some(audit.hosted),
                "failure",
            )
            .project(Some(audit.project))
            .version(Some(audit.version))
            .filename(Some(audit.filename))
            .digest(Some(audit.digest))
            .reason(Some(&reason))
            .emit();
            tracing::error!(error = ?err, "upload store failed");
            cache_error_response(&err, CacheContext::upload(audit.route, audit.project))
        }
    }
}

fn emit_upload_status_event(audit: &UploadAudit<'_>, block: &UploadStatusBlock) {
    security_upload_event(
        audit.headers,
        audit.actor.as_deref(),
        audit.route,
        Some(audit.hosted),
        block.result,
    )
    .project(Some(audit.project))
    .version(Some(audit.version))
    .filename(Some(audit.filename))
    .digest(Some(audit.digest))
    .reason(Some(&block.reason))
    .emit();
}

pub(super) struct UploadStatusBlock {
    pub(super) response: Response,
    pub(super) result: &'static str,
    pub(super) reason: String,
}

/// Preserve the existing policy-denial contract for a project-size reservation rejection.
fn upload_quota_denial(limit: u64, project: &str, filename: &str, total: u64) -> UploadStatusBlock {
    let reason = format!("project size {total} would exceed limit {limit}");
    let denial = PolicyDenial::new(
        PolicyAction::Upload,
        project,
        Some(filename),
        None,
        "max-project-size",
        "project_size",
        reason.clone(),
    );
    UploadStatusBlock {
        response: policy_denial_response(&denial),
        result: "denied",
        reason,
    }
}

fn upload_quota_failure(err: &CacheError, route: &str, project: &str) -> UploadStatusBlock {
    UploadStatusBlock {
        response: cache_error_response(err, CacheContext::upload(route, project)),
        result: "failure",
        reason: err.user_message(),
    }
}

fn upload_quota_result<T, E: Into<CacheError>>(
    result: Result<T, E>,
    route: &str,
    project: &str,
) -> Result<T, UploadStatusBlock> {
    result.map_err(|err| upload_quota_failure(&err.into(), route, project))
}

pub(super) fn upload_status_response(
    result: Result<ProjectStatus, CacheError>,
    index: &str,
    project: &str,
) -> Option<UploadStatusBlock> {
    match result {
        Ok(status) if status.allows_uploads() => None,
        Ok(status) => {
            let reason = format!("project {project:?} is {}; uploads are disabled", status.marker());
            Some(UploadStatusBlock {
                response: (StatusCode::FORBIDDEN, reason.clone()).into_response(),
                result: "denied",
                reason,
            })
        }
        Err(err) => {
            let reason = err.user_message();
            Some(UploadStatusBlock {
                response: cache_error_response(&err, CacheContext::upload(index, project)),
                result: "failure",
                reason,
            })
        }
    }
}

fn security_upload_event<'a>(
    headers: &'a HeaderMap,
    actor: Option<&'a str>,
    route: &'a str,
    hosted_index: Option<&'a str>,
    result: &'static str,
) -> peryx_events::security::Event<'a> {
    let event = peryx_events::security::Event::new("upload", result)
        .actor(actor)
        .index(route)
        .request(headers);
    if let Some(hosted_index) = hosted_index {
        event.hosted_index(hosted_index)
    } else {
        event
    }
}

#[cfg(test)]
mod tests {
    use peryx_storage::meta::MetaError;

    use super::*;

    #[test]
    fn test_upload_status_response_maps_policy_and_store_errors() {
        assert!(upload_status_response(Ok(ProjectStatus::Active), "root/pypi", "flask").is_none());
        let archived = upload_status_response(Ok(ProjectStatus::Archived), "root/pypi", "flask").unwrap();
        assert_eq!(archived.response.status(), StatusCode::FORBIDDEN);
        assert_eq!(archived.result, "denied");
        assert_eq!(archived.reason, "project \"flask\" is archived; uploads are disabled");

        let failure = upload_status_response(Err(CacheError::Meta(meta_error())), "root/pypi", "flask").unwrap();
        assert_eq!(failure.response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(failure.result, "failure");
        assert!(failure.reason.contains("metadata store error"));
    }

    #[test]
    fn test_upload_quota_failure_preserves_the_storage_fault() {
        let failure =
            upload_quota_result::<(), _>(Err(CacheError::Meta(meta_error())), "root/pypi", "flask").unwrap_err();

        assert_eq!(failure.response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(failure.result, "failure");
        assert!(failure.reason.contains("metadata store error"));
    }

    #[test]
    fn test_upload_quota_failure_describes_the_accounting_fault() {
        let failure = upload_quota_result::<(), _>(
            Err(peryx_storage::meta::QuotaError::Empty { field: "project" }),
            "root/pypi",
            "flask",
        )
        .unwrap_err();

        assert_eq!(failure.response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(failure.result, "failure");
        assert_eq!(failure.reason, "quota accounting error: project must not be empty");
    }

    fn meta_error() -> MetaError {
        MetaError::Decode(serde_json::from_str::<serde_json::Value>("not json").unwrap_err())
    }
}

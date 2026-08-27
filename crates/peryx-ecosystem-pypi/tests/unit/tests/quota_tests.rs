use peryx_storage::meta::{
    AccountingClass, MetaStore, NewQuotaReservation, QuotaLimit, QuotaLimits, QuotaReservationState,
};
use rstest::rstest;

use crate::quota::{Admission, PendingQuota, QuotaRejection, admit_upload};
use crate::{PackageName, quota_reservation};

#[test]
fn test_quota_reservation_normalizes_project_identity() {
    let project = PackageName::new("Zope.Interface");

    assert_eq!(
        quota_reservation("private", &project, Some("7.2"), "sha256:abc", 42, 100),
        NewQuotaReservation {
            repository: "private",
            resource: Some("zope-interface"),
            group: Some("7.2"),
            digest: "sha256:abc",
            bytes: 42,
            class: AccountingClass::Hosted,
            created_at_unix: 100,
        }
    );
}

#[test]
fn test_quota_admission_commits_project_bytes() {
    let (_dir, meta) = store();
    let project = PackageName::new("Flask");
    let mut pending = reservation(
        admit_upload(
            &meta,
            request(&project, "1.0", "sha256:first", 7, 1),
            QuotaLimits::default(),
            Some(8),
        )
        .unwrap(),
    )
    .ok()
    .expect("an upload within the limit to reserve capacity");
    let id = pending.record().id;

    meta.commit_quota_reservation(id).unwrap();
    pending.finish();

    assert_eq!(
        (
            meta.quota_resource_usage("private", "flask")
                .unwrap()
                .artifact_bytes
                .committed,
            meta.quota_reservation(id).unwrap().unwrap().state,
        ),
        (7, QuotaReservationState::Committed)
    );
}

#[test]
fn test_quota_admission_rejects_the_projected_total() {
    let (_dir, meta) = store();
    let project = PackageName::new("flask");
    let first = quota_reservation("private", &project, Some("1.0"), "sha256:first", 7, 1);
    let first = meta
        .reserve_resource_quota(first, QuotaLimits::default(), Some(10))
        .unwrap();
    meta.commit_quota_reservation(first.id).unwrap();

    let outcome = admit_upload(
        &meta,
        request(&project, "2.0", "sha256:second", 4, 2),
        QuotaLimits::default(),
        Some(10),
    )
    .unwrap();

    assert!(matches!(
        reservation(outcome),
        Err(QuotaRejection::ProjectBytes { total: 11 })
    ));
    assert_eq!(
        meta.quota_resource_usage("private", "flask")
            .unwrap()
            .artifact_bytes
            .reserved,
        0
    );
}

#[rstest]
#[case::project_bytes(
    QuotaLimits { audit: true, ..QuotaLimits::default() },
    Some(6),
    QuotaLimit::ArtifactBytes,
)]
#[case::projects(
    QuotaLimits { max_resources: Some(0), audit: true, ..QuotaLimits::default() },
    None,
    QuotaLimit::Resources,
)]
#[case::versions(
    QuotaLimits { max_groups_per_resource: Some(0), audit: true, ..QuotaLimits::default() },
    None,
    QuotaLimit::GroupsPerResource,
)]
fn test_quota_audit_admits_and_records_violation(
    #[case] limits: QuotaLimits,
    #[case] max_project_bytes: Option<u64>,
    #[case] expected: QuotaLimit,
) {
    let (_dir, meta) = store();
    let project = PackageName::new("flask");
    let mut pending = reservation(
        admit_upload(
            &meta,
            request(&project, "1.0", "sha256:first", 7, 1),
            limits,
            max_project_bytes,
        )
        .unwrap(),
    )
    .ok()
    .expect("audit mode to admit a would-reject upload");
    let id = pending.record().id;

    meta.commit_quota_reservation(id).unwrap();
    pending.finish();

    assert_eq!(meta.quota_reservation(id).unwrap().unwrap().violations, [expected]);
}

#[test]
fn test_quota_pending_drop_releases_cancelled_capacity() {
    let (_dir, meta) = store();
    let project = PackageName::new("flask");
    let pending = reservation(
        admit_upload(
            &meta,
            request(&project, "1.0", "sha256:first", 7, 1),
            QuotaLimits::default(),
            Some(8),
        )
        .unwrap(),
    )
    .ok()
    .expect("an upload within the limit to reserve capacity");

    drop(pending);

    assert_eq!(
        meta.quota_resource_usage("private", "flask")
            .unwrap()
            .artifact_bytes
            .reserved,
        0
    );
}

#[test]
fn test_quota_admission_returns_identity_errors() {
    let (_dir, meta) = store();
    let project = PackageName::new(&"a".repeat(513));

    let error = admit_upload(
        &meta,
        request(&project, "1.0", "sha256:first", 7, 1),
        QuotaLimits::default(),
        Some(8),
    )
    .err()
    .expect("an oversized identity to fail admission");

    assert_eq!(error.to_string(), "resource exceeds 512 bytes");
}

fn store() -> (tempfile::TempDir, MetaStore) {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    (dir, meta)
}

fn reservation(admission: Admission) -> Result<PendingQuota, QuotaRejection> {
    match admission {
        Admission::Reserved(pending) => Ok(pending),
        Admission::Rejected(rejection) => Err(rejection),
    }
}

const fn request<'a>(
    resource: &'a PackageName,
    group: &'a str,
    digest: &'a str,
    bytes: u64,
    created_at_unix: i64,
) -> NewQuotaReservation<'a> {
    quota_reservation("private", resource, Some(group), digest, bytes, created_at_unix)
}

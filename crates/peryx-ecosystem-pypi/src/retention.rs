//! The `PyPI` half of retention-plan evaluation: adapt one index's hosted upload records into the
//! neutral [`RetentionCandidate`]s the [`peryx_policy`] engine plans over.
//!
//! Uploads scan in key order (`{index}/{normalized}/{filename}`), so a project's files arrive
//! contiguously. This groups them, ranks their versions newest-first under
//! [PEP 440](https://peps.python.org/pep-0440/), and streams the resulting decisions one project at a
//! time, so a large index never materializes as one in-memory plan. The scan reads only indexed
//! metadata, so an interrupted evaluation writes nothing.
//!
//! Global version ranking and cross-referenced alternatives need one project's candidates in memory at
//! once, so the scan cannot stream within a project. It bounds that peak two ways: each raw
//! [`Uploaded`] record is projected to a compact [`RetentionCandidate`] and dropped as it is read,
//! never held alongside its decoded form; and a per-project byte budget over the surviving candidates'
//! footprint aborts a project that would exceed it, so one oversized project rejects its run instead of
//! allocating without limit.

use std::cmp::Ordering;
use std::collections::HashMap;

use peryx_policy::{
    RetentionCandidate, RetentionClass, RetentionDecision, RetentionFrontier, RetentionPolicy, RetentionSummary,
    RetentionVisibility,
};
use peryx_storage::meta::MetaStore;

use crate::policy::parse_upload_time;
use crate::store::scan_upload_policy_snapshot;
use crate::upload::Uploaded;
use crate::version::{VersionKey, version_key};
use crate::{Yanked, error_message};

/// Default ceiling on the candidate footprint one project may accumulate before a retention scan
/// rejects it, counting each candidate's struct plus its owned string bytes.
///
/// It bounds a run's peak memory independent of one project's artifact count; a project past it aborts
/// with a message rather than exhausting the process. 256 MiB leaves room for the largest realistic
/// project while still catching a pathological one.
pub const RETENTION_PROJECT_BUDGET_BYTES: usize = 256 * 1024 * 1024;

/// Evaluate one index's hosted uploads against `policy`.
///
/// Each artifact's decision passes to `emit` in deterministic order (newest version first). Returns the
/// plan's identity: the policy version and the metadata frontier the scan read. `emit` returns a
/// message to stop early (a disconnected export client or a filled page), and the scan aborts without
/// reading further; the whole path only reads metadata, so an interrupted plan writes nothing.
///
/// `budget` caps the candidate footprint one project may hold at once (see
/// [`RETENTION_PROJECT_BUDGET_BYTES`]); a project whose surviving candidates exceed it aborts the scan
/// so peak memory stays bounded regardless of any one project's artifact count.
///
/// # Errors
/// Returns a message when the store cannot be read, an upload record does not decode, `emit` stops the
/// scan, or a project's candidates exceed `budget`.
pub fn evaluate_retention<F>(
    meta: &MetaStore,
    index: &str,
    policy: &RetentionPolicy,
    now: Option<i64>,
    budget: usize,
    mut emit: F,
) -> Result<RetentionSummary, String>
where
    F: FnMut(RetentionDecision) -> Result<(), String>,
{
    let mut current: Option<String> = None;
    let mut group: Vec<RetentionCandidate> = Vec::new();
    let mut used: usize = 0;
    let generation = scan_upload_policy_snapshot(meta, index, |key, bytes| {
        let Some((project, _filename)) = key.split_once('/') else {
            return Ok(());
        };
        if current.as_deref() != Some(project) {
            if current.is_some() {
                plan_group(&mut group, policy, now, &mut emit)?;
            }
            current = Some(project.to_owned());
            used = 0;
        }
        let uploaded: Uploaded =
            serde_json::from_slice(bytes).map_err(|err| format!("corrupt upload record {key}: {err}"))?;
        let candidate = candidate(project, uploaded);
        used = used.saturating_add(footprint(&candidate));
        if used > budget {
            return Err(format!(
                "retention plan for project {project} exceeds the {budget}-byte per-project memory budget"
            ));
        }
        group.push(candidate);
        Ok::<(), String>(())
    })
    .map_err(error_message)?;
    if current.is_some() {
        plan_group(&mut group, policy, now, &mut emit).map_err(error_message)?;
    }
    Ok(RetentionSummary {
        policy_version: policy.version(),
        frontier: RetentionFrontier {
            repository: generation.repository,
            catalog: generation.catalog,
            policy: generation.policy,
        },
    })
}

fn plan_group<F>(
    group: &mut Vec<RetentionCandidate>,
    policy: &RetentionPolicy,
    now: Option<i64>,
    emit: &mut F,
) -> Result<(), String>
where
    F: FnMut(RetentionDecision) -> Result<(), String>,
{
    let mut group = std::mem::take(group);
    assign_ranks(&mut group);
    for decision in policy.plan_project(now, group) {
        emit(decision)?;
    }
    Ok(())
}

/// Project one raw upload record to its compact candidate, moving the fields retention keeps out of the
/// decoded record so its heavier remainder (the served URL, the full hash map, the metadata and
/// provenance blobs) drops as this returns. `rank` is filled once the whole project is grouped.
fn candidate(project: &str, uploaded: Uploaded) -> RetentionCandidate {
    let Uploaded { version, file, trashed } = uploaded;
    let class = if trashed.is_some() {
        RetentionClass::Trash
    } else {
        RetentionClass::Hosted
    };
    let visibility = match (&trashed, &file.yanked) {
        (Some(_), _) => RetentionVisibility::Hidden,
        (None, Yanked::No) => RetentionVisibility::Active,
        (None, Yanked::Yes | Yanked::Reason(_)) => RetentionVisibility::Yanked,
    };
    RetentionCandidate {
        project: project.to_owned(),
        artifact: file.filename,
        digest: file.hashes.get("sha256").cloned().unwrap_or_default(),
        class,
        visibility,
        source: None,
        bytes: file.size.unwrap_or(0),
        upload_time_unix: file.upload_time.as_deref().and_then(parse_upload_time),
        version: Some(version),
        rank: 0,
        orphan: false,
    }
}

/// The bytes one candidate holds: its struct plus the strings this adapter fills, so the budget tracks
/// string weight rather than record count alone. A pypi candidate carries no `source`, so none counts.
fn footprint(candidate: &RetentionCandidate) -> usize {
    size_of::<RetentionCandidate>()
        + candidate.project.len()
        + candidate.artifact.len()
        + candidate.digest.len()
        + candidate.version.as_deref().map_or(0, str::len)
}

/// Rank each distinct release newest-first. Two spellings of one release (`1.0`, `1.0.0`) collapse to
/// one rank; an unparseable legacy version ranks after every valid one, by string order.
fn assign_ranks(group: &mut [RetentionCandidate]) {
    let ranks = version_ranks(group);
    for candidate in &mut *group {
        candidate.rank = ranks[&version_key(candidate.version.as_deref().unwrap_or_default())];
    }
}

fn version_ranks(group: &[RetentionCandidate]) -> HashMap<VersionKey, u64> {
    let mut distinct: Vec<VersionKey> = group
        .iter()
        .map(|candidate| version_key(candidate.version.as_deref().unwrap_or_default()))
        .collect();
    distinct.sort_by(version_key_desc);
    distinct.dedup();
    distinct
        .into_iter()
        .enumerate()
        .map(|(rank, key)| (key, rank as u64))
        .collect()
}

fn version_key_desc(left: &VersionKey, right: &VersionKey) -> Ordering {
    match (left, right) {
        (VersionKey::Parsed(left), VersionKey::Parsed(right)) => right.cmp(left),
        (VersionKey::Raw(left), VersionKey::Raw(right)) => left.cmp(right),
        // A parsed release outranks any legacy spelling; both mixed orders resolve here, so neither
        // depends on which direction the sort happens to compare them.
        _ => parse_class(left).cmp(&parse_class(right)),
    }
}

const fn parse_class(key: &VersionKey) -> u8 {
    match key {
        VersionKey::Parsed(_) => 0,
        VersionKey::Raw(_) => 1,
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::BTreeMap;

    use peryx_policy::{
        RetentionClass, RetentionConfig, RetentionDecision, RetentionFrontier, RetentionOutcome, RetentionPolicy,
        RetentionSelector, RetentionVisibility,
    };
    use peryx_storage::meta::{MetaError, MetaStore};

    use super::{RETENTION_PROJECT_BUDGET_BYTES, evaluate_retention};
    use crate::store::PypiStore as _;
    use crate::upload::{TrashInfo, Uploaded};
    use crate::version::version_key;
    use crate::{CoreMetadata, File, Provenance, Yanked};

    fn store() -> (tempfile::TempDir, MetaStore) {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
        (dir, meta)
    }

    fn seed(meta: &MetaStore, index: &str, project: &str, version: &str, yanked: Yanked, trashed: Option<TrashInfo>) {
        let filename = format!("{project}-{version}.whl");
        let uploaded = Uploaded {
            version: version.to_owned(),
            file: File {
                filename: filename.clone(),
                url: format!("https://files/{filename}"),
                hashes: BTreeMap::from([("sha256".to_owned(), format!("sha-{version}"))]),
                requires_python: None,
                size: Some(1024),
                upload_time: Some("2020-01-01T00:00:00Z".to_owned()),
                yanked,
                core_metadata: CoreMetadata::Absent,
                dist_info_metadata: CoreMetadata::Absent,
                gpg_sig: None,
                provenance: Provenance::Absent,
            },
            trashed,
        };
        meta.put_upload(index, project, &filename, &serde_json::to_vec(&uploaded).unwrap())
            .unwrap();
    }

    fn plan(meta: &MetaStore, index: &str, policy: &RetentionPolicy) -> (Vec<RetentionDecision>, RetentionFrontier) {
        let mut decisions = Vec::new();
        let summary = evaluate_retention(meta, index, policy, None, RETENTION_PROJECT_BUDGET_BYTES, |decision| {
            decisions.push(decision);
            Ok(())
        })
        .unwrap();
        assert_eq!(summary.policy_version, policy.version());
        (decisions, summary.frontier)
    }

    fn expire_all_but_latest(count: u64) -> RetentionPolicy {
        RetentionPolicy::compile(&RetentionConfig {
            keep: vec![RetentionSelector::KeepLatest { count }],
            expire: vec![RetentionSelector::ProjectPrefix { prefix: String::new() }],
        })
    }

    #[test]
    fn test_evaluate_retention_orders_versions_by_pep440_and_keeps_the_newest() {
        let (_dir, meta) = store();
        for version in ["2.0", "1.0", "1.0rc1", "2.0+local", "not-a-version", "also-bad"] {
            seed(&meta, "pypi", "demo", version, Yanked::No, None);
        }

        let (decisions, _) = plan(&meta, "pypi", &expire_all_but_latest(2));

        let ordered: Vec<(&str, RetentionOutcome)> = decisions
            .iter()
            .map(|decision| (decision.version.as_deref().unwrap(), decision.outcome))
            .collect();
        assert_eq!(
            ordered,
            vec![
                ("2.0+local", RetentionOutcome::Retain),
                ("2.0", RetentionOutcome::Retain),
                ("1.0", RetentionOutcome::Remove),
                ("1.0rc1", RetentionOutcome::Remove),
                ("also-bad", RetentionOutcome::Remove),
                ("not-a-version", RetentionOutcome::Remove),
            ]
        );
    }

    #[test]
    fn test_evaluate_retention_lists_surviving_versions_as_alternatives() {
        let (_dir, meta) = store();
        seed(&meta, "pypi", "demo", "2.0", Yanked::No, None);
        seed(&meta, "pypi", "demo", "1.0", Yanked::No, None);

        let (decisions, _) = plan(&meta, "pypi", &expire_all_but_latest(1));

        let removed = decisions
            .iter()
            .find(|decision| decision.outcome == RetentionOutcome::Remove)
            .unwrap();
        assert_eq!(removed.version.as_deref(), Some("1.0"));
        assert_eq!(removed.retained_alternatives, vec!["2.0".to_owned()]);
    }

    #[test]
    fn test_evaluate_retention_marks_a_trashed_record_and_records_its_class() {
        let (_dir, meta) = store();
        seed(
            &meta,
            "pypi",
            "demo",
            "1.0",
            Yanked::No,
            Some(TrashInfo {
                deleted_at_unix: 0,
                actor: None,
                reason: None,
            }),
        );

        let policy = RetentionPolicy::compile(&RetentionConfig {
            keep: Vec::new(),
            expire: vec![RetentionSelector::Trash],
        });
        let (decisions, _) = plan(&meta, "pypi", &policy);

        assert_eq!(decisions[0].outcome, RetentionOutcome::Remove);
        assert_eq!(decisions[0].rule, Some("trash"));
        assert_eq!(decisions[0].class, RetentionClass::Trash);
        assert_eq!(decisions[0].visibility, RetentionVisibility::Hidden);
        assert_eq!(decisions[0].bytes, 1024);
    }

    #[test]
    fn test_evaluate_retention_records_yanked_visibility() {
        let (_dir, meta) = store();
        seed(&meta, "pypi", "demo", "1.0", Yanked::Reason("bad".to_owned()), None);

        let (decisions, _) = plan(&meta, "pypi", &RetentionPolicy::compile(&RetentionConfig::default()));

        assert_eq!(decisions[0].visibility, RetentionVisibility::Yanked);
        assert_eq!(decisions[0].class, RetentionClass::Hosted);
    }

    #[test]
    fn test_evaluate_retention_streams_each_project_independently() {
        let (_dir, meta) = store();
        seed(&meta, "pypi", "alpha", "2.0", Yanked::No, None);
        seed(&meta, "pypi", "alpha", "1.0", Yanked::No, None);
        seed(&meta, "pypi", "beta", "1.0", Yanked::No, None);

        let (decisions, _) = plan(&meta, "pypi", &expire_all_but_latest(1));

        let removed: Vec<&str> = decisions
            .iter()
            .filter(|decision| decision.outcome == RetentionOutcome::Remove)
            .map(|decision| decision.project.as_str())
            .collect();
        assert_eq!(removed, vec!["alpha"]);
    }

    #[test]
    fn test_evaluate_retention_skips_records_from_other_indexes() {
        let (_dir, meta) = store();
        seed(&meta, "pypi", "demo", "1.0", Yanked::No, None);
        seed(&meta, "other", "demo", "9.0", Yanked::No, None);

        let (decisions, _) = plan(&meta, "pypi", &expire_all_but_latest(1));

        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].version.as_deref(), Some("1.0"));
    }

    #[test]
    fn test_evaluate_retention_skips_a_malformed_upload_key() {
        let (_dir, meta) = store();
        meta.put_driver_value("pypi\u{0}u\u{0}pypi/malformed", b"not an upload")
            .unwrap();

        let (decisions, _) = plan(&meta, "pypi", &expire_all_but_latest(1));

        assert!(decisions.is_empty());
    }

    #[test]
    fn test_evaluate_retention_rejects_a_corrupt_upload_record() {
        let (_dir, meta) = store();
        // An earlier, valid project flushes through `emit` before the corrupt record aborts the scan,
        // so the emit path runs and the failure still surfaces.
        seed(&meta, "pypi", "aaa", "1.0", Yanked::No, None);
        meta.put_upload("pypi", "demo", "demo-1.0.whl", b"not json").unwrap();

        let mut seen = 0_u32;
        let result = evaluate_retention(
            &meta,
            "pypi",
            &expire_all_but_latest(1),
            None,
            RETENTION_PROJECT_BUDGET_BYTES,
            |_| {
                seen += 1;
                Ok(())
            },
        );

        assert_eq!(seen, 1);
        assert!(result.unwrap_err().contains("corrupt upload record"));
    }

    #[test]
    fn test_evaluate_retention_stops_the_scan_when_emit_returns_an_error() {
        let (_dir, meta) = store();
        seed(&meta, "pypi", "demo", "2.0", Yanked::No, None);
        seed(&meta, "pypi", "demo", "1.0", Yanked::No, None);

        let mut seen = 0_u32;
        let result = evaluate_retention(
            &meta,
            "pypi",
            &expire_all_but_latest(1),
            None,
            RETENTION_PROJECT_BUDGET_BYTES,
            |_| {
                seen += 1;
                Err("client hung up".to_owned())
            },
        );

        assert_eq!(seen, 1);
        assert!(result.unwrap_err().contains("client hung up"));
    }

    #[test]
    fn test_evaluate_retention_rejects_a_project_over_the_memory_budget() {
        let (_dir, meta) = store();
        // An earlier project flushes through `emit`; the over-budget project then aborts the scan
        // instead of accumulating its candidates without limit.
        seed(&meta, "pypi", "aaa", "1.0", Yanked::No, None);
        seed(&meta, "pypi", "demo", "2.0", Yanked::No, None);
        seed(&meta, "pypi", "demo", "1.0", Yanked::No, None);
        // One demo candidate (26 string bytes) fits; the second pushes the project past the budget.
        let budget = size_of::<super::RetentionCandidate>() + 40;

        let mut seen = 0_u32;
        let result = evaluate_retention(&meta, "pypi", &expire_all_but_latest(1), None, budget, |_| {
            seen += 1;
            Ok(())
        });

        assert_eq!(seen, 1);
        let message = result.unwrap_err();
        assert!(message.contains("project demo"), "{message}");
        assert!(message.contains("per-project memory budget"), "{message}");
    }

    #[test]
    fn test_evaluate_retention_plans_a_project_within_the_memory_budget() {
        let (_dir, meta) = store();
        seed(&meta, "pypi", "demo", "2.0", Yanked::No, None);
        seed(&meta, "pypi", "demo", "1.0", Yanked::No, None);
        let budget = 2 * size_of::<super::RetentionCandidate>() + 256;

        let mut decisions = 0_u32;
        evaluate_retention(&meta, "pypi", &expire_all_but_latest(1), None, budget, |_| {
            decisions += 1;
            Ok(())
        })
        .unwrap();

        assert_eq!(decisions, 2);
    }

    #[test]
    fn test_evaluate_retention_plans_nothing_for_an_empty_index() {
        let (_dir, meta) = store();

        let (decisions, frontier) = plan(&meta, "pypi", &expire_all_but_latest(1));

        assert!(decisions.is_empty());
        assert_eq!(frontier, RetentionFrontier::default());
    }

    #[test]
    fn test_evaluate_retention_reports_the_metadata_frontier() {
        let (_dir, meta) = store();
        meta.commit_driver_txn(|_| Ok::<_, MetaError>(((), vec![b"journal entry".to_vec()])))
            .unwrap();
        meta.advance_policy_generation("pypi").unwrap();
        seed(&meta, "pypi", "demo", "1.0", Yanked::No, None);

        let (_, frontier) = plan(&meta, "pypi", &expire_all_but_latest(1));

        assert_eq!(frontier.repository, 1);
        assert_eq!(frontier.policy, 1);
    }

    #[test]
    fn test_evaluate_retention_is_byte_identical_across_runs() {
        let (_dir, meta) = store();
        seed(&meta, "pypi", "demo", "2.0", Yanked::No, None);
        seed(&meta, "pypi", "demo", "1.0", Yanked::No, None);
        let policy = expire_all_but_latest(1);
        let render = || serde_json::to_string(&plan(&meta, "pypi", &policy).0).unwrap();

        assert_eq!(render(), render());
    }

    #[test]
    fn test_version_key_desc_ranks_releases_before_legacy_spellings() {
        let release = version_key("2.0");
        let older = version_key("1.0");
        let legacy = version_key("not-a-version");
        let other_legacy = version_key("also-bad");

        assert_eq!(super::version_key_desc(&release, &older), Ordering::Less);
        assert_eq!(super::version_key_desc(&release, &legacy), Ordering::Less);
        assert_eq!(super::version_key_desc(&legacy, &release), Ordering::Greater);
        assert_eq!(super::version_key_desc(&other_legacy, &legacy), Ordering::Less);
    }
}

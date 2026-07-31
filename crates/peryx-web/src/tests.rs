use rstest::rstest;

use crate::markdown::external_link_rel;
use crate::model::{
    PolicyDecisionFilters, UiPolicyDecision, UiPolicyDecisionPage, UiSearchPage, UiSnapshot, members_from_listing,
    projects_from_list,
};

fn policy_decision(state: &str, fresh: bool) -> UiPolicyDecision {
    UiPolicyDecision {
        id: "decision-1".to_owned(),
        repository: "private".to_owned(),
        project: "example".to_owned(),
        version: Some("1.0".to_owned()),
        filename: Some("example-1.0.whl".to_owned()),
        source: Some("pypi".to_owned()),
        action: "serve".to_owned(),
        state: state.to_owned(),
        rule: Some("blocked-project".to_owned()),
        reason: Some("project is blocked".to_owned()),
        evaluated_at_unix: 0,
        next_eligible_at_unix: None,
        fresh,
    }
}

#[rstest]
#[case::allow("allow", true, "Allowed")]
#[case::deny("deny", true, "Denied")]
#[case::wait("wait", true, "Waiting")]
#[case::stale("allow", false, "Stale Allowed")]
#[case::unknown("future", true, "Unknown")]
fn test_policy_decision_status(#[case] state: &str, #[case] fresh: bool, #[case] expected: &str) {
    assert_eq!(policy_decision(state, fresh).status(), expected);
}

#[test]
fn test_policy_decision_formats_times() {
    let mut decision = policy_decision("wait", true);
    decision.next_eligible_at_unix = Some(60);
    assert_eq!(decision.evaluated_at(), "1970-01-01T00:00:00Z");
    assert_eq!(decision.next_eligible_at(), "1970-01-01T00:01:00Z");
    decision.next_eligible_at_unix = None;
    assert_eq!(decision.next_eligible_at(), "—");
    decision.evaluated_at_unix = i64::MAX;
    assert_eq!(decision.evaluated_at(), i64::MAX.to_string());
    decision.evaluated_at_unix = -62_198_841_600;
    assert_eq!(decision.evaluated_at(), "-62198841600");
}

#[test]
fn test_policy_decision_filters_build_encoded_url() {
    let filters = PolicyDecisionFilters {
        repository: "team/private".to_owned(),
        state: "deny".to_owned(),
        rule: "blocked project".to_owned(),
        source: "pypi".to_owned(),
        from: "1970-01-01T00:01".to_owned(),
        to: "1970-01-01T00:02".to_owned(),
        limit: "50".to_owned(),
    };
    assert_eq!(
        filters.url(Some("next page")).unwrap(),
        "/+policy/decisions?repository=team%2Fprivate&state=deny&rule=blocked+project&source=pypi&from=60&to=120&limit=50&cursor=next+page"
    );
}

#[test]
fn test_policy_decision_filters_reject_invalid_datetime() {
    assert_eq!(
        PolicyDecisionFilters::default().url(None).unwrap(),
        "/+policy/decisions?limit=25"
    );
    let filters = PolicyDecisionFilters {
        from: "not-a-date".to_owned(),
        ..PolicyDecisionFilters::default()
    };
    assert_eq!(
        filters.url(None),
        Err("Invalid UTC date and time: not-a-date".to_owned())
    );
}

#[test]
fn test_policy_decision_page_deserializes_api_response() {
    let page: UiPolicyDecisionPage = serde_json::from_value(serde_json::json!({
        "decisions": [{
            "id": "decision-1", "repository": "private", "project": "example", "version": null,
            "filename": null, "source": null, "action": "serve", "state": "allow", "rule": null,
            "reason": null, "evaluated_at_unix": 0, "input_generation": {"repository": 0},
            "next_eligible_at_unix": null, "fresh": true
        }],
        "next_cursor": "next"
    }))
    .unwrap();
    assert_eq!(page.decisions[0].status(), "Allowed");
    assert_eq!(page.next_cursor.as_deref(), Some("next"));
}

#[test]
fn test_snapshot_from_status_roundtrip() {
    let value = serde_json::json!({
        "version": "0.0.1",
        "serial": 7,
        "requests": 12,
        "by_ecosystem": [
            {"ecosystem": "pypi", "pages": 12, "downloads": 4, "bytes": 900, "rejected": 0,
             "uploads": 0, "families": {"metadata": 3}}
        ],
        "metric_families": [
            {"key": "metadata", "label": "PEP 658 metadata hits", "roles": ["cached", "hosted", "virtual"]}
        ],
        "indexes": [{
            "name": "pypi",
            "route": "pypi",
            "ecosystem": "pypi",
            "kind": "cached",
            "layers": [],
            "uploads": false,
            "upstream": {"url": "https://pypi.org/simple/", "auth": {"kind": "none"}, "status": "configured"},
            "project_count": 2,
            "upload_count": 0,
            "recent_uploads": [],
        }],
    });
    let snapshot = UiSnapshot::from_status(&value);
    assert_eq!(snapshot.version, "0.0.1");
    assert_eq!(snapshot.serial, 7);
    assert_eq!(snapshot.requests, 12);
    assert_eq!(snapshot.ecosystems.len(), 1);
    assert_eq!(snapshot.ecosystems[0].ecosystem, "pypi");
    assert_eq!(snapshot.ecosystems[0].families["metadata"], 3);
    assert_eq!(snapshot.families[0].label, "PEP 658 metadata hits");
    assert_eq!(snapshot.indexes.len(), 1);
    assert_eq!(snapshot.indexes[0].kind, "cached");
    assert_eq!(snapshot.indexes[0].project_count, 2);
    assert_eq!(
        snapshot.indexes[0].upstream.as_ref().unwrap().url,
        "https://pypi.org/simple/"
    );
}

#[test]
fn test_projects_and_members_from_json() {
    let list = serde_json::json!({"projects": [{"name": "a"}, {"name": "b"}]});
    assert_eq!(projects_from_list(&list), ["a", "b"]);
    let listing =
        serde_json::json!({"members": [{"path": "x/METADATA", "size": 5, "kind": "text", "previewable": true}]});
    let members = members_from_listing(&listing);
    assert_eq!(members[0].path, "x/METADATA");
    assert_eq!(members[0].size, 5);
    assert_eq!(members[0].kind, "text");
    assert!(members[0].previewable);
}

#[test]
fn test_search_page_from_json() {
    let value = serde_json::json!({
        "query": "flask",
        "type": "override",
        "page": 2,
        "page_size": 50,
        "total": 51,
        "results": [{
            "display_name": "Flask",
            "normalized_name": "flask",
            "route": "root/pypi",
                        "type": "override",
            "summary": "web framework",
        }],
    });
    let page = UiSearchPage::from_search(&value);
    assert_eq!(page.query, "flask");
    assert_eq!(page.page, 2);
    assert_eq!(page.results[0].source_label(), "Override");
    assert_eq!(page.results[0].summary.as_deref(), Some("web framework"));
}

#[rstest]
#[case::http("http://example.com/docs", Some("external nofollow noopener noreferrer"))]
#[case::https("https://example.com/docs", Some("external nofollow noopener noreferrer"))]
#[case::mailto("mailto:maintainer@example.com", None)]
#[case::absolute_route("/pypi/files/veloxdemo-1.0.0.tar.gz", None)]
#[case::relative_route("../docs/", None)]
#[case::malformed("http://[invalid", None)]
fn test_external_link_rel(#[case] target: &str, #[case] expected: Option<&str>) {
    assert_eq!(external_link_rel(target), expected);
}

#[test]
fn test_stats_routes_sums_totals_and_sorts_busiest_first() {
    let value = serde_json::json!({
        "hosted": {"base": {"pages": 1, "downloads": 0, "bytes": 10}, "hosted": {"uploads": 2}},
        "root/pypi": {
            "base": {"pages": 5, "downloads": 3, "bytes": 900},
            "cached": {"refreshes": 2, "changed": 1}
        },
    });
    let stats = crate::model::stats_routes(&value);
    assert_eq!(stats.totals.pages, 6);
    assert_eq!(stats.totals.bytes, 910);
    assert_eq!(stats.totals.uploads, 2);
    assert_eq!(stats.totals.changed, 1);
    assert_eq!(stats.rows[0].0, "root/pypi");
    assert_eq!(stats.rows[1].0, "hosted");
}

#[test]
fn test_stats_index_reads_totals_and_projects() {
    let value = serde_json::json!({
        "totals": {
            "base": {"pages": 4, "downloads": 2, "rejected": 1},
            "cached": {"stale_served": 1, "upstream_errors": 1}
        },
        "projects": {
            "pandas": {"base": {"pages": 3, "downloads": 2, "bytes": 500}},
            "six": {"base": {"pages": 1, "downloads": 0}},
        },
    });
    let stats = crate::model::stats_index(&value);
    assert_eq!(stats.totals.stale_served, 1);
    assert_eq!(stats.totals.upstream_errors, 1);
    assert_eq!(stats.totals.rejected, 1);
    assert_eq!(stats.rows[0].0, "pandas");
    assert_eq!(stats.rows[0].1.bytes, 500);
}

#[test]
fn test_stats_project_reads_grouped_totals_and_files() {
    let value = serde_json::json!({
        "totals": {
            "base": {"pages": 3, "downloads": 2, "bytes": 500},
            "ecosystem": {"metadata": 2}
        },
        "files": {
            "pandas-3.0.3-cp314-cp314-macosx_11_0_arm64.whl":
                {"downloads": 2, "bytes": 500, "ecosystem": {"metadata": 2}},
        },
    });
    let stats = crate::model::stats_project(&value);
    assert_eq!(stats.totals.downloads, 2);
    assert_eq!(stats.totals.metadata, 2);
    assert_eq!(stats.rows.len(), 1);
    assert_eq!(stats.rows[0].1.metadata, 2);
}

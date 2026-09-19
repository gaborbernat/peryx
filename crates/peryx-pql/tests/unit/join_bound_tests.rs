use crate::catalog::{Column, DomainAuth, DomainSchema, FieldClass, Indexability};
use crate::cursor;
use crate::error::PqlError;
use crate::execute::{Page, execute};
use crate::parse::parse;
use crate::plan::OutputColumn;
use crate::scope::QueryScope;
use crate::source::{DataSource, FetchFilter};
use crate::value::{Row, Value, ValueType};

use super::support::{TestSource, decision, decisions, operator_scope, repository_scope};

/// Storage caps a repository's decision history here, so a self-join over it squares this count.
const HISTORY: i64 = 10_000;

const CURSOR_DOMAIN: &str = "policy.decisions\u{1}usage";

/// Both sides hold a single shared key value, so every pair matches and this many rows a side walks
/// past the match cap.
const WIDE_ROWS: i64 = 200;

fn history() -> Vec<Row> {
    (0..HISTORY)
        .map(|at| decision("alpha", "resource-a", "blocked", "cache", at, 1))
        .collect()
}

#[test]
fn test_execute_refuses_a_self_join_before_reading_the_history() {
    let source = TestSource::new(history());
    assert_eq!(
        (
            execute(
                &parse("from policy.decisions join policy.decisions on repository").expect("parses"),
                &repository_scope("alpha"),
                None,
                &source,
            ),
            source.fetches(),
        ),
        (
            Err(PqlError::UnboundedJoin(
                "`policy.decisions` joined to itself pairs its rows with each other, so the join cannot be bounded"
                    .to_owned()
            )),
            Vec::new(),
        )
    );
}

#[test]
fn test_execute_refuses_a_join_keyed_only_on_the_pinned_scope_column() {
    let source = TestSource::new(Vec::new());
    assert_eq!(
        (
            execute(
                &parse("from big join policy.decisions on repository").expect("parses"),
                &repository_scope("alpha"),
                None,
                &source,
            ),
            source.fetches(),
        ),
        (
            Err(PqlError::UnboundedJoin(
                "join key `repository` already pins this query, so it cannot narrow `policy.decisions`".to_owned()
            )),
            Vec::new(),
        )
    );
}

#[test]
fn test_execute_join_page_holds_the_rows_the_order_selects() {
    // The smallest matching timestamps sort last in the fetched rows, so a join that stopped at the
    // first pair it merged would answer with the largest one instead.
    assert_eq!(
        execute(
            &parse(
                "from policy.decisions join usage on repository, resource select resource, evaluated_at order by evaluated_at asc limit 1"
            )
            .expect("parses"),
            &operator_scope(),
            None,
            &TestSource::new(decisions()),
        ),
        Ok(Page {
            outputs: vec![
                output("resource", FieldClass::Repository, ValueType::Str),
                output("evaluated_at", FieldClass::Repository, ValueType::Timestamp),
            ],
            rows: vec![vec![Value::Str("resource-b".to_owned()), Value::Timestamp(200)]],
            next_cursor: Some(cursor::encode(CURSOR_DOMAIN, &operator_scope(), 1)),
        })
    );
}

#[test]
fn test_execute_join_page_counts_only_rows_the_predicate_keeps() {
    // One pair survives the predicate, so the page is complete and mints no cursor even though the
    // join walked further looking for a second.
    assert_eq!(
        execute(
            &parse("from policy.decisions join usage on repository, resource where hits >= 60 select resource limit 1")
                .expect("parses"),
            &operator_scope(),
            None,
            &TestSource::new(decisions()),
        ),
        Ok(Page {
            outputs: vec![output("resource", FieldClass::Repository, ValueType::Str)],
            rows: vec![vec![Value::Str("resource-a".to_owned())]],
            next_cursor: None,
        })
    );
}

#[test]
fn test_execute_join_pages_a_wide_fan_out() {
    // Every outer row matches every probe row, so only stopping at the page keeps this under the
    // match cap the two tests below hit.
    assert_eq!(
        wide("from wide_outer join wide_probe on key select key, rank limit 5"),
        Ok(Page {
            outputs: vec![
                output("key", FieldClass::Public, ValueType::Str),
                output("rank", FieldClass::Public, ValueType::Int),
            ],
            rows: vec![vec![Value::Str("same".to_owned()), Value::Int(199)]; 5],
            next_cursor: Some(cursor::encode("wide_outer\u{1}wide_probe", &operator_scope(), 5)),
        })
    );
}

#[test]
fn test_execute_refuses_a_wide_fan_out_under_an_aggregate() {
    assert_eq!(
        wide("from wide_outer join wide_probe on key aggregate count() as n by key"),
        Err(cost_exceeded())
    );
}

#[test]
fn test_execute_refuses_a_wide_fan_out_ordered_by_a_probe_column() {
    assert_eq!(
        wide("from wide_outer join wide_probe on key select key, weight order by weight desc limit 5"),
        Err(cost_exceeded())
    );
}

#[test]
fn test_execute_walks_exactly_the_match_cap_without_erroring() {
    // A single outer row against exactly `MAX_JOIN_MATCHES` probe rows walks the cap precisely once;
    // a predicate that never matches forces the full walk instead of stopping at a page bound.
    let source = WideJoinSource::sized(1, 25_000);
    assert_eq!(
        execute(
            &parse(r#"from wide_outer join wide_probe on key where key == "never" select key"#).expect("parses"),
            &operator_scope(),
            None,
            &source,
        ),
        Ok(Page {
            outputs: vec![output("key", FieldClass::Public, ValueType::Str)],
            rows: Vec::new(),
            next_cursor: None,
        })
    );
}

fn cost_exceeded() -> PqlError {
    PqlError::CostExceeded(
        "joining `wide_outer` to `wide_probe` matches more than 25000 row pairs; add a join key that narrows it"
            .to_owned(),
    )
}

fn wide(text: &str) -> Result<Page, PqlError> {
    execute(
        &parse(text).expect("parses"),
        &operator_scope(),
        None,
        &WideJoinSource::new(),
    )
}

fn output(name: &str, class: FieldClass, value_type: ValueType) -> OutputColumn {
    OutputColumn {
        name: name.to_owned(),
        class,
        value_type,
    }
}

struct WideJoinSource {
    outer: DomainSchema,
    probe: DomainSchema,
    outer_rows: i64,
    probe_rows: i64,
}

impl WideJoinSource {
    fn new() -> Self {
        Self::sized(WIDE_ROWS, WIDE_ROWS)
    }

    fn sized(outer_rows: i64, probe_rows: i64) -> Self {
        Self {
            outer: wide_schema("wide_outer", "rank"),
            probe: wide_schema("wide_probe", "weight"),
            outer_rows,
            probe_rows,
        }
    }
}

impl DataSource for WideJoinSource {
    fn schema(&self, domain: &str) -> Option<&DomainSchema> {
        assert!(matches!(domain, "wide_outer" | "wide_probe"));
        Some(if domain == "wide_outer" {
            &self.outer
        } else {
            &self.probe
        })
    }

    fn fetch(&self, domain: &str, _scope: &QueryScope, _filter: Option<&FetchFilter>) -> Result<Vec<Row>, PqlError> {
        assert!(matches!(domain, "wide_outer" | "wide_probe"));
        let metric = if domain == "wide_outer" { "rank" } else { "weight" };
        let rows = if domain == "wide_outer" {
            self.outer_rows
        } else {
            self.probe_rows
        };
        Ok((0..rows)
            .map(|index| {
                Row::new()
                    .with("key", Value::Str("same".to_owned()))
                    .with(metric, Value::Int(index))
            })
            .collect())
    }
}

fn wide_schema(name: &'static str, metric: &'static str) -> DomainSchema {
    DomainSchema {
        name,
        columns: vec![
            Column::new("key", ValueType::Str, FieldClass::Public, Indexability::Indexed, false),
            Column::new(
                metric,
                ValueType::Int,
                FieldClass::Public,
                Indexability::KeyOrdered,
                true,
            ),
        ],
        auth: DomainAuth::OperatorOnly,
        natural_order: metric,
        bounded: true,
        pushdown: &[],
    }
}

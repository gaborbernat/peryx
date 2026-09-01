use std::hint::black_box;

use criterion::{Criterion, Throughput};
use peryx_pql::{
    Column, DataSource, DomainAuth, DomainSchema, FetchFilter, FieldClass, FieldVisibility, Indexability, PqlError,
    QueryScope, RepoScope, Row, Value, ValueType, execute, parse,
};

const DISTINCT_GROUPS: usize = 50_000;

struct Groups {
    schema: DomainSchema,
    rows: Vec<Row>,
}

impl Groups {
    fn new() -> Self {
        Self {
            schema: DomainSchema {
                name: "groups",
                columns: vec![Column::new(
                    "resource",
                    ValueType::Str,
                    FieldClass::Public,
                    Indexability::Indexed,
                    false,
                )],
                auth: DomainAuth::OperatorOnly,
                natural_order: "resource",
                bounded: true,
                pushdown: &[],
            },
            rows: (0..DISTINCT_GROUPS)
                .map(|group| Row::new().with("resource", Value::Str(format!("resource-{group}"))))
                .collect(),
        }
    }
}

impl DataSource for Groups {
    fn schema(&self, domain: &str) -> Option<&DomainSchema> {
        (domain == self.schema.name).then_some(&self.schema)
    }

    fn fetch(&self, _domain: &str, _scope: &QueryScope, _filter: Option<&FetchFilter>) -> Result<Vec<Row>, PqlError> {
        Ok(self.rows.clone())
    }
}

fn main() {
    let source = Groups::new();
    let ast = parse("from groups aggregate count() as rows by resource limit 10").unwrap();
    let scope = QueryScope::new(
        RepoScope::All,
        FieldVisibility::new([FieldClass::Public]),
        "benchmark".to_owned(),
    );
    let mut criterion = Criterion::default().configure_from_args();
    {
        let mut group = criterion.benchmark_group("pql_distinct_groups");
        group.throughput(Throughput::Elements(DISTINCT_GROUPS as u64));
        group.bench_function("50000_limit_10", |bencher| {
            bencher.iter(|| black_box(execute(black_box(&ast), &scope, None, black_box(&source)).unwrap()));
        });
        group.finish();
    }
    criterion.final_summary();
}

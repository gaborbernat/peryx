#![expect(
    clippy::significant_drop_tightening,
    reason = "CodSpeed v5.0.1's criterion_group! macro creates this temporary"
)]

use criterion::{Criterion, criterion_group, criterion_main};
use peryx_ecosystem_pypi::parse_version_specifiers;

fn bench_parse_version_specifiers(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("name_version");
    group.bench_function("parse_version_specifiers", |bencher| {
        bencher.iter(|| parse_version_specifiers(std::hint::black_box(">=3.8,<4.0,!=3.9.1")));
    });
    group.finish();
}

criterion_group!(benches, bench_parse_version_specifiers);
criterion_main!(benches);

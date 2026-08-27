#![expect(
    clippy::significant_drop_tightening,
    reason = "CodSpeed v5.0.1's criterion_group! macro creates this temporary"
)]

use criterion::{Criterion, criterion_group, criterion_main};
use peryx_ecosystem_pypi::sorted_desc;

const LARGE: usize = 400;

fn bench_sorted_desc(criterion: &mut Criterion) {
    let versions: Vec<String> = (0..LARGE)
        .map(|index| format!("{}.{}.{}", index / 100, (index / 10) % 10, index % 10))
        .collect();
    let mut group = criterion.benchmark_group("name_version");
    group.bench_function("sorted_desc", |bencher| {
        bencher.iter(|| sorted_desc(std::hint::black_box(&versions)));
    });
    group.finish();
}

criterion_group!(benches, bench_sorted_desc);
criterion_main!(benches);

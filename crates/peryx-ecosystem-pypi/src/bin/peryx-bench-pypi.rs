#[tokio::main]
async fn main() -> anyhow::Result<()> {
    peryx_bench::run(peryx_ecosystem_pypi::bench::BENCHMARK_SUITE).await
}

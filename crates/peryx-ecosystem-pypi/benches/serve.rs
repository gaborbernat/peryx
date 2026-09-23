#![allow(
    clippy::significant_drop_tightening,
    reason = "criterion_group! expands to a temporary flagged by this nursery lint"
)]

#[path = "support/detail.rs"]
mod detail;

use std::hint::black_box;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use http::Request;
use http_body_util::BodyExt as _;
use peryx_driver::AppState;
use peryx_driver::rate_limit::{RateLimitConfig, RateLimiter, RouteClass, RouteLimit};
use peryx_ecosystem_pypi::store::PypiStore as _;
use peryx_ecosystem_pypi::store::{
    CachedIndex, CachedPageWrite, FileUiLookup, PublishedFileWrite, read_file_ui_records,
};
use peryx_ecosystem_pypi::to_json;
use peryx_ecosystem_pypi::{CoreMetadata, File, Meta, ProjectDetail, Provenance, Yanked};
use peryx_ha::{ArtifactPlacement, ArtifactSource};
use peryx_http::router;
use peryx_identity::{Action, Glob, Grant, IndexAcl, NamedToken};
use peryx_index::{Index, IndexKind};
use peryx_policy::Policy;
use peryx_storage::blob::BlobStore;
use peryx_storage::meta::MetaStore;
use peryx_upstream::UpstreamClient;
use tokio::runtime::Runtime;
use tower::ServiceExt as _;

use detail::project_detail;

const LARGE: usize = 400;
const PROJECT_UI_FILES: usize = if cfg!(debug_assertions) {
    64
} else if cfg!(codspeed) {
    10_000
} else {
    1_800_000
};
const PAGE_WRITE_BATCH: usize = 10_000;
const JSON: &str = "application/vnd.pypi.simple.v1+json";
const HTML: &str = "text/html";

fn writer_acl(secret: impl Into<String>) -> IndexAcl {
    IndexAcl {
        anonymous_read: true,
        tokens: vec![NamedToken {
            name: "uploader".to_owned(),
            secret: secret.into(),
            grants: vec![Grant {
                resources: vec![Glob::new("*")],
                actions: std::collections::BTreeSet::from([Action::Write, Action::Delete]),
            }],
            expires_at: None,
        }],
    }
}

// Router timing excludes the limiter because runtime jitter obscures its smaller cost.
fn bench_serve(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("serve");
    let detail = project_detail("flask", LARGE);
    let (_dir, state) = cached(RateLimitConfig::default(), &detail);
    let app = router(state);
    runtime.block_on(serve(app.clone(), "/pypi/simple/flask/", JSON));
    group.bench_with_input(BenchmarkId::new("simple_json", "disabled"), &app, |bencher, app| {
        bencher
            .to_async(&runtime)
            .iter(|| serve(app.clone(), "/pypi/simple/flask/", JSON));
    });
    group.bench_with_input(BenchmarkId::new("simple_html", "disabled"), &app, |bencher, app| {
        bencher
            .to_async(&runtime)
            .iter(|| serve(app.clone(), "/pypi/simple/flask/", HTML));
    });
    group.bench_with_input(BenchmarkId::new("legacy_json", "disabled"), &app, |bencher, app| {
        bencher
            .to_async(&runtime)
            .iter(|| serve(app.clone(), "/pypi/flask/json", JSON));
    });
    group.finish();
}

fn bench_project_ui_sources(criterion: &mut Criterion) {
    let fixture = ui_read_fixture(PROJECT_UI_FILES);
    let lookups = fixture.lookups();
    let runtime = runtime();
    let mut group = criterion.benchmark_group("project_ui_sources");
    group.throughput(Throughput::Elements(PROJECT_UI_FILES as u64));
    // Reopening redb clears process-local database state without evicting the operating system's page cache.
    group.bench_function(
        BenchmarkId::new("cold-reopen-os-cache-warm", PROJECT_UI_FILES),
        |bencher| {
            bencher.iter(|| {
                let meta = MetaStore::open(&fixture.database).unwrap();
                black_box(read_file_ui_records(&meta, black_box(&lookups)).unwrap())
            });
        },
    );
    let meta = MetaStore::open(&fixture.database).unwrap();
    group.bench_function(BenchmarkId::new("warm", PROJECT_UI_FILES), |bencher| {
        bencher.iter(|| black_box(read_file_ui_records(&meta, black_box(&lookups)).unwrap()));
    });
    drop(meta);
    group.bench_function(
        BenchmarkId::new("cold-page-reopen-os-cache-warm", PROJECT_UI_FILES),
        |bencher| {
            bencher.to_async(&runtime).iter(|| async {
                let app = ui_app(&fixture);
                serve(app, "/+ui/browse?index=pypi&project=flask", JSON).await;
            });
        },
    );
    let app = ui_app(&fixture);
    runtime.block_on(serve(app.clone(), "/+ui/browse?index=pypi&project=flask", JSON));
    group.bench_function(BenchmarkId::new("warm-page", PROJECT_UI_FILES), |bencher| {
        bencher
            .to_async(&runtime)
            .iter(|| serve(app.clone(), "/+ui/browse?index=pypi&project=flask", JSON));
    });
    group.finish();
}

// A warm batch amortizes moka maintenance and isolates steady-state limiter cost.
fn bench_rate_limit(criterion: &mut Criterion) {
    const BATCH: usize = 1024;
    let limiter = RateLimiter::new(enabled_limits());
    let client = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
    // Exclude the one-time bucket insertion.
    let _ = limiter.check_client(RouteClass::Listing, client);
    criterion.bench_function("rate_limit_decision", |bencher| {
        bencher.iter(|| {
            for _ in 0..BATCH {
                black_box(limiter.check_client(black_box(RouteClass::Listing), black_box(client)));
            }
        });
    });
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn cached(rate_limit: RateLimitConfig, detail: &ProjectDetail) -> (tempfile::TempDir, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaStore::open(dir.path().join("peryx.redb")).unwrap();
    meta.put_index(
        &format!("pypi/{}", detail.name),
        &CachedIndex {
            source: None,
            last_modified: None,
            etag: None,
            last_serial: None,
            fetched_at_unix: 1000,
            content_type: Some("application/vnd.pypi.simple.v1+json".to_owned()),
            fresh_secs: Some(3600),
            body: to_json(detail).into_bytes(),
        },
    )
    .unwrap();
    let blobs = BlobStore::new(dir.path().join("blobs"));
    let upstream = UpstreamClient::new("http://127.0.0.1:9/simple/").unwrap();
    let mut state = AppState::with_limits(
        meta,
        blobs,
        3600,
        vec![Index {
            name: "pypi".to_owned(),
            route: "pypi".to_owned(),
            ecosystem: peryx_ecosystem_pypi::ECOSYSTEM,
            kind: IndexKind::Cached {
                client: upstream,
                offline: false,
            },
            policy: Policy::default(),
            acl: writer_acl("secret"),
        }],
        Arc::new(|| 1000),
        rate_limit,
        [("pypi".to_owned(), 0)],
    );
    peryx_plugin_registry::PluginRegistry::new(vec![peryx_ecosystem_pypi::registration()])
        .unwrap()
        .activate([peryx_ecosystem_pypi::ECOSYSTEM])
        .unwrap()
        .install_drivers(
            &mut state.runtime_install_context().unwrap(),
            &std::collections::HashMap::new(),
        )
        .unwrap();
    (dir, Arc::new(state))
}

struct UiReadFixture {
    _directory: tempfile::TempDir,
    database: PathBuf,
    blobs: PathBuf,
    filenames: Vec<String>,
    digests: Vec<String>,
}

impl UiReadFixture {
    fn lookups(&self) -> Vec<FileUiLookup<'_>> {
        self.filenames
            .iter()
            .zip(&self.digests)
            .map(|(filename, digest)| FileUiLookup {
                filename,
                source_index: Some("pypi"),
                normalized: "flask",
                digest,
            })
            .collect()
    }
}

fn ui_read_fixture(file_count: usize) -> UiReadFixture {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("peryx.redb");
    let meta = MetaStore::open(&database).unwrap();
    let record = CachedIndex {
        source: None,
        last_modified: None,
        etag: None,
        last_serial: None,
        fetched_at_unix: 0,
        content_type: None,
        fresh_secs: None,
        body: Vec::new(),
    };
    let mut filenames = Vec::with_capacity(file_count);
    let mut digests = Vec::with_capacity(file_count);
    for start in (0..file_count).step_by(PAGE_WRITE_BATCH) {
        let files = (start..(start + PAGE_WRITE_BATCH).min(file_count))
            .map(|position| PublishedFileWrite {
                sha256: format!("{position:064x}"),
                filename: format!("flask-1.0-{position}-py3-none-any.whl"),
                url: format!("https://files.example/{position}"),
                size: Some(1),
                metadata: None,
            })
            .collect::<Vec<_>>();
        filenames.extend(files.iter().map(|file| file.filename.clone()));
        digests.extend(files.iter().map(|file| file.sha256.clone()));
        meta.put_cached_page(CachedPageWrite {
            key: "pypi/flask",
            record: &record,
            index: "pypi",
            normalized: "flask",
            display: "flask",
            source: "pypi",
            upstream: None,
            project_status: None,
            project_status_reason: None,
            files: &files,
            attestations: &[],
        })
        .unwrap();
        meta.commit_driver_txn(|txn| {
            for file in &files {
                txn.put_artifact_placement(&file.sha256, ArtifactPlacement::record(ArtifactSource::Proxy, true));
            }
            Ok::<_, peryx_storage::meta::MetaError>(((), Vec::new()))
        })
        .unwrap();
    }
    let detail = ProjectDetail {
        meta: Meta::default(),
        name: "flask".to_owned(),
        versions: vec!["1.0".to_owned()],
        files: filenames
            .iter()
            .zip(&digests)
            .map(|(filename, digest)| File {
                filename: filename.clone(),
                url: format!("https://files.example/{filename}"),
                hashes: std::collections::BTreeMap::from([("sha256".to_owned(), digest.clone())]),
                requires_python: None,
                size: Some(1),
                upload_time: None,
                yanked: Yanked::No,
                core_metadata: CoreMetadata::Absent,
                dist_info_metadata: CoreMetadata::Absent,
                gpg_sig: None,
                provenance: Provenance::default(),
                authoritative_version: Some("1.0".to_owned()),
            })
            .collect(),
    };
    meta.put_index(
        "pypi/flask",
        &CachedIndex {
            body: to_json(&detail).into_bytes(),
            ..record
        },
    )
    .unwrap();
    assert!(meta.get_artifact_placement(digests.last().unwrap()).unwrap().is_some());
    drop(meta);
    UiReadFixture {
        blobs: directory.path().join("blobs"),
        _directory: directory,
        database,
        filenames,
        digests,
    }
}

fn ui_app(fixture: &UiReadFixture) -> axum::Router {
    let meta = MetaStore::open(&fixture.database).unwrap();
    let upstream = UpstreamClient::new("http://127.0.0.1:9/simple/").unwrap();
    let mut state = AppState::with_limits(
        meta,
        BlobStore::new(&fixture.blobs),
        3600,
        vec![Index {
            name: "pypi".to_owned(),
            route: "pypi".to_owned(),
            ecosystem: peryx_ecosystem_pypi::ECOSYSTEM,
            kind: IndexKind::Cached {
                client: upstream,
                offline: true,
            },
            policy: Policy::default(),
            acl: writer_acl("secret"),
        }],
        Arc::new(|| 1000),
        RateLimitConfig::default(),
        [("pypi".to_owned(), 0)],
    );
    peryx_plugin_registry::PluginRegistry::new(vec![peryx_ecosystem_pypi::registration()])
        .unwrap()
        .activate([peryx_ecosystem_pypi::ECOSYSTEM])
        .unwrap()
        .install_drivers(
            &mut state.runtime_install_context().unwrap(),
            &std::collections::HashMap::new(),
        )
        .unwrap();
    router(Arc::new(state))
}

fn enabled_limits() -> RateLimitConfig {
    RateLimitConfig {
        listing: RouteLimit::new(u64::MAX, 60),
        ..RateLimitConfig::enabled_defaults()
    }
}

async fn serve(app: axum::Router, uri: &str, accept: &str) {
    let request = Request::builder()
        .uri(uri)
        .header("accept", accept)
        .body(Body::empty())
        .unwrap();
    send(app, request).await;
}

async fn send(app: axum::Router, request: Request<Body>) {
    let response = app.oneshot(request).await.unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    let _ = response.into_body().collect().await.unwrap().to_bytes();
}

criterion_group!(benches, bench_serve, bench_project_ui_sources, bench_rate_limit);
criterion_main!(benches);

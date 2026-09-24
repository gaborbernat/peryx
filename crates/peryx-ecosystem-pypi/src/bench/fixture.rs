use std::collections::BTreeMap;
use std::future::IntoFuture as _;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, bail};
use axum::Router;
use axum::body::Body;
use axum::extract::{Path as RoutePath, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse as _, Response};
use axum::routing::get;
use futures_util::stream::{self, TryStreamExt as _};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::task::JoinHandle;
use tokio_util::io::ReaderStream;

use super::corpus::{Artifact, Corpus};

pub(super) struct Fixture {
    pub url: String,
    task: JoinHandle<std::io::Result<()>>,
}

impl Fixture {
    pub async fn start(corpus: &Corpus, scratch: &Path, client: &reqwest::Client) -> anyhow::Result<Self> {
        let cache = scratch.join("fixture-v1");
        std::fs::create_dir_all(&cache)?;
        stream::iter(corpus.artifacts.iter().map(Ok))
            .try_for_each_concurrent(4, |artifact| hydrate(artifact, &cache, client))
            .await?;
        let artifacts = corpus
            .artifacts
            .iter()
            .map(|artifact| {
                let metadata = wheel_metadata(&cache.join(&artifact.sha256), artifact)?;
                Ok((
                    crate::name::normalize_name(&artifact.project),
                    FixtureArtifact {
                        artifact: artifact.clone(),
                        metadata,
                    },
                ))
            })
            .collect::<anyhow::Result<_>>()?;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let router = Router::new()
            .route("/simple/", get(projects))
            .route("/simple/{project}/", get(project))
            .route("/files/{sha256}/{filename}", get(file))
            .with_state(Arc::new(FixtureState { cache, artifacts }));
        Ok(Self {
            url: format!("http://{}/simple/", listener.local_addr()?),
            task: tokio::spawn(axum::serve(listener, router).into_future()),
        })
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn hydrate(artifact: &Artifact, cache: &Path, client: &reqwest::Client) -> anyhow::Result<()> {
    let destination = cache.join(&artifact.sha256);
    if verify_file(&destination, artifact).await? {
        return Ok(());
    }
    let temporary = destination.with_extension("part");
    let mut response = client
        .get(&artifact.url)
        .send()
        .await
        .with_context(|| format!("cannot download {}", artifact.url))?
        .error_for_status()?;
    let mut output = tokio::fs::File::create(&temporary).await?;
    let mut digest = Sha256::new();
    let mut size = 0u64;
    while let Some(chunk) = response.chunk().await? {
        digest.update(&chunk);
        size = size.saturating_add(chunk.len() as u64);
        output.write_all(&chunk).await?;
    }
    output.sync_all().await?;
    if size != artifact.size || hex::encode(digest.finalize()) != artifact.sha256 {
        let _ = tokio::fs::remove_file(&temporary).await;
        bail!("downloaded artifact does not match {}", artifact.filename);
    }
    tokio::fs::rename(temporary, destination).await?;
    Ok(())
}

async fn verify_file(path: &Path, artifact: &Artifact) -> anyhow::Result<bool> {
    let Ok(mut file) = tokio::fs::File::open(path).await else {
        return Ok(false);
    };
    if file.metadata().await?.len() != artifact.size {
        return Ok(false);
    }
    let mut digest = Sha256::new();
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()) == artifact.sha256)
}

fn wheel_metadata(path: &Path, artifact: &Artifact) -> anyhow::Result<Vec<u8>> {
    let mut archive = zip::ZipArchive::new(std::fs::File::open(path)?)
        .with_context(|| format!("cannot open {} as a wheel", artifact.filename))?;
    let mut metadata = None;
    for index in 0..archive.len() {
        let mut member = archive.by_index(index)?;
        let name = member.name();
        if name.ends_with(".dist-info/METADATA") && name.matches('/').count() == 1 {
            if metadata.is_some() {
                bail!("{} contains multiple METADATA files", artifact.filename);
            }
            let mut bytes = Vec::new();
            member.read_to_end(&mut bytes)?;
            metadata = Some(bytes);
        }
    }
    metadata.with_context(|| format!("{} contains no METADATA file", artifact.filename))
}

struct FixtureArtifact {
    artifact: Artifact,
    metadata: Vec<u8>,
}

struct FixtureState {
    cache: PathBuf,
    artifacts: BTreeMap<String, FixtureArtifact>,
}

async fn projects(State(state): State<Arc<FixtureState>>, headers: HeaderMap) -> Response {
    if accepts_json(&headers) {
        (
            [(header::CONTENT_TYPE, "application/vnd.pypi.simple.v1+json")],
            axum::Json(serde_json::json!({
                "meta": {"api-version": "1.1"},
                "projects": state.artifacts.keys().map(|name| serde_json::json!({"name": name})).collect::<Vec<_>>()
            })),
        )
            .into_response()
    } else {
        html_response(format!(
            "<!doctype html><html><body>{}</body></html>",
            state
                .artifacts
                .keys()
                .map(|name| format!("<a href=\"{name}/\">{name}</a>"))
                .collect::<Vec<_>>()
                .join("\n")
        ))
    }
}

fn accepts_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("application/vnd.pypi.simple.v1+json"))
}

fn html_response(body: String) -> Response {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], body).into_response()
}

async fn project(
    State(state): State<Arc<FixtureState>>,
    RoutePath(project): RoutePath<String>,
    headers: HeaderMap,
) -> Response {
    let Some(entry) = state.artifacts.get(&crate::name::normalize_name(&project)) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let artifact = &entry.artifact;
    let path = format!(
        "../../files/{}/{filename}",
        artifact.sha256,
        filename = artifact.filename
    );
    let metadata_sha256 = hex::encode(Sha256::digest(&entry.metadata));
    if accepts_json(&headers) {
        (
            [(header::CONTENT_TYPE, "application/vnd.pypi.simple.v1+json")],
            axum::Json(serde_json::json!({
                "meta": {"api-version": "1.1"},
                "name": artifact.project,
                "versions": [artifact.version],
                "files": [{
                    "filename": artifact.filename,
                    "url": path,
                    "hashes": {"sha256": artifact.sha256},
                    "core-metadata": {"sha256": metadata_sha256},
                    "requires-python": artifact.requires_python,
                    "size": artifact.size,
                    "upload-time": "2026-01-01T00:00:00Z",
                    "yanked": false
                }]
            })),
        )
            .into_response()
    } else {
        html_response(format!(
            "<!doctype html><html><body><a href=\"{path}#sha256={sha256}\" data-core-metadata=\"sha256={metadata_sha256}\"{requires_python}>{filename}</a></body></html>",
            sha256 = artifact.sha256,
            filename = artifact.filename,
            requires_python = artifact.requires_python.as_ref().map_or_else(String::new, |value| {
                format!(" data-requires-python=\"{}\"", html_escape(value))
            })
        ))
    }
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

async fn file(
    State(state): State<Arc<FixtureState>>,
    RoutePath((sha256, filename)): RoutePath<(String, String)>,
) -> Response {
    let Some(entry) = state.artifacts.values().find(|entry| {
        let artifact = &entry.artifact;
        artifact.sha256 == sha256
            && (artifact.filename == filename || format!("{}.metadata", artifact.filename) == filename)
    }) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let artifact = &entry.artifact;
    if filename.ends_with(".metadata") {
        return (
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            entry.metadata.clone(),
        )
            .into_response();
    }
    let Ok(file) = tokio::fs::File::open(state.cache.join(&artifact.sha256)).await else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let mut response = Response::new(Body::from_stream(ReaderStream::new(file)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&artifact.size.to_string()).expect("artifact size is a valid header"),
    );
    response
}

#[cfg(test)]
#[path = "../../tests/unit/bench/fixture.rs"]
mod tests;

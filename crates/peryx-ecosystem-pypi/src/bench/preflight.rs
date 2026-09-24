use std::collections::BTreeSet;

use anyhow::{Context as _, bail};
use peryx_bench_core::context::BenchmarkContext;
use peryx_bench_core::servers::Server;
use sha2::{Digest as _, Sha256};
use url::Url;

use super::corpus::{Candidate, Corpus};
use super::workloads::SIMPLE_ACCEPT;

pub(super) async fn verify(
    context: &BenchmarkContext,
    servers: &[Server],
    corpus: &Corpus,
    client: &reqwest::Client,
) -> anyhow::Result<()> {
    let expected = corpus.candidates();
    for server in servers {
        let scratch = tempfile::tempdir_in(context.scratch())?;
        let state = scratch.path().join("state");
        std::fs::create_dir(&state)?;
        let active = server.start(context, &state, client).await?;
        let mut actual = BTreeSet::new();
        for artifact in &corpus.artifacts {
            let response = client
                .get(format!(
                    "{}{}/",
                    active.url,
                    crate::name::normalize_name(&artifact.project)
                ))
                .header(reqwest::header::ACCEPT, SIMPLE_ACCEPT)
                .send()
                .await?
                .error_for_status()
                .with_context(|| format!("{} omitted {}", server.name, artifact.project))?;
            let page_url = response.url().clone();
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            let body = response.bytes().await?;
            actual.extend(candidates_from_response(&artifact.project, &page_url, &content_type, &body, client).await?);
        }
        verify_candidates(server.name, &expected, &actual)?;
        println!(
            "[preflight] {} resolved {} matching candidates",
            server.name,
            actual.len()
        );
    }
    Ok(())
}

fn verify_candidates(server: &str, expected: &BTreeSet<Candidate>, actual: &BTreeSet<Candidate>) -> anyhow::Result<()> {
    if actual != expected {
        bail!(
            "{server} resolved a different candidate set: missing {:?}, extra {:?}",
            expected.difference(actual).collect::<Vec<_>>(),
            actual.difference(expected).collect::<Vec<_>>()
        );
    }
    Ok(())
}

async fn candidates_from_response(
    project: &str,
    page_url: &Url,
    content_type: &str,
    body: &[u8],
    client: &reqwest::Client,
) -> anyhow::Result<BTreeSet<Candidate>> {
    let detail = if content_type.starts_with("application/vnd.pypi.simple.v1+json") {
        crate::parse_detail(body)?
    } else {
        crate::parse_detail_html(project, std::str::from_utf8(body)?, page_url)?
    };
    let mut candidates = BTreeSet::new();
    for file in detail.files {
        let version = file
            .release_version()
            .context("candidate filename has no release version")?
            .to_owned();
        let sha256 = match file.sha256() {
            Some(sha256) => sha256.to_owned(),
            None => download_sha256(client, &file.url)
                .await
                .with_context(|| format!("cannot verify {}", file.filename))?,
        };
        candidates.insert(Candidate {
            project: crate::name::normalize_name(project),
            version,
            filename: file.filename,
            sha256,
        });
    }
    Ok(candidates)
}

async fn download_sha256(client: &reqwest::Client, url: &str) -> anyhow::Result<String> {
    let mut response = client.get(url).send().await?.error_for_status()?;
    let mut digest = Sha256::new();
    while let Some(chunk) = response.chunk().await? {
        digest.update(chunk);
    }
    Ok(hex::encode(digest.finalize()))
}

#[cfg(test)]
#[path = "../../tests/unit/bench/preflight.rs"]
mod tests;

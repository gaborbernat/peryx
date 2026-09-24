use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context as _, bail};
use serde::Deserialize;

use super::packages::FLEET_PROJECT;

const SCHEMA: u64 = 1;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Corpus {
    schema: u64,
    pub pip_version: String,
    pub python: String,
    pub platform: String,
    pub roots: Vec<String>,
    pub artifacts: Vec<Artifact>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Artifact {
    pub project: String,
    pub version: String,
    pub filename: String,
    pub url: String,
    pub sha256: String,
    pub size: u64,
    pub requires_python: Option<String>,
    pub dependencies: Vec<String>,
    pub requested: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Candidate {
    pub project: String,
    pub version: String,
    pub filename: String,
    pub sha256: String,
}

pub(super) fn selected() -> anyhow::Result<Corpus> {
    parse(PLATFORM_CORPUS.context("the benchmark corpus does not support this platform")?)
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
const PLATFORM_CORPUS: Option<&str> = Some(include_str!("fixtures/corpus-macos-aarch64.json"));
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const PLATFORM_CORPUS: Option<&str> = Some(include_str!("fixtures/corpus-linux-x86_64.json"));
#[cfg(not(any(
    all(target_os = "macos", target_arch = "aarch64"),
    all(target_os = "linux", target_arch = "x86_64")
)))]
const PLATFORM_CORPUS: Option<&str> = None;

pub(super) fn parse(raw: &str) -> anyhow::Result<Corpus> {
    let corpus: Corpus = serde_json::from_str(raw).context("benchmark corpus is not valid JSON")?;
    corpus.validate()?;
    Ok(corpus)
}

impl Corpus {
    pub fn candidates(&self) -> BTreeSet<Candidate> {
        self.artifacts
            .iter()
            .map(|artifact| Candidate {
                project: crate::name::normalize_name(&artifact.project),
                version: artifact.version.clone(),
                filename: artifact.filename.clone(),
                sha256: artifact.sha256.clone(),
            })
            .collect()
    }

    pub fn fleet_root(&self) -> anyhow::Result<&str> {
        let prefix = format!("{FLEET_PROJECT}==");
        self.roots
            .iter()
            .find(|root| root.starts_with(&prefix))
            .map(String::as_str)
            .with_context(|| format!("benchmark corpus has no pinned {FLEET_PROJECT} root"))
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.schema != SCHEMA {
            bail!("unsupported benchmark corpus schema {}", self.schema);
        }
        if self.pip_version.is_empty() || self.python.is_empty() || self.platform.is_empty() {
            bail!("benchmark corpus tool and platform versions must not be empty");
        }
        if self.roots.is_empty() || self.artifacts.is_empty() {
            bail!("benchmark corpus must contain roots and artifacts");
        }
        let mut artifacts = BTreeMap::new();
        for artifact in &self.artifacts {
            validate_artifact(artifact)?;
            let project = crate::name::normalize_name(&artifact.project);
            if artifacts.insert(project.clone(), artifact).is_some() {
                bail!("benchmark corpus repeats project {project}");
            }
        }
        let mut roots = BTreeSet::new();
        for root in &self.roots {
            let (project, version) = root
                .split_once("==")
                .with_context(|| format!("benchmark root {root:?} is not exactly pinned"))?;
            let project = crate::name::normalize_name(project);
            let artifact = artifacts
                .get(&project)
                .with_context(|| format!("benchmark root {root:?} has no artifact"))?;
            if artifact.version != version {
                bail!(
                    "benchmark root {root:?} resolves to {}=={}",
                    artifact.project,
                    artifact.version
                );
            }
            if !roots.insert(project.clone()) {
                bail!("benchmark corpus repeats root {project}");
            }
        }
        let requested = artifacts
            .iter()
            .filter(|(_, artifact)| artifact.requested)
            .map(|(project, _)| project.clone())
            .collect::<BTreeSet<_>>();
        if roots != requested {
            bail!("benchmark corpus requested artifacts do not match its roots");
        }
        for artifact in &self.artifacts {
            if let Some(missing) = artifact
                .dependencies
                .iter()
                .find(|dependency| !artifacts.contains_key(&crate::name::normalize_name(dependency)))
            {
                bail!(
                    "benchmark artifact {} depends on {missing}, which the corpus lacks",
                    artifact.filename
                );
            }
        }
        Ok(())
    }
}

fn validate_artifact(artifact: &Artifact) -> anyhow::Result<()> {
    let filename = artifact
        .url
        .rsplit('/')
        .next()
        .context("benchmark artifact URL has no filename")?;
    if filename != artifact.filename {
        bail!("benchmark artifact URL does not end with {}", artifact.filename);
    }
    if artifact.size == 0 {
        bail!("benchmark artifact {} has no bytes", artifact.filename);
    }
    if artifact.sha256.len() != 64 || !artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("benchmark artifact {} has an invalid sha256", artifact.filename);
    }
    if artifact.requires_python.as_deref() == Some("") || artifact.dependencies.iter().any(String::is_empty) {
        bail!("benchmark artifact {} has empty metadata", artifact.filename);
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/bench/corpus.rs"]
mod tests;

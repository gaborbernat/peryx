//! Mirroring OCI images: pull a list of image references (each manifest and every blob it names)
//! into the store so a cached index can serve them with no upstream, the container analogue of
//! `peryx mirror sync`. A manifest list is followed into its per-platform manifests.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use futures_util::{StreamExt as _, stream};
use parking_lot::Mutex;
use peryx_driver::ServingState;
use peryx_driver::rate_limit::UpstreamLimits;
use peryx_identity::DigestDecision;
use peryx_index::{Index, IndexKind};
use peryx_storage::blob::{BlobErrorKind, Digest};
use peryx_upstream::UpstreamClient;
use serde::Serialize;
use tokio::sync::Semaphore;

use crate::name::{Reference, parse_image_reference};
use crate::registry::{DownloadError, MAX_MANIFEST_BYTES, bounded_body, download_blob, serving_members};
use crate::settings::{IndexSettings, upstream_repo};
use crate::store::{self, Descriptors, Manifest};
use crate::upstream::Upstream;

/// The media type recorded for a manifest whose upstream response omits one.
const DEFAULT_MANIFEST_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";

/// Upstream and storage work one mirror run overlaps when its index is uncapped. Containerd pulls
/// three layers at a time by default, and a mirror run asks the same registries for the same content,
/// so it starts from the same bound rather than firing a whole image index at once.
const DEFAULT_MIRROR_CONCURRENCY: usize = 3;

/// How many distinct manifests one mirror run fetches before it gives up. A cyclic or explosively
/// wide graph, which a non-SHA-256 descriptor lets an upstream build without a fixed point, would
/// otherwise never drain the queue; the cap bounds the work well above any real multi-arch index.
const MAX_GRAPH_NODES: usize = 1024;

/// How deep a mirror run descends into nested manifest lists. Real images nest an index over a
/// handful of per-platform manifests; this leaves ample headroom while still ending a chain a hostile
/// upstream keeps extending.
const MAX_GRAPH_DEPTH: usize = 32;

/// One line of a mirror run: a manifest or blob that was synced, already cached, or failed, plus a
/// closing summary. The verb `kind` and `status` keep the report machine-readable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MirrorRow {
    pub kind: &'static str,
    pub index: String,
    pub repo: String,
    pub reference: String,
    pub digest: String,
    pub status: &'static str,
    pub bytes: u64,
    pub reason: String,
}

impl MirrorRow {
    pub(super) fn selected(index: &str, raw: &str) -> Self {
        parse_image_reference(raw).map_or_else(
            || Self::row("manifest", raw, "", "", "selected", 0, String::new()).with_index(index),
            |image| {
                let (Reference::Tag(reference) | Reference::Digest(reference)) = &image.reference;
                Self::row(
                    "manifest",
                    &image.repository,
                    reference,
                    "",
                    "selected",
                    0,
                    String::new(),
                )
                .with_index(index)
            },
        )
    }

    pub(super) fn count(index: &str, name: &'static str, value: u64) -> Self {
        Self::row("summary", "", name, "", name, value, String::new()).with_index(index)
    }

    fn with_index(mut self, index: &str) -> Self {
        index.clone_into(&mut self.index);
        self
    }

    fn synced(kind: &'static str, repo: &str, reference: &str, digest: &str, bytes: u64) -> Self {
        Self::row(kind, repo, reference, digest, "synced", bytes, String::new())
    }

    fn cached(kind: &'static str, repo: &str, reference: &str, digest: &str) -> Self {
        Self::row(kind, repo, reference, digest, "cached", 0, String::new())
    }

    fn error(kind: &'static str, repo: &str, reference: &str, digest: &str, reason: String) -> Self {
        Self::row(kind, repo, reference, digest, "error", 0, reason)
    }

    fn row(
        kind: &'static str,
        repo: &str,
        reference: &str,
        digest: &str,
        status: &'static str,
        bytes: u64,
        reason: String,
    ) -> Self {
        Self {
            kind,
            index: String::new(),
            repo: repo.to_owned(),
            reference: reference.to_owned(),
            digest: digest.to_owned(),
            status,
            bytes,
            reason,
        }
    }
}

/// What a mirror run does with each reference. `Sync` pulls anything missing; `Verify` only reports
/// whether the target repository already holds the manifest and every blob it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorMode {
    Sync,
    Verify,
}

struct MirrorRun<'a> {
    state: &'a Arc<ServingState>,
    upstream: &'a Upstream,
    members: Vec<MirrorMember<'a>>,
    mode: MirrorMode,
    /// One transfer at a time per blob digest. Manifests that share a layer are the common case a
    /// mirror run meets - platform manifests over one base, two tags of one image - and overlapping
    /// them would otherwise pull those bytes once per manifest. The second arrival waits for the
    /// first and then reads the store, which is the `cached` row a serial run reported.
    blob_locks: Mutex<HashMap<String, Arc<Semaphore>>>,
}

struct MirrorMember<'a> {
    index: &'a Index,
    client: Option<&'a UpstreamClient>,
    settings: IndexSettings,
    transfers: Semaphore,
    concurrency: usize,
}

enum ManifestOutcome {
    Missing,
    Rejected,
    Found(Descriptors),
}

#[derive(Clone, Copy)]
struct ManifestRequest<'a> {
    position: usize,
    consulted: &'a [&'a str],
    repo: &'a str,
    reference: &'a str,
    tag: Option<&'a str>,
    allow_missing: bool,
}

/// One manifest the walk reached: the rows it produced and the children it names, carried with the
/// depth it was scheduled at so its own children are bounded at the next level down.
struct WalkStep {
    rows: Vec<MirrorRow>,
    children: Vec<String>,
    depth: usize,
}

/// The overlap a mirror run may take against a cached index: whatever an operator already grants that
/// index's serving fetches, and [`DEFAULT_MIRROR_CONCURRENCY`] while it is uncapped.
fn mirror_ceiling(limits: &UpstreamLimits, index: &str) -> usize {
    match limits.snapshots().into_iter().find(|snapshot| snapshot.index == index) {
        Some(snapshot) if snapshot.max_concurrent > 0 => snapshot.max_concurrent,
        _ => DEFAULT_MIRROR_CONCURRENCY,
    }
}

/// Runs `work` under `gate`. The permit is released with `work`, so a manifest that fans out never
/// holds the gate while the blobs and children it fanned out to queue for one.
async fn gated<T>(gate: &Semaphore, work: impl Future<Output = T>) -> T {
    let _permit = gate
        .acquire()
        .await
        .expect("a mirror gate stays open for the whole run");
    work.await
}

/// # Errors
/// Returns an error only where the run cannot go on to make sound statements: the index names no
/// cached upstream to pull from, or the metadata or blob store faulted. Everything upstream governs -
/// a missing image, a registry that cannot be reached, a body over the manifest ceiling, a connection
/// that drops mid-body - is a reported row, so one bad reference costs the run neither the references
/// beside it nor the summary that closes it.
pub async fn mirror(
    state: &Arc<ServingState>,
    index: &Index,
    settings: IndexSettings,
    refs: &[String],
    mode: MirrorMode,
) -> anyhow::Result<Vec<MirrorRow>> {
    mirror_with_settings(state, index, refs, mode, |_| settings.clone()).await
}

pub async fn mirror_with_settings(
    state: &Arc<ServingState>,
    index: &Index,
    refs: &[String],
    mode: MirrorMode,
    settings: impl Fn(&str) -> IndexSettings,
) -> anyhow::Result<Vec<MirrorRow>> {
    let mut rows = Vec::new();
    let members = serving_members(state, index)
        .into_iter()
        .map(|member| {
            let concurrency = mirror_ceiling(&state.upstream_limits, &member.name);
            MirrorMember {
                index: member,
                client: member.proxy_client(),
                settings: settings(&member.name),
                transfers: Semaphore::new(concurrency),
                concurrency,
            }
        })
        .collect::<Vec<_>>();
    if !matches!(index.kind, IndexKind::Virtual { .. }) && members.iter().all(|member| member.client.is_none()) {
        anyhow::bail!("index {:?} has no cached upstream", index.name);
    }
    let concurrency = members.iter().map(|member| member.concurrency).max().unwrap_or(1);
    let upstream = Upstream::new();
    let context = MirrorRun {
        state,
        upstream: &upstream,
        members,
        mode,
        blob_locks: Mutex::default(),
    };
    // `buffered` hands references back in the order they were selected however they finish, so a
    // stalled root holds back only the rows behind it and the report still reads in selection order.
    let mut walked = stream::iter(refs.iter().cloned())
        .map(|raw| context.one_ref(raw))
        .buffered(concurrency);
    while let Some(walk) = walked.next().await {
        rows.extend(walk?);
    }
    let (synced, cached, errors, bytes) =
        rows.iter()
            .fold((0u64, 0u64, 0u64, 0u64), |(synced, cached, errors, bytes), row| {
                (
                    synced + u64::from(row.status == "synced"),
                    cached + u64::from(row.status == "cached"),
                    errors + u64::from(row.status == "error"),
                    bytes.saturating_add(row.bytes),
                )
            });
    rows.push(MirrorRow::row(
        "summary",
        "",
        "",
        "",
        verdict(synced + cached, errors),
        bytes,
        format!("{synced} synced, {cached} cached, {errors} errors"),
    ));
    for row in &mut rows {
        row.index.clone_from(&index.name);
    }
    Ok(rows)
}

/// The status the closing summary row carries, so an operator reads the run's verdict off a fixed
/// column instead of the counts beside it. A run that mirrored part of what it selected is neither
/// outcome the two-valued verdict could say: calling it `synced` hides content the mirror is missing,
/// and calling it `error` hides the images that are now available offline.
const fn verdict(kept: u64, errors: u64) -> &'static str {
    match (kept, errors) {
        (_, 0) => "synced",
        (0, _) => "error",
        _ => "partial",
    }
}

/// Enqueue a manifest's child descriptors for the walk, deduplicating by digest and holding the graph
/// within `MAX_GRAPH_NODES` and `MAX_GRAPH_DEPTH`. A child already scheduled this run is skipped, so a
/// cycle or diamond fetches each digest once. Returns `true` when a bound is hit, after recording the
/// error row, so the caller stops the walk.
fn schedule_children(
    repo: &str,
    children: Vec<String>,
    depth: usize,
    visited: &mut HashSet<String>,
    pending: &mut Vec<(String, usize)>,
    rows: &mut Vec<MirrorRow>,
) -> bool {
    for child in children {
        if !visited.insert(child.clone()) {
            continue;
        }
        if depth > MAX_GRAPH_DEPTH {
            let reason = format!("manifest graph exceeds depth {MAX_GRAPH_DEPTH}");
            rows.push(MirrorRow::error("manifest", repo, &child, "", reason));
            return true;
        }
        if visited.len() > MAX_GRAPH_NODES {
            let reason = format!("manifest graph exceeds {MAX_GRAPH_NODES} nodes");
            rows.push(MirrorRow::error("manifest", repo, &child, "", reason));
            return true;
        }
        pending.push((child, depth));
    }
    false
}

/// What a manifest depends on, or `None` after reporting the schema rule its body breaks.
///
/// An upstream is not a push: peryx never accepted these bytes over the registry API, so nothing has
/// checked that they are a manifest at all. A body the media type's schema rejects names no
/// dependencies, which the walk would otherwise read as a graph that is already complete.
fn descriptors_of(
    manifest: &Manifest,
    repo: &str,
    reference: &str,
    digest: &str,
    rows: &mut Vec<MirrorRow>,
) -> Option<Descriptors> {
    match store::validated_descriptors(&manifest.media_type, &manifest.bytes) {
        Ok(descriptors) => Some(descriptors),
        Err(fault) => {
            rows.push(MirrorRow::error("manifest", repo, reference, digest, fault.to_string()));
            None
        }
    }
}

/// Whether a failed blob ingest belongs to the reference or to the run, the split PR #2110 drew for
/// manifests. The two arrive at one call site, so the answer has to come from the error rather than
/// from where it was raised.
///
/// A stream that broke is what the upstream sent, and so is content that hashes to something other
/// than the digest the manifest named: the store refused those bytes and is otherwise intact, so the
/// rows beside them still mean what they say. Every other fault the store reports is peryx's own
/// side, and after one of those no later `cached` or `synced` row is a claim anything checked.
const fn reference_scoped(err: &DownloadError) -> bool {
    match err {
        DownloadError::Timeout | DownloadError::Stream(_) => true,
        DownloadError::Blob(err) => matches!(err.kind(), BlobErrorKind::DigestMismatch),
    }
}

impl MirrorRun<'_> {
    /// The name `repo` is spelled with upstream. What lands in the store keeps the operator's spelling,
    /// so a mirrored image serves under the name it was asked for.
    fn upstream_repo<'a>(member: &MirrorMember<'_>, repo: &'a str) -> std::borrow::Cow<'a, str> {
        let client = member.client.expect("only online members have an upstream repository");
        upstream_repo(member.settings.library_prefix, client.base_url(), repo)
    }

    /// Mirror one selected reference into its own slice of the report, so references that overlap
    /// never interleave their rows.
    async fn one_ref(&self, raw: String) -> anyhow::Result<Vec<MirrorRow>> {
        let mut rows = Vec::new();
        let Some(image) = parse_image_reference(&raw) else {
            rows.push(MirrorRow::error(
                "manifest",
                &raw,
                "",
                "",
                "not a valid image reference".to_owned(),
            ));
            return Ok(rows);
        };
        let (reference, tag) = match &image.reference {
            Reference::Tag(tag) => (tag.as_str(), Some(tag.as_str())),
            Reference::Digest(digest) => (digest.as_str(), None),
        };
        let mut consulted = Vec::with_capacity(self.members.len());
        for position in 0..self.members.len() {
            consulted.push(self.members[position].index.name.as_str());
            let outcome = self
                .root_manifest(position, &consulted, &image.repository, reference, tag, &mut rows)
                .await?;
            match outcome {
                ManifestOutcome::Missing => {}
                ManifestOutcome::Rejected => return Ok(rows),
                ManifestOutcome::Found(descriptors) => {
                    self.walk_manifest(position, &consulted, &image.repository, descriptors, &mut rows)
                        .await?;
                    return Ok(rows);
                }
            }
        }
        rows.push(MirrorRow::error(
            "manifest",
            &image.repository,
            reference,
            "",
            if tag.is_some() {
                "tag not mirrored".to_owned()
            } else {
                "manifest missing".to_owned()
            },
        ));
        Ok(rows)
    }

    fn manifest_blocked(
        &self,
        consulted: &[&str],
        repo: &str,
        reference: &str,
        tag: Option<&str>,
        rows: &mut Vec<MirrorRow>,
    ) -> anyhow::Result<bool> {
        if tag.is_none() && self.revoked(reference)? {
            rows.push(MirrorRow::error(
                "manifest",
                repo,
                reference,
                reference,
                "manifest digest is revoked".to_owned(),
            ));
            return Ok(true);
        }
        if self.reference_hidden(consulted, repo, reference, tag)? {
            rows.push(MirrorRow::error(
                "manifest",
                repo,
                reference,
                "",
                "manifest hidden by a deleted reference".to_owned(),
            ));
            return Ok(true);
        }
        Ok(false)
    }

    async fn root_manifest(
        &self,
        position: usize,
        consulted: &[&str],
        repo: &str,
        reference: &str,
        tag: Option<&str>,
        rows: &mut Vec<MirrorRow>,
    ) -> anyhow::Result<ManifestOutcome> {
        if self.manifest_blocked(consulted, repo, reference, tag, rows)? {
            return Ok(ManifestOutcome::Rejected);
        }
        let member = &self.members[position];
        if let Some(tag) = tag
            && let Some(digest) = store::get_tag(&self.state.meta, &member.index.name, repo, tag)?
            && self.revoked(&digest)?
        {
            rows.push(MirrorRow::error(
                "manifest",
                repo,
                reference,
                &digest,
                "manifest digest is revoked".to_owned(),
            ));
            return Ok(ManifestOutcome::Rejected);
        }
        if self.mode == MirrorMode::Verify || member.client.is_none() {
            return self.verify_manifest(position, consulted, repo, reference, tag, rows);
        }
        gated(
            &member.transfers,
            self.pull_manifest(
                ManifestRequest {
                    position,
                    consulted,
                    repo,
                    reference,
                    tag,
                    allow_missing: true,
                },
                rows,
            ),
        )
        .await
    }

    /// Pull one manifest and hand back what it depends on. `None` is a reference this run reported on
    /// and will not walk.
    async fn manifest_of(
        &self,
        position: usize,
        consulted: &[&str],
        repo: &str,
        reference: &str,
        tag: Option<&str>,
        rows: &mut Vec<MirrorRow>,
    ) -> anyhow::Result<Option<Descriptors>> {
        if self.manifest_blocked(consulted, repo, reference, tag, rows)? {
            return Ok(None);
        }
        let member = &self.members[position];
        if self.mode == MirrorMode::Verify || member.client.is_none() {
            return Ok(
                match self.verify_manifest(position, consulted, repo, reference, tag, rows)? {
                    ManifestOutcome::Found(descriptors) => Some(descriptors),
                    ManifestOutcome::Missing => {
                        rows.push(MirrorRow::error(
                            "manifest",
                            repo,
                            reference,
                            reference,
                            "manifest not mirrored for this repository".to_owned(),
                        ));
                        None
                    }
                    ManifestOutcome::Rejected => None,
                },
            );
        }
        Ok(
            match gated(
                &member.transfers,
                self.pull_manifest(
                    ManifestRequest {
                        position,
                        consulted,
                        repo,
                        reference,
                        tag,
                        allow_missing: false,
                    },
                    rows,
                ),
            )
            .await?
            {
                ManifestOutcome::Found(descriptors) => Some(descriptors),
                ManifestOutcome::Missing | ManifestOutcome::Rejected => None,
            },
        )
    }

    /// Fetch a manifest from upstream and record it. Held under the run's gate for the whole
    /// transfer: the body is the bulk of it, so releasing at the response head would let a run
    /// stream more manifests at once than the ceiling allows.
    async fn pull_manifest(
        &self,
        request: ManifestRequest<'_>,
        rows: &mut Vec<MirrorRow>,
    ) -> anyhow::Result<ManifestOutcome> {
        let ManifestRequest {
            position,
            consulted: _,
            repo,
            reference,
            tag,
            allow_missing,
        } = request;
        let member = &self.members[position];
        let client = member.client.expect("only online members pull manifests");
        let response = match self
            .upstream
            .manifest(
                client,
                &Self::upstream_repo(member, repo),
                reference,
                &member.settings.token_realms,
            )
            .await
        {
            Ok(response) => response,
            Err(crate::upstream::UpstreamError::Status(reqwest::StatusCode::NOT_FOUND)) if allow_missing => {
                return Ok(ManifestOutcome::Missing);
            }
            Err(err) => {
                rows.push(MirrorRow::error("manifest", repo, reference, "", err.to_string()));
                return Ok(ManifestOutcome::Rejected);
            }
        };
        let (advertised, manifest) = match Self::read_manifest(response).await {
            Ok(pulled) => pulled,
            // Nothing local has been written yet, so a body over the ceiling or a connection that
            // drops mid-stream costs this reference and leaves the rest of the run sound to report.
            Err(fault) => {
                rows.push(MirrorRow::error("manifest", repo, reference, "", fault));
                return Ok(ManifestOutcome::Rejected);
            }
        };
        let digest = format!("sha256:{}", Digest::of(&manifest.bytes).as_str());
        // A by-sha256-digest reference (no tag) pins the exact bytes; if the upstream, or a proxy
        // between, returns something else, storing it under the computed digest would report `synced`
        // while the requested manifest was never mirrored. A digest in another algorithm the spec
        // permits, peryx cannot recompute, so it trusts the upstream bytes rather than reject a valid
        // pull, the same guard the serving path keeps.
        let sha256_mismatch = reference.starts_with("sha256:") && reference != digest;
        if tag.is_none() && sha256_mismatch {
            rows.push(MirrorRow::error(
                "manifest",
                repo,
                reference,
                "",
                format!("upstream digest {digest} does not match requested {reference}"),
            ));
            return Ok(ManifestOutcome::Rejected);
        }
        if let Some(advertised) = advertised
            && advertised.starts_with("sha256:")
            && advertised != digest
        {
            rows.push(MirrorRow::error(
                "manifest",
                repo,
                reference,
                "",
                format!("upstream digest {advertised} does not match manifest content {digest}"),
            ));
            return Ok(ManifestOutcome::Rejected);
        }
        // Storing first and walking after would cache bytes no client can use under a digest the run
        // then reports complete, because a document that does not parse names no dependencies.
        let Some(descriptors) = descriptors_of(&manifest, repo, reference, &digest, rows) else {
            return Ok(ManifestOutcome::Rejected);
        };
        if self.revoked(&digest)? {
            rows.push(MirrorRow::error(
                "manifest",
                repo,
                reference,
                &digest,
                "manifest digest is revoked".to_owned(),
            ));
            return Ok(ManifestOutcome::Rejected);
        }
        if !self.record_manifest(request, &digest, &manifest, rows)? {
            return Ok(ManifestOutcome::Rejected);
        }
        rows.push(MirrorRow::synced(
            "manifest",
            repo,
            reference,
            &digest,
            manifest.bytes.len() as u64,
        ));
        Ok(ManifestOutcome::Found(descriptors))
    }

    async fn read_manifest(response: reqwest::Response) -> Result<(Option<String>, Manifest), String> {
        let advertised = response
            .headers()
            .get("docker-content-digest")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let media_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or(DEFAULT_MANIFEST_TYPE)
            .to_owned();
        let bytes = bounded_body(response, MAX_MANIFEST_BYTES).await.map_err(String::from)?;
        Ok((
            advertised,
            Manifest {
                media_type,
                bytes: bytes.to_vec(),
            },
        ))
    }

    fn record_manifest(
        &self,
        request: ManifestRequest<'_>,
        digest: &str,
        manifest: &Manifest,
        rows: &mut Vec<MirrorRow>,
    ) -> anyhow::Result<bool> {
        let member = &self.members[request.position];
        match store::record_mirrored_manifest(
            &self.state.meta,
            &store::MirroredManifest {
                consulted: request.consulted,
                index: &member.index.name,
                repo: request.repo,
                tag: request.tag,
                digest,
                manifest,
                fetched_at: (self.state.clock)(),
            },
        ) {
            Ok(store::MirroredManifestWrite::Published) => {}
            Ok(store::MirroredManifestWrite::Hidden) => {
                rows.push(MirrorRow::error(
                    "manifest",
                    request.repo,
                    request.reference,
                    digest,
                    "manifest hidden by a deleted reference".to_owned(),
                ));
                return Ok(false);
            }
            Err(store::ManifestWriteError::MediaTypeTooLong(_)) => {
                rows.push(MirrorRow::error(
                    "manifest",
                    request.repo,
                    request.reference,
                    digest,
                    "manifest media type exceeds the storage limit".to_owned(),
                ));
                return Ok(false);
            }
            Err(store::ManifestWriteError::Store(err)) => return Err(err.into()),
        }
        let search_invalidation = crate::search_oci::SearchInvalidationGuard::arm(self.state, request.repo);
        store::record_content_placement(&self.state.meta, digest, store::OciArtifactOrigin::Mirrored, true)?;
        drop(search_invalidation);
        Ok(true)
    }

    fn verify_manifest(
        &self,
        position: usize,
        consulted: &[&str],
        repo: &str,
        reference: &str,
        tag: Option<&str>,
        rows: &mut Vec<MirrorRow>,
    ) -> anyhow::Result<ManifestOutcome> {
        let member = &self.members[position];
        let digest = if let Some(tag) = tag {
            let Some(digest) = store::get_tag(&self.state.meta, &member.index.name, repo, tag)? else {
                return Ok(ManifestOutcome::Missing);
            };
            digest
        } else {
            if !store::manifest_is_member(&self.state.meta, &member.index.name, repo, reference)? {
                if self.members.len() == 1 && store::get_manifest(&self.state.meta, reference)?.is_some() {
                    rows.push(MirrorRow::error(
                        "manifest",
                        repo,
                        reference,
                        reference,
                        "manifest not mirrored for this repository".to_owned(),
                    ));
                    return Ok(ManifestOutcome::Rejected);
                }
                return Ok(ManifestOutcome::Missing);
            }
            reference.to_owned()
        };
        if self.reference_hidden(consulted, repo, &digest, None)? || self.revoked(&digest)? {
            rows.push(MirrorRow::error(
                "manifest",
                repo,
                reference,
                &digest,
                "manifest is not available".to_owned(),
            ));
            return Ok(ManifestOutcome::Rejected);
        }
        let Some(manifest) = store::get_manifest(&self.state.meta, &digest)? else {
            rows.push(MirrorRow::error(
                "manifest",
                repo,
                reference,
                &digest,
                "manifest missing".to_owned(),
            ));
            return Ok(ManifestOutcome::Rejected);
        };
        // Manifest bytes dedupe into one content-addressed store, so holding them is not proof this
        // repository serves them. A tag read is already keyed by repository; a digest names the shared
        // store, and a by-digest pull authorizes against membership, so counting another repository's
        // cached bytes would call an image ready for offline use that a pull answers `manifest unknown`.
        if !store::manifest_is_member(&self.state.meta, &member.index.name, repo, &digest)? {
            rows.push(MirrorRow::error(
                "manifest",
                repo,
                reference,
                &digest,
                "manifest not mirrored for this repository".to_owned(),
            ));
            return Ok(ManifestOutcome::Rejected);
        }
        // A stored manifest that no longer parses cannot be reported cached: its empty descriptor list
        // would pass verification for an image whose layers were never mirrored.
        let Some(descriptors) = descriptors_of(&manifest, repo, reference, &digest, rows) else {
            return Ok(ManifestOutcome::Rejected);
        };
        rows.push(MirrorRow::cached("manifest", repo, reference, &digest));
        Ok(ManifestOutcome::Found(descriptors))
    }

    fn reference_hidden(
        &self,
        consulted: &[&str],
        repo: &str,
        reference: &str,
        tag: Option<&str>,
    ) -> anyhow::Result<bool> {
        for member in consulted {
            if let Some(tag) = tag
                && store::tag_is_trashed(&self.state.meta, member, repo, tag)?
            {
                return Ok(true);
            }
            if store::manifest_is_trashed(&self.state.meta, member, repo, reference)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn revoked(&self, digest: &str) -> anyhow::Result<bool> {
        let Ok(digest) = digest.parse() else {
            return Ok(false);
        };
        Ok(self.state.revocations.decision(&digest)? == DigestDecision::Revoked)
    }

    /// Follow a manifest to the blobs it needs, one level of the graph at a time: an image index
    /// enqueues its per-platform manifests; an image manifest names a config blob and layers. A level
    /// is fetched as a whole, so siblings a parent named together overlap up to the run's ceiling
    /// instead of each waiting out the one before it.
    ///
    /// A descriptor digest is scheduled at most once per run, so a self-referential or cyclic graph
    /// terminates and a diamond fetches each shared descendant a single time. Scheduling stays serial
    /// between levels, which is what keeps that deduplication and the bounds exact: a level is
    /// enqueued only once every sibling that could have named the same child has been parsed.
    /// Bounds are enforced when a child is scheduled, before it is fetched, so a graph a hostile
    /// upstream keeps growing stops on a stable error row without the fetch that would follow.
    async fn walk_manifest(
        &self,
        position: usize,
        consulted: &[&str],
        repo: &str,
        descriptors: Descriptors,
        rows: &mut Vec<MirrorRow>,
    ) -> anyhow::Result<()> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut pending: Vec<(String, usize)> = Vec::new();
        let (children, blobs) = descriptors;
        self.blobs(position, repo, blobs, rows).await?;
        if schedule_children(repo, children, 1, &mut visited, &mut pending, rows) {
            return Ok(());
        }
        while !pending.is_empty() {
            let mut walked = stream::iter(std::mem::take(&mut pending))
                .map(|(digest, depth)| self.node(position, consulted, repo, digest, depth))
                .buffered(self.members[position].concurrency);
            let mut level = Vec::new();
            while let Some(step) = walked.next().await {
                level.push(step?);
            }
            for step in level {
                rows.extend(step.rows);
                if schedule_children(repo, step.children, step.depth + 1, &mut visited, &mut pending, rows) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Pull one scheduled manifest and the blobs it names. A failure here is a row of this step, so a
    /// sibling that cannot be reached leaves the rest of its level running.
    async fn node(
        &self,
        position: usize,
        consulted: &[&str],
        repo: &str,
        digest: String,
        depth: usize,
    ) -> anyhow::Result<WalkStep> {
        let mut rows = Vec::new();
        let children = match self
            .manifest_of(position, consulted, repo, &digest, None, &mut rows)
            .await?
        {
            Some((children, blobs)) => {
                self.blobs(position, repo, blobs, &mut rows).await?;
                children
            }
            None => Vec::new(),
        };
        Ok(WalkStep { rows, children, depth })
    }

    /// Pull every blob one manifest names, overlapping them under the run's ceiling and reporting
    /// them in descriptor order. A level is drained before a store fault is raised, so the transfers
    /// already in flight finish and the fault the run ends on is the first in descriptor order rather
    /// than whichever future happened to fail first.
    async fn blobs(
        &self,
        position: usize,
        repo: &str,
        blobs: Vec<String>,
        rows: &mut Vec<MirrorRow>,
    ) -> anyhow::Result<()> {
        let mut pulled = stream::iter(blobs)
            .map(|digest| self.blob(position, repo, digest))
            .buffered(self.members[position].concurrency);
        let mut fault = None;
        while let Some(pull) = pulled.next().await {
            match pull {
                Ok(row) => rows.push(row),
                Err(err) => fault = fault.or(Some(err)),
            }
        }
        fault.map_or(Ok(()), Err)
    }

    /// Claim `digest` for the run. Taken before the ceiling permit, so a manifest waiting on a layer
    /// another manifest is already pulling occupies no transfer slot while it waits.
    fn blob_lock(&self, digest: &str) -> Arc<Semaphore> {
        Arc::clone(
            self.blob_locks
                .lock()
                .entry(digest.to_owned())
                .or_insert_with(|| Arc::new(Semaphore::new(1))),
        )
    }

    async fn blob(&self, position: usize, repo: &str, descriptor: String) -> anyhow::Result<MirrorRow> {
        let digest = descriptor.as_str();
        let Some(storage) = store::blob_digest(digest) else {
            return Ok(MirrorRow::error(
                "blob",
                repo,
                digest,
                digest,
                "unsupported digest".to_owned(),
            ));
        };
        let single_flight = self.blob_lock(digest);
        let _claim = single_flight
            .acquire()
            .await
            .expect("a mirror blob claim outlives the run that waits on it");
        let member = &self.members[position];
        gated(&member.transfers, self.transfer_blob(position, repo, digest, &storage)).await
    }

    async fn transfer_blob(
        &self,
        position: usize,
        repo: &str,
        digest: &str,
        storage: &Digest,
    ) -> anyhow::Result<MirrorRow> {
        let member = &self.members[position];
        if self.revoked(digest)? {
            return Ok(MirrorRow::error(
                "blob",
                repo,
                digest,
                digest,
                "blob digest is revoked".to_owned(),
            ));
        }
        let member_holds = store::blob_is_member(&self.state.meta, &member.index.name, repo, digest)?;
        match self.state.blobs.head(storage).await {
            Ok(Some(_)) if member_holds => return Ok(MirrorRow::cached("blob", repo, digest, digest)),
            Ok(Some(_)) if self.mode == MirrorMode::Verify => {
                return Ok(MirrorRow::error(
                    "blob",
                    repo,
                    digest,
                    digest,
                    "blob not mirrored for this repository".to_owned(),
                ));
            }
            Ok(Some(_) | None) => {}
            // Asking the store what it holds is peryx's own side. Once that fails the run no longer
            // knows what is cached, which is what a `cached` row claims.
            Err(err) => anyhow::bail!("blob store error: {err}"),
        }
        if self.mode == MirrorMode::Verify {
            return Ok(MirrorRow::error(
                "blob",
                repo,
                digest,
                digest,
                "blob missing".to_owned(),
            ));
        }
        let Some(client) = member.client else {
            return Ok(MirrorRow::error(
                "blob",
                repo,
                digest,
                digest,
                "blob missing from the selected index member".to_owned(),
            ));
        };
        match self
            .upstream
            .blob(
                client,
                &Self::upstream_repo(member, repo),
                digest,
                &member.settings.token_realms,
            )
            .await
        {
            Ok(response) => match download_blob(&self.state.meta, &self.state.blobs, storage, response).await {
                Ok(_) if self.revoked(digest)? => Ok(MirrorRow::error(
                    "blob",
                    repo,
                    digest,
                    digest,
                    "blob digest was revoked during download".to_owned(),
                )),
                Ok(bytes) => {
                    store::record_blob_membership(&self.state.meta, &member.index.name, repo, digest)?;
                    Ok(MirrorRow::synced("blob", repo, digest, digest, bytes))
                }
                Err(err) if reference_scoped(&err) => {
                    Ok(MirrorRow::error("blob", repo, digest, digest, err.to_string()))
                }
                Err(err) => Err(anyhow::Error::new(err)),
            },
            Err(err) => Ok(MirrorRow::error("blob", repo, digest, digest, err.to_string())),
        }
    }
}

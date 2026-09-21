use super::*;
use crate::error::{ErrorCode, error_response};
use crate::store::{self};
use axum::body::Body;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use mediatype::MediaType;
use peryx_driver::ServingState;
use peryx_upstream::UpstreamClient;

const TAG_RESOLUTION_CONCURRENCY: usize = 8;
const OCI_INDEX_TYPE: &str = "application/vnd.oci.image.index.v1+json";
const MAX_PAGE_SIZE: usize = 1_000;
const TAG_PAGE_CACHE_PREFIX: &str = "oci\0tp\0";
const MAX_TAG_PAGE_CACHE_ROWS: usize = 1_024;
const MAX_TAG_PAGE_CACHE_BYTES: usize = 64 << 20;
const TAG_PAGE_DELETE_BATCH: usize = 1_024;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
struct Pagination {
    limit: Option<usize>,
    last: Option<String>,
    clamped: bool,
}

impl Pagination {
    fn parse(query: &str) -> Result<Self, &'static str> {
        let mut limit = None;
        let mut last = None;
        let mut clamped = false;
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let Ok(key) = percent_decode(key) else {
                continue;
            };
            match key.as_str() {
                "n" if limit.is_some() => return Err("duplicate n query parameter"),
                "n" => {
                    let value = percent_decode(value)?;
                    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                        return Err("invalid n query parameter");
                    }
                    let limit_value = value.parse::<usize>().map_err(|_| "invalid n query parameter")?;
                    limit = Some(limit_value.min(MAX_PAGE_SIZE));
                    clamped = limit_value > MAX_PAGE_SIZE;
                }
                "last" if last.is_some() => return Err("duplicate last query parameter"),
                "last" => {
                    let value = percent_decode(value)?;
                    if value.is_empty() {
                        return Err("invalid last query parameter");
                    }
                    last = Some(value);
                }
                _ => {}
            }
        }
        Ok(Self { limit, last, clamped })
    }

    fn query(&self) -> String {
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        if let Some(limit) = self.limit {
            query.append_pair("n", &limit.to_string());
        }
        if let Some(last) = &self.last {
            query.append_pair("last", last);
        }
        query.finish()
    }

    const fn next(&self, last: String) -> Self {
        Self {
            limit: self.limit,
            last: Some(last),
            clamped: self.clamped,
        }
    }
}

fn percent_decode(value: &str) -> Result<String, &'static str> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let Some((&high, &low)) = bytes.get(index + 1).zip(bytes.get(index + 2)) else {
                return Err("invalid percent encoding");
            };
            let hex = |byte| match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                b'A'..=b'F' => Some(byte - b'A' + 10),
                _ => None,
            };
            let Some(high) = hex(high) else {
                return Err("invalid percent encoding");
            };
            let Some(low) = hex(low) else {
                return Err("invalid percent encoding");
            };
            decoded.push(high << 4 | low);
            index += 3;
        } else if bytes[index] == b'+' {
            decoded.push(b' ');
            index += 1;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| "query parameter is not UTF-8")
}

struct ProxyTagPage {
    response: Response,
    tags: Vec<String>,
    next: Option<Pagination>,
    stale_error: Option<crate::upstream::UpstreamError>,
}

enum TagTarget {
    Digest(String),
    Missing,
    Failed(crate::upstream::UpstreamError),
}

enum TagFilter {
    Visible(Vec<String>),
    Unresolved(Response),
}

impl<S: BuildHasher + Default + Send + Sync + 'static> OciRegistryWithHasher<S> {
    /// Serve the tag list. With no active revocations a lone online proxy passes upstream through;
    /// every other case filters and unions member tags before applying `n`/`last` pagination.
    pub(super) async fn serve_tags(
        &self,
        state: &ServingState,
        name: &str,
        query: &str,
    ) -> Result<Response, ServeError> {
        let Some((index, repo)) = resolve(&state.indexes, name) else {
            return Ok(error_response(ErrorCode::NameUnknown, "repository name unknown"));
        };
        if policy_blocks(index, PolicyAction::Serve, repo) {
            return Ok(error_response(ErrorCode::NameUnknown, "repository name unknown"));
        }
        // A tag list is a mutable derived view, so a replica hides a hosted index's until the search
        // view catches the serial that changed it.
        if holds_below_readable_frontier(state, index, hosted_last_serial(state, index)?) {
            return Ok(error_response(ErrorCode::NameUnknown, "repository name unknown"));
        }
        let pagination = match Pagination::parse(query) {
            Ok(pagination) => pagination,
            Err(message) => return Ok(error_response(ErrorCode::NameInvalid, message)),
        };
        if pagination.limit == Some(0) {
            return Ok(tag_list_response(name, &std::collections::BTreeSet::new(), &pagination));
        }
        let active = state.revocations.has_active()?;
        let members = policy_serving_members(state, index, repo);
        if let [member] = members.as_slice()
            && let Some(client) = member.proxy_client()
        {
            let page = self
                .proxy_tags(state, name, &member.name, client, repo, &pagination)
                .await?;
            return if active {
                self.filter_proxy_tag_page(state, name, &member.name, client, repo, page)
                    .await
                    .map(|page| page.response)
            } else {
                Ok(serve_proxy_tag_page(name, page))
            };
        }
        let tags = self.visible_tag_names(state, name, repo, active, &members).await?;
        Ok(tag_list_response(name, &tags, &pagination))
    }

    /// Collect tag names in member-shadowing order, with tombstones masking only their own or lower
    /// layers. The second tombstone pass closes a delete race while upstream pages are fetched.
    pub(super) async fn visible_tag_names(
        &self,
        state: &ServingState,
        name: &str,
        repo: &str,
        active: bool,
        members: &[&Index],
    ) -> Result<std::collections::BTreeSet<String>, ServeError> {
        if let [member] = members
            && member.proxy_client().is_none()
        {
            let mut tags = stored_tag_names(state, &member.name, repo, active)?
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>();
            for tag in store::list_trashed_tags(&state.meta, &member.name, repo)? {
                tags.remove(&tag);
            }
            return Ok(tags);
        }
        let mut tags = std::collections::BTreeMap::new();
        let mut hidden = std::collections::BTreeMap::new();
        for (position, member) in members.iter().enumerate() {
            let names = match member.proxy_client() {
                Some(client) => self
                    .fetch_tag_names(state, name, &member.name, client, repo, active)
                    .await?
                    .unwrap_or_default(),
                None => stored_tag_names(state, &member.name, repo, active)?,
            };
            for tag in names {
                tags.entry(tag).or_insert(position);
            }
            for tag in store::list_trashed_tags(&state.meta, &member.name, repo)? {
                hidden.entry(tag).or_insert(position);
            }
        }
        for (position, member) in members.iter().enumerate() {
            for tag in store::list_trashed_tags(&state.meta, &member.name, repo)? {
                hidden.entry(tag).or_insert(position);
            }
        }
        tags.retain(|tag, source| hidden.get(tag).is_none_or(|tombstone| tombstone > source));
        Ok(tags.into_keys().collect())
    }

    /// Serve a lone proxy's tag list, from the store while it is fresh.
    ///
    /// A tag list is mutable upstream, so it is trusted for `ttl_secs` and revalidated after. Passing
    /// every request through made a `tags/list` cost an upstream round trip rather than the registry,
    /// and made a burst of them cost the upstream once per client. When revalidation fails, the last
    /// tag list remains available until its stale-cache limit.
    async fn proxy_tags(
        &self,
        state: &ServingState,
        name: &str,
        index: &str,
        client: &UpstreamClient,
        repo: &str,
        pagination: &Pagination,
    ) -> Result<ProxyTagPage, ServeError> {
        let now = (state.clock)();
        let upstream_repo = self.upstream_repo(index, client, repo);
        let query = pagination.query();
        let cached = match store::tag_page(&state.meta, index, repo, &query)? {
            store::TagPageRead::Page(page) => {
                if let (Ok(tags), Ok(next)) = (
                    validate_tag_page(&page.2, &upstream_repo),
                    cached_next(page.1.as_deref(), pagination),
                ) && valid_tag_page(tags.len(), next.as_ref(), pagination)
                {
                    Some((page, tags, next))
                } else {
                    self.delete_tag_page(state, index, repo, &query).await?;
                    None
                }
            }
            store::TagPageRead::Invalid => {
                self.delete_tag_page(state, index, repo, &query).await?;
                None
            }
            store::TagPageRead::Missing => None,
        };
        let cached = match cached {
            Some(page) if state.max_stale_secs != 0 && !within_stale_bound(state, page.0.0) => {
                self.delete_tag_page(state, index, repo, &query).await?;
                None
            }
            cached => cached,
        };
        if let Some(((fetched_at, _, body), tags, next)) = &cached
            && now.saturating_sub(*fetched_at) < state.ttl_secs
        {
            return Ok(ProxyTagPage {
                response: tag_page_response(name, next.as_ref(), body.clone()),
                tags: tags.clone(),
                next: next.clone(),
                stale_error: None,
            });
        }
        match self
            .upstream
            .tags(client, &upstream_repo, &query, &self.token_realms(index))
            .await
        {
            Ok(response) => {
                let next = next_page(response.headers(), pagination)?;
                let body = bounded_body(response, MAX_TAGS_BYTES).await?;
                let tags = validate_tag_page(&body, &upstream_repo)?;
                if !valid_tag_page(tags.len(), next.as_ref(), pagination) {
                    return Err(invalid_tag_page("upstream tag page exceeds its effective limit"));
                }
                let link = next.as_ref().map(Pagination::query);
                let _guard = self.tag_page_gate.lock().await;
                store_tag_page(state, index, repo, &query, now, link.as_deref(), &body)?;
                Ok(ProxyTagPage {
                    response: tag_page_response(name, next.as_ref(), body.to_vec()),
                    tags,
                    next,
                    stale_error: None,
                })
            }
            Err(err) => match cached {
                Some(((fetched_at, _, body), tags, next)) if within_stale_bound(state, fetched_at) => {
                    Ok(ProxyTagPage {
                        response: tag_page_response(name, next.as_ref(), body),
                        tags,
                        next,
                        stale_error: Some(err),
                    })
                }
                _ => Ok(ProxyTagPage {
                    response: upstream_error_response(&err, "tags"),
                    tags: Vec::new(),
                    next: None,
                    stale_error: None,
                }),
            },
        }
    }

    async fn delete_tag_page(
        &self,
        state: &ServingState,
        index: &str,
        repo: &str,
        query: &str,
    ) -> Result<(), ServeError> {
        let _guard = self.tag_page_gate.lock().await;
        store::delete_tag_page(&state.meta, index, repo, query)?;
        Ok(())
    }

    /// Fetch a proxy member's tag names for aggregation, or `None` on any upstream failure so one
    /// unreachable member does not fail the whole list.
    pub(super) async fn fetch_tag_names(
        &self,
        state: &ServingState,
        name: &str,
        index: &str,
        client: &UpstreamClient,
        repo: &str,
        active: bool,
    ) -> Result<Option<Vec<String>>, ServeError> {
        let mut names = Vec::new();
        let mut pagination = Pagination::default();
        let mut seen = std::collections::HashSet::from([pagination.clone()]);
        let mut page = 0;
        loop {
            // Each page is cached under its own query, so a virtual index that unions several proxies
            // no longer re-walks every upstream's pagination on every request.
            let fetched = self.proxy_tags(state, name, index, client, repo, &pagination).await?;
            let tag_page = if active {
                self.filter_proxy_tag_page(state, name, index, client, repo, fetched)
                    .await?
            } else {
                fetched
            };
            if !tag_page.response.status().is_success() {
                return Ok(None);
            }
            names.extend(tag_page.tags);
            page += 1;
            let Some(next) = tag_page.next else {
                break;
            };
            if !seen.insert(next.clone()) {
                return Err(invalid_tag_page("upstream tag pagination cycles"));
            }
            if page == MAX_TAG_PAGES {
                return Err(invalid_tag_page("upstream tag pagination exceeds its page budget"));
            }
            pagination = next;
        }
        Ok(Some(names))
    }

    async fn filter_proxy_tag_page(
        &self,
        state: &ServingState,
        name: &str,
        index: &str,
        client: &UpstreamClient,
        repo: &str,
        page: ProxyTagPage,
    ) -> Result<ProxyTagPage, ServeError> {
        let ProxyTagPage {
            response,
            tags,
            next,
            stale_error,
        } = page;
        if !response.status().is_success() {
            return Ok(ProxyTagPage {
                response,
                tags,
                next,
                stale_error,
            });
        }
        drop(response);
        let tags = match self
            .visible_proxy_tags(state, index, client, repo, tags, stale_error.as_ref())
            .await?
        {
            TagFilter::Visible(tags) => tags,
            TagFilter::Unresolved(response) => {
                return Ok(ProxyTagPage {
                    response,
                    tags: Vec::new(),
                    next: None,
                    stale_error: None,
                });
            }
        };
        Ok(ProxyTagPage {
            response: tag_page_response(name, next.as_ref(), tag_page_body(name, &tags)),
            tags,
            next,
            stale_error: None,
        })
    }

    async fn visible_proxy_tags(
        &self,
        state: &ServingState,
        index: &str,
        client: &UpstreamClient,
        repo: &str,
        tags: Vec<String>,
        stale_error: Option<&crate::upstream::UpstreamError>,
    ) -> Result<TagFilter, ServeError> {
        let mut visible = Vec::new();
        if let Some(error) = stale_error {
            for tag in tags {
                let Some(digest) = stale_tag_digest(state, index, repo, &tag)? else {
                    return Ok(TagFilter::Unresolved(upstream_error_response(error, "tags")));
                };
                if digest_decision(state, &digest)? == DigestDecision::Clear {
                    visible.push(tag);
                }
            }
            return Ok(TagFilter::Visible(visible));
        }
        for (tag, target) in self.refresh_tag_targets(state, index, client, repo, tags).await? {
            let digest = match target {
                TagTarget::Digest(digest) => digest,
                TagTarget::Missing => continue,
                TagTarget::Failed(error) => match stale_tag_digest(state, index, repo, &tag)? {
                    Some(digest) => digest,
                    None => return Ok(TagFilter::Unresolved(upstream_error_response(&error, "tags"))),
                },
            };
            if digest_decision(state, &digest)? == DigestDecision::Clear {
                visible.push(tag);
            }
        }
        Ok(TagFilter::Visible(visible))
    }

    async fn refresh_tag_targets(
        &self,
        state: &ServingState,
        index: &str,
        client: &UpstreamClient,
        repo: &str,
        tags: Vec<String>,
    ) -> Result<Vec<(String, TagTarget)>, ServeError> {
        futures_util::stream::iter(tags.into_iter().map(|tag| async move {
            let target = self.refresh_tag_target(state, index, client, repo, &tag).await?;
            Ok::<_, ServeError>((tag, target))
        }))
        // Keep errors in page order so concurrent refreshes cannot change the response status.
        .buffered(TAG_RESOLUTION_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
    }

    async fn refresh_tag_target(
        &self,
        state: &ServingState,
        index: &str,
        client: &UpstreamClient,
        repo: &str,
        tag: &str,
    ) -> Result<TagTarget, ServeError> {
        if let Some((fetched_at, digest)) = store::tag_freshness(&state.meta, index, repo, tag)?
            && (state.clock)().saturating_sub(fetched_at) < state.ttl_secs
        {
            return Ok(TagTarget::Digest(digest));
        }
        let digest = match self
            .upstream
            .manifest_digest(
                client,
                &self.upstream_repo(index, client, repo),
                tag,
                &self.token_realms(index),
            )
            .await
        {
            Ok(Some(digest)) => digest,
            Ok(None) => {
                return Ok(TagTarget::Failed(crate::upstream::UpstreamError::Transport(
                    "upstream manifest response carries no docker-content-digest".to_owned(),
                )));
            }
            Err(crate::upstream::UpstreamError::Status(StatusCode::NOT_FOUND)) => return Ok(TagTarget::Missing),
            Err(error) => return Ok(TagTarget::Failed(error)),
        };
        let changed = store::put_tag(&state.meta, index, repo, tag, &digest)?;
        let search_invalidation = changed.then(|| crate::search_oci::SearchInvalidationGuard::arm(state, repo));
        store::set_tag_freshness(&state.meta, index, repo, tag, &digest, (state.clock)())?;
        if let Some(search_invalidation) = search_invalidation {
            drop(search_invalidation);
        }
        Ok(TagTarget::Digest(digest))
    }

    /// The referrer descriptors upstream records for `repo`/`digest`. A registry predating the referrers
    /// API answers `404`; the spec then directs a fallback to the referrers tag schema, an image index
    /// tagged after the subject digest, so a signature or SBOM pushed before the API existed stays
    /// discoverable through the cache.
    async fn upstream_referrers(
        &self,
        state: &ServingState,
        index: &str,
        client: &UpstreamClient,
        repo: &str,
        digest: &str,
    ) -> Result<Vec<serde_json::Value>, ReferrerLookupError> {
        let now = (state.clock)();
        if let Some((fetched_at, manifests)) = store::referrer_page(&state.meta, index, repo, digest)?
            && now.saturating_sub(fetched_at) < state.ttl_secs
        {
            return Ok(manifests);
        }
        let upstream_repo = self.upstream_repo(index, client, repo);
        let manifests = match self
            .upstream
            .referrers(client, &upstream_repo, digest, &self.token_realms(index))
            .await
        {
            Ok(response) => referrer_manifests(response, ReferrerSource::Native).await?,
            Err(crate::upstream::UpstreamError::Status(StatusCode::NOT_FOUND)) => {
                match self
                    .upstream
                    .manifest(
                        client,
                        &upstream_repo,
                        &crate::name::referrers_tag(digest),
                        &self.token_realms(index),
                    )
                    .await
                {
                    Ok(response) => referrer_manifests(response, ReferrerSource::Fallback).await?,
                    Err(crate::upstream::UpstreamError::Status(StatusCode::NOT_FOUND)) => Vec::new(),
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        };
        store::set_referrer_page(&state.meta, index, repo, digest, now, &manifests)?;
        Ok(manifests)
    }

    /// Serve `GET /v2/<name>/referrers/<digest>`: the manifests that declare the digest their subject,
    /// unioning what each member stored with what an online proxy's upstream reports, so a signature or
    /// SBOM pushed upstream is discoverable through a cached image. `artifactType` filters the result
    /// and is echoed in `OCI-Filters-Applied`.
    pub(super) async fn serve_referrers(
        &self,
        state: &ServingState,
        name: &str,
        digest: &str,
        query: &str,
    ) -> Result<Response, ServeError> {
        let Some((index, repo)) = resolve(&state.indexes, name) else {
            return Ok(error_response(ErrorCode::NameUnknown, "repository name unknown"));
        };
        if policy_blocks(index, PolicyAction::Serve, repo) {
            return Ok(error_response(ErrorCode::NameUnknown, "repository name unknown"));
        }
        if !crate::name::valid_content_digest(digest) {
            return Ok(error_response(
                ErrorCode::DigestInvalid,
                "referrers digest is malformed",
            ));
        }
        let filter = query_params(query).remove("artifactType");
        // The referrers list is a mutable derived view, so a replica hides a hosted index's until the
        // search view catches the serial that changed it, reporting an empty set as the spec's response
        // to a subject with none.
        if holds_below_readable_frontier(state, index, hosted_last_serial(state, index)?) {
            return Ok(referrers_response(&[], filter.as_deref()));
        }
        let active = state.revocations.has_active()?;
        if active && digest_decision(state, digest)? == DigestDecision::Revoked {
            return Ok(referrers_response(&[], filter.as_deref()));
        }
        let members = policy_serving_members(state, index, repo);
        if manifest_trashed_in(state, &members, repo, digest)? {
            return Ok(referrers_response(&[], filter.as_deref()));
        }
        let mut sources = Vec::with_capacity(members.len());
        for member in &members {
            let mut descriptors = Vec::new();
            for descriptor in store::list_referrers(&state.meta, &member.name, repo, digest)? {
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&descriptor) {
                    descriptors.push(value);
                }
            }
            if let Some(client) = member.proxy_client() {
                match self.upstream_referrers(state, &member.name, client, repo, digest).await {
                    Ok(upstream) => descriptors.extend(upstream),
                    Err(error) => return Ok(error.into_response()),
                }
            }
            sources.push(descriptors);
        }
        let mut seen = std::collections::HashSet::new();
        let mut manifests = Vec::new();
        for descriptor in sources.into_iter().flatten() {
            add_referrer(state, &members, repo, active, descriptor, &mut seen, &mut manifests)?;
        }
        if let Some(artifact_type) = &filter {
            manifests.retain(|descriptor| descriptor["artifactType"].as_str() == Some(artifact_type));
        }
        if manifest_trashed_in(state, &members, repo, digest)? {
            manifests.clear();
        }
        Ok(referrers_response(&manifests, filter.as_deref()))
    }
}

pub(super) fn stored_tag_names(
    state: &ServingState,
    index: &str,
    repo: &str,
    active: bool,
) -> Result<Vec<String>, ServeError> {
    if !active {
        return Ok(store::list_tags(&state.meta, index, repo)?);
    }
    let mut names = Vec::new();
    for (tag, digest) in store::list_tag_targets(&state.meta, index, repo)? {
        if digest_decision(state, &digest)? == DigestDecision::Clear {
            names.push(tag);
        }
    }
    Ok(names)
}

enum ReferrerLookupError {
    Store(peryx_storage::meta::MetaError),
    Upstream(crate::upstream::UpstreamError),
}

impl From<peryx_storage::meta::MetaError> for ReferrerLookupError {
    fn from(error: peryx_storage::meta::MetaError) -> Self {
        Self::Store(error)
    }
}

impl From<crate::upstream::UpstreamError> for ReferrerLookupError {
    fn from(error: crate::upstream::UpstreamError) -> Self {
        Self::Upstream(error)
    }
}

impl ReferrerLookupError {
    fn into_response(self) -> Response {
        match self {
            Self::Store(error) => ServeError::Store(error).into_response(),
            Self::Upstream(error) => upstream_error_response(&error, "referrers"),
        }
    }
}

#[derive(Clone, Copy)]
enum ReferrerSource {
    Native,
    Fallback,
}

async fn referrer_manifests(
    response: reqwest::Response,
    source: ReferrerSource,
) -> Result<Vec<serde_json::Value>, crate::upstream::UpstreamError> {
    let is_index = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            MediaType::parse(value).is_ok()
                && value
                    .split(';')
                    .next()
                    .is_some_and(|value| value.trim().eq_ignore_ascii_case(OCI_INDEX_TYPE))
        });
    if !is_index {
        return match source {
            ReferrerSource::Native => Err(invalid_referrers("content type is not an OCI image index")),
            ReferrerSource::Fallback => Ok(Vec::new()),
        };
    }
    let bytes = bounded_body(response, MAX_MANIFEST_BYTES)
        .await
        .map_err(|error| match error {
            ServeError::Timeout => crate::upstream::UpstreamError::Timeout,
            error => invalid_referrers(&error.message()),
        })?;
    let document = serde_json::from_slice::<serde_json::Value>(&bytes)
        .map_err(|error| invalid_referrers(&format!("body is not valid JSON: {error}")))?;
    let Some(fields) = document.as_object() else {
        return Err(invalid_referrers("body is not an object"));
    };
    if fields.get("schemaVersion").and_then(serde_json::Value::as_u64) != Some(2) {
        return Err(invalid_referrers("schemaVersion is not 2"));
    }
    if fields.get("mediaType").is_some_and(|value| {
        value
            .as_str()
            .is_none_or(|value| !value.eq_ignore_ascii_case(OCI_INDEX_TYPE))
    }) {
        return Err(invalid_referrers("body mediaType is not an OCI image index"));
    }
    let Some(manifests) = fields.get("manifests").and_then(serde_json::Value::as_array) else {
        return Err(invalid_referrers("manifests is not an array"));
    };
    if !manifests.iter().all(valid_referrer_descriptor) {
        return Err(invalid_referrers("manifests contains an invalid descriptor"));
    }
    Ok(manifests.clone())
}

fn valid_referrer_descriptor(descriptor: &serde_json::Value) -> bool {
    let Some(fields) = descriptor.as_object() else {
        return false;
    };
    fields
        .get("mediaType")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| MediaType::parse(value).is_ok())
        && fields
            .get("digest")
            .and_then(serde_json::Value::as_str)
            .is_some_and(crate::name::valid_content_digest)
        && fields.get("size").and_then(serde_json::Value::as_u64).is_some()
        && fields
            .get("artifactType")
            .is_none_or(|value| value.as_str().is_some_and(|value| MediaType::parse(value).is_ok()))
        && fields.get("annotations").is_none_or(|value| {
            value
                .as_object()
                .is_some_and(|annotations| annotations.values().all(serde_json::Value::is_string))
        })
}

fn invalid_referrers(reason: &str) -> crate::upstream::UpstreamError {
    crate::upstream::UpstreamError::Transport(format!("upstream referrers response is invalid: {reason}"))
}

fn add_referrer(
    state: &ServingState,
    members: &[&Index],
    repo: &str,
    active: bool,
    descriptor: serde_json::Value,
    seen: &mut std::collections::HashSet<String>,
    manifests: &mut Vec<serde_json::Value>,
) -> Result<(), ServeError> {
    let Some(digest) = descriptor["digest"].as_str() else {
        return Ok(());
    };
    if (!active || digest_decision(state, digest)? == DigestDecision::Clear)
        && !manifest_trashed_in(state, members, repo, digest)?
        && seen.insert(digest.to_owned())
    {
        manifests.push(descriptor);
    }
    Ok(())
}

fn referrers_response(manifests: &[serde_json::Value], filter: Option<&str>) -> Response {
    let document = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": manifests,
    });
    let mut response = (
        [(header::CONTENT_TYPE, "application/vnd.oci.image.index.v1+json")],
        document.to_string(),
    )
        .into_response();
    if filter.is_some() {
        response
            .headers_mut()
            .insert("oci-filters-applied", HeaderValue::from_static("artifactType"));
    }
    response
}

/// Apply distribution-spec `n`/`last` pagination to a sorted set: the page after `last`, truncated to
/// `n`, and the `(n, last-of-page)` cursor for a `Link` when more remains.
fn paginate(items: &std::collections::BTreeSet<String>, pagination: &Pagination) -> (Vec<String>, Option<Pagination>) {
    let last = pagination.last.as_deref().unwrap_or_default();
    let limit = pagination.limit;
    // The spec requires `n=0` to return an empty list with no `Link`; without this special case
    // truncate(0) empties the page while `page.len() > 0` still asks for a next cursor, so the marker
    // falls back to `""` and the self-referencing `Link` loops a following client forever.
    if limit == Some(0) {
        return (Vec::new(), None);
    }
    // The set is sorted, so `range` seeks past `last` and yields the tail lazily; only `n`/`n+1`
    // members are ever visited, keeping peak memory proportional to the page rather than the set.
    let mut rest = items.range::<str, _>((std::ops::Bound::Excluded(last), std::ops::Bound::Unbounded));
    let Some(n) = limit else {
        return (rest.cloned().collect(), None);
    };
    let page: Vec<String> = rest.by_ref().take(n).cloned().collect();
    let next = rest
        .next()
        .and_then(|_| page.last())
        .map(|marker| pagination.next(marker.clone()));
    (page, next)
}

fn stale_tag_digest(state: &ServingState, index: &str, repo: &str, tag: &str) -> Result<Option<String>, ServeError> {
    let Some((fetched_at, digest)) = store::tag_freshness(&state.meta, index, repo, tag)? else {
        return Ok(None);
    };
    Ok(within_stale_bound(state, fetched_at).then_some(digest))
}

fn tag_list_response(name: &str, tags: &std::collections::BTreeSet<String>, pagination: &Pagination) -> Response {
    let (page, next) = paginate(tags, pagination);
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(next) = next {
        builder = builder.header(
            header::LINK,
            format!("</v2/{name}/tags/list?{}>; rel=\"next\"", next.query()),
        );
    }
    builder
        .body(Body::from(
            serde_json::json!({ "name": name, "tags": page }).to_string(),
        ))
        .expect("tag list response builds from validated parts")
}

pub(super) fn serve_catalog(state: &ServingState, query: &str) -> Result<Response, ServeError> {
    let pagination = match Pagination::parse(query) {
        Ok(pagination) => pagination,
        Err(message) => return Ok(error_response(ErrorCode::NameInvalid, message)),
    };
    if pagination.limit == Some(0) {
        return Ok(catalog_response(&std::collections::BTreeSet::new(), &pagination));
    }
    let repositories = state.meta.read_driver_txn(|txn| {
        let mut repositories = std::collections::BTreeSet::new();
        for index in &state.indexes {
            if index.ecosystem != crate::ECOSYSTEM {
                continue;
            }
            for repo in store::list_catalog_repositories(txn, &index.name)? {
                if policy_blocks(index, PolicyAction::Serve, &repo) {
                    continue;
                }
                repositories.insert(if index.route.is_empty() {
                    repo
                } else {
                    format!("{}/{repo}", index.route)
                });
            }
        }
        Ok::<_, peryx_storage::meta::MetaError>(repositories)
    })?;
    Ok(catalog_response(&repositories, &pagination))
}

fn catalog_response(repositories: &std::collections::BTreeSet<String>, pagination: &Pagination) -> Response {
    let (page, next) = paginate(repositories, pagination);
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(next) = next {
        builder = builder.header(header::LINK, format!("</v2/_catalog?{}>; rel=\"next\"", next.query()));
    }
    builder
        .body(Body::from(serde_json::json!({ "repositories": page }).to_string()))
        .expect("catalog response builds from validated parts")
}

/// A tag-list page as this registry answers it: the validated upstream body, and a `Link` to the next
/// page rewritten to this registry's client-facing name. The upstream's `Link` names the upstream
/// repository (`/v2/library/nginx/...`, no index route), which a client would resolve back against
/// peryx and 404; only its query carries over. The body's `name` is the upstream repository too and is
/// rewritten by [`serve_proxy_tag_page`] on the client-facing path; the aggregation path carries the
/// validated tags separately.
fn tag_page_response(name: &str, next: Option<&Pagination>, body: Vec<u8>) -> Response {
    let mut response = ([(header::CONTENT_TYPE, "application/json")], body).into_response();
    if let Some(next) = next
        && let Ok(value) = HeaderValue::from_str(&format!("</v2/{name}/tags/list?{}>; rel=\"next\"", next.query()))
    {
        response.headers_mut().insert(header::LINK, value);
    }
    response
}

/// Serialize a validated proxy tag page with its client-facing repository name. Tag order and count
/// carry over, the already-rewritten `Link` stays, and an upstream error passes through untouched.
fn serve_proxy_tag_page(name: &str, page: ProxyTagPage) -> Response {
    let ProxyTagPage { response, tags, .. } = page;
    if !response.status().is_success() {
        return response;
    }
    let (mut parts, _) = response.into_parts();
    parts.headers.remove(header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(tag_page_body(name, &tags)))
}

fn tag_page_body(name: &str, tags: &[String]) -> Vec<u8> {
    serde_json::json!({ "name": name, "tags": tags })
        .to_string()
        .into_bytes()
}

fn validate_tag_page(body: &[u8], expected_name: &str) -> Result<Vec<String>, ServeError> {
    let document: serde_json::Value = serde_json::from_slice(body).map_err(invalid_tag_page)?;
    let Some(fields) = document.as_object() else {
        return Err(invalid_tag_page("tag page is not an object"));
    };
    if fields.get("name").and_then(serde_json::Value::as_str) != Some(expected_name) {
        return Err(invalid_tag_page("tag page names another repository"));
    }
    let Some(tags) = fields
        .get("tags")
        .and_then(serde_json::Value::as_array)
        .and_then(|tags| tags.iter().map(serde_json::Value::as_str).collect::<Option<Vec<_>>>())
    else {
        return Err(invalid_tag_page("tag page has no string tag array"));
    };
    if !tags_are_ordered(&tags) {
        return Err(invalid_tag_page("tag page tags are not ordered"));
    }
    Ok(tags.into_iter().map(str::to_owned).collect())
}

fn tags_are_ordered(tags: &[&str]) -> bool {
    tags.windows(2).all(|pair| pair[0] <= pair[1])
        || tags
            .windows(2)
            .all(|pair| pair[0].to_ascii_lowercase() <= pair[1].to_ascii_lowercase())
}

fn page_within_limit(tag_count: usize, pagination: &Pagination) -> bool {
    pagination.limit.is_none_or(|limit| tag_count <= limit)
}

fn valid_tag_page(tag_count: usize, next: Option<&Pagination>, pagination: &Pagination) -> bool {
    page_within_limit(tag_count, pagination)
        && (!pagination.clamped || tag_count < pagination.limit.unwrap_or_default() || next.is_some())
}

fn invalid_tag_page(error: impl std::fmt::Display) -> ServeError {
    ServeError::Transport(format!("upstream tag list is invalid: {error}"))
}

fn cached_next(link: Option<&str>, pagination: &Pagination) -> Result<Option<Pagination>, &'static str> {
    let next = match link {
        Some(link) if link.starts_with('<') => {
            let value = HeaderValue::from_str(link).map_err(|_| "invalid cached continuation")?;
            let mut headers = reqwest::header::HeaderMap::new();
            headers.append(reqwest::header::LINK, value);
            next_page(&headers, pagination).map_err(|_| "invalid cached continuation")?
        }
        Some(link) => {
            let next = Pagination::parse(link)?;
            if next.limit == Some(0) || next.last.is_none() {
                return Err("invalid cached continuation");
            }
            Some(next_controls(next, pagination))
        }
        None => None,
    };
    Ok(next)
}

fn tag_page_cache_timestamp(key: &str, value: &[u8]) -> Option<i64> {
    let key = key.strip_prefix(TAG_PAGE_CACHE_PREFIX)?;
    let mut fields = key.split('\0');
    let (Some(index), Some(repo), Some(query), None) = (fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return None;
    };
    if index.is_empty() || repo.is_empty() || Pagination::parse(query).ok()?.query() != *query {
        return None;
    }
    let (timestamp, rest) = value.split_first_chunk::<8>()?;
    let (length, rest) = rest.split_first_chunk::<4>()?;
    let length = u32::from_be_bytes(*length) as usize;
    let (link, body) = rest.split_at_checked(length)?;
    let link = (!link.is_empty()).then(|| std::str::from_utf8(link)).transpose().ok()?;
    let pagination = Pagination::parse(query).ok()?;
    let next = cached_next(link, &pagination).ok()?;
    if body.len() > MAX_TAGS_BYTES {
        return None;
    }
    let document: serde_json::Value = serde_json::from_slice(body).ok()?;
    let fields = document.as_object()?;
    fields.get("name")?.as_str()?;
    let tags = fields.get("tags")?.as_array()?;
    let tags = tags.iter().map(serde_json::Value::as_str).collect::<Option<Vec<_>>>()?;
    tags_are_ordered(&tags).then_some(())?;
    valid_tag_page(tags.len(), next.as_ref(), &pagination).then_some(())?;
    Some(i64::from_be_bytes(*timestamp))
}

fn reclaim_tag_page_rows(txn: &mut peryx_storage::meta::DriverTxn, state: &ServingState) -> Result<usize, ServeError> {
    let mut retained = std::collections::BTreeSet::new();
    let mut remove = Vec::new();
    let mut bytes: usize = 0;
    txn.scan_prefix(TAG_PAGE_CACHE_PREFIX, |key, value| {
        if let Some(fetched_at) = tag_page_cache_timestamp(key, value)
            && (state.max_stale_secs == 0 || within_stale_bound(state, fetched_at))
        {
            let row_bytes = key.len().saturating_add(value.len());
            if row_bytes > MAX_TAG_PAGE_CACHE_BYTES {
                remove.push(key.to_owned());
            } else {
                let row = (fetched_at, std::cmp::Reverse(key.to_owned()), row_bytes);
                let mut retain = true;
                while retain
                    && (retained.len() == MAX_TAG_PAGE_CACHE_ROWS
                        || bytes.saturating_add(row_bytes) > MAX_TAG_PAGE_CACHE_BYTES)
                {
                    if remove.len() == TAG_PAGE_DELETE_BATCH {
                        return Ok(std::ops::ControlFlow::Break(()));
                    }
                    if &row <= retained.first().expect("retained tag page exists") {
                        remove.push(key.to_owned());
                        retain = false;
                    } else {
                        let (_, old_key, old_bytes) = retained.pop_first().expect("retained tag page exists");
                        bytes -= old_bytes;
                        remove.push(old_key.0);
                    }
                }
                if retain {
                    retained.insert(row);
                    bytes += row_bytes;
                }
            }
        } else {
            remove.push(key.to_owned());
        }
        Ok::<_, ServeError>(if remove.len() < TAG_PAGE_DELETE_BATCH {
            std::ops::ControlFlow::Continue(())
        } else {
            std::ops::ControlFlow::Break(())
        })
    })?;
    for key in &remove {
        txn.remove_local(key)?;
    }
    Ok(remove.len())
}

pub(super) fn reclaim_tag_pages(state: &ServingState) -> Result<usize, ServeError> {
    state
        .meta
        .commit_driver_cache_txn(|txn| reclaim_tag_page_rows(txn, state))
}

fn store_tag_page(
    state: &ServingState,
    index: &str,
    repo: &str,
    query: &str,
    now: i64,
    link: Option<&str>,
    body: &[u8],
) -> Result<(), ServeError> {
    state.meta.commit_driver_cache_txn(|txn| {
        store::set_tag_page_txn(txn, index, repo, query, now, link, body)?;
        reclaim_tag_page_rows(txn, state)?;
        Ok(())
    })
}

fn next_page(headers: &reqwest::header::HeaderMap, pagination: &Pagination) -> Result<Option<Pagination>, ServeError> {
    let mut next = None;
    for value in headers.get_all(reqwest::header::LINK) {
        let link = value.to_str().map_err(|_| invalid_tag_page("invalid Link header"))?;
        for value in link_values(link)? {
            let value = trim_ows(value);
            if value.is_empty() {
                continue;
            }
            let Some(target) = value.strip_prefix('<').and_then(|value| value.split_once('>')) else {
                return Err(invalid_tag_page("malformed Link header"));
            };
            let (target, parameters) = target;
            let parameters = trim_ows(parameters);
            if !parameters.is_empty() && !parameters.starts_with(';') {
                return Err(invalid_tag_page("malformed Link header"));
            }
            if !link_is_next(parameters)? {
                continue;
            }
            let target = target.split_once('#').map_or(target, |(target, _)| target);
            let Some((_, query)) = target.split_once('?') else {
                return Err(invalid_tag_page("next Link has no query"));
            };
            let parsed = Pagination::parse(query).map_err(invalid_tag_page)?;
            if parsed.limit == Some(0) || parsed.last.is_none() {
                return Err(invalid_tag_page("invalid next Link"));
            }
            let pagination = next_controls(parsed, pagination);
            if next
                .replace(pagination.clone())
                .is_some_and(|existing| existing.query() != pagination.query())
            {
                return Err(invalid_tag_page("conflicting next Link headers"));
            }
        }
    }
    Ok(next)
}

fn next_controls(parsed: Pagination, requested: &Pagination) -> Pagination {
    if parsed.limit.is_none() {
        requested.next(parsed.last.expect("next Link has a last cursor"))
    } else {
        parsed
    }
}

/// Split an RFC 8288 `Link` header into its link-values. A comma separates link-values, but is also a
/// legal unencoded query sub-delimiter (RFC 3986) inside the angle-bracketed target, so a comma within
/// `<…>` belongs to that target rather than ending it. Splitting on every comma would break a cursor
/// that carries one, drop the `next` link-value, and silently truncate the listing.
fn link_values(link: &str) -> Result<Vec<&str>, ServeError> {
    let mut values = Vec::new();
    let mut start = 0;
    let mut in_target = false;
    let mut in_quote = false;
    let mut escaped = false;
    for (index, byte) in link.bytes().enumerate() {
        if escaped {
            escaped = false;
        } else if in_quote && byte == b'\\' {
            escaped = true;
        } else if !in_target && byte == b'"' {
            in_quote = !in_quote;
        } else if !in_quote && byte == b'<' {
            in_target = true;
        } else if in_target && byte == b'>' {
            in_target = false;
        } else if !in_target && !in_quote && byte == b',' {
            values.push(&link[start..index]);
            start = index + 1;
        }
    }
    if in_target || in_quote || escaped {
        return Err(invalid_tag_page("malformed Link header"));
    }
    values.push(&link[start..]);
    Ok(values)
}

fn link_is_next(parameters: &str) -> Result<bool, ServeError> {
    let mut relation = None;
    for parameter in link_parameters(parameters)? {
        let parameter = trim_ows(parameter);
        if parameter.is_empty() {
            continue;
        }
        let Some((name, value)) = parameter.split_once('=') else {
            continue;
        };
        match trim_ows(name) {
            name if name.eq_ignore_ascii_case("anchor") => return Ok(false),
            name if name.eq_ignore_ascii_case("rel") && relation.is_none() => {
                let value = trim_ows(value);
                relation = Some(if value.starts_with('"') {
                    quoted_link_value(value)?
                } else {
                    value.to_owned()
                });
            }
            _ => {}
        }
    }
    Ok(relation.is_some_and(|relation| {
        relation
            .split_ascii_whitespace()
            .any(|relation| relation.eq_ignore_ascii_case("next"))
    }))
}

fn trim_ows(value: &str) -> &str {
    value.trim_matches([' ', '\t'])
}

fn link_parameters(parameters: &str) -> Result<Vec<&str>, ServeError> {
    let mut values = Vec::new();
    let mut start = 0;
    let mut in_quote = false;
    let mut escaped = false;
    for (index, byte) in parameters.bytes().enumerate() {
        if escaped {
            escaped = false;
        } else if in_quote && byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            in_quote = !in_quote;
        } else if !in_quote && byte == b';' {
            values.push(&parameters[start..index]);
            start = index + 1;
        }
    }
    if in_quote || escaped {
        return Err(invalid_tag_page("malformed Link parameter"));
    }
    values.push(&parameters[start..]);
    Ok(values)
}

fn quoted_link_value(value: &str) -> Result<String, ServeError> {
    if value.len() < 2 || !value.ends_with('"') {
        return Err(invalid_tag_page("malformed Link parameter"));
    }
    let mut decoded = String::new();
    let mut escaped = false;
    for character in value[1..value.len() - 1].chars() {
        if escaped {
            decoded.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '"' {
            return Err(invalid_tag_page("malformed Link parameter"));
        } else {
            decoded.push(character);
        }
    }
    Ok(decoded)
}

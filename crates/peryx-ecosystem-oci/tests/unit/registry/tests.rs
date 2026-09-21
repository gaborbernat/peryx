use super::*;

#[test]
fn test_serve_error_maps_every_fault_to_a_gateway_error() {
    let decode = serde_json::from_str::<u8>("nope").unwrap_err();
    assert_eq!(
        ServeError::from(MetaError::Decode(decode)).into_response().status(),
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        ServeError::from(std::io::Error::other("disk")).into_response().status(),
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        ServeError::Transport("reset".to_owned()).into_response().status(),
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        ServeError::Timeout.into_response().status(),
        StatusCode::GATEWAY_TIMEOUT
    );
    assert_eq!(
        ServeError::Fenced.into_response().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[test]
fn test_manifest_write_store_error_remains_a_store_error() {
    let decode = serde_json::from_str::<u8>("nope").unwrap_err();
    assert!(matches!(
        ServeError::from(ManifestWriteError::from(MetaError::Decode(decode))),
        ServeError::Store(MetaError::Decode(_))
    ));
}

#[test]
fn test_serve_error_message_describes_every_fault() {
    let decode = serde_json::from_str::<u8>("nope").unwrap_err();
    assert!(
        ServeError::from(MetaError::Decode(decode))
            .message()
            .contains("metadata store error")
    );
    assert!(
        ServeError::Io(std::io::Error::other("disk"))
            .message()
            .contains("blob io error")
    );
    assert!(
        ServeError::Transport("reset".to_owned())
            .message()
            .contains("upstream transfer failed")
    );
    assert_eq!(ServeError::Timeout.message(), crate::error::TIMEOUT_MESSAGE);
    assert_eq!(ServeError::Fenced.message(), "repository authority moved");
}

#[tokio::test]
async fn test_serve_error_wraps_a_transport_failure() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let err = reqwest::Client::new()
        .get("http://127.0.0.1:1/")
        .send()
        .await
        .unwrap_err();
    assert_eq!(ServeError::from(err).into_response().status(), StatusCode::BAD_GATEWAY);
}

#[test]
fn test_classify_route_buckets_blob_pulls_as_artifacts() {
    use peryx_driver::rate_limit::RouteClass;
    use peryx_driver::serving::AbsoluteProtocolDriver as _;
    let registry = OciRegistry::default();
    let digest = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    assert_eq!(
        registry.classify_route(&format!("/v2/store/app/blobs/{digest}")),
        RouteClass::Artifact
    );
    assert_eq!(
        registry.classify_route("/v2/store/app/manifests/1.0"),
        RouteClass::Listing
    );
    assert_eq!(registry.classify_route("/v2/store/app/tags/list"), RouteClass::Listing);
    assert_eq!(
        registry.classify_route(&format!("/v2/store/app/blobs/{digest}/contents")),
        RouteClass::Listing
    );
    assert_eq!(registry.classify_route("/v2/token"), RouteClass::Authentication);
    assert_eq!(registry.classify_route("/v2/token/"), RouteClass::Authentication);
}

#[test]
fn test_serve_error_converts_to_its_message_string() {
    assert_eq!(
        String::from(ServeError::Io(std::io::Error::other("disk"))),
        "blob io error: disk"
    );
    assert_eq!(
        String::from(ServeError::Transport("reset".to_owned())),
        "upstream transfer failed: reset"
    );
    assert_eq!(String::from(ServeError::Timeout), crate::error::TIMEOUT_MESSAGE);
}

#[tokio::test]
async fn test_read_body_returns_bytes_within_the_cap_and_rejects_an_over_cap_body() {
    assert_eq!(
        read_body(Body::from(b"hello".to_vec()), 1 << 20).await.unwrap(),
        "hello"
    );
    assert!(read_body(Body::from(vec![0u8; 2 << 20]), 1 << 20).await.is_err());
}

fn resolvable_index(name: &str, route: &str) -> Index {
    Index {
        name: name.to_owned(),
        route: route.to_owned(),
        ecosystem: crate::ECOSYSTEM,
        kind: IndexKind::Hosted { volatile: false },
        policy: peryx_policy::Policy::default(),
        acl: peryx_identity::IndexAcl::default(),
    }
}

#[test]
fn test_resolve_rejects_a_route_prefix_with_no_repository_name() {
    let index = resolvable_index("store", "store");
    assert!(resolve(std::slice::from_ref(&index), "store/").is_none());
}

/// Two indexes whose route matches equally well must resolve to the earlier one: the longest-prefix
/// rule only ever prefers a strictly longer route, never one merely as long.
#[test]
fn test_resolve_prefers_the_first_index_among_equal_length_routes() {
    let indexes = [resolvable_index("first", "store"), resolvable_index("second", "store")];
    let (resolved, repo) = resolve(&indexes, "store/app").unwrap();
    assert_eq!(resolved.name, "first");
    assert_eq!(repo, "app");
}

#[test]
fn test_session_gate_release_keeps_a_lock_a_holder_still_references() {
    let gate = SessionGate::default();
    let held = gate.lock("s");
    gate.release("s");
    let after = gate.lock("s");
    assert!(
        Arc::ptr_eq(&held, &after),
        "an in-flight session's lock must survive release"
    );
}

#[test]
fn test_session_gate_release_drops_a_lock_nobody_holds() {
    let gate = SessionGate::default();
    drop(gate.lock("s"));
    gate.release("s");
    assert!(
        !gate.locks.lock().unwrap().contains_key("s"),
        "a lock nobody holds must not linger"
    );
}

#[test]
fn test_blob_membership_cache_remove_forgets_the_key_and_its_weight() {
    let mut cache: BlobMembershipCache<RandomState> = BlobMembershipCache::default();
    cache.insert("a".to_owned());
    cache.insert("bb".to_owned());
    cache.remove("a");
    assert!(!cache.contains("a"));
    assert!(cache.contains("bb"));
    assert_eq!(cache.key_bytes, 2);
}

/// A key `remove` drops must leave the insertion order naming only what is still there - not the key
/// that just left it - so the next eviction reaches the entry that is actually oldest.
#[test]
fn test_blob_membership_cache_remove_leaves_the_survivor_evictable() {
    let mut cache: BlobMembershipCache<RandomState> = BlobMembershipCache::default();
    cache.insert("a".to_owned());
    cache.insert("bb".to_owned());
    cache.remove("a");
    cache.key_bytes = BLOB_MEMBERSHIP_CACHE_BYTES - 1;
    cache.insert("cc".to_owned());
    assert!(!cache.contains("bb"), "the oldest surviving entry must be evicted");
    assert!(cache.contains("cc"));
}

#[test]
fn test_blob_membership_cache_insert_keeps_a_cache_exactly_at_the_weight_cap() {
    let mut cache: BlobMembershipCache<RandomState> = BlobMembershipCache::default();
    cache.entries.insert(Arc::from("old12"));
    cache.insertion_order.push_back(Arc::from("old12"));
    cache.key_bytes = BLOB_MEMBERSHIP_CACHE_BYTES - 1;

    cache.insert("x".to_owned());

    assert!(cache.contains("old12"), "landing exactly on the cap must not evict");
    assert!(cache.contains("x"));
    assert_eq!(cache.key_bytes, BLOB_MEMBERSHIP_CACHE_BYTES);
}

#[test]
fn test_blob_membership_cache_insert_subtracts_the_evicted_keys_own_weight() {
    let mut cache: BlobMembershipCache<RandomState> = BlobMembershipCache::default();
    cache.entries.insert(Arc::from("old12"));
    cache.insertion_order.push_back(Arc::from("old12"));
    cache.key_bytes = BLOB_MEMBERSHIP_CACHE_BYTES;

    cache.insert("abc".to_owned());

    assert!(!cache.contains("old12"));
    assert!(cache.contains("abc"));
    assert_eq!(cache.key_bytes, BLOB_MEMBERSHIP_CACHE_BYTES - 2);
}

#[tokio::test]
async fn test_layer_error_message_reports_an_unreadable_error_body() {
    let response = (StatusCode::BAD_GATEWAY, Body::from(vec![0u8; 2 << 20])).into_response();
    let message = layer_error_message("store/app", "sha256:x", response).await;
    assert!(message.contains("502"), "{message}");
}

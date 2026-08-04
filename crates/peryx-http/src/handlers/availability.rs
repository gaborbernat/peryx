//! `GET /+availability/topology`: one immutable, role-filtered picture of the availability group, and
//! `GET /+availability/topology/stream`: the same picture pushed as bounded Server-Sent Events.
//!
//! An operator page renders this instead of traversing live membership and storage state on every poll.
//! The roster identities, datacenters, and roles stay public; the live frontier and per-node liveness
//! need `operator:read`; the advertised peer addresses need `administration:read`. A caller reads only
//! the fields at or below its class, the response is never cached, and the snapshot carries its own
//! observation time so a stale render shows as age rather than passing for health.
//!
//! The stream reuses the same projection: it emits the current snapshot on connect, then one event only
//! when the meaningful state changes, so its traffic tracks the change rate rather than the object count
//! or the connection count. It never widens what the one-shot endpoint would reveal.

use std::collections::hash_map::DefaultHasher;
use std::convert::Infallible;
use std::hash::{Hash as _, Hasher as _};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse as _, Response};
use futures_util::stream::{self, Stream};
use peryx_core::{LocalStatus, NodeLiveness, TopologySnapshot, TopologyView};
use peryx_driver::state::AppState;

use super::status::status_authorization;
use crate::response_security::{FieldClassification, ProtectedCachePolicy, ResponseAuthorization};

/// How often a stream connection re-reads the live snapshot. It bounds the peer-liveness and frontier
/// resolution and matches the dashboard's own refresh cadence, so a stream costs no more sampling than
/// the poll it replaces.
const TOPOLOGY_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// How often an idle stream sends a keep-alive comment. It holds the connection open through proxies and
/// lets a client tell a live-but-quiet group from a dropped connection.
const TOPOLOGY_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// `GET /+availability/topology`: the availability topology snapshot, filtered to the caller's class.
pub async fn availability_topology(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let view = topology_view(status_authorization(&state, &headers).await);
    let local = local_status(&state).await;
    let snapshot = state.availability_topology().snapshot(view, local, (state.clock)());
    let mut response = axum::Json(snapshot).into_response();
    ProtectedCachePolicy::NoStore.apply(response.headers_mut());
    response
}

/// `GET /+availability/topology/stream`: the caller's snapshot as a bounded Server-Sent Events stream.
///
/// The first event carries the current snapshot; a later event fires only when the meaningful state
/// changes, so `captured_at` advancing on its own never emits one and an idle group carries only
/// keep-alive comments. A slow reader coalesces to the latest snapshot rather than a backlog, because
/// each tick re-reads the live state and the connection holds no per-event queue.
pub async fn availability_topology_stream(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let view = topology_view(status_authorization(&state, &headers).await);
    let sse = Sse::new(topology_events(state, view))
        .keep_alive(KeepAlive::new().interval(TOPOLOGY_HEARTBEAT_INTERVAL).text("heartbeat"));
    let mut response = sse.into_response();
    ProtectedCachePolicy::NoStore.apply(response.headers_mut());
    response
}

/// The per-connection event stream. The first interval tick fires immediately, so a client sees the
/// current snapshot on connect; each later tick re-reads the snapshot and yields an event only when the
/// state moved. Between changes the future stays pending, so the keep-alive fills the silence.
fn topology_events(state: Arc<AppState>, view: TopologyView) -> impl Stream<Item = Result<Event, Infallible>> {
    let interval = tokio::time::interval(TOPOLOGY_SAMPLE_INTERVAL);
    stream::unfold(
        (state, view, interval, None::<u64>, 0_u64),
        |(state, view, mut interval, mut last, mut sequence)| async move {
            // Break out of the sample loop with the event to emit, so the yield past the loop is reachable
            // rather than a dead tail an early `return` would leave uncovered.
            let event = loop {
                interval.tick().await;
                let local = local_status(&state).await;
                let snapshot = state.availability_topology().snapshot(view, local, (state.clock)());
                let version = state_version(&snapshot);
                if last != Some(version) {
                    last = Some(version);
                    sequence += 1;
                    // Serialize with the infallible `data`: a snapshot is a plain serde value that encodes
                    // to one line, so there is no error branch to leave uncovered and no newline to split.
                    break Event::default()
                        .id(sequence.to_string())
                        .event("topology")
                        .data(serde_json::to_string(&snapshot).unwrap_or_default());
                }
            };
            Some((Ok(event), (state, view, interval, last, sequence)))
        },
    )
}

/// A fingerprint of a snapshot's meaningful state, with `captured_at` zeroed. Two snapshots that differ
/// only in observation time share a version, so re-sampling an unchanged group emits nothing.
fn state_version(snapshot: &TopologySnapshot) -> u64 {
    let mut probe = snapshot.clone();
    probe.captured_at = 0;
    let mut hasher = DefaultHasher::new();
    serde_json::to_vec(&probe).unwrap_or_default().hash(&mut hasher);
    hasher.finish()
}

/// The topology view a caller reads at. A repository token administers no server surface, so it reads
/// the same public roster as an anonymous caller; only an operator or administrator role widens it.
const fn topology_view(authorization: ResponseAuthorization) -> TopologyView {
    match authorization.field_class() {
        Some(FieldClassification::Administrator) => TopologyView::Administrator,
        Some(FieldClassification::Operator) => TopologyView::Operator,
        // A repository token, a public caller, and a denied decision all read the public roster.
        _ => TopologyView::Public,
    }
}

/// This node's own live self-observation: its role, whether its local stores can serve, and the metadata
/// frontier it has committed. The role is the configured authority role, so a read-only primary reports
/// itself as the writer rather than reading its read-only posture as a replica.
async fn local_status(state: &AppState) -> LocalStatus {
    let serial = state.meta.current_serial();
    let serving = serial.is_ok() && state.blobs.health().await.is_ok();
    LocalStatus {
        role: state.availability_role(),
        liveness: if serving {
            NodeLiveness::Live
        } else {
            NodeLiveness::Unready
        },
        frontier: serial.unwrap_or(0),
    }
}

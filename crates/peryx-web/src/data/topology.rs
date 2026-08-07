#![allow(
    clippy::future_not_send,
    reason = "browser fetch futures are single-threaded by nature; callers wrap them in SendWrapper"
)]

use crate::model::TopologySnapshot;

/// The endpoint the browser subscribes to for live snapshot deltas. It carries the same role-filtered
/// projection as the one-shot snapshot, so a subscription never widens what the page already renders.
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
const TOPOLOGY_STREAM_URL: &str = "/+availability/topology/stream";

/// The named Server-Sent Event the stream emits, matching the server's `Event::event("topology")`. A
/// browser dispatches a named event to a matching listener, not to the default `message` handler.
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
const TOPOLOGY_STREAM_EVENT: &str = "topology";

/// The availability topology snapshot, projected to the caller's class.
///
/// The server reads and projects `AppState`; the hydrated browser fetches `/+availability/topology`,
/// which projects the same fields. Both sides yield the identical `TopologySnapshot`.
///
/// # Errors
///
/// Returns a message when the snapshot cannot be reached or does not parse.
pub async fn load_topology() -> Result<TopologySnapshot, String> {
    #[cfg(feature = "ssr")]
    {
        Ok(crate::ssr::topology().await)
    }
    #[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
    {
        send_wrapper::SendWrapper::new(async {
            let value = super::fetch_json_required("/+availability/topology")
                .await
                .map_err(|_| "The availability topology could not be reached.".to_owned())?;
            serde_json::from_value(value).map_err(|_| "The availability topology returned invalid data.".to_owned())
        })
        .await
    }
    #[cfg(all(not(feature = "ssr"), not(feature = "hydrate")))]
    {
        Err("The availability topology is unavailable.".to_owned())
    }
}

/// Deserialize one streamed snapshot event body, so the browser hands the page the same
/// [`TopologySnapshot`] the one-shot loader would.
///
/// # Errors
///
/// Returns a message when the event body does not parse as a snapshot.
#[cfg(any(test, all(not(feature = "ssr"), feature = "hydrate")))]
pub fn parse_topology_snapshot(data: &str) -> Result<TopologySnapshot, String> {
    serde_json::from_str(data).map_err(|_| "The availability topology stream sent invalid data.".to_owned())
}

/// A live subscription to the availability topology stream. Dropping it closes the underlying
/// `EventSource`, so a page that navigates away stops the browser reconnecting in the background.
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
pub struct TopologyStream {
    source: web_sys::EventSource,
    _on_snapshot: wasm_bindgen::closure::Closure<dyn FnMut(web_sys::MessageEvent)>,
    _on_open: wasm_bindgen::closure::Closure<dyn FnMut()>,
    _on_error: wasm_bindgen::closure::Closure<dyn FnMut()>,
}

#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
impl Drop for TopologyStream {
    fn drop(&mut self) {
        self.source.close();
    }
}

/// Open the bounded topology stream and drive two callbacks: `on_snapshot` for each delta the server
/// coalesces onto the wire, and `on_status` as the connection opens, drops, and reconnects. Returns
/// `None` when the browser cannot open an `EventSource`, so the caller keeps the initial snapshot rather
/// than clearing the page.
///
/// `on_status` starts the badge live only once the connection opens or a valid event arrives, never on the
/// strength of a pending connection. A body that will not decode reports `Stale`; the browser reconnects on
/// its own after a drop, reporting `Connecting` while it retries and `Offline` once it gives up, so a frozen
/// render is always visible as such.
#[cfg(all(not(feature = "ssr"), feature = "hydrate"))]
#[must_use]
pub fn subscribe_topology(
    on_snapshot: impl Fn(TopologySnapshot) + 'static,
    on_status: impl Fn(crate::model::StreamStatus) + 'static,
) -> Option<TopologyStream> {
    use wasm_bindgen::JsCast as _;
    use wasm_bindgen::closure::Closure;

    use crate::model::StreamStatus;

    let source = web_sys::EventSource::new(TOPOLOGY_STREAM_URL).ok()?;
    let on_status: std::rc::Rc<dyn Fn(StreamStatus)> = std::rc::Rc::new(on_status);

    let snapshot_status = std::rc::Rc::clone(&on_status);
    let on_snapshot = Closure::<dyn FnMut(web_sys::MessageEvent)>::new(move |event: web_sys::MessageEvent| {
        let Some(data) = event.data().as_string() else {
            return;
        };
        match parse_topology_snapshot(&data) {
            // A valid event proves the stream is delivering, so the badge turns live even if `onopen` has
            // not fired yet; a body that will not decode marks the render stale rather than dropping it
            // silently, so a protocol error can never freeze under a live badge.
            Ok(snapshot) => {
                snapshot_status(StreamStatus::Live);
                on_snapshot(snapshot);
            }
            Err(_) => snapshot_status(StreamStatus::Stale),
        }
    });
    source
        .add_event_listener_with_callback(TOPOLOGY_STREAM_EVENT, on_snapshot.as_ref().unchecked_ref())
        .ok()?;

    let opened = std::rc::Rc::clone(&on_status);
    let on_open = Closure::<dyn FnMut()>::new(move || opened(StreamStatus::Live));
    source.set_onopen(Some(on_open.as_ref().unchecked_ref()));

    let errored_source = source.clone();
    let error_status = std::rc::Rc::clone(&on_status);
    let on_error = Closure::<dyn FnMut()>::new(move || {
        // `CLOSED` means the browser stopped retrying, so the feed is frozen; any other state is a
        // transient drop it is already reconnecting through.
        error_status(if errored_source.ready_state() == web_sys::EventSource::CLOSED {
            StreamStatus::Offline
        } else {
            StreamStatus::Connecting
        });
    });
    source.set_onerror(Some(on_error.as_ref().unchecked_ref()));

    Some(TopologyStream {
        source,
        _on_snapshot: on_snapshot,
        _on_open: on_open,
        _on_error: on_error,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_topology_snapshot;

    #[test]
    fn test_parse_topology_snapshot_reads_a_streamed_event() {
        let snapshot = parse_topology_snapshot(
            r#"{"mode":"dc","group":"east","captured_at":7,"node_count":1,"local":{"role":"writer","liveness":"live","frontier":42},"nodes":[]}"#,
        )
        .unwrap();
        assert_eq!(snapshot.captured_at, 7);
        assert_eq!(snapshot.local.frontier, Some(42));
    }

    #[test]
    fn test_parse_topology_snapshot_rejects_invalid_data() {
        assert!(parse_topology_snapshot("not a snapshot").is_err());
    }
}

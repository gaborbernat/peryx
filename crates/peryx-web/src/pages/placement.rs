#![allow(
    clippy::must_use_candidate,
    reason = "the #[component] macro consumes attributes, so #[must_use] cannot reach the generated functions"
)]

use leptos::prelude::*;

use crate::data::load_placements;
use crate::model::{
    PlacementHealth, PlacementRow, PlacementView, byte_availability_label, file_source_label, format_instant,
};

#[component]
pub fn ArtifactPlacements() -> impl IntoView {
    // `None` is the first page; a cursor pages the administrator's rows in digest order. The resource
    // re-reads whenever the cursor moves, so a click fetches the next page without a full navigation.
    let (cursor, set_cursor) = signal(None::<String>);
    let view = Resource::new(move || cursor.get(), load_placements);
    view! {
        <section class="page placements-page">
            <div class="ops-title">
                <h1>"Artifact placement health"</h1>
                <span class="badge">"read-only"</span>
                <a href="/+availability/placements"><code>"/+availability/placements"</code></a>
            </div>
            <p class="dim">
                "How the store's bytes are placed: how many artifacts serve locally, how many depend on an \
                 upstream, and how many cannot be served at all. The counts cover the whole store; the \
                 per-digest rows need administrator access and page in digest order. A digest names an \
                 artifact without revealing where it lives or who owns it."
            </p>
            <Suspense fallback=|| view! { <p class="dim" role="status" aria-live="polite">"loading"</p> }>
                {move || Suspend::new(async move {
                    match view.await {
                        Ok(view) => view! { <PlacementBody view set_cursor /> }.into_any(),
                        Err(error) => view! { <p class="error" role="alert">{error}</p> }.into_any(),
                    }
                })}
            </Suspense>
        </section>
    }
}

#[component]
fn PlacementBody(view: PlacementView, set_cursor: WriteSignal<Option<String>>) -> impl IntoView {
    let captured = format_instant(view.captured_at);
    let health = view.health;
    let rows = view.rows;
    let next_cursor = view.next_cursor;
    view! {
        <HealthSummary health captured />
        {rows.map_or_else(
            || view! {
                <p class="dim" role="note">
                    "Per-digest placement rows need administrator access. The counts above cover the whole store."
                </p>
            }.into_any(),
            |rows| view! { <PlacementRows rows next_cursor set_cursor /> }.into_any(),
        )}
    }
}

#[component]
fn HealthSummary(health: PlacementHealth, captured: String) -> impl IntoView {
    let local = byte_availability_label(peryx_core::UiByteAvailability::Local);
    let remote = byte_availability_label(peryx_core::UiByteAvailability::RemoteOnly);
    let unavailable = byte_availability_label(peryx_core::UiByteAvailability::Unavailable);
    view! {
        <div class="stat-row placement-summary">
            <div class="stat">
                <strong>{health.local}</strong>
                <span class="badge avail-local" title=local.hint>{local.text}</span>
            </div>
            <div class="stat">
                <strong>{health.remote_only}</strong>
                <span class="badge avail-remote-only" title=remote.hint>{remote.text}</span>
            </div>
            <div class="stat">
                <strong>{health.unavailable}</strong>
                <span class="badge avail-unavailable" title=unavailable.hint>{unavailable.text}</span>
            </div>
            <div class="stat"><strong>{health.total}</strong><span>"total artifacts"</span></div>
            <div class="stat"><strong>{captured}</strong><span>"observed at (UTC)"</span></div>
        </div>
    }
}

#[component]
fn PlacementRows(
    rows: Vec<PlacementRow>,
    next_cursor: Option<String>,
    set_cursor: WriteSignal<Option<String>>,
) -> impl IntoView {
    if rows.is_empty() {
        return view! {
            <p class="dim" role="status">"No artifact placements are recorded yet."</p>
        }
        .into_any();
    }
    let count = rows.len();
    let table_rows = rows.into_iter().map(placement_row).collect_view();
    view! {
        <div class="table-scroll">
            <table class="files ops-table placement-table">
                <caption>"Recorded artifact placements, one row per digest, in digest order."</caption>
                <thead>
                    <tr>
                        <th scope="col">"Digest"</th>
                        <th scope="col">"Source"</th>
                        <th scope="col">"Byte availability"</th>
                    </tr>
                </thead>
                <tbody>{table_rows}</tbody>
            </table>
        </div>
        <div class="pager placement-pager">
            <p class="result-count" role="status" aria-live="polite">
                {format!("Showing {count} placement rows on this page.")}
            </p>
            <button type="button" on:click=move |_| set_cursor.set(None)>"First page"</button>
            {next_cursor.map(|cursor| view! {
                <button type="button" on:click=move |_| set_cursor.set(Some(cursor.clone()))>"Next page"</button>
            })}
        </div>
    }
    .into_any()
}

fn placement_row(row: PlacementRow) -> AnyView {
    let source = file_source_label(row.source);
    let availability = byte_availability_label(row.availability);
    view! {
        <tr>
            <td><code>{row.digest}</code></td>
            <td><span class=format!("badge placement-source src-{}", source.key) title=source.hint>{source.text}</span></td>
            <td>
                <span class=format!("badge placement-avail avail-{}", availability.key) title=availability.hint>
                    {availability.text}
                </span>
            </td>
        </tr>
    }
    .into_any()
}

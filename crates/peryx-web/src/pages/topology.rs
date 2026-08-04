#![allow(
    clippy::must_use_candidate,
    reason = "the #[component] macro consumes attributes, so #[must_use] cannot reach the generated functions"
)]

use leptos::prelude::*;

use crate::data::load_topology;
use crate::model::{
    HealthLabel, LocalNode, RoleFilter, TopologyNode, TopologySnapshot, format_instant, liveness_health, mode_label,
    role_label,
};

#[component]
pub fn AvailabilityTopology() -> impl IntoView {
    let topology = Resource::new(|| (), |()| load_topology());
    view! {
        <section class="page topology-page">
            <Suspense fallback=|| view! { <p class="dim">"loading"</p> }>
                {move || Suspend::new(async move {
                    match topology.await {
                        Ok(snapshot) => view! { <TopologyBody snapshot /> }.into_any(),
                        Err(error) => view! { <p class="error" role="alert">{error}</p> }.into_any(),
                    }
                })}
            </Suspense>
        </section>
    }
}

#[component]
fn TopologyBody(snapshot: TopologySnapshot) -> impl IntoView {
    let (filter, set_filter) = signal(RoleFilter::All);
    let mode = mode_label(snapshot.mode).to_owned();
    let group = snapshot.group.clone();
    let captured = format_instant(snapshot.captured_at);
    let node_count = snapshot.node_count;
    let shown = snapshot.nodes.len();
    let capped = node_count > shown;
    let show_address = snapshot.nodes.iter().any(|node| node.address.is_some());
    let local = snapshot.local.clone();
    let nodes = snapshot.nodes;
    let rows_nodes = nodes.clone();
    let rows = move || {
        let current = filter.get();
        rows_nodes
            .iter()
            .filter(|node| current.matches(node.role))
            .map(|node| node_row(node, show_address))
            .collect_view()
    };
    let visible = move || nodes.iter().filter(|node| filter.get().matches(node.role)).count();
    view! {
        <div class="ops-title">
            <h1>"Availability topology"</h1>
            <span class="badge">"read-only"</span>
            <a href="/+availability/topology"><code>"/+availability/topology"</code></a>
        </div>
        <p class="dim">
            "A single role-filtered picture of the availability group, taken at one instant. Peer liveness \
             stays "<em>"unknown"</em>" until a consensus layer observes it, so stale peer data never reads \
             as healthy. Fields above your access are withheld rather than shown."
        </p>
        <div class="stat-row topology-summary">
            <div class="stat"><strong>{mode}</strong><span>"mode"</span></div>
            <div class="stat"><strong>{group.unwrap_or_else(|| "—".to_owned())}</strong><span>"group"</span></div>
            <div class="stat"><strong>{node_count}</strong><span>"roster nodes"</span></div>
            <div class="stat"><strong>{captured.clone()}</strong><span>"snapshot taken (UTC)"</span></div>
        </div>
        {capped.then(|| view! {
            <p class="usage-retention" role="note">
                {format!("Showing {shown} of {node_count} nodes. The roster is capped per snapshot; the count above is the full size.")}
            </p>
        })}
        <h2>"This node"</h2>
        <LocalNodePanel local captured />
        <h2>"Roster"</h2>
        {if node_count == 0 {
            view! {
                <p class="dim" role="status">
                    "This node runs standalone. No availability roster is configured, so there are no peers to show."
                </p>
            }.into_any()
        } else {
            view! {
                <form class="policy-filters topology-filters" on:submit=|event| event.prevent_default()>
                    <label for="topology-role">"Role"</label>
                    <select
                        id="topology-role"
                        on:change:target=move |event| set_filter.set(RoleFilter::from_value(&event.target().value()))
                    >
                        <option value="all">"All roles"</option>
                        <option value="writer">"Writer"</option>
                        <option value="replica">"Replica"</option>
                    </select>
                </form>
                <p class="result-count" role="status" aria-live="polite">
                    {move || format!("Showing {} of {node_count} roster nodes.", visible())}
                </p>
                <div class="table-scroll">
                    <table class="files ops-table topology-table">
                        <caption>"Configured availability roster, one row per node."</caption>
                        <thead>
                            <tr>
                                <th scope="col">"Node"</th>
                                <th scope="col">"Datacenter"</th>
                                <th scope="col">"Role"</th>
                                <th scope="col">"Health"</th>
                                <th scope="col" class="num">"Frontier"</th>
                                {show_address.then(|| view! { <th scope="col">"Address"</th> })}
                            </tr>
                        </thead>
                        <tbody>{rows}</tbody>
                    </table>
                </div>
            }.into_any()
        }}
    }
}

#[component]
fn LocalNodePanel(local: LocalNode, captured: String) -> impl IntoView {
    let health = liveness_health(local.liveness);
    view! {
        <div class="stat-row topology-local">
            <div class="stat"><strong>{role_label(local.role)}</strong><span>"role"</span></div>
            <div class="stat">
                <strong><HealthBadge health /></strong>
                <span>"liveness"</span>
            </div>
            <div class="stat">
                <strong>{local.frontier.map_or_else(|| "restricted".to_owned(), |value| value.to_string())}</strong>
                <span>"committed frontier"</span>
            </div>
            <div class="stat"><strong>{captured}</strong><span>"observed at (UTC)"</span></div>
        </div>
    }
}

fn node_row(node: &TopologyNode, show_address: bool) -> AnyView {
    let health = liveness_health(node.liveness);
    let node_cell = if node.local {
        view! { <td><code>{node.node.clone()}</code>" "<span class="badge topology-self">"this node"</span></td> }
            .into_any()
    } else {
        view! { <td><code>{node.node.clone()}</code></td> }.into_any()
    };
    view! {
        <tr>
            {node_cell}
            <td>{node.dc.clone()}</td>
            <td><span class=format!("badge role-{}", role_label(node.role).to_lowercase())>{role_label(node.role)}</span></td>
            <td><HealthBadge health /></td>
            <td class="num">{node.frontier.map_or_else(|| "—".to_owned(), |value| value.to_string())}</td>
            {show_address.then(|| view! {
                <td>{node.address.clone().unwrap_or_else(|| "—".to_owned())}</td>
            })}
        </tr>
    }
    .into_any()
}

#[component]
fn HealthBadge(health: HealthLabel) -> impl IntoView {
    view! { <span class=format!("badge {}", health.class)>{health.text}</span> }
}

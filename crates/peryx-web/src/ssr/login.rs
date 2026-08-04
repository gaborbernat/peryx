use std::sync::Arc;

use axum::http::HeaderMap;
use leptos::prelude::*;
use peryx_driver::AppState;

use crate::model::UiLoginState;

/// The login page's state, read straight from `AppState`: the configured OIDC providers and the user
/// the request's session cookie identifies.
#[must_use]
pub async fn login_state() -> UiLoginState {
    let app = expect_context::<Arc<AppState>>();
    let headers = leptos_axum::extract::<HeaderMap>().await.unwrap_or_default();
    let user = peryx_http::handlers::session_user(&app, &headers).map(|user| user.name.display().to_owned());
    let providers = app.oidc_providers().into_iter().map(str::to_owned).collect();
    UiLoginState { user, providers }
}

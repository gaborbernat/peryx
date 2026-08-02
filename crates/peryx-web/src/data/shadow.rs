use base64::Engine as _;

use crate::model::UiShadowPage;

/// Fetch one project's shadowed candidates with a repository token or local login. The credentials
/// travel only in the authorization header, never in the URL, so they stay out of navigation history.
pub async fn load_shadow_candidates(url: &str, user: &str, password: &str) -> Result<UiShadowPage, String> {
    let credentials = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    let response = gloo_net::http::Request::get(url)
        .header("accept", "application/json")
        .header("authorization", &format!("Basic {credentials}"))
        .send()
        .await
        .map_err(|_| "The shadow inspection service could not be reached.".to_owned())?;
    match response.status() {
        200 => response
            .json()
            .await
            .map_err(|_| "The shadow inspection service returned invalid data.".to_owned()),
        400 => Err("The shadow inspection request was rejected as invalid.".to_owned()),
        401 => Err("The username or password was not accepted.".to_owned()),
        403 => Err("This repository token cannot inspect shadowed candidates.".to_owned()),
        404 => Err("The repository was not found or is not available to this user.".to_owned()),
        _ => Err("The shadow inspection service is unavailable.".to_owned()),
    }
}

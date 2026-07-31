use base64::Engine as _;

use crate::model::UiPolicyDecisionPage;

pub async fn load_policy_decisions(url: &str, user: &str, password: &str) -> Result<UiPolicyDecisionPage, String> {
    let credentials = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    let response = gloo_net::http::Request::get(url)
        .header("accept", "application/json")
        .header("authorization", &format!("Basic {credentials}"))
        .send()
        .await
        .map_err(|_| "The policy decision service could not be reached.".to_owned())?;
    match response.status() {
        200 => response
            .json()
            .await
            .map_err(|_| "The policy decision service returned invalid data.".to_owned()),
        400 => Err("One or more policy decision filters are invalid.".to_owned()),
        401 => Err("The username or password was not accepted.".to_owned()),
        403 => Err("This repository token cannot inspect policy decisions.".to_owned()),
        404 => Err("The repository was not found or is not available to this user.".to_owned()),
        _ => Err("The policy decision service is unavailable.".to_owned()),
    }
}

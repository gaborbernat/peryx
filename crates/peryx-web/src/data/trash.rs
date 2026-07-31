use base64::Engine as _;

use crate::model::UiTrashPage;

pub async fn load_trash(url: &str, user: &str, password: &str) -> Result<UiTrashPage, String> {
    let credentials = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    let response = gloo_net::http::Request::get(url)
        .header("accept", "application/json")
        .header("authorization", &format!("Basic {credentials}"))
        .send()
        .await
        .map_err(|_| "The trash inspection service could not be reached.".to_owned())?;
    match response.status() {
        200 => response
            .json()
            .await
            .map_err(|_| "The trash inspection service returned invalid data.".to_owned()),
        400 => Err("One or more trash filters are invalid.".to_owned()),
        401 => Err("The username or password was not accepted.".to_owned()),
        403 => Err("This token cannot inspect trash.".to_owned()),
        404 => Err("The repository was not found or is not available to this user.".to_owned()),
        _ => Err("The trash inspection service is unavailable.".to_owned()),
    }
}

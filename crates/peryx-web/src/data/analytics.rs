use base64::Engine as _;

use crate::model::{AnalyticsView, UiUsagePage};

pub async fn load_analytics(url: &str, view: AnalyticsView, user: &str, password: &str) -> Result<UiUsagePage, String> {
    let credentials = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    let response = gloo_net::http::Request::get(url)
        .header("accept", "application/json")
        .header("authorization", &format!("Basic {credentials}"))
        .send()
        .await
        .map_err(|_| "The analytics service could not be reached.".to_owned())?;
    match response.status() {
        200 => {
            let value = response
                .json()
                .await
                .map_err(|_| "The analytics service returned invalid data.".to_owned())?;
            UiUsagePage::parse(view, &value).ok_or_else(|| "The analytics service returned invalid data.".to_owned())
        }
        400 => Err("One or more analytics filters are invalid.".to_owned()),
        401 => Err("The username or password was not accepted.".to_owned()),
        403 => Err("This repository token cannot inspect usage analytics.".to_owned()),
        404 => Err("The repository was not found or is not available to this user.".to_owned()),
        _ => Err("The analytics service is unavailable.".to_owned()),
    }
}

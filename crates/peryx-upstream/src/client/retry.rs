//! Retry policy: which failures are worth retrying and the jittered backoff between attempts.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::StatusCode;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use url::Url;

use super::redact_url;

pub const MAX_RETRIES: u32 = 2;
const RETRY_BASE_MILLIS: u64 = 100;
const RETRY_CAP_MILLIS: u64 = 2_000;
const RETRY_AFTER_CAP: Duration = Duration::from_secs(30);

pub(super) fn should_retry_status(status: StatusCode) -> bool {
    status.is_server_error() || matches!(status, StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS)
}

/// The wait a retryable response asks for through `Retry-After`, clamped to a bounded cap.
///
/// Honors both RFC 9110 forms: a delay in seconds or an HTTP-date. Returns `None` when the header is
/// absent, malformed, or already in the past, which leaves the caller on its jittered backoff.
#[must_use]
pub fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    let delay = value.parse::<u64>().map(Duration::from_secs).ok().or_else(|| {
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(SystemTime::now())
            .ok()
    })?;
    Some(delay.min(RETRY_AFTER_CAP))
}

#[must_use]
pub fn should_retry_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_body() || err.is_decode()
}

pub async fn sleep_before_retry(url: &Url, attempt: u32, err: &reqwest::Error) {
    sleep_before_retry_str(url.as_str(), attempt, err).await;
}

pub(super) async fn sleep_before_retry_str(url: &str, attempt: u32, err: &reqwest::Error) {
    let delay = retry_delay(attempt);
    let url = redact_url(url);
    let error = err
        .status()
        .map_or_else(|| "request".to_owned(), |status| status.as_u16().to_string());
    let delay_ms = delay.as_millis();
    tracing::debug!(url, error, delay_ms, "upstream retry");
    tokio::time::sleep(delay).await;
}

pub(super) async fn sleep_before_retry_status(url: &Url, attempt: u32, status: StatusCode, headers: &HeaderMap) {
    let delay = retry_after(headers).unwrap_or_else(|| retry_delay(attempt));
    let url = redact_url(url.as_str());
    let delay_ms = delay.as_millis();
    tracing::debug!(url, %status, delay_ms, "upstream returned retryable status");
    tokio::time::sleep(delay).await;
}

fn retry_delay(attempt: u32) -> Duration {
    let cap = RETRY_CAP_MILLIS.min(RETRY_BASE_MILLIS.saturating_mul(1_u64 << attempt.min(20)));
    let floor = cap / 2;
    Duration::from_millis(floor + jitter(cap - floor + 1))
}

fn jitter(modulus: u64) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| u64::from(duration.subsec_nanos()) % modulus)
}

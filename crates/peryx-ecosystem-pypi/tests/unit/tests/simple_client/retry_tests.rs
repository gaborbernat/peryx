use peryx_upstream::UpstreamClient;
use peryx_upstream::retry::MAX_RETRIES;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::simple_client;
use crate::simple_client::{CachedValidators, SimpleClientExt as _};

struct RetryServer {
    url: String,
    address: std::net::SocketAddr,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl std::ops::Deref for RetryServer {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.url
    }
}

impl Drop for RetryServer {
    fn drop(&mut self) {
        let _ = std::net::TcpStream::connect(self.address);
        let _ = std::net::TcpStream::connect(self.address);
        let joined = self.handle.take().unwrap().join();
        if !std::thread::panicking() {
            joined.expect("retry fixture panicked");
        }
    }
}

fn truncated_then_ok_server(body: &'static [u8], content_type: Option<&'static str>) -> RetryServer {
    truncated_times_then_ok_server(1, body, content_type)
}

/// Answers `truncated` connections with a body cut short of its `Content-Length`, then one with the
/// whole body.
fn truncated_times_then_ok_server(
    truncated: usize,
    body: &'static [u8],
    content_type: Option<&'static str>,
) -> RetryServer {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = std::thread::spawn(move || {
        for _ in 0..truncated {
            write_response(
                listener.accept().unwrap().0,
                &body[..body.len().min(4)],
                body.len() + 16,
                content_type,
            );
        }
        write_response(listener.accept().unwrap().0, body, body.len(), content_type);
    });
    RetryServer {
        url: format!("http://{addr}/simple/"),
        address: addr,
        handle: Some(handle),
    }
}

/// Writes the whole response in one call, so a response to a peer that already hung up (as the
/// connections `RetryServer` opens on drop do) still lands in the socket buffer rather than failing
/// on a later write.
fn write_response(mut socket: std::net::TcpStream, body: &[u8], content_length: usize, content_type: Option<&str>) {
    use std::io::{Read as _, Write as _};

    let mut buffer = [0; 1024];
    let _ = socket.read(&mut buffer);
    let mut response = format!("HTTP/1.1 200 OK\r\ncontent-length: {content_length}\r\nconnection: close\r\n");
    if let Some(content_type) = content_type {
        response.push_str("content-type: ");
        response.push_str(content_type);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    let mut response = response.into_bytes();
    response.extend_from_slice(body);
    socket.write_all(&response).unwrap();
}

#[tokio::test]
async fn test_fetch_index_retries_body_errors() {
    let base = truncated_then_ok_server(
        b"{\"meta\":{},\"projects\":[]}",
        Some("application/vnd.pypi.simple.v1+json"),
    );
    let client = UpstreamClient::new(&base).unwrap();

    let response = client.fetch_index().await.unwrap();

    assert_eq!(&response.body[..], b"{\"meta\":{},\"projects\":[]}");
}

#[tokio::test]
async fn test_fetch_project_retries_transient_statuses() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/simple/flask/"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/simple/flask/"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(b"{\"meta\":{}}".to_vec(), "application/vnd.pypi.simple.v1+json"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = simple_client(&server);

    let response = client
        .fetch_project("flask", CachedValidators::default())
        .await
        .unwrap();

    assert_eq!(response.status, 200);
}

#[tokio::test]
async fn test_fetch_project_retries_body_errors() {
    let base = truncated_then_ok_server(b"{\"meta\":{}}", Some("application/vnd.pypi.simple.v1+json"));
    let client = UpstreamClient::new(&base).unwrap();

    let response = client
        .fetch_project("flask", CachedValidators::default())
        .await
        .unwrap();

    assert_eq!(&response.body[..], b"{\"meta\":{}}");
}

/// A body cut short is retried only `MAX_RETRIES` times; the next failure is the answer even when the
/// upstream would have served the page on one more attempt.
#[tokio::test]
async fn test_fetch_project_stops_retrying_body_errors_after_the_retry_budget() {
    let base = truncated_times_then_ok_server(
        MAX_RETRIES as usize + 1,
        b"{\"meta\":{}}",
        Some("application/vnd.pypi.simple.v1+json"),
    );
    let client = UpstreamClient::new(&base).unwrap();

    let error = client
        .fetch_project("flask", CachedValidators::default())
        .await
        .unwrap_err();

    assert!(matches!(error, peryx_upstream::UpstreamError::Http(_)), "{error:?}");
}

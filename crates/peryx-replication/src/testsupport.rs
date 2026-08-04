//! Shared in-process listeners for replication transport tests.
//!
//! A raw [`tokio::net::TcpListener`] lets a test drive the socket directly, forcing the loss arms a
//! well-behaved [`axum`] server never produces: a body cut short of its declared `Content-Length`,
//! which surfaces as a mid-stream disconnect in the client.

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// A raw TCP server that answers every request with `200 OK` and a `Content-Length` promising
/// `declared` bytes, writes only `sent`, then closes — so a client reading the body hits EOF before
/// the promised length and maps it to a disconnect.
pub struct TruncatedBodyServer {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl TruncatedBodyServer {
    pub async fn start(declared: usize, sent: &'static [u8]) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {declared}\r\n\r\n");
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = [0_u8; 1024];
                let _ = stream.read(&mut request).await;
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(sent).await;
            }
        });
        Self {
            url: format!("http://{address}/"),
            task,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for TruncatedBodyServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

use std::net::SocketAddr;
use std::task::{Context, Poll};
use std::time::Duration;

/// A raw peer for tests that need a real connection without a protocol response.
pub struct ControlledPeer {
    listener: tokio::net::TcpListener,
}

impl ControlledPeer {
    /// Bind a peer for the client a test is about to drive.
    ///
    /// # Panics
    ///
    /// Panics when the local listener cannot bind.
    pub async fn start() -> Self {
        Self {
            listener: tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        }
    }

    #[must_use]
    ///
    /// # Panics
    ///
    /// Panics when the listener no longer has a local address.
    pub fn address(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    /// Run the clock while a client dials or exchanges bytes with the peer.
    pub fn run_clock(&self) {
        tokio::time::resume();
    }

    /// Accept the connection a test has asked its client to make.
    ///
    /// # Panics
    ///
    /// Panics when no client connects within `within` or the listener fails.
    pub async fn accept(&self, within: Duration) -> tokio::net::TcpStream {
        tokio::time::timeout(within, self.listener.accept())
            .await
            .expect("the client connects to its peer")
            .unwrap()
            .0
    }

    pub fn poll_accept(&self, context: &mut Context<'_>) -> Poll<std::io::Result<tokio::net::TcpStream>> {
        self.listener
            .poll_accept(context)
            .map(|result| result.map(|(stream, _)| stream))
    }

    /// Pause only after the request arrives, then advance its virtual deadline.
    pub async fn advance_to(deadline: tokio::time::Instant) {
        tokio::time::pause();
        tokio::time::advance(deadline.saturating_duration_since(tokio::time::Instant::now())).await;
    }
}

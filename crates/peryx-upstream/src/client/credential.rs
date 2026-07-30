use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::Mutex;

use super::Auth;

type LoadFuture = Pin<Box<dyn Future<Output = Result<LoadedCredential, CredentialError>> + Send>>;
type Loader = dyn Fn() -> LoadFuture + Send + Sync;
static NEXT_PROVIDER_ID: AtomicU64 = AtomicU64::new(0);

pub(super) struct LoadedCredential {
    pub auth: Auth,
    pub refresh_after: Duration,
}

/// Request behavior after a credential source cannot be refreshed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialFailure {
    /// Reject requests until a later refresh succeeds.
    Fail,
    /// Retry without authentication.
    Anonymous,
}

/// When and how a provider reloads credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialRefresh {
    /// Minimum time between source reads.
    pub interval: Duration,
    /// Reload once when an upstream rejects the current generation.
    pub on_unauthorized: bool,
    /// Request behavior when the source read fails.
    pub failure: CredentialFailure,
}

/// A redacted credential-source failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct CredentialError {
    message: Arc<str>,
}

impl CredentialError {
    /// Wrap a source error that contains no credential material.
    #[must_use]
    pub fn new(message: impl Into<Arc<str>>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Opaque identity of one configured credential provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CredentialProviderId(u64);

/// Opaque identity of one provider's credential generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CredentialIdentity {
    provider: CredentialProviderId,
    generation: u64,
}

impl CredentialIdentity {
    /// The configured provider that owns this generation.
    #[must_use]
    pub const fn provider(self) -> CredentialProviderId {
        self.provider
    }
}

/// One immutable credential generation used for the lifetime of a request attempt.
#[derive(Debug)]
pub struct CredentialSnapshot {
    auth: Auth,
    error: Option<CredentialError>,
    identity: CredentialIdentity,
    refresh_deadline: Option<Duration>,
}

impl CredentialSnapshot {
    /// The coherent credential generation for one request.
    #[must_use]
    pub const fn auth(&self) -> &Auth {
        &self.auth
    }

    /// Generation number within one provider.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.identity.generation
    }

    /// Provider and generation identity for credential-bound caches.
    #[must_use]
    pub const fn identity(&self) -> CredentialIdentity {
        self.identity
    }

    pub(super) const fn configured_auth(&self) -> &Auth {
        &self.auth
    }

    fn resolved(snapshot: Arc<Self>) -> Result<Arc<Self>, CredentialError> {
        snapshot.error.clone().map_or(Ok(snapshot), Err)
    }
}

struct CredentialProviderInner {
    snapshot: ArcSwap<CredentialSnapshot>,
    started: std::time::Instant,
    refresh: Option<CredentialRefresh>,
    loader: Option<Arc<Loader>>,
    refresh_gate: Mutex<()>,
}

/// Shared credentials with lock-free reads and single-flight refreshes.
#[derive(Clone)]
pub struct CredentialProvider(Arc<CredentialProviderInner>);

impl std::fmt::Debug for CredentialProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialProvider")
            .field("refresh", &self.0.refresh)
            .finish_non_exhaustive()
    }
}

impl CredentialProvider {
    /// Build a provider that never reloads its credential.
    #[must_use]
    pub fn fixed(auth: Auth) -> Self {
        let identity = CredentialIdentity {
            provider: CredentialProviderId(NEXT_PROVIDER_ID.fetch_add(1, Ordering::Relaxed)),
            generation: 0,
        };
        Self(Arc::new(CredentialProviderInner {
            snapshot: ArcSwap::from_pointee(CredentialSnapshot {
                auth,
                error: None,
                identity,
                refresh_deadline: None,
            }),
            started: std::time::Instant::now(),
            refresh: None,
            loader: None,
            refresh_gate: Mutex::new(()),
        }))
    }

    /// Build a provider that reloads through `loader` under `refresh` policy.
    #[must_use]
    pub fn refreshing<F, Fut>(auth: Auth, refresh: CredentialRefresh, loader: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Auth, CredentialError>> + Send + 'static,
    {
        Self::with_loader(auth, refresh.interval, refresh, move || {
            let future = loader();
            async move {
                future.await.map(|auth| LoadedCredential {
                    auth,
                    refresh_after: refresh.interval,
                })
            }
        })
    }

    pub(super) fn lazy<F, Fut>(refresh: CredentialRefresh, loader: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<LoadedCredential, CredentialError>> + Send + 'static,
    {
        Self::with_loader(Auth::None, Duration::ZERO, refresh, loader)
    }

    fn with_loader<F, Fut>(auth: Auth, refresh_deadline: Duration, refresh: CredentialRefresh, loader: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<LoadedCredential, CredentialError>> + Send + 'static,
    {
        let identity = CredentialIdentity {
            provider: CredentialProviderId(NEXT_PROVIDER_ID.fetch_add(1, Ordering::Relaxed)),
            generation: 0,
        };
        Self(Arc::new(CredentialProviderInner {
            snapshot: ArcSwap::from_pointee(CredentialSnapshot {
                auth,
                error: None,
                identity,
                refresh_deadline: Some(refresh_deadline),
            }),
            started: std::time::Instant::now(),
            refresh: Some(refresh),
            loader: Some(Arc::new(move || Box::pin(loader()))),
            refresh_gate: Mutex::new(()),
        }))
    }

    /// Return one coherent snapshot, reloading first when its deadline has passed.
    ///
    /// # Errors
    /// Returns the latest source failure under [`CredentialFailure::Fail`].
    pub async fn credential(&self) -> Result<Arc<CredentialSnapshot>, CredentialError> {
        let snapshot = self.0.snapshot.load_full();
        if !self.refresh_due(&snapshot) {
            return CredentialSnapshot::resolved(snapshot);
        }
        self.refresh(snapshot.generation()).await
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> Arc<CredentialSnapshot> {
        self.0.snapshot.load_full()
    }

    /// Reload after an upstream rejected `rejected_generation`, unless another request already did.
    ///
    /// # Errors
    /// Returns the source failure under [`CredentialFailure::Fail`].
    pub async fn refresh_after_unauthorized(
        &self,
        rejected_generation: u64,
    ) -> Result<Arc<CredentialSnapshot>, CredentialError> {
        if !self.0.refresh.is_some_and(|refresh| refresh.on_unauthorized) {
            return self.current();
        }
        self.refresh(rejected_generation).await
    }

    /// Return the last snapshot without applying its refresh deadline.
    ///
    /// # Errors
    /// Returns the latest source failure under [`CredentialFailure::Fail`].
    pub fn current(&self) -> Result<Arc<CredentialSnapshot>, CredentialError> {
        CredentialSnapshot::resolved(self.0.snapshot.load_full())
    }

    fn refresh_due(&self, snapshot: &CredentialSnapshot) -> bool {
        snapshot
            .refresh_deadline
            .is_some_and(|deadline| self.0.started.elapsed() >= deadline)
    }

    async fn refresh(&self, generation: u64) -> Result<Arc<CredentialSnapshot>, CredentialError> {
        let _guard = self.0.refresh_gate.lock().await;
        let current = self.0.snapshot.load_full();
        if current.generation() != generation {
            return CredentialSnapshot::resolved(current);
        }
        let refresh = self.0.refresh.expect("a refresh needs refresh policy");
        let result = self.0.loader.as_ref().expect("a refresh needs a loader")().await;
        let (auth, error, refresh_after) = match result {
            Ok(loaded) => (loaded.auth, None, loaded.refresh_after),
            Err(_) if refresh.failure == CredentialFailure::Anonymous => (Auth::None, None, refresh.interval),
            Err(error) => (current.auth.clone(), Some(error), refresh.interval),
        };
        let snapshot = Arc::new(CredentialSnapshot {
            auth,
            error,
            identity: CredentialIdentity {
                provider: current.identity.provider,
                generation: current.generation().wrapping_add(1),
            },
            refresh_deadline: Some(self.0.started.elapsed().saturating_add(refresh_after)),
        });
        self.0.snapshot.store(snapshot.clone());
        CredentialSnapshot::resolved(snapshot)
    }
}

//! Excludes failing blob sources from placement selection after a configured number of consecutive
//! losses. Each admitted call owns a permit. After cooldown, one permit claims the half-open probe;
//! unresolved and expired probes reopen the source.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const DEFAULT_CIRCUIT: CircuitConfig = CircuitConfig {
    trip_after: 3,
    cooldown: Duration::from_secs(30),
    probe_timeout: Duration::from_secs(30),
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CircuitConfig {
    /// Zero is treated as one.
    pub trip_after: u32,
    pub cooldown: Duration,
    /// Bounds a half-open claim so cancellation cannot hold it forever.
    pub probe_timeout: Duration,
}

impl Default for CircuitConfig {
    fn default() -> Self {
        DEFAULT_CIRCUIT
    }
}

pub type CircuitClock = Arc<dyn Fn() -> Duration + Send + Sync>;

#[derive(Debug, Clone)]
enum State {
    Closed { failures: u32 },
    Open { until: Duration },
    HalfOpen { claim: Arc<()>, until: Duration },
}

struct Shared {
    config: CircuitConfig,
    clock: CircuitClock,
    state: Mutex<BreakerState>,
}

#[derive(Debug, Default)]
struct BreakerState {
    sources: HashMap<String, State>,
}

#[derive(Clone)]
pub struct CircuitBreaker {
    shared: Arc<Shared>,
}

impl CircuitBreaker {
    #[must_use]
    pub fn new(config: CircuitConfig, clock: CircuitClock) -> Self {
        Self {
            shared: Arc::new(Shared {
                config,
                clock,
                state: Mutex::new(BreakerState::default()),
            }),
        }
    }

    /// Atomically admits closed traffic or claims the one half-open probe after cooldown.
    #[must_use]
    pub fn admit(&self, source: &str) -> Option<CircuitPermit> {
        let now = (self.shared.clock)();
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let claim = match state.sources.get(source).cloned() {
            None | Some(State::Closed { .. }) => None,
            Some(State::Open { until }) if now < until => return None,
            Some(State::HalfOpen { until, .. }) if now < until => return None,
            Some(State::HalfOpen { until, .. }) if now < until + self.shared.config.cooldown => {
                state.sources.insert(
                    source.to_owned(),
                    State::Open {
                        until: until + self.shared.config.cooldown,
                    },
                );
                return None;
            }
            Some(State::Open { .. } | State::HalfOpen { .. }) => {
                let claim = Arc::new(());
                state.sources.insert(
                    source.to_owned(),
                    State::HalfOpen {
                        claim: Arc::clone(&claim),
                        until: now + self.shared.config.probe_timeout,
                    },
                );
                Some(claim)
            }
        };
        drop(state);
        Some(CircuitPermit {
            shared: Arc::clone(&self.shared),
            source: source.to_owned(),
            claim,
            resolved: false,
        })
    }
}

pub struct CircuitPermit {
    shared: Arc<Shared>,
    source: String,
    claim: Option<Arc<()>>,
    resolved: bool,
}

impl CircuitPermit {
    pub fn success(mut self) {
        self.resolve(true);
    }

    pub fn failure(mut self) {
        self.resolve(false);
    }

    fn resolve(&mut self, success: bool) {
        let now = (self.shared.clock)();
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match self.claim.as_ref() {
            Some(claim) => {
                let Some(State::HalfOpen {
                    claim: active_claim,
                    until,
                }) = state.sources.get(&self.source).cloned()
                else {
                    self.resolved = true;
                    return;
                };
                if !Arc::ptr_eq(&active_claim, claim) {
                    self.resolved = true;
                    return;
                }
                state.sources.insert(
                    self.source.clone(),
                    if now >= until || !success {
                        State::Open {
                            until: cooldown_deadline(now, until, self.shared.config.cooldown),
                        }
                    } else {
                        State::Closed { failures: 0 }
                    },
                );
            }
            None => record_closed_outcome(&mut state.sources, &self.source, now, self.shared.config, success),
        }
        self.resolved = true;
    }
}

impl Drop for CircuitPermit {
    fn drop(&mut self) {
        let Some(claim) = self.claim.as_ref().filter(|_| !self.resolved) else {
            return;
        };
        let now = (self.shared.clock)();
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(State::HalfOpen { claim: active, until }) = state.sources.get(&self.source).cloned()
            && Arc::ptr_eq(&active, claim)
        {
            state.sources.insert(
                self.source.clone(),
                State::Open {
                    until: cooldown_deadline(now, until, self.shared.config.cooldown),
                },
            );
        }
    }
}

fn cooldown_deadline(now: Duration, probe_deadline: Duration, cooldown: Duration) -> Duration {
    if now >= probe_deadline {
        probe_deadline + cooldown
    } else {
        now + cooldown
    }
}

fn record_closed_outcome(
    sources: &mut HashMap<String, State>,
    source: &str,
    now: Duration,
    config: CircuitConfig,
    success: bool,
) {
    if success {
        sources.insert(source.to_owned(), State::Closed { failures: 0 });
        return;
    }
    let threshold = config.trip_after.max(1);
    let failures = match sources.get(source) {
        Some(State::Closed { failures }) => *failures,
        Some(State::Open { .. } | State::HalfOpen { .. }) => threshold,
        None => 0,
    };
    sources.insert(
        source.to_owned(),
        if failures + 1 < threshold {
            State::Closed { failures: failures + 1 }
        } else {
            State::Open {
                until: now + config.cooldown,
            }
        },
    );
}

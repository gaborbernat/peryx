use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use tokio::time::Instant;
use url::Url;

use super::{RANGE_SUPPRESSION_CAPACITY, RANGE_SUPPRESSION_TTL};

#[derive(Default)]
pub(super) struct RangeSuppressions(Mutex<SuppressionState>);

impl std::fmt::Debug for RangeSuppressions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("RangeSuppressions").finish_non_exhaustive()
    }
}

impl RangeSuppressions {
    pub(super) fn contains(&self, url: &Url) -> bool {
        let mut state = self.0.lock().expect("range suppression lock is never poisoned");
        state.prune(Instant::now());
        state.deadlines.contains_key(url)
    }

    pub(super) fn insert(&self, url: Url) {
        let mut state = self.0.lock().expect("range suppression lock is never poisoned");
        let now = Instant::now();
        state.prune(now);
        state.order.retain(|(candidate, _)| candidate != &url);
        state.deadlines.remove(&url);
        while state.deadlines.len() >= RANGE_SUPPRESSION_CAPACITY {
            state.evict_oldest();
        }
        let deadline = now + RANGE_SUPPRESSION_TTL;
        state.deadlines.insert(url.clone(), deadline);
        state.order.push_back((url, deadline));
    }
}

#[derive(Default)]
struct SuppressionState {
    deadlines: HashMap<Url, Instant>,
    order: VecDeque<(Url, Instant)>,
}

impl SuppressionState {
    fn prune(&mut self, now: Instant) {
        while self.order.front().is_some_and(|(_, deadline)| *deadline <= now) {
            self.evict_oldest();
        }
    }

    fn evict_oldest(&mut self) {
        if let Some((url, _)) = self.order.pop_front() {
            self.deadlines.remove(&url);
        }
    }
}

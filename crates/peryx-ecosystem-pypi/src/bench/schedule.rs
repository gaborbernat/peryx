#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Entry {
    pub server: usize,
    pub round: usize,
}

pub(super) struct Schedule {
    pub seed: u64,
    pub rounds: usize,
    entries: Vec<Entry>,
}

impl Schedule {
    pub fn new(servers: usize, rounds: usize, seed: u64) -> Self {
        let mut random = seed.max(1);
        let mut entries = Vec::with_capacity(servers.saturating_mul(rounds));
        for round in 1..=rounds {
            let mut current = (0..servers).collect::<Vec<_>>();
            for index in (1..current.len()).rev() {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                current.swap(
                    index,
                    usize::try_from(random % (index as u64 + 1)).expect("index fits usize"),
                );
            }
            entries.extend(current.into_iter().map(|server| Entry { server, round }));
        }
        Self { seed, rounds, entries }
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
}

#[cfg(test)]
#[path = "../../tests/unit/bench/schedule.rs"]
mod tests;

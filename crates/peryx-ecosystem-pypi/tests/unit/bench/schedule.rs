use super::*;

#[test]
fn schedule_interleaves_every_server_round_once() {
    let schedule = Schedule::new(3, 2, 1161);

    assert_eq!(
        schedule
            .entries()
            .iter()
            .map(|entry| (entry.server, entry.round))
            .collect::<Vec<_>>(),
        [(0, 1), (1, 1), (2, 1), (0, 2), (2, 2), (1, 2)]
    );
}

#[test]
fn schedule_repeats_for_the_same_seed() {
    assert_eq!(Schedule::new(5, 4, 7).entries(), Schedule::new(5, 4, 7).entries());
}

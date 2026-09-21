use std::alloc::System;

use peryx_index::serving::ServingCache;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[test]
fn test_resource_ticket_churn_releases_invalidated_names() {
    let cache = ServingCache::new(1024, 60);
    let region = Region::new(ALLOCATOR);
    let padding = "x".repeat(131_072);

    for round in 0..8 {
        for resource in 0..128 {
            let _ = cache.representation_key("route", &format!("{round}-{resource}{padding}"), "json");
        }
    }

    let stats = region.change();
    let held = stats.bytes_allocated - stats.bytes_deallocated;
    assert!(held < 32 << 20, "cache retained {held} bytes");
}

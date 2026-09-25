//! PR #1233 review: the pressure cascade does not fire for the cold tier's
//! dead-slot ledger. The published per-shard figure counts it (it is
//! resident RAM, so `used_memory` must show it), but page-cache eviction,
//! vector demotion and KV eviction cannot free a byte of it — a cascade it
//! drives only pushes live data to disk for nothing.

use std::sync::Arc;

use super::*;
use crate::storage::Database;

#[test]
fn the_pressure_cascade_ignores_the_dead_slot_ledger() {
    use clap::Parser;
    let dbs = vec![vec![Database::new()]];
    let (shared, _inits) = super::super::shared_databases::ShardDatabases::new(dbs);
    let rt = crate::config::RuntimeConfig {
        maxmemory: 1024 * 1024,
        num_shards: 1,
        ..Default::default()
    };
    let runtime_config = Arc::new(parking_lot::RwLock::new(rt));
    let server_config = Arc::new(crate::config::ServerConfig::parse_from::<[&str; 0], &str>(
        [],
    ));
    // 95% published, of which 60 points are ledger: the evictable part is at
    // 35%, well under the 85% threshold.
    shared.publish_memory(0, (1024 * 1024 * 95) / 100);
    let ledger = (1024 * 1024 * 60) / 100;
    assert!(
        should_run_pressure_cascade(&runtime_config, &server_config, &shared, 0, 0, 0),
        "fixture: without the exclusion the ledger alone fires the cascade"
    );
    assert!(
        !should_run_pressure_cascade(&runtime_config, &server_config, &shared, 0, 0, ledger),
        "the ledger must not drive the cascade"
    );
    // Evictable memory over the threshold still fires it.
    shared.publish_memory(0, (1024 * 1024 * 95) / 100 + ledger);
    assert!(should_run_pressure_cascade(
        &runtime_config,
        &server_config,
        &shared,
        0,
        0,
        ledger
    ));
}

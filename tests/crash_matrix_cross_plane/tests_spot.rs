//! Spot-check cells: the 4 remaining config cells outside the curated
//! production/legacy set (brief: "spot-check the remaining cells with 1-2
//! representative scenarios each").

use crate::harness::Config;
use crate::scenarios;

// ---------------------------------------------------------------------------
// appendonly=no + disk-offload=enable (spill is a documented loud no-op
// without a durability backstop — src/config.rs `disk_offload_spill_inert`;
// persistence_dir is None here too, so this is the same "no durability
// contract on any plane" shape as the legacy_no_s1 cell).
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_spot_offload_no_s1_no_durability_contract_liveness() {
    scenarios::no_durability_liveness(&Config::SPOT_OFFLOAD_NO_S1);
}

#[test]
#[ignore]
fn cross_plane_spot_offload_no_s4_no_durability_contract_liveness() {
    scenarios::no_durability_liveness(&Config::SPOT_OFFLOAD_NO_S4);
}

// ---------------------------------------------------------------------------
// Legacy mode at shards=4 (excluded from the full matrix — "legacy mode has
// no cross-shard novelty" — but the brief still wants 1-2 representative
// scenarios). graph_isolated here used to reproduce the SAME RED gap as
// legacy_yes_s1 (the bug was legacy-mode WAL replay, not shard-count-
// specific). GREEN as of task #60 (see `tests_legacy.rs` module doc for the
// root cause); confirms the fix is not accidentally shard-count-dependent.
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_spot_legacy_s4_graph_isolated() {
    scenarios::graph_isolated(&Config::SPOT_LEGACY_S4);
}

#[test]
#[ignore]
fn cross_plane_spot_legacy_s4_mq_isolated() {
    scenarios::mq_isolated(&Config::SPOT_LEGACY_S4);
}

#[test]
#[ignore]
fn cross_plane_spot_legacy_no_s4_no_durability_contract_liveness() {
    scenarios::no_durability_liveness(&Config::SPOT_LEGACY_NO_S4);
}

//! Spot-check cells: the 4 remaining config cells outside the curated
//! production/legacy set (brief: "spot-check the remaining cells with 1-2
//! representative scenarios each").

use crate::harness::{self, Config};
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
// scenarios). graph_isolated here reproduces the SAME RED gap as
// legacy_yes_s1 (the bug is legacy-mode WAL replay, not shard-count-specific;
// spot-checking at shards=4 confirms the gap is not accidentally
// shard-count-dependent). Gated by `harness::red_guard` — see that
// function's doc for why `#[ignore = "RED: ..."]` alone does not skip it.
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_spot_legacy_s4_graph_isolated() {
    if !harness::red_guard(
        "legacy-mode (--disk-offload disable) graph WAL replay does not \
         reconstruct the graph — same gap as \
         cross_plane_legacy_yes_s1_graph_isolated, confirmed shard-count- \
         independent (PR #288 / task #43 changelog). Not this stage's job \
         to fix — tracked separately.",
    ) {
        return;
    }
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

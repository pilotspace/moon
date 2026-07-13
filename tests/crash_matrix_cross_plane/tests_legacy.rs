//! Legacy cells (`--disk-offload disable`), shards=1 only per the brief
//! ("legacy mode has no cross-shard novelty for this suite's purpose").
//!
//! GREEN as of task #60: the 5 cells below were RED because
//! `replay_graph_wal` (`src/shard/shared_databases.rs`, legacy-mode-only —
//! the disk-offload path uses the separate `replay_graph_wal_v3`) passed
//! the raw RESP-encoded `WalRecord::payload` blob directly as the `cmd`
//! argument to `CommandReplayEngine::replay_command`, instead of parsing it
//! into frames and passing the bare command name + `&[Frame]` args first
//! (the pattern `replay_graph_wal_v3` and `recovery.rs`'s KV Command replay
//! both already used). `GraphReplayCollector::is_graph_command` compares
//! against literal names like `b"GRAPH.CREATE"`, so it never matched a
//! multi-line RESP blob — `graph_command_count()` stayed 0 and the entire
//! graph plane was silently dropped on restart. Fixed by adding the same
//! parse-then-dispatch step; cells un-gated (no more `harness::red_guard`).

use crate::harness::Config;
use crate::scenarios;

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_legacy_yes_s1_kv_isolated() {
    scenarios::kv_isolated(&Config::LEGACY_YES_S1);
}

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_legacy_yes_s1_graph_isolated() {
    scenarios::graph_isolated(&Config::LEGACY_YES_S1);
}

#[test]
#[ignore]
fn cross_plane_legacy_yes_s1_vector_isolated() {
    scenarios::vector_isolated(&Config::LEGACY_YES_S1);
}

#[test]
#[ignore]
fn cross_plane_legacy_yes_s1_ws_isolated() {
    scenarios::ws_isolated(&Config::LEGACY_YES_S1);
}

#[test]
#[ignore]
fn cross_plane_legacy_yes_s1_mq_isolated() {
    scenarios::mq_isolated(&Config::LEGACY_YES_S1);
}

// The same root cause (formerly: legacy-mode graph WAL replay never
// reconstructed the graph — see the module doc) covered the 4 cells below
// too, including BOTH txn_isolated halves (review round 3, P1
// restructuring, task #52). Fixed alongside `graph_isolated` by task #60;
// all now GREEN and un-gated.

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_legacy_yes_s1_txn_isolated_committed() {
    scenarios::txn_isolated_committed(&Config::LEGACY_YES_S1);
}

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_legacy_yes_s1_txn_isolated_atomicity() {
    scenarios::txn_isolated_atomicity(&Config::LEGACY_YES_S1);
}

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_legacy_yes_s1_mixed_all_planes_synced() {
    scenarios::mixed_synced(&Config::LEGACY_YES_S1);
}

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_legacy_yes_s1_mixed_all_planes_mid_pass_c() {
    scenarios::mixed_mid_pass_c(&Config::LEGACY_YES_S1);
}

#[test]
#[ignore]
fn cross_plane_legacy_yes_s1_mixed_all_planes_concurrent_burst_no_corruption() {
    scenarios::concurrent_burst_no_corruption(&Config::LEGACY_YES_S1);
}

// ---------------------------------------------------------------------------
// legacy_no_s1: appendonly=no + disk-offload=disable — NO durability contract
// on any plane (persistence_dir never constructed). Liveness/no-corruption
// only, per the brief's "vacuous by design, not a bug" note.
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn cross_plane_legacy_no_s1_no_durability_contract_liveness() {
    scenarios::no_durability_liveness(&Config::LEGACY_NO_S1);
}

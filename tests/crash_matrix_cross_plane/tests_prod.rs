//! Production cells: the full plane×workload matrix on the 2 configs the
//! brief calls out as "the config that matters most" (disk-offload enable,
//! appendonly yes, shards {1,4}). Naming: `cross_plane_<config>_<cell>`
//! (flat function names, per the brief's explicit suggestion — avoids the
//! `g1_` prefix collision `crash_recovery_graph_durability.rs` already has).

use crate::harness::Config;
use crate::scenarios;

/// FIXED (kernel M3 stage 2 / K2, task #53). Root cause: `save_graph_store`
/// (`src/graph/recovery.rs`) wrote the reference/floor
/// (`graph_metadata.json` via `store.save_metadata`) BEFORE the payload it
/// claims durable (CSR segments + `manifest.json`) — an ARIES-inverted
/// ordering. A kill-9 landing between the two writes left a
/// fully-advanced floor pointing at a payload that was never persisted,
/// so recovery trusted the floor and skipped WAL replay for records the
/// floor claimed were already covered, total-losing the graph batch. Fixed
/// by reordering `save_graph_store` to write CSR segments + manifest.json
/// FIRST, `store.save_metadata` LAST, and making `save_metadata` itself
/// atomic (temp+fsync+rename+dir-fsync via
/// `persistence::atomic::atomic_write_durable`) so the floor write can
/// never itself be torn. Safe against double-replay because
/// `graph::replay::node_present` checks both `write_buf` and loaded CSR
/// segments before re-inserting a WAL-logged node/edge — replaying a
/// record whose payload actually made it to disk is a no-op, not a
/// duplicate. Confirmed via `MOON_CRASH_MATRIX_RED=1
/// MOON_CRASH_MATRIX_ITERS=20` soak, 20/20 clean on both prod_s1 and
/// prod_s4 (pre-fix baseline: prod_s1 hit at iteration 11/20, prod_s4 at
/// iteration 7/20). See `scenarios::mixed_mid_checkpoint`'s doc for the
/// full scenario analysis.

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s1_kv_isolated() {
    scenarios::kv_isolated(&Config::PROD_S1);
}

#[test]
#[ignore]
fn cross_plane_prod_s1_kv_spilled_isolated() {
    scenarios::kv_spilled_isolated(&Config::PROD_S1);
}

#[test]
#[ignore]
fn cross_plane_prod_s1_graph_isolated() {
    scenarios::graph_isolated(&Config::PROD_S1);
}

/// Kernel M4, task #50: term-dict + FST sidecar durability -- default-GREEN
/// tripwire, see `scenarios::text_fts_sidecar_isolated`'s doc comment.
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s1_text_fts_sidecar_isolated() {
    scenarios::text_fts_sidecar_isolated(&Config::PROD_S1);
}

#[test]
#[ignore]
fn cross_plane_prod_s1_vector_isolated() {
    scenarios::vector_isolated(&Config::PROD_S1);
}

#[test]
#[ignore]
fn cross_plane_prod_s1_ws_isolated() {
    scenarios::ws_isolated(&Config::PROD_S1);
}

#[test]
#[ignore]
fn cross_plane_prod_s1_mq_isolated() {
    scenarios::mq_isolated(&Config::PROD_S1);
}

/// FIXED (kernel M3 stage 3 / task #52). Root cause: `execute_transaction_sharded`
/// (`src/server/conn/shared.rs`) had no GRAPH.* branch at all — a queued
/// GRAPH.ADDNODE fell through to the generic KV `dispatch()` table (which
/// doesn't know graph commands), erroring as "unknown command" and never
/// reaching the graph store, let alone its WAL. Fixed by routing GRAPH.*
/// commands queued inside MULTI/EXEC to the graph engine (mirroring the
/// single-command path in `try_handle_graph_command`) and flushing the
/// drained wal-v3 records after the transaction body runs. Verified 3x
/// consecutive GREEN at both shards=1 and shards=4.
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s1_txn_isolated_committed() {
    scenarios::txn_isolated_committed(&Config::PROD_S1);
}

/// Ungated (review round 3, P1) — atomicity is unrelated to task #52's
/// graph-leg-durability finding and is GREEN here; see
/// `scenarios::txn_isolated_atomicity`'s doc for why it must not share the
/// committed half's `red_guard`.
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s1_txn_isolated_atomicity() {
    scenarios::txn_isolated_atomicity(&Config::PROD_S1);
}

#[test]
#[ignore]
fn cross_plane_prod_s1_mixed_all_planes_synced() {
    scenarios::mixed_synced(&Config::PROD_S1);
}

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s1_mixed_all_planes_mid_checkpoint() {
    scenarios::mixed_mid_checkpoint(&Config::PROD_S1);
}

/// Task #53 review round 2 / P0-1 regression cell — see
/// `scenarios::graph_drop_survives_repeated_checkpoints`'s doc.
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s1_graph_drop_survives_repeated_checkpoints() {
    scenarios::graph_drop_survives_repeated_checkpoints(&Config::PROD_S1);
}

#[test]
#[ignore]
fn cross_plane_prod_s1_mixed_all_planes_concurrent_burst_no_corruption() {
    scenarios::concurrent_burst_no_corruption(&Config::PROD_S1);
}

#[test]
#[ignore]
fn cross_plane_prod_s4_kv_isolated() {
    scenarios::kv_isolated(&Config::PROD_S4);
}

#[test]
#[ignore]
fn cross_plane_prod_s4_kv_spilled_isolated() {
    scenarios::kv_spilled_isolated(&Config::PROD_S4);
}

#[test]
#[ignore]
fn cross_plane_prod_s4_graph_isolated() {
    scenarios::graph_isolated(&Config::PROD_S4);
}

#[test]
#[ignore]
fn cross_plane_prod_s4_vector_isolated() {
    scenarios::vector_isolated(&Config::PROD_S4);
}

/// Kernel M4, task #50 -- shards=4 leg (multi-shard: verifies the sidecar
/// is keyed/loaded per-shard, not once globally).
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s4_text_fts_sidecar_isolated() {
    scenarios::text_fts_sidecar_isolated(&Config::PROD_S4);
}

#[test]
#[ignore]
fn cross_plane_prod_s4_ws_isolated() {
    scenarios::ws_isolated(&Config::PROD_S4);
}

#[test]
#[ignore]
fn cross_plane_prod_s4_mq_isolated() {
    scenarios::mq_isolated(&Config::PROD_S4);
}

/// FIXED (kernel M3 stage 3 / task #52). See
/// `cross_plane_prod_s1_txn_isolated_committed`'s doc for the root cause.
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s4_txn_isolated_committed() {
    scenarios::txn_isolated_committed(&Config::PROD_S4);
}

/// Ungated (review round 3, P1) — see
/// `cross_plane_prod_s1_txn_isolated_atomicity`'s doc.
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s4_txn_isolated_atomicity() {
    scenarios::txn_isolated_atomicity(&Config::PROD_S4);
}

#[test]
#[ignore]
fn cross_plane_prod_s4_mixed_all_planes_synced() {
    scenarios::mixed_synced(&Config::PROD_S4);
}

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s4_mixed_all_planes_mid_checkpoint() {
    scenarios::mixed_mid_checkpoint(&Config::PROD_S4);
}

/// Task #53 review round 2 / P0-1 regression cell — see
/// `scenarios::graph_drop_survives_repeated_checkpoints`'s doc.
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s4_graph_drop_survives_repeated_checkpoints() {
    scenarios::graph_drop_survives_repeated_checkpoints(&Config::PROD_S4);
}

#[test]
#[ignore]
fn cross_plane_prod_s4_mixed_all_planes_concurrent_burst_no_corruption() {
    scenarios::concurrent_burst_no_corruption(&Config::PROD_S4);
}

//! Production cells: the full plane×workload matrix on the 2 configs the
//! brief calls out as "the config that matters most" (disk-offload enable,
//! appendonly yes, shards {1,4}). Naming: `cross_plane_<config>_<cell>`
//! (flat function names, per the brief's explicit suggestion — avoids the
//! `g1_` prefix collision `crash_recovery_graph_durability.rs` already has).

use crate::harness::{self, Config};
use crate::scenarios;

const TXN_GRAPH_LEG_RED_REASON: &str = "cross-store TXN graph leg not durable, task #52. A \
     committed cross-store transaction (SET+GRAPH.ADDNODE) survives kill-9 \
     on the KV leg (PR #247's persist_txn_aof) but the graph leg is lost — \
     reviewed and confirmed not a harness artifact (property-value query, \
     no VALID_AT, both legs' WAL flush confirmed synced before the kill), \
     and confirmed deterministic 3x consecutive at both shards=1 and \
     shards=4 after the task #52 hardening pass (own crash cycle per \
     round, --checkpoint-timeout 3600, kill immediately post-sync-wait). \
     Minimal repro in scenarios::txn_isolated_committed's doc comment. \
     P1 finding for kernel M3 — tracked separately, not this stage's job to \
     fix. Gates ONLY the committed-transaction half (review round 3, P1): \
     the atomicity half (`cross_plane_*_txn_isolated_atomicity`) is a \
     genuinely different, unrelated, GREEN claim and runs ungated.";

/// NEW finding (kernel M3, not a harness artifact — see
/// `scenarios::mixed_mid_checkpoint`'s doc for the full analysis): some
/// kill offsets in the post-`BGSAVE` window cause TOTAL loss of the graph
/// plane's already-wal-v3-synced content. PROBABILISTIC, not
/// deterministic — a single default run (`MOON_CRASH_MATRIX_ITERS=1`) has
/// a real chance of NOT hitting the window and reporting green even with
/// `MOON_CRASH_MATRIX_RED=1` set. Reproduce reliably with
/// `MOON_CRASH_MATRIX_RED=1 MOON_CRASH_MATRIX_ITERS=20` (confirmed hit
/// within 20 iterations on both shard counts during this stage's
/// investigation — prod_s1 at iteration 11/20, prod_s4 at iteration
/// 7/20).
const MID_CHECKPOINT_GRAPH_LOSS_RED_REASON: &str = "checkpoint-Finalize window can total-loss the graph plane, NEW. Some \
     kill offsets in the 0-150ms post-BGSAVE window lose the FULL synced \
     graph batch (not just the unsynced tail) while KV/vector/WS/MQ all \
     survive in the same run — reviewed and confirmed not a harness \
     artifact. PROBABILISTIC (~1-in-7 to 1-in-12 per MOON_CRASH_MATRIX_ITERS \
     sample in this stage's investigation) — reproduce reliably with \
     MOON_CRASH_MATRIX_RED=1 MOON_CRASH_MATRIX_ITERS=20, a single default \
     run may report green by chance. Strong P0 candidate for kernel M3 \
     stage 2 (K2). Tracked separately, not this stage's job to fix.";

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

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s1_txn_isolated_committed() {
    if !harness::red_guard(TXN_GRAPH_LEG_RED_REASON) {
        return;
    }
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
    if !harness::red_guard(MID_CHECKPOINT_GRAPH_LOSS_RED_REASON) {
        return;
    }
    scenarios::mixed_mid_checkpoint(&Config::PROD_S1);
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

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn cross_plane_prod_s4_txn_isolated_committed() {
    if !harness::red_guard(TXN_GRAPH_LEG_RED_REASON) {
        return;
    }
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
    if !harness::red_guard(MID_CHECKPOINT_GRAPH_LOSS_RED_REASON) {
        return;
    }
    scenarios::mixed_mid_checkpoint(&Config::PROD_S4);
}

#[test]
#[ignore]
fn cross_plane_prod_s4_mixed_all_planes_concurrent_burst_no_corruption() {
    scenarios::concurrent_burst_no_corruption(&Config::PROD_S4);
}

//! Kernel M3 / G1: cross-plane kill-9 crash-matrix, RED-first.
//!
//! Execution brief: `.planning/reviews/kernel-m3-brief-2026-07-12.md`
//! (Stage 1). This suite is the tripwire for Stage 2 (K2 — unified
//! per-shard floor register): "no data loss, provably, across every plane"
//! is an empty claim until something checks it, and every prior kernel
//! milestone shipped the failing test FIRST, then fixed it — never the
//! other way around (task #41/#42/#43's history). **Stage 1 does not fix
//! anything** — it exists to produce an honest RED/GREEN inventory before
//! K2 touches the recycle-floor invariant.
//!
//! Internal naming: `cross_plane_<config>_<cell>` (NOT a `g1_` prefix —
//! `tests/crash_recovery_graph_durability.rs` already has its own internal
//! `g1_.../g5_...` scenario numbering; a second, differently-scoped "G1" in
//! a different file with the same prefix would confuse `cargo test g1`
//! filtering and any future grep — brief §Stage 1).
//!
//! # Axes
//!
//! - **Plane:** KV (incl. spilled collections), graph (structural +
//!   GraphTemporal via `TEMPORAL.INVALIDATE`), vector, WS, MQ (incl.
//!   DLQ/PEL/triggers), cross-store TXN (`MULTI`/`EXEC` spanning ≥2 planes).
//!   FTS/TEMPORAL(node upsert) are WAL-recycle non-participants per the
//!   brief §1.2 and are out of scope here.
//! - **Workload:** (a) single-plane isolated writes (regression baseline)
//!   and (b) concurrent mixed writes across ALL planes on the SAME shard
//!   (`tests/crash_matrix_cross_plane/mixed.rs` — the genuinely new
//!   coverage; no existing suite does this).
//! - **Kill point:** after a synced marker at a deterministic or randomized
//!   offset in the write stream (`kill_after_marker`, the baseline pattern
//!   from `crash_recovery_graph_durability.rs`); mid-checkpoint-Finalize
//!   (`kill_mid_checkpoint` — see the honesty note below); mid-Pass-C-recycle
//!   (`kill_mid_pass_c`, tiny `--wal-segment-size`/`--max-wal-size`); mid a
//!   cross-store `MULTI` (`kill_mid_txn` — kill before `EXEC`, proving
//!   atomicity: nothing queued must apply); genuinely-concurrent mid-burst
//!   (`mixed::start_concurrent_burst`, no synchronization at all).
//!
//!   **Honesty note on `kill_mid_checkpoint`:** this codebase has no
//!   fault-injection hooks for "kill exactly at Finalize step N" (verified —
//!   `src/shard/persistence_tick.rs`'s `CheckpointAction::Finalize` arm has
//!   no test-only interrupt points). What this suite does instead is
//!   probabilistic: trigger `BGSAVE`, then kill after a short randomized
//!   sleep drawn across the window a small checkpoint typically completes
//!   in. Repeated / soaked runs (`MOON_CRASH_MATRIX_ITERS`) sample different
//!   points in that window; a single run is NOT a proof any specific
//!   Finalize step (1-6) was hit. This is a real coverage gap relative to
//!   the brief's phrasing ("each individually") — flagged explicitly rather
//!   than silently claimed.
//!
//! - **Config:** 8 cells = `appendonly {yes,no}` × `disk-offload
//!   {enable,disable}` × `shards {1,4}`. Curated per the brief: full
//!   plane×workload matrix on the 2 production cells (`PROD_S1`, `PROD_S4`)
//!   and the 2 legacy cells at shards=1 only (`LEGACY_YES_S1`,
//!   `LEGACY_NO_S1`); 1-2 representative spot-checks on the remaining 4
//!   cells (`tests_spot.rs`). `appendonly=no` cells have NO durability
//!   contract on ANY plane (not just KV — `persistence_dir`, and therefore
//!   the WAL writer, is only constructed when `appendonly=="yes" ||
//!   save.is_some()`), so those cells assert liveness/no-corruption only.
//!
//! # RED/GREEN cell inventory
//!
//! Final state after 7 full runs on the Linux VM (`orb run -m moon-dev`,
//! ELF release binary) and three coordinator review rounds that caught 12
//! harness bugs total (wrong durability-log-per-plane assumptions,
//! `VALID_AT` syntax, internal-node-id-vs-property-value comparison, a
//! double-firing `BGSAVE` assertion, an invalid `FT.SEARCH` no-corruption
//! probe, an ineffective `#[ignore = "RED: ..."]` gating convention, a
//! vacuous kv-spill filler, a shared `red_guard` masking an unrelated GREEN
//! atomicity claim, a missing `GRAPH.CREATE` sync-wait in the split-out
//! atomicity check, a substring-based benign-error allowlist) — see git
//! history for the fix-by-fix trail. Kernel M3 stage 2 (K2, task #53) then
//! root-caused and fixed former RED cell 4 (checkpoint-Finalize graph total
//! loss), flipping its 2 cells GREEN. A K2 adversarial review round then
//! found and fixed a second graph-durability P0 (drop-resurrection across
//! repeated checkpoints — an empty-but-dirty graph store's checkpoint save
//! was wrongly short-circuited) and added 2 new regression cells,
//! `cross_plane_prod_s1_graph_drop_survives_repeated_checkpoints` /
//! `cross_plane_prod_s4_graph_drop_survives_repeated_checkpoints`, both
//! GREEN by default (RED-first verified against the pre-fix code before
//! the fix landed). **42 cells total: 33 GREEN by default, 9 RED.**
//!
//! RED cells are gated by `harness::red_guard` — NOT by
//! `#[ignore = "RED: ..."]` alone, because this suite's own execution
//! convention is `cargo test ... --ignored` (every cell needs a release
//! binary), and `--ignored` RUNS every ignored test regardless of its
//! reason string. `red_guard` early-returns (printing `SKIP: RED cell
//! (...)`) unless `MOON_CRASH_MATRIX_RED=1` is set, so the default
//! `--ignored` run is green-only (a clean tripwire for regressions) while
//! every RED reproduction stays one env var away on demand:
//!
//! ```text
//! cargo test --release --test crash_matrix_cross_plane -- --ignored --test-threads=1              # green-only (default)
//! MOON_CRASH_MATRIX_RED=1 cargo test --release --test crash_matrix_cross_plane -- --ignored --test-threads=1  # RED cells run too
//! ```
//!
//! **RED cell 1 — legacy-mode graph reconstruction (one root cause, 6
//! cells):** `cross_plane_legacy_yes_s1_graph_isolated`,
//! `cross_plane_legacy_yes_s1_txn_isolated_committed`,
//! `cross_plane_legacy_yes_s1_txn_isolated_atomicity`,
//! `cross_plane_legacy_yes_s1_mixed_all_planes_synced`,
//! `cross_plane_legacy_yes_s1_mixed_all_planes_mid_pass_c`, plus the
//! shard-count spot-check `cross_plane_spot_legacy_s4_graph_isolated` (6
//! cells, one cause). PR #288's own changelog: graph command replay from
//! WAL v3 does not reconstruct the graph in legacy (`--disk-offload
//! disable`) mode even with zero Pass C interference — a pre-existing gap
//! unrelated to WAL recycling, deliberately not asserted by
//! `tests/crash_recovery_wal_recycle_legacy.rs` either. Every graph read
//! post-restart errors `ERR graph not found`. Not this stage's job to fix.
//!
//! **RED cell 2 — MQ resurrection via generic DEL:**
//! `cross_plane_seeded_red_mq_generic_del_resurrection`. PR #291's
//! changelog: durable MQ streams deleted via generic `DEL` are resurrected
//! — with full content — by MQ WAL replay after a restart; there is no MQ
//! tombstone record (same bug class as the already-fixed KV/vector
//! cold-plane resurrection, PR #257, not yet applied to MQ). Kernel M3
//! brief §1.4's WS half of this finding turned out GREEN —
//! `cross_plane_seeded_red_ws_drop_durability` passes cleanly (WorkspaceDrop
//! WAL records DO replay in order); it stays an ordinary `#[ignore]` (needs
//! a release binary), not RED.
//!
//! **RED cell 3 — cross-store TXN graph leg not durable, task #52 (NEW, 2
//! cells):** `cross_plane_prod_s1_txn_isolated_committed`,
//! `cross_plane_prod_s4_txn_isolated_committed`. A committed cross-store
//! transaction (`SET` + `GRAPH.ADDNODE` in one `MULTI`/`EXEC`) survives
//! kill-9 on the KV leg (PR #247's `persist_txn_aof`) but the graph leg is
//! lost. Reviewed and confirmed NOT a harness artifact; confirmed
//! deterministic 3× consecutive at both shard counts after a task #52
//! hardening pass (own crash cycle per round, `--checkpoint-timeout 3600`,
//! kill immediately after the sync-wait — see
//! `scenarios::txn_isolated_committed`'s doc for the full analysis and
//! minimal repro). New P1 finding for kernel M3. The SIBLING atomicity
//! claim (`cross_plane_prod_s1_txn_isolated_atomicity` /
//! `_s4_txn_isolated_atomicity` — a transaction queued but killed BEFORE
//! `EXEC` must apply NOTHING) is a genuinely different, unrelated claim and
//! is GREEN on these two configs (review round 3, P1: it used to share this
//! same `red_guard`, incorrectly hiding a working regression tripwire by
//! default — split into its own ungated `#[test]` per config). It stays RED
//! on `legacy_yes_s1` only, folded into RED cell 1 above (same
//! legacy-graph-reconstruction root cause — confirmed on the VM, not
//! assumed: `GRAPH.CREATE` itself never survives a restart there, so the
//! atomicity check's own graph-leg assertion errors "graph not found"
//! before it can ever evaluate atomicity).
//!
//! **FIXED (was RED cell 4) — checkpoint-Finalize window total-losses the
//! graph plane, kernel M3 stage 2 / K2, task #53 (2 cells, now GREEN):**
//! `cross_plane_prod_s1_mixed_all_planes_mid_checkpoint`,
//! `cross_plane_prod_s4_mixed_all_planes_mid_checkpoint`. Found via
//! `MOON_CRASH_MATRIX_ITERS` soak, NOT the default single-iteration run —
//! this was a PROBABILISTIC finding (some, not all, kill offsets in the
//! 0-150ms post-`BGSAVE` window triggered it — confirmed absent in a
//! default `MOON_CRASH_MATRIX_RED=1`/`ITERS=1` full-suite run, exactly as
//! this caution note warns). Some offsets caused TOTAL loss of the graph
//! plane's content — not the unsynced tail, the FULL `MixedPlan::default()`
//! batch that was already confirmed durable via a wal-v3 sync-wait BEFORE
//! `BGSAVE` was even issued — while KV, vector, WS, and MQ all survived in
//! the same run. Root cause: `save_graph_store`
//! (`src/graph/recovery.rs`) wrote the reference/floor
//! (`graph_metadata.json`, via `GraphStore::save_metadata`) BEFORE the
//! payload it claims durable (CSR segments + `manifest.json`) — an
//! ARIES-inverted ordering. A kill-9 landing between the two writes left a
//! fully-advanced floor pointing at a payload that never made it to disk,
//! so recovery trusted the floor and skipped WAL replay for records the
//! floor claimed were already covered. Fixed by reordering
//! `save_graph_store` to write CSR segments + `manifest.json` FIRST,
//! `store.save_metadata` LAST, and making `save_metadata` itself atomic
//! (temp+fsync+rename+dir-fsync via
//! `persistence::atomic::atomic_write_durable`). Safe against
//! double-replay: `graph::replay::node_present` checks both `write_buf`
//! and loaded CSR segments before re-inserting a WAL-logged node/edge, so
//! replaying a record whose payload DID make it to disk before the kill is
//! a no-op. See `scenarios::mixed_mid_checkpoint`'s doc for the full
//! scenario analysis. Confirmed fixed via `MOON_CRASH_MATRIX_RED=1
//! MOON_CRASH_MATRIX_ITERS=20` soak, 20/20 clean at both shard counts
//! (pre-fix baseline: prod_s1 hit at iteration 11/20, prod_s4 at iteration
//! 7/20) — these two tests now run ungated (green-only default suite).
//!
//! **Every other cell (33/42) is GREEN** — including, notably,
//! `kv_isolated`/`kv_spilled_isolated`/`vector_isolated` on ALL configs (KV
//! and vector-HSET durability via the AOF; `kv_spilled_isolated`'s filler
//! sizing was hardened in review round 3 — see
//! `scenarios::kv_spilled_isolated`'s doc — and now passes its own hard
//! `heap-*.mpf` precondition on both shard counts), `graph_isolated` on
//! both `prod_s1`/`prod_s4` (structural writes + GraphTemporal
//! `TEMPORAL.INVALIDATE`, incl. a VALID_AT positive control — checkpoint-backed
//! mode does NOT share legacy mode's gap), `mq_isolated` everywhere (PR
//! #291's effect-record fix holds), `txn_isolated_atomicity` on
//! `prod_s1`/`prod_s4` (queuing without `EXEC` then killing applies nothing
//! — verified 3× consecutive), `mixed_all_planes_synced`/`mid_pass_c` on
//! both production shard counts, `mixed_all_planes_mid_checkpoint` on both
//! production shard counts (kernel M3 stage 2 / K2 fix above — now
//! unconditionally GREEN, not just at the default single-iteration
//! sample), and `mixed_all_planes_concurrent_burst_no_corruption` on all 3
//! configs it runs on (no plane's on-disk structures were ever corrupted
//! badly enough to error post-restart, beyond the expected "schema object
//! never made it to disk before the kill" not-found case).
//!
//! # Soak cadence
//!
//! `MOON_CRASH_MATRIX_ITERS` (default `1`) — set to e.g. `100` to repeat the
//! curated cells' probabilistic kill points (`kill_mid_checkpoint`,
//! `kill_mid_pass_c`, `mixed::start_concurrent_burst`) that many times in a
//! loop inside the SAME `#[test]` fn, sampling more of the timing window
//! each rerun. No new CI workflow — this is a manual/on-demand env-gated
//! knob, run from a VM-local `--dir` (never `/Volumes/Games` directly — see
//! `gotcha_vm_diskfull_shared_volume` / `gotcha_vm_tmpfs_target_dirs_full`),
//! matching `scripts/bench-soak-7d.sh`'s local-only cadence (brief §Stage 1
//! cadence, Open Decision #4).
//!
//! # Running
//!
//! ```text
//! cargo build --release
//! cargo test --release --test crash_matrix_cross_plane -- --ignored --test-threads=1
//! MOON_CRASH_MATRIX_ITERS=100 cargo test --release --test crash_matrix_cross_plane \
//!   soaked -- --ignored --test-threads=1
//! ```
//!
//! `--test-threads=1`: every scenario spawns a real server process and
//! drives it with multiple threads/connections; running scenarios
//! concurrently multiplies port/tmpdir/VM-resource pressure for no benefit
//! (crash suites are I/O- and wall-clock-bound, not CPU-bound).

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

#[path = "crash_matrix_cross_plane/harness.rs"]
mod harness;
#[path = "crash_matrix_cross_plane/mixed.rs"]
mod mixed;
#[path = "crash_matrix_cross_plane/planes.rs"]
mod planes;
#[path = "crash_matrix_cross_plane/resp.rs"]
mod resp;
#[path = "crash_matrix_cross_plane/scenarios.rs"]
mod scenarios;
#[path = "crash_matrix_cross_plane/tests_legacy.rs"]
mod tests_legacy;
#[path = "crash_matrix_cross_plane/tests_prod.rs"]
mod tests_prod;
#[path = "crash_matrix_cross_plane/tests_seeded_red.rs"]
mod tests_seeded_red;
#[path = "crash_matrix_cross_plane/tests_spot.rs"]
mod tests_spot;

/// How many times a soak-eligible scenario repeats its probabilistic kill
/// window. `1` (default) = the normal PR-gated single pass; the brief's
/// ×100 soak is opt-in via this env var, never a hardcoded loop.
pub(crate) fn soak_iters() -> u32 {
    std::env::var("MOON_CRASH_MATRIX_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(1)
}

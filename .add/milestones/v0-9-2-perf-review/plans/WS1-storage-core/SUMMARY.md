# WS1-storage-core SUMMARY

> Committed by the orchestrator from the agent's final report (the harness refuses subagent
> writes of SUMMARY.md). Branch `perf/ws1-storage-core`, base `a925e64` (= `935c555` + plan):
> 13 commits, one issue each (`fixes` + `refs` follow-ups) plus the NOTES.md docs commit
> `8aae4ce`. Designs for everything deferred are in NOTES.md.

## Per-issue verdict
| issue | verdict | commits | evidence (test names, numbers) | follow-ups |
|---|---|---|---|---|
| moon#1159 DashTable H2 | **FIXED** | `5147640` H2 from bits 32..=38 · `1b07586` in-place split · `229fe08` slice-keyed upsert | `h2_fingerprint_keeps_key_compares_near_one_per_hit` (test-only compare counter, 100K keys): per hit **6.675 → 1.045**, per miss **20.454 → 0.162** (fails with the top-7-bit H2). `h2_fingerprint_is_independent_of_the_directory_bits`: 300 keys sharing the top 10 bits had **1** distinct H2 → ≥ 90. `split_moves_only_the_upper_half_and_keeps_stayers_in_place` (fails on the old split). `insert_or_update_slice_builds_the_key_only_on_a_miss`, `db::ws1_tests::set_overwrite_builds_no_owned_key`: 0 key builds per overwrite. Release A/B (1M keys, `--shards 1`, P16, 7 interleaved pairs): GET **+11–17%** (median pair +14%), SET **+36%**, 40-byte-key SET **+25–30%**. Dashtable `unsafe {}` blocks 63 → 55; `from_raw_parts` removed. | Re-measure on GCE; the `dashtable_probe` 40-byte variant is compile-checked only |
| moon#1161 reads never touch LRU/LFU | **FIXED** | `dfe438d` · `62b3d11` (cross-ownership) · `d593ebe` | Review scenario verbatim (release): hot-key retention **LRU 0.3% → 99.5%, LFU 0.1% → 100%** (redis 100/100). `tests/perf_ws1_lru_hit_ratio.rs`: baseline 0.3/0.2% (fails) → 99.1–99.4/100%. Clock-stepped `ws1_tests::hit_ratio_1161`: LRU 96.2%, LFU 98.2%; control without read tracking (= HEAD) 0.0%. Parity: IDLETIME after `SET; sleep 3; GET` **3 → 0** (redis 0); TTL/TYPE/EXISTS/OBJECT don't reset it, TOUCH does; FREQ after 200 GETs **5 → 12** (redis 10), survives SET; OBJECT on a missing key `ERR no such key` → nil. Same-binary GET P16: noeviction 628K / LRU 627K / LFU 617K rps (within noise). Entry stays 32 B (compile-time assertion). Also fixes moon#1211 (inert LFU params; NOTOUCH introspection). | Kept divergences: FREQ under a non-LFU policy and IDLETIME under LFU answer values where redis errors; under noeviction GET does not reset IDLETIME |
| moon#1190 large removals stall the shard | **PARTIAL** | `4372744` · `e79b203` (cross-ownership) · `123c9f0` · `25eccd6` | 1M-field UNLINK, same-shard PING: baseline reply 105–173 ms / worst PING 102–170 ms → reply **0.25–0.52 ms**, worst PING **6–11 ms** (the last stall — one 167 MB madvise, 29 ms on `shard-0` per strace — now runs on `moon-lazyfree`). `tests/perf_ws1_unlink_latency.rs` fails on baseline (UNLINK 1.5 ms vs DEL 157 ms after). Expiring 100×100K-field hashes: PING max **572 → 14.6 ms**. `ws1_tests::lazy_free_1190`: 7 value kinds; freed bytes return `used_memory` exactly to the pre-SET value. | DEFERRED: O(1) `entry_overhead` (~90 accounting call sites in WS2/WS3 files) and FLUSHALL ASYNC (async flag through 8+ call sites). DEL, overwrite and eviction still walk the value synchronously (eviction defers only the drop) |
| moon#1189 expiry index | **FIXED** (quick wins) · structural index **DEFERRED** | `dc5b91b` · `c7cf73e` | `sweep_costs_one_table_probe_per_expired_key`: **2000 → 1000** probes for 1000 expired keys (fails with the old two-probe sweep). Sweep pops the head pair (no key clone), looks up by borrowed `(ts, &[u8])` (no `CompactKey` build), removes in one probe via `remove_if` (no new unsafe). Keys removed in 10 s at the same budget **72K/87K → 123K/138K**. | `(deadline, key_hash)` index designed in NOTES.md — needs an RSS measurement and a hasher hook to force collisions |

## Measurements
Host: 4-vCPU x86_64 container shared with 6 agents (load 3–5) — relative only; control/treatment interleaved; `--shards 1 --appendonly no --disk-offload disable --maxmemory 0` unless noted. Binaries: `baseline-935c555`; `ws1-1159` (through `229fe08` + the then-unused AtomicU32 entry change — relaxed atomics compile to plain loads/stores); `ws1-final` = `c7cf73e`; `ws1-debug-shell` (dev build of `25eccd6`).
- #1159 session 1: GET 624/529/516K vs 722/763/475K; SET 572/487/451K vs 723/769/557K; SET40 451/410/370K vs 501/523/384K.
- #1159 session 2: GET 622/597/552/485K vs 545/697/610/661K; SET 517/536/453/425K vs 562/726/667/695K; SET40 420/423/374/351K vs 440/546/490/486K.
- #1161 read-recording cost: noeviction 724/630/531K, LRU 595/656/630K, LFU 604/647/602K.
- Expiring 100×100K-field hashes, final: p50 0.121 ms, p99 1.96 ms, max 14.6 ms (baseline p50 0.076, p99 1.27, max 572 ms).
- Gates: `cargo test --lib` 6053 passed / 1 failed (root-only `cold_index_rebuild_tests::unreadable_file…`, not a WS1 file, fails before and after); tokio storage/expiry/key/config lib tests 933 passed / same 1 failed; fmt, clippy ×2, tokio check, `--tests --bench dashtable_probe` clean; unsafe audit 0 missing SAFETY comments, 244 blocks, no new unsafe.

## Cross-ownership edits
- `62b3d11`: `command/config.rs` + `main.rs` call `publish_lfu_params`; `command/server_admin.rs` (MEMORY USAGE, DEBUG OBJECT) and `command/debug_digest.rs` use NOTOUCH `peek*`; `server/conn/blocking.rs` comment only.
- `e79b203`: `shard/event_loop.rs` gains two `drain_lazy_free_tick` calls.
- `key.rs` beyond UNLINK/OBJECT/TOUCH: TTL/PTTL/EXPIRETIME/PEXPIRETIME/TYPE/KEYS/SCAN (+ readonly twins) use `peek`.

## Risks / things the orchestrator must re-check at integration
1. `CompactEntry.metadata` is a private `AtomicU32` with a manual `Clone` — direct `entry.metadata` readers elsewhere no longer compile.
2. Read accessors record LRU/LFU access when the policy is LRU/LFU; metadata/admin/whole-keyspace readers must use `peek*`.
3. OBJECT on a missing key answers nil (redis parity) — re-run `scripts/test-commands.sh` / `scripts/test-consistency.sh` on the merged binary (not run by WS1).
4. Large UNLINKed/expired values stay in `used_memory` until the drain frees them (~0.25 s per 1M elements; ~10× slower while the shard is idle-parked).
5. New lazily started `moon-lazyfree` thread (values ≥ 65,536 elements only).
6. Under tokio, UNLINK no longer uses `spawn_blocking`; both runtimes use the queue.
7. `perf_ws1_*` tests spawn real servers (pin `MOON_BIN`); the UNLINK test uses ~300 MB transiently.
8. One root-only test fails before and after (named above).
9. Measured binaries were copied under `/home/user/wt/bin` and symbol-checked; passing integration tests were re-run with `MOON_BIN` pinned to them.

## Self-evaluation (0–1)
Completeness 0.88 (O(1) memory counter + FLUSHALL ASYNC need WS2/WS3 call sites) · Clarity 0.92 · Practicality 0.93 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9

# WS9-scripting-pubsub SUMMARY

> Committed by the orchestrator from the agent's final report (the harness refuses subagent
> writes of SUMMARY.md). Branch `perf/ws9-scripting-pubsub`, base `f32546c`, 4 commits
> (3 fixes + NOTES.md `8157124`).

## Per-issue verdict
| issue | verdict | commits | evidence (test names, numbers) | follow-ups |
|---|---|---|---|---|
| moon#1167 EVAL/EVALSHA recompile per call | **FIXED** | a40d9e3 | unit: `evalsha_reuses_one_compiled_function`, `script_flush_clears_compiled_functions`, `compiled_cache_is_lru_bounded_but_source_is_not`, `cached_function_has_fresh_locals_and_keys_argv_per_call`, `cached_eval_path_still_enforces_acl`; integration: `script_key_routing` (21), `acl_script_key_enforcement` (5). A/B: EVALSHA 1-line **116.7K → 266.2K rps (2.28×)**; EVALSHA 1.4 KB rate limiter **19.7K → 99.2K rps (5.02×)** | `--shards > 1`: double sha + 3× `parse_eval_args` in `server/conn/shared.rs` (WS7's territory) |
| moon#1180 PUBLISH clones every subscriber | **FIXED** (mechanism; wall-clock within noise) | 9b43cb4 | `Arc<[Subscriber]>` copy-on-write: the publish snapshot is 1 Arc clone per channel + 1 per matching pattern (was N flume-Sender clone+drop ≈ 4N atomics + a heap alloc). `pubsub_resp3_push` (3), `pubsub_burst_delivery` (2), `keyspace_notifications` (11, incl. cross-shard), `pubsub::` unit suite | a criterion bench of `publish_shared` with pre-filled bounded channels to isolate the snapshot cost below `try_send` |
| moon#1214 item 2 keyspace alloc without a subscriber | **FIXED** | 3023302 | unit: `keyspace_subscription_classifier_never_undercounts`, `listener_count_is_balanced_and_saturating`; integration: `perf_ws9_keyspace_listener` (late psubscribe, churn never negative, cross-shard). A/B: SET + `notify-keyspace-events KEA`, no subscriber: **453.9K → 696.1K rps (1.53×)**; KEA overhead vs SET-off **−25.1% → −1.9%** (noise) | #1214 items 1 and 3 (SPSC notify lock, `Database::get` double probe) are WS8/WS10 |

## What changed
- **#1167** `src/scripting/cache.rs`, `src/scripting/mod.rs`: a per-shard bounded LRU (`[u8;40]` → `mlua::Function`, cap 1024) beside the unbounded, redis-parity source map. `compile_user_script` reproduces `Chunk::eval`'s expression-first/statement mode choice exactly, so a cached function behaves byte-identically to HEAD's `.eval()` — sandbox globals, per-call KEYS/ARGV, `@user_script` error text and `redis.call` ACL are unchanged, and the load mode is HEAD's. EVALSHA lowercases into a stack `[u8;40]`, looks the source up zero-copy and parses numkeys/keys/argv straight from `args[1..]`; EVAL computes the sha once. SCRIPT FLUSH (incl. ASYNC/SYNC) clears both maps. `resident_bytes` is O(1) (running counter instead of a per-100 ms walk).
- **#1180** `src/pubsub/mod.rs`: channels, patterns and shard channels store `Arc<[Subscriber]>`, rebuilt copy-on-write on subscribe / unsubscribe / slow-subscriber removal. Delivery order, slow-drop eviction, RESP3 push, sharded pubsub and keyspace delivery are unchanged.
- **#1214** `src/notify.rs` + `src/pubsub/mod.rs`: a lock-free process-global `KEYSPACE_LISTENERS` count, moved at each keyspace-relevant channel/pattern present↔absent transition (saturating, never negative). `notify_keyspace_event` returns before allocating when the count is 0 (one extra Relaxed load per write). The classifier is conservative (never undercounts); sharded channels are excluded.

## Measurements
4-vCPU x86_64 Linux container under heavy build contention. `/home/user/wt/bin/ws9-scripting-pubsub`
(release-fast; the first build was an aliased 0.24 s no-op — caught by binary size and rebuilt) vs
`/home/user/wt/bin/baseline-935c555`, oracle redis-server 7.0.15. `--shards 1`,
`redis-benchmark -P 16 -c 50 -r 100000`, base/ws9 interleaved, 3 reps, medians. Noise floor: GET ±8%,
PUBLISH@1000 ±30%.

| rps (median of 3) | GET | EVALSHA 1-line | EVALSHA 1.4 KB RL |
|---|---|---|---|
| baseline | 914,634 | 116,686 | 19,747 |
| ws9 | 964,630 | 266,193 | 99,206 |
| redis | 828,729 | 354,610 | 126,263 |
| ws9 / base | ~1.0× | 2.28× | 5.02× |
| ws9 / redis | 1.16× | 0.75× | 0.79× |

| SET + keyspace events, no subscriber | SET (KEA, no sub) | SET (off) | KEA overhead |
|---|---|---|---|
| base | 453,858 | 606,061 | −25.1% |
| ws9 | 696,056 | 709,220 | −1.9% (noise) |

PUBLISH (C-client publisher, N background subscribers): N=1 638,978 → 584,795; N=100 550,964 →
597,015; N=1000 341,297 → 323,102 — within run-to-run variance. Throughput here is bound by the
per-subscriber flume `try_send` both builds pay, so the snapshot saving does not rise above ±30%
noise on this box (the caveat #1180 itself states).

## Cross-ownership edits
None. `handle_eval`/`handle_evalsha` and `parse_eval_args` signatures are unchanged, so no call site
(dispatch.rs, spsc_handler.rs, handler_sharded, txn_script, server/conn/shared.rs) was edited;
`src/acl/**` untouched.

## Risks / things the orchestrator must re-check at integration
1. `pubsub/mod.rs` is touched by both #1180 (9b43cb4, Arc COW only; compiles and passes on its own — the fix to the pre-existing test helper `assert_no_missing_reverse_entries` is folded in) and #1214 (3023302, counting hooks). `notify.rs` only in 3023302.
2. `ScriptCache` holds `mlua::Function`s tied to the shard's single Lua VM: 1:1 for the shard's lifetime, cleared by SCRIPT FLUSH, LRU-capped at 1024, never sent across threads.
3. `KEYSPACE_LISTENERS` is process-global and transition-tracked with a saturating decrement; churn and cross-shard integration tests cover balance.
4. Tokio leg verified with `clippy --lib --test perf_ws9_keyspace_listener --features runtime-tokio` (rc=0); full tokio `--all-targets` was not run to completion under contention. WS9 adds no benches and no graph/text-index-dependent targets.
5. On base `f32546c`, `clippy --all-targets` shows one pre-existing error (`useless_vec` in `tests/perf_ws6_aof_record_alloc.rs`), already fixed on PR #1221's head.

## Gates
`cargo fmt`; `clippy --lib --test perf_ws9_keyspace_listener -D warnings` (default and tokio);
`clippy --all-targets -D warnings` (default; clean apart from item 5); `cargo check --lib --tests`;
no new `unsafe`. Unit and integration suites above green on both runtimes (45+ integration tests);
redis-cli parity spot-checked for EVAL/EVALSHA/SCRIPT and error texts against redis 7.0.15.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.93 · Practicality 0.95 · Optimization 0.90 · Edge cases 0.93 ·
Self-evaluation 0.92 — #1180's wall-clock win is not measurable above this box's noise and is
reported as such; mechanism and semantics are proven.

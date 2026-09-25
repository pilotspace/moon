# WS18-parity-residuals SUMMARY

- **Branch:** `perf/ws18-parity-residuals`.
- **Base:** `int/part3b` @ `d155cd6`, plus the plan commit `81fb3e9`.
- **Personas:** routing-dispatch-engineer (lead), performance-engineer, ci-test-integrity-engineer.
- **Oracle:** redis-server 7.0.15. Every parity expectation below was captured from it with the same bytes.
- **Binaries.** Red runs used `baseline-ae21476` and `ws18-dbg-base-d155cd6`. Green runs used, in `/home/user/wt/bin/`:
  - `ws18-dbg-*`, a debug build per item;
  - `ws18-dbg-final` and `ws18-dbg-tokio-final`, debug builds at `c3ec01d`;
  - `ws18-rf1`, a release-fast build at `c3ec01d`.
- **Environment:** this box is below moon's 5% `--disk-free-min-pct` default, so every server ran with `--disk-free-min-pct 0`.
- **NOTES.md** holds the mechanisms, designs, risks and per-item self-scores.
- **Authorship:** the orchestrator committed this file, because the harness refused the subagent's write (TEAM-RULES §6). The content is the agent's final report.

## Per-issue verdict

| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1234: DEL/UNLINK/GETDEL never emit `del` | FIXED | d64a900 | `tests/perf_ws18_del_notify.rs`, at `--shards 1` and `--shards 4`.<br>Covers plain, spanning, absent and repeated keys; UNLINK of lists; GETDEL on a hit, a wrong type and a miss (`keymiss`); MULTI; Lua; class filtering.<br>Red on `ae21476` and on `d155cd6`: "DEL keyevent: got []".<br>Script rows: 4 in test-consistency and 3 in test-commands. They match redis at shards 1 and 4, and all 4 consistency rows differ on `ae21476`. | Other keyspace events are still missing: `expire` on SET PX, `del` when a pop empties a collection, `lpush`/`hset`/… |
| moon#1235: LOAD racing FLUSH leaves shards disagreeing | FIXED | 365082f (cross-ownership refactor), 0724c1b, cadec43, 96436b6 | See "moon#1235 evidence" below. | The plan's shard-0 sequencer was not built. Flush epochs (for the script cache) plus a single order token (for the function registry) give the same guarantee; NOTES.md explains the choice. |
| moon#567: NOSCRIPT after SCRIPT LOAD | FIXED | 023fb99 | Test: `script_load_partial_fanout_is_reported_not_swallowed`.<br>Red on `ae21476` and on `d155cd6`: the reply was the sha (`$40\r\n25c2…`).<br>Green: `-MOONERR partialfanout SCRIPT LOAD applied on 1 of 4 shards…`. | Self-healing EVALSHA was not done; NOTES.md says why. |
| moon#1226, lists: WATCH parity | FIXED | 1d96a35 | Test: `a_no_op_lrem_or_linsert_does_not_bump_the_watch_version` (red: the version went 2→5).<br>Live against redis: a no-op LREM or LINSERT lets EXEC run; a real LREM aborts it. | — |
| moon#1226, lists: pop_back_n and docs | FIXED | 867ab91 | Test: `rpop_and_lmpop_with_a_count_are_one_cut_on_a_listpack`.<br>Red: 100 head seeks for `RPOP q 100`. Green: 0. | — |
| moon#1226, lists: SRANDMEMBER with a negative count | FIXED | 30aeaca | Test: `negative_count_on_a_listpack_copies_each_member_once`.<br>Red: 20,000 member buffers for 100 members. Green: ≤ 100. | — |
| moon#1226, lists: moon#1209 rows | FIXED | 59f6b89, c3ec01d | Added the missing `LPOS … COUNT -1` and `RANK -1 COUNT 0` rows, plus counted-pop and SRANDMEMBER rows. All match redis. | — |
| moon#1226, storage: per-shard lazy-free counter | FIXED | 8fdf92a, 741d4a7 | Test: `an_idle_shard_takes_no_guard_while_another_shard_drains`.<br>Red: 1,008 write guards over 63 idle ticks. Green: 0.<br>Test: `the_first_database_drained_rotates`. Red: both ticks started at db 0. | — |
| moon#1226, storage: dashtable split test and file size | FIXED | f657565, cceaaa9 | Split tests (3): 2 of the 3 fail when the flag is cleared.<br>`dashtable/mod.rs` went from 2,020 to 970 lines by a verbatim move. | — |
| moon#1226, pub/sub | FIXED | 53e2977, 0d57817, 73c8c57 | The listener count uses Release/Acquire.<br>The monoio lib run of `pubsub::tests` went from 0 to 27.<br>`pubsub/mod.rs` went from 1,716 to 1,025 lines. | — |
| moon#1226, protocol: deferred commands run before a fault | FIXED | c12b1eb | `tests/perf_ws18_proto_fault_defer.rs`, at shards 1 and 4, both runtimes. Red: the deferred SET's `+OK` was missing.<br>Also fixed:<br>• monoio's inline replies were dropped before a fault;<br>• RESP2 subscriber mode closed without an error. | — |
| moon#1226, persistence: tokio AOF BufWriter | FIXED | 77b0ee7 | The writer held 8,388,608 bytes; now ≤ 2 MiB, with the same number of hops. | — |
| moon#1226, test harness: MOON_BIN | FIXED | 136b8b1 | Four suites now resolve the binary through `common::find_moon_binary`. | — |

### moon#1235 evidence
`tests/perf_ws18_script_order.rs` runs 200 barrier-released trials per test, then EVALSHA/FCALL on one key per shard and SCRIPT EXISTS from 16 connections. Mixed trials out of 200:

| test | `d155cd6` | `ae21476` | fix |
|---|---|---|---|
| SCRIPT, `--shards 2` | 86 | 16 | 0 |
| SCRIPT, `--shards 4` | 173 | 17 | 0 |
| FUNCTION, `--shards 2` | 42 | 91 | 0 |
| FUNCTION, `--shards 4` | 167 | 104 | 0 |

## Measurements
DEL hot path:
- `redis-benchmark -n 1M -r 1M -P16 -c8 DEL k:__rand_int__`, `--shards 1`.
- 6 alternating reps against `baseline-ae21476`.

| | baseline median | ws18 median |
|---|---|---|
| notifications off | 616K rps | 615K rps |
| KEA, no listener | 594K rps | 624K rps |

- There is no measurable regression within ±5%.
- The baseline is main, so this is not a pure control for this one change.
- The complexity and cost claims rest on the deterministic tests above.

## Cross-ownership edits
- **365082f:** the SCRIPT/FUNCTION arms of three handler files now call one shared body. No behaviour change.
- **8fdf92a:** `storage/db/lazy_free.rs`, plus one re-export line in `storage/db/mod.rs`.
- **c12b1eb:** `handler_sharded/pubsub.rs`, the parse-error arm only.
- **d64a900:** `command/string/string_read.rs`, the GETDEL body only.
- **New files:**
  - `persistence/aof/writer_task/buf_tests.rs`;
  - `server/conn/script_fanout.rs`, a verbatim move out of `shared.rs`.

## Risks / things to re-check at integration
1. **ShardMessage shape changed.** `ScriptLoad` is now `{script, epoch, ack}`, and `ScriptFlush` gained `{epoch}`.
2. **Process-global function-order token.** FUNCTION mutations are serialized and wait up to 2 s. On timeout they answer `MOONERR partialfanout FUNCTION not applied …`.
3. **Behaviour change.** When a SCRIPT LOAD fan-out gives up, the reply is `MOONERR partialfanout` instead of the sha. This only happens with a wedged mesh.
4. **Lazy-free hint.** A queue filled from a foreign thread gets its first drain within 64 ticks. No production caller does that today.
5. **Merge surfaces:**
   - the protocol-fault regions of both handlers;
   - the ScriptLoad/ScriptFlush arms of `spsc_handler.rs`;
   - the move out of `shared.rs`;
   - `lazy_free.rs`;
   - the new script rows.
6. **Reds that already exist on `int/part3b`:**
   - `tests/list_pop_alloc_942.rs`: the full-encoding control expects 0 allocations and sees 50, because of the moon#1160 exact-size copy from WS10 `783beb8`.
   - The stream_effect `a_read_is_one_forced_claim_per_stream_that_replays_exactly` wall-clock race. WS17's moon#1222 fix covers it once merged.
7. **Files still over 1,500 lines** (all were already over on the base): `shared.rs` (6,388), `handler_monoio/mod.rs` (5,109), `spsc_handler.rs` (4,891), and others. Now under the limit: `dashtable/mod.rs` (970), `pubsub/mod.rs` (1,025) and `scripting/mod.rs` (834).

## Gates at the last code commit c3ec01d
- **Lint and audits.** All clean:
  - `fmt --check`;
  - `audit-unsafe` and `audit-unwrap`;
  - `clippy --all-targets -D warnings`;
  - tokio clippy;
  - tokio `check --all-targets`.
- **Lib tests.**
  - monoio: 6545 passed, 2 failed (the known root-only test, and the stream_effect race from risk 6).
  - tokio: 5616 passed, 1 failed (the root-only test).
- **Integration, monoio,** with `MOON_BIN` pinned:
  - the WS18 suites;
  - the harness suites it touched;
  - perf_ws8_script_flush_fanout, function_in_multi_697, scripts_in_multi_894, perf_ws9_keyspace_listener, protocol_error_lifetime and perf_ws4_deferral_carry.

  All green except `list_pop_alloc_942` (risk 6).
- **Integration, tokio:** the WS18 suites, pipeline_cross_shard_ordering, script_function_fanout and protocol_error_lifetime are green.

## CHANGELOG bullets
- **Fixed:** DEL, UNLINK and GETDEL publish the `del` keyspace event (class g), once per key removed. This holds on every path: plain, spanning shards, MULTI/EXEC and Lua. GETDEL of a missing key publishes `keymiss` (moon#1234).
- **Fixed:** SCRIPT LOAD racing SCRIPT FLUSH, and FUNCTION LOAD racing FUNCTION FLUSH, from different shards no longer leave the shards disagreeing:
  - script-cache mutations carry flush epochs;
  - function-registry mutations are serialized.

  At `--shards 4`, mixed trials went from 173/200 (SCRIPT) and 167/200 (FUNCTION) to 0 (moon#1235).
- **Changed:** when a SCRIPT LOAD cannot reach every shard, it answers `-MOONERR partialfanout …` instead of a sha that some shards would answer NOSCRIPT for (moon#567).
- **Fixed:** an LREM that removes nothing, and an LINSERT with a missing pivot, no longer abort a WATCHing EXEC (moon#1226).
- **Fixed:** in a pipeline with a protocol error, these are now answered before the error, as in redis (moon#1226):
  - commands deferred behind a blocking pop, SUBSCRIBE or the cross-shard ordering guard;
  - fast-path GET/SET replies;
  - RESP2 subscriber mode, which now reports the error instead of closing silently.
- **Performance:** RPOP key n and LMPOP … RIGHT COUNT n cut a listpack's tail once, and a single RPOP steps back over one entry. SRANDMEMBER key -N on a listpack set copies each member once (moon#1226).
- **Performance:** the 1 ms lazy-free tick is gated per shard, and the first database drained rotates (moon#1226).
- **Performance:** each tokio AOF writer's buffer is 2 MiB instead of 8 MiB (moon#1226).
- **Internal** (moon#1226):
  - the pub/sub registry tests run on monoio;
  - the keyspace-listener count uses Release/Acquire;
  - four suites honour `MOON_BIN`;
  - `dashtable/mod.rs`, `pubsub/mod.rs`, `scripting/mod.rs` and `shared.rs` were shrunk by verbatim moves.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.92 · Practicality 0.93 · Optimization 0.92 · Edge cases 0.91 · Self-evaluation 0.9.

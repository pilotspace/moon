# WS40-fsync-agent SUMMARY

Wave 2, lane B. Branch `w2/ws40-fsync-agent`, base `bdb9486` (WS37). Linux container, not the merge bar.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1266 Option 3 | FIXED as specified (all three parts, both runtimes, s1 and s4). PARTIAL against the "0 lost" target: the window is narrowed, not closed | `cac8f9c` test, `8322bd1` tokio flush every batch, `201c682` fsync agent + INFO `aof_delayed_fsync` + loom model, `06c5f5b` warm poll then park, `bf00a43` move poll to `writer_task/poll.rs`, `7952784` `MOON_AOF_WARM_POLL_US` knob + env-knobs doc, `653010a` fmt, `52f6b00` test assertion, `2c28c57` + `e620e49` docs, `6d4699c` step 500 µs | `tests/aof_everysec_kill9_1266.rs` (10 tests), `tests/loom_aof_fsync_agent.rs`, unit tests in `fsync_agent.rs` (7) and `writer_task/poll.rs` (7). Numbers below | 1A (WS46) is needed to reach 0. Latency A/B needs a quiet window. Possible: tokio writers could `write(2)` synchronously on their dedicated thread instead of via `tokio::fs` |

## What changed
1. **The everysec fsync runs on an agent thread.**
   - `src/persistence/aof/fsync_agent.rs` adds one `aof-fsync-<idx>` thread per writer, spawned by the writer and joined on exit.
   - At the deadline the writer hands over a dup of its fd and goes straight back to its channel.
   - The agent records the fsync-latency metric and `record_everysec_fsync_result`, exactly as the inline fsync did.
   - A failed fsync is retried at the next deadline even with no new writes.
   - With no agent or a failed dup, the writer fsyncs inline as before; nothing is dropped.
   - The writer now hands off only when something was written (or the last fsync failed), so an idle writer no longer fsyncs every second.
2. **The hand-off state machine** is one atomic word in `src/persistence/aof/fsync_handoff.rs`: IDLE → IN_FLIGHT by writer CAS, back to IDLE by agent `finish` (outcome stored first) or writer `abort`.
   - At most one fsync is in flight per writer.
   - A deadline that finds one running is postponed and retried on the next wake. It is counted once per deadline in the new INFO field `aof_delayed_fsync`.
   - The write itself is never postponed (redis postpones the write).
   - `tests/loom_aof_fsync_agent.rs` compiles the real file under `--cfg loom`; 3 models pass.
   - Mutation check: storing IDLE before the outcome makes `loom_two_deadlines_first_fsync_fails` fail.
3. **Tokio: every non-`always` batch ends with `flush()`.** This also waits for `tokio::fs::File`'s in-flight write. It replaces the 8 KiB tail bound from moon#1187.
4. **Monoio poll (`writer_task/poll.rs`).**
   - WARM (the last receive got a message): poll every 500 µs for up to 5 ms. Producers still pay no futex.
   - Then, or when COLD: park in `recv_timeout`. The first record after idle is picked up at once instead of after up to 50 ms.
   - `always` still parks. `MOON_AOF_WARM_POLL_US` (10–50,000) overrides the step.
5. **`always` is unchanged.** Its per-batch fsync stays on the writer, before the acks.
6. **Test hooks.**
   - New, test-only: `MOON_TEST_AOF_SYNC_GATE=<path>` holds every AOF data fsync (the agent's and `always`'s) while the file exists.
   - `MOON_TEST_AOF_FSYNC_STALL_MS` still holds the WRITER at its deadline. It now models a writer blocked behind a slow disk, so the 769/838/1272/1294/ws15 suites keep their mechanism.
7. **File sizes.** `writer_task.rs` went from 2154 to 2079 lines; `pool.rs` is untouched. New modules: `fsync_agent.rs` (462), `fsync_handoff.rs` (133), `writer_task/poll.rs` (~260).

## Measurements

**Durability leg.** 10,000 acked SETs (unpipelined, or pipelined 100 deep), or 1 SET after 1.5 s idle; SIGKILL 1 ms after the last ack; restart; count missing. 20 reps per cell. Listed as acked-but-missing per rep.

Base `lane-b-ws37-*`:

| cell | monoio | tokio |
|---|---|---|
| lone s1 | 1 in 19/20 reps | 1 in 20/20 |
| lone s4 | 1 in 20/20 | 1 in 20/20 |
| pipelined s1 | [10000,0,3856,1500,3856,0,1200,100,1200,600,1100,100,10000,10000,700,300,0,900,360,1600] | [46,101,170,0,74,72,100,222,152,200,124,109,74,100,177,211,121,109,26,159] |
| pipelined s4 | [513,322,155,254,209,777,56,318,25,79,117,305,113,273,52,258,422,271,278,136] | [513,162,432,463,435,332,390,308,322,491,470,317,281,225,197,81,511,331,77,158] |
| unpipelined s1 | [32,59,12,44,37,0,14,9,0,30,0,40,30,0,0,34,43,0,0,0] | [140,174,50,71,208,125,229,204,20,6,72,211,198,247,162,8,214,1,34,197] |
| unpipelined s4 | [15,4,16,4,8,5,3,9,15,22,4,2,8,24,7,4,13,0,16,25] | [341,231,79,98,257,155,444,167,109,182,447,288,274,351,403,238,118,254,209,306] |

- 226 of 240 reps lost something. Median lossy rep: 1 to about 1,100 keys; worst 10,000 (all of them).
- Base does NOT already lose 0 on monoio. Its 3–50 ms poll step and inline fsync lose data on every shape.
- Tokio's losses on base come from the `BufWriter` tail plus the writer blocking on its inline fsync.

After the fix:

| run | lossy reps (non-zero values) | all other cells |
|---|---|---|
| monoio, 500 µs step (knob run on the same code as final) | 5/120: pipelined s1 400, 546, 18, 1100; unpipelined s1 3 | 0 |
| monoio, 100 µs step (earlier default) | 2/120: pipelined s1 800, 1900 | 0 |
| tokio (final binary) | 4/120: lone s4 1; pipelined s1 800; unpipelined s4 1, 1 | 0 |

- A second 20-rep tokio run (`lane-b-ws40-tokio` built at `bf00a43`, before the fmt/knob/test commits; tokio code identical) lost in 7/120: lone s4 1; pipelined s4 17, 19; unpipelined s1 40; unpipelined s4 2, 1. Base on the same 20-rep terms was 119/120. The 7/240 tokio figure in the lead combines both runs.
- The final monoio binary also passed the 20-rep suite in the gate rerun (per-rep counts not printed there).
- Every residual is a writer that was descheduled or whose `write(2)` stalled for more than 1 ms. Supporting evidence on this 4-vCPU host with other lanes building:
  - a plain `write(2)` has a max of 2–70 ms even with no fsync running;
  - an ack-to-file lag probe gave p50 0.35 ms and max 3–4 ms on the fix, versus p50 13 ms and max 28 ms on base.
- Option 3 cannot close this window; 1A can.

**Held-fsync test** (fsync gated for 2.5 s under write load):
- Every ack arrived within 4.5–14 ms; `aof_delayed_fsync` was at least 1.
- A kill -9 during the held fsync lost 0 (s1 and s4, both runtimes).
- `always` with the fsync held sends no reply until release (s1 and s4, both runtimes).

**Latency A/B: READY TO BENCH.**
- Script: `/tmp/claude-0/-home-user-moon/d1b785a6-84fa-5659-9361-52c93e4ab21f/scratchpad/bench_ws40.sh <reps> <shards> "<P:c:n …>" <out.csv>`. It interleaves fresh servers and records rps, p50, p99 and server core-µs/op. `BENCH_BINS="bin bin%500"` sets `MOON_AOF_WARM_POLL_US` per arm.
- Every run was at load 0.7–7, from other lanes plus the bench itself. The same binary varied up to 2× between reps, so the noise floor was ±20% or worse.
- Same-binary knob test, `--shards 1`, 3 reps, load 1.6–2.5, medians:
  - p1 c50: base 103.5K; 100 µs 93.6K (CPU/op +18%); 500 µs 107.7K; 3000 µs 103.2K;
  - p16 c50: all arms within noise; CPU/op base 1.66, 100 µs 1.85, 500 µs 1.77, 3000 µs 1.56.
  - Conclusion: the step drives the cost, and the fsync agent itself costs nothing measurable. That is why the default became 500 µs.
- Final-binary `--shards 1` grid, 3 reps, load rising 0.9 → 4.7:
  - p1 c1: inconclusive (−16% and +18% in different reps);
  - p1 c50: −12% in the quietest rep;
  - p16 c1: −7% to −14% in all 3 reps, CPU/op +10–24%. An isolation run right after was pure noise (194K–434K for the same binary), so this could not be pinned down;
  - p16 c50: within noise.
- `--shards 4` sanity: p1 c50 −2% to −11% (CPU/op +4–26%); p16 c50 within noise.
- Tokio `--shards 1`: ws40 is faster — p16 c50 132K → 241K, p16 c1 +30–130%, p1 c50 +20–60%. The likely cause is that the old inline fsync blocked the tokio writer and backpressured producers.
- Suggested quiet-window run: arms base, ws40, ws40%100, ws40%3000; cells 1:1, 1:50, 16:1, 16:50 at `--shards 1`, plus 1:50 and 16:50 at `--shards 4`.

## Gates (exit codes)
- `cargo fmt --check`: 0.
- `cargo clippy --all-targets -D warnings`: monoio 0, tokio 0.
- `cargo check --manifest-path fuzz/Cargo.toml --all-targets`: 0.
- `cargo test --lib persistence`: monoio 1015 passed, tokio 1014 passed.
- Integration, `--include-ignored`, `MOON_BIN` pinned, `--test-threads 1`. All exit 0 on both runtimes:
  - aof_everysec_kill9_1266, aof_replay_clock_1283 (11/11), aof_everysec_backpressure_769, wal_group_commit, aof_backpressure_reply_1272
  - crash_matrix_per_shard_aof, aof_multidb_kill9, perf_ws21_aof_drain, perf_ws21_aof_writer_start, script_write_fsync_barrier_831
  - default_config_aof_backpressure_838 (tokio runs 0 tests), aof_append_status_heals_on_rewrite, eviction_reason_del_run_budget_1294, perf_ws15_spill_withdraw_after_fold, aof_fsync_err_subscribe_ordering, loom smoke
  - Monoio was rerun in full on the final 500 µs binary, including kill9 at 20 reps.
- Loom under `--cfg loom`: 3/3 pass (last run on the final tree).
- Red proofs:
  - the durability leg on base binaries (above);
  - the two new poll unit tests fail with the old `wait/16` step: medians 30 ms and 3.0 ms against bounds of 10 ms and 1.5 ms;
  - the loom mutation above.
- None of the known failures from the brief were in scope, and none were hit.

## Cross-ownership edits
- `src/command/connection.rs`: one new INFO persistence field, `aof_delayed_fsync`, in commit `201c682`.
- Docs edited: `docs/production-guide.md`, `docs/PRODUCTION-CONTRACT.md` (everysec process-crash RPO row), `docs/internal/env-knobs.md`.

## Risks / things the orchestrator must re-check at integration
- **The kill9 suite's default assertion is the Option-3 property**: median rep 0, at most 25% of reps lossy. `MOON_1266_STRICT=1` asserts 0 in every rep — the bar for WS46/1A. On a heavily loaded host the 25% bar could flake; per-rep numbers are always printed.
- **Warm step 500 µs versus 100 µs** is a durability/throughput trade, and the throughput cost is still unconfirmed on a quiet host.
- **Semantic change:** an idle everysec writer no longer fsyncs every second (only when dirty or after a failure), and after a failed fsync the retry waits 1 s instead of retrying on every wake.
- **Test-hook semantic change:** `MOON_TEST_AOF_FSYNC_STALL_MS` still stalls the writer, but a real slow fsync no longer does. Suites that need to hold the fsync itself should use `MOON_TEST_AOF_SYNC_GATE`.
- **WS46 must keep** `EverysecSync` (the fsync agent) when the write moves to the shard thread, and remove the warm poll it no longer needs.

## README.md:280 replacement text (shared artifact, not edited)
Replace `everysec` p=16 is a **1.32× win** — with kill-9-lossless recovery. with:

> `everysec` p=16 is a **1.32× win**. Its fsync runs off the writer on a background thread (redis's model). An acknowledged write reaches the kernel page cache — which survives a `kill -9` — within the AOF writer's pickup latency: ≤ ~0.5 ms while writing, immediately after idle. A kill inside that window, or while the writer thread is stalled, can still lose the last acknowledged writes. Measured with SIGKILL 1 ms after the last ack, 20 reps per cell (`tests/aof_everysec_kill9_1266.rs`, 2026-09-30, 4-vCPU Linux container): losses in 9 of 240 reps, down from 226 of 240. See the production guide, "What a process crash can lose under `everysec`".

## CHANGELOG bullet
- **Fixed (persistence):** `appendfsync everysec` no longer loses most acknowledged writes to a process crash (moon#1266, Option 3).
  - The once-a-second fsync now runs on a per-writer agent thread (`aof-fsync-<n>`), so a slow disk no longer stops the AOF writer from draining acked writes into the file.
  - At most one fsync is in flight per writer. A postponed one is counted in the new INFO field `aof_delayed_fsync`. Unlike redis, the write is never postponed.
  - The tokio writer flushes every batch to the kernel; the 8 KiB user-space tail is gone.
  - The monoio writer polls every 500 µs while writes flow and parks when idle, so the first write after idle is picked up at once (it waited up to 50 ms).
  - Acked writes lost to a kill -9 1 ms after the last ack: 226 of 240 test reps before, 9 of 240 after. Closing the remaining sub-millisecond window is moon#1266 1A.
  - `appendfsync always` is unchanged: acks still follow the fsync.
  - New knob `MOON_AOF_WARM_POLL_US` (diagnostic). New test hook `MOON_TEST_AOF_SYNC_GATE`.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.8 · Edge cases 0.9 · Self-evaluation 0.9

Optimization is below 0.9 because the throughput cost could not be measured cleanly: every bench window had other lanes running, and the p16 c1 −7% to −14% signal on the final binary is unresolved. It needs the quiet-window run above.

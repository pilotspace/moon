# R2b2-fix-b SUMMARY — R2b round-2 A+B review fixes (moon#1322 F1; moon#1266 F2–F6)

Branch `w2/r2b2-fix-b`, base int-2b `88e2e98`. Commits: 9724ed4 (F1 fix), e6eaecf (F2+F5), 03c753a (F6), 92e57cb (F3 hook v1), e552523 (F1 suite), e8e3e05 (CHANGELOG correction), fb7eb92 (F3 hook reworked), f49d875 (F4 loom).

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| F1 MAJOR (moon#1322): cross-shard multi-key writes never barriered remote legs | FIXED | 9724ed4 e552523 e8e3e05 | `tests/cross_shard_write_barrier_1322.rs` (s4, `always`, one remote writer's fsync held by the gate; spanning MSET/DEL/UNLINK/BITOP/COPY with no local key + FLUSHALL from a fourth shard; no reply within 300 ms while held, each after release). Red on an F1-off mutant on monoio io_uring, epoll and tokio (replies in 0.2–5 ms); green on the fix in all 3. strace s4 below. | Admin console gateway (pre-existing; follow-up issue) |
| F2 MINOR: inline-SET gate and `try_send_append_durable` read policy before enqueue | FIXED | e6eaecf | Pinned by the F4 loom negative control and unit test `held_reply::a_held_lane_after_the_append_owes_a_barrier_once` (race not reproducible live) | — |
| F3 MINOR: boot test could not reach the boot hold | FIXED | 92e57cb fb7eb92 | Hook `MOON_TEST_AOF_WRITER_START_DELAY_MS` (writers sleep before first receive; `main` skips `await_hand_over`). Red on a mutant with `self.lanes[idx].hold()` removed on all three configs s1+s4 (every rep lost every acked key); green on the fix. A first "delay first offer" hook was insensitive (unheld writer warm-polls within ~0.5 ms) and replaced | — |
| F4 NIT: loom two-producer model | FIXED | f49d875 | `model_two_producers` (stale-hold producer) + negative control `loom_reading_the_hold_before_the_append_is_caught` | — |
| F5 NIT: inline gate used the pool-wide view | FIXED | e6eaecf | gate reads `fsync_policy_for(shard_id)` | — |
| F6 NIT: agent-less everysec writer rebuilt every wake | FIXED | 03c753a | unit test `an_everysec_writer_without_an_agent_is_not_rebuilt_every_wake` | — |
| F7 file sizes | skipped as instructed | — | pool.rs +2, writer_task.rs +0; new logic in new modules | — |

## Mechanism
- **F1** (`src/shard/coordinator/remote_barrier.rs`): after every remote leg replies, one `fsync_barrier(target)` per *written* remote shard (every key of MSET/MSETNX/DEL/UNLINK; the BITOP/COPY destination; reads write nothing), as `coordinate_swapdb` did. Runs in `coordinate_multi_key` before replying; a failed barrier turns a success into the barrier refusal. `coordinate_flush_broadcast` barriers every flushed leg (covers MULTI and script fan-outs) and takes the pool as a new parameter (6 call sites). Cost: one pool-wide Acquire load; only under `always` or a held lane are keys re-hashed (SmallVec, no heap up to 8 shards) and barriers sent, and each barrier re-checks its own target lane. Other cross-shard writers: single-key routed / single-owner multi-key already barrier; TXN and routed scripts barrier the owner; RENAME/SMOVE/*STORE/multi-shard scripts get `CROSSSLOT`; MGET/EXISTS/TOUCH and the vector broadcast write no AOF record. Not covered: the admin console gateway (runs commands as remote `Execute` with no pool in reach).
- **F2**: `try_send_append_durable` calls `fsync_barrier(shard)` after its send (no-op unless held or `always`). Inline SET: `aof::held_reply::note_after_append` re-reads the lane after the append and records a thread-local debt that the monoio handler pays (`take_owed` → `fsync_barrier`) right after the inline loop, before any reply in the batch leaves (same thread, no await between). If that barrier fails the connection is closed (the `+OK` is already buffered).
- **F6**: `EverysecSync` records the policy it was built for instead of inferring from `agent.is_some()`.
- **Test infra**: `MOON_TEST_AOF_SYNC_GATE_WRITERS=<n,...>` holds only writers `aof-writer-<n>` / `aof-fsync-<n>`.

## Measurements
stracecheck.py, s4, 8 conns × 40, 2 reps (1280 acks per cell; "≥6" = output cut at 6 lines):

| config | policy | mset | del | unlink |
|---|---|---|---|---|
| monoio epoll, fixed | always | 0 | 0 | 0 |
| monoio epoll, fixed | after-always | 0 | 0 | 0 |
| tokio, fixed | always | 0 | 0 | 0 |
| tokio, fixed | after-always | 0 | 0 | 0 |
| monoio epoll, F1-off mutant | always | ≥6 | 4 | ≥6 |
| monoio epoll, mutant | after-always | 0 | 0 | 1 |
| tokio, mutant | always | ≥6 | ≥6 | ≥6 |
| tokio, mutant | after-always | 0 | 0 | 0 |

Throughput: not benchmarked (READY TO BENCH). Steady state adds one Acquire load per multi-key write and per durable/inline append.

## Gates (Linux container, not merge bar)
- HEAD f49d875: `cargo fmt --check`; clippy `-j2 --all-targets -D warnings` monoio and tokio: pass.
- Tree before the final loom edit: fuzz check pass; `cargo test --release --lib -- persistence shard server::conn` monoio 1721, tokio 1658 passed.
- Loom (standalone `rustc --cfg loom`): 17 tests pass (9 models incl. 5 negative controls failing as required, 8 protocol unit tests) at bound 3 (6 s); the two new models also at bound 5 (349 s). Smoke `cargo test --test loom_aof_lane` 12/12.
- Integration (`MOON_BIN` pinned, `--include-ignored`), monoio io_uring and epoll all pass: aof_shard_write_1266 12, cross_shard_write_barrier_1322 1, aof_everysec_kill9_1266 10, aof_fsync_stall_r1 4, script_write_fsync_barrier_831 3, crash_matrix_per_shard_aof 4, txn_crash_atomicity_1300 39. tokio: all pass except six `txn_crash_atomicity_1300` replica tests (five known + `a_replica_of_a_multi_shard_master_keeps_same_id_txns_apart`), all failing identically on base `i2b-88e2e98-tokio` (no master-side PSYNC on tokio).
- Binaries `r2bfb2-v1-*` (e552523), `r2bfb2-v2-*` (fb7eb92 source), mutants `r2bfb2-mut-*`, `r2bfb2-mut2-*`; markers verified, mutants `cmp`-distinct.

## Cross-ownership edits
`src/shard/coordinator.rs` +21; handler_monoio / handler_sharded `{mod,write}.rs` + `shared.rs` (pool argument at six flush call sites; inline debt payment in handler_monoio/mod.rs); `src/server/conn/blocking.rs` (inline gate); `src/main.rs` (skip boot wait under the hook); `src/persistence/aof/{pool.rs,fsync_agent.rs,mod.rs,writer_task/lane_hooks.rs}`; new `src/persistence/aof/{held_reply.rs,lane_test_hook.rs}`, `src/shard/coordinator/remote_barrier.rs`; CHANGELOG line ~18791 correction.

## Risks
1. Admin console gateway writes under `always` still acked without fsync (pre-existing, operator plane) — follow-up issue.
2. Inline SET barrier failure closes the connection (raced F2 path only).
3. Under `always` every spanning write now pays one fsync per written remote shard (correct; more latency than before).
4. `coordinate_flush_broadcast` / `broadcast_txn_flushes` take a new trailing `aof_pool` parameter.

## CHANGELOG bullets
- Cross-shard writes confirm their remote legs before replying (moon#1322): spanning MSET/MSETNX/DEL/UNLINK/BITOP/COPY and the FLUSHALL/FLUSHDB broadcast (incl. script and MULTI fan-outs) fsync-barriered only the connection's own shard; under `appendfsync always` they were acked with no fsync of the remote shard's AOF (pre-existing, both runtimes), and with Option 1A the reply could leave before the remote record's write(2) while a lane was held. The coordinator now barriers each written remote shard before the reply, as SWAPDB did. Steady-state cost one atomic load. Under strace, 0 of 7,680 checked acks replied early on tokio and monoio epoll.
- AOF Option 1A round-2 hardening (moon#1266): read-before-enqueue paths (monoio inline SET, `try_send_append_durable`) re-check the lane after the enqueue; the inline SET gate reads its own shard's lane; an everysec writer whose fsync agent failed to start no longer resets its deadline every wake; test hook `MOON_TEST_AOF_WRITER_START_DELAY_MS` pins the boot hold.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.9 · Practicality 0.9 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9

# WS18-parity-residuals — working notes

Base `d155cd6` (int/part3b) + plan `81fb3e9`. Branch `perf/ws18-parity-residuals`.
`export CARGO_TARGET_DIR=/home/user/wt/target CARGO_INCREMENTAL=0`. Ports 7500–7519.
Oracle: redis-server 7.0.15 (`/usr/bin/redis-server`). Red binaries:
`/home/user/wt/bin/baseline-ae21476` (main) and `/home/user/wt/bin/ws18-dbg-base-d155cd6`
(debug build of the integration base, built from this worktree before any change).

Environment note: this box's root filesystem sits at ~4.4% free, below moon's 5% default
`--disk-free-min-pct`, so every write answers `MOONERR diskfull` unless the server is
started with `--disk-free-min-pct 0` (the existing integration harnesses already pass it).

## moon#1234 — DEL / UNLINK / GETDEL never emit `del`

**Mechanism (verified).** `notify_keyspace_event` is called from exactly five places
(SET, INCRBY family, RENAME, active expiry, keymiss); `"del"` appears only in
`notify_fanout.rs` unit tests. `key::del` / `key::unlink` / `string::getdel` remove keys
and never queue an event. Every path that deletes on a client's behalf runs one of those
three bodies: `command::dispatch` (DEL/UNLINK/GETDEL are writes, so never in
`dispatch_read`; `try_inline_dispatch` only inlines GET/SET), the coordinator's spanning
DEL (`coordinate_multi_del_or_exists` → `run_local` for the local slice and one
`<CMD> k…` per owner leg, both → `cmd_dispatch`), MULTI/EXEC (executor → dispatch) and Lua
(`bridge` → `Database::execute_command` → `command::dispatch`). The outbox is drained by
the connection at batch end (`flush_from_connection`) and by the shard loop after an SPSC
drain (`flush_from_shard`), so an event queued in the body reaches subscribers on every
path, with nothing double-queued (each key is removed by exactly one body run).

**Oracle (live, redis 7.0.15 vs baseline-ae21476, `--shards 1` and `4`, flags `KEA`):**
- `DEL a nx b a` → `:2`, redis: `del a`, `del b` (keyspace + keyevent each); moon: nothing.
- `UNLINK l a zz` → same shape. `GETDEL g` (hit) → redis `del g` only.
- `GETDEL nope` with `KEAm` → redis `keymiss nope` (lookupKeyRead's miss event); moon nothing.
- `GETDEL` on a list → WRONGTYPE, no event. `DEL` of an expired key → `:0`, only `expired`.
- MULTI `DEL a; UNLINK a` → one `del a`. `EVAL "return redis.call('DEL',...)"` → `del a`.
- Class filtering: `Kg` → keyspace only; `E$` → no del (class `g`).

**Fix.** Queue `del` (class `g`) per key actually removed in `key::del` / `key::unlink`
(the counting variants already say "removed", including a cold-only key), and in
`getdel` after the remove; `keymiss` (class `m`) on a GETDEL miss, as GET does. Disabled
cost: one Relaxed load + two masks per removed key (the `notify_keyspace_event` gate).

**Risks.** Spanning deletes publish from several shards, so cross-shard event ORDER is
not argument order (redis's is); per-shard order is. The test compares sets at `--shards 4`
and sequences at `--shards 1`.

## moon#1235 — SCRIPT/FUNCTION LOAD racing FLUSH leaves shards disagreeing

**Mechanism (verified).** `try_handle_script` / the FUNCTION arms apply the op to the
origin shard's cache/registry, then `fanout_to_other_shards` pushes it to every other
shard's ring and awaits the acks. Two origins replay on their OWN rings, so a LOAD from
shard a and a FLUSH from shard b reach shard c in whichever order c's drain meets the two
rings. Nothing orders them; both clients get `+OK`. Red evidence, 200 barrier-released
trials each (`tests/perf_ws18_script_order.rs`): on `d155cd6` SCRIPT 86/200 mixed at
`--shards 2`, 173/200 at 4; FUNCTION 42/200 and 167/200. On `ae21476` (FLUSH still
local-only, moon#1229) 16/17 SCRIPT and 91/104 FUNCTION.

**Why not the shard-0 sequencer the plan prefers.** A sequencer shard must re-push every
op it receives onto its own rings in arrival order, from inside its SPSC drain.
`handle_shard_message_shared` is synchronous and holds neither the producers nor the
notifiers (spsc_handler.rs documents the same limit for MULTI/Lua flushes: fanning out
from inside a shard's message loop is the shard-to-shard wait cycle), so it needs a new
per-shard sequencer task plus event_loop plumbing (not this workstream's file) and one
extra hop per op. Two cheaper mechanisms give the same guarantee — one order, reply only
after every shard applied it — each fitted to its store:

* **Script cache → flush epochs** (order-free convergence). The cache is a set of
  digest-keyed bodies that only a whole flush removes. A process-wide `AtomicU64`
  (`scripting::order`) numbers flushes; every insert carries the epoch current when it
  was issued; a shard keeps an entry only while `tag >= newest flush applied`, and drops
  a late insert tagged below it. Final content on every shard = the bodies inserted at or
  after the newest flush, whatever the arrival order (unit test: all 120 arrival orders
  of 3 loads/2 flushes agree). Linearization: LOAD at its tag read, FLUSH at its bump.
  It is the only rule that also covers EVAL's IMPLICIT insert: the origin claims the body
  under epoch e and replays it with the same e; the execution-time insert on the
  executing shard (a second, shard-local insert under a later epoch) is skipped at
  `--shards > 1` — a flush landing between claim and execution would otherwise leave the
  body on that one shard. `mark_fanned_out` ignores a publish a newer flush superseded,
  or the next EVAL would skip the republish.
* **Function registry → one process-wide order token.** LOAD without REPLACE fails on an
  existing library and function names collide ACROSS libraries, so no per-message rule
  converges; mutations must be serialized. `run_function_command` takes a single-token
  `flume::bounded(1)` channel before the local apply and drops it after every shard
  acked. flume's cross-thread wake reaches `!Send` monoio tasks (the reply oneshots rely
  on it); the guard releases on every exit path. The wait is bounded by the fan-out
  budget; on timeout the op is applied NOWHERE and the client gets `MOONERR partialfanout
  FUNCTION not applied …` (only a wedged shard holds the token that long).

**Wire.** `ShardMessage::ScriptLoad { script, epoch, ack }` (the receiver computes the
digest, so the `String` sha field is gone and the enum stays ≤ 64 B) and
`ScriptFlush { epoch, ack }`. Sequential replies unchanged (`+OK`, the sha).

**Risks.** (1) Unique-body EVAL storms read one atomic per first sight — negligible.
(2) Concurrent FUNCTION mutations from many connections now queue (one fan-out round
trip each) — administrative verbs. (3) A fan-out that times out (wedged shard) releases
the token with the op possibly still queued there; that op is reported partial and a
later op can overtake it on that shard — the documented re-issue contract. (4) The epoch
and token are process-global; several servers in one process (unit tests) only share
ordering, never state.

**moon#567 (same fan-out).** With the acked fan-out (moon#515) and now one order, a
`SCRIPT LOAD` is installed on every shard before the sha is returned on every healthy
path. The one path left was the give-up (ring full past the retry budget, ack not back
inside the 2 s budget): it logged and still answered the sha, so redis-py's
`Lock.release()` (SCRIPT LOAD + EVALSHA retry, no EVAL fallback) met NOSCRIPT for a sha
the server had just returned. Fixed: SCRIPT LOAD takes the FLUSH/FUNCTION contract —
`-MOONERR partialfanout SCRIPT LOAD applied on X of N shards; re-issue it to converge`.
Not done: a self-healing EVALSHA that ships the body to a target missing it (rewriting
the routed EVALSHA as EVAL changes what the target records in commandstats/slowlog;
carrying the body needs a new field on the hot routed message). Red:
`script_load_partial_fanout_is_reported_not_swallowed` answered the sha on ae21476/d155cd6.

**Split into commits.** The call-site unification (`run_script_command`,
`run_function_command`, cross-ownership into both handlers) is its own behaviour-neutral
commit, so the fix commit touches only the owned files; the fan-out block later moved to
`server/conn/script_fanout.rs` (shared.rs 7,002 → 6,388 lines, below its base size).

Self-score #1234: Completeness 0.92 · Clarity 0.93 · Practicality 0.95 · Optimization 0.95 ·
Edge cases 0.92 (absent, repeated, cold-only, wrong type, expired, MULTI, Lua, class
filter, K-only) · Self-evaluation 0.9 (moon still misses most OTHER keyspace events —
`expire` on SET PX, pop-emptied `del` — named in the commit, not claimed).
Self-score #1235: Completeness 0.93 · Clarity 0.92 · Practicality 0.92 · Optimization 0.93 ·
Edge cases 0.91 (EVAL implicit insert, superseded publish, partial fan-out, token
timeout) · Self-evaluation 0.9 (the plan's preferred sequencer was not built; the
reason is written above and the guarantee is the same).

## moon#1226 — lists / listpack

- **WATCH parity.** Taking a list's mutable handle IS the WATCH bump (moon#926); LREM
  and LINSERT took it before looking. `list_route_holding` decides on the `&self` view
  (`peek_list_ref_if_alive` + `for_each_match`, first match stops) and the no-op answers
  from there; `touch_list` records the one access (LFU `OBJECT FREQ` equal to redis: 13
  after RPUSH + 5 LREM + 3 LINSERT no-ops). Red: version moved 2 → 5.
- **Backlen docs / iter_rev.** WS10's moon#1206 made backlens redis-ordered (pinned
  byte-for-byte vs redis DUMP), so a backward step is sound for every width. `iter_rev`
  stays `pub` (tested on wide entries by WS10); the stale "never walk backwards" /
  "NEARER end" docs now say what the code does.
- **pop_back_n.** `pop_end(false)` steps back over one backlen (O(1), 0 head seeks);
  `pop_n(front, n)` finds the cut once (n backlen steps at the back, the decoding walk
  at the front) and truncates/drains ONCE. A backlen that does not land on an entry
  boundary (never for a moon-built listpack) falls back to the forward single pops. Red:
  `RPOP q 100` = 100 head seeks; green 0. One-allocation pop floor unchanged.
- **SRANDMEMBER −N on a listpack.** One walk materialises each member once into a
  `SmallVec<[Bytes; 128]>`; each draw clones a handle (refcount). Same draws, same
  order. Red: 20,000 distinct buffers for −20000 on 100 members; green ≤ 100.
- **moon#1209 rows.** test-consistency had all; test-commands lacked `COUNT -1` and
  `RANK -1 COUNT 0` — added.

Self-score: Completeness 0.92 · Clarity 0.92 · Practicality 0.94 · Optimization 0.93 ·
Edge cases 0.92 · Self-evaluation 0.9 (`listpack.rs` itself stays 2,316 lines — its
`into_bytes`/`OWNED_DECODES` move is a separate #1226 bullet not in this plan).

## moon#1226 — storage core

- **Lazy-free tick.** `PENDING_ITEMS` is process-wide, so every shard took 16 write
  guards per 1 ms tick while ANY shard drained. A thread-local hint (a shard's dbs are
  mutated only on its thread; D3 foreign writes have no production caller) gates the
  tick; a full pass resyncs it; every 64th tick still sweeps while anything is pending
  anywhere (the hint cannot see a queue filled from another thread). The start db
  rotates. Red: 1,008 guards over 63 idle ticks; tick 1 and 2 both started at db 0.
  Risk: a queue filled on a foreign thread waits ≤ 64 ms for its first slice.
- **Split test gap.** Identity-hashed u64 keys homed to group 0 force the "any free slot"
  fallback; three fixtures (off-home stayer keeps the flag, off-home mover lowers it,
  overflow in the new segment raises its flag). Code was correct: shown red by mutation.
- **dashtable/mod.rs** 2,020 → 970 (tests to `tests.rs`, verbatim).

Self-score: 0.92 · 0.92 · 0.93 · 0.92 · 0.9 · 0.9.

## moon#1226 — pub/sub, protocol, persistence, harness

- **KEYSPACE_LISTENERS** Release/Acquire with the pairing documented; subscribe /
  psubscribe now count AFTER inserting the entry (the pairing's premise). x86 TSO: no
  red test constructible; stated.
- **pubsub tests on monoio.** Moved to `pubsub/tests.rs` (mod.rs 1,716 → 1,025); the 12
  `#[tokio::test]`s became plain tests reading with `try_recv` (publish delivers
  synchronously via `try_send`). Monoio lib run: 0 → 27 tests.
- **Protocol fault after a deferral.** Verified live vs redis 7.0.15: `RPUSH l a; BLPOP l
  0; SET x 1; <bad>` — redis answers `:1, [l,a], +OK, -ERR Protocol error…`; moon dropped
  the SET. Also found: monoio's inline fast path dropped its replies before a fault, and
  the RESP2 subscriber loop closed mute. Fix (both runtimes): keep the fault latched while
  frames are carried (loop without reading, no parse), clear it when the carry is spilled
  to bytes for the subscriber loop (the bad bytes still follow, so that loop reports it),
  flush inline replies with the error. Integration test at shards 1/4, both runtimes.
- **Tokio AOF BufWriter** 8 MiB → 2 MiB = tokio::fs's per-hop max (DEFAULT_MAX_BUF_SIZE);
  hop count unchanged (model test), capacity observed via first hop.
- **MOON_BIN**: three suites ignored it; replication_swapdb honoured it but spawned "" for
  an empty value. All four use `common::find_moon_binary()`; shown with a logging wrapper.

Self-score: 0.93 · 0.92 · 0.93 · 0.92 · 0.91 · 0.9.

## Measurement: DEL hot path (#1234 adds the notify gate per removed key)

Release-fast `ws18-rf1` (`c3ec01d`) vs `baseline-ae21476`, `--shards 1`, prefill
`SET k:__rand_int__` then timed `DEL k:__rand_int__`, `redis-benchmark -n 1M -r 1M -P16
-c8`, binaries alternating, 6 reps, 4 vCPU at load ~2. rps medians: notifications off
616K (base) vs 615K (ws18); `KEA` with no listener 594K vs 624K. Rep-to-rep swing ~±5%:
no measurable regression. The baseline is main, so ws18 also carries the rest of
int/part3b — not a pure control for the one change.

## Gates at the last code commit `c3ec01d`

- `cargo fmt --check`, `scripts/audit-unsafe.sh` (0 missing SAFETY), `scripts/audit-unwrap.sh`
  (within baseline): clean.
- `cargo clippy --all-targets -- -D warnings`, tokio `clippy -- -D warnings`, tokio
  `check --all-targets`: clean.
- `cargo test --lib` (monoio, full): 6545 passed, 2 failed —
  `cold_index_rebuild_tests::unreadable_file_…` (known root-only) and
  `replication::stream_effect::tests::a_read_is_one_forced_claim_per_stream_that_replays_exactly`:
  a wall-clock race in the test (two streams read in one XREADGROUP straddle a 1 ms tick;
  the test takes stream s's delivery time for both); 1 of 3 isolated reruns fails. No
  file on its path is touched by this branch; not A/B'd on the base.
- `cargo test --lib` (tokio, full): 5616 passed, 1 failed (the known root-only test).
- Integration, pinned `MOON_BIN=ws18-dbg-final` (monoio, `c3ec01d`): perf_ws18_del_notify 2,
  perf_ws18_script_order 5, perf_ws18_proto_fault_defer 2, pipeline_cross_shard_ordering 18,
  ft_search_yield_red 4, script_function_fanout 13, perf_ws8_script_flush_fanout 2,
  function_in_multi_697 4, scripts_in_multi_894 5, perf_ws9_keyspace_listener 3,
  protocol_error_lifetime 8, perf_ws4_deferral_carry 3, replication_swapdb (--ignored) 3 —
  all green. `list_pop_alloc_942` is RED, deterministically: its FULL-encoding control
  ("a VecDeque push/pop pair allocates 0") counts 50 — WS10's moon#1160 (`783beb8`, in
  d155cd6) made every full-encoding LPUSH `detach` (an exact-size copy). The listpack
  numbers this branch changes are at their floor (50/50/50). Not this branch's file.
- Integration, tokio (`ws18-dbg-tokio-final`): perf_ws18_del_notify 2, perf_ws18_script_order
  5, perf_ws18_proto_fault_defer 2, pipeline_cross_shard_ordering 18, script_function_fanout
  13, protocol_error_lifetime 8 — green (ft_search_yield_red is monoio-only: 0 tests).
- Script rows: extracted and run against redis-server 7.0.15 on ports 7500–7510 (the
  scripts' own cleanup `pkill -f`s by pattern, which the team rules forbid here).

## Method notes

- A container restart killed a running `cargo test` (exit 137) mid-session; everything
  interrupted was re-run.
- Artifact aliasing bit once: a `list_pop_alloc_942` run "passed" with a FULL-pair count of
  0 — a test binary built from another tree. Every later run in this tree is red. Pinned
  server binaries (`MOON_BIN`) and `strings` provenance checks were used for every
  integration result above.
- SUMMARY.md: the harness refused the subagent write (TEAM-RULES §6); its full content is
  in the final report for the orchestrator to commit.

# WS8-shard-coordination — working notes

Branch `perf/ws8-shard-coordination`, base `ae21476` (+ plan commit `6537679`).
Personas: routing-dispatch-engineer (lead), performance-engineer.
Read before any cross-shard change: `docs/internal/cross-shard-cost-model.md` (read in full;
dead ends 1-8 and the §8.2 note on the coordinator being invisible to the park counters are
the constraints used below).

Box: 4 vCPU x86_64 container, same-host client — every number here is RELATIVE (interleaved
A/B against `/home/user/wt/bin/baseline-ae21476`), never an absolute claim. Ports 7260-7279.

---

## moon#1162 — spanning DEL/UNLINK never tombstones vectors (BUG)

**Mechanism verified in code.**
- A multi-key `DEL`/`UNLINK` whose keys span shards reaches
  `coordinator::coordinate_multi_del_or_exists` (both runtimes: monoio
  `handler_monoio/dispatch.rs` `coordinate_multi_key` call block, tokio
  `handler_sharded/mod.rs` same call). The local leg runs `cmd_dispatch` under
  `with_shard_db` and nothing else; remote legs arrive as
  `ShardMessage::MultiExecute`.
- The `MultiExecute` arm (`spsc_handler.rs`) runs COW, eviction gate, dispatch,
  AOF, `wake_written_keys`, `flush_every_database_on_flushall` — and NO index
  hook at all: no `auto_delete_vectors`, no `auto_drop_mq_streams`, no HSET
  auto-index, no HDEL tombstone, no `auto_flush_indexes`.
- Every other write path runs them: `PipelineBatchSlotted` (HSET, DEL, HDEL,
  FLUSH index hooks — but NOT the MQ drops), the monoio local write path
  (`handler_monoio/mod.rs` ~3700-3780: all five, incl. MQ), `shared.rs`
  MULTI executor (all five), `replication/apply.rs` (all five). The
  `Execute` arm has only HSET + an inlined DEL tombstone (no MQ, HDEL, FLUSH).
- Text indexes have no live DEL hook anywhere; `mark_deleted_for_key_for_db`
  also records the delete in the recovery ledger that text-only indexes read
  (moon#1124), so the vector hook is the text-index cleanup too.

**Found while verifying (same root cause, same arm):** `coordinate_flush_broadcast`
sends the other shards' `FLUSHALL`/`FLUSHDB` as `MultiExecute`, so at `--shards 4`
a FLUSHALL clears only the connection shard's vector index contents. Measured on
`baseline-ae21476`: 40 docs → FLUSHALL → FT.SEARCH still returns 32 (FLUSHDB: 31).
The D-2 doc comment on `coordinate_flush_broadcast` claims index clearing applies
"exactly as for the local leg" — false on HEAD.

Repro of the issue table on `baseline-ae21476 --shards 4`: `DEL doc:0..doc:19` → `:20`,
FT.SEARCH KNN 40 → 40 hits, 20 of them deleted keys.

**Fix design.**
- One helper, `shard::write_hooks::run_post_write_hooks(s, cmd, args, db, reply)`,
  owning the complete hook set: HSET auto-index; DEL/UNLINK vector tombstone + MQ
  drop; HDEL vector tombstone; FLUSHDB/FLUSHALL index-contents clear + MQ drop.
  Gated on `(len, first byte)` so an ordinary write pays one length compare (the
  arms used to pay up to six `eq_ignore_ascii_case` per command).
- Called from: `Execute`, `MultiExecute`, `PipelineBatchSlotted` (after the db
  guard is released — `auto_drop_mq_streams` needs `&mut ShardSlice`), and the
  coordinator's `run_local` (every coordinator local leg; DEL/UNLINK now go
  through `run_local` too).
- Not changed: the connection-local paths (WS7 files) already run all five.

**Risks.** Hooks now run on the SPSC arms' MQ path (`auto_drop_mq_streams` emits a
WAL `MqDrop` via `wal_append_tx`): same call the local path makes from the same
thread, and it early-returns when the shard has no durable-queue registry (the
common case: one `Option` check). The `PipelineBatchSlotted` hooks move after
`wake_written_keys`; the two are independent (the waker reads the keyspace, the
hooks touch the index stores).

## moon#1229 — SCRIPT FLUSH clears only the connection's shard (BUG)

**Mechanism verified.** `scripting::handle_script_subcommand` FLUSH arm calls
`ScriptCache::flush()` on the connection's shard (`ctx.script_cache`), nothing else.
`SCRIPT LOAD` already fans out (`script_fanout_bounded` → `ShardMessage::ScriptLoad`,
acked, moon#515); FUNCTION FLUSH fans out (`function_registry_fanout`, moon#514). No
flush message existed. Measured on `baseline-ae21476 --shards 2`: after `SCRIPT FLUSH`,
`SCRIPT EXISTS` from 10 fresh connections answered `0 1 0 0 1 1 0 1 0 1`.
Also: `SCRIPT FLUSH BOGUS` answered `+OK` and flushed; redis 7.0.15:
`ERR SCRIPT FLUSH only support SYNC|ASYNC option` (checked with redis-cli).

**Entry points.** monoio `handler_monoio/dispatch.rs::try_handle_script`, tokio
`handler_sharded/txn_intercepts.rs::try_handle_script`; the MULTI fillers
(`fill_txn_intercept_slots`, both runtimes) call these same functions. The console
gateway routes SCRIPT through `ShardMessage::Execute` → `cmd_dispatch`, which has no
SCRIPT arm (answers unknown command) — no flush happens there, nothing to broadcast.

**Fix.** `ShardMessage::ScriptFlush { ack }`; the shard arm calls
`ScriptCache::flush()` (source map + moon#1167 compiled cache + fan-out duties) and
acks `true`. `shared::script_flush_fanout` uses `fanout_to_other_shards` (one budget
for pushes + acks, loud drops); the reply waits for every ack; a partial fan-out turns
the reply into `MOONERR partialfanout SCRIPT FLUSH applied on X of N shards; re-issue
it to converge` (same contract as FUNCTION). Only an ACCEPTED flush fans out
(`is_accepted_script_flush`: FLUSH subcommand + non-error reply).

**Risks.** No total order between a `SCRIPT LOAD` fan-out and a concurrent `SCRIPT
FLUSH` from another shard (same documented limitation as FUNCTION); a flush is
idempotent so a re-issue converges. Follow-up (not in scope, scripting/mod.rs EXISTS
arm): `SCRIPT EXISTS` with no sha answers `*0` where redis answers
`ERR wrong number of arguments for 'script|exists' command`.

## moon#1228 item 1 — MSET coordinator local leg bypasses BGSAVE pre-image capture (BUG)

**Mechanism verified.** `coordinate_mset` all-local fast path called
`command::string::mset(db, args)` directly and the spanning local slice ran a
`db.set_string` loop; neither goes through `command::dispatch`, which is where
`snapshot_cow::capture_dispatch_pre_image` runs (moon#558/#1217). Remote legs are
captured on their owners (`cow_intercept` in the `MultiExecute` arm).

**Fix / new capture site.** Both local legs now run `MSET <local pairs>` through
`run_local` → `cmd_dispatch`, so the capture site is the existing dispatch hook
(`capture_written_keys` walks every `KeyRole::Write` position of MSET). Audit-table row
for the WS12/WS16 NOTES: *coordinator MSET local legs (fast path + spanning local
slice) — captured via `command::dispatch` (moon#1228)*. `run_local` is also the path of
every other coordinator local leg (MSETNX, DEL/UNLINK since moon#1162, BITOP/COPY
writes), so the coordinator no longer has a capture-free write.

**Evidence.** Unit (deterministic, `snapshot_cow::arm` + `pending_for_test`):
`multikey_leg_tests::mset_all_local_fast_path_captures_every_pre_image` and
`::spanning_mset_local_slice_captures_every_pre_image` — red with the fix reverted
(`left: []`, no pre-image captured), green after. End to end
(`tests/perf_ws8_mset_bgsave_capture.rs`, `--shards 4`, 400K keys, spanning 32-pair
MSETs during BGSAVE, SIGKILL, restore from the snapshot alone): red on
`baseline-ae21476` — 138 of 400000 keys restored with the post-epoch value `new`;
green on the fixed binary.

## moon#1184 — spanning MSET/DEL/UNLINK split per key

**Mechanism verified.** `coordinate_mset` built one `SET k v` sub-command per remote pair
and `coordinate_multi_del_or_exists` one `<CMD> k` per remote key (plus a
`to_ascii_uppercase` allocation per call and a `Bytes::from(cmd_upper.clone())` per key).
The owner's `MultiExecute` arm runs, per sub-command: `databases.write(db)`, eviction
gate, COW, `probe.observe` (so `total_commands_processed` counted keys), dispatch,
`serialize_effect_for_log` + `wal_append_and_fanout`. A `DEL` of an absent key is
`Verbatim` in `effect_rewrite`, so it was logged.

**Fix.**
- `group_by_owner(name, args, stride, num_shards) -> Vec<Vec<Frame>>` (indexed by
  shard, ascending = the VLL order, one allocation for the outer Vec instead of a
  BTreeMap node per shard). Each non-empty group is `[name, items…]` in argument order
  — it is at once the remote leg's frame, the local leg's argv (`[1..]`) and the local
  AOF record (`serialize_command_parts`, no `Frame::Array` copy).
- Remote legs: ONE sub-command per owner (`MSET k v …` / `DEL k1 k2 …` /
  `UNLINK …` / `EXISTS …` / `TOUCH …`), static command-name bytes
  (`summed_command_name`). Replies: DEL/UNLINK/EXISTS/TOUCH integers summed (duplicates
  stay on their one owner, so `EXISTS k k` still counts 2 and `DEL k k` 1); MSET `OK`.
- All remote legs are sent BEFORE the local slice runs (the owners apply in parallel
  with this shard); the moon#1084 ordering (log the local slice right after applying
  it, before awaiting the remote legs) is unchanged.
- `MultiExecute` arm: a `DEL`/`UNLINK` whose reply is `:0` logs nothing
  (`write_hooks::deleted_nothing`) — redis propagates a delete only when it deleted
  something; the local leg already skipped `n == 0`.
- Replay: a per-owner `MSET`/`DEL` record is an ordinary command over keys that owner
  owns — the same shape the local leg has logged since review Finding 1.

**Evidence.** `tests/perf_ws8_spanning_write_merge.rs` (`--shards 4 --appendonly yes`,
40-pair MSET, 20-key DEL, 10-key UNLINK, 40-key DEL of absent keys, per-shard AOF
sync markers, then SIGKILL + replay):
| binary | MSET records | per-key SET records | DEL records | UNLINK records | absent-key mentions |
|---|---:|---:|---:|---:|---:|
| baseline-ae21476 | 1 | 30 | 52 | 9 | 35 |
| fixed | 4 | 0 | 4 | 4 | 0 |
Replay after SIGKILL restores every surviving key and none of the deleted ones.
Unit: `multikey_leg_tests::spanning_mset_sends_one_mset_per_remote_owner`,
`::spanning_del_family_sends_one_sub_command_per_remote_owner_and_sums` (fake owner
shard records what it is sent) — red on the pre-fix coordinator (`left: 5, right: 1`
sub-commands), green after.

**Not done here.** Moving spanning writes into the slotted batch (moon#513 / cost-model
§8.2 — the coordinator is still a batch boundary per spanning write). That is the lever
the cost model names for this family; this change cuts per-key owner work, not parks.

## moon#1198 — shard items

**Item 7, dead SPSC arms (verified).** Whole-repo grep (`src`, `tests`, `benches`, `fuzz`)
for `ExecuteSlotted`, `MultiExecuteSlotted`, `ShardMessage::PipelineBatch` (not
`…Slotted`): constructed only by two unit tests (`drain_cap_tests`, the
`aof_admission_tests::one` helper); matched in `spsc_handler.rs` and
`aof_admission.rs`; mentioned in comments of other workstreams' integration tests. The
handlers send every routed command as `PipelineBatchSlotted` (a lone command is a
batch of one), the coordinator `MultiExecute`, console/scripts `Execute`. Removed the
three variants, their ~690 lines of arms, and their admission cases; the two test
producers now send a one-command `PipelineBatchSlotted`. `spsc_handler.rs` 4812 → ~4100
lines (still over 1500 — pre-existing; this wave only shrinks it).

**Item 2, shard part (verified).** `MultiExecute` and `PipelineBatchSlotted` took
`s.databases.write(db).refresh_now_from_cache(clock)` once per message — an exclusive
acquisition of its own — then `write(db)` again per command. Now the refresh runs once
per message inside the first command's guard (`clock_fresh` flag); the MOVE/COPY-DB
intercept refreshes the two dbs it takes itself (unchanged). Not dead end #8: that one
downgraded command guards to shared; this removes a hold. Evidence: test-only
`db_plane::exclusive_count` (cfg(test), zero cost shipped) +
`guard_count_tests::batch_arms_take_one_exclusive_guard_per_command`: (MultiExecute of
1, PipelineBatchSlotted of 3) exclusive acquisitions (2, 4) → (1, 3).

Not in scope here (other owners): non-inline local GET / inline SET double guards
(connection handlers, WS7); `ReadVersions` goes shared under moon#1183.

## moon#1177 — redundant cross-shard write work

**Mechanism verified.**
1. Origin: `handler_monoio/mod.rs` built `aof_bytes = Some(serialize_command_for_log(
   &dispatch_frame))` per remote persisted write; its only consumer tested
   `.is_some()` for the barrier. Tokio: `aof_bytes = if aof_wanted && (!is_local ||
   is_two_db)` — the remote leg serialized and discarded; the local two-db intercepts
   really use the bytes (kept).
2. Owner: `wal_append_and_fanout(data: &[u8])` → `Bytes::copy_from_slice(data)` into the
   AOF pool (and for the replica leg) although every SPSC arm owned a `Bytes`.
3. Backlog: `repl_backlog.lock()` on every write; the comment said "no lock acquire
   when None" and "once per 1ms tick".
4. Origin bookkeeping: monoio `frame.clone()` (deep FrameVec clone) only to satisfy the
   borrow of `cmd`; `Arc::new(dispatch_frame)` per command; `HashMap<usize, Vec<…>>`
   (SipHash per command); `drain()` + `unzip()` per target per batch; ~210 B tuple with an
   inline `Option<SmallVec<[Bytes; 4]>>`.

**Fix.**
- `shard::remote_batch::{RemoteBatch, RemoteMeta}`: shard-indexed `Vec<Vec<Frame>>` +
  `Vec<Vec<RemoteMeta>>`, hoisted per connection in both handlers (lazy: allocated on the
  first cross-shard command, moon#1179 item 3 kept), commands handed over with
  `mem::take` (they cross threads, so one allocation per target per batch is the floor),
  bookkeeping `Vec` recycled after the fold. `RemoteMeta { sink, persisted_write: bool,
  track_keys: Option<Box<TrackedWriteKeys>>, shape }` ≤ 32 B (unit-tested).
  Deterministic ascending-shard dispatch order (was HashMap order).
- `ShardMessage::PipelineBatchSlotted.commands: Vec<Frame>` (no `Arc`).
- monoio: every use of `cmd`/`cmd_args` before the dispatch point; the request frame
  MOVES into the message (no deep clone). Tokio already moved it.
- `wal_append_and_fanout_bytes(data: Bytes, …)`: WAL/backlog borrow, replica fan-out
  shares (refcount) when no SELECT prefix, AOF pool gets the record moved in. The
  slice-taking `wal_append_and_fanout` stays as a thin wrapper (one copy) for
  `replication/reason_del.rs` — not my file; its reason-DELs are background-only.
- Backlog locked only when `!replica_txs.is_empty() || fanout_hint_active()`. Soundness:
  local writes already skip the backlog while the hint is false; every activation site
  realigns the backlog to the shard offset before any cut
  (`ensure_backlogs_allocated`'s invariant), and the shard's own
  `RegisterReplica`/`PrepareReplicaSync` arm sets the hint on THIS thread first. Comment
  corrected.

**Deferred (ownership):** the tokio per-batch `batch`/`responses` `Vec`s (hoist) — they
sit in the connection loop proper (WS7's region), not the remote-dispatch block.

**Evidence.** `fanout_record_tests::the_aof_pool_receives_the_callers_record_without_a_copy`
(pointer identity; red with the copy restored: `0x…b70 != 0x…c40`) and
`::a_write_with_no_replica_never_takes_the_backlog_lock` (backlog mutex held by the test
thread; red: `Err(Timeout)` — the write blocked on it). `remote_batch::tests` (grouping,
capacity reuse, `RemoteMeta` ≤ 32 B). A/B throughput below (Measurements).

## moon#1183 — WATCH sequential round trips, no fast path, exclusive guards

**Mechanism verified.** `coordinator::snapshot_versions` (called by
`server/conn/watch.rs`, shared by both handlers) looped over a `HashMap` of owners and
`spsc_send` + `recv_reply_bounded(..).await` INSIDE the loop (m owners = m round trips
in series); the local group and the `ReadVersions` arm used `with_shard_db` (exclusive)
for `Database::get_version(&self)`.

**Ordering argument for the fast path.** WATCH is a connection-level intercept:
`may_be_inline_intercepted(WATCH)` is true, so `must_wait_for_pending_remote` defers it
whenever `pending_mask != 0`. When `snapshot_versions` runs, no write of this connection
is in flight on any shard — the GET fast path's `pending_mask` gate is satisfied by
construction. No `is_hot` gate is needed: the owner arm calls the very same
`get_version`, which never consults the cold tier, so both paths answer identically
for a spilled key.

**Fix.** Owners grouped in a shard-indexed `Vec<SmallVec<[usize; 4]>>` (positions into
`keys`); local group under `with_shard_db_read`; each remote owner first
`try_foreign_db_read` (gated on `cross_shard_fast_path_enabled()`, one CAS, never
parks); declined owners: every `ReadVersions` sent, then all awaited; `ReadVersions`
arm under the shared guard.

**Evidence.**
- `tests/perf_ws8_watch_versions.rs` (`--shards 4`, WATCH of one key per shard + UNWATCH
  × 200; `INFO stats spsc_notify_wakes` delta): baseline-ae21476 **603** owner-loop
  wakes (≈3 per WATCH, one per remote owner) → fixed **3**. Same test: WATCH one key per
  shard, INCR from another connection → EXEC `*-1`; untouched → EXEC commits.
- Unit `coordinator::watch_versions_tests`: local group exclusive acquisitions 1 → 0;
  barrier test (fake owners hold replies until all 3 requests arrived): old
  `[false, false, true]` → `[true, true, true]`.
- WATCH suites by name, green: `watch_cas_transactions` (10), `watch_container_mutation_926` (14).

**Runtime note.** The foreign-read leg follows `--cross-shard-fast-path`, whose `auto`
resolves ON only for monoio builds (`resolve_cross_shard_fast_path`). On a tokio build
with `auto`, every remote owner is declined and WATCH takes the batched path: all
`ReadVersions` sent, then all awaited — m owners cost one overlapped round trip instead
of m serial ones, but the wake count stays ≈3 per WATCH (the RED run on tokio showed
603). The integration test therefore pins `--cross-shard-fast-path on` (099fc2a); with it
tokio also shows 3 wakes. `main.rs`'s comment that an explicit `on` "does nothing" on
tokio is now stale for WATCH (owner: whoever owns `main.rs`/config docs).

## moon#1182 — FT.SEARCH local-first scatter, synchronous remote searches

**Mechanism verified.** `scatter_vector_search_remote` ran the local
`search_local_filtered` inside `with_shard` BEFORE sending any `VectorSearch`; the same
local-first order in `scatter_text_search` (phase 1 DocFreq and phase 2 TextSearch),
`scatter_hybrid_search` (phase 1 DFS and phase 2 three-stream search) and
`coordinate_keys`. The `VectorSearch` SPSC arm searched synchronously inside
`drain_spsc_shared`. `handler_monoio/ft.rs` routes to the scatter whenever
`num_shards > 1`, so the C5 cooperative path (`ft_search_capture` →
`search_mvcc_yielding`) was `--shards 1` only. A dead sibling,
`coordinator::scatter_vector_search` (no caller in src/tests/benches/fuzz, held a
`&mut VectorStore` across awaits), removed.

**Fix.**
- All scatters: every remote request is sent before the local leg runs; the merge input
  order is unchanged (local first, then remote shards ascending), so merges are
  byte-identical. KEYS builds one `Arc` request shared by every leg.
- `shard::vector_scatter::{capture_knn, run_knn, spawn_knn_reply}`: capture the owned
  snapshot with `capture_dense_knn_snapshot` (made `pub(crate)` — cross-ownership,
  isolated commit) using the scatter's own parameters (default field, no filter,
  unpaginated), await/install COLD reloads (moon#1070, as ft.rs does), then
  `search_mvcc_yielding` + `build_search_response(.., 0, usize::MAX)`. `None` (unknown
  index, dimension mismatch) → the synchronous `search_local_filtered` for its exact
  error frames.
- Coordinator's local leg: `run_knn(...).await` on the connection task.
- `VectorSearch` arm: snapshot captured synchronously in message order (every write
  drained before the message is visible, none after — the C5 isolation argument), then
  `<Spawner as RuntimeSpawn>::spawn_local` a task that runs the cooperative search and
  sends the reply; the drain returns immediately.

**Evidence.**
- `tests/perf_ws8_ft_scatter.rs` (`--shards 4`, 2400 vectors, `MOON_FT_YIELD_CHUNK=64`
  as in `ft_search_yield_red`): baseline-ae21476 `ft_search_cooperative_yields_total`
  delta **0** over 4 KNN searches (red) → >0 fixed; exact-match document first and k=10
  hits for a query on every shard.
- Result identity vs HEAD (`scratchpad ft_identity.py` + `ft_compare.py`, same fixture,
  `--shards 4`, 2400 docs with TEXT + VECTOR): 20 KNN replies byte-identical; 4 text
  replies identical totals + score sequences; 4 hybrid totals identical; KEYS set
  identical — 29/29. Baseline vs baseline is 29/29 on the same comparator; text key
  order among equal BM25 scores and hybrid RRF ranks vary run-to-run on the baseline
  itself (ties), which is why those compare by score/total.
- Suites by name on the fixed binary: see SUMMARY.

**Not measured here.** The latency win (the issue's "≈ local search time" per
multi-shard query) needs a Linux perf host with a real corpus; on this 4-vCPU box with
a same-host client it would be noise. The mechanism is the reordering itself.

## moon#1214 item 1 — SPSC Notify takes the target's flume lock per push (PROFILED → DEFERRED)

**Mechanism verified.** `runtime/channel.rs` `Notify` is `flume::bounded(1)`;
`notify_one()` does `try_send(())` after every SPSC push (`spsc_send`, the handlers'
phase-2b push). flume's `Shared::send` takes the channel's lock, pushes if empty and
fires the receiver's `AsyncSignal` (the cross-thread wake), or returns `Full`.

**Profile (the plan's gate).** No `perf` on the box; installed
`linux-tools-6.8.0-106-generic` (perf 6.8.12; kernel-agnostic binary). Fixture:
`baseline-ae21476 --shards 4 --appendonly no`, 99,744 keys populated, then
`redis-benchmark -t set -r 100000 -P 1 -c 200 --threads 2` (same host);
`perf record -e cpu-clock -F 499 --call-graph dwarf,8192 -p <moon> -- sleep 8`
(6,005 samples; ~85K rps under the profile). The `Notify` code is identical on the
branch, so the baseline binary is the right subject.

| bucket | share of all samples |
|---|---:|
| kernel | 62.78% |
| moon user | 34.24% |
| `flume::Shared<T>::send`, inclusive (every caller is the connection handler's `notify_one`, per the caller graph on all 4 shard threads) | 5.35% |
| — of which `AsyncSignal::fire` (the receiver wake → monoio waker → eventfd write) | 4.23% |
| — `Shared::send` self (lock + queue, both the push and the already-full case) | 0.83% |

The proposed `AtomicBool pending` fast path skips the lock only when a token is
ALREADY queued; it cannot remove `fire`, which is the wake a newly queued token needs
(a parked consumer must be woken — cost model §1: the park/wake is the cost, the message
is ~free). The removable part is bounded above by `Shared::send`'s self time, 0.83% of
samples (~2.4% of moon user time), before subtracting the swap it adds per push and the
SeqCst fence per drain the consumer would need. That is below this box's A/B noise floor
(±10–20% run to run while other agents build), so the change could not be shown to pay,
and it would add a new atomic state machine (loom model) on the cross-shard path.

**Verdict: DEFERRED** with this profile. Re-profile at `--shards 8` on the GCE
`t2a-standard-8` rig from a dedicated load generator (8 producers per consumer lock) —
if `Shared::send` self exceeds ~2% of samples there, build the fast path with its loom
model.

## Measurements (relative only — 4-vCPU container, same-host client, other agents building)

Method: `scratchpad/ab.sh`, fresh server + dir per run, `--shards 4 --appendonly yes`,
A = `baseline-ae21476`, B = `ws8-rf-1` (release-fast of `7acb13c`), alternating A/B,
3 reps per row (6 for SET).

| workload | A rps (reps) | B rps (reps) | AOF bytes A → B |
|---|---|---|---|
| SET -d 256 -P 16 -c 50 -r 100000 (moon#1177) | 228.2k 242.5k 264.7k 242.2k 271.7k 225.2k (mean 245.7k) | 246.6k 297.2k 258.4k 298.2k 240.3k 249.9k (mean 265.0k) | varies run to run (not comparable) |
| MSET ×10 pairs, P1 c50 (moon#1184) | 19.7k 21.5k 19.1k | 20.9k 20.4k 21.6k | 73.8 MB → 56.8 MB (−23%) |
| MSET ×10 pairs, P16 c50 | 75.7k 104.2k 100.5k | 135.3k 113.1k 62.0k | 98.4 MB → 75.7 MB (−23%) |
| DEL ×10 absent keys, P1 c50 (moon#1184) | 22.2k 21.0k 21.5k | 19.6k 20.9k 20.8k | 51.7 MB → ~0.3 KB |

Reading: throughput differences are inside the run-to-run noise here and are NOT
claimed. The deterministic effects are: AOF bytes/records per spanning write
(−23% for MSET×10; no-op DELs no longer logged), per-command exclusive holds (moon#1198:
(2,4)→(1,3)), owner wakes per WATCH (603→3 per 200), zero-copy records and no
unconditional backlog lock (moon#1177 unit tests). At p=1 a spanning write is still one
coordinator batch boundary (cost model §8.2) — the lever that would move its throughput
is moving spanning writes into the slotted batch (moon#513), not per-key owner work.

## Parity probe (redis-compat method)

`ws8-rf-1 --shards 4` vs `redis-server` 7.0.15, redis-cli, 8 keys spread over the shards:
`EXISTS a..h a a zz` → 10/10; `TOUCH a b c d x y a` → 5/5; `DEL a b c zz a b` → 3/3;
`UNLINK d..h zz` → 5/5; `MSET k1 v1 k2 v2 k3` → same wrong-args error; 6-pair `MSET` → OK
and `MGET` identical; `DEL`/`EXISTS` of the 6 → 6 / 0; `SCRIPT FLUSH`, `… sync`,
`… ASYNC` → OK; `SCRIPT FLUSH nope`, `… SYNC ASYNC` → `ERR SCRIPT FLUSH only support
SYNC|ASYNC option` on both. 15/15 identical.

Consistency-script rows (`scripts/test-consistency.sh`, `scripts/test-commands.sh`)
were not added: no command was added, the reply shapes are unchanged, and those
shared scripts are outside this workstream's files; the `--shards 2/4` behaviours are
pinned by the `tests/perf_ws8_*` integration tests instead.

## Tokio leg (`--no-default-features --features runtime-tokio,jemalloc`, debug)

By name, green: `perf_ws8_spanning_del_hooks` (3), `perf_ws8_script_flush_fanout` (2),
`perf_ws8_mset_bgsave_capture` (1), `perf_ws8_spanning_write_merge` (1),
`perf_ws8_watch_versions` (1, with the flag pin), `perf_ws8_ft_scatter` (1),
`ft_search_multi_shard_as_of` (2), `client_tracking_invalidation` (10),
`pipeline_cross_shard_ordering` (18), `watch_cas_transactions` (10).
`pipeline_auto_index` and `hybrid_filter_multishard` compile to 0 tests on this leg
(they need `text-index`, absent without default features) — they are covered on monoio.
Lib tests (`shard::`, `server::conn`, `scripting::`): monoio 598 passed, tokio 548
passed, each including this workstream's 16 new unit tests.

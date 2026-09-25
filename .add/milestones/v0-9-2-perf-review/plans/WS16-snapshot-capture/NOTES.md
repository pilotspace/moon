# WS16-snapshot-capture — working notes

Branch `perf/ws16-snapshot-capture`, base `016ca5a` (int/part3b + the part-4 plan).
Personas: storage-durability-engineer (lead), ci-test-integrity-engineer.
ADD-style notes kept outside `.add/state.json` (TEAM-RULES: no `add.py` state changes).

## ORIENT

Which recovery path trusts the file this workstream fixes (read from
`shard::persistence_tick`, `persistence::snapshot`, `persistence::snapshot_cow`, and
WS12's ORIENT table, re-checked on `016ca5a`):

| producer | consumer at restart | tail replayed on top |
|---|---|---|
| explicit BGSAVE / `--save` (`handle_pending_snapshot`, `check_auto_save_trigger`) | `restore_from_persistence_v2` | legacy `appendonly.aof`, else WAL v3, else NOTHING |
| same with `--disk-offload enable` | `recovery.rs` v3 path | WAL v3 from `last_lsn` |

RDB-only (`--appendonly no`, no WAL) has no tail: the file alone is the recovered state,
so every capture gap is directly visible after `kill -9` + restart. That is the mode the
integration tests use. Both runtimes share `persistence_tick` / `SnapshotState` /
`snapshot_cow`; none of the four writer fixes below is runtime-specific.

## moon#1228 item 1 — writers that bypass pre-image capture

### Mechanism verified (016ca5a)

A write under an armed epoch must call `snapshot_cow::capture_*` for every key it will
change BEFORE changing it (first capture wins; absence is captured as a tombstone). The
generic choke point is `command::dispatch` → `capture_dispatch_pre_image`. The writers
below never reach it:

1. **MOVE / COPY … DB n.** Every live path intercepts them before `dispatch` because they
   need two databases, and calls `move_cmd::move_core` / `copy_core` directly or through
   `TwoDbOp::apply`: `handler_monoio/mod.rs` (MOVE, COPY), `handler_sharded/mod.rs` (MOVE,
   COPY), `shard/spsc_two_db.rs` (routed MOVE, COPY), `server/conn/shared.rs` (both MULTI
   executors via `TwoDbOp::apply`), `scripting/bridge.rs::run_two_db_op` (via
   `TwoDbOp::apply`), `replication/apply.rs::apply_two_db` (replica), `persistence/replay.rs`
   (startup replay), `handler_single.rs` (not shipped). None captures on either database.
   A script's `capture_command_pre_image` does run for MOVE/COPY but captures only the
   keyspec's write position IN THE SOURCE db (`MOVE k` → `(src, k)`; `COPY a b DB n` →
   `(src, b)`, the wrong database) — never the destination.
   - `MOVE k 1` after db 0's range with `k` was written, before db 1: the file has `k` in
     db 0 AND db 1 (duplicate). With a WAL tail the replayed MOVE finds the destination
     occupied, answers `:0`, and `k` stays in db 0 — resurrected.
   - `MOVE k 1` with `k`'s db 0 range still pending: db 0's serialization no longer sees
     `k`, db 1's does → the file has `k` in the wrong db.
   - `MOVE k 0` from db 1 into a db 0 range already written: db 1 no longer has it, db 0's
     range is past → `k` is missing from the file entirely.
   - `COPY a b DB 1 [REPLACE]`: db 1 later serializes the copy (a key that did not exist at
     epoch start, or the post-REPLACE value).
2. **Workspace drop sweep.** `WS DROP` deletes every `{wsid}:` key of every db with
   `db.remove` in three copies of one loop: `handler_monoio/write.rs` (owner = self),
   `handler_sharded/write.rs` (owner = self), `spsc_handler.rs` `WsDropCleanup` arm (routed).
   No capture: a dropped key in a pending range is missing from the file.
3. **MQ.** `shard/mq_exec.rs` runs MQ.CREATE (`get_or_create_stream`), PUSH
   (`get_stream_mut` + `add`), POP (`get_stream_mut` + `read_group_new` + PEL release +
   `xack` + DLQ `get_or_create_stream` + `add`) and ACK (`get_stream_mut` + `xack`) on the
   owner shard with no capture. Siblings found by reading every `get_stream_mut` /
   `get_or_create_stream` outside `command/`: the TXN.COMMIT MQ-push intents
   (`spsc_handler.rs` `MqTxnMaterialize` arm, `handler_monoio/txn.rs`, `handler_sharded/txn.rs`)
   and the replica apply of MQ records (`shard_databases::apply_mq_*`, called from
   `replication/apply.rs` at runtime and from WAL replay at startup).
4. **Stream wake.** `blocking/stream_wake.rs::try_wake_stream_waiter_budgeted` writes the
   stream in two places: `group_reader_ready` creates a missing consumer
   (`get_stream_mut_unsignalled` + `create_consumer`), and `serve_group_read` runs the
   `>` read (`get_stream_mut` + `read_group_new`: PEL entries, consumer pending set, group
   cursor). `wakeup::serve_ready_key` captures the key before calling it (WS12), but three
   other callers do not: the `BlockRegister` arm (`spsc_handler.rs`, the re-check right
   after a remote registration), `blocking::group::register_group` (multi-key remote
   registration), and `wakeup::recheck_group_readers` (after an unsignalled removal).

### Fix design

- **`snapshot_cow::capture_two_db(src, src_idx, src_key, dst, dst_idx, dst_key)`** (the
  WS12 NOTES design), called INSIDE `move_core` / `copy_core`, which now take both
  database indexes. Putting the capture in the cores instead of at each intercept makes
  coverage a compile-time property: every present and future caller has to supply the
  indexes, and the capture comes with them. The caller edits (index arguments only, no
  logic) are one isolated cross-ownership commit. MOVE captures `(src, key)` and
  `(dst, key)`; COPY captures `(dst, dst_key)` only (the source is read). Captured
  unconditionally while armed (a MOVE of a missing key or a refused COPY costs one clone
  or tombstone, never a wrong file).
- **WS drop sweep:** capture per key, not an epoch abort. A prefix sweep is an admin-rare
  bulk DEL, and per-key capture is exactly what `DEL k1..kN` already pays through
  `dispatch`; an abort would fail the whole BGSAVE for every WS DROP during a save, the
  liveness problem item 2 removes for FLUSHDB. One `snapshot_cow::capture_write_pre_image`
  call per swept key, in the three sweep loops (cross-ownership, loop body only).
- **MQ:** one capture per written key in `mq_exec.rs` (queue key for CREATE/PUSH/POP/ACK,
  plus the DLQ key for POP), inside the existing `with_shard_db` closures, before the
  first mutating accessor. Siblings: the three TXN MQ-push loops and `apply_mq_*`
  (cross-ownership, one call each).
- **Stream wake:** capture inside `stream_wake.rs` right before the two mutating calls,
  so all four callers are covered in the owned file and a wake that serves nothing (the
  common XREAD case) captures nothing.

### Risks

- `capture_two_db` clones the destination entry on a COPY that turns out refused (no
  REPLACE, destination exists) — one clone while armed only.
- The sweep captures up to every workspace key of the shard while armed (a deep clone per
  existing key); held until its range is written, like any DEL. Counted by item 2's
  `current_cow_size`.
- The capture adds one thread-local `bool` load to each MOVE/COPY/MQ/WS-drop/stream-wake
  when no BGSAVE runs.

## moon#1228 item 2a — FLUSHDB / FLUSHALL / SWAPDB discard the whole epoch

### Mechanism verified (016ca5a)

`snapshot_cow::note_flush` (the FLUSH arm of the dispatch hook) and `note_swapdb`
(called by `ShardDbSet::swap`) queue an ABORT whenever the flushed / swapped database is
one the epoch has not finished (FLUSHDB of an empty table and databases already written
excepted). The next drain fails the snapshot: `rdb_last_bgsave_status:err`, previous file
kept. A workload that runs FLUSHDB (or a SWAPDB-based dataset refresh) more often than one
full save takes never completes a BGSAVE. redis does not kill its child on FLUSHDB: the
fork keeps the pre-flush pages, so the save completes with the pre-flush image.

### Fix design — the epoch keeps its own view of each database

`SnapshotState` gets one `Source` per database of the EPOCH (the logical db the file
records): `Live(slot)` — read the table in shard slot `slot` — or `Frozen(table)` — a table
detached by a flush. The capture side (`snapshot_cow` thread-local `Progress`) keeps the
inverse, `logical_of_slot`.

- **FLUSH\*:** `Database::clear` is the one place a flush replaces a table (FLUSHDB,
  FLUSHALL's selected db through `dispatch`, FLUSHALL's other dbs through
  `flush_every_database`, scripts' deferred flush, the SPSC arm). It now hands the old
  table to `snapshot_cow::note_cleared_table` instead of dropping it. If the slot maps to a
  logical db the epoch has not finished, the table is FROZEN into the epoch (moved, O(1),
  no clone) and the slot is unmapped: its new contents are post-epoch, so writes to it
  capture nothing. The frozen table is never written again, so it and the pre-images
  captured before the flush are exactly that db's epoch-start state. Otherwise (slot
  already written, or unmapped) the table is dropped where `clear` dropped it before.
- **SWAPDB a b:** `note_swapdb` swaps `logical_of_slot[a]` and `[b]` at once (captures
  after the swap map to the right logical db) and queues the swap for the state, which
  swaps its `Live(a)` / `Live(b)` sources. The persistence tick then asks the state which
  slot holds the current database (`source_db_index`, one-line cross-ownership edit in
  `persistence_tick.rs`) instead of assuming slot == logical.
- **Slot identity for `clear`:** `clear` only has `&mut self`, and `Database.db_index`
  is not a slot identity (`ShardDbSet::swap` swaps it with the contents, `*live = temp`
  resets it). The epoch records each slot's ADDRESS at arm time (the databases live at
  fixed addresses inside the shard's `ShardDbSet`; a SWAPDB swaps contents, not
  addresses); `note_cleared_table` looks its `&Database` up by address. No slot known
  (a thread with no shard slice): the old behaviour, abort an unfinished epoch.
- A replica full resync keeps its abort (`note_table_replace`, WS12 F6), queued before its
  `clear` calls: the resync is not a record of the replica's own log, so a pre-resync image
  with the post-resync WAL tail replayed on top would mix the two datasets. The tables its
  `clear` calls freeze are dropped with the aborted epoch. A FLUSH or SWAPDB IS logged, so a
  pre-flush image plus the replayed FLUSH lands on the live keyspace.

### Risks
- Memory: a flushed-but-unwritten table stays allocated until the epoch passes its
  database (then it is dropped on the shard thread, as `clear` dropped it before) — the
  same memory a fork keeps. Counted by item 2b's `current_cow_size`.
- The address table must describe the slots the tick serializes from; recorded once per
  arm from the shard's own `ShardDbSet`, whose boxed slots never move.
- One-line edit in `persistence_tick.rs` (WS19's area): `advance_snapshot_segment` reads
  `snap.source_db_index()` instead of `snap.current_db_index()` for the db it borrows.

### Fixtures other workstreams' tests built on the old abort (cross-ownership, test-only)
- `perf_ws12_bgsave_split::table_swaps_during_bgsave_fail_the_save_not_the_server` asserted
  `rdb_last_bgsave_status:err` after FLUSH*/SWAPDB mid-save — the behaviour moon#1228 removes.
  Renamed `…_keep_the_save_point_in_time`: status `ok`, and the snapshot restored alone after
  SIGKILL holds every `pre:` key in db 0 and nothing in db 1. It is the real-server red test of
  item 2a (red on baseline-ae21476: status `err`).
- `perf_ws12_bgsave_split::an_aborted_bgsave_cannot_corrupt_the_next_one` (moon#1227 F1) used a
  FLUSHALL to abort a save whose writer holds a backlog. The one production abort left is a
  replica full resync, so the test now makes the node a replica of an empty master mid-save
  (`REPLICAOF` → link up → `REPLICAOF NO ONE`). The resync takes 50–300 ms on a debug build,
  and a save whose walk finished first is — correctly — not aborted, so the attempt is set up
  again with a longer walk (small keys interleaved with the big values) until the log shows the
  abort. The F1 property (the next save restores) is asserted unchanged.
  **monoio only** (2037de5): a master answers PSYNC only under runtime-monoio ("-ERR PSYNC
  requires runtime-monoio on the master"), so on the tokio leg the test is `ignore`d with that
  reason. The writer-cancel logic is runtime-independent and its unit guards
  (`stream_tests::an_aborted_snapshot_cannot_*`) run on both runtimes — the tokio END-TO-END F1
  guard is what no longer runs (it used to, via FLUSHALL).
- `perf_ws15_bgsave_status` forced its failed save with the same FLUSHALL. It now squats a
  directory on every shard's snapshot path, so the writer's final `rename` fails (EISDIR) —
  deterministic, no timing window; every status/LASTSAVE/dirty assertion is unchanged.

## moon#1228 item 2b — convergence under insert rate, and COW memory visibility

### Mechanism verified (016ca5a)
`advance_budgeted_db` stops a tick after a CONSTANT 1,024 entries / 64 segments / 1 MiB. An
insert into a pending range costs a tombstone (overflow pre-image, first-wins set is skipped for
tombstones) and splits pending segments the walk must still visit. When the pending range
receives more new segments per tick than 64, the walk falls behind for as long as the flood
lasts, and the overflow map grows by one tombstone per insert meanwhile. Nothing reports that
memory: `used_memory` never saw pre-images, the dedupe set or (new in 2a) frozen tables.

### Fix design
- **Budget scales with the backlog:** a tick's entry and segment budgets are multiplied by
  `1 + pending_pre_images / 1,024`, capped at 16× (1,024 segments / 16,384 entries per tick);
  the byte budget by the same factor capped at 4× (4 MiB — large values must not turn one tick
  into a stall), and the writer-backlog check still stops a tick. Pending pre-images are the
  direct measure of how far writers are ahead of the walk (every one is a write that hit a
  range not yet written), so the walk speeds up exactly when the flood would out-split it and
  falls back to the old per-tick cost otherwise. Worst tick grows from ~100 µs to ~1.6 ms,
  only under a flood. Measured (deterministic, `cow_budget_tests`): with the constant budget
  the walk does converge but slowly — its pace in hash space shrinks as the flood grows the
  table (99 ticks, 73,708 peak pre-images at 5,000 inserts/tick over 50K keys); scaled, 7
  ticks and 7,369.
- **Visibility — INFO `current_cow_size`, not `used_memory`.** `used_memory` drives
  `maxmemory` eviction: counting a save's transient copies there would evict (or refuse)
  user writes because a BGSAVE is running, and the bytes vanish as soon as the walk passes
  their range. redis does not count fork COW in `used_memory` either; it reports it as
  `current_cow_size` in INFO persistence — the field operators already graph. moon reports
  the same name: per shard, pre-images (entry estimate or key + node for a tombstone),
  frozen tables (their `used_memory` at flush), and the first-wins key set, summed across
  shards; 0 when no save runs.

### Risks / deferred
- The Linux-perf-host measurement at pipelined insert rates (the issue's second half) is
  DEFERRED: this box is a 4-vCPU shared container, and the numbers would not transfer.
  The deterministic in-process flood test is the evidence here.

## moon#1250 (added by the orchestrator) — MQ writes never charged to used_memory

### Mechanism verified (016ca5a)
moon#1163 (part 3b) keeps a per-stream `unbilled` byte delta that every stream mutation
updates and every X* command drains into `used_memory` (`Stream::take_unbilled` → `bill`).
The MQ subcommands (`mq_exec`), the TXN MQ.PUBLISH materialization, the replica MQ apply and
the stream waker's group reads mutate streams outside those commands and never drain it.
Second half: MQ is intercepted by both connection handlers BEFORE the generic write path, so
it never ran the maxmemory / per-db-quota gate either — billing alone could not make
`maxmemory` bind. Red, `perf_ws16_mq_billing` on 864a3c8 (== 016ca5a for MQ): `--shards 1`
accepted 20,000 of 20,000 pushes, MEMORY USAGE q 5,720,321, used_memory +24,534.

### Fix design
- `mq_exec::bill_stream_delta(db, stream.take_unbilled())` after every stream mutation:
  CREATE, PUSH, POP (main stream incl. release/dead-letter ack, the DLQ stream, and the early
  returns after a claim), ACK, `materialize_mq_intents`; the stream waker's consumer creation
  and `>` read; the replica `apply_mq_{create,push,ack}` and the replica DLQ stream.
- `execute_mq_on_owner` takes the caller's write gate (`MqWriteGate`), run (only while a limit
  is configured) before CREATE and PUSH — the two subcommands that grow the keyspace; POP/ACK
  are never refused (they shrink or keep it; redis's XREADGROUP/XACK are not `denyoom`
  either). Callers pass the gate their runtime already runs for any other write: monoio
  `run_write_eviction_gate`, the tokio arm's `evict_to_budget` + per-db quota
  (`handler_sharded::write::mq_write_gate`), and on a routed hop `spsc_eviction_gate` with the
  same plain-drop DEL records the routed write arms emit.

### Residual
- The replica `apply_mq_pop` writes the PEL through `group.pel` directly (not through a
  `Stream` method), so those bytes are not tracked in `unbilled` at all — a replica-only
  under-count (never an over-credit). Not touched here.

## Capture-site audit table (WS12's table, updated at the end of WS16)

Every path that mutates a key's table entry while a BGSAVE epoch may be armed, and how the
epoch learns the key's epoch-start state. "slot" = the shard slot the write runs in; captures
are filed under the epoch database that slot's table belongs to (moon#1228 item 2a).

| writer | capture | since |
|---|---|---|
| `command::dispatch` (local, MULTI/EXEC, scatter, SPSC `Execute`/`MultiExecute`, scripts' `redis.call`, replica apply of commands) | every `KeyRole::Write` position | moon#558 / moon#1217 |
| routed arms (`spsc_handler::cow_intercept`) | same queue as dispatch | WS12 |
| monoio inline SET | `capture_key_pre_image` | moon#558 |
| coordinator MSET local legs | via `run_local` → dispatch | WS8 (part 3a) |
| blocking wakers: list / zset pops, BLMOVE dst (`wakeup.rs`) | `capture_wake_pre_image` | moon#1217 |
| blocking commands served on the spot / in MULTI | dispatch hook | moon#1227 F2 |
| **MOVE / COPY … DB n** — every path (connection intercepts ×2 runtimes, SPSC, both MULTI executors, scripts, replica apply, replay) | `capture_two_db` inside `move_core` / `copy_core` (indexes are a required argument) | **WS16 c83598d** |
| **WS DROP key sweep** (monoio / tokio owner leg, routed `WsDropCleanup`) | per key in `workspace::sweep_prefix` | **WS16 035b774** |
| **MQ CREATE / PUSH / POP (+DLQ) / ACK** (`mq_exec`) | `capture_write_pre_image` per written key | **WS16 0328e7a** |
| **TXN.COMMIT MQ.PUBLISH materialization** (self legs ×2, `MqTxnMaterialize`) | per intent in `materialize_mq_intents` | **WS16 0328e7a** |
| **replica MQ apply** (`apply_mq_{create,push,pop,ack,drop}`) | per written key | **WS16 0328e7a** |
| **stream waker group reads** (all four callers) | in `stream_wake.rs` before consumer creation and the `>` read | **WS16 fc1c863** |
| **FLUSHDB / FLUSHALL** (every path — all reach `Database::clear`) | the detached table is frozen into the epoch | **WS16 64816f8** (was: abort) |
| **SWAPDB** (`ShardDbSet::swap`) | slot ↔ epoch-database map follows the tables | **WS16 64816f8** (was: abort) |
| replica full resync (`load_snapshot`) | abort (`note_table_replace`) — foreign data | moon#1227 F6 |
| TXN.ABORT KV undo (`transaction::abort`) | not captured. A key the TXN wrote inside the epoch was captured by that write (dispatch), so the file holds its epoch-start state; for a TXN whose writes preceded the epoch the file gets the restored pre-TXN value instead of the uncommitted one — **open question, residual 2** | — |
| active / lazy expiry, hash-field TTL sweep | none — safe: TTLs are absolute, the loader filters | — |
| eviction victims, plain drop and spill (`storage::eviction`) | **none** — a victim in a pending range is missing from the file | residual (moon#1185 blocker) |
| spill-completion failure re-insert (`persistence_tick`) | none — re-inserts the value eviction removed (same key, same value) | — |

Residuals (not in WS16's plan, recorded for the next wave):
1. **Eviction** removes keys without a pre-image. For BGSAVE that is a key missing from a
   point-in-time image (eviction is lossy by contract, redis's fork would still hold it); for
   an incremental AOF fold it is silent loss (see moon#1185 below).
2. **TXN.ABORT** undo writes (`transaction::abort::abort_cross_store_txn`) restore old entries
   with `db.set`/`db.remove` outside dispatch. When the TXN's writes ran BEFORE the epoch armed
   and the abort runs during it, the file gets the pre-TXN value, not the uncommitted value the
   keyspace held at epoch start. For an RDB-only restore that is arguably the better answer
   (uncommitted data is not persisted); for an exactly-once fold it is a state change with no
   pre-image. Needs a decision, not a one-line capture — not changed here.

## moon#1185 remainder — incremental COW AOF fold: re-assessed, DEFERRED

WS12 deferred the fold because three mutation paths took no pre-image: MOVE / COPY … DB n,
eviction with spill, replica full resync. After WS16:
- MOVE / COPY … DB n: captured (c83598d). ✔
- replica full resync: aborts an unfinished epoch (moon#1227 F6) — for a fold consumer that is
  "fold fails, old generation stays committed", the existing failure path. ✔
- WS DROP, MQ, stream wake, FLUSH*/SWAPDB: now captured / followed. ✔
- **eviction (plain AND spill) still captures nothing.** For the fold's exactly-once argument a
  key evicted with spill after the fold cut F, in a range not yet written, is absent from the
  hot base while its cold copy lives in a spill file above the fold's cold watermark → lost on
  replay. A plain eviction after F makes the rewrite differ from the atomic fold. ✘
- TXN.ABORT undo (residual 2 above) needs a decision for the same argument. ?

So the WS12 design does not hold yet → **DEFERRED**. Exact remaining blocker: a pre-image
capture in `storage::eviction` for every victim (`evict_one*` / `evict_one_with_spill` /
`evict_one_async_spill`, before `db.remove`) — which needs the victim's db index threaded
through `EvictionRun` (the plain run has none today) — plus a decision on the TXN.ABORT
undo. Both are outside WS16's plan (`storage/eviction.rs`, `transaction/abort.rs`). Then the fold
itself (WS12 NOTES design: a consumer registry in `snapshot_cow`, each consumer with its own
cursor, overflow and slot map; the fold consumer serialized through `FoldImageSink`; aborts →
`FoldChunk::Failed`), which must also adopt the item-2a slot map and frozen tables, since a
FLUSH during a fold would otherwise abort it.

## Self-evaluation per issue (0–1: Completeness · Clarity · Practicality · Optimization · Edge cases · Self-evaluation)

- **1228 item 1 (writers):** 0.95 · 0.9 · 0.95 · 0.95 · 0.9 · 0.9. Every named writer plus the
  siblings found by grepping every `get_stream_mut` / `get_or_create_stream` / two-db call; the
  capture sits where the write is (cores, shared sweep, `materialize_mq_intents`, the waker), so
  new callers inherit it. Refined once: the first cut captured MOVE/COPY at the intercepts; moved
  into the cores to make coverage a compile-time property. Stream wake has no real-server test
  (no deterministic trigger) — the in-process test is the guard.
- **1228 item 2a (FLUSH/SWAPDB):** 0.92 · 0.9 · 0.9 · 0.95 · 0.9 · 0.9. O(1) freeze, SWAPDB
  followed through a slot map, randomized interleavings (12 seeds). Refined: the first F1
  fixture rewrite missed its window 4 of 5 times on a debug build; now retried on a longer walk
  and measured. Residual risk: slot identity by address (documented, falls back to abort).
- **1228 item 2b (budget + visibility):** 0.9 · 0.9 · 0.9 · 0.9 · 0.9 · 0.9. First test design
  assumed the constant budget "never converges"; the measurement showed it converges, slowly
  (the pace shrinks with table growth), so the test was reframed to the measured ratio (14x
  ticks, 10x tombstones) with a 4x bound. The Linux-perf-host number is DEFERRED by the brief.
- **1250 (MQ billing):** 0.92 · 0.9 · 0.95 · 0.9 · 0.9 · 0.9. Billing alone could not make
  `maxmemory` bind (MQ bypassed the gate), so the gate was added too; every stream mutation
  outside the X* commands now drains. Replica `apply_mq_pop` PEL bytes remain untracked (noted).
- **1185 remainder:** DEFERRED with the exact blocker (eviction capture, TXN.ABORT decision).

## Final gate run (HEAD 2037de5; debug builds of this branch copied to /home/user/wt/bin and pinned)

- `cargo fmt --check` clean; `scripts/audit-unsafe.sh` PASS (0 missing SAFETY);
  `scripts/audit-unwrap.sh` PASS (baseline 0).
- `cargo clippy --all-targets -- -D warnings` (monoio) exit 0;
  `cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings` exit 0;
  `cargo check --all-targets --no-default-features --features runtime-tokio,jemalloc` exit 0
  (all at 396dc1e; 2037de5 changes two test files only, re-clippied on both runtimes).
- `cargo test --lib` filtered to persistence::snapshot, shard::persistence_tick, shard::mq_exec,
  blocking::stream_wake, blocking::wakeup, command::keyspace::move_cmd, scripting::bridge,
  shard::spsc_two_db, shard::shared_databases, replication::apply, shard::db_plane, workspace,
  server::conn::tests, storage::db: monoio 581 passed / 1 ignored; tokio 545 passed / 1 ignored.
- Integration, monoio (`MOON_BIN=/home/user/wt/bin/ws16-dbg-monoio-head`): perf_ws16_bgsave_capture
  6/6, perf_ws16_mq_billing 2/2, perf_ws12_bgsave_split 4/4, perf_ws15_bgsave_status 3/3,
  perf_ws8_mset_bgsave_capture 1/1, move_copy_db_crash_recovery_1046 4/4, bgsave_startup_race 1/1,
  and with `--include-ignored`: multi_move_copy_db_1062 8/8, script_move_copy_db_1068 9/9,
  replication_mq 4/4, replication_swapdb 3/3, crash_recovery_mq_effects 3/3.
- Integration, tokio (`MOON_BIN=/home/user/wt/bin/ws16-dbg-tokio-head`): perf_ws16_bgsave_capture
  6/6, perf_ws16_mq_billing 2/2, perf_ws12_bgsave_split 3/3 + 1 ignored (F1, above),
  perf_ws15_bgsave_status 3/3, perf_ws8_mset_bgsave_capture 1/1, mq_integration 17/17,
  workspace_integration 13/13, multi_move_copy_db_1062 6/6, script_move_copy_db_1068 6/6,
  move_copy_db_crash_recovery_1046 4/4, kill_snapshot 4/4, bgsave_startup_race 1/1.

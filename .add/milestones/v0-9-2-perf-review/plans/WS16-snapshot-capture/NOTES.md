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
  with the post-resync WAL tail replayed on top would mix the two datasets. A FLUSH or SWAPDB
  IS logged, so a pre-flush image plus the replayed FLUSH lands on the live keyspace.

### Review 4 — FLUSHALL aborts again (redis parity), FLUSHDB / SWAPDB keep the freeze

redis's FLUSHALL runs `flushAllDataAndResetRDB()`, which calls `killRDBChild()`: an
in-flight RDB save is aborted. 64816f8 froze every table a FLUSHALL detached instead, so the
save completed with the pre-flush image — and held the whole pre-flush dataset beside the
new one until the walk passed each table (up to ~2x RSS). ad3a795b restores the abort for
FLUSHALL only: the dispatch hook queues it when the command will accept its arguments
(`flush_args_accepted`, restored) and the epoch still has databases to write. While an abort
is queued (FLUSHALL, replica resync) `note_cleared_table` drops each detached table at once
instead of freezing it until the next drain (review nit). FLUSHDB and SWAPDB keep the
freeze / slot map — redis keeps its child for those.

**FLUSHDB of every database in a loop** (FLUSHALL by another name). Review 4's claim that
it held "at most the epoch-start dataset" was WRONG (review 5): only the COUNT was bounded.
A flushed slot is unmapped, so only the FIRST flush of each epoch database freezes a table,
but that table was frozen AS FLUSHED, with every row inserted since the epoch began (all
tombstoned, useless to the file). The pin test flushed BEFORE inserting, so it never saw
one. The reviewer's reproduction held one save open under `--maxmemory 64mb` and ran
8 x (40 MB of SETs into db N, FLUSHDB). It measured `current_cow_size` 344,782,240 and RSS
+341 MB, with `used_memory` still at the epoch-start 302,165 B. Main aborts that save
instead (RSS +48 MB).

**The fix (review 5):** the drain trims a flushed table to its epoch-start rows before it
freezes it (`SnapshotState::trim_to_epoch_start`, `snapshot/frozen.rs`). The trim has three
steps:
1. Every key written since the epoch began has a pre-image in the database's overflow map.
   The drain now folds the queued captures in before it applies table events, and nothing
   captures for the slot after the flush. The trim removes that key's post-epoch row and
   puts the epoch-start entry back (a tombstone puts nothing back), which empties the map.
2. For the database in progress it also drops the rows below the cursor: written already,
   or post-epoch writes into a written range.
3. If the post-epoch rows had more than doubled the table's epoch-start segment count, it
   moves the kept rows into a table sized for them, so no skeleton of the inserts survives.

The kept rows are a subset of the epoch-start rows with their epoch-start entries. After
every drain the epoch therefore holds at most the epoch-start bills of the databases it has
not written, and releases each as the walk passes it. This is the same worst case as redis's
FLUSHDB child. The bill is the table's `used_memory` at the flush, minus the removed rows'
`entry_overhead`, plus the restored rows'.

**The wait before the drain.** Until the drain (one tick) a flushed table waits whole. One
grown table may wait: its rows were in `used_memory` an instant before. A second flush of a
grown database before the drain (MULTI, a script) fails the save once the waiting post-epoch
bytes pass `FREEZE_WAIT_SLACK` (8 MiB). An abort now also releases waiting tables at once.

**Cost.** All on the shard thread at the drain:
- step 1: one remove plus insert per key written since the epoch began, already paid for at
  capture;
- step 2: the segments below the cursor, already visited by the walk;
- step 3: only after a doubling, at most the epoch-start rows.

**Evidence:**
- `flushdb_of_every_database_holds_at_most_the_epoch_start_tables` now inserts 300 x 1 KiB
  and THEN flushes, 20 rounds x 4 dbs, and checks after every drain. It went red on the old
  code ("round 0, db 1: the epoch holds 1297793 B after a drain; the epoch-start dataset is
  1123560 B") and is green now.
- `a_frozen_table_keeps_no_skeleton_of_post_epoch_rows` went red (515 of 515 segments kept,
  epoch-start 2) and is green for a pending database and for the one in progress.
- `grown_tables_flushed_before_one_drain_fail_the_save` went red (the save completed) and is
  green.
- Real server, `perf_ws16_bgsave_capture::flushdb_after_post_epoch_inserts_holds_only_the_epoch_start_rows`
  (the reviewer's reproduction adopted): red on the old binary (`current_cow_size`
  344,782,240, RSS +341 MB), green 3 of 3 (`current_cow_size` 0, RSS +49..+58 MB). The save
  completes and restores db 0's 2,000 keys with dbs 1-8 empty.
- The randomized flush/swap workload passed over 400 seeds (12 committed).

**Property tests (review 6, adopted).** The reviewer checked fix A with two property tests,
now permanent:
- `persistence::snapshot::prop_tests` (lib): random mixed-type workloads, including MOVE,
  COPY, SWAPDB, expiries, lazy-free UNLINKs, FLUSHDB (biased onto the database in progress)
  and grow-then-FLUSHDB, with held, one-segment and budgeted ticks. After every drain the
  frozen rows must stay within the epoch-start bills of the unwritten databases. At the end
  the file must be EXACTLY the epoch-start image, and file + tail the live keyspace. It runs
  16 seeds by default (~6 s debug); `MOON_TEST_SNAPSHOT_PROP_SEEDS` runs more. The reviewer
  ran 2,000 with no counterexample. Here 64 seeds pass in 25.6 s, with 223 frozen flushes,
  61 rebuilds and a max bill error of 0.
- `tests/perf_ws16_bgsave_prop.rs` (real server): a random workload while the hold is
  toggled, then kill -9 and restore the file alone. It runs 3 seeds at `--shards 1` and 3 at
  `--shards 4` (20 s); the reviewer ran 80.
- **Mutation evidence** (re-run here):
  - Removing the pre-image restore from the trim makes seed 1 red: "the file is not the
    epoch-start image: 5 missing".
  - Applying the table events BEFORE the captures in `drain_into` makes the bound check red
    at seed 1: "db 0's frozen rows are 30090 B; its epoch-start bill 0 B". The trim ran
    without that tick's tombstones, so post-epoch rows stayed frozen.

**Review 6, S1: the trim is budgeted.** Review 5's trim ran whole inside one drain on the
shard thread. The reviewer measured it (release-fast, in-process):
- a 1M-row db in progress, cursor at 50%: 62 ms, against 27 ms for main's FLUSHDB drop;
- 0.5M epoch-start + 1.5M post-epoch rows in a pending db: 0.88 s;
- 2M + 6M: 4.33 s.

Now `snapshot/frozen.rs` keeps a per-table state machine: pre-images, then written rows,
then an optional rebuild. The drains advance it by at most `TRIM_BUDGET` = 512 row
operations in all, lowest database first; a segment that would not fit waits for the next
drain.
- **The file never depends on the trim.** Until a key's pre-image is folded into the table,
  the walk shadows the table's row with it, and the walk never reads below its cursor. So
  steps 1 and 2 run while the walk writes the same database. Only the rebuild (rows moving
  between tables) makes the walk wait a tick.
- **Seed 104 found a bug in the first version.** With the walk interleaved, it takes the
  pre-images of the range it passes itself, and those keys' post-epoch rows stayed below the
  cursor. Step 2 now covers the cursor as it stands when step 1 ends. The property test was
  red at seed 104 ("db 3's frozen rows are 674492 B; its epoch-start bill 658842 B") and is
  now green over 400 seeds. Half of those run with a tiny random budget (1-40), giving 5,704
  mid-trim observations and 11 ticks where the walk waited on a rebuild.
- **The byte bound holds once the trim is done:** after ceil(work / 512) drains, where work
  = keys written since the epoch began + rows below the cursor + (on a rebuild) the rows kept.
  8 x 10K rows: about 20-40 drains each. 2M + 6M: about 16K drains.
- **The rebuilt table grows by splits** (`Table::new`), since `with_capacity` adds a depth
  level of headroom. The emptied old table, and any removed post-epoch value of 4,096 or more
  elements, are freed on a lazily started `moon-snapdrop` helper thread (the `moon-lazyfree`
  pattern). The old 8M-row skeleton alone took 19.6 ms to free inline.

Measured end to end: release-fast server (jemalloc, its baked `background_thread:true,
dirty_decay_ms:1000`), `--shards 1`, a held save. The db gets N epoch-start rows then 3N
post-epoch rows, then FLUSHDB, while a second connection PINGs in a loop:

| Case | FLUSHDB reply | PING after it: p99 / p99.9 / max | bill at its final value |
|---|---|---|---|
| main's FLUSHDB (no save), 0.5M + 1.5M | 45 ms | max 45 ms | — |
| main's FLUSHDB (no save), 2M + 6M | 192-193 ms | max 192-193 ms | — |
| budget 512, 0.5M + 1.5M | 0.2 ms | 0.33 / 0.69 / 4.2 ms | 2.9 s |
| budget 512, 2M + 6M | 0.3 ms | 0.42 / 1.01 / 9.2 ms | 11.8 s |
| budget 2,048, 2M + 6M (rejected) | 0.3 ms | 1.0-1.5 / 10-27 / 427-865 ms | 3.1-3.4 s |
| budget 1,024, 2M + 6M (rejected) | 0.3 ms | 0.56 / 0.89 / 52.8 ms | 5.9 s |

Why the larger budgets stall:
- An operation costs ~0.1-1.3 us, so a 2,048-op drain can outlast the 1 ms tick, and the
  event loop then runs ticks back to back ahead of connection I/O.
- The 0.4-0.9 s stall at the end of a large rebuild is jemalloc returning the ~1 GB step 1
  freed while the rebuild allocates. With purging disabled
  (`background_thread:false,dirty_decay_ms:-1`) the max is 63 ms; with `dirty_decay_ms:0` it
  is 617 ms; leaking the old table instead of freeing it did not remove it.
- At 512 the purge spreads out over the longer trim, and no such stall was seen.

Lib-test timings (`table_swap_tests::trim_cost_of_a_large_flushed_table`, `--ignored`) run
on glibc: lib tests use the system allocator. glibc's fastbin consolidation put 26-98 ms
into single `BTreeMap::pop_first` calls; `GLIBC_TUNABLES=glibc.malloc.mxfast=0` removes it
(worst drain 13 ms). So those numbers are not the server's. The cheap budget assertion is
`table_swap_tests::one_drain_trims_at_most_the_budget`: a 50,000-row table, at most
`TRIM_BUDGET` operations per drain, the trim done after about ceil(ops / budget) drains, and
bound and file correct. It was red on the unbudgeted trim ("one drain trimmed 50000 rows").

The frozen figure is `used_memory` at the flush, not `estimated_memory()` (52544bf3:
spill-in-flight payloads are not in the table; red 1,467,473 B vs 418,890 B with 1 MiB in
flight).

### Risks
- Memory: a FLUSHDB'd-but-unwritten table's EPOCH-START rows stay allocated until the
  epoch passes its database (then they are dropped on the shard thread, as `clear` dropped
  them before) — at most that database's epoch-start bill, the memory a fork keeps (review
  5 trim, above). Until the next drain the table waits whole (one grown table at a time;
  more fail the save). Counted by item 2b's `current_cow_size`. A FLUSHALL aborts and frees
  at once.
- The address table must describe the slots the tick serializes from; recorded once per
  arm from the shard's own `ShardDbSet`, whose boxed slots never move.
- One-line edit in `persistence_tick.rs` (WS19's area): `advance_snapshot_segment` reads
  `snap.source_db_index()` instead of `snap.current_db_index()` for the db it borrows.

### Fixtures other workstreams' tests built on the old abort (cross-ownership, test-only)
- `perf_ws12_bgsave_split::table_swaps_during_bgsave_fail_the_save_not_the_server` asserted
  `rdb_last_bgsave_status:err` after FLUSH*/SWAPDB mid-save — the behaviour moon#1228 removes.
  Renamed `…_keep_the_save_point_in_time`: status `ok`, and the snapshot restored alone after
  SIGKILL holds every `pre:` key in db 0 and nothing in db 1. It is the real-server red test of
  item 2a (red on baseline-ae21476: status `err`). Since review 4 its FLUSHALL case expects
  `err`, the abort logged and no file published; it waits for `shard-0.rrdshard.tmp` (the
  armed epoch, part 3b's method) instead of a fixed 30 ms sleep (3c4e73fc).
- `perf_ws12_bgsave_split::an_aborted_bgsave_cannot_corrupt_the_next_one` (moon#1227 F1) used a
  FLUSHALL to abort a save whose writer holds a backlog. 5f6728d re-triggered it with a replica
  full resync (monoio only, 2037de5). **Review 4 proved that rewrite NOT red:** with the
  writer-cancel fix (5d02343) reverted it passed (1 of 1, and the same fixture passed every
  later run on the reverted binary) — the small keys that let the resync land inside the walk
  left the writer no backlog to race with. Now (236f563e, 5487322e) one helper,
  `aborted_bgsave_then_resave`, runs two tests:
  - `an_aborted_bgsave_cannot_corrupt_the_next_one` — FLUSHALL (it aborts again), ~375 MiB of
    64 KiB values only (writer-bound), the walk held by `MOON_TEST_SNAPSHOT_HOLD_FILE` after
    128 MiB is written while the FLUSHALL lands, the next BGSAVE within a millisecond of its
    reply. **Both runtimes** — the tokio end-to-end F1 guard is back. RED on the reverted
    binary 5 of 5 runs alone ("its 167973249-byte file did not restore"); GREEN 3 of 3 beside
    the file's other tests, where freeing ~375 MiB in the FLUSHALL let the straggler drain
    first. F1 is a race; its deterministic guards are `stream_tests::an_aborted_snapshot_cannot_*`.
    Tried and dropped: six 64 MiB values (cheaper FLUSHALL, deeper backlog) was LESS red.
  - `a_resync_mid_bgsave_fails_it_and_the_next_save_publishes` — the resync abort path
    (`note_table_replace`) end to end, walk held until the resync is in (it used to need
    800K–3.2M keys and still missed up to three attempts: in a DEBUG build beside the other
    tests, link up took 0.7–1.7 s during a save vs ~40 ms idle). Review 5: a release build
    did not reproduce that. It was a debug-build and load artifact, not command dispatch
    starving during a save (see the item 2b trade-off below). Documented as NOT an F1
    regression. monoio only (PSYNC).
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

### Review 5 — the latency trade-off, measured (release A/B); decision: KEEP the scaling
The reviewer A/B'd release builds with and without the 2b budget scaling:

| Load during one BGSAVE (release, `--shards 1`) | Without scaling | With scaling (WS16) |
|---|---|---|
| SET flood, light: write p99 | 804 µs | 1,818 µs |
| SET flood, heavy: write p99 | 2,383 µs | 4,294 µs |
| SET flood: write p99.9 | — | +25–80% |
| SET flood: max latency | — | no worse |
| SET flood: save duration | 2.26 s | 0.88 s (light) / 0.77 s (heavy): 2.6–2.9x sooner |
| Reads only; and `--shards 4` | — | no regression |
| Worst stall, 76 release saves | 39 ms (100 B values), 92 ms (64 KiB) | the same |

**Trade-off.** Under a write flood on one shard, the scaled budget does up to 16x the
entries and segments (4x the bytes) per tick while writers are ahead of the walk. Each tick
is longer, so write p99 during the save roughly doubles and p99.9 rises 25–80%. The save
ends 2.6–2.9x sooner, which shortens the window in which every write to a pending range
costs a pre-image (the tombstone memory 2b exists to bound). Max latency and the worst
stall are unchanged. Reads and multi-shard loads show no regression.

**Decision (orchestrator): keep the scaling, document the trade-off.** CHANGELOG "Changed"
bullet in SUMMARY.

**Correction.** The 0.7–1.7 s replica-link delay during a save (F1 fixtures, above) did not
reproduce in release. It was a debug-build and load artifact. The release worst stall
(39 / 92 ms, both builds) is the measured bound. The two `perf_ws12_bgsave_split` comments
that read like a starved dispatch now say what was observed: a debug build beside other
tests.

### Risks / deferred
- The Linux-perf-host measurement at pipelined insert rates (the issue's second half) is
  DEFERRED: this box is a 4-vCPU shared container, and the numbers would not transfer. The
  release A/B above (review 5) covers latency and save duration on `--shards 1` and 4.

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

### Review 4 — a POP's released surplus is credited (d0bd7f4e)
`handle_pop` claims `COUNT + MAXDELIVERY` entries with `read_group_new` (billed: `PEL_SLOT +
PENDING_SLOT` each through `unbilled`), then releases the surplus. It removed the released ids
with direct `group.pel.remove` / `pending.remove` — untracked — so every POP billed PEL bytes
that no longer existed, and the next POP re-claimed and billed them again: `used_memory`
drifted up for ever (review proof: billed − true = 286,500 B after 500 POPs, control 0). The
release now goes through `Stream::xack` (credits both slots) and rewinds `last_delivered_id`.
Real server, `perf_ws16_mq_billing::pop_ack_churn_keeps_the_charge_exact_{single_shard,four_shards}`
(`--appendonly no`, 16 KiB backlog, settled `used_memory` reads, a `MAXDELIVERY 0` control
queue): prefix +97,586..+279,991 B over 500 POP/ACK vs MEMORY USAGE +176; fix +176..+367 on
both queues, 3 runs.

### Review 5 — the MqPop apply bills its claims (moon#1261)
Review 4 filed this as a residual: "the replica `apply_mq_pop` writes the PEL untracked — a
replica-only under-count (never an over-credit)". **That was wrong on both counts.**
- **Not replica-only:** `apply_mq_pop` also serves the master's own WAL replay.
- **An over-credit:** the claims were never charged, but `apply_mq_ack` credits them through
  `Stream::xack`. Every replayed or replicated POP+ACK credited ~191 B that was never
  charged, so the queue's bill drained toward 0.
- Reviewer's measurement: MEMORY USAGE q was 124,097 live and 28,421 after a restart's
  replay; 124,097 on the master against 28,421 on its replica.

**Fix:** the new `Stream::restore_claims` does what the apply did by hand, with every byte
tracked in `unbilled` as `read_group_new` / `xack` track it on the master:
- inserts the PEL entries and the consumer's pending ids, creating the consumer if needed;
- sets the cursor;
- removes the dead letters with `xack`.
`apply_mq_pop` then drains `take_unbilled` into `bill_stream_delta`, for both callers (replay,
replica).

**Evidence:**
- `restore_claims_bills_what_the_masters_claim_billed`: the same claims through
  `read_group_new` + `xack` move the same bytes.
- `test_replay_mq_wal_bills_pop_and_ack_like_the_live_server`: red (tracked 627 B vs a scan of
  1,185 B), now green.
- `perf_ws16_mq_billing::a_restart_bills_a_churned_queue_as_the_live_server_did` and
  `::a_replica_bills_a_churned_queue_as_its_master_does` (the reviewer's proofs, adopted):
  red with 28,421 vs 124,097 B; now exactly equal (124,097 = 124,097), 2 runs each.
- `::a_pop_surplus_release_agrees_on_master_replica_and_restart` (the reviewer's Q1 proof,
  adopted without its MULTI leg, moon#1262): master, replica and restart hold the same PEL
  and cursor, and serve the rest once, in order. It passed before and after.

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
| **replica `WS.DROP.APPLY`** (`replication::apply::apply_ws_drop` — a fourth sweep copy) | `workspace::sweep_prefix` | **WS16 review 4 f743bab2** (was: none; proof: 497 of 500 dropped keys missing from a replica's own BGSAVE) |
| **MQ CREATE / PUSH / POP (+DLQ) / ACK** (`mq_exec`) | `capture_write_pre_image` per written key | **WS16 0328e7a** |
| **TXN.COMMIT MQ.PUBLISH materialization** (self legs ×2, `MqTxnMaterialize`) | per intent in `materialize_mq_intents` | **WS16 0328e7a** |
| **replica MQ apply** (`apply_mq_{create,push,pop,ack,drop}`) | per written key | **WS16 0328e7a** |
| **stream waker group reads** (all four callers) | in `stream_wake.rs` before consumer creation and the `>` read | **WS16 fc1c863** |
| **FLUSHDB** (every path — all reach `Database::clear`) | the detached table is frozen into the epoch | **WS16 64816f8** (was: abort) |
| **FLUSHALL** (every path runs the dispatch hook first) | abort the unfinished epoch — redis parity (`killRDBChild`); the detached tables are dropped at once | **WS16 review 4 ad3a795b** (64816f8 had frozen them) |
| **SWAPDB** (`ShardDbSet::swap`) | slot ↔ epoch-database map follows the tables | **WS16 64816f8** (was: abort) |
| replica full resync (`load_snapshot`) | abort (`note_table_replace`) — foreign data | moon#1227 F6 |
| TXN.ABORT KV undo (`transaction::abort`) | not captured (review 4: `capture_write_pre_image`'s doc wrongly listed it as a caller; corrected in 52544bf3). A key the TXN wrote inside the epoch was captured by that write (dispatch), so the file holds its epoch-start state; for a TXN whose writes preceded the epoch the file gets the restored pre-TXN value instead of the uncommitted one — **open question, residual 2** | — |
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
  outside the X* commands now drains. The MqPop apply (replay and replica) bills its claims since
  review 5 (moon#1261).
- **1185 remainder:** DEFERRED with the exact blocker (eviction capture, TXN.ABORT decision).

## Gate run before review 4 (HEAD 2037de5; debug builds of this branch copied to /home/user/wt/bin and pinned)

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

## Review 4 (MERGE-AFTER-FIXES) — what changed, and the merge of main

| item | commit | evidence |
|---|---|---|
| 1 BLOCKING: MQ POP over-charged for ever (untracked surplus release) | d0bd7f4e (+ 04a0458d, a needless `mut`) | proof `r4_red_mq_pop_surplus_release_leaks_billed_bytes` red → green; real-server churn test above |
| 2 replica `apply_ws_drop`, a fourth sweep copy with no capture | f743bab2 | proof `r4_red_replica_ws_drop_apply_captures_pre_images` red (497 of 500 missing) → green; audit table row |
| 3 FLUSHALL parity (abort), FLUSHDB/SWAPDB keep the freeze; FLUSHDB-every-db bound | ad3a795b | 3 unit tests red with the hook + abort-drop neutralized; FLUSHDB bound test; real-server table-swap test |
| 4 fixed sleeps → wait for `shard-0.rrdshard.tmp` | 3c4e73fc | — (flakiness fix) |
| 5 prove the F1 fixture red | 236f563e, 5487322e | the resync rewrite was NOT red; the FLUSHALL variant is (5/5 alone); see the fixtures section |
| 6 nits: TXN.ABORT doc, frozen bytes without spill-in-flight, drop tables at once on a queued abort | 52544bf3, ad3a795b | `a_frozen_table_is_billed_without_the_spill_in_flight_bytes` red (1,467,473 vs 418,890 B) → green; resync test asserts nothing queued to freeze |
| merge `origin/main` (part 3b, 7ddc0cb) | 9915a665 | conflicts: `mq_exec::handle_push` (3b's `Stream::add` → `Option`, kept WS16's gate / capture / billing around its refusal arm); `perf_ws12_bgsave_split` (WS16's `wait_epoch_armed` already carries 3b's wait) |
| post-merge: adopt 3b's `MOON_TEST_SNAPSHOT_HOLD_FILE` | 5487322e, 9e94d945 | the F1 fixtures' aborts always land; `perf_ws16_bgsave_capture` failed its 4-shard MQ case beside its siblings on the merged tree (0 rounds inside the save in 5 of 5 attempts; 2/2 alone) and now overlaps on the first attempt (6/6 in 6.5 s) |

The review's proof file ran on the merged tree (registered temporarily, not committed): 5 of
6 green; `r4_red_plain_eviction_mid_epoch_drops_epoch_start_keys` stays red — the eviction
pre-image gap, residual 1, which the orchestrator is filing separately.

## Gate run after review 4 and the merge (code HEAD 9e94d945; debug builds of the merged tree, pinned)

- `cargo fmt --check` clean; `scripts/audit-unsafe.sh` PASS (0 missing SAFETY),
  `scripts/audit-unwrap.sh` PASS (baseline 0), `audit-test-tempdirs` PASS,
  `audit-encoding-limits` OK.
- `cargo clippy --all-targets -- -D warnings` (monoio) exit 0;
  `cargo clippy --all-targets --no-default-features --features runtime-tokio,jemalloc -- -D warnings`
  exit 0.
- `cargo test --lib`, filtered to persistence::snapshot, shard::persistence_tick,
  shard::mq_exec, blocking::stream_wake, blocking::wakeup, command::keyspace::move_cmd,
  scripting::bridge, shard::spsc_two_db, shard::shared_databases, replication::apply,
  shard::db_plane, workspace, server::conn::tests, storage::db: monoio 598 passed / 1 ignored;
  tokio 562 passed / 1 ignored.
- Integration, monoio (`MOON_BIN=/home/user/wt/bin/ws16-dbg-r4-merged-monoio`):
  perf_ws12_bgsave_split 5/5, perf_ws15_bgsave_status 3/3, perf_ws8_mset_bgsave_capture 1/1,
  perf_ws16_bgsave_capture 6/6, perf_ws16_mq_billing 4/4; with `--include-ignored`:
  replication_ws 4/4, replication_readonly_ws_mq 1/1, replication_mq 4/4, replication_swapdb 3/3.
  (mq_integration and workspace_integration compile to no tests on monoio.)
- Integration, tokio (`MOON_BIN=/home/user/wt/bin/ws16-dbg-r4-merged-tokio`):
  perf_ws12_bgsave_split 4/4 + 1 ignored (the resync variant: PSYNC needs a monoio master),
  perf_ws15_bgsave_status 3/3, perf_ws8_mset_bgsave_capture 1/1, perf_ws16_bgsave_capture 6/6,
  perf_ws16_mq_billing 4/4, mq_integration 17/17, workspace_integration 13/13 + 1 ignored.

## Review 5 (MERGE-AFTER-FIXES) — what changed

| item | commit | evidence (red → green) |
|---|---|---|
| A BLOCKING: the FLUSHDB freeze had no byte bound (a regression against main) | 11cf616e | Bound test (insert THEN flush): 1,297,793 B held vs an epoch-start 1,123,560 B → within. Skeleton test: 515 of 515 segments kept → trimmed. Two grown tables before one drain: completed → fails. Real server (the reviewer's reproduction): `current_cow_size` 344,782,240, RSS +341 MB → 0, +49..+58 MB (3/3). Details in item 2a above. |
| B moon#1261: the MqPop apply wrote the PEL untracked; the MqAck apply credits it | 6446832f, f9b89d65 | Lib replay test: 627 vs 1,185 B → equal. Restart and replica (the reviewer's proofs): 28,421 vs 124,097 → 124,097 = 124,097. Details under moon#1250 above. |
| C the 2b budget's latency trade-off (docs; keep) | ac6a4896 | The reviewer's release A/B, in item 2b above. The 0.7–1.7 s delay was a debug-build and load artifact. |
| D `count + mdc` overflowed on POP COUNT near `usize::MAX` | 48414029 | Debug panic ("attempt to add with overflow", mq_exec.rs:718) → delivers all 3. |
| E POP's `cut == 0` rewind and empty-batch return are unreachable | 9037ebdd | Kept, with one-line comments naming the invariant (COUNT >= 1, `test_validate_mq_pop_count_zero`). Comment-only, no red. |
| F a push or dead letter `Stream::add` refuses was still logged | 54d0f2c1 | TXN push: 1 MqPush logged for a refused add → 0. DLQ: routed (1, 1) with the entry acked away → (1, 0), the entry pending. `Stream::next_auto_id`'s `seq + 1` panicked a debug build at the last ID → explicit wrap. |
| file size | b559c1f1 | mq_exec.rs's tests moved to mq_exec/tests.rs (1,616 → 1,094 lines). |

Notes:
- The adopted server tests use `common::spawn_listening` (collision-safe reservation), not a
  fixed port range.
- The reviewer's Q1 proof is adopted without its MULTI leg (moon#1262, not WS16's).
- F (DLQ site): dropping the routing alone would have diverged on replay, which removes a
  dead letter from the PEL only through its routing. So the DLQ adds now come first, and only
  routed dead letters are acked.

## Gate run after review 5 (production code 54d0f2c1; test/doc commits after it; pinned debug builds)

- `cargo fmt --check` clean; audit-unsafe, audit-unwrap, audit-test-tempdirs,
  audit-encoding-limits PASS.
- `cargo clippy --all-targets -- -D warnings` clean on monoio and on
  `--no-default-features --features runtime-tokio,jemalloc`.
- `cargo test --lib`, monoio, FULL: 6,652 passed, 14 ignored.
- `cargo test --lib`, filtered to the touched modules (persistence::snapshot,
  shard::persistence_tick, shard::mq_exec, blocking::stream_wake, blocking::wakeup,
  command::keyspace::move_cmd, scripting::bridge, shard::spsc_two_db, shard::shared_databases,
  replication::apply, shard::db_plane, workspace, server::conn::tests, storage::db,
  storage::stream, command::mq): monoio 677 passed, tokio 641 passed.
- Integration, monoio (`MOON_BIN=/home/user/wt/bin/ws16-r5-final-monoio`):
  - perf_ws12_bgsave_split 5/5, perf_ws15_bgsave_status 3/3, perf_ws8_mset_bgsave_capture 1/1;
  - perf_ws16_bgsave_capture 7/7, perf_ws16_mq_billing 7/7;
  - with `--include-ignored`: replication_ws 4/4, replication_readonly_ws_mq 1/1,
    replication_mq 4/4, replication_swapdb 3/3.
- Integration, tokio (`MOON_BIN=/home/user/wt/bin/ws16-r5-final-tokio`):
  - perf_ws12_bgsave_split 4/4 + 1 ignored (resync), perf_ws15_bgsave_status 3/3,
    perf_ws8_mset_bgsave_capture 1/1;
  - perf_ws16_bgsave_capture 7/7, perf_ws16_mq_billing 5/5 + 2 ignored (replica);
  - mq_integration 17/17, workspace_integration 13/13 + 1 ignored.

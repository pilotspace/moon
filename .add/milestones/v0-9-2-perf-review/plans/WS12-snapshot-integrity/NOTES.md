# WS12-snapshot-integrity — working notes

ADD-style notes kept outside `.add/state.json` (TEAM-RULES: no `add.py new-task/advance`).
Branch `perf/ws12-snapshot-integrity`, base `f32546c`.

## ORIENT (storage-durability-engineer)

Who produces a `.rrdshard`, and who trusts it at recovery (read from
`shard::persistence_tick`, `shard::mod::restore_from_persistence_v2`,
`persistence::recovery`):

| producer | runtime × shards | consumer at restart | tail replayed on top |
|---|---|---|---|
| explicit BGSAVE (`handle_pending_snapshot`) | both × any | `restore_from_persistence_v2` when no multi-part AOF is the KV authority | legacy `appendonly.aof` if present, else WAL v3 (disaster fallback), else NOTHING (RDB-only) |
| `--save` auto-save (`check_auto_save_trigger`) | both × any | same | same |
| same, `--disk-offload enable` | both × any | `recovery.rs` v3 path: snapshot + WAL v3 from `last_lsn` | WAL v3 |

The AOF rewrite fold (moon#1185) does NOT use `SnapshotState`: the `AofFold`
arm serializes the whole keyspace synchronously (`fold_stream::stream_fold_image`).

Consequences that shaped the fixes:
- **RDB-only mode has no tail**: the file alone is the recovered state, so it
  must be ONE instant — a missing key is lost data, an extra/post-epoch value
  is a state that never existed.
- **With a tail** (WAL v3 from `last_lsn` = the epoch-start stamp), a
  post-epoch value in the file is replayed onto again: double-applied
  non-idempotent writes (`INCR`, `LPUSH`, `LMOVE`, `MOVE`).
- Both runtimes run the same `persistence_tick` / `SnapshotState` /
  `snapshot_cow` code; the only runtime split in the capture paths is the
  monoio inline SET (`capture_key_pre_image`, `cfg(runtime-monoio)`).

## moon#1216 — split during the epoch (FIXED)

**Specify.** The file must equal the epoch-start keyspace exactly: every
key present at the start once with its start value, no key created later.

**Options weighed.**
1. *Split hook in `DashTable::split_segment`* notifying the armed snapshot
   (mark the new segment pending, re-bucket its pre-images). Needs: a
   thread-local registry of armed snapshots keyed by table identity (a
   `DashTable` does not know its db index; pointer identity breaks under
   SWAPDB's `mem::swap`), a hook on the split path (one TLS load per split),
   re-bucketing of `overflow[(db, seg)]` for moved keys, AND fixing the
   `snapshot_cow` drain (it re-derives the segment at drain time) and the
   `PROGRESS` mirror (`seg < count` → "never pending"). Three places to keep
   in sync with the split implementation (WS1 just rewrote it in place).
2. *Hash-range iteration* (chosen). Extendible hashing: a segment of local
   depth `d` owns the aligned block of hash space with its top-`d` prefix;
   a split halves a block. Walk each db by a hash cursor: serialize the
   segment covering the cursor, move the cursor to its block end. The cursor
   only lands on block boundaries and splits only add boundaries, so every
   segment is wholly written or wholly pending forever; pending ⇔
   `hash ≥ cursor`. No DashTable change at all → **zero cost when unarmed**
   (not even a TLS load on the split path); the predicate is a pure function
   of (db, hash, cursor), so the drain and the `PROGRESS` mirror need no
   segment knowledge (one semantics — the reviewer's "second copy of
   progress" trap is gone by construction). Multiple splits of one segment,
   directory doubling, splits of the segment "currently being serialized"
   (serialization of one segment is synchronous — nothing interleaves) are
   all the same case.
- Pre-images: per-db `BTreeMap<(hash, key), Option<Entry>>`; the written
  range is `split_off` the front in O(log n). Only pending keys are inserted
  (enforced in `capture_cow`), so the map never holds anything below the
  cursor — which is what makes the range take exact.
- Absence is a pre-image (tombstone): keys created during the epoch used to
  be serialized (the old comment claimed otherwise). Keys are copied on
  capture so a capture never pins a connection read buffer for the epoch.
- `cow_intercept` now goes through the same queue as `dispatch`: two capture
  paths with two dedupe sets let a routed write file the post-local-write
  value first.
- A table REPLACED mid-epoch (FLUSH*) has a segment whose block starts below
  the cursor: its keys below the cursor are skipped (and moon#1224's abort
  normally fires first).

**Pacing (found by the evidence run, commit 344270b).** A correct walk
must visit every segment covering a pending range, and an insert flood keeps
splitting them: at the old one-segment-per-1 ms-tick pace (~1,000 segments/s)
against ~9,000 new segments/s the epoch never converged (11.7 GB RSS, 60 s
PING stall at 1M keys). The old index walk "finished" only because it skipped
the split-off halves, which was the data loss itself. The tick now serializes up to
64 segments / 1,024 entries (`advance_budgeted_db`); measured max PING during
BGSAVE 1.1–2.7 ms. Tombstones skip the epoch dedupe set (FIFO queue +
first-wins `capture_cow` make it redundant for them).

**Memory while armed:** every key written during the epoch in a pending
range costs one captured key (+ its deep-cloned value when it existed) until
its range is written; `PENDING_KEYS` (first-wins across ticks) is held for
the epoch, as before. Same shape as a fork's COW pages; not in `used_memory`
(pre-existing).

## moon#1224 — FLUSH*/SWAPDB during the epoch (FIXED, scope added mid-wave)

Crash gone with the hash walk (the segment is always found through the
current directory). Semantics chosen: a table swap touching a database the
epoch has not finished ABORTS the snapshot (loud: error log, BGSAVE reply
`Err`, `rdb_last_bgsave_status:err`; stream abandoned → temp file removed;
previous file untouched). Redis kills the BGSAVE child on FLUSHALL — same
answer. Harmless cases continue (db already written; FLUSHDB of an empty
table; a FLUSH whose args the command refuses). Hooks: `ShardDbSet::swap`
(every SWAPDB path) and the dispatch capture hook (every FLUSH path that is a
command). A false positive costs a retried save, never data.
- Considered and rejected for now: making FLUSHDB epoch-aware by detaching
  the old table into the snapshot (true point-in-time, redis-FLUSHDB parity).
  Needs `Database::clear` to hand its table to the epoch (storage/db, not
  owned) and must never fire for a refused flush. Recorded as follow-up.
- Residual: a replica full resync (`replication::apply`, `Database::clear`
  outside dispatch) during a BGSAVE on that replica is not detected (walk
  stays in bounds; that db's image mixes).

## moon#1217 — multi-key COW (FIXED)

Capture every `KeyRole::Write` position of `acl::keyspec::command_key_positions`
(the walker blocking wake-ups and tracking use); `Unknown` argv falls back
to the primary key. Scripts gate on `is_write` like dispatch. Blocking
waker (cross-ownership, `blocking/wakeup.rs`): captures the source key and
a BLMOVE destination before popping/pushing.
- **Not covered — follow-up:** `MOVE` and `COPY ... DB n` go through the
  two-database intercepts (`shard/spsc_two_db.rs`, `handler_monoio` /
  `handler_sharded` `mod.rs`, `scripting/bridge.rs`, the MULTI executors via
  `TwoDbOp::apply`), which capture nothing on either database. Worst case
  with a WAL tail: `MOVE k 1` after db 0's range with `k` was written and
  before db 1's → file has `k` in both dbs; the replayed MOVE fails (dst
  exists) → `k` resurrected in db 0. Fix: one
  `snapshot_cow::capture_two_db(src, src_idx, dst, dst_idx, src_key, dst_key)`
  call at each intercept (they all hold both databases and both indices).
  Files belong to WS7/WS8.

## moon#1185 remainder — incremental COW fold (DEFERRED)

**Exactly-once argument required.** At the `AofFold` instant F the writer
cuts `pending_aof_count` and the #455 `fold_epoch`: records stamped below
the epoch are DROPPED from the new incr (their effect must be in the base),
records at/above it are REPLAYED on the base (their effect must NOT be in
it). So the base must be exactly the keyspace at F — not "fuzzy", not
"subset". With the synchronous fold that is free (nothing runs while the
arm serializes). An incremental fold serializing across ticks needs, for
every mutation after F of a key whose range is not yet written, its F-state
captured first. Audit of mutation paths against the WS12 capture points:

| path | captured? | consequence in an incremental fold if not |
|---|---|---|
| `command::dispatch` (local, MULTI/EXEC, scatter, SPSC, scripts, replica apply) | yes, every written key (moon#1217) | — |
| monoio inline SET | yes | — |
| blocking wakers (pops, BLMOVE dst) | yes (this wave) | — |
| FLUSHDB/FLUSHALL/SWAPDB | abort (moon#1224); SWAPDB already refused during BGREWRITEAOF | fold would abort + retry |
| **MOVE / COPY ... DB n** | **no** | double-apply / resurrection (see above) |
| **eviction with spill** (`eviction::evict_one_async_spill` → `db.remove`) | **no** | key absent from the hot base, its cold copy in a file ≥ the fold's cold watermark → not a valid base → **lost on replay** |
| **replica full resync** (`Database::clear` + RDB load) | **no** | whole-db mix |
| active expiry / lazy expiry / hash-field TTL sweep | no | safe: TTLs are absolute, load filters them (base with or without the key replays to the same state) |
| plain eviction (no spill) | no | base lacks a key evicted after F; a logged DEL replays as a no-op, an unlogged one makes the rewrite differ from the atomic fold (not a double-apply) — verify `record_reason_del` covers the AOF plane before relying on it |

Three rows need capture hooks in files other workstreams own (WS7/WS8
connection + SPSC intercepts, `storage/eviction.rs`, `replication/apply.rs`),
and the spill row is silent data loss if missed. The argument therefore
cannot be made airtight inside WS12 → DEFERRED; WS6's interim streaming fold
(no copy, bounded chunks, O(dataset) stall) stays.

**Design for the follow-up** (after the three hooks land):
1. `snapshot_cow` becomes a registry of armed CONSUMERS (BGSAVE epoch, fold
   epoch), each with its own `SnapshotState`-like hash cursor, overflow map
   and first-wins set; `capture_key` clones once and offers the pre-image to
   every consumer still pending for that key; the drain fans out.
2. `AofFold` arm: take the three cuts exactly as today, reply, then arm a
   fold consumer whose serializer is `RdbStreamWriter` over the existing
   `FoldImageSink` (same byte format as `stream_fold_image` — the
   byte-identity test in `fold_stream.rs` becomes the oracle for an
   unwritten-to epoch), advanced from the persistence tick with a per-tick
   budget (N segments or ~250 µs), finished with `finish()` + `End`.
3. Aborts (FLUSH*/table swap) → `FoldChunk::Failed` → the writer aborts the
   fold, the old generation stays committed (exactly today's failure path).
4. Tests: deterministic fold model (no dependence on the `#[ignore]`d
   `aof_fold_exactly_once_455`, red on HEAD per moon#1134): arm a fold at F,
   interleave INCR/LPUSH/MSET/LMOVE/BLMOVE/MOVE/eviction-spill with advances
   and splits, finish; load base + replay every record stamped ≥ fold_epoch
   → must equal the live keyspace; randomized over seeds, both runtimes.
5. Evidence: BGREWRITEAOF max PING stall at 1.5M keys (today 476–538 ms per
   WS6) must drop to the per-tick budget.

## Evidence (release-fast, this 4-vCPU box, relative only)
`bench_ws12.py one <bin> 1000000 <shards> <tag>` (scratchpad; port 7341):
preload 1M `pre:` keys, start `redis-benchmark -t set -r 1e9 -P16 -c4`,
BGSAVE, poll PING + INFO until done, SIGKILL, restart from the snapshot,
GET every `pre:` key. One run per process, A/B interleaved.

| run | binary | shards | inserts during epoch | BGSAVE | max PING | peak RSS | pre-epoch keys missing/wrong |
|---|---|---|---|---|---|---|---|
| A0 | base f32546c | 1 | 9.35M | 35.3 s | 40.1 ms | 954 MiB | 726,429 |
| B0 | final 344270b | 1 | 0.40M | 1.5 s | 1.1 ms | 205 MiB | 0 |
| A1 | base | 1 | 6.74M | 34.3 s | 419.9 ms | 852 MiB | 697,457 |
| B1 | final | 1 | 0.36M | 1.5 s | 2.7 ms | 197 MiB | 0 |
| A0s4 | base | 4 | 0.41M | 7.8 s | 9.2 ms | 142 MiB | 41,610 |
| B0s4 | final | 4 | 0.03M | 0.3 s | 1.1 ms | 131 MiB | 0 |

The final binary's restored DBSIZE equals the keyspace at the epoch start
(1,447,659 vs 1,447,451 at the BGSAVE command; 1.85M live afterwards), so
keys created during the epoch are excluded, as designed.

Harness artifact (not a moon regression): the SECOND server a single
Python process starts had its preload connection closed while the server
stayed alive and logged nothing, reproduced with the base binary too. Worked
around by one run per process.

## Evidence harness
`src/persistence/snapshot/epoch_harness.rs`: drives `SnapshotState` exactly
as `advance_snapshot_segment` does (drain → advance → publish cursor),
writes go through `command::dispatch`, the file is read back as RAW records
(duplicates visible). `tests/perf_ws12_bgsave_split.rs`: real server, BGSAVE
under a concurrent insert stream, SIGKILL, restart, diff every pre-epoch
key; FLUSH*/SWAPDB mid-BGSAVE must leave the server serving and fail the save.

> **Update (PR #1227 review, `09a1f0b`):** a replica full resync now aborts an unfinished epoch
> (`snapshot_cow::note_table_replace`), so it is no longer an undetected gap. The remaining capture gaps
> are tracked in moon#1228.

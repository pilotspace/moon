# WS6-persistence — working notes

## ORIENT (storage-durability-engineer)
Recovery / rewrite path per configuration (read from `src/main.rs` manifest
branches + `command::persistence::bgrewriteaof_start_sharded`):

| runtime | shards | manifest | AOF writer loop | BGREWRITEAOF path |
|---|---|---|---|---|
| monoio | 1 | TopLevel (`initialize` / `initialize_with_base`) | `aof_writer_task` monoio (sync `std::fs`) | `RewriteSharded` → `do_rewrite_sharded` (C4 `AofFold`) → `advance_with[_base]` |
| tokio | 1 | none (legacy single file `appendonly.aof`) | `aof_writer_task` tokio (`BufWriter<tokio::fs::File>`) | `RewriteSharded` → `rewrite_aof_sharded_sync` (C4 `AofFold`, tmp+rename of the flat file) |
| both | >=2 | PerShard (`initialize_multi`) | `per_shard_aof_writer_task` (monoio sync / tokio async) | `RewritePerShard` → `do_rewrite_per_shard` (C4 `AofFold`) → `advance_shard[_staged]` + coordinator commit |
| monoio | 1 | TopLevel | — | legacy `Rewrite(SharedDatabases)` → `do_rewrite_single` (handler_single only; not the shipped server) |

All three shipped fold paths share the `AofFold` arm (moon#1185 change) and
differ only in how the writer lands the base. All four writer loops were
touched for moon#1187 (TopLevel/PerShard x monoio/tokio) — each verified.

BGSAVE (moon#1186): per-shard `SnapshotState` on the event loop, both
runtimes, `.rrdshard` in `--dir` (or the disk-offload shard dir).
WAL v3 (moon#1188/#1181): `<dir>/shard-N/wal-v3/` when `--appendonly yes`;
KV records only with `--wal-kv-log on` (or a CDC subscriber).

## Decisions
- #1187 staging buffer: DEFERRED. One hand-off per event-loop iteration
  needs every `send_append_group` / `try_send_append_durable` call site in
  `server/conn/**` (WS7, wave 2) to write into a per-shard buffer and a flush
  point before every `AofFold` and `fsync_barrier`; the writer would then
  receive pre-framed chunks, which changes the PerShard framing (per-record
  `[lsn][len]` headers written by the shard), the TopLevel SELECT injection
  (writer-side `last_db`), and the #455 per-record epoch filter (epoch per
  chunk instead). Doing it inside WS6 files alone is not possible.
- #1185 incremental COW fold: DEFERRED. The BGSAVE COW capture is primary-key
  only (`LMOVE src dst` captures `src`), and a DashTable split during an epoch
  moves keys of a pending segment into a segment index >= the epoch-start
  count, which is never serialized. Fuzzy BGSAVE + WAL replay tolerates some
  of that; the AOF fold's exactly-once contract does not (the base must be
  exactly the state at the fold epoch or a replayed INCR double-applies).
  The interim (serialize from the keyspace, stream chunks) removes the copy
  and the doubling image; the stall stays O(dataset).
- #1185 cross-ownership: `AofFoldSnapshot.dbs` → `image` in
  `src/shard/dispatch.rs` kept inside the #1185 commit (an isolated commit
  would not compile on its own).
- #1181: identical replies vs HEAD except a corrupt record in a segment
  wholly below `from_lsn` (HEAD stalled there forever). Gap-in-chain
  behaviour (stop at a missing sequence) deliberately kept identical.

## Findings outside scope
- **BGSAVE loses keys when a pending segment splits mid-epoch** (pre-existing,
  unchanged by this branch). `SnapshotState` serializes segment indices
  `< segment_counts[db]` captured at epoch start; a DashTable split pushes
  the new half at an index `>=` that count, which is never serialized and
  which `is_segment_pending` reports as not pending (no COW either). Probe
  `split_probe_repro.rs.txt` (drop into `src/persistence/snapshot/` as a
  `#[cfg(test)] mod`): 2000 pre-epoch keys, 1 segment serialized, 20 000
  inserts (62 → 529 segments) → 1744 / 2000 pre-epoch keys missing from the
  loaded snapshot. Needs a split hook / split-aware tracking in
  `storage::dashtable` (WS1) — recommend a new P0 issue.

## Evidence harness
`bench_ws6.py <scenario> <binA> <binB> [reps] [arg]` — scenarios `rewrite`,
`bgsave`, `cdc`, `walrot`, `aof`; fresh server + dir per rep, A/B
interleaved, port 7231, data under `.bench/run/` (deleted after each rep).

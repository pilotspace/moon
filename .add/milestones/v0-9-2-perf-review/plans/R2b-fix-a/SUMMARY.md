# R2b-fix-a SUMMARY

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| W1 MAJOR: TXN block ids collide across master shards in the replication stream | FIXED | b914e61 | `txn_crash_atomicity_1300::a_replica_of_a_multi_shard_master_keeps_same_id_txns_apart` (master `--shards 2` with aligned ids, replica s1, master SIGKILL, `REPLICAOF NO ONE`). RED on efe223b (B read `txnB`, expected `origB`), GREEN after. Unit tests `persistence::replay::txn::tests::two_shards_same_manager_id_stay_two_blocks`, `record_ctx::tests::log_ids_name_their_origin_shard`. | Risk 1 (pre-existing) |
| W2 MINOR: each newly held key deep-cloned twice | FIXED | 81aa706 | `txn_hold_one_copy_1300::a_txn_write_to_a_large_hash_keeps_one_copy_of_it` (300k-field hash, VmRSS). efe223b: first TXN write +77.9 MB for a 51.4 MB hash (1.52x), two later writes +65 MB. Fixed: +44.3 MB (0.84x, 2 runs identical), later writes +0.0 MB. Unit test `conn_capture::tests::a_held_keys_image_is_kept_once_and_restored_from_the_hold`. | none |

## Design decisions
- **W1, ids encoded, grammar unchanged.** Every `MOON.TXN` marker carries the log id `aof::txn_log_id(shard, id) = ((shard+1) << 48) | (id & (2^48-1))`.
  - Never 0, below `TXN_END_FLAG` (bit 63), idempotent per shard.
  - Grammar is still `MOON.TXN BEGIN|PAUSE|END <non-zero u64>`. Older logs carry bare ids (origin field 0), never collide with encoded ones, and read as before.
  - Every producer takes `(shard_id, txn_id)` and encodes itself (`AppendStamp::in_txn/end_of`, `txn_end_record`, `txn_log::repl_record`, `send_append_bounded_blocking_in_txn`), so the compiler forces every call site to supply the shard. Forward records, abort compensation, END, the fold's `enqueue_reopen` and the full sync's `stream_reopen` all agree.
  - Ids are opaque identities, never compared with MVCC ids.
- **AOF layouts do not merge shards.** Per-shard files hold one shard each; a TopLevel manifest with `--shards >= 2` is refused at boot; `migrate_aof` copies one origin's markers. The ids are encoded in the AOF too, so there is one identity everywhere.
- **Fuzz:** `aof_incr_replay` gains shape bit 7 (same local id encoded as two shards' log ids). Already in both `fuzz.yml` matrices.
- **W2, the hold owns the only copy.**
  - `isolation::hold(db, key, txn, Option<Entry>)` takes the image by move.
  - New `UndoRecord::Held { key, deleted }` (key and kind only) for a present key's first write and every later write of a held key; later writes copy nothing. An absent key's first write stays an `Insert`.
  - The XactCommit WAL image is unchanged: `Held{deleted:false}` encodes like `Update`, `Held{deleted:true}` like `Delete`.
  - The script leg folds its records the same way (`txn_script_undo::hold_script_undo`).
  - On abort, `kv_compensation::undo_one` copies the image from the hold via `with_held_pre` (copy, not move: the hold and every snapshot's view of it must stay until END). The transient abort peak is unchanged; during the TXN there is one copy instead of two.
  - A `Held` first record that finds no image logs an error and leaves the key as is.
  - No `Arc`, no new hot-path allocation.

## Gates (Linux container, not merge bar)
- `cargo fmt --check`, clippy `--all-targets -D warnings` on both feature sets, `cargo check --manifest-path fuzz/Cargo.toml --all-targets`: all 0.
- `cargo test --release --lib -- transaction persistence replication server::conn`: monoio 1529, tokio 1479, 0 failed.
- Integration (`r2bfa-final-{monoio,tokio}`): txn_crash_atomicity_1300 39/39 both (tokio replica tests self-skip); txn_hold_one_copy_1300 1/1; review_w1_txn_abort_no_aof_snapshot_1285 3/3; txn_abort_durability_1285 25/25; txn_isolation_1299 20/20; txn_exit_epilogue_1299 8/8; txn_close_after_epilogue_1299 6/6; txn_multikey_undo_500 5/5; held_release_txn_open_1289 3/3; cow_stream_1295 5/5; aof_replay_clock_1283 19/19 (all both runtimes); replication_streaming 7/7, replication_multishard 9/9, replication_hardening 6/6 (monoio only).
- `aof_shard_write_1266`: 8 pass, 4 fail on both runtimes. The four (`{after_always,boot_window}_kill_on_ack_loses_nothing_s{1,4}`) are R2b-fix-b's new regression tests, picked up through the shared target dir; they fail identically on the efe223b binaries and are expected to pass only once fix B is integrated. Re-checked at integration.

## Risks
1. **Pre-existing: master restart plus partial resync.** The master persists its repl id and recovers its offset from the AOF, so a replica can PSYNC-continue after a master restart. The new process issues TXN ids from 1 again, so its same log id can END a stale open block on the replica, making never-committed writes permanent there while the master's boot replay rolled them back. Suggested fix: regenerate the repl id at boot whenever replay rolled back a TXN block, forcing a full resync. To be filed.
2. Shared-target aliasing happened twice during this work; binaries were rebuilt and marker-verified. Re-verify markers on the integrated tree.
3. The W2 RSS bound (1.2x) relies on a clone being tighter than the grown original; re-measure if allocator settings change.
4. W1 encodes the local id in 48 bits and the origin in 15 bits (saturates past 32766 shards). Documented.

## CHANGELOG bullets
- moon#1300 W1: a replica of a multi-shard master could keep a never-committed `TXN`'s writes after `REPLICAOF NO ONE`; one shard's `MOON.TXN END` closed another shard's open block with the same id. Markers now carry the origin shard in the id. Grammar unchanged; older logs replay as before.
- moon#1300 W2: a `TXN` write kept two deep copies of each newly held key's pre-transaction value, plus one per later write. The hold now keeps the only copy (~0.84x of the value measured vs 1.52x), and later writes copy nothing.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.92 · Practicality 0.93 · Optimization 0.93 · Edge cases 0.90 · Self-evaluation 0.90

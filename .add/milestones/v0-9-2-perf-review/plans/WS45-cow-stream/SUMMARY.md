# WS45 SUMMARY

All results are from the Linux container (4 vCPU x86_64, io_uring available). They are **not the merge bar**. Other lanes were building throughout (load average about 4), so the numbers are noisy.

## Per-issue verdict

| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1295 | **FIXED** at `--shards 1` on both runtimes. Also fixed for any locally owned key at `--shards N`. | `6338adf` design and implementation · `b340385` time-bounded chunks and unit tests · `b366090` EXEC waits, chunk-time cap, waiter-wake fix, integration suite, STORAGE-FORMAT note · `2dcb8c9` gap bound · `c98ef84` no-progress defence · `1eb6c7f` EXEC test | 3 interleaved reps per runtime, 5M-field hash, HSET of one field (raw numbers below). The max PING gap went from 1.3–1.7 s to 31–52 ms, RSS from +836 MB to +3 MB, and HLEN 5M after restart every run. `tests/cow_stream_1295.rs`: 5 tests. All 5 fail on base `ws42-final-monoio` (base gap 2302 ms with the Rust pinger, RSS +836 MB). All 5 pass on both final binaries. 11 unit tests in `snapshot/key_stream_tests.rs`. | See F1–F4 below. |

**Follow-ups:**
- **F1.** A hash or sorted set has a HashMap of members, which has no positional cursor. Each tick re-skips to the stream position: about 2 ns per element, so 10 ms at 5M fields on a quiet core and 20–35 ms on this box. That re-skip is the remaining PING gap. Removing it needs either an unsafe raw-table cursor, which requires maintainer approval, or an indexed hash encoding. Lists and sets already resume in O(1).
- **F2.** Writes that cannot wait still deep-copy a large key that is not streaming yet. These are routed SPSC legs at `--shards >1`, Lua `redis.call`, blocking-pop wakers and replica apply. Making SPSC Execute deferrable would close the shards>1 gap.
- **F3.** The walk itself still serializes a large value it reaches untouched in one tick. On base, releasing the hold stalled the shard 1.09–1.40 s. A streamed key is skipped by the walk, so that stall is gone in the harness: it measured 2–6 ms. A large value that nobody wrote still stalls. The same key-block mechanism can fix it.
- **F4.** Keys with a TTL, hashes with field TTLs and streams still copy.

## Design

The issue's option 2, streamed. The on-disk format does not change.

**The waiting write.**
- Before its local dispatch, a connection's write calls `snapshot_cow::stream::admit_write` (both runtimes).
- A request is queued and the writer awaits a `flume` receiver when the key is:
  - pending in the epoch;
  - not captured yet;
  - free of a key TTL;
  - a hash, list, set or sorted set of at least 8192 elements.
- EXEC waits for every write in its body the same way, following the body's own SELECTs.
- Nothing has changed in the keyspace at that point.

**The tick.** The persistence tick runs `stream::service` before the walk, and also while a test holds the walk. It opens a key block:
- `[0xFE db] 0xFD u32::MAX u32 1 <entry> crc`. This is the ordinary segment-block encoding with one entry.
- A resumable `KeyCursor` writes exactly `rdb::write_entry`'s bytes. It writes one chunk per tick, stopping at 8 MiB or after max(2 ms, the re-skip time), with that time capped at 5 ms.
- The walk pauses while the block is open, so the block is contiguous in the file.
- A tombstone pre-image stops the walk from writing the key again, so the key is in the file exactly once, at its epoch-start value.
- The walk re-emits its database selector after a key block for another database. Every reader since v1 already accepts repeated selectors and ignores the segment index.

**When the block closes.** The waiters wake, find the key captured, and run. The dispatch hook copies nothing.

**Writers that cannot wait.** All capture and hot-removal paths go through `capture_key` and the two `kv_ops` removal funnels.
- A key that is already streaming is finished inline by `guard_write`: the rest of the serialization, still no copy.
- A key that is only queued has its request dropped, and the write copies it as before.

**Whole-table changes.**
- SWAPDB: the stream follows the table.
- FLUSHDB: the stream finishes from the detached table.
- FLUSHALL and replica resync: abort the save, as before.

**Safety net.** Every chunk re-checks the payload address, kind and length. A mismatch, or a chunk that makes no progress, fails the save loudly instead of writing a block that does not parse.

**moon#1300 (keys held by an open TXN).**
- A large held key streams from its held pre-transaction image. The TXN's own writes change the live value, not the source, so nothing waits.
- If the hold is released mid-stream, the stream takes the image by move.
- If the hold is released while the request is still queued, the image is filed as the pre-image by move.

**INFO persistence** gains two counters: `rdb_cow_streamed_keys` and `rdb_cow_stream_waits`.

## Measurements (method, reps, raw numbers)

**Harness.** `scratchpad/ws45/bench.py`:
- Server: `--shards 1 --appendonly no --save "" --disk-offload disable`, with `MOON_DISK_FREE_MIN_PCT=0`.
- Load: a 5M-field hash plus 20K filler keys.
- Steps: touch the hold file, BGSAVE, start a PING loop on a second connection and sample `VmRSS` every 2 ms, HSET one new field, release the hold, wait for the save, kill -9, restart, check HLEN and HEXISTS.
- Base and new interleaved, 3 reps per runtime, ports rotated through 7600–7617. The raw output is `scratchpad/ws45/ab.jsonl`.

| run | HSET ms | max PING gap ms (gaps >20 ms) | RSS spike MB | gap during release ms | HLEN after restart |
|---|---|---|---|---|---|
| base monoio ×3 | 1443 / 1334 / 1286 | 1532 / 1460 / 1375 | 836 / 836 / 836 | 1269 / 1247 / 1400 | 5M ×3 |
| new monoio ×3 | 1709 / 2429 / 2273 | 31.5 / 41.0 / 52.2 (44/57/57) | 2.9 / 2.9 / 2.9 | 4.9 / 2.0 / 5.9 | 5M ×3 |
| base tokio ×3 | 1271 / 1676 / 1359 | 1347 / 1676 / 1460 | 836 / 836 / 836 | 1207 / 1253 / 1091 | 5M ×3 |
| new tokio ×3 | 2696 / 2913 / 2580 | 45.7 / 39.5 / 42.1 (71/12/48) | 1.6 / 4.0 / 1.4 | 4.5 / 1.6 / 5.6 | 5M ×3 |

- In every run the new field was absent after restart, which is correct: the HSET came after the epoch started.
- Ping gaps in the 2 s before the write: max 1.3–30 ms.
- A 5M-element list, monoio, base → new: LPUSH 1057 → 189 ms, gap 1112 → 55 ms, RSS +312 → +7 MB. Lists resume in O(1).
- Microbenchmark of a 5M-entry `HashMap<Bytes,Bytes>`:

  | operation | cost |
  |---|---|
  | `clone` | 1.38 s |
  | serialization | about 200 ns per field (two cache misses each) |
  | `iter().nth()` skip | about 2 ns per element |

  That is why chunks are time-bounded rather than byte-bounded. The first cut used 2 MiB chunks, which took 25 ms per tick.

**READY TO BENCH (quiet window):**
```
for rep in 1 2 3; do for rt in monoio tokio; do for b in ws42-final ws45-final; do
  python3 scratchpad/ws45/bench.py /home/user/wt/bin/$b-$rt <port>; done; done; done
```
Add `--kind list|set|zset` for the other kinds, and rotate ports.

## Gates (Linux container, not the merge bar)

**Static checks.**
- `cargo fmt --check`: 0.
- `cargo clippy --all-targets -D warnings`, monoio and tokio: 0.
- `cargo check --manifest-path fuzz/Cargo.toml --all-targets`: 0.

**`cargo test --release --lib`, monoio:**

| filter | result |
|---|---|
| persistence | 1011 ok |
| storage | 965 ok |
| command | 1693 ok |
| transaction | 67 ok |
| shard | 376 ok |

**`cargo test --release --lib`, tokio:**

| filter | result |
|---|---|
| persistence | 1008 ok |
| storage | 961 ok |
| command | 1519 ok |
| transaction | 61 ok |
| shard | 364 ok |

**Integration, `--include-ignored`, `MOON_BIN` pinned to the final binaries. Green on both runtimes:**
- cow_stream_1295 (5/5)
- perf_ws21_snapshot_without_save_rules (9)
- review_w1_txn_abort_no_aof_snapshot_1285 (3)
- held_release_txn_open_1289 (3)
- crash_recovery_cold_no_aof (10)
- perf_ws16_bgsave_capture (8)
- perf_ws16_bgsave_prop (2)
- perf_ws8_mset_bgsave_capture (1)
- perf_ws21_eviction_bgsave (2)
- perf_ws15_bgsave_status (3)
- perf_ws21_flushall_save (11)

**Runtime-specific integration results:**
- **monoio:**
  - txn_crash_atomicity_1300: 38/38.
  - perf_ws12_bgsave_split: 5/5.
  - Replication full-sync suites: replication_streaming 7, replication_swapdb 3, replication_flushall 3.
  - replication_planes 9/11. The 2 failures are `eviction_parity_hash_disk_offload_shards{1,4}`, a known failure.
- **tokio:**
  - kill_snapshot 4/4 and replication_test 5/5. Both are in-process suites under the tokio feature build.
  - txn_crash_atomicity_1300: 33/38.
  - perf_ws12_bgsave_split: 4/5.
  - All 6 failures say "replica link never came up", the known class (tokio has no master-side PSYNC). `a_resync_mid_bgsave…` and `a_replica_attached_before_the_txn_agrees_on_commit` also fail on base `ws42-final-tokio`.

**Not run:**
- kill_snapshot and replication_test against the monoio binary: they are `cfg(feature = "runtime-tokio")` suites.
- Windows, MSRV and macOS.

## Cross-ownership edits

**Small hooks:**

| file | change |
|---|---|
| `src/server/conn/handler_monoio/mod.rs` | +7 lines: wait before the local write |
| `src/server/conn/handler_sharded/mod.rs` | +6 lines: wait before the local write |
| `handler_{monoio,sharded}/write.rs` | +7 lines each: EXEC waits |
| `src/storage/db/kv_ops.rs` | removal guard |
| `src/transaction/isolation.rs` | `with_held_pre`; hold release hands the image to the snapshot |
| `src/shard/persistence_tick.rs` | service call before the hold check |
| `src/command/connection.rs` | 2 INFO fields |
| `src/storage/value_codec.rs` | 3 put helpers made `pub(crate)` |
| `docs/STORAGE-FORMAT-V1.md` §3.2 | blocks and key blocks, documented as writer behaviour |

**New and restructured files:**
- New: `snapshot_cow/stream.rs` and `snapshot/key_stream.rs`.
- `snapshot.rs`: its test-only accessors moved to `snapshot/test_access.rs`. The file went from 1513 to 1466 lines, back under the cap.
- `snapshot_cow.rs`: 1499 → 1484 lines.

## Risks / things the orchestrator must re-check at integration

1. **The guarantee depends on every in-place mutator of a hot value going through `capture_key` or a `remove_hot*` funnel** (the moon#1217 invariant). Keys with a TTL are excluded, so expiry cannot remove a key mid-stream. If a mutator ever bypasses those paths, the per-chunk check (payload address, kind, length) catches a replace, insert or remove and fails the save. It cannot catch a same-length in-place value overwrite. Any new write path must capture.
2. **The written key waits for its stream.** On base the HSET cost 1.3–1.7 s at 5M fields; now that client waits 1.7–2.9 s while its own key streams. A connection that disconnects while waiting leaves its request queued; the stream still runs, which is harmless.
3. **The walk pauses while a block is open.** Many large written keys in one epoch lengthen the save, but each key streams at most once per epoch.
4. **WS46 conflict.** WS46 changes the AOF framing, but it touches the same monoio and tokio handler files near the write path. A cherry-pick conflict is possible around the `is_local` block.

## Self-evaluation (0–1)

| criterion | score | why |
|---|---|---|
| Completeness | 0.9 | Every invariant listed in the brief has a test: exactly once at the epoch-start value, first capture wins, the moon#1269 move path kept, the moon#1300 held image, multi-db, SWAPDB, FLUSHDB, kill -9 and both runtimes. Cold and spilled keys are not streamed; they keep the existing path, and `crash_recovery_cold_no_aof` stays green. The F2 paths are documented fallbacks. |
| Clarity | 0.9 | |
| Practicality | 0.9 | |
| Optimization | 0.85 | Cannot reach 0.9 here. The remaining 30–50 ms gap is the std `HashMap` re-skip, and an O(1) cursor needs either new `unsafe`, which needs maintainer approval, or a hash-encoding change. Lists and sets are O(1). The trade-off is a slower write for the client that triggered the stream. |
| Edge cases | 0.9 | |
| Self-evaluation | 0.9 | |

## CHANGELOG bullet (ready to paste)

- **perf(persistence):** a write to a large collection that a running BGSAVE has not saved yet (HSET, LPUSH, SADD, ZADD, …) no longer copies the whole collection on the shard thread (moon#1295). The collection's pre-write contents are streamed into the snapshot a few milliseconds per tick while that write waits; other clients keep being served. 5M-field hash: the longest stall for other clients fell from 1.3–1.7 s to 31–52 ms, and the RSS spike from +836 MB to +3 MB. MULTI/EXEC waits the same way. No snapshot format change. New INFO fields: `rdb_cow_streamed_keys`, `rdb_cow_stream_waits`.

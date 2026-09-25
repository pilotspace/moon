# WS10-memory-ownership SUMMARY

- **Branch:** `perf/ws10-memory-ownership`, based on `ae21476` plus the plan commit `6537679`.
- **Personas:** storage-durability-engineer (lead) and performance-engineer.
- **NOTES.md** covers mechanisms, designs, per-issue self-scores and gate results.
- The orchestrator committed this file; the harness refused the subagent's write (TEAM-RULES §6). The content is the agent's final report.

## Per-issue verdict

| issue | verdict | commits |
|---|---|---|
| moon#1225: a list move loses an element on a cold fault | **FIXED** | `6ab99ea` (refactor: LREM compaction moved to list_compact.rs so list_write.rs stays under 1500 lines), `90d6a78` (fix), `a3d1bc1` (SMOVE, same class) |
| moon#1160: collection elements pin the read/replay buffer | **FIXED** | `783beb8`, `15ba955` (test), `b6d7a74` (refactor) |
| moon#1163: streams not charged to used_memory | **FIXED** | `ef4e940`, `1215dea` (MEMORY USAGE, cross-ownership), `995d531` (test) |
| moon#1206: listpack backlen byte order | **FIXED** | `c2f0899` |
| moon#1212: listpack residuals | **FIXED** | `24e8ac5`, `b461c5c` (consistency rows for #1209/#1211), `2db1c8d` (buffer growth), `9d20fc5` (refactor into db/list_blocking.rs) |
| moon#1198: storage items 4, 5a, 5b, 6 | **FIXED** | `afa6735`, `7803011`, `4f358a9`, `4d29655` |
| moon#1214 item 3: second probe in `Database::get` | **DEFERRED** (with evidence) | — |

### moon#1225
Evidence:
- `command::list::cold_fault_1225_tests` (5 tests), red 4/5 on ae21476:
  - LMOVE: "must be -IOERR, got BulkString(b"h1")".
  - LMPOP popped a later key.
  - BLMOVE immediate: "got Some(BulkString(b"h1"))".
- `tests/perf_ws10_list_cold_fault.rs`, on a live server at shards 1 and 4. Red on base: LMOVE answered `$2\r\ns1`.
- `command::set::smove_cold_fault_tests`. Red: SISMEMBER answered 0 after the -IOERR.

Follow-ups:
- The same "write acts on an absent answer" class remains in RENAMENX, COPY and similar commands.
- One narrow case is logged but not refused: the probe's cold read succeeds, and then the push's second read fails.

### moon#1160
Evidence:
- `storage::owned_bytes::tests`: a pointer-range check across 5 types (red 5/6).
- `persistence::replay::chunks::tests` (5).
- `tests/perf_ws10_collection_rss.rs`, extra RSS after 100K small elements:

  | command | baseline | fixed |
  |---|---|---|
  | SADD | +561.5 MiB | +9.7 MiB |
  | HSET | +567.1 MiB | +11.5 MiB |
  | ZADD | +573.1 MiB | +20.2 MiB |
  | RPUSH | +559.6 MiB | +5.6 MiB |
  | XADD | +575.0 MiB | +18.6 MiB |

- Replaying a 79.6 MiB log (debug build): RSS 98.1 → 41.5 MiB.

Follow-up: the first clone of a stored element allocates an unbilled header (Risk 1).

### moon#1163
Evidence:
- `storage::db::stream_accounting_tests` (5). Red: "1000 XADDs grew used_memory by only 354 B".
- Live, 20K XADDs:
  - used_memory +24,684 B → +4,120,370 B (206 B/entry).
  - MEMORY USAGE 114 → 4,120,114.
- Under `--maxmemory 4mb noeviction`: before, all 200,000 XADDs were accepted; now -OOM after 20,359.
- DEL returns used_memory exactly to baseline. Both runtimes.

Follow-up: some stream mutations are billed by the next stream command on that key, not at their own call site:
- MQ (`shard/mq_exec.rs`);
- transaction intents;
- the XREADGROUP-BLOCK wake.

### moon#1206
Evidence:
- `storage::listpack::backlen_tests` (5), red 4/5.
- Byte-identical to redis 7.0.15 DUMP, including the length seams 258/4097/4101/16382/16383. A 16383-byte entry needs a 3-byte backlen.

No persisted format carries raw listpack bytes, so there is no version bump. moon DUMP → redis RESTORE works.

### moon#1212
Evidence:
- `listpack_residuals_1212_tests` (4), plus the repointed moon#832 pin `lpushx_and_rpushx_keep_a_listpack_moon1212`. Red 5/5.
- `storage::listpack::growth_tests` (2):
  - red for doubling: 8 reallocations, but capacity lands off the size class on all 120 pushes;
  - red for bare exact growth: 120 reallocations;
  - green: 23 reallocations for 23 size classes.
- Capped list: 3,791 → 2,257 B/key (redis 2,224). RSS +36.6 → +22.7 MiB.
- Script rows checked against redis: 11/11 at shards 1, 8/8 at shards 4.

Follow-up: BLMPOP's `try_immediate_pop` in `server/conn/blocking.rs` still flattens through `db.get_list`.

### moon#1198
Red evidence per item:
- Hot-key tick: "fired 0".
- StreamId: "5 B slack".
- KEYS: probes per call went from 202 (mutable) / 101 (shared) to 0.
- Stall gauge: "a shard with no backlog cleared another shard's stall".
- MVCC gauges: committed 3, expected 13.

Follow-up: a shard thread that exits while stalled leaves its +1 on the gauge.

### moon#1214 item 3
Profiled with perf on a release-fast build, `--shards 1`:
- Plain GET never reaches `Database::lookup` (0 samples).
- With GETSET, samples were attributed by call site:
  - 100K keys: first probe 12.51%, re-probe **0.61%**.
  - 100 hot keys: first probe 4.51%, re-probe **1.01%**.

Removing the re-probe needs `unsafe` (NLL problem case #3) or restructuring the expired/cold branches. That is not justified for ≤1%.

## Measurements
- **Setup:** a 4 vCPU box at load 6–8, so the numbers are relative only.
- **Binaries:**
  - `baseline-ae21476`;
  - `ws10-rf1` (release-fast at `c2f0899`);
  - `ws10-rf2` (release-fast at `4d29655`, plus bare-exact growth and the list_blocking move; provenance in `/home/user/wt/bin/ws10-rf2.rev`);
  - `ws10-debug-final` and `ws10-debug-tokio-final`.

**#1160 throughput** (3 interleaved reps, `-P16 -c8 -r1e6 -n300k`). Median ratio rf1/baseline: HSET 1.033, SADD 1.030, RPUSH 0.935, ZADD 0.944, SET control 0.876. The control moved most, so the box cannot resolve a regression.

**#1212 memory** (10K lists × 110 × (LPUSH 16 B + LTRIM 0 99), fresh server, 3 reps):

| build | used_memory B/key | RSS growth |
|---|---|---|
| baseline | 3,793 / 3,791 / 3,791 | 36.6 / 36.5 / 36.6 MiB |
| rf1 | 3,791 / 3,791 / 3,793 | 36.6 / 36.6 / 36.5 MiB |
| rf2 | 2,257 ×3 | 22.7 / 22.7 / 27.5 MiB |
| redis 7.0.15 | 2,224 | — |

The committed size-class variant on the final debug build: 2,257 B/key, RSS +22.6 MiB.

**#1212 CPU**, as an upper bound (rf2 is bare exact growth, about 5× more reallocations than the committed variant):
- LPUSH median ×1.104 (paired mean ×1.04);
- HSET ×1.036;
- SADD ×1.012;
- SET control ×0.857.

All of this is inside the control's ±15% swing. **Re-check on the integration build.**

**First-read promotion:** about +2.6 MiB RSS per 100K elements read once.

## Cross-ownership edits
- `src/command/server_admin.rs` (`1215dea`): one match arm, so MEMORY USAGE of a stream reports its measured size.
- `src/storage/db/kv_ops.rs` (`ef4e940`): `set_recording` and `insert_for_load` call `settle_stream_billing`. WS15's cold branch of `remove_counting_cold` is untouched.
- `src/storage/db/lazy_free.rs` and `src/storage/entry.rs` (`ef4e940`): stream billing.
- `src/storage/bptree.rs` (`783beb8`): new `BPTree::take`.
- `src/persistence/aof/mod.rs`, `aof_manifest/shard_replay.rs`, `replay.rs` (`783beb8`, `b6d7a74`): the bounded replay reader. WS15 also works in persistence; watch this merge.
- `src/command/list/lrem_lpos_1173_tests.rs` (`6ab99ea`): one import path.
- `scripts/test-consistency.sh` and `scripts/test-commands.sh` (`b461c5c`): new rows only.

## Risks / things to re-check at integration
1. **The first clone of a stored element allocates.** `detach` stores a Vec-backed `Bytes`, so its first `clone()` allocates a 24 B shared header (32 B class). That header is not billed to used_memory: about +2.6 MiB RSS per 100K elements read once. RDB/AOF/cold-loaded elements already behaved this way. Follow-up: bill it, or build replies from borrowed slices.
2. **#1212 growth CPU:** the committed variant was not measured in release. Its upper bound measured within noise.
3. **Timing test under load:** `hrandfield_does_not_materialize_the_hash` (a 5× wall-clock ratio) failed once in the full lib run at load 7. It passed 3/3 when run alone.
4. **Merge surfaces:**
   - `storage/stream.rs` (+325/−79);
   - `listpack.rs`;
   - `list_write.rs`;
   - `persistence/aof/mod.rs`;
   - the RECL gauge functions in `shard/timers.rs`.
5. **Files over 1500 lines** (all already over on ae21476):
   - `listpack.rs` 2,264 → 2,310
   - `db/mod.rs` 3,675 → 3,691
   - `hash_write.rs` 1,548 → 1,565
   - `entry.rs` 1,589 → 1,593
   - `list/mod.rs` 2,037 → 2,045
   - `sorted_set/mod.rs` 5,529 → 5,533
   - `set/mod.rs` 1,958 → 1,960
   - `accessors.rs` shrank (1,683 → 1,651).
6. **Known environment-only failure:** `cold_index_rebuild_tests::unreadable_file_is_counted_and_skipped_never_queued_for_unlink`.

## Gates at the last code commit `9d20fc5`
- `cargo fmt --check`, audit-unsafe and audit-unwrap: clean.
- clippy `--all-targets`, tokio clippy and tokio `check --all-targets`: clean.
- `cargo test --lib` (monoio): 6421 passed; the only failure is the known root-only test.
- tokio `--lib` over the WS10 modules: 1202 passed.
- `perf_ws10_list_cold_fault`, `perf_ws10_stream_memory` and `perf_ws10_collection_rss`: green on both runtimes.

## CHANGELOG bullets
- **Fixed:** LMOVE, RPOPLPUSH, BLMOVE, BRPOPLPUSH, LMPOP, BLMPOP and SMOVE now refuse with `-IOERR` before popping when an endpoint's cold-tier copy cannot be read. Before, the element was popped, the push onto the unreadable destination was dropped, and the element was returned (moon#1225).
- **Fixed:** these are now stored as exact-size copies instead of slices of the connection read buffer:
  - hash fields and values;
  - set and sorted-set members;
  - list elements;
  - stream fields and names.

  AOF replay streams through a bounded 1 MiB buffer. 100K small set members held +561 MiB RSS; now +10 MiB (moon#1160).
- **Fixed:** streams are charged to `used_memory` (about 206 B per one-field entry), so `maxmemory` binds on stream workloads. DEL credits exactly what was charged. MEMORY USAGE of a stream reports its measured size (moon#1163).
- **Fixed:** listpack backlen bytes are written in redis's byte order and widths. Backward walks over entries of 128 B or more are correct, and listpack bytes match redis (moon#1206).
- **Performance:** LPUSHX, RPUSHX and the blocking serve path keep small lists in listpack form. SORT reads listpack elements borrowed. Listpack buffers grow to the allocator size class of the new length instead of doubling: a capped list went from 3,791 to 2,257 B/key (redis 2,224) (moon#1212).
- **Performance** (moon#1198):
  - hot-key sampling uses a per-thread tick;
  - stream IDs render without `format!`;
  - KEYS no longer clones or probes per key;
  - the segment-stall and MVCC reclamation gauges are per-shard contributions, so one shard can no longer clear another shard's write stall.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.92 · Practicality 0.93 · Optimization 0.91 · Edge cases 0.91 · Self-evaluation 0.9. Risks 1 and 2 hold the scores back: both are measured and stated, but not closed here.

# WS12-snapshot-integrity — PLAN (wave 2)
personas: `.add/personas/storage-durability-engineer.md` (lead — an acknowledged write is a promise) · `.add/personas/performance-engineer.md`
Context: read `plans/WS6-persistence/{SUMMARY.md,NOTES.md,split_probe_repro.rs.txt}` and `plans/WS1-storage-core/SUMMARY.md` (WS1 changed `split_segment` to an in-place split in moon#1159) first.

## Issues (order: data loss first)
1. **moon#1216 (P0)** BGSAVE drops pre-snapshot keys when a DashTable segment splits mid-epoch (1744/2000 in the repro).
   - Must: the repro (as a committed test) green; splits of NOT-yet-serialized segments during an armed epoch never lose keys (split hook marking the new segment pending + extending `serialized_segments`/`segment_counts`, or hash-range iteration — choose, justify); keys moved into a still-pending segment and then written are COW-captured with their epoch-start value; splits of ALREADY-serialized segments never write a key twice; deletes/writes during the epoch; both runtimes; `--shards 1` and 4. Zero cost when no snapshot is armed (one thread-local load).
2. **moon#1217** COW captures only `command[1]`.
   - Must: capture every WRITTEN key position (use the existing written-key metadata — `for_each_written_key` / keyspec write flags) in both `spsc_handler::cow_intercept` and `snapshot_cow::capture_command_pre_image`; tests per family (LMOVE/RPOPLPUSH/SMOVE dst, MSET/MSETNX k2..kN, RENAME/RENAMENX, COPY, *STORE, BITOP, SORT STORE, LMPOP/ZMPOP, DEL/UNLINK k2..kN) with the non-first key in a pending segment → loaded snapshot holds the pre-epoch value.
3. **moon#1185 (remainder)** incremental COW AOF-rewrite fold — ONLY after 1 and 2 are green: segment-at-a-time fold with pre-image capture so the shard thread does bounded work per tick (today the fold stalls the shard ~0.5 s per 1.5M keys); exactly-once #455 tests green (note `aof_fold_exactly_once_455` is `#[ignore]`d and red on HEAD — moon#1134; don't rely on it alone). If the exactly-once argument cannot be made airtight in this workstream, DEFER with the design and keep WS6's interim streaming.

## Owned files
`src/storage/dashtable/**` (split hook only), `src/persistence/snapshot.rs`, `src/persistence/snapshot_cow.rs`, `src/persistence/aof/rewrite.rs` + fold plumbing, `src/shard/spsc_handler.rs` ONLY `cow_intercept` + the `AofFold` arm, tests `tests/perf_ws12_*.rs`.

## Not yours
`src/shard/**` other than the two regions above (WS8), connection handlers (WS7), `src/storage/db/accessors.rs` + listpack (WS10).

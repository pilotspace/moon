# FIX3B-CM — SUMMARY (PR #1242 review fixes: connection + memory/parity)

- **Base:** `5d1a37c`. These fix the REVIEW3B-MEM findings (S1–S4, N1, N2, N4, P3, moon#1249) and the REVIEW3B-CONN SHOULD/NIT items.
- **Design reasoning:** NOTES.md has the design notes and 0–1 scores.
- **Security:** no vulnerability found.

## Verdicts

| # | Item | Verdict | Commits | Red → green |
|---|---|---|---|---|
| 1 | DEL/UNLINK/GETDEL of an expired key (S1+P2) | FIXED | `333bc82` (+ `b799dbb`, `939a9cf`, `e4a387f`) | Red on 5d1a37c: DEL/UNLINK answered `[1,1,1,0,…]` at s1 and s4. With the AOF-logging part reverted, red: "DEL …: the key appears 1 time(s) in the AOF". Green on both runtimes. |
| 2 | Events after SWAPDB name the wrong db (S2) | FIXED | `70c527c` (+ `ca027a2`) | Red on 5d1a37c: `DEL b` in db 0 after `SWAPDB 0 3` published `@3`. Green on both runtimes. |
| 3 | `MOON_BIN` loudness (S4) | FIXED | `874ea40` | Red with the old helper: a missing pin fell back to `target/debug/moon`. |
| 4 | `list_pop_alloc_942` full-push assert (N2) | FIXED | `64b6aea` | Red with `detach` reverted: `full_push` 0 instead of 50. |
| 5 | MEMORY USAGE of a stream (N4) | FIXED (same value, now O(1)) | `ec5ec98` | On a 200K-entry stream, the same 38000114 B: 19.0 ms before, 0.048 ms after. It is checked against the scan throughout the stream-accounting tests. |
| 6 | Listpack comments (N1) | FIXED (comments only) | `76b6bc9` | Checked against redis: its DUMP of a small list is type 18, moon's is type 1. |
| 7 | moon#1249: XSETID below the top item | FIXED (Fixes #1249) | `549a0c5` | Red on 5d1a37c: `+OK` at s1 and s4, and the original entry was overwritten. Green on both runtimes (plain, MULTI, Lua), plus unit tests. |
| 8 | Script bodies and channel names pin the request buffer (P3) | FIXED | `89d9e68` | Red with `detach` reverted: "SCRIPT LOAD body: the stored bytes are a slice of the request buffer", and the same for the channel map key. |
| 9 | `tracking/mod.rs` over 1500 lines | FIXED | `dd8a618` | Verbatim move (diff-checked; only visibility changed). mod.rs 1765 → 1297, table.rs 486. |
| 10 | Stale inline-path comments | FIXED | `cd8d8bc` | Comments only. |
| 11 | `perf_ws7_tracking` proves the inline path | FIXED | `55aa5e9` | Red on baseline-ae21476 ("…was not served inline"). Green on both runtimes. |
| 12 | BCAST test vs the no-lock deadline test | FIXED | `23a9a40` | Serialized with a shared test lock; the deadline is unchanged. |

### Related bugs found and fixed
- **Scripts ran against a stale database clock** (`b799dbb`, `939a9cf`): routed scripts at `--shards 4`, tokio local scripts, and scripts queued in MULTI. Lua could therefore read an expired key as live.
- **`rdb::load` reset every database's `db_index` to 0** (`ca027a2`): after a restart from an AOF with an RDB base, db-3 events named db 0.
- **Two waits in the new tests raced under load** (`e4a387f`).

### Design notes
- **Item 1:**
  - DEL/UNLINK reap the expired key synchronously, answer `:0` and publish `expired` before the reply, as redis does.
  - A DEL arriving from the master counts the key as live and publishes `del`, as on a redis replica.
  - The coordinator's local leg and the owner's merged-DEL leg now still log a delete that reaped an expired key, so replicas get the record.
  - GETDEL keeps the lazy path: its reply was already nil, and its `expired` event comes on the next expiry tick, ≤100 ms. Making it synchronous would have cost replicas their delete record.
- **Item 5:** it reports stored size plus pending changes, not `billed_memory()` alone, because an MQ push does not settle its accounting. There is no debug assertion, since MQ edits the PEL directly.
- **Item 7:** `Stream::add` returns `Option` and refuses any ID ≤ `last_id`; it never overwrites.

## Gate (final tree `e4a387f`)
- fmt, audit-unsafe (no new `unsafe`) and audit-unwrap clean.
- clippy `--all-targets -D warnings` on monoio and tokio; tokio `check --all-targets`.
- Lib: monoio full run 6607 passed; tokio touched modules 1130 passed.
- Integration: every touched suite on both runtimes, pinned binary, provenance checked. `replication_swapdb` 3/3 on monoio (`--include-ignored`); it cannot run on tokio by design.
- Rows were added to `test-consistency.sh` and `test-commands.sh` for DEL of an expired key and for XSETID. Their commands were checked against redis 7.0.15, the baseline and the fix at s1 and s4. The two scripts were not run end to end.

## Cross-ownership edits
- `src/shard/mq_exec.rs` (WS16): one hunk in `handle_push`, for the new `Stream::add` return type (`549a0c5`).
- `src/storage/db/kv_ops.rs`: the DEL-path region only, plus a small enum next to `ExpiredRemoval` (`333bc82`). The file is at 1495 lines.
- Unowned files:
  - clock fixes: `spsc_handler.rs`, `handler_sharded/mod.rs`, `shared.rs`;
  - item 1 and the RDB fix: `rdb.rs`, `coordinator.rs`, `replication/apply.rs`.

## Risks
1. **Binary aliasing:** `target/debug/moon` is overwritten by other worktrees. Verify provenance on any re-run.
2. **Clock granularity:** an idle shard refreshes its clock every 10 ms, so a key can read as live up to ~10 ms past its TTL (pre-existing). The tests wait 15 ms, the script rows 50 ms.
3. **Commit order:** the tokio Lua row at s1 passes only from `939a9cf` on.
4. **Replica parity depends on `apply_local`:** a new replica apply path would need the same marker.
5. **Old AOFs:** a replayed below-top XSETID is now refused and keeps the original entry, where the old server had overwritten it. MQ WAL replay of an ID ≤ `last_id` whose entry is absent is skipped.
6. **Merge conflicts:** the `Stream::add` signature change conflicts with WS16's `mq_exec.rs`.
7. **Pre-existing, not fixed:**
   - `seq + 1` overflow at `u64::MAX` panics a debug build; a release build refuses the wrapped ID;
   - XSETID `ENTRIESADDED`/`MAXDELETEDID` are ignored;
   - MQ's direct PEL edits drift from the stream accounting;
   - SET with options (KEEPTTL, EX) publishes no `set` event, and SET PX publishes no `expire`;
   - the tokio in-process test listener stamps every db as 0.

## CHANGELOG bullets
- DEL/UNLINK of a key whose TTL has passed now answers 0 and publishes `expired`, not `del`, on the plain, spanning, MULTI and Lua paths, as redis 7.0.15 does. The deletion still reaches the AOF and replicas. (moon#1234)
- Keyspace events name the right db after SWAPDB, and after a restart from an AOF with an RDB base. (moon#1234)
- XSETID below the stream's top item is refused with redis's error, and XADD can no longer overwrite an existing entry. (Fixes #1249)
- Lua scripts no longer read an expired key as live: routed scripts, tokio local scripts, and MULTI-queued scripts.
- MEMORY USAGE / DEBUG OBJECT of a stream is O(1), with the same value.
- Script bodies and pub/sub channel names no longer pin the connection's read buffer. (moon#1160)
- Tests: a `MOON_BIN` pin that names no file panics instead of falling back; stronger tracking and list-allocation assertions; a test-order flake removed from the tracking tests.

(Committed by the orchestrator from the FIX3B-CM agent's final report, because the harness refused the agent's SUMMARY write.)

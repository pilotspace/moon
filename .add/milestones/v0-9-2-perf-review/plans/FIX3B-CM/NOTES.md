# FIX3B-CM NOTES — part 3b review fixes (connection WS7 + memory/parity WS10/WS18)

- **Branch:** `fix/3b-conn-mem-review`, base `5d1a37c` (PR #1242 head).
- **Personas:** storage-durability-engineer (lead), routing-dispatch-engineer, ci-test-integrity-engineer.
- **Oracle:** redis-server 7.0.15 on PATH (`redis-server --version`: v=7.0.15). Every parity claim below was captured from it with the same bytes, through `scratchpad/fix3b-cm/probe_del.py`.
- **Binaries:** red runs use `moon-base`, a debug build of `5d1a37c` from this worktree. Green runs use `moon-fix-*`, debug builds of this branch. Provenance is checked by a string only the fixed code contains (`exhausted the last possible ID`: base 0 hits, fix 2).
- **Environment:** `--disk-free-min-pct 0` everywhere (the box is below moon's 5% default).

Each item: the mechanism I verified, the design, the risks, and a 0–1 self-score on
Completeness / Clarity / Practicality / Optimization / Edge cases / Self-evaluation.

---

## 1. S1 + P2 — DEL/UNLINK/GETDEL of an expired key (moon#1234 residual)

**Oracle (redis 7.0.15, `SET k 1 PX 1`, 3 ms later, KEA):**

| command | reply | events |
|---|---|---|
| DEL k | `:0` | `expired` (after `expire` from the SET) |
| UNLINK k | `:0` | `expired` |
| GETDEL k | nil | `expired` |
| MULTI; DEL k; EXEC | `[0]` | `expired` |
| EVAL redis.call('DEL') | `:0` | `expired` |
| DEL 6-expired + 1-live + 1-absent | `:1` | 6 × `expired`, `del` for the live one only |

**moon `5d1a37c` (measured):** DEL / UNLINK / MULTI / EVAL answered `:1` and published `del` at both `--shards 1` and `--shards 4`. The spanning DEL answered `:7` at s1. GETDEL answered nil with `expired` at s4. At s1 it once answered the value, because the idle shard's clock was stale; see Risks.

**Mechanism.**
- `DEL` calls `Database::remove_counting_cold` and `UNLINK` calls `Database::unlink`. Both counted any hot entry as removed, expired or not.
- `key::del` / `key::unlink` then published `del` for it.
- `GETDEL` already reads through `Database::get`, which hides an expired key and queues it for the active-expiry drain (`note_lazy_expired`). The drain then deletes it, publishes `expired` and records the dual-plane DEL. So GETDEL's reply was already right at every shard count, except when the idle clock was stale.

**Design — synchronous reap in the command body.**
- `remove_counting_cold` counts a hot entry only when it is live, the same rule it already applied to cold entries; DEL tells an expired reap by the hot entry it returns. The new `unlink_key` returns `KeyDeletion::{Absent, Live, Expired}` (`unlink` keeps its `bool`). `remove_hot_lazily` reports whether the entry it freed had expired, so UNLINK needs no second probe. `kv_ops.rs` stays at 1496 lines.
- `key::settle_deletion`:
  - `Live` → count it and publish `del`;
  - `Expired` → publish `expired`, count nothing;
  - `Absent` → nothing.
- The body runs on every path: plain, each owner leg of a spanning delete, MULTI/EXEC, and Lua.

**Why synchronous, and not the read path's "hide and let the drain delete".**
1. redis publishes `expired` BEFORE it replies, and the ws18 harness's FIFO sentinel settle depends on that ordering.
2. A replica runs no expiry of its own. It deletes an expired key only when the master's DEL arrives, and that DEL runs this very body. If the body hid the key instead of deleting it, every TTL key would leak on the replica forever.

**Propagation (durability lens).**
- A reaped key answers `:0` but IS deleted, so the deletion has to reach the AOF and the replicas.
- DEL and UNLINK propagate verbatim whatever they answer (`effect_rewrite` has no DEL arm). That covers the handler's local path, the SPSC legs, MULTI and Lua.
- Two paths skip a `:0`, and both now also log when the thread-local `key::expired_reaps()` counter moved during the leg:
  - the coordinator's in-process leg (`coordinate_multi_del_or_exists`: `n > 0` at the fast path and at the spanning local group);
  - the owner shard's merged `DEL k1 k2 …` leg in `MultiExecute` (`write_hooks::deleted_nothing`, moon#1184). The first cut of this fix missed it; the AOF test caught it (`DEL ws18:aof:DEL:1: the key appears 1 time(s) in the AOF`).
- Test: `a_spanning_delete_that_reaps_expired_keys_still_logs_them_at_shards_4` greps every `incr.aof` under `appendfsync always`.

**Replica parity.**
- redis's `expireIfNeeded` returns "not expired" for the master client.
- `replication::apply::apply_local` now marks its dispatch with a scoped thread-local (`MasterStreamScope`).
- An expired key deleted by the master's stream is therefore counted and publishes `del`, as on a redis replica. It does NOT publish `expired`.

**GETDEL.**
- GETDEL keeps the lazy path (hide + drain). Reaping synchronously in GETDEL would stop the drain's dual-plane DEL, and a replica's GETDEL hides the key rather than deleting it, so the replica would leak the key.
- Divergence kept on purpose: GETDEL's `expired` arrives on the next active-expiry tick (≤ 100 ms) instead of before the reply. The test waits for it.

**Found on the way: routed scripts ran on a stale clock (separate commit).**
- At `--shards 4` a script routed to the key's owner shard (`spsc_handler`'s EVAL/EVALSHA and FCALL arms) ran against a database whose cached clock no command there had refreshed.
- Measured on this branch before that commit: `SET k v PX 1`, then `EVAL "return redis.call('GET',KEYS[1])"` returned `v` up to 95 ms after expiry (4 of 6 trials), and `redis.call('DEL')` of an expired key answered `1` (5 of 6). `moon-base` (5d1a37c) returned `v` in 1 of 6; after the commit, 0 of 6.
- Both arms now `refresh_now_from_cache(cached_clock)` before running the script, as the Execute arms do before their dispatch.
- The tokio leg then failed the Lua row at `--shards 1`: tokio's handler refreshes the clock per command in its dispatch arm, which its LOCAL EVAL/EVALSHA and FCALL arms never reach. A script queued in MULTI (`txn_script`, via `execute_transaction_sharded`) had the same gap. A second commit refreshes the clock in all three. Red, with only the MULTI refresh removed, on a tokio build: "MULTI EVAL DEL: left [Int(1)], right [Int(0)]" at s1 and s4. monoio refreshes once per batch before any of these, so it was never red there.

**Risks.**
- **Clock granularity.** moon judges expiry against the shard's cached clock. An idle shard refreshes it every 10 ms (the idle park), so a key can read as live up to ~10 ms after its TTL. redis reads the real clock. The tests wait 15 ms past a 1 ms TTL for this reason. This is a pre-existing property of every moon expiry check, not something this fix introduces.
- **AOF replay.** A replayed DEL of an already-expired key reaps it (same state as before) and publishes `expired` into an empty listener set. No behaviour change.
- **Double-record.** On a master, the active-expiry tick can reap a key while a DEL for it is in flight. Each path then logs its own DEL. Harmless: a DEL of an absent key replays as a no-op.

**Scores.** Completeness 0.9 · Clarity 0.9 · Practicality 0.95 · Optimization 0.95 (zero added probes; one thread-local increment on the expired branch only) · Edge cases 0.9 · Self-evaluation 0.9.

---

## 2. S2 — keyspace events after SWAPDB name the wrong db

**Oracle.** After `SWAPDB 0 3`, `SET b` and `DEL b` in db 0 publish `__keyevent@0__:set` and `@0:del`. After `SELECT 3`, events publish `@3`.

**moon `5d1a37c`.** `DEL b` in db 0 published `__keyevent@3__:del` at s1 and s4. `SET b` published `@3` at s4 and `@0` at s1: the inline SET names the db from the connection, not from the database.

**Audit: every reader of `Database::db_index`.** I renamed the field and ran `cargo check --all-targets`. The compiler listed 17 sites, which are the only accesses in the crate (tests and benches included):
- **readers:** `command/key.rs` (DEL, UNLINK, RENAME's `rename_from`/`rename_to`) and `command/string/string_{read,write}.rs` (`set`, `keymiss`, `del`, …). Every one is `notify_keyspace_event(…, db.db_index)`.
- **writers:** `shard/mod.rs:166` (stamped once when the shard builds its array), plus the two new fixes.
- **No WAL/AOF/cold/spill/tracking reader:**
  - the AOF and WAL use the connection's selected db / the message's `db_idx`;
  - spill uses `SpillContext.db_index`, restamped per loop index (`timers.rs:214`);
  - cold files carry `FileEntry.db_index` from the spill request;
  - tracking is keyed by key bytes;
  - active expiry names its event from the loop index `i`.

So the index is the SLOT's identity — "which logical db this is". After a SWAPDB, the contents in slot 0 are what `SELECT 0` addresses. The events must say 0.

**Design.**
- `db_plane::swap_contents(a, b)`: `mem::swap` the databases, then swap the two `db_index` values back.
- It is used by the live swap (`ShardDbSet::swap`, shared by the coordinator, the SPSC leg and replica apply), by AOF/WAL replay (`replay.rs`, which used a bare `mem::swap`) and by the legacy tokio single-listener (`handler_single.rs`).

**Same class, found by the audit (separate commit).**
- `rdb::load` and `rdb::load_from_bytes` replace each live db with a `Database::new()` (`*live = temp`, `db_index` 0).
- Measured on `5d1a37c`: after a restart from an AOF with an RDB base, `DEL seed` in db 3 published `__keyevent@0__:del` at s1 and s4.
- A restart from a per-shard RDB snapshot (`--save`) was already correct.
- `keep_slot_identity` carries the live slot's index onto the temp.
- Unlike the cold wiring, the index is right for EVERY caller (local replay, replica full sync, DEBUG RELOAD): it names the slot, not the old contents.

**Residual.** `server/listener.rs` (the tokio in-process test harness `run_with_shutdown`) builds every db with `Database::new()`, so all its events say db 0. Production never runs it and no test exercises notifications through it. Noted, not changed.

**Scores.** Completeness 0.95 · Clarity 0.95 · Practicality 1.0 · Optimization 1.0 · Edge cases 0.9 · Self-evaluation 0.9.

---

## 3. S4 — `find_moon_binary` falls back silently

- **Mechanism:** a set, non-empty `MOON_BIN` whose file does not exist fell through to `CARGO_BIN_EXE_moon`.
- **Design:** `assert!(p.is_file())`, with a message that names the pin and the two ways out. Unset, empty and whitespace-only `MOON_BIN` still fall back.
- **Test:** `tests/moon_bin_pin.rs` re-executes its own test binary as a child per case, so no `set_var` touches the process env.
- **Red:** with the old helper, the missing pin resolved to `target/debug/moon` (`moon_bin_pin.rs:48`).
- **Risk:** a Windows user who set `MOON_BIN=.../moon` without `.exe` now gets a panic instead of a silent fallback. That is the intent.

**Scores.** 0.95 · 0.95 · 1.0 · 1.0 · 0.9 · 0.95.

---

## 4. N2 — `list_pop_alloc_942` full-encoding push

- Adds `assert_eq!(full_push, ROUNDS, …)`: one exact-size copy per push since moon#1160.
- Zero would mean the push stores a request-buffer slice. More than one would mean a double copy or a deque reallocation inside a stationary window.
- Red: revert `detach` in `list_write.rs` push, and `full_push` drops to 0.

**Scores.** 1.0 · 0.95 · 1.0 · 1.0 · 0.9 · 0.95.

---

## 5. N4 — MEMORY USAGE of a stream runs an O(n) scan

**Mechanism.**
- `estimate_serialized_length`'s stream arm (used by both `MEMORY USAGE` and `DEBUG OBJECT`) called `Stream::estimate_memory()`, a walk of every entry, group, consumer and PEL slot.

**Design.**
- New `Stream::memory_usage()` = `STREAM_BASE + billed + unbilled` (clamped at 0), in O(1).
- It equals the scan by the moon#1163 lockstep: every mutating method keeps `unbilled` exact.
- After any stream command it IS `billed_memory()`, because each command drains before it returns.

**Why not `billed_memory()` verbatim (the brief's wording).**
- An MQ push (`Stream::add` from `mq_exec`) does not drain. After 20K MQ PUSHes, `billed_memory()` would report ~STREAM_BASE for a large queue, while the scan and `memory_usage()` report the real size.
- The MQ billing gap itself is the separate moon#1163 residual the review filed. It is not an item here.

**Evidence.** A 200K-entry stream, debug builds, same host: `MEMORY USAGE st` = 38000114 on both, averaging 19.0 ms on `moon-base` and 0.048 ms on this branch.

**Guard.**
- `assert_memory_usage_is_the_scan` runs inside `assert_exact`, so it covers every step of `every_stream_write_keeps_billed_equal_to_measured` (27 stream commands) and the whole-stream and unbilled tests.
- Its assertion: `MEMORY USAGE == 48 + key + estimate_memory()`.
- No debug assertion in the server: MQ's direct `group.pel.remove` / `pel.insert` (`mq_exec.rs:686`, `shared_databases.rs:808`) bypass `unbilled`, so a debug assertion there would abort a debug server on MQ queues.

**Scores.** Completeness 0.9 · Clarity 0.95 · Practicality 0.95 · Optimization 1.0 · Edge cases 0.85 (the MQ PEL drift is pre-existing and outside this item) · Self-evaluation 0.9.

---

## 6. N1 — wrong listpack comments

**Verified against redis 7.0.15, `RPUSH l a b 7`.**
- redis `DUMP l` starts `0x12` (QUICKLIST_2) and carries the listpack.
- moon `DUMP l` starts `0x01` (LIST) and carries the elements.
- `dump_payload::decode` refuses encodings 16–20 (`UnsupportedEncoding`).
- The test `persisted_forms_rebuild_through_the_encoder` does not exist.

**Change.**
- Both comments now say that only the IN-MEMORY bytes match, and that the goldens were captured from the listpack embedded in redis's DUMP.
- They cite `value_codec`'s `small_*_round_trips_back_to_listpack*` tests and `dump_payload`'s `a_listpack_encoding_is_named_not_called_corrupt`.
- Comment-only.

**Scores.** 1.0 · 0.95 · 1.0 · 1.0 · 1.0 · 0.95.

---

## 7. moon#1249 — XSETID below the top item, then XADD * overwrites

**Oracle (redis 7.0.15).**
- Below the top item → `ERR The ID specified in XSETID is smaller than the target stream top item`.
- Equal to or above the top item → OK, even below `last_id`.
- Empty stream → any ID.
- Missing key → `ERR no such key`.

**moon `5d1a37c`.** Every case answered OK. Five `XADD *` after a below-top XSETID left XLEN 2 and `orig5` gone. `BTreeMap::insert` replaced the entry, `length` counted it again, and `last_id` moved backward.

**Design.**
- `xsetid` refuses `id < top item` (`entries.last_key_value()`), with the exact text. The one body serves every path: plain, MULTI, Lua, replica apply and replay.
- `Stream::add` now returns `Option<StreamId>`:
  - `id <= last_id` → `None`, nothing changed;
  - an occupied slot (a damaged load with `last_id` below an entry) → `None`, via the entry API — the same single traversal an insert costs.
- Callers that ignore the result keep compiling (`Option` is not `must_use`); every one of them passes `next_auto_id`.
- Five `storage::stream` unit tests used `0-0` as their first fixture ID, which no command can produce (XADD refuses it). The full lib run caught this. They now start at `1-0`, with the same assertions shifted.
- `xadd` and `mq_exec::handle_push` answer a refusal with redis's "exhausted the last possible ID" text. Only a sequence wrap at `u64::MAX` can reach it.

**Risks.**
- **Old AOFs.** Replaying an AOF that recorded an accepted below-top XSETID now refuses it. The later explicit-ID XADD that overwrote the entry is then refused by XADD's own validation. The replayed state keeps the original entry instead of the overwrite. That divergence exists only for data the bug had already corrupted.
- **MQ WAL replay** (`apply_mq_push`) of an ID ≤ `last_id` whose entry is absent is now skipped instead of re-inserted with `last_id` moving backward. Such an ID can only belong to an entry deleted before the snapshot that carries the higher `last_id`.
- **Pre-existing, not fixed:**
  - `next_auto_id` and XADD `ms-*` compute `seq + 1` unchecked. A debug build panics at `u64::MAX`; a release build wraps, and `add` now refuses the wrapped ID.
  - XSETID's `ENTRIESADDED` / `MAXDELETEDID` options are parsed by redis and ignored by moon, because moon's Stream has no such fields.

**Scores.** Completeness 0.9 · Clarity 0.95 · Practicality 0.95 · Optimization 1.0 · Edge cases 0.9 · Self-evaluation 0.9.

---

## 8. P3 — script bodies and channel names pin the request buffer

**Mechanism.** `ScriptCache::store_source` stored the caller's `Bytes`: a slice of the request buffer from SCRIPT LOAD, EVAL, the fan-out claim and fan-out arrivals. `PubSubRegistry::{subscribe, psubscribe, ssubscribe}` and `RemoteSubscriberMap::{add, add_shard_channel}` stored channel and pattern names the same way. The cache keeps a body until SCRIPT FLUSH and a registry keeps a name for the life of the subscription, so each held a whole read buffer.

**Design.** `storage::owned_bytes::detach` at insert.
- The script cache copies on a real (Vacant) insert only.
- `subscribe`/`ssubscribe` make one copy shared by the forward and reverse maps.
- `psubscribe` and the remote map copy a NEW name only.

**Test.** A shared `owned_bytes::test_support::WireArgs` parses a real RESP request into a 4 KiB buffer and checks stored pointers against its range, the WS10 method. `stored_bodies_never_share_the_request_buffer` covers the script cache. `stored_channel_names_never_share_the_request_buffer` covers the registry and remote map.

**Red, with `detach` reverted:** "SCRIPT LOAD body: the stored bytes are a slice of the request buffer (moon#1160)", and "channel map key: …".

**Scores.** 0.95 · 0.95 · 1.0 · 0.95 (one copy per SUBSCRIBE even when the name exists; SUBSCRIBE is not a hot path) · 0.9 · 0.9.

---

## 9. `tracking/mod.rs` over 1500 lines

- A verbatim move of `TrackingTable` (struct, `Default`, impl) into `tracking/table.rs`, re-exported as `crate::tracking::TrackingTable`.
- Fields and `new_global` become `pub(super)`: the same reach they had as private items of `tracking`. Nothing else changed.
- The tests stay in `mod.rs`. Result: `mod.rs` 1765 → ~1300 lines, `table.rs` ~490.

**Scores.** 1.0 · 0.95 · 1.0 · 1.0 · 1.0 · 0.95.

---

## 10. Stale inline-path tracking comments

- `blocking.rs` (`try_inline_dispatch` doc) and `handler_monoio/mod.rs` (read-gate comment) still described the pre-moon#1166 design.
- Both are corrected. The gate summary's stale "no spill_sender" term (moon#660) is fixed in passing.
- Comment-only.

**Scores.** 1.0 · 0.95 · 1.0 · 1.0 · 1.0 · 0.95.

---

## 11. `perf_ws7_tracking` proves which path served the SET

**Change.**
- The default-mode block finds the writer's home shard: the one whose key its plain SET is inlined for, via the `local_inline` delta.
- It tracks a key on that shard, and asserts `local_inline` moved by exactly 1 around the tracked SET on monoio (0 on tokio, which has no inline path).
- The NOLOOP section documents that `writer_client_id` on the inline path is defensive, because a tracking connection never inlines writes. The same note is on `try_inline_dispatch`'s parameter.

**Red.** `MOON_BIN=/home/user/wt/bin/baseline-ae21476` (pre-#1166): the tracked SET is not inlined while `t` tracks, delta 0.

**Scores.** 0.95 · 0.95 · 0.95 · 1.0 · 0.9 · 0.9.

---

## 12. BCAST prefilter test vs the no-lock deadline test

**Mechanism.**
- `bcast_prefixes_are_counted_and_released` registers BCAST prefixes on a GLOBAL table.
- While they are live, `global_may_track` answers `true` for every key. That is correct: any prefix may match.
- So a concurrent `untracked_write_takes_no_global_table_lock` worker takes the lock its own thread holds and misses the 500 ms deadline.

**Design.** A shared `prefilter::GLOBAL_COUNTERS_TEST_LOCK` (`parking_lot::Mutex<()>`, const-initialised, test-only), held for the whole body of both tests. The deadline is unchanged.

**Evidence.**
- No other lib test registers a BCAST prefix on a global table: the `client_cmd` tests and `invalidation`'s BCAST test use private tables, and private tables never touch the counters.
- A scratch probe, not committed, ran the no-lock test's exact body with one BCAST prefix registered on a global table. It failed: "the untracked write blocked behind the lock while a BCAST prefix was live". That is the flake's mechanism, made deterministic.

**Scores.** 0.95 · 0.95 · 1.0 · 1.0 · 0.9 · 0.9.

---

## Instrument notes (ci-test-integrity lens)

- **Shared target, aliased binaries.** Other worktrees build the same package into `/home/user/wt/target`, and `target/debug/moon` is overwritten by whoever links last. One monoio round here ran against a foreign binary and failed exactly as `5d1a37c` does. The provenance check caught it: `exhausted the last possible ID` had 0 hits in the copy. Every verdict in SUMMARY comes from a copy whose marker was checked right after the build.
- **Timing.** The expired-key tests wait 15 ms (50 ms in the script rows) past a 1 ms TTL, because moon's idle shard refreshes its cached clock every 10 ms. Whether the 100 ms active-expiry tick reaps a key first does not change the expected reply or events, so the tests do not depend on when that tick lands. They go red on `5d1a37c` for every key the tick has not reaped: 10 of 12 in the recorded run.
- **`replication_swapdb`** is `#[ignore]`: it needs a monoio master (PSYNC-as-master is monoio-only). It was run with `--include-ignored` against the monoio debug build, 3/3. It cannot run on the tokio leg by design.
- **Script rows.** The new `test-consistency.sh` / `test-commands.sh` rows were exercised by extracting their capture function and commands. They were run against redis-server 7.0.15 and against `moon-base` / `moon-fix` at s1 and s4. Fix = redis at both. Base differs on the spanning row at both and on the first row at s1. The whole scripts were not run end to end: they build their own release binary.
- **Stress.** Under CPU load (6 busy loops on 4 cores), two waits in this branch's own new tests turned out to be races, and both were fixed in `e4a387f`:
  - the restart test killed the server before a slow rewrite had started;
  - the AOF test read the file before the active-expiry tick's asynchronous `record_reason_del` landed.
  After the fix: 8 + 10 loaded runs, all green.

## Final gate (tree at `e4a387f`)

| gate | result |
|---|---|
| `cargo fmt --check` | clean |
| `scripts/audit-unsafe.sh` | PASSED: 244/244 blocks have SAFETY comments; no new `unsafe` |
| `scripts/audit-unwrap.sh` | PASSED (0 against a baseline of 0) |
| `cargo clippy --all-targets -- -D warnings` (monoio) | clean |
| `cargo clippy --all-targets --no-default-features --features runtime-tokio,jemalloc -- -D warnings` | clean (a superset of the brief's lib-only tokio clippy) |
| `cargo check --all-targets` (tokio) | clean |
| lib, monoio, full | 6607 passed, 0 failed, 14 ignored |
| lib, tokio, touched modules + `server::conn` + `shard::{coordinator,spsc_handler}` | 1130 passed, 0 failed |
| integration, monoio, pinned verified binary | perf_ws18_del_notify 5/5 · perf_ws7_tracking 3/3 · list_pop_alloc_942 1/1 · watch_container_mutation_926 14/14 · moon_bin_pin 1/1 · keyspace_event_db_index 4/4 · xsetid_top_item_1249 2/2 · replication_swapdb 3/3 (`--include-ignored`) |
| integration, tokio, pinned verified binary | the same suites, all green (tokio has no inline path, so perf_ws7_tracking runs 2/2; replication_swapdb is monoio-only by design) |

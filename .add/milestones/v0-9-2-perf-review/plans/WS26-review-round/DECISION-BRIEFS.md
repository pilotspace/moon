# Decision briefs: #1283, #1266, #1185 (TXN.ABORT scope)

Base: `main @ 273e6bc`. These briefs come from reading the code only. Nothing was built or run.
A claim marked **[probe]** was inferred from the code and should be confirmed with a test before anyone acts on it.

---

## Brief 1: #1283, a time record in the AOF for the replay expiry clock

### What moon does today (the premise needs a correction)
- **Option (b) is already shipped.** Every relative expiry is already logged with an absolute deadline:
  - `EXPIRE`/`PEXPIRE` are logged as `PEXPIREAT`.
  - `SETEX`/`PSETEX`/`SET … EX|PX` are logged as `SET … PXAT`.
  - `GETEX … EX|PX` is logged as `PEXPIREAT`.
  - The `HEXPIRE` family is logged as `HPEXPIREAT`.
  - Sources: `src/persistence/aof/encode.rs:11-13,134,214-294`, `src/persistence/aof/mod.rs:810-830`, `replication::expire_rewrite` / `effect_rewrite` (called from `serialize_effect_for_log`, `aof/mod.rs:836-869`), and the doc comment in `src/persistence/replay/clock.rs:37-43`.
  - So replay never *computes* a deadline from a clock.
- **What #1283 is really about is the expiry judgment clock**: at replay time, was key K alive when record r was written?
  - moon#542 hides a lazily expired key and defers its deletion (`storage/db/kv_ops.rs:81-87,118`).
  - The next active-expiry tick emits the `DEL`, but only if the key is still expired when the tick re-checks it (`server/expiration.rs:237-256`).
  - So if a write replaced the key first, the log has `SET k v PXAT D … INCR k` with **no DEL between them**. Replay has to decide for itself that k had expired by the time of the INCR.
- Part 5 pins that judgment to the newest log file's mtime, capped at the wall clock:
  - the pin itself: `clock.rs:106-115`;
  - applied per replayed record: `replay.rs:344-367`;
  - pinned at `aof/mod.rs:1044`, `aof_manifest/shard_replay.rs:112,537`, `recovery.rs:601` and `wal_v3/replay.rs:164`.
- When the mtime is earlier than the true write time, the pin behaves like expiry suppression (`clock.rs:44-58`). That is the touchback failure: 27–36 of 40 keys wrong.

### Redis 7.x / 8.x
- **Replay needs no clock.** `keyIsExpired()` returns 0 while `server.loading`, so nothing expires during AOF load.
- **This is exact because of how expiry is logged.** When a command finds a key expired, `expireIfNeeded` calls `deleteExpiredKeyAndPropagate`, which calls `propagateDeletion`. That logs the `DEL`/`UNLINK` through `alsoPropagate` **before the command's own record**, in the same execution unit: `propagatePendingCommands` flushes it, wrapped in MULTI/EXEC when there is more than one op. `feedAppendOnlyFile` then appends it to `aof_buf`.
- **Relative-to-absolute rewriting:**
  - Before 7.0, `feedAppendOnlyFile` did it: `catAppendOnlyExpireAtCommand`, plus SETEX/SET EX turned into `SET` + `PEXPIREAT`.
  - Since 7.0 it happens at propagation time (`rewriteClientCommandVector` in `expireGenericCommand` / `setGenericCommand`, e.g. `SET … PXAT`), so replicas get it too.
- **`#TS:<unix-seconds>` (7.0 multi-part AOF, `aof-timestamp-enabled`, default `no`):**
  - Where it is written:
    - `feedAppendOnlyFile` adds it through `genAofTimestampAnnotationIfNeeded(0)` whenever `server.unixtime` has advanced;
    - the rewrite forces one at the head of the base.
  - What it is for: point-in-time recovery only (`redis-check-aof --truncate-to-timestamp`).
  - The loader skips `#` lines. **Redis never uses `#TS` to judge expiry.**

### Options
| | What it is | Fixes touchback? | Format impact |
|---|---|---|---|
| (a) TS record | A periodic time record; replay moves its judgment clock forward from it | Yes, if the stamp is exact (see below) | New record in the incr / WAL stream |
| (b) Absolute expiry | Already done | No: it answers a different question | — |
| (c) Both | Same as (a) | Same as (a) | Same as (a) |
| (d) Root cause (redis parity) | Log the `DEL` for a lazily observed expiry **before** the record of the command that saw it; then suppress expiry during replay, as redis does | Yes, and replay no longer needs a clock at all | Needs a per-generation "complete log" marker so replay can tell new logs from old ones (old logs keep the mtime fallback). Has to reach every write path: dispatch, scripts, MULTI, blocking wakers, cross-shard legs |

### Encoding: redis's `#TS:` line would break moon's own readers
- In moon's parser, `#` is the RESP3 Boolean type and must be exactly `t` or `f` (`protocol/parse.rs:364-372,538-556`). `#TS:…` is therefore a parse error.
- Flat or TopLevel RESP stream: replay treats the error as corruption and **stops**, dropping every record after it (`aof/mod.rs:1167-1196`).
- Per-shard framed incr: a parse error or a non-array frame fails with `RewriteFailed`, so **the boot fails** (`aof_manifest/shard_replay.rs:329-395`).
- **WAL v3:** a new `record_type` byte decodes to `from_u8 → None` (`wal_v3/record.rs:126-152,281`), which replay reads as a torn record and stops (`wal_v3/replay.rs:471-481`).
- **Safe encoding:** a RESP array pseudo-command, `MOON.TS <ms>`.
  - Precedent: `MOON.COLDCUT` / `MOON.SPILLED` (`persistence/cold_records.rs:1-24`), which are intercepted before dispatch (`replay.rs:379-388`).
  - Old binaries send it to dispatch, get "unknown command", classify it as `ReplayRoute::Unhandled` and skip it silently (`replay.rs:546-556`). Nothing branches on `Unhandled`.
  - Write it with `lsn = 0` so the framed `max_lsn` is unaffected (`shard_replay.rs:367-370`).
  - Keep it AOF-only, like `MOON.SPILLED`: replicas apply on the wall clock (`clock.rs:62-63`).
  - In WAL v3, carry it as a `Command` record, not as a new type byte.
- **STORAGE-FORMAT-V1 §2 rule 4** (`docs/STORAGE-FORMAT-V1.md:36-41`) is not triggered: the WAL record layout, the RDB v2 preamble and the manifest framing are all unchanged.
  - §3.3 should list the replay-only pseudo-commands.
  - A downgraded binary reads the new logs correctly, with the mtime judgment.
  - A redis server loading a moon AOF already fails on `MOON.COLDCUT`, so this adds no new incompatibility.

### Making the stamp exact (the part the issue's proposal gets wrong)
- **A stamp taken on the writer thread is only an upper bound.**
  - A record can sit in the 10k channel for milliseconds up to its backlog before the writer sees it.
  - So a record written after a TS can have *executed* before it.
- **The exact value is the shard clock the command itself judged with.**
  - `CachedClock` updates once per 1 ms tick (`storage/db/mod.rs:1350-1355`).
  - `serialize_effect_for_log` already reads that clock (`aof/mod.rs:855-860`).
- **Design:**
  1. Carry `clock_ms` in `AofMessage::Append`. It stays in memory only, like `epoch` (`aof/mod.rs:343-344`).
  2. The writer emits `MOON.TS <clock_ms>` whenever the value differs from the last one it emitted in stream order. That also handles reordering from parked enqueues.
  3. Replay sets the judgment clock to the most recent TS. It does **not** take a running maximum, so the clock can move backwards if records arrive out of order.
  4. Replay falls back to the mtime until a file's first TS.
- **Volume:** at most one TS per 1 ms tick in which the shard logs a write, i.e. ≤1000/s/shard at about 35 B each (≤35 KB/s/shard), roughly +1% records at 100K writes/s.
- Also emit one TS at each generation head, next to `MOON.COLDCUT`.
- A side benefit: it enables AOF point-in-time recovery later, which is redis's actual use of `#TS`.

### Fuzz impact
- No existing target exercises AOF *replay*. The `resp_parse*` targets cover only the RESP layer; `wal_v3_record` covers only the record decoder.
- The new argument decoder, plus the replay-framing paths it touches, need a new target (e.g. `aof_incr_replay`: framed and flat bytes through `DispatchReplayEngine` into a `Database`).
- Add it to **both** matrices in `.github/workflows/fuzz.yml` (PR list :49-78, nightly list :137-166) and to the lint `cargo check --manifest-path fuzz/Cargo.toml --all-targets`.

### Recommendation
1. **Ship (a) with the exact stamp**: `MOON.TS` as a pseudo-command, stamped from the shard clock carried on `AofMessage`.
   - It is format-compatible in both directions and small (writer, replay clock, one enum field).
   - It makes the touchback probe deterministic.
   - Tests:
     - the `touchback` probe;
     - a mixed old/new generation;
     - a downgrade-read test: an old-format replay of a file containing `MOON.TS` gives the same keyspace;
     - the existing `moon_1277_*` suites.
2. Treat (b) as done. It is not a lever here.
3. File (d) as the redis-parity end state (clock-free replay, which also fixes replicas). It is a much larger audit of write paths.
4. Independently: make the flat-AOF reader skip a top-level `#…\r\n` line that is not a Boolean.
   - A redis 7 incr file with `aof-timestamp-enabled yes` fed to moon's legacy `appendonly.aof` import today stops at the first `#TS:`.
   - Low priority: moon does not import redis's `appendonlydir` manifest.

---

## Brief 2: #1266, `appendfsync everysec` loses acknowledged writes on kill -9

### Redis 7.x / 8.x ordering, and a nuance the issue omits
- **Where the log bytes go:** `call()` → `propagateNow` → `feedAppendOnlyFile`, which appends to `server.aof_buf` (process memory).
- **Order in `beforeSleep`:**
  1. `flushAppendOnlyFile(0)` does the `write(2)`;
  2. **then** `handleClientsWithPendingWritesUsingThreads()` sends the replies.
  - The source comment: "must be done before handleClientsWithPendingWrites… in case of appendfsync=always".
  - Replies are never written from inside `call()`.
- **Fsync under everysec:** `aof_background_fsync()` queues a `BIO_AOF_FSYNC` job at most once per second.
- **Nuance: the kill -9 guarantee is not absolute in redis either.**
  - When an fsync is still running in the background (`sync_in_progress`), `flushAppendOnlyFile` **postpones the write** for up to 2 s (`aof_flush_postponed_start`). The reason given: on Linux, `write(2)` would block behind the fsync anyway.
  - During that window **replies are still sent**. After 2 s it writes anyway and increments `aof_delayed_fsync`.
  - So on a slow disk redis can lose up to ~2 s of acknowledged writes to kill -9. On a healthy disk it loses 0.
- **Write errors:** `aof_last_write_status = C_ERR`, and further writes are refused with `-MISCONF` (`writeCommandsDeniedByDiskError`).

### moon's order today
- **Where the record goes before the reply:**
  - The command runs; `serialize_effect_for_log` builds the record; `send_append_group` / `try_send_append_durable` **enqueue it into a bounded flume channel** of capacity 10,000 per shard (`main.rs:958-977`; `aof/pool.rs:298-333,377-386`; `handler_monoio/mod.rs:3898-3975`).
  - The reply is pushed right after. Under everysec, nothing waits for the writer (`fsync_barrier` returns immediately, `pool.rs:537-558`).
  - So **the reply is sent while the record is still only in process memory.**
- **The writer is a dedicated OS thread per shard** (`aof-writer-{sid}`, `main.rs:965-977`). Its monoio loop:
  - polls the channel with `std::thread::sleep(step)`, not a parked wait (`writer_task.rs:156-180`);
  - `step` is between 0.5 and 50 ms (`AOF_IDLE_WAIT_STEPS` 50 ms → 1 s, `:90-94`), so the first write after an idle period waits **up to 50 ms** before the writer picks it up;
  - handles each drained batch with one `write_all` (`:1869`) and no fsync under everysec (`:1905-1913`);
  - runs the 1 s fsync **inline on the writer thread** (`:2055-2080`), so the channel does not drain while a fsync is slow.
- **Everything a kill -9 can lose (for Option 2's documented bound):**
  - the channel backlog (≤10k records per shard);
  - the poll latency (≤50 ms after idle);
  - a fsync stall;
  - the **tokio leg** additionally keeps up to 8 KiB in a user-space `BufWriter` after each batch (`writer_task.rs:30,37,60-69`);
  - **WAL v3** (for non-KV engines, and the KV log when there is no multi-part AOF) buffers in the shard until 4 KiB (`wal_v3/segment.rs:912-921`, flushed from `event_loop.rs:1612,2418`), otherwise until the 1 s `request_sync` (`shard/timers.rs:735-741`). A small write can stay in process memory for **up to 1 s**.

### Options and what each costs in moon
| | Mechanism | Cost | Risk |
|---|---|---|---|
| **1A** Write on the shard thread | Per-shard staging buffer on the shard thread. The first connection in a loop iteration to reach its reply flush (the `resolve_local_leg_barrier` point, `server/conn/shared.rs:4953`) does **one `write(2)`** of everything staged. Later flushes in the same iteration find it empty | 1 syscall per loop iteration that has writes. Estimated ~1–3 µs for a small page-cache append on ext4/xfs **[probe]**. Amortized over c and P. No park | The writer currently owns framing, SELECT injection (`aof/mod.rs:927`), the fold floor (`keep_unless_folded`, `:1006`), the rewrite overflow and the incr switch. Moving the write means handing the fd over at rewrite. `write(2)` can block behind a concurrent fdatasync on the same inode (redis's reason to postpone), so the fsync must move to an agent thread (the `wal_v3/sync_agent.rs` pattern) and the postpone rule be reproduced. Largest refactor |
| **1B** Write barrier on the writer thread | Under everysec, a zero-length barrier (like `fsync_barrier`'s empty `AppendSync`), acked **after `write_all`**, awaited once per connection batch | **One park per connection batch.** The measured park costs ≈24.9 core-µs (`docs/internal/cross-shard-cost-model.md:33`), so at P=1 per-op CPU could roughly double **[estimate]**; at P=16 it is ~1.5 µs/op | The writer must park on `recv` (a futex wake on the shard thread, which `writer_task.rs:146-155` deliberately avoids). The inline 1 s fsync would block every barrier → a p99 spike each second, so the fsync must move off the writer thread first. Smallest code change, largest hot-path cost |
| **2** Keep and document | Put the bound above in the README, the `appendfsync` docs and `env-knobs.md`; pin it in the crash matrix | 0 | Semantics differ from redis's documented behaviour |
| **3** Narrow the window (combines with 2) | Park-free but shorter poll steps after idle; flush the tokio tail every batch; move the everysec fsync to an agent thread (redis's BIO model) | ~0 | Shrinks the window; does not close it |

### Measurement plan (A/B on the same host, Linux only per CLAUDE.md)
- **Binaries:** baseline (`273e6bc`) and a 1A prototype (plus 1B if cheap to build). Same release flags, no `target-cpu=native` on either.
- **Server:** `moon --shards 1 --appendonly yes --appendfsync everysec --dir <local NVMe ext4>`, fresh process and emptied directory for every run. Also run redis 7.x/8.x with `appendonly yes appendfsync everysec` as the reference.
- **Load:** `redis-benchmark -t set -n 2000000 -r 1000000 -d 16` over the matrix **P ∈ {1,16} × c ∈ {1,50}**.
  - Always pass `-r` (CLAUDE.md gotcha).
  - Parse output with `tr '\r' '\n'` and take fields by position.
- **Repetitions:** ≥3 per cell, interleaved A,B,A,B,A,B. Report the median and the range.
- **Metrics:**
  - rps, p50 and p99 (redis-benchmark summary);
  - core-µs/op (utime+stime delta from `/proc/<pid>/stat` ÷ ops);
  - `write`/`pwrite64` and `futex` syscalls per op (`perf stat -e 'syscalls:sys_enter_write,syscalls:sys_enter_futex' -p <pid>`), to confirm 1A adds about one write per iteration and 1B about one futex per batch.
- **Stress arms:**
  - (i) a slow-fsync arm: concurrent `fio --rw=randwrite --direct=1` on the same device, to compare p99 against redis's postpone behaviour;
  - (ii) `--shards 4`, as a sanity check only.
- **Durability acceptance (crash-matrix leg):** N=10,000 acknowledged SETs, `kill -9` within 1 ms of the last reply, restart, count missing keys; 20 repetitions.
  - Option 1 must lose 0 on a healthy disk.
  - Option 2 pins the measured maximum as the documented bound.
- **Suggested gate for 1A:** median rps within −3% at P=16 (both c) and at P=1 c=50; within −8% at P=1 c=1; p99 no worse than +10%.

### Recommendation
- Do **3 now in any case**, since it is cheap and narrows the window:
  - move the everysec fsync off the writer thread;
  - flush the tokio tail on every batch;
  - shorten the post-idle poll step.
- **Then measure 1A.** Adopt it if it passes the gate above; otherwise take Option 2 with the measured bound.
- Do not pursue 1B. The park cost model (`cross-shard-cost-model.md` §1) predicts it is the most expensive way to get the same guarantee.
- Either way, document redis's 2 s postpone caveat, so moon's claim about everysec is not stronger than redis's.
- Fuzz and format impact: **none**. The barrier is never written to disk, and the on-disk bytes do not change.

---

## Brief 3: #1185, the scope of TXN.ABORT before the incremental COW fold

### What TXN is in moon
- `TXN.BEGIN/COMMIT/ABORT` is a moon-only cross-store transaction, **local to one shard**:
  - it covers KV, vector, graph and MQ (`src/transaction/mod.rs:1-5,91-120`);
  - cross-shard writes, MOVE and SWAPDB are refused and poison it (`handler_monoio/mod.rs:3380-3395,4246-4256`; `dispatch.rs:1406-1415`).
- **Writes apply eagerly to the live keyspace.**
  - Before dispatch, a KV undo record is captured for every written key: `UndoRecord::{Insert,Update,Delete}` (`transaction/undo_log.rs`, capture at `handler_monoio/mod.rs:3644-3710`).
  - Other TXN readers are filtered through `kv_write_intents`.
- **The KV writes are also logged to the AOF and the replica stream at execution time.** The effect-log path (`handler_monoio/mod.rs:3913-3975`) has no `in_cross_txn` check.
- COMMIT additionally writes a WAL v3 `XactCommit` (`handler_monoio/txn.rs:126-148`). There is no AOF equivalent.
- **ABORT** (`transaction/abort.rs:95-127`):
  - replays the KV undo through `db.set` / `db.remove`, with **no AOF or replication record and no COW pre-image**. `snapshot_cow.rs:828-829` states explicitly that it is not a capture caller.
  - Graph rollback does log compensating WAL records (`abort.rs:140-160`). Vector rollback tombstones in memory. MQ intents are deferred until commit, so an abort just drops them.
- `persistence/vec_undo.rs` is an on-disk undo *page* format for vector metadata under MVCC. Nothing outside the file uses it (grep), and it is **not part of TXN.ABORT**.

### Pre-existing correctness gap (independent of the fold) **[probe]**
- Because the forward writes reach the AOF and the undo does not, an **aborted TXN's KV writes come back after a restart** that replays the multi-part AOF. A replica keeps them too.
- The existing tests check abort only live, or after a reconnect (`tests/txn_kv_wiring.rs:402-560,739`). None restarts after an abort.
- Probe: `--appendonly yes`; `TXN BEGIN; SET k new; TXN ABORT`; wait more than 1 s for everysec; SIGTERM; restart; `GET k` should return the old value.
- Redis parity: MULTI queues commands and propagates them only at EXEC, wrapped in MULTI/EXEC. DISCARD propagates nothing. On load, an AOF that ends inside a MULTI is reverted to the point before the MULTI ("Revert incomplete MULTI/EXEC transaction").

### Options and what each unlocks for the incremental fold
The fold's exactly-once contract (WS12 NOTES, "moon#1185 remainder"):
- the base must equal the keyspace at the fold instant F;
- records stamped below F are dropped;
- records at or above F are replayed on top of the base.

Today an abort after F changes the keyspace with no record at or above F and no pre-image, so the contract breaks.

| Option | What changes | What it unlocks | Format / fuzz |
|---|---|---|---|
| **(a) KV only** | In `abort.rs:115-127`: call `snapshot_cow::capture_write_pre_image` before each undo write, and emit **compensating effect records** through the normal append path (AOF + replication, stamped with the fold epoch): `DEL k` for an `Insert`; `RESTORE k <abs-ms> <dump> REPLACE ABSTTL` for an `Update`/`Delete` (RESTORE ABSTTL exists, `command/dump_restore.rs:89-122`; payload from `dump_payload::encode`) | Exactly-once holds for KV, which is all the fold needs: the AOF base is KV-only RDB. BGSAVE images become point-in-time. **Also fixes the restart and replica gap above** | None. Existing commands; the `dump_payload` fuzz target is already in both matrices. |
| **(b) All engines** | (a), plus logging the vector tombstones and auditing graph/MQ | Nothing more for the AOF fold: vector and graph are not in the AOF base. It matters for WAL-v3 and vector-segment crash consistency, which is a separate epic | Possibly new WAL payloads, which need fuzz targets |
| **(c) Abort only before the first shared write** | Either (c1) refuse ABORT once anything has reached the log, a replica or an epoch (this makes TXN.ABORT close to useless), or (c2) the redis model: **hold the TXN's records until COMMIT**, so ABORT never has a shared write to undo | c2 gives redis-like crash atomicity for the AOF. It does **not** unlock the fold by itself: moon's TXN spans event-loop iterations with in-place writes, so a fold or BGSAVE instant can still catch an uncommitted value in the base with no log record ever to correct it. It needs (a)'s pre-image hook as well, plus either a snapshot that serializes the undo before-image for keys with open intents, or refusing to cut F while a TXN is open on the shard | Large change: MULTI/EXEC-style markers in the AOF, and a crash rule for an unterminated block |

### Recommendation
- **Take (a).** It is the smallest change that makes the fold's exactly-once argument hold. It uses existing record types, so there is no format bump. It also closes the aborted-TXN-resurrects-on-restart gap once the probe confirms it.
- Tests:
  - a unit test: arm an epoch, run TXN writes before F and ABORT after F, finish the fold, load base + incr, and compare with the live keyspace;
  - a real-server abort → restart test on both runtimes;
  - a replica-apply parity test.
- Record (c2) as the follow-up for crash atomicity: today, a crash in the middle of a TXN restores the partial TXN from the AOF.
- Record (b) as a WAL-v3/vector item that does not block the fold.
- Relation to #1269 (piecewise COW):
  - (a) adds one pre-image capture per undo write. For a large collection that is the same O(n) deep-clone stall #1269 describes.
  - The pre-image to capture is the value the undo *replaces*, which is the uncommitted value that was live at F. It leaves the keyspace anyway, so it can be *moved* into the pre-image map rather than cloned: the move-not-copy fix #1269 proposes for DEL/UNLINK. If the forward write came after F, dispatch has already captured the key (first capture wins) and the undo capture does nothing.
  - Doing (a) together with #1269's move-for-removals path avoids adding another O(n) stall.

---

## Addendum (orchestrator, after the briefs): the TXN.ABORT restart probe — CONFIRMED

The #1185 brief marked "an aborted TXN comes back after a restart" as **[probe]**. Run on the 273e6bc
binaries (Linux container, release-fast, `--shards 1 --appendonly yes --appendfsync always`):

```
SET k original
TXN BEGIN ; SET k aborted ; GET k -> aborted ; TXN ABORT ; GET k -> original
kill -9, restart with --appendonly yes
GET k -> aborted            (monoio AND tokio)
```

The TXN's writes reach the AOF as they run and `TXN.ABORT` restores the old value in memory only
(no compensating record, no COW pre-image), so AOF replay — and a replica — keep the aborted value.
This is a data-integrity bug independent of the #1185 scope decision; option (a) of the brief (log a
compensating `DEL` / `RESTORE … REPLACE ABSTTL` per KV undo step) fixes it for KV. Filed separately.

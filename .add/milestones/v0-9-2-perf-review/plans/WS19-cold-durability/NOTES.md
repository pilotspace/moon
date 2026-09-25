# WS19-cold-durability — working notes

Branch `perf/ws19-cold-durability`, base `016ca5a` (int/part3b `4115798` + the
part-4 plan commit). Personas: storage-durability-engineer (lead),
ci-test-integrity-engineer. Ports 7500–7519.

Red references:
- `/home/user/wt/bin/baseline-ae21476` (main, monoio release-fast).
- `/home/user/wt/bin/ws19-red-5b14b82-monoio`: a copy of the part-3b reviewers'
  `REVIEW3B-CONN-head`, built from `5b14b82` = `016ca5a` + a CHANGELOG-only commit
  (`git diff --stat 5b14b82 016ca5a` touches only `CHANGELOG.md`). Its startup line
  reads `(1 shards, monoio)`, and it contains a part-3a-only log string ("cold
  reclaim: adopted compacted spill files after a committed AOF fold") that
  `baseline-ae21476` lacks. It is the code of `016ca5a`.

## ORIENT — which paths a fold, a sweep and a restart take (read in code)

- Every shipped fold takes its snapshot in the `AofFold` arm
  (`shard/spsc_handler.rs`), on the shard thread: `advance_epoch()` on
  `pool.overflow_for(shard_id)` and the cold watermark `spill_file_id.get()` at
  the same instant. TopLevel + `--shards >= 2` refuses to start (`main.rs`), so
  the TopLevel overflow is always shard 0's own.
- A committed fold raises `RewriteOverflow::committed_floor()` through
  `FoldOutcome::adopt` (five writer call sites in `writer_task.rs`), never on an
  abort. A mutation stamped `e` (`stamp()`) is in the committed base iff
  `e < committed_floor()`.
- The orphan sweep (`shard/timers.rs::run_cold_orphan_sweep`, every
  `--cold-orphan-sweep-interval-secs`, default 60) is the ONLY place that unlinks
  a zero-ref spill file on its own schedule: `sweep_known_orphans` and
  `sweep_expired` both end in `ColdIndex::drain_pending_unlink`. The reclaim's
  adoption unlinks the files it emptied through `ColdIndex::unlink_now`.
- Recovery (every layout): cold index rebuilt from every manifest-Active KvLeaf
  file -> base -> incr (`MOON.COLDCUT w` makes files `< w` readable from the first
  record; files `>= w` only after their `MOON.SPILLED` marker replays).

## moon#1231 — promote-then-sweep

### Mechanism (verified in code, not assumed)
1. A key `k` cold at the fold instant `T` (index entry in file `F`, `F < w`) is
   not in the base: `stream_fold_image` iterates `db.data()` plus in-flight
   payloads only.
2. The new generation reads `k` from `F` whenever a replayed record reads `k`
   before overwriting it, and `F` is `k`'s only copy for the whole generation.
3. `promote_cold_outcome` (`kv_ops.rs`) does `self.set(key, entry)` +
   `ci.remove(key)`. Nothing is logged for a GET promotion; an RMW promote logs
   only the RMW (APPEND/INCR/...). `ColdIndex::remove` -> `ref_dec(F)` ->
   `pending_unlink.push(F)` on the last referrer.
4. The next sweep's `drain_pending_unlink` unlinks `F` and tombstones it.
5. Restart: base (no `k`) + incr (no record, or an APPEND that replays onto
   nothing). `k` is gone or wrong.

### It is wider than promotion (found while reading)
The same generation needs `F` for ANY record that reads `k` from the cold plane
before `k`'s first blind write in the incr — not only after a promotion:
- a non-promoting cold read logged as a write: `SUNIONSTORE dst k`,
  `ZUNIONSTORE`, `SORT ... STORE`, `COPY k dst` (`get_*_ref_if_alive` ->
  `cold_read_only`, WS15 NOTES item 4);
- an RMW that promoted `k`, followed later by `DEL k`: the RMW replays onto
  nothing (and `LMOVE k dst` puts nothing in `dst`), the DEL makes `k`'s own final
  state right, but `dst` is wrong.

So "every key it held is dead" (the issue's first condition) is not sufficient
once a record read the key after `T`. Tracking "was `k` read by a logged command
since `T`" per key is not available and would cost the hot path.

Also found: recovery's `release_older_copies` (moon#1140) queues older-copy files
at replay close. A second crash before any fold replays the same generation,
whose gate may need that older copy again (newest file listed, its marker lost);
unlinking it after the first recovery loses the key on the second. Same shape.

### Fix design: hold zero-ref files below the latest cut until a fold covers them
The unlink decision moves to the moment of unlink (the drain), where a fresh
reading of the fold state is available, and applies to every zero-ref file
whatever made it zero-ref:

- `FoldView { epoch = stamp(), committed_floor, next_file_id = spill counter }`,
  read by the sweep immediately before each drain, on the shard thread.
- `hold_below`: raised to `next_file_id` whenever the view's epoch differs from
  the one seen last (a fold cut happened since: the cut came from the same
  counter, so the counter now is >= it), and set on the first view (every file
  that exists at boot is below the recovered generation's cut or older).
- A queued zero-ref file `F >= hold_below` was minted after every cut any
  committed or in-progress generation has: unlinked as today.
- `F < hold_below` is HELD, stamped with the view's epoch `e` (>= the epoch of
  its zero-ref, so the stamp can only err late). Released once
  `e < committed_floor`: a fold whose snapshot follows the zero-ref has
  committed, so its base has every key `F` backed (hot, DEL'd by the ledger head,
  or cold in another listed file).
- No AOF writer in the process (`aof_pool == None`): no view, today's behaviour.
  There is no fold and no committed generation to protect, and a hold could
  never be released.
- A drain with no fresh view on an index that has had one unlinks nothing new
  (a stale view could under-state `hold_below`, which is the unsafe direction).

Why not a promotion hook (the issue's second piece): it would cover promotion
only; the hold covers every cause above with no hot-path cost (the decision runs
on the sweep, once per interval, per queued file).

### The reclaim adoption has the same hole (found while reading `adopt_compactions`)
A compaction whose survivors ALL changed before adoption is discarded (its copy
never listed), and the old file is then unlinked by `unlink_now`. If a survivor
changed by PROMOTION after the fold that allowed the adoption, the committed
generation still needs the old file for it (cold at that fold). Fix: the
adoption unlinks an old file only when every output of its compaction was listed
(each survivor then has a durable copy below the committed cut); otherwise the
old file goes through the hold.

### Crash windows
| window | durable state | recovery | ok? |
|---|---|---|---|
| promote `k` after fold `T`, crash before any sweep | `F` listed | `k` from `F` | yes (as before) |
| promote, sweep HOLDS `F`, crash | `F` listed, on disk | `k` from `F` (< cut) | yes (the fix) |
| promote, next fold `T2` committed, sweep releases `F`, crash | `F` unlinked | `k` in `T2`'s base | yes |
| promote, fold `T2` in progress (not committed), sweep | `e >= floor` -> held | old gen reads `F` | yes |
| fold `T2` aborts | floor unchanged, still held | old gen | yes |
| file spilled after the latest cut, zero-ref | unlinked | its keys' records are in the incr | yes (as before) |
| restart, no fold yet in this process, promote | first view: `hold_below` = counter >= every boot file | held | yes |
| replay-close older-copy release, second crash | held | gate finds the older copy | yes |

### Costs and limits
- Disk: a zero-ref file below the latest cut stays on disk (listed) until the
  next committed fold plus one sweep. It is the only durable copy of what it
  backs, so this is the durability cost, not a leak. With
  `auto-aof-rewrite-percentage 0` only a manual BGREWRITEAOF releases it.
- RAM: the held file's dead-slot ledger entries stay until it is unlinked (they
  must: the file is listed and on disk). While the shard's ledger is over the
  reclaim threshold and some held file is not yet covered by a committed fold,
  the shard asks the auto-rewrite monitor for a fold (the reclaim's own signal),
  so admission cannot stay wedged on held-file ledger bytes.
- INFO `cold_files_pending_unlink` counts held files too.

## moon#1232 — sharded `--save` never fires

### Mechanism (verified in code)
- `main.rs` creates `change_counter: Arc<AtomicU64>` and hands it only to
  `persistence::auto_save::run_auto_save_sharded` (tokio listener runtime and
  the monoio `auto-save` thread). `grep change_counter`: the only incrementers
  are `server/conn/handler_single.rs` / `single_aof_log.rs` and
  `server/listener.rs` — the legacy single-listener path, not the sharded
  server. The sharded trigger (`changes >= threshold`) therefore never holds
  for a threshold >= 1.
- A per-shard dirty count ALREADY exists: `admin::metrics_setup`'s
  `KEYSPACE_CHANGE_COUNTERS` — padded per-thread slots, one relaxed add on the
  thread's own line at the storage funnels (`Database::set`, `remove`,
  `get_mut`, `clear`, `set_expiry`, the incr/string_mut writers), summed on
  read. `rdb_changes_since_last_save()` = sum − the sum stored by
  `mark_save_completed()` at the last successful save (`bgsave_shard_done`,
  on success only, since moon#1230). That is exactly the design the issue asks
  for (moon#1176: no shared per-write `fetch_add`), and INFO already reads it.

### Fix
- `run_auto_save_sharded` reads `rdb_changes_since_last_save()` — the number
  INFO shows — instead of the dead counter, and no longer zeroes anything: the
  reset point is the successful save (`mark_save_completed`), as in redis
  (`dirty` moves on success only). The decision is a pure function
  (`save_rule_due`) shared by both runtime arms.
- After a FAILED save redis retries only every `CONFIG_BGSAVE_RETRY_DELAY`
  (5 s) (`lastbgsave_status != C_OK && now - lastbgsave_try <= 5` blocks the
  rule). Before this fix a failed auto-save could not re-trigger (the counter
  was zeroed at the trigger and never grew); with the real count it would
  re-trigger every second, so the retry delay is mirrored.
- `main.rs`: the dead counter is removed from the auto-save wiring.
- No new increment site: the count already covers every sharded write path
  (it is the same number INFO has always reported).

### Found while reading: writes made DURING a background save were forgotten
`mark_save_completed()` stores the sum at COMPLETION, so every write between
the save's snapshot and its completion is counted as saved although the
snapshot does not hold it (redis: `dirty -= dirty_before_bgsave`). With a
working trigger this is a lost-write window for `--save` users (no AOF): such
writes are in no snapshot and would not re-arm the rule. Fix (isolated
cross-ownership commit): `mark_save_started()` records the sum when a
background save starts (`bgsave_start`, `bgsave_start_sharded`), and
completion stores that start value. A start site that does not call it can
only make the count HIGHER (a spare save), never lower.

### Behaviour change (for the CHANGELOG)
Every deployment that passes `--save "<secs> <changes>"` to the sharded server
starts writing periodic snapshots it silently never wrote.

### Residuals (named)
- Embedded mode (`server/embedded.rs`) still does not spawn auto-save; its
  comment explains a limitation this fix removes. Not owned; follow-up.
- The rule's time base is the last auto-save start (or task start), not
  redis's `lastsave` (last successful save of any kind, or startup): a manual
  BGSAVE does not restart the rule's clock. Pre-existing; unchanged.

## moon#1240 — cold reclaim I/O off the shard thread

### What runs on the shard thread today (read in code)
`cold_reclaim_tick::run`, from the 100 ms eviction tick:
- `compact_file`: `std::fs::read` of the whole old file, decode, then
  `spill_thread::flush_buffer` = temp write + file fsync + rename + dir fsync.
  Capped at 2 files / 2 MiB per tick.
- `adopt_compactions`: `manifest.commit()` to list the outputs, then
  `unlink_now` -> `remove_file` + `manifest.commit()` for the tombstones.
  With deferred sync enabled (`event_loop.rs` always enables it when a manifest
  exists) `commit()` is `commit_durable`: the fsync runs on the manifest-sync
  thread but the shard thread BLOCKS on its ack. Capped at one db per tick.

### Design
Compaction becomes a two-round-trip pipeline on the shard's existing spill
thread (a second channel pair; the thread polls it every loop iteration, so a
job waits at most its 100 ms `recv_timeout`):
1. shard: pick candidates (in memory), mark in flight, send `Read`.
2. spill thread: read + decode every slot of the file (no index access).
3. shard: keep the slots whose key's index entry is exactly that slot (the
   same filter as before), mint the output ids from the shard counter, stamp
   the fold epoch NOW (`stamp()`: the ids and the stamp are taken in one
   synchronous section, so a fold whose snapshot is later than the stamp cut
   after the ids — the WS15 invariant `F' < w`), send `Write`.
4. spill thread: `flush_buffer` (same durable write), on failure remove what
   it wrote.
5. shard: pair outputs with survivors (order checked), record the pending
   compaction exactly as before.
Adoption becomes two phases across ticks:
- A: list the outputs (as before, incl. the moon#1231 rule) and hand the
  snapshot to the manifest-sync thread with an ACK the shard polls
  (`ShardManifest::commit_acked`, new; the fsync never blocks the shard).
- B (a later tick, once the ack is in): Ok -> re-validate every survivor NOW
  (unchanged -> re-point; changed -> dead slot in the durable output), unlink
  the old files, tombstones by `commit_deferred`. Err -> unlist + remove the
  outputs; nothing was re-pointed or unlinked.

### Crash-window argument, re-checked
- The outputs are adopted only after (a) the write completed (durable file)
  and (b) `committed_floor > stamp` — the fold that commits is cut after the
  ids were minted, so every output is below its cut. Same as WS15, whatever the
  order of (a) and (b).
- A survivor unchanged from step 3 to phase B was cold in the old file at that
  fold; its output has the same value and is listed durably (ack) before the
  old file can go.
- The old file is unlinked only after the listing is DURABLE (phase B waits
  for the ack) — a deferred listing lost in a crash with the old file already
  gone would lose every survivor. Before the ack nothing is re-pointed, so the
  orphan sweep cannot see the old file as zero-ref because of the adoption.
- A lost tombstone commit (deferred) leaves the old file listed but missing:
  `files_missing`, counted and retired — the orphan sweep's existing window.
- Ack Err: the listing may still become durable via a later snapshot while the
  outputs are being removed -> listed-but-missing outputs, benign: the
  survivors were never re-pointed and the old file was never unlinked.
- A job whose database's cold index was replaced (FLUSH keeps it; DEBUG RELOAD
  / full sync replace it) finds no in-flight record: a read result is dropped,
  a written result's outputs are removed (unlisted).
- Shutdown with jobs in flight: their outputs are unlisted files, removed by
  the startup orphan sweep (as for a crash before adoption).

### Blocking I/O left on the shard thread by the reclaim (named)
`remove_file` of an unadopted output or of an adopted old file (metadata ops,
no fsync, same as the orphan sweep's), and the in-memory manifest edits.

### Implementation notes (as built)
- Compaction start is paced at 2 files per tick and 8 in flight per shard;
  the old 2 MiB read budget is gone with the reads (they run on the spill
  thread and compete with spills there, not with connections).
- The spill-completion path (`apply_completion_vec`,
  `rehydrate_unpublished_spill`) is untouched; reclaim answers arrive on their
  own channel and are applied at the top of `cold_reclaim_tick::run`.
- Red evidence is by revert-only-the-fix (the test needs the new tick
  signature): variant A (inline compaction) and variant B (inline blocking
  adoption: the tick blocked 301.9 ms behind a 300 ms injected fsync).

## moon#1241 — shrink-only commands in scripts answered -OOM

### Mechanism (verified in code)
`make_redis_call_fn` (bridge.rs) runs `eviction_ctx.gate(db, db_idx)` for
every WRITE-flagged `redis.call`; `gate` had no command, so no
`is_shrink_only_command` exemption, unlike `run_write_eviction_gate`
(connection) and `spsc_eviction_gate` (routed leg, moon#1215 row F).

### What redis 7.0.15 actually does (measured, port 7501, noeviction, over maxmemory)
- EVAL (no shebang = compat mode): `redis.call('DEL')` -> 1, UNLINK -> 1,
  LPOP -> nil, EXPIRE -> 1, HDEL -> 0; `redis.call('SET')` -> `-OOM ...
  script: <sha>, on @user_script:1.`; `redis.pcall('SET')` -> `-OOM ...`.
- redis also lets a compat script that has ALREADY written keep writing
  (`SCRIPT_WRITE_DIRTY`): "DEL then SET" -> OK.
- FCALL of a function WITHOUT flags that only DELs -> `-OOM` (the whole call
  is refused up front); WITH `flags={'allow-oom'}` -> 1.
- A shebang EVAL without flags -> `-OOM` up front; with `flags=allow-oom` -> 1.
- moon HEAD (5b14b82 binary, port 7502): every one of these -> `-OOM command
  not allowed when used memory > 'maxmemory'` (no trailing '.'; moon's OOM
  text differs from redis everywhere, pre-existing). moon rejects a shebang
  EVAL body outright ("syntax error ... near '#'"), pre-existing.

### Fix
- `gate_command(cmd, ..)` for the script's own write: eviction runs; for a
  shrink-only command the maxmemory reject and the per-db quota reject are
  bypassed; per-db quota via `check_db_maxmemory_for_command` (same as the
  connection gate). `gate()` (COPY ... DB destination, WS16's region) is
  unchanged.
- Parity boundary: the bypass is on for EVAL (moon has only compat mode) and
  for a FUNCTION registered with `allow-oom`; off for a flagless FUNCTION
  (redis refuses it whole). Carried by a per-script thread-local reset to
  `true` by `set_script_db`/`clear_script_db`, set by the FCALL runner from
  `FunctionDef::flags` (cross-ownership one-liner in functions.rs; the flag
  was parsed before but never read).

### Residuals (named, pre-existing, not changed)
- `SCRIPT_WRITE_DIRTY`: moon still refuses a growing command after a script
  has written (redis allows it).
- `allow-oom` functions: moon still refuses GROWING commands in them (redis
  skips the OOM check entirely).
- Shebang EVAL flags are not supported by moon at all.

## Measurement — moon#1240 PING latency A/B (NATIVE, this 4-vCPU container)
Method: `scratchpad/ab_reclaim_latency.py`, the `perf_ws15_ledger_bound`
workload (`--shards 1 --maxmemory 16mb` allkeys-lru, disk offload, AOF on;
4 rounds of 20,000 SETs with 990-byte keys, 3 s, DEL all but 1 in 50, 3 s;
then 12 s settle) with a second connection sending PING every 1 ms for the
whole run. Interleaved A B | B A | A B, fresh server and data dir per run,
port 7510. A = `ws19-red-5b14b82-monoio` (= 016ca5a / 4115798 code),
B = `ws19-monoio-db3490a`. Both release-fast monoio. Relative evidence only
(same-host client, other agents building on the box). The Linux perf-host
number is DEFERRED (no perf host here).

| run | pings | p50 ms | p99 ms | p99.9 ms | max ms | >=5 ms | >=10 ms | ledger at end |
|---|---|---|---|---|---|---|---|---|
| A1 | 28,184 | 0.127 | 1.759 | 15.991 | 66.3 | 82 | 37 | 127 |
| B1 | 28,361 | 0.135 | 1.085 | 4.381 | 41.1 | 24 | 10 | 652 |
| B2 | 28,536 | 0.128 | 1.181 | 4.969 | 1,498.9 | 28 | 13 | 646 |
| A2 | 27,287 | 0.139 | 1.392 | 5.179 | 1,261.9 | 30 | 14 | 189 |
| A3 | 28,210 | 0.133 | 1.408 | 14.635 | 36.3 | 58 | 34 | 557 |
| B3 | 28,856 | 0.117 | 0.940 | 4.991 | 40.2 | 28 | 12 | 198 |

- p99: A 1.39–1.76 ms (median 1.41), B 0.94–1.18 ms (median 1.09): -23%.
- p99.9: A 5.2–16.0 ms (median 14.6), B 4.4–5.0 ms (median 5.0).
- PINGs >= 10 ms: A 14–37 (median 34), B 10–13 (median 12).
- max: one 1.2–1.5 s outlier in each binary's second rep (A2, B2): not the
  reclaim I/O (B has none on the shard thread); consistent with host noise or
  the fold's base serialization, which runs on the shard thread in both. The
  remaining stalls in B are of that kind, not reclaim's.
- Both binaries refused 40,000 of 80,000 SETs (rounds over budget until a
  reclaim fold; the same in every run) and brought the ledger back to 127–652
  slots, so B's reclaim still completes.

## Gates at the final code commit `db3490a`
- `cargo fmt --check`, `scripts/audit-unsafe.sh` (0 missing SAFETY),
  `scripts/audit-unwrap.sh` (within baseline): clean.
- `cargo clippy --all-targets -- -D warnings` (monoio) and
  `cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings`: clean.
- `cargo check --all-targets --no-default-features --features runtime-tokio,jemalloc`:
  clean (a first attempt linked another worktree's lib rlib — its error cited
  a line my `auto_save.rs` no longer has; the re-run was clean).
- `cargo test --lib -- storage:: persistence:: shard:: command:: scripting:: admin::`:
  monoio 3779 passed / 0 failed; tokio 3590 passed / 0 failed (own binary
  checked by the presence of this branch's test names).
- Integration by name, MOON_BIN pinned to the release binaries, both
  runtimes, all green: `perf_ws19_save_rules` 2/2, `perf_ws19_script_oom` 2/2,
  `perf_ws15_ledger_bound` 2/2, `perf_ws15_spanning_cold_del` 2/2,
  `crash_recovery_cold_del_rewrite --ignored` 11/11 at shards 1 and 4,
  `perf_ws15_bgsave_status` 3/3 and `perf_ws12_bgsave_split` 4/4 (these two
  re-run under temporary unique test names: the first tokio run of
  `perf_ws12_bgsave_split` executed WS16's test binary — its backtrace path
  was `/home/user/wt/WS16-snapshot-capture/tests/...` — and
  `perf_ws15_bgsave_status` differs between the two trees under the same
  test names).

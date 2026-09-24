# WS15-durability-followups — working notes

Branch `perf/ws15-durability-followups`, base `ae21476` (+ plan `6537679`).
Personas: storage-durability-engineer (lead), ci-test-integrity-engineer.

## ORIENT — which paths a rewrite and a restart take (read from `src/main.rs`,
`persistence/aof/rewrite.rs`, `shard/spsc_handler.rs`)

| runtime | shards | AOF layout | fold path (writer side) | head written by | recovery |
|---|---|---|---|---|---|
| monoio | 1 | TopLevel manifest | `do_rewrite_sharded` | `open_new_incr` inside `advance_with_base` (before the manifest flip) | multi-part replay |
| tokio | 1 | legacy single file `appendonly.aof` (no manifest, #96) | `rewrite_aof_sharded_sync` | inline after the RDB preamble in the tmp file (before the rename) | v2 / v3-fallback replay of `appendonly.aof` |
| both | >=2 | PerShard manifest | `do_rewrite_per_shard` | `open_new_incr` before the coordinator commit | per-shard replay |
| monoio | 1 | TopLevel | legacy `do_rewrite_single` (`handler_single`, not the shipped server) | `advance_with` | — |

All three shipped folds take their base from ONE place: the `AofFold` arm
(`shard/spsc_handler.rs`, WS8's file) calls `fold_stream::stream_fold_image`
(mine) with shared guards on every db, at the same instant it cuts
`pending_aof_count`, the #455 fold epoch and the cold watermark
(`spill_file_id`, so every file that exists or is in flight is `< w`). The
writer drains the image to the end (`write_fold_image`) BEFORE it writes the
new incr's head, on all three paths. So anything the shard computes inside
`stream_fold_image` can reach the head without touching WS8's arm: ship it
through the image channel.

Recovery order (every path): cold index rebuilt from every manifest-Active
KvLeaf file (newest `(file_id,page,slot)` wins, older copies kept for the
gated replay, moon#1140) -> base -> incr (the `MOON.COLDCUT` head installs the
gate; tombstoning paths — DEL/UNLINK/FLUSH — bypass the gate, moon#257) ->
`close_replay_generation` (hot wins).

## The durable state after a fold, stated as an invariant

At fold instant T with cut w, recovery of the new generation (listed files +
base + head, nothing else) must reproduce, for every key k, its state at T:

1. k hot and not expired at T: in the base; hot wins over any listed slot. OK.
2. k IN FLIGHT at T (hot copy removed by `evict_one_async_spill`, payload in
   `spill_inflight`, file F < w not published yet): **not in the base** — the
   base streams `db.data()` only. Its only source is F, IF F is published
   after T. moon#1223.
3. k cold at T (indexed at F_idx < w, not hot): the rebuild picks the newest
   listed slot of k, which is F_idx (file ids are minted monotonically at
   eviction; a key reaches a newer file only by being spilled again, which
   re-points the index; a superseded or withdrawn spill never becomes the
   index entry). Listed: the `flush_all_agents` barrier makes every deferred
   manifest commit durable before the old generation is pruned. OK.
4. k absent at T but with a listed slot somewhere (deleted, flushed, expired
   while hot, promoted-then-deleted, overwritten-then-deleted, a ghost slot of
   a superseded spill): nothing in the new generation says so. The rebuild
   re-indexes the slot and COLDCUT authorizes it. **Resurrection — moon#1215.**
   The only record of the delete was the DEL in the generation the fold
   discarded; `kv_page`'s `TOMBSTONE` flag is never written or read.

Verified in code (not assumed): `ColdIndex::remove` (`cold_index.rs`) only
edits RAM and queues the file only on its LAST referrer; `stream_fold_image`
iterates `db.data()` only; `rebuild_from_manifest_per_db` indexes every slot of
every Active file; `write_cold_cut_head` writes one record.

Red on `ae21476` (this box, before any fix):
- `cold_del_rewrite_tests::a_cold_key_deleted_before_a_rewrite_stays_deleted`
  (`--include-ignored`): `left: Some("v1") right: None` — k1 comes back.
- handoff `rv_integ_withdrawn_spill_after_fold_has_a_durable_source`:
  `live=Some("acked-value") in_base=false manifest_lists_file=false
  in_post_fold_log=false` (the review's run).
- real-server suite: see SUMMARY.

## moon#1223 — the fold's base omits in-flight keys

### Options
- **(A) preferred-small in the issue**: when the `MOON.SPILLED` marker is
  refused and F < the latest fold cut, publish F without the marker instead of
  withdrawing it. Value-correct by #1202's (a)–(c).
- **(B) heavier**: the base image includes every in-flight payload whose key
  is not hot, as the key's value at T.

### Choice: (B), with the argument
1. (B) fixes the invariant where it is broken — case 2 above: the base must
   be the keyspace at T, and an in-flight key IS in the keyspace (`EXISTS`,
   `DBSIZE`, `GET` all answer from the in-flight plane, #459). (A) patches one
   of the THREE paths that put an in-flight key back in RAM without a log
   record after its spill does not publish: marker refused (#1202 withdraw),
   pwrite failed (`!c.success` re-insert), and file id already listed (#893
   re-insert). After a committed fold, (A) leaves the other two exactly as
   lossy as #1223 (key live in RAM, in no durable artifact).
2. (A) needs the latest fold cut on the completion path. The cut is taken in
   the `AofFold` arm (`shard/spsc_handler.rs`, WS8) and never stored; plumbing
   it is a cross-ownership edit, and "latest fold" is ambiguous for a fold
   that later aborts (the old generation then still needs the marker-less
   file only if F >= its own cut — case analysis per outcome, on the shard
   thread, which never learns commit vs abort).
3. (B) leaves #1202 untouched: a refused marker still withdraws; withdraw is
   value-correct again because the key is in the base (fold after eviction)
   or in the old generation's records (eviction after the fold).
4. Cost: O(in-flight) payload rehydrates + RDB encodes inside the fold arm.
   The in-flight plane is bounded by the spill request queue (4096) + the
   thread's buffer (256) + completions not yet drained (one 100 ms tick), so
   this is noise next to the O(hot dataset) the arm already serializes.
   No format change: the base is an ordinary RDB image with a few more keys.

### What recovery does with an in-flight key in the base (every outcome)
- Completion publishes WITH its marker (post-T, so in the new incr): the
  marker drops the base's hot copy, the key is cold in F — restart-as-cold
  kept. Manifest commit lost in a crash: the marker finds no cold entry in F
  and keeps the hot copy. OK.
- Marker refused -> withdrawn: key back in RAM, base has it. OK.
- pwrite failed / id rejected -> re-inserted: base has it. OK.
- Deleted/overwritten after T: the DEL/SET is in the new incr and replays on
  the base copy; F (if published for other keys) holds a ghost slot -> the
  #1215 ledger (below) records it. OK.
- Fold aborted: the old generation holds the key's records. OK.
- Payload expired at T: skipped, like an expired hot key (the base's filter).
- Payload that does not rehydrate (corrupt in RAM): cannot be encoded; logged
  as an error and counted; it is NOT treated as dead (no DEL is emitted for it —
  the published file is then its only hope).

## moon#1215 — a durable record of cold deletes that survives the fold

### Options (from the issue) and what each costs
- **(a) new `MOON.COLDDEL <file_id> key…` record.** New AOF record semantics:
  an older binary replays it through dispatch as an unknown command. Still
  needs a per-key memory of dead slots to re-emit at every rewrite.
- **(b) write the page `TOMBSTONE` flag in place** (CRC updated, fsynced
  before the rewrite commits). An in-place rewrite of a sealed page that also
  holds LIVE neighbours: a torn write fails the page CRC and turns one deleted
  key into up to a page of lost live keys — a strictly worse failure than the
  bug. Also a pwrite+fsync per delete (or a batch the fold must wait for), and
  older binaries ignore the flag.
- **(c) compact files with dead slots before the cut.** New file ids below
  the cut, index re-pointing, manifest swap, all on the fold's critical path.
  Much larger, and still needs to know which slots are dead.
- **(d) chosen: a dead-slot ledger in `ColdIndex` + plain `DEL` records in
  the new generation's head.** Every slot that is on disk in a listed file but
  is not its key's current index entry is remembered (key + file). At the fold
  instant, every remembered key that is not alive at T gets a `DEL` in the new
  incr, right after `MOON.COLDCUT`, in its `SELECT`ed db.

### Why (d)
- **Format-neutral.** The new generation contains only `SELECT` and `DEL`,
  which every moon binary replays, and the replayed DEL already tombstones the
  cold plane through the gate (moon#257). No record type, page flag or
  manifest field changes, so no version gate is needed: a downgrade after the
  fix KEEPS the deletes. (What an upgrade cannot fix: a generation an OLDER
  binary already rewrote has lost its deletes; that is gone.)
- **Exactly the slots that matter.** "Not alive at T" uses the base's own
  predicate at the same `now_ms` (see below), so a DEL can never remove a key
  the base or a cold entry still carries.
- No disk I/O on the delete path; the fold writes a few records.

### The ledger (in `ColdIndex`, so every mutation site is covered centrally)
`dead: HashMap<file_id, Vec<Bytes>>` + byte accounting. A (file, key) enters
when the slot stops being the key's index entry, by ANY path:
`remove` (DEL/UNLINK, second write over a shadow, read promotion, expired-read
reclaim, replay reconcile), `insert` over a different file (re-spill),
`clear_all` (FLUSH*), `sweep_known_orphans` (hot-shadow reclaim),
`sweep_expired`, `release_older_copies[_of]` (a rebuild's older copies of a
key, once dropped — otherwise the older slot survives the newer one's file
and resurrects an OLDER value), and — outside `ColdIndex` — a spill
completion that publishes a file containing a key it did NOT index
(superseded/withdrawn entries: a ghost slot), via a new `note_dead_slot`.
A file's entries leave the ledger only when the file is unlinked (or found
already gone) by `drain_pending_unlink`; a failed unlink keeps them.
Completeness at boot: the rebuild indexes every listed slot (or keeps it as an
older copy), so every slot re-enters the ledger through the same transitions
when the replay (head DELs included) removes it.

### "Alive at T" (computed inside `stream_fold_image`, same `now_ms` as the base)
For each candidate key of db d (ledger keys, plus every hot-but-EXPIRED key
that still has a cold entry — a stale shadow the base filter also drops):
- hot and not expired -> alive (in the base) — skip;
- hot and expired -> dead — its shadow would come back — DEL;
- not hot, in flight, payload included in the base -> alive — skip;
- not hot, in flight, payload expired -> dead — DEL (an older ledger slot of it
  must not win if the in-flight file is withdrawn);
- not hot, in flight, payload does not rehydrate -> treated alive (logged);
- not hot, indexed -> alive-cold (its newest slot is the index entry; its own
  TTL governs) — skip;
- otherwise dead -> DEL.

### Crash windows (every step, both issues together)
| window | durable artifacts | recovery | ok? |
|---|---|---|---|
| DEL k logged, crash before any fold | old gen incr has DEL (fsync policy) | replayed DEL tombstones | as before |
| fold at T: head (COLDCUT + DELs) written + fsynced, crash BEFORE commit | old gen still committed (incr has DEL k) | old gen | yes |
| crash AFTER commit | new gen head has DEL k | head DEL | yes (the fix) |
| crash mid-head write | new incr not committed | old gen | yes |
| DEL k after T, before commit | record epoch >= fold epoch -> new incr (or old incr on abort) | replayed | yes |
| sweep unlinks F after T | head DEL of k is a no-op on replay | — | yes |
| sweep: unlink ok, crash before manifest tombstone commit | F listed but missing | `files_missing`, no entry | yes (ledger for F dropped only after a successful unlink/NotFound) |
| unlink fails | file stays, re-queued | ledger for F kept | yes |
| FLUSHDB of cold keys, fold before the sweep | every flushed key in the ledger | head DELs | yes (the FLUSHDB-before-sweep case) |
| restart after a fixed fold | head DELs replay -> `ColdIndex::remove` -> ledger repopulated | next fold re-emits | yes |
| k dead at T, re-created + spilled to F2 >= w with marker | head DEL removes k's entry wherever it points; post-T records rebuild k, marker finds no F2 entry and keeps it hot | value correct (restart-as-cold lost for k once) | yes |
| k in flight at T, completion after commit | base has k; marker (new incr) re-cuts it cold | yes | yes |
| k in flight at T, marker refused after commit (#1223) | base has k | hot from base | yes (the fix) |
| k in flight at T, pwrite fails after commit | base has k | yes | yes |
| fold aborts | old gen authoritative, overflow drained into it | as before | yes |

### Interaction with #1202 (spill withdraw) — must not regress
The withdraw stays exactly as #1202 wrote it (marker first, publish after, in
one synchronous section; refused -> rehydrate, file stays out of the manifest).
#1223's fix makes the withdraw safe after a fold instead of changing it. The
ledger records ghost slots only for files that ARE published; a fully
withdrawn file is not listed and cannot resurrect anything (the startup
orphan sweep removes it).

### Costs and limits (surfaced, not hidden) — measured values in the appendix below
- RAM: one `Bytes` (key) per dead slot in a file that still has a live
  referrer, charged to `ColdIndex::resident_bytes` (so `used_memory` and the
  eviction budget see it). Bounded by the slots of heap files still on disk; a
  file's entries go with the file. Worst case: many files each kept alive by
  one live key — the ledger then holds ~every dead key. Compaction of mostly
  dead files (option (c)) would bound it; recorded as a follow-up.
- Fold stall: O(ledger + expired-hot) probes in the `AofFold` arm, next to the
  O(hot dataset) it already serializes.
- Head size: one `DEL` argument per dead key (batched 512 keys per record).
  FLUSH of a huge cold plane followed by a rewrite before the sweep writes one
  argument per flushed key, once.

### Residuals (named, not fixed here)
- First-upgrade capture (`main.rs`, monoio `--shards 1`, legacy
  `appendonly.aof` -> `initialize_with_base` + `seed_cold_cut`): the new
  generation opens with COLDCUT only; deletes replayed from the retired legacy
  file are not re-emitted. One-time migration path; the fix needs `main.rs`
  to pass the ledger (not owned).
- Legacy `do_rewrite_single` (not the shipped server) gets the same treatment
  only if cheap — see the commit.
- BGSAVE `SnapshotState` also omits in-flight payloads (same class as #1223)
  — matters only where a snapshot is the KV base; WS16 / moon#1228 territory.

## moon#1230 — BGSAVE status

Verified: `bgsave_shard_done` only ever stores `false`; the last shard ALSO
advances `rdb_last_save_time` and resets `rdb_changes_since_last_save` even
when a shard failed (redis: `lastsave`/dirty only on success). Consequences:
`rdb_last_bgsave_status:err` forever after one failure, and — worse — every
later `SHUTDOWN SAVE` (and bare `SHUTDOWN` with save points) is refused with
"background save error" because it reads the same flag after a successful
save (`handler_*/dispatch.rs`).

Siblings found while reading (same accounting, same fix):
- sharded auto-save (`run_auto_save_sharded`) bumps the epoch without the
  counter: its completions arrive at counter 0 and are ignored -> `LASTSAVE`
  never advances under auto-save, a failure latches `err` forever.
- a shard that consumes the epoch but does not save (`is_dir_lost`, or no
  persistence dir — the default `--appendonly no` without `--save`) never
  reports: `rdb_bgsave_in_progress:1` forever, every later BGSAVE "already in
  progress" (reproduced on the baseline binary, port 7321).

Fix: a per-save failure latch reset when a sharded save starts; the last
shard publishes `ok` iff no shard failed, and only a successful save advances
`rdb_last_save_time` / resets the dirty counter. Auto-save starts through
`bgsave_start_sharded` (counted, using the resolved shard count). A shard that
skips a save reports it as failed (loud, instead of hanging). Follow-up (not
owned: `main.rs`/`event_loop.rs`): BGSAVE with no persistence dir should
write to `--dir` like redis does, instead of failing.

## What the build and the runs established (appended after implementation)

### Harness defects found and fixed before any verdict could be trusted
- `tests/crash_recovery_cold_del_rewrite.rs` (cherry-picked, written on
  macOS): the filler connection closed without reading its replies; on Linux
  the RST made the server discard the unread tail — 8,289 of 16,000 SETs
  landed, nothing spilled, all 8 cases failed their precondition on the BASE
  binary. Fixed in its own commit (drain the replies). Only then did the base
  binary reproduce the issue's table.
- The first `perf_ws15_spill_withdraw_after_fold` checked a preloaded key set;
  under LRU the keys in flight at the fold are the flood's own recent keys, so
  it could not go red on base. Rewritten to check every flood key acked before
  BGREWRITEAOF, retry BGREWRITEAOF while the saturated channel refuses it, and
  stop the server right after the commit.

### #1223: why the real-server test is not the red/green guard
Red on base only at `--shards 1` kill -9 (1,067 of 63,049 pre-rewrite keys
lost). At `--shards 4` routed legs are refused unapplied under the stall
(moon#769) so the channel rarely refuses a marker, and on SHUTDOWN the final
completion drain publishes with markers into an idle writer. The deterministic
guard is the in-process `fold_inflight_tests` (all three rehydrate paths).

### Rejected, recorded: moving the manifest-sync barrier before the flip
Every flip (`advance_with_base`, the per-shard coordinator, the tokio rename)
runs `flush_all_agents()` AFTER the new generation is committed (it gates only
the prune). A crash between the flip and the async persist of a deferred spill
placement leaves a key cold at the fold in no durable artifact (the manifest
lacks its file; the new base lacks the key). Moving the barrier before the
flip (abort the fold on failure) closes it, BUT `flush_all_agents` walks a
process-global agent registry: in the unit-test process another test's
injected persist failure would then abort unrelated `advance` calls — a new
cross-test flake channel of the moon#750 class. Needs the registry scoped per
AOF manifest (or per shard set) first. Window: flip -> async persist, ms.
Follow-up, not done here.

### NEW FINDING (pre-existing, not one of WS15's issues): promote-then-sweep loss ("P")
A key COLD at a fold is not in that fold's base; its spill slot is its only
durable copy until the next fold. Read-promotion (`promote_cold_outcome`)
and every read-modify-write promote remove it from the cold index WITHOUT a
log record; once every key of its file has left the index (neighbours
deleted/promoted), the orphan sweep unlinks the file and tombstones its
manifest entry. A restart then loses the key (and an APPEND/INCR logged after
the promotion replays onto nothing). Reproduced on the base binary
(`scratchpad/probe_promote_sweep.py`: 200 probes spilled, BGREWRITEAOF, DEL the
fillers, GET every probe, 1 s sweeps, kill -9): **88 of 200 acknowledged,
never-deleted keys lost**. The #1215 ledger does not change it (a promoted
key is alive at the next fold and gets no DEL; the unlink policy is
untouched). Fix direction: a file below the COMMITTED generation's cut may be
unlinked only when every key it held is dead or no longer derives its value
from it (blind-overwritten, re-spilled, or captured by a later committed
fold) — needs a fold-commit signal in the sweep (`shard/timers.rs`, WS8) and a
promotion hook (`storage/db/kv_ops.rs`, WS10). Recommend a new P0/P1 issue.

### Downgrade (real binaries)
Fixed monoio binary: DEL 100 of 200 cold probes, BGREWRITEAOF (4 shards; each
new incr carries its DEL record), SHUTDOWN; then the BASE binary boots on that
data: 0 of 100 deleted probes back, 0 of 100 live neighbours lost. The head is
plain `DEL`, so older binaries keep the deletes.

### Measured cost of the #1215 ledger (release-fast, relative, this 4-vCPU box)
Interleaved A/B, 3 reps each, one server per run (`scratchpad/ab_ledger_cost.py`):
`--shards 1`, `--maxmemory 16mb`, 300,000 cold-heavy keys (200 B), DEL 294,000
of them keeping every 50th (so their files stay listed), BGREWRITEAOF with a
PING probe every 0.5 ms.

| run | binary | used_memory after the DELs | max PING during rewrite | p99 PING | head DEL records | new incr bytes |
|---|---|---|---|---|---|---|
| A1 | ae21476 | 578,303 | 4.04 ms | 2.15 ms | 0 | 35 |
| B1 | af103a2 | 15,732,016 | 251.72 ms | 1.79 ms | 559 | 5,729,580 |
| A2 | ae21476 | 417,485 | 5.31 ms | 0.65 ms | 0 | 35 |
| B2 | af103a2 | 16,060,629 | 254.37 ms | 3.07 ms | 571 | 5,850,380 |
| A3 | ae21476 | 575,806 | 3.15 ms | 0.32 ms | 0 | 35 |
| B3 | af103a2 | 16,095,572 | 252.40 ms | 2.02 ms | 572 | 5,863,195 |

- RAM: ~54 B per dead slot (13 B keys), charged to the cold index, so the
  eviction budget sees it. Before the fix those bytes were freed — and the
  keys came back after the next rewrite.
- Fold stall: ~0.85 us per dead slot on the shard thread (the AofFold arm).
  Commit 1c36c11 moved the dedupe (a HashSet<Bytes> per key) to the writer
  thread; the selection pass on the same 294,000-entry ledger, debug build:
  916-979 ms before, 325-413 ms after (2.6-2.9x). The release stall after
  1c36c11 is NOT re-measured: the team rules allow two release builds and
  both were spent on the verified binaries (af103a2, monoio + tokio).
- What would bound both: compacting mostly-dead files (rewrite their live
  keys into a new file, unlink the old, drop its ledger entries) — option (c)
  as a background job rather than on the fold path. Follow-up.

### Other pre-existing observations (not fixed, not owned)
- HYPOTHESIS, not reproduced: SWAPDB swaps whole `Database`s, cold index
  included, while the spill files' manifest entries keep their original
  `db_index`. Replay of the logged SWAPDB re-applies the swap to the rebuilt
  indexes, so a plain restart is consistent; but an AOF rewrite drops that
  record (its base is written per current db), after which a restart would
  attribute the swapped dbs' cold files to their original dbs. Same class as
  #1215 (a rewrite discarding the record that made the cold plane right).
  Flagged for whoever owns SWAPDB; needs a reproduction first.
- The ledger's size is not in INFO (`command/connection.rs`, not owned):
  `cold_dead_slots` / bytes would make the cost observable.

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
  fix keeps the deletes — but only UNTIL the older binary runs a rewrite of
  its own, which has no ledger and writes no DELs (corrected in the PR #1233
  review: 0 of 100 back on the old binary's first boot, 37 of 100 back after
  its own rewrite plus a boot). (What an upgrade cannot fix: a generation an
  OLDER binary already rewrote has lost its deletes; that is gone.)
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
  referrer, charged to `ColdIndex::resident_bytes`, so `used_memory` saw it —
  but NOT the eviction budget: the write gate compares
  `Database::estimated_memory()`, which excludes the cold index (corrected in
  the PR #1233 review, which measured 5.0x maxmemory with zero writes
  refused). Bounded only by the slots of heap files still on disk; a file's
  entries go with the file. Worst case: many files each kept alive by one
  live key — the ledger then holds ~every dead key. Fixed in the PR #1233
  review: charged at write admission, and bounded by reclaiming mostly-dead
  files — see "PR #1233 review fixes" at the end.
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
plain `DEL`, so older binaries keep the deletes — until the older binary runs
its own rewrite: it has no ledger, so its generation carries no DELs and the
next boot brings them back (PR #1233 review, measured: 37 of 100 back after
the old binary's own rewrite plus a boot). A downgrade is safe only until the
first rewrite on the older binary.

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

- RAM: ~54 B per dead slot (13 B keys), charged to the cold index and so to
  `used_memory` — NOT to the eviction budget, which is exactly what the PR
  #1233 review found (corrected). Before the fix those bytes were freed — and
  the keys came back after the next rewrite.
- Fold stall: ~0.85 us per dead slot on the shard thread (the AofFold arm).
  Commit 1c36c11 moved the dedupe (a HashSet<Bytes> per key) to the writer
  thread; the selection pass on the same 294,000-entry ledger, debug build:
  916-979 ms before, 325-413 ms after (2.6-2.9x). The release stall after
  1c36c11 is NOT re-measured: the team rules allow two release builds and
  both were spent on the verified binaries (af103a2, monoio + tokio).
- What would bound both: compacting mostly-dead files (rewrite their live
  keys into a new file, unlink the old, drop its ledger entries) — option (c)
  as a background job rather than on the fold path. (Done in the PR #1233
  review, with the unlink deferred to after a committed fold — see "Item 4"
  below for why it cannot be immediate.)

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
  `cold_dead_slots` / bytes would make the cost observable. (Done in the PR
  #1233 review: INFO Memory `cold_dead_slots`, `cold_dead_slot_bytes`.)

---

## PR #1233 review fixes (FIX3-ws15, branch `fix3/ws15-ledger`, from `d4a2fd3`)

Two reviewers (REVIEW3-INTEG, REVIEW3-WS15) found that the moon#1215 ledger
was charged to `used_memory` but not to the maxmemory write gate, so a
delete-churn workload that leaves one live key per spill file grew RAM
without bound (INTEG: `--shards 1 --maxmemory 16mb`, 4 x 20,000 SETs of ~1 KB
keys, DEL all but 1 in 50: used_memory 20.7 -> 82.4 MB = 5.0x maxmemory, 4.9x
on tokio, RSS 143 MB, zero writes refused; `ae21476` stayed at 1.75 MB).
Every rewrite also serialized the whole ledger as one head `Vec<u8>` (78 MB
incr vs 34 B on base).

### Item 1 — record only slots that can come back (verified mechanism)

What the rebuild does with a slot whose own TTL has passed, read in code:
`rebuild_from_manifest_per_db` indexes every slot WITHOUT a TTL check (it
copies `kv.ttl_ms` into `ColdLocation::ttl_ms`), but every value-giving cold
read judges it: `cold_read::read_cold_entry` returns `Expired` when
`now_ms > entry.ttl_ms` (the on-disk deadline), and the index-level liveness
checks in `kv_ops` (`remove_counting_cold`, `unlink`, `cold_contains_alive`,
the promotion paths at ~L1196/L1219) use `loc.ttl_ms.is_none_or(|t| now <= t)`.
So an expired slot, even when it is the newest slot of its key after a
rebuild, reads as absent. The existing recovery-level test
`cold_del_rewrite_tests::a_cold_key_expired_before_a_rewrite_stays_expired`
already proves it on the production recovery path. An expired slot cannot
bring its key back and needs no `DEL`; an OLDER slot of the same key is a
separate ledger entry (per-slot granularity), so dropping the expired one
never hides a slot that can come back.

The only consumer of the ledger is an AOF rewrite fold (`cold_deletes_of`,
`fold_cold_deletes`); grep finds no other reader. `CONFIG SET appendonly`
only edits `runtime_config` (`command/config.rs`, "accepted but don't take
live effect"); writer pools are created once in `main.rs` when
`appendonly == "yes"`, BEFORE recovery replays the log.

Recording sites audited (all inside `ColdIndex` + one ghost site):

| site | class | verdict |
|---|---|---|
| `remove` (DEL/UNLINK, promotion, expired-read reclaim, replay reconcile) | can come back unless its TTL passed | KEEP, with TTL (pruned once the TTL passes) |
| `insert` over a different file (re-spill) | older slot wins if the newer file goes | KEEP, with TTL |
| `clear_all` (FLUSH*) | every slot, until the sweep unlinks | KEEP, with TTL |
| `sweep_known_orphans` (hot shadow) | the hot copy may die later | KEEP, with TTL |
| `release_older_copies[_of]` | older copy wins if the newer file goes | KEEP, with TTL |
| ghost slots of a published file (`apply_completion_vec`) | listed file, never indexed | KEEP, with TTL |
| `sweep_expired` | own TTL passed by definition | DROP (never recorded) |
| any entry whose TTL passes later | reads as expired from then on | DROP (`prune_expired`, run by the expiry sweep; the fold skips it) |
| any site, process without an AOF writer | nothing can consume it | DROP (`enable_ledger` is called by `RewriteOverflow::with_cap`, i.e. by every AOF writer pool, before replay) |
| same-file move, unpublished ghosts | not on disk / still live | not recorded (unchanged) |

No clock is read on the delete path: the slot's TTL is stored with the entry
(`DeadSlot { key, ttl_ms }`, 40 B, the same per-entry charge as before) and
judged later, by the expiry sweep (which already holds `now_ms`) and the fold.
Risk: a wall clock stepped backwards could make an "expired" slot readable
again after a restart — the same exposure an absolute `PEXPIREAT` in the base
has; accepted and stated.

### Item 2 + reviewer A — make maxmemory bind on WRITE ADMISSION only

The two reviews pull in different directions, and both are right:
- INTEG: the gate (`evict_to_budget`, inline pre-gate) compares
  `Database::estimated_memory()`, which excludes the cold index, so the ledger
  never binds.
- WS15 reviewer: the ledger IS counted in the published per-shard memory, so
  it drives the pressure-cascade trigger (`should_run_pressure_cascade`) and
  the elastic budget; eviction cannot free a single ledger byte, so a ledger
  that drives the eviction/spill cascade thrashes the hot set (and the
  cascade's page-cache and vector-demotion steps) to disk for nothing.

Chosen interplay (one rule per reviewer point):
- (i) Admission: a write is admitted only when `evictable + ledger <= budget`
  after the policy's eviction ran. `evict_to_budget` answers OOM otherwise;
  the inline pre-gate skips the slow path only when `evictable + ledger` is
  under budget (`eviction::admission_memory`).
- (ii) Eviction never pays for the ledger past a floor: the eviction target
  is `max(budget - ledger, budget / 2)`. A small ledger is paid for by
  evicting live keys (it is real memory); a ledger past half the budget does
  not push the hot set below half the budget — the write is answered OOM
  instead, per policy, and later writes are refused without evicting again.
  The pressure-cascade trigger excludes the ledger for the same reason.
  With no ledger the target is the budget and the function is unchanged.
- (iii) The real bound is reclaim (item 4).

`used_memory` keeps counting the ledger (it is resident RAM).

### Item 2b — the head DEL emission is streamed and charged
(see the commit; design: the fold ships `FoldChunk::ColdDeletes` chunks of at
most 512 keys; the writer writes one `DEL` record per chunk straight into the
new incr; the chunks' key handles are charged to their database's ledger
while a rewrite is in progress.)

### Item 4 — the real bound: reclaim of mostly-dead files (design (a), made safe)

**Why neither literal variant is safe as written, verified in code.** A
survivor `k` of a mostly-dead file `F` is cold, so its value is only in
`F`'s slot. The COMMITTED generation's replay may read `k` from the cold
plane: `get_*_ref_if_alive` -> `cold_read_only` -> `get_cold_value` reads a
cold key WITHOUT promoting it (`storage/db/accessors.rs`), so a logged
`SUNIONSTORE dst k` / `ZUNIONSTORE` / `SORT … STORE` reads `k` on replay.
Replay only reads files below the generation's `MOON.COLDCUT` or authorized
by a replayed `MOON.SPILLED` (`cold_location_visible`, moon#902). So:
- (a) as written — re-spill into a new PUBLISHED file `F'` (id minted now,
  so `F' >= cut`), re-point, let the sweep unlink `F`: a replay of the
  committed generation reads `k` (for a record logged before `F'`'s marker)
  with `F'` hidden and `F` gone: the read sees nothing. That is the
  promote-then-sweep class (moon#1231) created afresh, whether or not `F'`'s
  marker is durable.
- (b) log-and-promote with an fsync barrier has the same hole: the promoted
  values are logged AFTER the records that read `k`.
- A pin on `F` held in RAM until the next committed fold is not enough
  either: a restart before that fold rebuilds the index with `k` in `F'`,
  `F` then has no referrer after the replay closes, and the sweep would
  unlink it while the committed generation still needs it.

**Chosen: compact now, list after the next committed fold.**
1. Compact (shard tick, while the shard's ledger > budget/4, or > 64 MiB
   without maxmemory; at most 16 files / 16 MiB read per tick; at most 256
   pending per db): write `F`'s live slots (index entry == this slot) into
   a new spill file `F'` through `spill_thread::flush_buffer` (temp, fsync,
   rename, dir fsync). `F'` is NOT listed and nothing is re-pointed. Stamp
   the compaction with the writer's fold epoch `E = RewriteOverflow::stamp()`.
2. Adopt (shard tick) once `RewriteOverflow::committed_floor() > E` — a fold
   whose snapshot instant is after the compaction COMMITTED (new signal:
   `FoldOutcome::adopt` -> `note_committed`, never on abort): list `F'` with
   one durable `manifest.commit()`, re-point every survivor still exactly
   where it was read, record the `F'` slots of survivors that changed
   meanwhile as dead, then unlink `F` (`ColdIndex::unlink_now`: only the
   adopted files, so the orphan sweep's own schedule is untouched) — its
   ledger entries go with it.
3. The AOF auto-rewrite monitor dispatches a rewrite while compactions wait
   (`cold_reclaim::awaiting_fold() > 0`) once the compaction burst settled
   (no new compaction since its previous tick) or after 5 s of waiting, at
   most one every 3 s, and not when `auto-aof-rewrite-percentage 0`
   disables automatic rewrites (a manual BGREWRITEAOF adopts them too).
   (Cadence tuned in `2669a90`: the first cut — 8 files per tick, one fold
   per 10 s, dispatched at the first monitor tick with anything waiting —
   folded while the shard was still compacting and adopted part of the
   burst. On the debug bound test the ledger then stayed over half the
   budget for a round: all 20,000 round-1 SETs and 44,287 in total answered
   OOM (bounded, used_memory peak 1.07x, but slow to recover). Tuned: 0
   refused, peak 0.56x.)

`F'` was minted before the fold's instant `T`, so `F' < w(T)`: for the
generation committed at `T` it is a file below the cut, authorized from the
first record. That is what makes the unlink of `F` safe.

| window | durable state | recovery | ok? |
|---|---|---|---|
| crash during the compaction write | `F` listed + referenced; `F'` a temp or unlisted file | startup orphan sweep removes `F'` (id reserved first, moon#1114); `k` from `F` | yes (`a_crash_before_the_fold_recovers_everything_from_the_old_file`) |
| crash after compaction, before a fold | same | old generation replays, reads `k` from `F`; its DELs delete the dead keys | yes (same test) |
| fold at `T` aborted | nothing adopted (floor unchanged) | old generation | yes |
| crash after the fold committed, before adoption | `F` listed; `F'` unlisted | new generation: head DELs for `F`'s dead keys (the ledger still had them at `T`), `k` from `F` (< cut) | yes (`a_crash_between_the_fold_and_adoption_keeps_every_key_exact`) |
| adoption: `add_file(F')` + durable commit fails | rolled back: tombstoned in RAM, `F'` removed | as above | yes (nothing re-pointed) |
| crash after the adoption commit, before re-point / unlink | `F` and `F'` listed | rebuild: `k` newest in `F'` (< cut, authorized); `F`'s copy is an older copy, released at close; the sweep may unlink `F` — the committed generation no longer needs it | yes |
| crash after unlinking `F`, before its tombstone commit | `F` listed, missing | `files_missing` (benign, counted) | yes |
| `k` deleted / rewritten / re-spilled between compaction and adoption | not re-pointed; `F'`'s slot recorded as dead (listed file) | next fold DELs `k` | yes (`a_survivor_that_changed_before_adoption_leaves_a_dead_slot_in_the_new_file`) |
| every survivor changed (or FLUSH) before adoption | `F'` never listed, removed | — | yes (`a_compaction_whose_survivors_all_changed_is_discarded`) |
| full cycle then restart | `F'` listed, `F` gone | survivors from `F'`, deleted keys deleted, ledger empty | yes (`after_adoption_a_restart_keeps_survivors_and_deletes_with_no_ledger_left`) |

It does not touch the unlink policy for files that reach zero referrers on
their own (the orphan sweep's, moon#1231 is WS16's) and it never unlinks a
file the committed generation still reads. Skipped: a file with a live slot
that does not decode (never compacted by this process, `skip` set); the
legacy multi-shard TopLevel layout (one fold epoch for several shards cannot
say which shard's compaction a commit covers); a process without an AOF
(no ledger).

Costs: per compaction one file read (<= a spill batch), one small write +
2 fsyncs; per adoption tick one durable manifest commit (+ the tombstone
commit of the drain). Plus one fold per reclaim cycle, rate-limited to one
per 3 s. Pending compactions' keys are charged with the ledger
(`ColdIndex::dead_slot_bytes`).

### Found while writing the risks — a delete routed to another shard was refused for memory (`5e3be4c`)

Mechanism, read in code: the connection's own shard lets a command that can
only shrink memory (`db_quota::is_shrink_only_command`: DEL, UNLINK, HDEL,
LPOP, ...) past a refused gate — eviction still runs, only the reject is
bypassed (WS6, `run_write_eviction_gate` / `handler_sharded`). The leg of a
command routed to ANOTHER shard runs `spsc_eviction_gate`
(`shard/spsc_handler.rs`, three call sites + the cross-db COPY gate in
`spsc_two_db.rs`), which returned the OOM for every write. Pre-existing
under `noeviction` (base `ae21476`, `--shards 4`, 4 MiB: 144 of 200
single-key DELs answered `-OOM`). Item 2 made it reachable under EVERY
policy: once a shard's ledger is past half its budget, admission refuses
and eviction cannot shrink the ledger — so the deletes a user sends to free
memory were refused on 3 of 4 shards, exactly when they were needed.

Fix: `spsc_eviction_gate` takes the routed command and applies the same
bypass, for the maxmemory gate and the per-db quota. Test (real server,
`--shards 4`, noeviction, filled past maxmemory; single, pipelined
DEL/UNLINK and multi-key DELs spanning shards):
`perf_ws15_ledger_bound::a_delete_routed_to_another_shard_is_never_refused_for_memory`,
red on `baseline-ae21476` and `fix3-ws15-monoio-082e521`, green after.
Cross-ownership (WS8's files); `fix3/ws8` does not touch these hunks.
Not changed: the Lua bridge gate (`scripting/bridge.rs::gate`, no command
in scope) — a `redis.call('DEL', ...)` inside a script over budget is still
refused (pre-existing; the ledger widens it the same way). Filed as a risk.

### CodeRabbit re-review (Major) — an unencodable in-flight payload committed a lossy generation (`2c8b465`)

Mechanism, read in code: `fold_stream::for_each_in_flight_base_entry`
(shared by the streaming folds via `write_in_flight_entries` and by the
legacy `do_rewrite_single`) only counted and logged a payload that did not
rehydrate; the fold then published a generation without the key. If that
spill is then withdrawn, fails its pwrite or is refused its file id, the
completion puts the value back in RAM only, and a restart loses it — the
moon#1223 class.

Fix: that branch returns `AofError::RewriteFailed` after the counter bump
and the log. Every caller propagates it before anything is published
(streaming: `sink.fail` -> `write_fold_image(_file)` Err in
`do_rewrite_per_shard`, `do_rewrite_sharded`, the tokio
`rewrite_aof_sharded_sync` which also removes its tmp file; legacy: `?`
before `manifest.advance_with`). The expired-payload branch is unchanged.
The in-flight record ends with the spill's completion
(`spill_inflight_clear` on every completion path), so a failed rewrite is
retried and succeeds afterwards. Behaviour change: while such a payload is
in flight, rewrites fail loudly instead of committing a lossy generation.
Tests: `fold_stream::tests::an_undecodable_in_flight_payload_fails_the_fold`,
`rewrite::fold_tests::an_unencodable_in_flight_payload_aborts_the_rewrite_on_the_committed_generation`
(manifest never flips in memory or on disk, no new incr, appends land in
the committed incr); both red with only the `return Err` reverted.

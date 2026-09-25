# WS7-conn-hotpath — working notes

Personas: performance-engineer (lead), acl-security-gatekeeper (every ACL
change is fail-CLOSED, HARD-STOP reviewed). Base `ae21476`.

Red/green method. Most of these fixes remove a lock or a shared write that has
no wire-visible effect, so a red test needs a seam. Three kinds are used:

1. **Binary A/B through the wire**: an integration test (`tests/perf_ws7_*.rs`)
   that spawns `MOON_BIN`. The red run is the same test with
   `MOON_BIN=/home/user/wt/bin/baseline-ae21476`. Counters used are the
   ones the review itself used (`INFO stats` / `moon_dispatch_path_total`).
2. **Lock-held unit tests**: the test thread holds the lock the hot path must
   not take, and the call under test must return within a deadline. On base
   the same call blocks until the deadline.
3. **Source-scan guards** (the `intercept_flag_drift` pattern) where the
   invariant is "this lock is not taken per batch" and the handler loop has no
   seam: they fail on base with the offending line in the assertion text.

## moon#1175 — CLIENT PAUSE + RuntimeConfig per batch

### Mechanism verified at ae21476
- monoio `handler_monoio/mod.rs:1918-1921`, tokio `handler_sharded/mod.rs:683-686`:
  every batch that reaches frame parsing runs `expire_if_needed()` (PAUSE
  **write** lock, unconditional) then `check_pause(true)` (PAUSE read lock).
- Query-buffer limits: monoio reads `runtime_config.read()` at the ceiling
  check every read iteration (`:1555`) and on every hinted read (`:1124`);
  tokio at `:622` per read and `:561` on a hint. `config_set` has no arm for
  `client-query-buffer-limit*` (grep `src/command/config.rs`), so the value
  is fixed for the process lifetime: reading it once per connection is exact.
- tokio per command (`handler_sharded/mod.rs:1452-1491`): `runtime_config.read()`
  for `client_pause_deadline_ms`. The only writer is `handler_single.rs:745/761`
  (the tokio single-shard test/embedded listener); the sharded handler's
  `CLIENT PAUSE` arm (`handler_sharded/dispatch.rs:149-169`) calls
  `client_pause::pause` and never writes it. Dead read in sharded mode.
- `handler_single` has no batch-top PAUSE check (it uses its own deadline);
  untouched.

### Design
- `client_pause::batch_pause_remaining()`: `pause_possibly_active()` first
  (one Relaxed load), then the unchanged `expire_if_needed(); check_pause(true)`.
  The memory-ordering argument on `PAUSE_ANY` is unchanged: the flag is
  published under the same write lock that sets `active`, so `false` is
  authoritative. Cross-thread visibility after `CLIENT PAUSE` returns +OK is
  ordered by the reply/request socket round-trip (kernel socket locks give the
  happens-before), exactly the argument the inline SET pre-gate
  (`blocking.rs`) already relies on.
- Expiry still clears: while the hint is `true` the gate calls
  `expire_if_needed`, which clears `active` and the hint together.
- Query-buffer limits snapshotted once per connection beside the existing
  `write_timeout` snapshot (both handlers).
- tokio per-command deadline read removed (dead in sharded mode).

### Risks
- A future `CONFIG SET client-query-buffer-limit` arm would reach only new
  connections — documented at the snapshot site.

### Result (commit 85ce913)
Red: `batch_gate_takes_no_lock_when_no_pause_is_possible` with the pre-fix
body -> "the batch pause gate took the PAUSE lock ... left: Err(Timeout)";
`handlers_take_no_global_lock_per_batch` on the ae21476 sources -> "handler_monoio/mod.rs:
`client_pause::expire_if_needed(` is called directly on the batch path ... (1918)".
Self-score: Completeness 0.95 · Clarity 0.92 · Practicality 0.95 ·
Optimization 0.92 (every per-batch/per-read/per-command global lock on the
PAUSE+config path is gone; the WRITE-mode batch granularity is unchanged
by design) · Edge cases 0.92 (expiry still clears the hint; second pause
re-publishes; query limits are not runtime-settable) · Self-evaluation 0.9.
Not measured: the win is contention on >=8 cores (issue text), this box has 4.

## moon#1165 — ACL cache, lock type, redundant checks, restricted users

### Mechanism verified at ae21476
- `acl_skip_allowed() = cached_acl_unrestricted && acl_cache_fresh()`;
  `refresh_acl_cache` only at setup and in `adopt_user` (AUTH/HELLO). Any
  version bump left every existing connection stale for life -> inline gates
  false (monoio `can_inline_reads/writes`) and `try_enforce_acl` took the
  global `std::sync::RwLock<AclTable>` per command.
- Mutators bump the version INSIDE their `&mut AclTable` methods (the old
  comment on `refresh_acl_cache` said "after releasing the write lock" — it
  is inside; either way a reader holding the read guard sees a consistent
  (data, version) pair).
- RESET identity semantics are out of scope (plan). Constraint adopted for
  every change here: nothing new trusts a cached verdict for an identity it
  was not computed for, and nothing changes what RESET does.

### Design
1. `refresh_acl_cache_if_stale` at batch top in all three handlers. It only
   changes the cache; every command still re-checks freshness via
   `acl_skip_allowed()`, and the refresh never changes `current_user`.
2. `AclTable` lock -> `parking_lot::RwLock` everywhere (cross-ownership,
   isolated commit; 30 in-process integration tests construct it).
3. Restricted users: `AclTable.users` holds `Arc<AclUser>` (copy-on-write),
   and `refresh_acl_cache` keeps the resolved entry (`acl_cache_user`). The
   per-command gate (`ConnectionState::acl_denial`, used by all three
   handlers) checks against that snapshot when it is fresh AND was taken for
   the current identity (`username == current_user`, a conservative bind:
   any identity change without a refresh falls back to the lookup), else one locked
   lookup for command + keys (was two lookups). `command_denial` lowercases
   into a stack buffer (was a `String` per command).
4. PUBLISH/SPUBLISH/EXEC-time publish and `ScriptAcl` construction skip the
   lock when the connection may skip ACLs FOR ITS CURRENT IDENTITY (same
   identity bind, so no new trust is extended to a stale identity).

### Not done (Should, recorded)
- First-arg rule probes for commands without first-arg rules (2 SipHash per
  restricted command). A per-user "has first-arg rules" bit would need to be
  recomputed on every mutation path; a stale `false` is fail-OPEN
  (`-config|set` skipped). Not worth the risk for two hash probes.
- Inline GET/SET for restricted users (glob on raw key bytes): would put an
  ACL decision inside the inline fast path — the exact bypass class the
  gatekeeper persona treats as HARD-STOP. Left on the generic path.

## moon#1176 — replication offsets / fan-out probe on the write path

### Mechanism verified at ae21476
- `AofWriterPool::issue_append_lsn(&ctx.repl_state, ..)` = `rs.read().issue_lsn()`
  per write: the process-global `RwLock<ReplicationState>` read lock (two RMWs
  on one line every shard writes) + `shard_offsets[i].fetch_add` (unpadded
  `Arc<[AtomicU64]>`: 8 shards share one line) + `master_repl_offset.fetch_add`.
  Connection call sites: inline SET `blocking.rs:3203`, monoio `mod.rs:3410,
  3524, 3926`, tokio `mod.rs:2382, 2481, 2905`, `shared.rs:983` (txn AOF).
- With the fan-out hint set (`FANOUT_HINT`, never cleared):
  `ft::replication_fanout_active` = `rs.read()` + shard 0's backlog MUTEX
  (every shard locks shard 0's backlog just to test `is_some()`), then
  `record_local_write_db` takes `rs.read()` again; at `--shards>1` every
  record is copied into a fresh `Vec` behind a freshly serialized `SELECT`.
- `ReplicationBacklog::append` used `extend(data.iter().copied())`. Checked on
  this toolchain (rustc 1.94.1 -O, standalone): that lowers to a 32-byte SSE
  loop; `extend(data.iter())` lowers to `memcpy` (VecDeque's `Extend<&T>`
  slice specialisation). The issue's "byte-by-byte" is stale, but memcpy is
  still the better lowering for KB-sized records.
- Backlog slots are allocated once (`ensure_backlogs_allocated`) and never
  reset to `None`; the slot `Arc`s and the offset `Arc`s are stable for the
  process (the event loop already clones its slot once at startup).

### Decision: `master_repl_offset` keeps its per-write fetch_add
The value it returns IS the write's LSN (`issue_lsn` returns the master
offset before the add): the globally unique, monotonic tag per-shard AOF
replay merges on (per-shard AOF RFC §2 Rule 3), and `seed_master_offset`
requires it to be >= the max recovered LSN. A sum of shard offsets on read
cannot hand out unique cross-shard LSNs, so removing it would change AOF
semantics, not just INFO. Kept; the cost left is one contended RMW per
logged write.

### Design
- `ReplicationState.shard_offsets: Arc<[CachePadded<AtomicU64>]>`.
- `ReplicationState.plane_live: Arc<AtomicBool>`, set (Release) when backlogs
  are allocated; the fan-out probe becomes hint + one Acquire load. Same
  truth value as the old probe (backlog allocated ⇔ plane live; a replica is
  only registered after its PSYNC allocated the backlogs), and the old probe
  was already a racy point-in-time check released before the write.
- `ReplWriteHandle` (per shard, cloned once into `ConnectionContext`):
  offsets handle + this shard's backlog slot + `stream_db` + `plane_live`.
  LSN issue and `record_local_write[_db]` go through it with no RwLock.
- Pre-serialized `SELECT 0..15` (`Bytes::from_static`), byte-identical to
  `serialize_select_record` (unit-tested), prefix + payload fused into one
  buffer as before.

### Result (#1165: ef4cc82, 7a12096, 7725637, 376c2cf)
Red runs: `existing_connection_returns_to_inline_path_after_acl_setuser` on
the ae21476 binary -> "50 GETs were inlined 0 times"; `acl_table_lock_is_parking_lot`
on the ae21476 sources -> "44 std::sync::RwLock sites remain";
`restricted_check_takes_no_table_lock` and
`unrestricted_script_and_publish_checks_take_no_table_lock` with the lock-free
path mutated away -> Err(Timeout). Fail-closed suites green on BOTH binaries.
Self-score: Completeness 0.9 (Musts done; restricted-user Should done except
first-arg probes) · Clarity 0.92 · Practicality 0.92 · Optimization 0.9 ·
Edge cases 0.93 (revocation plain + pipelined, DELUSER, stale snapshot,
name-bound snapshot, copy-on-write) · Self-evaluation 0.9.

### #1176 result notes
- `ReplWriteHandle` + `plane_live` + padded offsets + static SELECT +
  `extend(iter())`; `master_repl_offset` kept (it IS the LSN allocator).
- Red: `write_handle_takes_no_state_lock` with `issue_lsn` routed through
  `state.read()` -> Err(Timeout); `perf_ws7_repl_offsets` on the ae21476
  sources -> 7 `issue_append_lsn(&ctx.repl_state ..)` sites + `.read()` in the
  probe. `shard_offsets_do_not_share_a_cache_line`: on ae21476 the slice is
  `Arc<[AtomicU64]>`, elements 8 bytes apart by construction (the test's
  assertion `b - a >= 64` cannot hold there).
- Not converted (no `ConnectionContext`, not on the shipped path):
  `handler_single` / `single_aof_log` (library single-shard handler),
  `blocking/pop_log.rs` and `shard/coordinator.rs` (not WS7 files; WS8 owns
  the coordinator) — they keep `AofWriterPool::issue_append_lsn`.

## moon#1166 — CLIENT TRACKING global mutex + inline SET disabled

### Mechanism verified at ae21476
- `tracking_active()` (ACTIVE_TRACKERS > 0) gates every write's
  `invalidate_after_write` -> `written_keys()` (a Vec) -> `invalidate_keys`,
  which takes the ONE `parking_lot::Mutex<TrackingTable>` for any non-empty
  key list, even for keys nobody tracks, and scans every BCAST prefix inside.
- monoio `can_inline_writes` carried `!tracking_active()`: one idle tracker
  pushed every connection's SET to the generic path (the -41%).
- Production uses exactly one table (`tracking::global_table()`: event loop,
  listener, embedded); unit tests build private tables.

### Design (what shipped)
- `tracking/prefilter.rs`: a 4096-bucket counting filter over the GLOBAL
  table's `key_clients`, a tracked-key count, and a BCAST-prefix count, all
  updated under the table mutex (inc BEFORE a key/prefix becomes visible,
  dec AFTER it is gone), all SeqCst. `invalidate_keys` /
  `invalidate_after_write` / `invalidate_after_blocking_serve` /
  `invalidate_server_removed` skip the lock (and, when the table is idle,
  the key extraction) when the filter proves no match. Ordering argument in
  the module doc: a lock-free read and a registration are totally ordered
  exactly as the two lock acquisitions were.
- inline SET stays enabled under tracking and calls
  `invalidation::invalidate_inline_write(key, writer_id)` (writer id threaded
  for NOLOOP). A connection that is itself tracking still stands down.
- BCAST: with any prefix registered every write takes the lock (unchanged
  behaviour, only for BCAST deployments).

### Deferred: striping the table
Plan item "striped tracking table (by key hash)". Not done, on the
performance-engineer qualification gate: the measured defect (an IDLE
tracker taxing every writer) is removed by the pre-filter + inline SET —
the lock is now taken only when a write hits a key some client actually
tracks, when BCAST is on, and on tracked reads. Striping would split
`key_clients`/`client_keys` into N locks, but `route()` (redirects,
inboxes, broken-redirect bookkeeping) needs the client-side state on every
delivery, so a real split is a redesign of the table's two halves (plus
max_keys eviction across stripes), with no measurement available here to
justify it (4 vCPU shared box; the contention it targets needs >=8 cores
and an actively tracking workload). Recorded as the #1166 remainder.

### #1166 result (ef12de2)
Red: `untracked_write_takes_no_global_table_lock` with the pre-filter check
removed (= ae21476's `invalidate_keys`) -> blocked; `idle_tracker_keeps_inline_set`
on the ae21476 binary -> "50 plain SETs were inlined 0 times". Five tracking
suites (86 tests) green. Self-score: Completeness 0.85 (striping deferred —
below 0.9 by design, see above) · Clarity 0.92 · Practicality 0.93 ·
Optimization 0.9 · Edge cases 0.92 (NOLOOP, BCAST, RESP2 redirect, cap
eviction, flush, private tables) · Self-evaluation 0.9.

## moon#1178 — Prometheus per-event registry lookups

### Mechanism verified at ae21476
- `record_keyspace_hit/miss`, `record_dispatch_*`, `record_pubsub_published`:
  per-thread slot bump (#774) PLUS `counter!()` per event (registry lookup:
  key hash + map probe + Arc clone/drop).
- `CachedMetricsHandles`: one command's counter/histogram/error handles per
  connection; `ensure()` re-registers all three on every command switch.
- `record_spsc_drain`: `to_string()` label + `histogram!` per drain.
- `SPSC_NOTIFY_WAKES` / `_RENOTIFY` / `_SKIPPED`: unsharded globals.
- metrics-exporter-prometheus 0.18.3, `build_recorder()`: upkeep is the
  caller's job ("The caller is responsible for ensuring that upkeep is run
  periodically"); moon ran it only on scrape -> histogram samples grow
  without bound when the exporter is on and nobody scrapes.

### Design
- Slots for everything counted per event; `publish_sharded_counters()` at
  scrape (`.absolute`, a `fetch_max` in the exporter -> monotone).
- Per-command counts: per-thread row by label index (`CMD_LABELS`, generated
  from the old match; `cmd_label_index` is the same cardinality guard).
- Parity rules found by an actual scrape diff against ae21476: never register
  a zero-total series (the first cut did — `cmd="zadd"` appeared; fixed by
  building the handle only for a non-zero total), but DO keep
  `moon_command_errors_total{cmd} 0` for every command that ran (the old
  handle cache registered it).
- Upkeep task every 5 s on the admin runtime.

### Result (a6956e9)
Red: `hot_path_recording_does_not_reach_the_registry_per_event` on the
ae21476 implementation -> 10000 lookups / 1000 rounds; green <= 8. Scrape
diff identical series sets. Self-score: Completeness 0.95 · Clarity 0.9 ·
Practicality 0.92 · Optimization 0.92 · Edge cases 0.92 · Self-evaluation 0.9.
Not measured on >=8 cores (the contention part); CPU A/B below.

## moon#1198 (connection items)

- Item 2 (done): inline SET pre-gate reads `estimated_memory()` under the
  SHARED guard (was exclusive, then exclusive again for the write); the
  non-inlined local GET asks `is_hot` under the shared guard and only a
  not-hot key takes the exclusive guard (was exclusive + a key clone for
  every such GET). Owner-only mutation makes the hot answer stable until
  `dispatch_read`.
- Item 3 (done, monoio loop): `metadata::lookup(cmd)` once per command
  (`cmd_meta` / `cmd_is_write`), reused by the NO_INTERCEPT gate, the TXN
  multi-key guard, the local write branch and the cross-txn guard. The two
  remaining `is_write(cmd)` calls sit in the remote-dispatch region (WS8's)
  and were left alone; callees (`single_owner_shard`, key walkers) still do
  their own lookup.
- Item 1 (DEFERRED): cluster-mode lock-free served-slot bitmap + inline
  path. The bit must be "I own it AND it is not migrating AND not
  fail-closed", and ClusterState is mutated at many sites (ADDSLOTS/DELSLOTS/
  SETSLOT in cluster/command.rs, gossip epoch takeovers, failover promotion,
  migration). A stale bit serves a slot this node no longer owns — the
  silent #485 misroute. The robust design is a `ClusterLock` newtype whose
  write guard recomputes the bitmap on drop (256 words + the migrating map),
  so no mutation site can forget; that changes the lock type across
  src/cluster/**, main.rs and WS8-owned shard/conn_accept.rs +
  shard/event_loop.rs, and needs the multi-node cluster suites
  (formation/failover/migration) as its gate. Not attempted inside this wave.

## moon#1187 remainder — per-shard AOF staging buffer: DEFERRED

Read first: WS6 NOTES (Decisions, #1187). Verified at ae21476:
- Every connection-path append is one `AofMessage::Append { lsn, db, bytes,
  epoch }` through the shard's flume channel (`send_append_group`,
  `send_append_bounded_blocking`, `try_send_append_durable`); the writer
  applies the #455 fold filter (`keep_unless_folded(msg, floor)`) and the
  TopLevel SELECT injection (`inject_select_records`, writer-side `last_db`)
  PER MESSAGE, in `src/persistence/aof/{mod.rs,writer_task.rs,group_commit.rs}`.
- Those files are WS15's this wave (`src/persistence/**`, cold-tier/spill
  durability P0/P1 fixes in flight on the same writer).

Why it is not done here (named blockers, not effort):
1. Exactly-once needs the writer to accept a pre-framed CHUNK and apply the
   fold floor per chunk (one epoch per chunk, stamped when the chunk is
   sealed) and to stop injecting SELECTs for chunk contents — a writer
   protocol change in WS15's files, concurrent with their durability fixes.
2. The ack contract changes. Today a record that cannot enter the channel is
   refused BEFORE its client is answered: the inline SET answers
   `-MOONERR AOF backpressure` (moon#838) and the generic leg surfaces
   `AOF_FSYNC_ERR` per write. A staging buffer admits records at FLUSH time,
   after the batch's replies were built; making a failed flush fail-loud per
   client needs a per-record reply patch path that does not exist.
3. A per-SHARD buffer (not per connection) is required to keep apply order
   across the shard's connections, so the flush point is the shard event
   loop (`src/shard/**`, WS8), plus a flush before every `AofFold` handler and
   every `fsync_barrier`.
Recommendation: a follow-up owned jointly by the AOF writer owner and WS8,
landing the chunk message + per-chunk epoch in the writer first, then the
shard-loop flush hook, then the connection call sites.

## Measurements (release-fast `ws7-rf1` @ 71e163a vs `baseline-ae21476`)
Harness: `bench_ws7.py` (scratch; logic summarised here). Shared 4-vCPU box,
other agents building (load 2-7): RELATIVE evidence only. `--shards 1
--appendonly no --save "" --maxmemory 0 --disk-offload disable`, redis-benchmark
7.0.15 on the same host, A/B interleaved per rep (A = baseline, B = ws7), 3 reps.
- tracker (SET k:__rand_int__ v, -P16 -c50 -r100000 -n2M; without / with ONE
  idle `HELLO 3` + `CLIENT TRACKING ON` connection):
  A 1072961/660502, 1194743/645370, 753864/511771  (with/without 0.62, 0.54, 0.68)
  B 1140251/961538, 986680/795229, 748783/763650   (0.84, 0.81, 1.02)
  -> idle-tracker cost: baseline -39% (mean), ws7 -11% (redis: -9% in the review).
- acl (GET -P16 -c50 -r100000, `ACL SETUSER probe on nopass +ping` at t=6s;
  mean progress rps in [2,6)s / [8,12)s):
  A 780874/475707, 840901/513399, 800485/511222   (-39%, -39%, -36%)
  B 810466/783080, 825066/817269, 780851/838533   (-3%, -1%, +7%)
  -> review: moon -48.6%, redis -1.8%.
- metrics (GET -P16 -c50 -r100000 -n2M; --admin-port off / on):
  A 788644/728067, 843170/729395, 851789/752729   (-8%, -13%, -12%)
  B 816660/822030, 830565/823045, 888099/1145475  (+1%, -1%, +29% noise)
  -> exporter cost on GET: baseline ~-11%, ws7 ~0%.
Not measured (needs >=8 cores / a dedicated load generator): the PAUSE and
RuntimeConfig lock contention (#1175), ReplicationState/offset line bouncing
(#1176), the exclusive-guard windows (#1198) — each is proven lock-free /
guard-free by its test instead.

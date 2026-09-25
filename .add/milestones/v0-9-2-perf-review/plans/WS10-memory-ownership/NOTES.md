# WS10-memory-ownership — working notes (part 3, base `ae21476`)

Personas applied: storage-durability-engineer (lead: accounting + replay correctness),
performance-engineer (hot-path allocation rules, interleaved A/B).

## moon#1225 — list multi-key writes lose an element on a cold fault

### Mechanism (verified in code, `ae21476`)
- `list_route` (`command/list/list_write.rs`) asks `peek_list_ref_if_alive` →
  `ref_if_alive` → hot miss → `cold_read_only` → `get_cold_value`. On
  `ColdReadOutcome::Unreadable` that raises `Database::cold_fault` and answers `None`,
  so the route is `Ok(None)` — "no such key" — with the flag pending.
- LMOVE/RPOPLPUSH: the destination probe only looks at `Err`, so `Ok(None)` passes; the
  source is popped; `push_one` → `get_or_create_list_listpack` → `settle_not_live` reads
  the cold file again, faults, `take_cold_fault()` consumes the flag and answers
  `Err(IOERR)`, which `push_one` swallowed (`Err(_) => return`). The flag is gone, so the
  dispatch-boundary `cold_fault_gate` passes the element through: the client is told the
  move happened, AOF/replicas receive it, and the element exists nowhere.
- LMPOP: `Ok(None) => continue` skips the faulted key and pops a LATER key; the gate then
  sees the still-pending flag and rewrites the reply to `-IOERR`. The popped element is
  gone on the primary; the error reply is not propagated, so replicas keep it.
- BLMOVE/BRPOPLPUSH immediate (outside MULTI) do NOT go through `lmove_inner`: they run
  `server::conn::blocking::try_immediate_pop`, whose `move_destination_error` probes the
  destination with `get_list_ref_if_alive(dest).err()` — the same `Ok(None)` blind spot —
  then `list_pop_*` + `list_push_*`, and `list_push_*` swallows the refusal inside
  `if let Ok(list)`. Same loss. The WAKE path (`blocking/wakeup.rs:599`) has the same
  destination probe. Inside MULTI, BLMOVE is rewritten to LMOVE (covered by `list_route`).
  A faulted SOURCE in the blocking scan (`blocking_wrongtype_error` → `.err()`) answered
  "nothing to pop" and PARKED, leaving the fault flag set for the next command on the db
  (the blocking path is outside `cold_fault_gate`).

### Fix design
- Choke point = the two `&self` list probes every one of those paths already calls:
  `get_list_ref_if_alive` / `peek_list_ref_if_alive` answer `Err(cold_fault_error())`
  when the hot miss was a cold FAULT (`take_cold_fault()` after `Ok(None)`: one relaxed
  load on a miss, zero on a hit). Every list-ref caller already forwards `Err` to the
  client (audited: list_read ×4, list_route, blocking ×3, blocking_txn, wakeup ×3,
  group, spsc BlockRegister) — so LMOVE, RPOPLPUSH, LMPOP, BLMOVE/BRPOPLPUSH immediate,
  the BLMOVE wake destination, and BLPOP/BLMPOP on a faulted key all refuse BEFORE any
  pop, with no edit to `server/conn/**` or `blocking/**`.
- NOT done generically in `ref_if_alive`: hash/set/zset/stream callers include
  `Err(_) => None` / `_ => continue` arms (hash_write.rs:1452, ft_aggregate,
  ft_text_search) that would swallow an IOERR the gate can no longer see.
- `list_route` keeps its own explicit check too (belt and braces, documents the rule).
- `push_one` returns `Result`; `lmove_inner` restores the popped element onto the source
  end it came from if the push is refused (only reachable by a transient fault on the
  SECOND cold read of a readable destination, microseconds after the first), and answers
  the error. The full-encoding arm calls `get_or_create_list` directly instead of the
  swallowing accessor.
- `Database::list_push_*` (blocking-only callers, signature kept — changing it touches
  ~60 test call sites in files this WS does not own) now LOGS a refused push
  (`tracing::error!`) instead of dropping it silently.

### Risks
- Wire change: a list command on a faulted key that used to be rewritten to `-IOERR` by
  the gate is now `-IOERR` from the accessor — same bytes. BLPOP/BLMPOP/BLMOVE on a
  faulted key answer `-IOERR` instead of parking (redis has no cold tier; parking on a
  key whose data exists but cannot be read would hide the fault forever).
- LMPOP with a faulted key before a readable one now answers `-IOERR` without popping the
  later key (conservative: the faulted key may hold the element redis would pop first).

### Self-score (#1225)
Completeness 0.93 (tokio integration leg run with the tokio binary later) · Clarity 0.92 ·
Practicality 0.95 · Optimization 0.95 (one relaxed load per list MISS) · Edge cases 0.92
(restore-on-second-read-fault path is reasoned, not exercised: it needs a fault injected
between two reads microseconds apart) · Self-evaluation 0.9.

### Found while fixing (sibling path, same class): SMOVE
`smove` probes the destination with `db.get_set` (= `get_promoted`): a faulted cold
destination raises the flag and answers `Ok(None)`, the member is `swap_take`n from the
source, then `get_or_create_set(destination)` refuses with -IOERR — the member is gone.
Fixed in its own commit (refs moon#1225).

## moon#1160 — collection elements pin the read buffer / the replay buffer

### Mechanism (verified)
- `protocol/parse.rs` freezes the request (`split_to(..).freeze()`) and every argument is a
  `Bytes` slice of that allocation. Full-encoding write sites stored `extract_bytes(..).clone()`
  — a refcount bump on the WHOLE read buffer. Strings copy (`heap_string_owned` → Vec),
  listpack/intset copy into their own buffer, `CompactKey` copies, GEOADD already
  `copy_from_slice`s, the HashWithTtl sidecar already copies.
- Sites that stored the slice: HSET/HMSET/HSETNX (field+value), HINCRBY/HINCRBYFLOAT (new
  field), SADD (both the full path and the intset-crossing loop), SMOVE (destination got the
  request's member), ZADD/ZINCRBY (new member into map+tree) AND every RESCORE
  (`zset_update_existing` re-inserted the caller's probe into the B+tree), LPUSH/RPUSH/
  LPUSHX/RPUSHX full path, LINSERT, LSET, XADD fields/values (also MQ + txn intents through
  `Stream::add`), XGROUP CREATE group name, consumer names (CREATECONSUMER, XREADGROUP,
  XCLAIM, XAUTOCLAIM auto-create) and every PEL entry's consumer (a clone of the ARG).
- Replay: `replay_multi_part` (monoio `--shards 1`) and legacy `replay_aof` (tokio
  `--shards 1`) did `std::fs::read` + `BytesMut::from(&data[..])` (2x the log) and every
  replayed argument sliced that buffer. The framed per-shard path copies each entry's payload
  into its own small buffer (1x the log, per-entry pins only).

### Fix design
- `storage::owned_bytes::detach(&[u8]) -> Bytes` (exact-size `copy_from_slice`); allocation
  lives in storage so `src/command/` keeps its rule. Applied only where a request slice is
  STORED; update paths avoid wasted copies where the probe count is unchanged (HINCRBY*:
  one `get_mut` now serves read+write; SMOVE `swap_take`s the source's stored member and moves
  it — zero copies; a zset rescore moves the tree's own stored member via new
  `BPTree::take` — zero copies, one refcount clone fewer than before).
- HSET/HMSET/SADD on an EXISTING member still copy then drop (one malloc+free instead of an
  atomic inc+dec): keeping ONE hash probe beats saving the copy (std HashMap/IndexSet have no
  borrowed-key entry API).
- Streams: detaching inside `Stream::add`, `create_group`, `insert_consumer` covers the
  callers outside this workstream (MQ exec, txn intents, blocking stream wake). PEL entries
  now hold the consumer's STORED name.
- Replay: `persistence::replay::chunks::ReplayChunks` streams RESP through a bounded buffer
  (1 MiB refills, geometric growth only for a frame larger than what is buffered, resumable
  parser cursor). Used by `replay_incr_resp` and the RESP tail of `replay_aof` (the RDB
  preamble still needs one whole-file read, dropped before the tail streams). Offsets in
  every warning/error stay absolute; best-effort resync semantics kept.

### Risks
- One extra allocation per stored element on the full encodings (redis pays the same).
  A/B throughput for SADD/HSET/RPUSH measured with the release-fast build (see SUMMARY).
- `BPTree::remove` now returns via `take` (same tree walk; `REMOVE_CALLS` counter unchanged).
- Found late (measured, not fixed): `detach` yields a `Vec`-backed ("promotable") `Bytes`;
  its FIRST `clone()` — every reply built as `Frame::BulkString(elem.clone())` — allocates
  the bytes crate's 24-B `Shared` header (32-B class) and keeps it, unbilled. 100K HGETs:
  ≈ +2.6 MiB RSS vs baseline. Already true before this branch for every element loaded
  from RDB/AOF/cold tier; request slices avoided it only by pinning whole read buffers.
  Follow-up: bill the header on first read, or build replies from borrowed slices.

### Self-score (#1160)
Completeness 0.93 (every request-slice store site found by walking `get_or_create_*` +
`Stream::*` callers; GEOADD already copied) · Clarity 0.92 · Practicality 0.94 · Optimization
0.9 (one malloc per stored element, as redis; rescore/SMOVE/HINCRBY paths got cheaper) · Edge
cases 0.92 (update paths, replay offsets, huge frames, resync) · Self-evaluation 0.9.

## moon#1163 — streams never charged to used_memory

### Mechanism (verified)
- No stream write site charged anything (`charge_memory`/`adjust_memory`: 0 hits in
  `command/stream/`). `entry_overhead` → `RedisValue::estimate_memory` → `Stream::estimate_memory`
  scanned the whole stream, so a DEL credited the FULL scan that was never charged:
  `credit_memory` saturated at 0 and every other key was under-counted (moon#861 class).
- `MEMORY USAGE` answered a hard-coded 64 for the payload (`server_admin.rs`).
- Mutators outside this WS: MQ (`shard/mq_exec.rs`, including a DIRECT `group.pel.remove`),
  txn intents (`handler_*/txn.rs` → `Stream::add`), the XREADGROUP-BLOCK wake
  (`blocking/stream_wake.rs` → `create_consumer`/`read_group_new`). Loaders
  (`rdb.rs`, `redis_rdb.rs`, kv_serde) build streams by direct field mutation.

### Fix design
- Allocator-truthful cost model in `stream.rs` shared by the O(1) deltas and the O(n) scan:
  entry = BTreeMap leaf share (append-only map ⇒ half-full leaves) + field-vector capacity at
  its size class + each field/value at its size class; group/consumer = hash slot + name;
  PEL / pending ids = leaf share.
- `Stream.billed` (bytes the ledger carries) is what `RedisValue::estimate_memory` reports for
  a stream, so charge and credit cannot disagree. Mutating methods keep an exact `unbilled`
  delta from their insert/remove results; every stream write command drains it
  (`take_unbilled`) and applies it to `used_memory` in the same command. A mutation by a
  caller outside the stream commands is not lost: the next stream command drains it; until
  then it is simply not counted — it can never be credited without being charged.
- `settle_billing` measures a stream entering the keyspace whole (`Database::set*`,
  `insert_for_load`) once. Lazy free credits exactly the bill (per entry, capped, remainder at
  the end).
- `MEMORY USAGE` reports the measured size (`Stream::estimate_memory`) — separate
  cross-ownership commit (`server_admin.rs`).

### Risks
- A direct field mutation outside the stream methods (MQ's PEL surplus release) leaves the
  bill ABOVE the truth until DEL (over-report — the safe direction for a gate).
- Per-entry cost is ~3.5x redis's listpack-in-rax (moon stores a BTreeMap of Vecs); the ledger
  now says so, which means `maxmemory` binds sooner on stream workloads than before (correct).

### Self-score (#1163)
Completeness 0.92 (every owned stream write drains; MQ/txn/wake are caught up by the next
stream command — not billed at their own site, which needs edits in WS7/WS15 files) ·
Clarity 0.92 · Practicality 0.93 · Optimization 0.95 (O(1) per op; the one O(n) scan runs only
where a whole value arrives, which is already O(n)) · Edge cases 0.93 (PEL re-delivery after
SETID, stale PEL on XCLAIM/XAUTOCLAIM, FORCE, NOACK, MKSTREAM, lazy free, bulk load) ·
Self-evaluation 0.9.

## moon#1206 — listpack backlen byte order

### Mechanism (verified)
- `encode_backlen_into` wrote the 7-bit groups LOW first with 0x80 on all but the last
  (`129` → `81 01`); `decode_backlen` reads from the tail the way redis's `lpDecodeBacklen`
  does and expects `01 81`. Forward walks use `backlen_size(entry_len)` and never read the
  bytes, so only backward walks (`Listpack::iter_rev`; all WS3 list walks were made
  forward-only) broke, and only for entries ≥ 128 B (latent under the 64 B policy).
- Also found: redis's width boundaries are `< 2^(7n) - 1`, so an entry of exactly 16383 B
  takes THREE bytes in redis (`00 ff ff`, captured from redis 7.0.15) where moon's
  `backlen_size` said two — a forward-walk disagreement for that exact length.

### Persisted-format question (the issue's "decision needed")
- PROVEN no persisted artifact carries listpack bytes: `Listpack` has one constructor
  (`new()`), `data` is private; the RDB writers (`rdb.rs`, `redis_rdb::write_typed_value` —
  shared by DUMP) emit `HASH`/`LIST`/`SET`/`ZSET_2` element lists for listpack values; the
  cold tier (`value_codec`/`kv_serde`) encodes elements; every loader rebuilds through
  `push_*`; RESTORE refuses redis's 0x10–0x14 listpack/quicklist payloads. So the flip is an
  in-memory change: no version bump, no normalize pass (a forward-walk normalizer would have
  nothing to read). The stale doc comment claiming redis_rdb emits `*_LISTPACK` verbatim is
  corrected. `persisted_forms_carry_elements_not_listpack_bytes` pins the property.

### Fix
- Encoder writes redis's order; `backlen_size` uses redis's boundaries (generalised past 5
  bytes only for lengths no u32 listpack can hold); the test oracle is a case-by-case
  transcription of `lpEncodeBacklen`. Goldens re-captured from redis-server 7.0.15 DUMP.

### Self-score (#1206)
Completeness 0.95 (encoder, width boundaries, oracle, goldens, doc; persisted-format question
answered with a proof, not a migration) · Clarity 0.93 · Practicality 0.95 · Optimization 0.95
(same byte count, same loop) · Edge cases 0.93 (127/128, 16382/16383/16384, 2^21-1, 2^28,
u32::MAX, wide head/tail seams, iter_rev) · Self-evaluation 0.92.

## moon#1212 — listpack residuals

### Mechanism (verified)
- LPUSHX/RPUSHX: one-probe `&self` existence gate, then the FLATTENING `get_or_create_list`
  (the moon#832 pin test asserted the flatten on purpose). Redis 7.2's `pushxGenericCommand`
  calls the same `listTypePush` as LPUSH (listpack kept, converted past the size policy).
- Blocking serve path: `Database::list_pop_*`/`list_push_*` (BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/
  BLMPOP on the spot and on wake) reached the list through `get_mut_if_present` /
  `get_or_create_list`, which upgrade on access.
- SORT: owning `iter()` per listpack element (a `Vec` each, then `to_bytes` copied it again);
  zset listpacks collected members AND scores before keeping half.
- Growth: `write_entry` grew with `Vec::resize` (doubling). Measured on the capped-list
  fixture a 50-entry listpack of 907 B was billed (and held) at a 1,792 B class.

### Fix design
- LPUSHX/RPUSHX route the gate's `ListRoute` straight into LPUSH/RPUSH's own push (listpack
  arm in place, full arm via `push_full`) — a list already seen full skips the listpack
  probe, so the moon#942 probe budget stays 3/3/1/1.
- `Database::list_push_end` is THE one-element push (listpack in place, promotion past the
  policy, full form otherwise) shared by LMOVE's `push_one` and the blocking accessors, so
  they cannot drift; pops try `list_pop_listpack` (non-creating `&self` probe) first.
- SORT walks `iter_refs` (one copy per KEPT element), zset listpack via `step_by(2)`,
  intset members through itoa.
- Growth: when a write needs more than the capacity, `reserve_exact` up to
  `size_class(new_len)` before the `resize` — the block redis's `lp_realloc` to the new
  `total_bytes` gets from jemalloc, and no more. First measured as bare exact growth
  (`reserve_exact(grow)`, release-fast #2): capped-list fixture 3,791 -> 2,257 B/key
  (redis 2,224), RSS 36.6 -> 22.7 MiB; server CPU per op within the ±15 % noise of the
  SET control, LPUSH-growth median +10 % (paired mean +4 %). Bare exact growth reallocates
  on EVERY push (120 of 120 in the unit counter); rounding the request to the class it
  lands in anyway costs zero bytes (same jemalloc block, same `size_class(capacity)` bill)
  and reallocates once per class entered (23 for 120 pushes; doubling: 8, but every one of
  them off-class). That is the committed variant. Trims keep the capacity (no shrink), so a
  capped list stops reallocating at its steady-state peak.

### Risks
- Growth: ~3x more `realloc` calls than doubling on a GROWING listpack (23 vs 8 for 120
  pushes; bare exact growth was 120). The committed class-rounded variant was proven by the
  deterministic counter test; its release CPU was NOT re-measured (release-build budget:
  #2 measured bare exact growth, the upper bound) — re-run `cpu1212.sh` on the integration
  build.
- BLMPOP's `try_immediate_pop` length read (`server/conn/blocking.rs`, not mine) still
  flattens through `db.get_list` — reported, not fixed.

### Self-score (#1212)
Completeness 0.9 (all five residuals addressed in owned files; the BLMPOP length read is
outside the ownership map and handed on) · Clarity 0.92 · Practicality 0.93 · Optimization 0.9
(measured, not assumed) · Edge cases 0.91 (promotion past policy on LPUSHX, born-listpack
destination, drain-to-empty, LTRIM keeping capacity) · Self-evaluation 0.9.

## moon#1198 — storage items (4, 5a, 5b, 6)

### Mechanism (verified)
- Item 4: `HotKeySketch::tick` = relaxed `fetch_add` on an atomic inside `Database`, run by
  the owner's inline dispatch AND every foreign shard's fast-path read of that database — a
  cache line bouncing between cores on every command.
- Item 5a: `StreamId::to_bytes` = `Bytes::from(format!(..))` per reply entry: growing String +
  a second allocation for the shared header (capacity > len).
- Item 5b: mutable `key::keys` (multi-shard KEYS through the coordinator) cloned every key,
  then `exists` + `peek_if_alive` per key (2 probes); `keys_readonly` probed once per key.
- Item 6: `RECL_SEGMENT_STALL_ACTIVE` / `RECL_MVCC_*` are process-global and each shard's 1 s
  tick OVERWROTE them — a clean shard cleared another shard's stall; INFO's MVCC figures
  were the last writer's.

### Fix design
- 4: `thread_local! Cell<u32>` tick — each thread samples 1 in 64 of ITS commands (the rate
  the sketch was tuned for), no shared write. Observe path unchanged.
- 5a: two `itoa` renders into a 41-byte stack buffer, one exact copy.
- 5b: `keys` IS `keys_readonly` over `iter_live_keys(now_ms)`; expired keys skipped (redis
  `keysCommand` leaves them to active expiry).
- 6: stall gauge = number of stalled shards, moved ±1 on each shard's own transition
  (thread-local previous state, saturating down); readers test `!= 0`. MVCC committed/active
  = SUMS of per-shard wrapping deltas (exact under any interleaving); lag/age = MAX over 256
  per-shard slots.

### Risks
- 6: a shard thread that exits while stalled leaves its +1 (shards live for the process).
- 4: a thread issuing < 64 commands never samples (was already true per database).

### Self-score (#1198)
Completeness 0.92 (the four storage items; the non-storage items belong to other WSs) ·
Clarity 0.93 · Practicality 0.94 · Optimization 0.93 · Edge cases 0.9 (u64::MAX ids, expired
keys during KEYS, concurrent shard ticks) · Self-evaluation 0.9.

## moon#1214 item 3 — `Database::get` second probe

### Mechanism (verified)
- `lookup` probes once to classify (live / expired / absent), then RE-probes on a live hit:
  NLL problem case #3 (returning a borrow conditionally, with `&mut self` needed on the other
  arms for `note_lazy_expired` / cold promotion) rejects returning the first borrow. Safe
  removal needs Polonius or `unsafe` (forbidden here); restructuring the expired/absent arms
  to not need `&mut self` changes lazy-expiry and cold-promotion semantics.

### Evidence (perf, release-fast #1, `--shards 1`, monoio)
- Plain `GET` never reaches `Database::lookup` (0 samples; inline/`dispatch_read` path is
  one probe via `get_if_alive`).
- Workload where EVERY command takes the path — `GETSET key:__rand_int__ <32B>`, P16 c8:
  DWARF call graphs, samples attributed per call SITE in `lookup` (disassembly: `+0x1a` first
  probe, `+0x89` live re-probe). 100K keys: first probe 12.51 %, re-probe **0.61 %** of
  server CPU (9,385 samples). 100 hot keys (cache-resident): 4.51 % vs **1.01 %**.
- The first probe pays the cache misses; the re-probe hits the lines it just loaded.

### Decision
DEFERRED: the removable cost is ≤ 1 % of server CPU on a synthetic worst case and 0 % on
plain GET, well inside the ±12 % the SET control swung in this box's interleaved A/B runs — and removing it needs
`unsafe` or a semantic restructuring. Revisit with Polonius (or a `get_with_hash` DashTable
API that would drop the re-hash, ~⅓ of the re-probe) if a profile ever shows it.

### Self-score (#1214 item 3)
Completeness 0.92 · Clarity 0.94 · Practicality 0.95 · Optimization 0.9 (no change is the
optimal change at ≤ 1 %) · Edge cases 0.9 · Self-evaluation 0.92.

## Gates at the last code commit (`9d20fc5`)
`cargo fmt --check` 0 · `audit-unsafe.sh` 0 · `audit-unwrap.sh` 0 · `clippy --all-targets -D
warnings` 0 · `clippy` tokio 0 · `check --all-targets` tokio 0 · `cargo test --lib` monoio
6421 passed / 1 failed (the known root-only `unreadable_file_is_counted_and_skipped_never_queued_for_unlink`)
· tokio `--lib` over the WS10 modules 1202 passed · integration `perf_ws10_list_cold_fault`,
`perf_ws10_stream_memory`, `perf_ws10_collection_rss` green on both runtimes
(`ws10-debug-final`, `ws10-debug-tokio-final`). SUMMARY.md: the harness refused the
subagent write; its content is in the hand-off report (TEAM-RULES §6).

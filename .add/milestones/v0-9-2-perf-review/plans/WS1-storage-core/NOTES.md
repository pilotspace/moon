# WS1-storage-core — spec notes and deferred designs

ADD-style notes kept outside `.add/state.json` (TEAM-RULES: no `add.py new-task/advance`).

## moon#1159 — DashTable H2 fingerprint

- **Specify**: H2 must not overlap any bit another consumer of the xxh64 hash
  reads. Consumers: directory (top `depth` bits), `home_buckets`
  (`(h>>8)%56`, `(h>>16)%56`), shard routing (`xxh64 % N`), SCAN cursor /
  cold index (`h>>16`, identity only — not a partition). Chosen: bits 32..=38
  (`segment::H2_SHIFT`). Not a format change (ctrl bytes never persisted).
- **Scenarios**: 100K-key table (depth ≥ 10): compares/hit ≈ 1, /miss ≈ 0.2;
  keys sharing the top 10 bits spread over ~116 of 128 fingerprints.
- **Contract**: test-only per-thread key-compare counter
  (`segment::take_key_compares`). Red on top-7 H2: 6.675 / 20.454; green
  1.045 / 0.162.
- Follow-ups (same issue, separate commits): in-place split (stayers keep
  slot + ctrl byte, no heap `Vec`), two-phase upsert probe
  (`probe_for_upsert` + `write_vacant`) so `insert_or_update_slice(&[u8])`
  builds the owned key only on a miss and the `from_raw_parts` alias is gone.

## moon#1161 — reads record LRU/LFU

- **Specify**: reads recorded through `&self` (shared guard + foreign
  readers), entry stays 32 B, WATCH version bits untouched, zero cost when the
  policy tracks nothing.
- **Decision — tracking follows the policy, not `maxmemory`**: the plan said
  "zero cost when no LRU/LFU policy or maxmemory == 0". Redis answers
  `OBJECT FREQ` under an LFU policy (and ages IDLETIME under LRU) regardless
  of maxmemory, and the review's `OBJECT FREQ after 200 GETs: moon 5 / redis
  10` probe is taken without a limit. So: `*-lru` → Lru, `*-lfu` → Lfu, every
  other policy (incl. the `noeviction` default and `--maxmemory 0` benches) →
  Off = one relaxed load + branch. `allkeys-lru` with `maxmemory 0` therefore
  pays the (tiny) stamp cost; that config is explicitly asking for LRU data.
- **Consequence under `noeviction`**: GET does not reset `OBJECT IDLETIME`
  (redis does). TOUCH does (forced LRU stamp). Documented divergence, chosen
  to keep the default/bench path free.
- **Kept divergences** (moon behaviour predates this wave and moon-only
  tooling depends on it): `OBJECT FREQ` under a non-LFU policy answers the raw
  counter (redis: error); `OBJECT IDLETIME` under LFU answers idle seconds
  (redis: error). Changing them needs `scripts/test-consistency.sh` (HOTKEYS
  section), `scripts/test-commands.sh` (`assert_moon_ok "OBJECT FREQ"`) and
  `command::tests` updates — orchestrator's call.
- **NOTOUCH map** (redis `LOOKUP_NOTOUCH`): OBJECT, TTL/PTTL, EXPIRETIME /
  PEXPIRETIME, TYPE, EXISTS, KEYS/SCAN membership, MEMORY USAGE, DEBUG OBJECT,
  DEBUG DIGEST walks → `peek*` accessors. Everything else that goes through
  `get`, `get_if_alive`, `get_if_alive_any_plane`, typed `get_ref_if_alive`,
  `get_promoted`, write accessors on an existing key → records.
- **LRU resolution**: 1 s (same as redis). The integration test paces rounds
  (100 ms) so the verdict does not depend on how many rounds a host fits into
  one second; the clock-stepped unit test is the deterministic pin.

## moon#1190 — large removals

### What shipped
Per-database lazy-free queue with fused walk+free, time-budgeted drain on the
1 ms shard tick (both runtimes) and in the non-sharded tokio expiry task.
UNLINK and active expiry defer walk AND drop (deferred credit); eviction keeps
a synchronous credit and defers only the drop.

### Deferred: O(1) `entry_overhead` (exact running byte count)
Every collection's ledger bytes change through ~90 write-site deltas in
`command/{hash,list,set,sorted_set,geo,…}` (`db.charge_memory(delta)` etc.),
none of which names the key. An exact per-container counter therefore needs
either (a) the counter inside the boxed container (e.g.
`Box<Counted<HashMap<..>>>` in `compact_value.rs`/`entry.rs`) AND every write
site updating it alongside `charge_memory`, or (b) the accessors handing out a
guard type that carries the counter. Both touch WS2/WS3-owned files wholesale.
Attributing deltas to "the last key handed out" was rejected: multi-key
commands (SMOVE, LMOVE, *STORE) interleave handles, and a misattributed delta
silently corrupts the ledger in release builds. Recommended follow-up after the
wave merges: (a), landing with a `debug_assert_eq!(counter, estimate_memory())`
at every accessor hand-out in test builds.

### Deferred: FLUSHALL/FLUSHDB ASYNC
`server_admin::flushdb/flushall` treat ASYNC == SYNC and the FLUSHALL fan-out
runs through `flush_every_database[_locked]` from 8+ dispatch paths
(handlers, spsc_handler, replication apply, AOF replay, Lua pending flush).
Design: `Database::clear_lazily()` = `mem::replace(data, DashTable::new())` +
clear()'s bookkeeping + push a `Work::Table` item that pops segments
(`DashTable::pop_segment`, safe), extracts large values into their own
(uncharged) lazy-free items via `Segment::remove`, and drops the rest ≤ 60
entries per step. Wiring needs an `async` flag through `flush_every_database`.

## moon#1189 — expiry index (quick wins shipped)

### Deferred: structural `(deadline_ms, key_hash)` index (16 B/entry)
- Element `(u64 deadline, u64 xxh64)`; ordering integer-only.
- Resolve at pop: `segment_index_for_hash(hash)` → scan that segment's FULL
  slots whose `hash_key(k) == hash && entry.ttl_ms == deadline` (rehash per
  candidate, or store H2 to prefilter). Collisions (two keys, same deadline,
  same 64-bit hash) are astronomically rare but must be correct: keep a
  multiset (`BTreeMap<(u64,u64), u32>` count) and remove ALL matching keys at
  pop, decrementing per removal.
- Unindex on DEL/TTL change needs the hash (one xxh64 of the key — cheap).
- `volatile-ttl` eviction and `keys_with_expiry` need keys back: resolve via
  the same segment scan.
- Not done in this wave: the win is memory (~55-75 → ~25-30 B/volatile key)
  and needs its own RSS measurement (`scripts/bench-resources.sh` 1M volatile
  keys) plus a collision-forcing test harness (a test hasher hook).

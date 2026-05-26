# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0-alpha] — Unreleased

The v0.2 enterprise beachhead. Built additively on per-shard WAL v3 + the
dual-root manifest; no changes to the KV hot path, MVCC, page format, or
transaction layer.

**Headline capabilities landed in alpha:**

- **Point-in-Time Recovery (PITR)** — `--recovery-target-lsn` /
  `--recovery-target-time` restore to any LSN or wall-clock boundary
  inside the WAL retention window.
- **Change Data Capture (CDC)** — `CDC.READ` polling command with
  Debezium-compatible JSON envelopes, resumable cursors, segment-rotation
  safety.
- **Hash-field TTL** — full Valkey 9.0 / 9.1 surface (`HEXPIRE` /
  `HPEXPIRE` / `HEXPIREAT` / `HPEXPIREAT` / `HEXPIRETIME` / `HPEXPIRETIME`
  / `HTTL` / `HPTTL` / `HPERSIST` / `HGETDEL` / `HGETEX`) with O(1) HGET +
  HLEN fast path. Three-way benchmark vs Redis 8.0.2 / Valkey 9.1.0
  ships in `docs/perf/2026-05-27-hash-ttl-3way-bench.md`.
- **Tier 2 Lane A** — `SWAPDB`, `MOVE`, `COPY ... DB n`,
  `CLUSTER REPLICAS` / `SLAVES`, `CLUSTER COUNT-FAILURE-REPORTS`. All
  WAL-durable with cross-shard atomic semantics.
- **Storage format v1 commitment** — RDB v2 + WAL v3 + multi-part AOF
  manifest grouped under a single `--storage-format v1` umbrella with
  ≥18-month LTS forward-read guarantees.
- **Embedded sharded server** — `server::embedded::run_embedded(config,
  cancel)` exposes the full sharded handler (with `TXN.*`) to in-process
  embedders.

**What is not yet in alpha:** PITR live-snapshot LSN wiring (P3c),
`CDC.SUBSCRIBE` push channel (C3b), and the multi-shard master PSYNC
deferred from v0.1.10. Tracked in `.planning/rfcs/v02-enterprise-architecture.md`.

### Docs — Hash-field TTL three-way benchmark suite (PR #127)

- `scripts/bench-hash-ttl.sh` (2-way harness) + `scripts/bench-hash-ttl-3way.sh`
  (3-way harness with Valkey 9.1.0). Both use `redis-benchmark` against
  concurrent Moon / Redis / Valkey servers on distinct ports with trap-based
  cleanup, FLUSHALL + re-seed between scenarios, and median-of-3 RPS reporting.
- `docs/perf/2026-05-26-hash-ttl-bench.md` — pre-fix 2-way baseline that
  surfaced the two HashWithTtl perf issues fixed in PR #126.
- `docs/perf/2026-05-27-hash-ttl-3way-bench.md` — headline Moon vs Redis 8.0.2
  vs Valkey 9.1.0 comparison across 26 scenarios. Plain HGET p=16 ties both
  competitors (1.00–1.01×). HEXPIRE-family Moon vs Valkey: 0.90–0.99× across
  the surface; HGETEX hits parity at 0.99×. Redis 8.x has no HEXPIRE-family —
  Moon is the only Redis-compatible alternative aside from Valkey.

### Performance — HashWithTtl HGET + HLEN O(1) fast path (PR #126)

Resolves the two HashWithTtl perf issues surfaced by the 2026-05-26 bench:

- `HGET` on `HashWithTtl` was 39.9% slower than plain `Hash` at p=16; now
  **+4% faster** (within VM measurement noise — effective parity).
- `HLEN` on `HashWithTtl` was 80.5× slower than plain `Hash` at 1000 fields;
  now **1.00–1.03× parity** (the O(N) live-count scan is fully eliminated when
  no field has expired).

Two changes shipped together (variant layout forces both at once):

- **`HashWithTtl.ttls` BTreeMap → HashMap.** Per-field TTL probe becomes O(1)
  HashMap lookup instead of O(log N) BTreeMap descent. Active-expire iteration
  doesn't require ordered keys.
- **`HashWithTtl.min_expiry_ms: u64` cached minimum.** Tracks the smallest
  expiry across all per-field TTLs. Invariant: `min_expiry_ms = min(ttls.values())`.
  When `cached_now_ms < min_expiry_ms`, no field can be expired, so `HGET`
  skips the `ttls` probe and `HLEN` returns `fields.len()` directly. The hot
  path is a single `u64 < u64` compare. Invariant maintenance is amortized
  O(1) (one `min(min, ts_ms)` per `HEXPIRE`; conditional recompute on
  `HPERSIST` / overwrite / active reap when the removed TTL equalled the min).

No on-disk format change. `min_expiry_ms` is recomputed at load time from the
existing v2 RDB per-field TTL trailer. 6 new invariant tests cover HEXPIRE /
HPERSIST / HSET-overwrite / active-reap / persistence-decode paths.

### Docs — Hash-field TTL audit follow-up (PR #123)

- `docs/commands.mdx` — Hashes section bumped from "(14)" → "(25)" and now
  lists all 11 new Valkey 9.0/9.1 commands plus a paragraph on the per-field
  return convention and the active-expiry downgrade behaviour.
- `docs/STORAGE-FORMAT-V1.md` §3.2 — added the per-field hash TTL trailer
  format: every `TYPE_HASH` body is followed by
  `[ttl_count u32][field_len varint | field_bytes | ttl_ms u64]*` in v2 RDB
  files; v1 readers stop after the hash body.
- `src/command/metadata.rs` — `HEXPIRETIME` / `HPEXPIRETIME` / `HTTL` /
  `HPTTL` PHF flags flipped from `R` (READONLY) to `RF` (READONLY|FAST) —
  per-field TTL lookup is O(1) thanks to the PR #126 fast path. Cosmetic;
  no behavioural impact.

### Added — HGETDEL / HGETEX atomic compound hash commands (phase 199, issue #110)

Two new Valkey 9.1 atomic compound hash commands:

- `HGETDEL key FIELDS numfields field [field ...]` — returns the values of
  the specified fields and deletes them from the hash atomically. Returns a
  RESP Array with one `BulkString(value)` per found field and `Null` for
  missing fields. If the hash becomes empty after all deletes the key is
  removed entirely (auto-cleanup).

- `HGETEX key [EX s | PX ms | EXAT unix-s | PXAT unix-ms | PERSIST] FIELDS numfields field [field ...]`
  — returns the values of the specified fields and optionally updates (or
  removes) their per-field TTLs atomically. TTL modes:
  - `EX s`         — set relative expiry in **seconds** from now.
  - `PX ms`        — set relative expiry in **milliseconds** from now.
  - `EXAT unix-s`  — set absolute expiry as unix **seconds**.
  - `PXAT unix-ms` — set absolute expiry as unix **milliseconds**.
  - `PERSIST`      — remove any existing per-field TTL.
  - (no mode)      — pure read; no TTL change (fast path, zero DB mutation).
  TTL changes apply only to live (non-expired) fields. Missing / expired
  fields return `Null` and leave TTLs untouched.

Atomicity: per-shard single-threaded execution gives atomicity for free
across the entire field list — no client can observe a partial state between
reads and deletes/TTL-updates within a single call.

Implementation:
- Two new `Database` primitives in `src/storage/db.rs`:
  - `hash_get_and_delete_field` — atomically reads and removes a single
    field; handles Hash, HashListpack, and HashWithTtl (also removes TTL
    sidecar). Downgrades HashWithTtl → Hash when the last TTL is removed.
  - `cleanup_empty_hash` — removes the key when its hash has become empty;
    called once after a HGETDEL/HGETEX field loop.
- `HGETDEL` handler uses `parse_key_and_fields` (phase-198 shared parser)
  plus a single `cleanup_empty_hash` call after the field loop.
- `HGETEX` parser (`parse_hgetex_args`) scans the optional mode token(s)
  before `FIELDS` with mutual-exclusion enforcement; uses saturating i128
  arithmetic + u64 clamp for safe overflow handling (mirrors phase 196).
- Both commands dispatch as writes (WF flags, `&mut Database`); neither is
  added to `is_dispatch_read_supported`.
- 17 new unit tests: 8 for HGETDEL, 9 for HGETEX.

### Added — Hash-field TTL read + persist (phase 198, issue #109)

Five new Valkey 9.0 hash-field TTL commands:

- `HEXPIRETIME key FIELDS numfields field [field ...]` — absolute expiry
  per field as a unix timestamp in **seconds**.
- `HPEXPIRETIME key FIELDS numfields field [field ...]` — absolute expiry
  per field as a unix timestamp in **milliseconds**.
- `HTTL key FIELDS numfields field [field ...]` — remaining TTL per field
  in **seconds**; already-expired-but-not-reaped fields return `0`.
- `HPTTL key FIELDS numfields field [field ...]` — remaining TTL per field
  in **milliseconds**; same `0` edge-case for expired-but-not-reaped.
- `HPERSIST key FIELDS numfields field [field ...]` — removes the per-field
  TTL; downgrades `HashWithTtl` back to plain `Hash` when the last TTL is
  removed (handled by the phase-195 `hash_persist_field` primitive).

Per-field return codes (Valkey 9.0):
- `-2` — field does not exist (or key is missing — **not** a WRONGTYPE error)
- `-1` — field exists but has no TTL
- `≥0` — absolute unix time or remaining duration (HEXPIRETIME/HPEXPIRETIME)
- `1` — TTL successfully removed (HPERSIST only)

WRONGTYPE is returned immediately (before field iteration) when the key
holds a non-hash value.

Implementation:
- `FieldState` tri-state enum and `hash_field_state` helper (pre-landed in
  phase 197) provide the zero-allocation field-state read used by all five
  commands.
- `parse_key_and_fields` shared parser (`hash_write.rs`) extracts
  `key FIELDS numfields field [field ...]`; reuses `SmallVec<[&[u8]; 4]>`
  to avoid heap allocation for the common ≤4-field case.
- Four read handlers (`HEXPIRETIME`, `HPEXPIRETIME`, `HTTL`, `HPTTL`) take
  `&Database`; `HPERSIST` takes `&mut Database`.
- All five commands routed in both `dispatch()` (mutable path) and
  `dispatch_read_inner()` / `is_dispatch_read_supported()` (shared-read
  path) for the four read commands.
- 14 unit tests cover all return-code variants, the already-expired edge
  case, WRONGTYPE, missing key, numfields=0, and encoding downgrade.

### Added — Hash-field active expiration (phase 197, issue #108)

All 9 hash read commands (`HGET`, `HMGET`, `HGETALL`, `HEXISTS`, `HLEN`,
`HKEYS`, `HVALS`, `HSCAN`, `HRANDFIELD`) now respect per-field TTLs set by
the `HEXPIRE` family. Expired fields are invisible to callers without
requiring the active-expiry tick to have run first (lazy expiry).

**Lazy expiry** (read path, `&Database`): `HashRef` gains a third variant
`WithTtl { fields, ttls, now_ms }` that filters expired fields on every
field-level operation. The `get_hash_ref_if_alive` accessor now returns this
variant for `HashWithTtl` entries instead of falling through to `WRONGTYPE`.
All 9 mutable read commands now call `get_hash_ref_if_alive` instead of the
unfiltered `get_hash`, so expired fields are never returned.

**Active expiry** (tick path, `&mut Database`): the per-shard `expire_cycle`
gains a second sweep via `Database::hashes_with_field_expiry()` and
`Database::reap_expired_fields_one_hash()`. The reaper removes expired
fields from the `fields` and `ttls` maps, downgrades the hash back to plain
`Hash` when the last TTL sidecar entry is drained, and signals `KeyDeleted`
when all fields expire (the key is then removed by the caller).

The `maybe_has_expiring_keys` fast-path flag is now cleared only when
**both** the whole-key sweep and the hash-field sweep return empty, preventing
premature flag-clearing that would have silenced future field reaping.

**Complexity change**: `HLEN` is now O(N) for `HashWithTtl` hashes (counts
only live fields). Plain `Hash` and `HashListpack` remain O(1) / O(N)
respectively — no regression.

### Added — HEXPIRE-family write commands (phase 196, issue #107)

- `HEXPIRE key seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]`
- `HPEXPIRE key milliseconds [NX|XX|GT|LT] FIELDS numfields field [...]`
- `HEXPIREAT key unix-seconds [NX|XX|GT|LT] FIELDS numfields field [...]`
- `HPEXPIREAT key unix-ms [NX|XX|GT|LT] FIELDS numfields field [...]`

Per-field return codes match Valkey 9.0: `0` (no such field), `1` (TTL set or
updated), `2` (field expired during this call and was deleted), `-2` (NX / XX /
GT / LT condition not met). Wrong-type key returns `WRONGTYPE`; missing key
returns one `0` per requested field. PHF + dispatch routes the four commands
to `hash::{hexpire,hpexpire,hexpireat,hpexpireat}`; AOF replay already handled
by the phase-200 intercepts.

Side fix — HashWithTtl-aware hash-write path: `HSET`, `HMSET`, `HSETNX`,
`HDEL`, `HINCRBY`, and `HINCRBYFLOAT` previously returned `WRONGTYPE` once
`HEXPIRE` had promoted a hash to the `HashWithTtl` encoding. Extended
`Database::get_or_create_hash` / `get_or_create_hash_listpack` to handle
`HashWithTtl` (returning the inner `fields` map / `Ok(None)` respectively).
Added `Database::hash_clear_field_ttls` (called by HSET/HMSET on overwrite,
no-op on plain `Hash`) and `Database::hash_delete_field` (used by HDEL —
removes from both the fields map and the per-field TTL sidecar, downgrades
back to plain `Hash` when the last TTL is dropped). HSETNX correctly leaves
existing TTLs intact; HINCRBY / HINCRBYFLOAT preserve TTL on the incremented
field.

### Fixed — Test infra: txn_kv_wiring flake diagnosis

- `test_txn_commit_wal_crash_recovery` previously masked moon-server crashes
  as "Connection refused (os error 61) after 60s" because the spawned moon
  binary's stdout / stderr were piped to `Stdio::null()`. Hardened the
  child-process harness:
  - `ChildGuard::spawn` now redirects child stdout/stderr to
    `moon-phase-{1,2}.log` inside the per-test temp dir.
  - `ChildGuard::poll_exit` checks `try_wait()` inside the connection retry
    loop — if the child exited, the test fails immediately with the exit
    status instead of timing out for a useless 60 s.
  - `ChildGuard::dump_log` is called on every failure path
    (`wait_for_server` timeout, `connect_redis_with_retry` timeout,
    child crash), so the CI log records the actual server output.
  - `wait_for_server` deadline widened from 5 s → 15 s, matching the
    realistic CI-runner spawn + WAL boot envelope.
- No semantic change for the happy path. Future flakes become diagnosable
  instead of silently retrying for 60 s.

### Docs — Storage Format v1 commitment (phase 192, PR #115)

- `docs/STORAGE-FORMAT-V1.md` — public on-disk format contract for v0.2.x.
  Documents the WAL v3 / RDB v2 / AOF multi-part sub-formats as a single
  "storage format v1" umbrella with explicit forward-read, reverse-read,
  crash-recovery, and migration guarantees through ≥18 months of LTS.
  Adds cross-reference doc-comments to `src/persistence/aof_manifest.rs`,
  `snapshot.rs`, and `wal_v3/segment.rs` pointing readers at the canonical
  on-disk markers. Reserves a `--storage-format <v1>` CLI flag for the
  follow-up code PR closing issue #103's second checkbox.

### Added — Hash-Field TTL primitive (phase 195, PR #116)

- `RedisValue::HashWithTtl { fields, ttls }` storage variant + borrowed
  `RedisValueRef::HashWithTtl` view. Per-field TTL sidecar (`BTreeMap<Bytes, u64>`)
  carries absolute unix-ms expiry alongside an unchanged `HashMap<Bytes, Bytes>`
  field map. `OBJECT ENCODING` reports `hashtable`.
- `Database::hash_set_field_ttl(key, field, abs_ms, cond)` — Valkey 9.0 parity
  result codes (0 missing / 1 set / 2 deleted-on-set / -2 cond not met).
  NX / XX / GT / LT semantics matching Valkey (non-volatile is +∞ for GT/LT).
  Auto-promotes `Hash` / `HashListpack` → `HashWithTtl` on first per-field TTL;
  past-expiry short-circuits to in-place delete with code 2.
- `Database::hash_persist_field(key, field)` — clears one field's TTL;
  downgrades back to plain `Hash` when the last TTL is removed.
- `Database::hash_get_field_ttl_ms` + `Database::hash_field_state` — read
  helpers returning the tri-state `FieldState::{Missing, NoTtl, Ttl(u64)}`
  consumed by phase-198 HTTL / HEXPIRETIME / HPTTL / HPEXPIRETIME read commands.
- Additive `HashWithTtl` match arms in `command::key::should_async_drop`,
  `server_admin::estimate_serialized_length`, `eviction::evict_one_async_spill`,
  `tiered::kv_spill`, `tiered::kv_serde`, `persistence::rdb::write_entry`,
  `persistence::redis_rdb`, and `persistence::aof::generate_rewrite_commands`.
  Persistence-side TTL payload lands in PR #117 (phase 200); arms here are
  TTL-stripping placeholders so HEXPIRE handlers (phase 196) cannot be
  merged ahead of the persistence wiring.

### Added — Hash-Field TTL persistence wiring (phase 200, PR #117)

- **RDB v2** — bumped `RDB_VERSION 1 → 2`. New per-hash trailer
  `[ttl_count u32][field, ttl_ms u64]*` follows every `TYPE_HASH` body
  (count=0 for non-TTL hashes). Reader accepts both v1 and v2 files;
  `count_entries_per_db` / `skip_entry` / `read_entry_zero_copy` /
  `read_entry` all plumb a `has_hash_ttl_trailer` flag so the format
  is parsed correctly on both code paths (file load + in-memory bytes).
- **Shard RDB V3** — `SHARD_RDB_VERSION 2 → 3` with the same trailer
  plumbed through `rdb::read_entry`. V1 (legacy) / V2 (PITR LSN+ts) /
  V3 (TTL trailer) all load via per-version preamble + min-file-size
  branching.
- **Tiered KV serde** — per-hash trailer on disk-offload blobs. Plain
  Hash + HashListpack serializers now append `ttl_count = 0`;
  HashWithTtl serializer writes the real trailer. Deserializer treats
  a truncated trailer as zero TTLs for graceful migration from
  pre-trailer in-process spill blobs.
- **BGREWRITEAOF** — `RedisValueRef::HashWithTtl` arm emits
  `HSET key f1 v1 f2 v2 ...` followed by per-field
  `HPEXPIREAT key abs_ms FIELDS 1 field`, one per TTL'd field. Per-
  field framing keeps the replay shim simple (single-field parse).
- **Replay shim** — `CommandReplayEngine::replay_command` intercepts
  `HEXPIRE` / `HPEXPIRE` / `HEXPIREAT` / `HPEXPIREAT` / `HPERSIST`
  (case-insensitive) **before** `command::dispatch` and routes
  directly to `Database::hash_set_field_ttl` / `hash_persist_field`.
  This bypasses the phase-196 command handlers (which do not yet
  exist) so crash-restart restores per-field TTLs from any AOF stream
  emitted by either user-typed HEXPIRE or BGREWRITEAOF.
- **Redis-compat RDB** — emits `tracing::warn!` when dropping per-field
  TTLs on Redis-compat export. Redis 7.4 hash-field-TTL opcode
  emission deferred to a future cross-vendor compat phase.

### Added — Tier 2 Lane A (PR #100)

- **T2.1** `c381b31` — `SWAPDB` cross-shard atomic swap via `ShardMessage::SwapDb`;
  WAL-durable; BGREWRITEAOF concurrency guard; restart-replay test.
- **T2.2** `4958dc9` — `MOVE key db` with `with_two_dbs_locked` (lower-index-first
  lock ordering); WAL-durable; intercept in all four handler paths.
- **T2.3** `bbc6117` — `COPY ... DB n` cross-database; reuses `with_two_dbs_locked`;
  WAL-durable.
- **T2.4** `f538589` — `CLUSTER REPLICAS` / `CLUSTER SLAVES`; shared
  `format_node_line(node, self_node_id)` helper extracted from `CLUSTER NODES`.
- **T2.5** `ebd240a` — `CLUSTER COUNT-FAILURE-REPORTS`; counts non-stale
  `pfail_reports`; exposes `DEFAULT_NODE_TIMEOUT_MS` as `pub(crate)`.

### Fixed

- **PERF** `608e2d1` — collapse duplicate `is_write` PHF gate on MOVE/COPY hot
  path; restores s=1 SET p=1 throughput (−9.5 % → +0.9 % vs. merge base).
- **CR** _(this PR)_ — SWAPDB now runs **after** the ACL gate in `handler_monoio`,
  closing a runtime-specific authorization bypass.
- **CR** _(this PR)_ — `with_two_dbs_locked` and `ShardDatabases::swap_dbs` now
  hard-assert non-equal indices in release builds, preventing same-index
  self-deadlock.
- **CR** _(this PR)_ — `SWAPDB` strict arity (exactly two args) across all three
  handlers; rejects `SWAPDB 0 1 extra` with the canonical wrong-arity error.
- **CR** _(this PR)_ — `DashTable::Segment::insert_or_update_at` now sets
  `has_non_home_keys = true` whenever the chosen free slot is in a non-home
  group; fixes a latent miss where `find()` could not locate a fallback-placed
  key on subsequent lookups.
- **CR** _(this PR)_ — Local `MOVE` / `COPY` AOF append is gated on
  `Frame::Integer(1)` (success) rather than `!Error`, matching the
  `handler_single` behavior and suppressing no-op `:0` log entries.
- **CR** _(this PR)_ — `MOVE`, `COPY ... DB n`, and `SWAPDB` are rejected with
  `ERR_TXN_CROSS_SHARD` while an `active_cross_txn` is in flight; previously the
  intercepts bypassed undo/intents bookkeeping and escaped `TXN.ABORT` rollback.
- **CR** _(this PR)_ — `spsc_handler` `MOVE`/`COPY` arms call
  `refresh_now_from_cache` on both source and destination DBs before
  `move_core`/`copy_core`; fixes expired-key visibility skew on the local-write
  path.

### Refactor

- `e429b2b` — `src/storage/dashtable/segment.rs` (1587 LOC) split into
  `segment/{mod,find,insert,ops}.rs`; mechanical refactor, zero semantic change,
  brings all files under the 1500-LOC limit ahead of future hot-path additions.

### Added — Point-in-Time Recovery (PITR)

- **P0** `ac3aa92` — `WalWriterV3::new()` now scans existing `.wal` segments on
  open and resumes `next_lsn` from `max_observed_lsn + 1`. Fixes a latent
  durability bug where LSN reset to 1 on every restart and blocked both PITR
  and CDC.
- **P1** `e1e9bda` — `FileEntry` extended with `last_modified_lsn` (offset
  48..56, struct size 48 → 56). Manifest `format_version` bumped to v2;
  backward-compat reader synthesizes `last_modified_lsn = created_lsn` for v1
  entries. New CLI flags `--recovery-target-lsn` and `--recovery-target-time`.
- **P2** `25ece4b` — Snapshot header bumped to `SHARD_RDB_VERSION = 2`,
  embedding `last_lsn` and `created_at_unix_ms`. v1 snapshots load with
  `last_lsn = 0` and are conservatively skipped by PITR. Adds
  `read_snapshot_metadata` peek API.
- **P3a** `048a883` — `replay_wal_v3_dir_until(stop_at_lsn)` and
  `resolve_target_time_to_lsn()` (scans `TemporalUpsert` / `GraphTemporal`
  records for `system_from` anchors).
- **P3b** `a496413` — `recover_shard_v3_pitr()` honors `target_lsn`: skips
  snapshots whose `last_lsn` is unknown or past the target, then stops replay
  at the cutoff without advancing `wal_flush_lsn`.

See `docs/guides/pitr.md` for operator usage.

### Added — Change Data Capture (CDC)

- **C1** `b271e21` — `WalTailReader` with resumable `TailCursor { segment_seq,
  byte_offset, last_lsn }`. Re-stats segment metadata on each call for
  torn-write safety, auto-advances on segment rotation.
- **C2** `e97b80d` — New `src/cdc/` module with typed `CdcEvent` enum,
  `decode_wal_record()` translator, and hand-rolled
  Debezium-compatible JSON envelope serializer
  (`encode_debezium`). Non-UTF-8 keys fall back to `{"_b64":"..."}`.
- **C3 v1** `bf4230b` — `CDC.READ <wal_dir> <from_lsn> [LIMIT N]` polling
  command. Returns RESP array `[next_lsn, env1, env2, ...]`; default LIMIT
  256, hard ceiling 10 000. Idle response is `[from_lsn]` (length 1) — stable
  no-new-data signal.

See `docs/guides/cdc.md` for consumer integration.

### Added — Embedded sharded server

- **PR #95** — `server::embedded::run_embedded(config, cancel)` exposes the
  full sharded handler (with TXN.* cross-store transactions) to in-process
  embedders such as `helios moon-daemon`. The existing `run_with_shutdown`
  drives `handler_single`, which deliberately does not implement TXN.
  Embedded mode skips TLS, console, cluster bus, admin port, and multi-part
  AOF manifest replay; it does include per-shard RDB + WAL recovery,
  graph/temporal/workspace/MQ WAL replay, SO_REUSEPORT, NUMA pinning, and
  cancel-driven graceful shutdown.

### Fixed — PR #96 test deflake + tokio AOF replay

- `main.rs` AOF recovery: gated the multi-part AOF manifest replay block to
  `#[cfg(feature = "runtime-monoio")]`. Under tokio, the legacy single-file
  `appendonly.aof` is loaded via the v2 recovery chain; the multi-part loader
  no longer creates an empty manifest at first boot that wiped v2-loaded
  state on the next restart (every tokio SET was lost on restart).
- `tests/txn_kv_wiring.rs`: made `test_txn_commit_wal_crash_recovery`
  runtime-agnostic by polling for either the monoio multi-part `.base.rdb`
  artifact or the tokio single-file `appendonly.aof`. Added
  `connect_redis_with_retry` helper to bound and retry the post-bind RESP
  handshake (was racing the shard accept loop, surfacing as EAGAIN on Linux
  and ECONNRESET on macOS CI).

### Fixed — PR #95 review hardening

- `main.rs` `malloc_conf` symbol: replaced the union-based unsafe pun with
  a `#[repr(transparent)]` `Sync` wrapper around a `c"..."` literal.
- `command/server_admin.rs` `get_vsz_bytes`: replaced four `unsafe`
  libc::{open,read,close,sysconf} blocks with safe `/proc/self/status`
  parsing on the cold MEMORY DOCTOR path.
- `main.rs` arena scan now uses `env::args_os()` so non-UTF-8 argv no
  longer panics before clap reports the error.
- `server/embedded.rs` shutdown sequence: cancel → join shard threads →
  drop the outer `aof_tx` → join the AOF thread, so the writer never
  exits while shards are still queuing appends. Thread join panics are
  now propagated through the function result.
- `storage/db.rs` annotated two `.expect()` calls in `Database::set`'s
  `insert_or_update` closures for the hot-path unwrap ratchet.

### Deferred to v0.2 follow-ups

- **P3c** — wire `SnapshotState::set_last_lsn(wal_flush_lsn)` into the live
  persistence tick so freshly-written snapshots embed their LSN (currently
  PITR falls back to full WAL replay when only v1-shaped snapshots exist).
- **C3b** — push-based `CDC.SUBSCRIBE` over RESP3 Push frames, per-shard
  subscriber registry hooked into `wal_append_and_fanout`, slow-consumer
  disconnect policy. Envelope format unchanged.
- Integration suites (`tests/pitr_integration.rs`,
  `tests/cdc_integration.rs`), `scripts/test-pitr-cdc.sh` end-to-end smoke,
  and the benchmark gates (PITR restart ±10%, CDC ≥100K events/s/shard,
  write p99 ±5%).

## [0.1.12] — 2026-05-12

Performance & memory observability release. 50 commits since v0.1.11, no
public API breaks, no on-disk format change. Validated on OrbStack `moon-dev`
(2026-05-12) and locally green for both `runtime-monoio` and
`runtime-tokio,jemalloc`.

### Performance — DashTable hot-path (Phase 189, PERF-07 + PERF-09)

- **Pre-sized DashTable.** `DashTable::with_capacity()` plus the new
  `--initial-keyspace-hint <N>` flag size the segment array up front so
  steady-state operation hits zero `split_segment` calls. Pre-size
  invariant test confirms zero splits at 1 M keys. The 27 % CPU spent
  in `split_segment` during SET p=16 (PERF-07) is fully eliminated.
- **`Database::set` rewrite.** New `DashTable::insert_or_update` /
  `Segment::insert_or_update_at` single-probe helpers replace the
  previous `find + remove + insert` triple-probe pattern.
- **`Segment::find` fallback elimination + force-inlined SIMD.** The
  cold "key spilled to non-home group" fallback path is removed once
  `has_non_home_keys` is invariant-tracked on insert (the
  `insert_or_update_at` change above already maintains the flag);
  the SIMD probe helpers are `#[inline(always)]`. PERF-09
  attributed 12.65 % of `Segment::find` self-time to the fallback;
  remaining cost is the irreducible per-hit `memcmp` confirm
  (threshold amended to <3 %). 1 M-key correctness gate validates
  zero false positives/negatives.

### Performance — Memory observability (Phase 190)

- **`moon_memory_bytes{kind=…}` Prometheus gauge.** Seven subsystem
  labels — `dashtable`, `hnsw`, `csr`, `wal`, `sealed_replication_backlog`,
  `allocator_overhead`, and the rolled-up `total`. Updated every
  scrape via a single hook so the sum reconciles to `RSS` within the
  CI tolerance window.
- **`MEMORY DOCTOR` full schema.** Multi-line RESP response covering
  every subsystem, the rolled-up total, and a derived `allocator_overhead`
  pseudo-kind (RSS − Σ subsystems). Adds operator triage signal beyond
  the legacy single-line summary.
- **`resident_bytes()` trait** implemented across `Database`,
  `DashTable`, `VectorStore` (HNSW + IVF), `GraphStore` (CSR + SlotMap),
  `WalWriter`, `ReplicationBacklog` (sealed-segment side), and
  `AllocatorOverhead`. Zero-allocation, on-demand poll.
- **Memory steady-state CI job.** `scripts/bench-memory-steady-state.sh`
  + baseline fixture; gate widened to `±10 %` on RSS / Σ ratio after a
  Linux-CI tolerance pass.

### Changed — Allocator UX (Phase 191)

- **jemalloc `narenas:8` cap** with `--memory-arenas-cap <N>` CLI
  override. Caps the per-CPU arena explosion that inflates VSZ on
  high-core hosts; mostly a cosmetic fix on Linux containers but
  produces a meaningfully tighter `top`/`ps` reading for operators.
- **Tri-state allocator selection.** New `mimalloc-alt` cargo
  feature alongside the existing `jemalloc` / `mimalloc` (fallback)
  paths; mutually exclusive at compile time. A/B benchmark script
  `scripts/bench-allocator-ab.sh` ships with the release.
- **`docs/OPERATOR-GUIDE.md` — Memory Accounting section.** Documents
  the VSZ-vs-RSS distinction, MEMORY DOCTOR field-by-field, and the
  `--memory-arenas-cap` / `mimalloc-alt` tuning knobs.

### Added — Dispatch Observability (Phase 177)

- **`moon_dispatch_path_total{path=...}` Prometheus counter**: four-way classification of every command by shard-routing decision — `local_inline` (SIMD fast path), `local` (standard local branch), `cross_read_fast` (RwLock shared-read bypass of SPSC), `cross_spsc` (deferred cross-shard write via `PipelineBatchSlotted`). Ratio `cross_spsc / Σ` is the ground-truth signal for dispatch-layer optimization work. Zero-allocation hot-path overhead (`&'static str` labels, `#[inline]` with early-return on `!METRICS_INITIALIZED`). Verified on macOS + Linux: counter sums close exactly to driven traffic, no overcount.

### Changed

- **`text-index` is now a default feature.** BM25 full-text search (`FT.SEARCH` BM25 mode), `FT.AGGREGATE`, and three-way RRF hybrid fusion are included in all standard builds. No longer requires `--features text-index`. To exclude it (e.g. minimal embedded builds): `--no-default-features --features runtime-monoio,jemalloc,graph`.

### Added — SDK Validation

- **Python SDK `sdk/python/examples/validate.py`**: End-to-end live validator for all SDK sub-clients: ping, strings, counter, hash, list, set, zset, vector index lifecycle, graph engine, session search, semantic cache, text search (BM25 + aggregate + hybrid), and server info. Result against Moon with `text-index`: **114 PASS / 0 FAIL / 0 SKIP**. Gracefully skips text sections when server built without `text-index`.
- **Rust SDK `sdk/rust/examples/validate.rs`**: Re-validated against `text-index` build — **85 PASS / 0 FAIL**.

### Fixed — Python SDK

- **`moondb.graph._parse_neighbors`**: server returns alternating `[edge_map, node_map, ...]` as flat key-value arrays (`b'id'`, `int`, `b'src'`, `int`, `b'dst'`, `int`, …). Previous parser expected positional `[node_id, label, props]` — caused `int() on b'id'` crash. Now correctly identifies node entries by `labels` key and parses them from the flat kv format.

### Fixed — CI Hygiene

- **`tests/pipeline_auto_index.rs`**: tighten outer cfg from `runtime-tokio` to `all(runtime-tokio, text-index)` so the file compiles to zero tests when text-index is disabled. Previously the file compiled but the FT.SEARCH text fast path was `#[cfg]`-ed out, causing `@name:corpus` queries to fall through to the KNN-only parser and panic with "invalid KNN query syntax".
- **4 FT unwraps**: add inline `#[allow(clippy::unwrap_used)]` with invariant justifications in `vector_search/ft_text_search.rs` (3 sites inside `apply_post_processing` where `do_summarize` / `do_highlight` implies the Option is Some) and `handler_monoio/ft.rs:165` (`is_text` was derived from `query_bytes.as_ref().map_or(false, _)`). Restores the audit-unwrap baseline to 0.

### Compatibility

- **Wire protocol**: unchanged. Drop-in replacement for v0.1.11.
- **Persistence on-disk format**: unchanged.
- **Default feature set**: `text-index` is now on by default. Minimal
  embedded builds need an explicit `--no-default-features --features
  runtime-monoio,jemalloc,graph`.

## [0.1.11] — 2026-04-27

Hot-path perf release — eliminates two atomic-CAS hot paths in the write
dispatch loop discovered via ARM `perf annotate` on c4a-16 (GCloud Axion).
Empirically validated on the same hardware: **8-shard SET p=64 c=200
throughput 1.84M → 3.87M RPS (+110%)** when run with `--disk-offload disable`,
or **+15% under default flags**. No public API change.

Sprint 3.5a and 3.5b from `.planning/rfcs/v02-enterprise-architecture.md`.

### Performance — Sprint 3.5a: Lock-free `is_replica` mirror

`try_enforce_readonly` was taking `RwLock::try_read()` on
`Arc<RwLock<ReplicationState>>` for every command before dispatch — an
atomic CAS on the per-command hot path. ARM annotate showed `mov w8, #0xfffd;
cmp w11, w9` consuming **84% of self-time inside the function** (10% of
total CPU on 8-shard SET p=64).

Fix: replace the per-command lock probe with a single
`AtomicBool::load(Acquire)`.

- **`ReplicationState::is_replica_mirror: Arc<AtomicBool>`** — lock-free mirror
  of `role == Replica { .. }`, kept in sync via the new
  `ReplicationState::set_role(&mut self, role)` method (single owner of the
  invariant).
- **`ConnectionContext::is_replica_mirror: Option<Arc<AtomicBool>>`** —
  snapshotted from `ReplicationState` once at connection setup; per-command
  `try_enforce_readonly` is now just an atomic load with no lock
  acquisition.
- **All 6 production `rs_guard.role = ...` sites** in `handler_single`,
  `handler_monoio/dispatch`, and `handler_sharded/dispatch` migrated to
  `set_role()`. Test fixtures in `replication/handshake.rs` migrated too so
  the invariant holds in test code.
- Round-5 verification on commit `32f48c4` (c4a-16, 8-shard SET p=64 c=200):
  `try_enforce_readonly` is now **0%** of profile (down from 10%). Sprint
  3 acceptance criterion `<1%` met.

### Performance — Sprint 3.5b: WAL no-op bypass

`wal_append_and_fanout` was acquiring a `parking_lot::Mutex` (replication
backlog) and a `std::sync::RwLock` (replication state) on every write,
even when no replica was connected and no WAL writer existed. ARM annotate
showed `caslb`/`casab` ARM CAS-byte atomics dominating self-time (~21% of
total CPU on 8-shard SET p=64 with `--appendonly no` and zero replicas).

Fix: hoist a single early-return at the top of the function:

```rust
if wal_writer.is_none() && wal_v3_writer.is_none() && replica_txs.is_empty() {
    return;
}
```

The criterion is fully derivable from existing inputs — no new shared
state. Skips both the backlog `Mutex::lock` and the `repl_state`
`RwLock::read` on the cold path.

- **Round-5 verification with `--disk-offload disable`** (commit `32f48c4`):
  `wal_append_and_fanout` is now **0.05%** of profile (down from 21%).
  Sprint 3 acceptance criterion `<2%` met.
- **Operator note**: the bypass only fires when (a) `--appendonly no`,
  (b) `--disk-offload disable`, AND (c) no replicas are connected. Default
  builds with disk-offload on (the production default) keep
  `wal_v3_writer = Some(_)` and the function continues to do real WAL v3
  work — that path is unaffected.

### Throughput Impact

| Workload (8-shard SET p=64 c=200, c4a-16, frame pointers ON) | v0.1.10 | v0.1.11 | Δ |
|---|---|---|---|
| Default flags | 1.84M RPS | **2.11M RPS** | **+15%** |
| `--disk-offload disable` | ~2.95M RPS (projected) | **3.87M RPS sustained** | **+31%** |
| `try_enforce_readonly` self-time | 10.0% | **0%** | -10pp |
| `wal_append_and_fanout` self-time | 21.2% | **0.05%** (with disk-offload disable) | -21.1pp |

Production builds without frame pointers should clear 5.5–6.5M RPS at the
same flag set. Round-4 baseline data: `memory/benchmark_perf_round4_2026_04_27.md`.
Round-5 verification data: `memory/benchmark_perf_round5_2026_04_27.md`.

### Tests Added

- `replication::state::tests::test_set_role_updates_is_replica_mirror`
- `replication::state::tests::test_is_replica_mirror_default_false`
- `shard::spsc_handler::wal_append_tests::test_wal_append_bypass_when_no_writers_no_replicas`
- `shard::spsc_handler::wal_append_tests::test_wal_append_writes_backlog_when_replicas_present`

All 2450 lib tests passing locally (`cargo test --no-default-features
--features runtime-tokio,jemalloc --lib`); clippy clean (`cargo clippy
--no-default-features --features runtime-tokio,jemalloc -- -D warnings`).

### Drive-by Fixes

- **`src/shard/mod.rs`**: pre-existing test compile failure where two
  `drain_spsc_shared` call sites passed `&mut None` for `repl_backlog`
  instead of a `SharedBacklog` (`Arc<Mutex<Option<...>>>`). Fixed by
  constructing `Arc::new(parking_lot::Mutex::new(None))` in the test fixture.
  This was blocking `cargo test --lib` on `main` independent of this
  release.
- **`src/shard/dispatch.rs`**: pre-existing clippy `doc_lazy_continuation`
  error on the `TextSearchPayload` doc comment, blocking
  `cargo clippy -- -D warnings` on `main`.

### Compatibility

- **Wire protocol**: unchanged. Drop-in replacement for v0.1.10.
- **Public API**: only additive (`ReplicationState::set_role`,
  `ReplicationState::is_replica_mirror` field). No breaking changes.
- **Persistence on-disk format**: unchanged.
- **Replication wire format**: unchanged.

## [0.1.10] — 2026-04-23

Stable replication marker. **Single-shard PSYNC2 wired end-to-end and
production-ready** for `--shards 1` master with any `--shards N` replica
topology. Multi-shard master PSYNC is scheduled for v0.2 (see
`.planning/rfcs/multi-shard-replication-design.md`).

- **Replication** (`081c43b`): single-shard master PSYNC2 end-to-end wired,
  REPLCONF validated, `master_link_status` reports the actual handshake
  state instead of the legacy `up` stub.
- **Performance**: batch-level eviction gate; `try_handle_*` paths
  `#[inline]`-ed; DashTable carries through the v0.1.10 pre-size
  groundwork (capacity hint + headroom).
- **Docs**: BENCHMARK.md §2.7 updated with the 2026-04-22 GCloud
  re-measurement; v0.1.x replication scope documented under
  `docs/guides/clustering.mdx#replication`.

## [0.1.9] — 2026-04-19

**Lunaris Retriever Gap Closure.** Every v0.1.8 client-side fallback in
the Lunaris SDK is now closed so `HybridRRFRetriever` (dense path),
`GraphFirstRetriever`, and `PathReasoningRetriever` run Moon-native.

- **Phase 167 CYP-01/02**: Cypher `CREATE` / `MERGE` writes participate
  in `CrossStoreTxn` via `record_graph()`; `TXN.ABORT` rolls them back.
- **Phase 168 CYP-03/06**: `coalesce()` built-in + single-hop edge-var
  binding in variable-length `EXPAND`.
- **Phase 169 CYP-04/05**: `shortestPath()` parser + Dijkstra executor
  bridge with path-variable binding.
- **Phase 170 HYB-01/02/04**: `FT.SEARCH HYBRID` dense stream honours
  `as_of_lsn`.
- **Phase 171 SCAT-01/02/03**: `ShardMessage::VectorSearch` +
  `FtHybridPayload` carry `as_of_lsn` for multi-shard `AS_OF` correctness.
- **Phase 172 PIPE-01/02/03**: pipeline-aware HSET auto-indexing
  regression guard (3-test suite).

Audit status: **PASSED_WITH_DOCUMENTED_DEFERRALS**. 15 / 20 requirements
fully satisfied; HYB-03 BM25 MVCC deferred and closed in v0.1.10
follow-up (G-1); Phase 173 hygiene HYG-02 handler split RFC'd.

Stats: 6 phases shipped, 17 plans, 27 files changed, +2924 / −376 LOC.

## [0.1.8] — 2026-04-18

### Added — Cross-Store ACID Transactions (Phases 157, 161-163)

- **TXN.BEGIN**: Start a cross-store transaction — buffers KV, vector, and graph writes as intents.
- **TXN.COMMIT**: Commit all changes atomically with WAL record (`XactCommit` 0x34) for crash recovery.
- **TXN.ABORT**: Roll back all changes via undo-log replay with before-images.
- **KvWriteIntents**: Sparse MVCC side-table for uncommitted KV writes during transactions.
- **DeferredHnswInserts**: Vector index inserts deferred until commit, avoiding partial graph states.
- **UndoLog**: SmallVec-based KV rollback with before-image recording for all mutation types.
- **WAL transaction records**: `XactBegin` (0x33), `XactCommit` (0x34), `XactAbort` (0x37) with crash recovery replay.
- **Mutual exclusion**: `TXN` and `MULTI/EXEC` cannot be mixed (enforced at handler level).

### Added — Bi-Temporal MVCC (Phase 158)

- **TEMPORAL.SNAPSHOT_AT**: Record wall-clock → WAL LSN binding for point-in-time queries.
- **TEMPORAL.INVALIDATE**: Set `valid_to` on a graph entity (NODE or EDGE) for temporal visibility control.
- **FT.SEARCH AS_OF**: Query vector indexes at a historical timestamp via LSN resolution.
- **GRAPH.QUERY VALID_AT**: Execute Cypher queries against graph state valid at a specific timestamp.
- **TemporalRegistry**: BTreeMap-backed wall-clock → LSN mappings with O(log n) range lookups.
- **TemporalKvIndex**: Sparse versioned KV index with lazy initialization.
- **CSR segment format v2**: Bi-temporal `NodeMeta` with `valid_from`/`valid_to` fields.
- **WAL temporal records**: `TemporalUpsert` (0x35) and `GraphTemporal` (0x36) for crash recovery.

### Added — Workspace Partitioning (Phase 159)

- **WS CREATE**: Create a workspace with UUID v7 (time-ordered, 74-bit random). Name max 64 bytes.
- **WS DROP**: Delete a workspace and its registry entry.
- **WS AUTH**: Bind a connection to a workspace — all subsequent commands transparently prefixed.
- **WS INFO**: Return workspace metadata (name, creation timestamp).
- **WS LIST**: Enumerate all registered workspaces.
- **Transparent key rewriting**: `workspace_rewrite_args()` injects `{ws_hex}:` hash tag prefix on key arguments and strips it from responses.
- **WorkspaceRegistry**: Per-shard metadata with creation timestamps and WAL persistence.

### Added — Durable Message Queues (Phase 160)

- **MQ CREATE**: Create a durable queue with `MAXDELIVERY` (default 3) and `DEBOUNCE` options.
- **MQ PUSH**: Enqueue messages with field/value pairs (returns stream ID).
- **MQ POP**: Claim messages with optional `COUNT` (defaults to 1). Increments delivery counter.
- **MQ ACK**: Acknowledge messages by stream ID.
- **MQ DLQLEN**: Return dead-letter queue depth.
- **MQ TRIGGER**: Register debounced trigger callbacks with configurable debounce interval.
- **MQ PUBLISH**: Transactional enqueue within a `TXN` block — applied on `TXN COMMIT`.
- **Dead-letter queue**: Automatic DLQ at `{queue_key}::mq:dlq` after exceeding `MAXDELIVERY` attempts.
- **TriggerRegistry**: Debounced callback execution via pub/sub publish.
- **WAL recovery**: `replay_mq_wal()` with cursor rollback for durable queue state restoration.

### Added — Handler Parity

- All TXN/TEMPORAL/MQ/WS commands wired into `handler_monoio.rs`, `handler_sharded.rs`, `uring_handler.rs`, and `handler_single.rs`.
- 12 KV transaction integration tests, 14 MQ integration tests, workspace cross-shard dispatch tests.
- TEMPORAL/MQ/WS/TXN entries added to `test-commands.sh` and `test-consistency.sh`.

## [0.1.7] — 2026-04-17

### Added — BM25 Full-Text Search Engine (Phases 149-156)

- **BM25 inverted index**: Full-text search with multi-field boosting and per-field term frequency tracking.
- **TEXT field type**: Unicode tokenization with stemming and normalization in `FT.CREATE`.
- **TAG field type**: Categorical tag filtering with multi-value support (`@field:{val1|val2}`).
- **NUMERIC field type**: Range filtering (`@field:[min max]`) in `FT.CREATE` and `FT.SEARCH`.
- **FT.AGGREGATE**: Aggregation pipeline with `GROUPBY`/`REDUCE`, scatter-gather across shards, HLL `COUNT_DISTINCT`.
- **Three-way RRF hybrid fusion**: Combines BM25 + dense vector + sparse vector results via Reciprocal Rank Fusion.
- **Typo tolerance**: FST Levenshtein fuzzy matching (`%%term%%`) and prefix search (`term*`).
- **HIGHLIGHT/SUMMARIZE**: Post-processors for formatting search results with matched term highlighting.
- **Multi-shard DFS global IDF**: Distributed frequency statistics for accurate BM25 scoring regardless of shard count.
- **FT.DROPINDEX DD**: Atomic index + document deletion flag — deletes all hash keys matching index prefixes.
- **Python SDK text module**: `client.text.text_search()`, `client.text.aggregate()`, `client.text.hybrid_search()` with typed pipeline DSL.
- **LangChain/LlamaIndex hybrid adapters**: Framework integrations updated for three-way hybrid search.

### Statistics

- 8 phases (149-156), 27 plans, 26 requirements, 122 commits.

## [0.1.6] — 2026-04-15

### Added — AI-Native Data Primitives

- **Multi-field vector indexes** (`FT.CREATE`): multiple VECTOR fields per index, per-field segment storage, field-targeted `@field_name` syntax in `FT.SEARCH` KNN clause.
- **Sparse vector module** (`src/vector/sparse/`): inverted index with `SparseStore`, enabling BM25-style sparse retrieval alongside dense HNSW.
- **Hybrid dense+sparse search**: `SPARSE` clause in `FT.SEARCH` with Reciprocal Rank Fusion (RRF) for combining dense and sparse results.
- **Text index** (`src/vector/text_index.rs`): Unicode tokenization pipeline with stemming and normalization, feature-gated under `text-index`.
- **Boolean and geo filter expressions**: `BoolEq` and `GeoRadius` filter variants with evaluation logic in `FilterExpr`.
- **FT.RECOMMEND**: centroid-based recommendation over vector indexes — computes centroid of seed vectors and returns nearest neighbors.
- **FT.NAVIGATE**: multi-hop knowledge graph navigation from vector search results, bridging vector and graph queries.
- **FT.EXPAND**: GraphRAG expansion command — traverses graph edges from vector search results to discover related entities, with configurable depth.
- **FT.CACHESEARCH**: semantic cache-or-search command — returns cached results on similarity hit, falls back to full search on miss.
- **FT.CONFIG SET/GET**: runtime configuration for per-index knobs (e.g., `AUTOCOMPACT` toggle).
- **SESSION clause**: session-scoped filtering in `FT.SEARCH` for multi-tenant and agent memory isolation.
- **RANGE threshold post-filter**: distance threshold filtering in `FT.SEARCH` results.
- **LIMIT pagination**: `LIMIT offset count` support in `FT.SEARCH` with multi-segment merge.

### Added — Production Infrastructure

- **moondb Python SDK** (`python/moondb/`): high-level client with vector, graph, session, cache, and framework integrations (LangChain, LlamaIndex).
- **5 quickstart examples**: RAG, semantic cache, GraphRAG, AI agent tools, memory engine — all using real MiniLM embeddings.
- **Production deployment guide** (`docs/production-guide.md`): configuration reference, TLS setup, monitoring, ACL, tuning.
- **Dockerfile improvements**: OCI labels, admin port exposure, production defaults.
- **docker-compose.yml**: production configuration with resource limits, health checks, ulimits.
- **Prometheus metrics**: counters and histograms for v0.1.6 commands (FT.RECOMMEND, FT.NAVIGATE, FT.EXPAND, FT.CACHESEARCH, FT.CONFIG).
- **OpenTelemetry stubs**: `otel` feature flag with tracing infrastructure for future OTLP export.
- **Benchmark script** (`scripts/bench-v0.1.6.sh`): automated benchmarks for new vector search features.

### Added — Graph-Vector Integration

- **EXPAND GRAPH clause** in `FT.SEARCH`: inline graph expansion during vector search with configurable depth.
- **`graph_expand.rs` bridge module**: connects vector search results to graph traversal engine.
- **`key_to_node` mapping** on `NamedGraph`: enables graph expansion from vector search key hashes.

### Fixed — Critical Production Bugs

- **Deadlock in cross-shard HSET auto-index**: `parking_lot::Mutex` re-entry in `PipelineBatch` and `PipelineBatchSlotted` handlers — `shard_databases.vector_store(shard_id)` attempted to re-lock a non-reentrant mutex already held by the caller. Fixed by using the passed-in reference.
- **Auto-index HSET inside MULTI/EXEC**: vector auto-indexing now works correctly within transactions and pipeline batch paths.
- **FT.RECOMMEND filter bug**: filter expressions were not applied correctly to recommendation results.
- **FT.CONFIG/RECOMMEND/NAVIGATE/EXPAND routing**: commands now dispatched correctly in all connection handlers (monoio, tokio, single-threaded).
- **Session deduplication**: fixed duplicate session entries in search results.
- **Inline filter parsing**: corrected parsing of filter expressions in inline command mode.
- **FT.COMPACT, FT.CONFIG, FT.CACHESEARCH metadata**: registered in phf command metadata table for ACL and COMMAND DOCS.

### Fixed — CI/Release Pipeline

- **nfpm download URL**: pinned v2.46.1 with correct `Linux_x86_64` asset name (old `nfpm_linux_amd64.tar.gz` returns 404).
- **SBOM filenames**: `cargo-cyclonedx` v0.5+ writes `.json` not `.cdx.json`.
- **Cosign keyless signing**: added `id-token: write` permission for Sigstore OIDC (was falling back to interactive device flow).
- **Package job race condition**: deb/rpm upload now waits for GitHub release to be created first.
- **upload-artifact**: upgraded v4 → v7 (Node.js 20 deprecation).
- **Test compilation**: added tokio dev-dependency with `process` feature for `blocking_list_timeout.rs` under default (monoio) features.

### Changed

- **Graph enabled by default**: `graph` feature now included in default feature set.
- **Vector index persistence**: v2 format with backward-compatible v1 migration.
- **Memory engine example**: rewritten as 142-line script with real MiniLM embeddings (was 487-line complex agent loop).
- **Clippy 1.94 compliance**: all warnings resolved for Rust 1.94 MSRV.

### Validation

- 2,613+ unit tests pass (release mode, default features).
- 2,139 library tests pass under `runtime-tokio` feature set.
- 184 unsafe blocks, all with SAFETY comments (audit pass).
- 0 unannotated unwraps on hot paths (ratchet pass).
- Zero clippy warnings (default + `runtime-tokio,jemalloc` feature sets).
- `cargo fmt --check` clean.
- 8 fuzz targets in CI.
- Full release pipeline validated: 6 binary targets, Docker image, deb/rpm packages, SBOMs, cosign signatures.

## [0.1.5] — 2026-04-12

### Added — Moon Console (Interactive Data Client)

- **HTTP/WebSocket gateway** (`src/admin/`): REST endpoints (`/api/v1/info`, `/api/v1/command`, `/api/v1/keys`, `/api/v1/key/*`, `/api/v1/memory/treemap`, `/api/v1/hnsw/trace`), WebSocket-to-RESP3 bridge at `/ws/console`, SSE metrics stream at `/sse/metrics` (1 Hz), CORS allowlist, per-IP token-bucket rate limit, HMAC-SHA256 Bearer auth, HTTP/2 support, static file serving via `rust-embed`.
- **React 19 console** (`console/`): 7-view SPA (Dashboard, Browser, Console, Vector Explorer, Graph Explorer, Memory, Help) served at `/ui/`. 50.9 KB gzipped initial bundle (6× under 300 KB target) via Vite 8 + manual chunk splitting (Three.js/Monaco/Recharts lazy).
- **Real-time Dashboard**: 7 widgets (QPS, latency P50/P99, memory, clients, ops by type, keyspace) driven by SSE stream.
- **KV Data Browser**: namespace tree, virtual-scrolled key list (TanStack Virtual), type-specific editors for Strings/Hashes/Lists/Sets/Sorted Sets/Streams, TTL display + edit, bulk delete with toasts.
- **Query Console**: Monaco editor with RESP + Cypher Monarch syntax, 233-command auto-complete, multi-tab, history, Cmd+Enter (current line) / Cmd+Shift+Enter (whole buffer), line-by-line execution for paste safety.
- **Vector 3D Explorer**: UMAP projection in a web worker, HNSW layer overlay, KNN search with distance rings, lasso selection, Three.js r183 + React Three Fiber.
- **Graph 3D Explorer**: force-directed layout (d3-force-3d worker), Cypher editor, node/edge property inspector, hybrid query integration.
- **Memory view**: keyspace treemap (server-aggregated `/api/v1/memory/treemap`), slowlog table, command stats.
- **Built-in Help guide**: 427-line Getting Started tutorial with seed examples.
- **Core admin commands** (`src/command/server_admin.rs`): FLUSHALL, FLUSHDB, DBSIZE, DEBUG OBJECT/SLEEP/JMAP, MEMORY USAGE — closing pre-existing dispatch gaps.
- **Multi-shard SCAN fan-out** (`src/admin/scan_fanout.rs`): composite cursor `{shard_id}:{cursor}` so Browser sees unified keyspace.
- **Frontend test infrastructure**: 56 Vitest unit tests + 9 Playwright E2E specs. New `scripts/test-integration.sh` harness and `.github/workflows/console-integration.yml`.
- **Admin-port hardening** (`src/admin/{auth,cors,rate_limit,middleware}.rs`): Bearer auth, CORS allowlist, per-IP rate limit.

### Fixed

- **WebSocket request ID echo**: errors now echo back the client's `id`, preventing client-side promise timeouts on malformed input.
- **Console type badges**: `execCommand` response was returning the full `{result, type}` envelope; now unwraps `.result` correctly.
- **Multi-line paste in Console**: Cmd+Enter now executes the current line only (redis-cli paste behavior). Cmd+Shift+Enter executes the whole buffer line-by-line.

### Validation

- 101+ Rust unit/integration tests pass on both `runtime-tokio` and `runtime-monoio`.
- 56 Vitest tests + 9 Playwright specs pass.
- Zero clippy warnings (default + `runtime-tokio,jemalloc` feature sets).
- `cargo fmt --check` clean.
- 8 fuzz targets in CI.

## [0.1.4] — 2026-04-11

### Added — Graph Engine Integration (v0.1.4, 2026-04-11)

- **Property graph engine** (`src/graph/`, feature-gated under `graph`): segment-aligned CSR storage with SlotMap generational indices, ArcSwap lock-free reads, Roaring validity bitmaps, and Rabbit Order compaction for cache locality. 8,500+ LOC, 319 tests.
- **12 GRAPH.\* commands**: CREATE, ADDNODE, ADDEDGE, NEIGHBORS, QUERY, RO_QUERY, EXPLAIN, VSEARCH, HYBRID, INFO, LIST, DELETE — all with RESP3 Map responses and ACL annotations.
- **Cypher subset parser**: hand-rolled recursive descent with logos lexer, 12 clauses (MATCH/WHERE/RETURN/CREATE/DELETE/SET/MERGE/WITH/UNWIND/CALL/ORDER/LIMIT), parameterized queries ($param), nesting depth limit (64), plan caching.
- **Hybrid graph+vector queries**: graph-filtered vector search, vector-to-graph expansion, vector-guided walk with automatic strategy selection.
- **Traversal engine**: BFS/DFS/Dijkstra with bounded frontiers (100K cap), temporal decay + distance scoring, segment merge reader across mutable + immutable segments.
- **Graph indexes**: per-label/type Roaring bitmaps, boomphf minimal perfect hash (~3 bits/key), property B-tree for range queries.
- **Cross-shard traversal**: scatter-gather via SPSC mesh, graph hash tags for shard co-location, snapshot-LSN forwarding, configurable depth limit.
- **Graph MVCC**: extends existing TransactionManager with graph write intents, snapshot-isolated multi-hop traversal, bounded epoch hold (30s).
- **Graph WAL durability**: RESP-encoded graph commands in per-shard WAL, two-pass replay (nodes before edges), CRC32-validated CSR segment persistence.
- **Cost-based planner**: GraphStats with incremental degree tracking, graph-first vs vector-first strategy selection, P99 hub detection.
- **Criterion benchmarks**: CSR 1-hop 1.02ns, edge insert 64.8ns, 2-hop BFS 4.99µs, CSR freeze 5.12ms, SIMD cosine 384d 33.9ns.
- **Fair comparison benchmark** (`tests/graph_bench_compare.rs`): Moon 2.4x FalkorDB on Cypher MATCH, 19x on native 1-hop, 23x on population.
- **New dependencies**: `slotmap` 1.x (generational indices), `boomphf` 0.6 (MPH), `logos` 0.14 (Cypher lexer, optional).

### Added — High-Impact Redis Command Parity (2026-04-10)

- **COPY command** — atomic key duplication with DESTINATION, REPLACE options (Redis 6.2+).
- **Bit operations** — GETBIT, SETBIT, BITCOUNT (byte/bit range modes), BITOP (AND/OR/XOR/NOT), BITPOS (byte/bit range modes) with read-only dispatch variants.
- **SORT command** — full BY/GET/LIMIT/ALPHA/ASC/DESC/STORE support for lists, sets, and sorted sets.
- **Geospatial commands** — GEOADD (NX/XX/CH), GEOPOS, GEODIST (M/KM/FT/MI), GEOHASH (11-char base32), GEOSEARCH (FROMLONLAT/FROMMEMBER, BYRADIUS/BYBOX, WITHCOORD/WITHDIST/WITHHASH), GEOSEARCHSTORE.
- **CONFIG REWRITE** — atomic write of runtime config to `<dir>/moon.conf` (tmpfile + rename). CONFIG RESETSTAT stub.
- **CLIENT PAUSE/UNPAUSE** — delays command processing with WRITE-only mode support. CLIENT INFO, CLIENT LIST (stub), CLIENT NO-EVICT/NO-TOUCH accepted.
- **MEMORY USAGE/DOCTOR/HELP** — key memory estimation via `estimate_memory()`.
- **Lazyfree threshold** — configurable via `CONFIG SET lazyfree-threshold N` (default 64).
- **GETBIT/SETBIT metadata** — added to PHF command registry.
- **GEOADD/GEOSEARCHSTORE** — added to AOF write commands test list.
- **EXPIREAT/PEXPIREAT** — absolute Unix timestamp expiry (seconds/milliseconds).
- **EXPIRETIME/PEXPIRETIME** — read back absolute expiry timestamp.
- **FLUSHDB/FLUSHALL** — clear all keys in current database.
- **TIME** — server clock as `[seconds, microseconds]`.
- **RANDOMKEY** — return a random key from the database.
- **TOUCH** — refresh LRU/LFU access time without reading value.
- **SHUTDOWN** — dispatch entry (graceful stop via signal handler).
- **BITFIELD** — GET/SET/INCRBY with type specifiers (u8/i16/u32/...), OVERFLOW WRAP/SAT/FAIL.
- **LCS** — Longest Common Substring with LEN option.
- **XSETID** — set stream last-delivered ID without adding entries.
- **GEORADIUS/GEORADIUSBYMEMBER** — deprecated wrappers translating to GEOSEARCH.
- **OBJECT FREQ/IDLETIME/REFCOUNT** — LFU counter, idle seconds, reference count introspection.
- **LOLWUT** — Easter egg returning Moon version.

### Added — Client Connection Security Hardening (2026-04-10)

- **`--maxclients` (P0):** Connection limit with atomic CAS rejection (default 10000, 0=unlimited). Returns `-ERR max number of clients reached` when exceeded.
- **`--timeout` (P0):** Client idle timeout in seconds (default 0=disabled). Disconnects idle clients via `tokio::time::timeout` / `monoio::select!`.
- **`--tcp-keepalive` (P0):** TCP keepalive interval (default 300s, 0=disabled). Sets `SO_KEEPALIVE` + `TCP_KEEPIDLE` on accepted sockets via `socket2`.
- **AUTH rate limiting (P0):** Per-IP exponential backoff on AUTH failures (100ms base, 10s cap, 60s auto-reset). New module `src/auth_ratelimit.rs`.
- **CLIENT LIST / INFO / KILL (P1):** Global client registry with Drop-guard deregister. Redis-compatible output format. Kill by ID/ADDR/USER. New module `src/client_registry.rs`.
- **CLIENT PAUSE / UNPAUSE (P1):** Server-wide pause with ALL/WRITE modes and auto-expiry. New module `src/client_pause.rs`.
- **CLIENT NO-EVICT / NO-TOUCH (P1):** Accepted stubs for Redis compatibility.
- **ACL GENPASS (P1):** Cryptographically secure random password generation (1-4096 bits, hex output).
- **CONFIG GET/SET** support for `maxclients`, `timeout`, `tcp-keepalive` (runtime-mutable).
- **Monoio connection tracking:** Added missing `record_connection_opened` / `record_connection_closed` for accurate `connected_clients` metric.

### Fixed — Deep Review Findings (2026-04-11)

- **DoS protection**: `execute_profile` and `execute_mut` Cypher paths now enforce MAX_HOPS_LIMIT=20 and MAX_RESULT_ROWS=100K (were unbounded).
- **WAL correctness**: Cypher DELETE passes actual LSN to `remove_node`/`remove_edge` (was hardcoded to 0).
- **GRAPH.DROP metadata**: added missing phf dispatch table entry.
- **SAFETY comments**: added to all 7 unsafe SIMD/mmap functions.
- **BFS 30% faster**: scratch buffer reuse in SegmentMergeReader, zero-alloc CsrStorage callback, MergedNeighbor derives Copy.
- **ParallelBfs**: uses plain HashSet on sequential path (was DashSet with 64 shards overhead).
- **Recovery hardening**: CSR manifest path traversal validation, WAL embedding dimension cap (65536), LSN saturating_add.
- **CI optimized**: consolidated 26 jobs → 4 per PR, concurrency groups cancel superseded runs, fixed org runner group for public repos.

### Fixed — Wave 0-4 Gap Closure (2026-04-09)

- **ZREVRANGEBYSCORE/ZREVRANGEBYLEX correctness bug:** Fixed double-swap of min/max bounds in `zrange_by_score` and `zrange_by_lex` that caused empty results for finite score ranges (e.g., `ZREVRANGEBYSCORE key 3 1`). Added finite-range test to `test-commands.sh`.
- **INFO command enriched:** Clients section now reports `connected_clients`, Memory section reports `used_memory`/`used_memory_human`/`used_memory_rss` (from `/proc/self/status`), Replication section wired to actual `ReplicationState` (role, connected_slaves, master_replid, master_repl_offset).
- **Tracing spans:** Added `#[instrument]` to connection handlers (single, monoio), replication master (tokio, monoio), HNSW compaction, and AOF rewrite — 6 new spans.
- **Replication lag metric wired:** `moon_replication_lag_bytes` Prometheus gauge now updated from `get_replication_info()`.
- **CI supply chain security:** `cargo deny check` + `cargo audit` added to CI pipeline (deny.toml was previously unenforced).
- **Release pipeline:** aarch64-unknown-linux-gnu build added via `cross` for primary production target.
- **Crash matrix expanded:** BGSAVE and BGREWRITEAOF crash cells added (6/7 coverage).
- **Compatibility tests expanded:** Stream (XADD/XLEN/XRANGE/XTRIM), Lua scripting (EVAL/EVALSHA/SCRIPT), and ACL (WHOAMI/LIST) tests added to `redis_compat.rs`.

### Added — Production Contract (Phase 87, 2026-04-08)

- **`docs/PRODUCTION-CONTRACT.md`** — Moon's v1.0 promises: per-command-class SLOs (provisional until Phase 97), supported platform matrix (Linux aarch64 primary, Linux x86_64 secondary contingent on `PERF-04`, macOS dev-only via OrbStack), durability mode semantics per `appendfsync` × failure-class, availability & replication guarantees, security guarantees, explicit out-of-scope list, and a machine-checkable GA Exit Criteria checklist that every v0.1.3 phase ticks off. This is the contract every downstream hardening phase (88–100) tests against.
- **`docs/runbooks/`** — stub directory for operator runbooks authored in Phase 99 (`REL-05`).

### Changed — Toolchain Upgrade (Phase 88, 2026-04-08)

- **MSRV bumped from Rust 1.85 to 1.94.0.** `rust-toolchain.toml` committed so fresh clones auto-install the pinned version; CI workflows (`ci.yml`, `codeql.yml`, `release.yml`) and OrbStack `moon-dev` VM provisioning in `CLAUDE.md` updated. No language/runtime behavior change; downstream phases benefit from new clippy lints and std/compiler improvements. Contributors must run `rustup update` on next pull.

### Added — Production Readiness Phases 92-105 (2026-04-09)

- **Observability:** Prometheus `/metrics` on `--admin-port`, SLOWLOG GET/LEN/RESET/HELP, HEALTHZ + READYZ commands, `/healthz` + `/readyz` HTTP endpoints, INFO extended with Server/Clients/Memory/Stats/CPU sections, `--check-config` flag, per-command latency histograms + connection metrics wired into dispatch
- **Durability proof:** Crash-injection test matrix, torn-write WAL v3 tests (CRC32C validated), Jepsen-lite linearizability harness, backup/restore workflow test
- **Replication hardening:** PSYNC partial resync, full resync, network partition, kill-restart, replica promotion tests
- **Client compatibility:** CI matrix (redis-py, go-redis, jedis, ioredis, node-redis, redis-rs, hiredis), 24 Redis compat tests, vector client smoke script, `docs/redis-compat.md`
- **Performance gates:** Criterion regression CI with baseline caching, RSS-per-key memory gate script
- **Security hardening:** `deny.toml` (cargo-deny), `SECURITY.md`, `docs/THREAT-MODEL.md`, `docs/security/lua-sandbox.md`, TLS cipher suite freeze
- **Release engineering:** `docs/versioning.md`, 6 operator runbooks, CHANGELOG CI gate, user docs (getting-started, configuration, monitoring), release pipeline SHA256 checksums + SBOM + cosign

## [0.1.3] — 2026-04-10

Production-readiness foundation: dispatch hot-path recovery, vector-search
4× QPS + correctness fixes, and the tiered disk-offload landing with 100 %
crash recovery across 7 persistence configurations. Bundles three work
streams originally tracked as separate Unreleased blocks (Apr 7–8).

### Dispatch Hot-Path Recovery (2026-04-08)

**Pipelined SET +37%, pipelined GET +68% at p=16 after PR #43 regression recovery.**

Three targeted perf fixes landed after flamegraph-driven analysis of pipelined
SET on aarch64 (OrbStack moon-dev, 1 shard, default config, redis-benchmark
-c 50 -n 3M -P 16 -r 100000 -d 64):

| Metric                  | Broken baseline | After T0a+T0b+T0c | Δ      |
|-------------------------|----------------:|------------------:|-------:|
| SET p=1   (ratio Redis) | 0.99x           | **1.12x**         | +13pp  |
| SET p=16                | 1.42M/s         | **1.94M/s**       | +37%   |
| SET p=32                | 2.06M/s         | **2.26M/s**       | +10%   |
| GET p=16                | 2.40M/s         | **4.04M/s**       | +68%   |
| GET p=128 vs Redis      | 1.87x           | **1.91x**         | +4pp   |

### Perf fixes

- **T0a — Thread-local cached clock** (4041b0d). `Entry::new_*` constructors
  were calling `SystemTime::now()` / `clock_gettime` on every write, showing up
  at **10.14% of CPU** in the perf profile. Added a thread-local `Cell<u32>` /
  `Cell<u64>` refreshed once per shard tick (~1 ms) from `CachedClock::update()`.
  `current_secs` / `current_time_ms` now read the Cell and fall back to the
  syscall only on tests / cold init. `__kernel_clock_gettime` dropped from
  10.14% → **0%** of CPU.

- **T0b — Hot command dispatch bypasses phf SipHasher** (4b0eec3). The command
  metadata registry is a `phf::Map` keyed by `&'static str` using `SipHasher` —
  cryptographic overkill for a 173-entry ASCII table. Combined `phf::Map::get`
  + `SipHasher::write` + `hash_one` was **~6% of CPU**. Added a direct match
  path in `command::metadata::lookup`: pack the first ≤8 bytes of the command
  name as a `u64` with ASCII letters uppercased, match against 24 hand-picked
  hot commands (GET/SET/DEL/TTL/MGET/MSET/INCR/DECR/HSET/HGET/HDEL/HLEN/LPOP/
  RPOP/LLEN/PING/LPUSH/RPUSH/EXPIRE/EXISTS/INCRBY/DECRBY/SELECT/HGETALL).
  Hot-path resolves through a pre-resolved `LazyLock<[&'static CommandMeta; 24]>`
  — single array index, no hashing. Cold commands fall through to phf unchanged.
  Correctness asserted by `hot_path_matches_phf_map` test: every hot entry must
  return the same `&'static` pointer as a direct phf probe, in both upper and
  lowercase.

- **T0c — ACL unrestricted-user short-circuit** (4603511). Every command
  executed `check_command_permission` + `check_key_permission` even for the
  default `on nopass ~* &* +@all` user, burning **2.11% of CPU** on
  lowercasing, `extract_command_keys`, and glob matching. Added a cached
  `unrestricted: bool` field to `AclUser`, true iff the user is enabled, has
  `AllAllowed` commands, only `~*` read/write key patterns, and only `*`
  channel patterns. The three `check_*_permission` methods early-return `None`
  on `unrestricted` before any allocation or iteration. The cache is
  recomputed once at the end of `apply_rule` (the single mutation entry point
  used by ACL SETUSER / LOAD / reset). Correctness covered by three new tests
  (`default_user_is_unrestricted`, `restrictions_clear_unrestricted_flag`,
  `unrestricted_user_passes_all_checks`).

### Correctness fix (PR #43 review)

- **Inline monoio fast-path restricted to GET** (613c164). The previous inline
  dispatch in `try_inline_dispatch` handled both GET and SET directly against
  the DashTable, bypassing replica READONLY enforcement, ACL checks, maxmemory
  eviction, client-side tracking invalidation, keyspace notifications,
  replication propagation, and blocking-waiter wakeups. Under any of those
  configurations the inlined SET would silently diverge from the normal path —
  accepted writes on replicas, ACL-denied clients writing, maxmemory overshoot,
  stale client-side caches. Fix: inline only handles `*2\r\n$3\r\nGET` now;
  SET and everything else fall through to the full dispatcher where all
  side-effects run.

### Cold-tier lock hygiene (PR #43 review)

- **Release shard read guard before cold-tier disk read** (ff51135). The
  cold-tier fallback in `server::conn::blocking` previously called
  `get_cold_value()` — which does a synchronous `std::fs::read()` — while still
  holding the per-shard read guard, blocking all concurrent operations on that
  shard during disk I/O. Split the path: `Database::cold_lookup_location`
  returns the `(ColdLocation, PathBuf)` under the lock, the guard is dropped,
  and `cold_read::read_cold_entry_at` performs the disk read unlocked.

### Additional PR #43 fixes

- `read_overflow_chain` now bounded at 1000 iterations (cycle guard against
  corrupted `next_page` links)
- `recovery.rs` FPI replay replaces `.unwrap()` on `try_into()` with explicit
  byte-array construction (coding-guidelines compliance)
- `bench-production.sh`: fixed unsupported `-t zrangebyscore` (→ `zpopmin`),
  MSET rps parser for `"MSET (10 keys):"` output, heredoc `$(date)` expansion,
  and Redis RSS probe (`pgrep`/`/proc` instead of missing `lsof`)
- `bench-cold-tier.sh`: removed stray `&` backgrounding `FT.CREATE`
- `test-recovery-all-cases.sh`: `NoPersistence` case now PASSes at 0 keys
- `benches/resp_parsing.rs`, `benches/get_hotpath.rs`: wrap `Vec<Frame>` in
  `FrameVec` via `.into()` after frame.rs type change

All 1872 unit tests pass under `--no-default-features --features
runtime-tokio,jemalloc`. Follow-up work (T1 `dispatch_raw` zero-alloc entry
point, Tier 2 storage/DashTable optimization, residual ACL SipHash elimination)
captured as todo in `.planning/todos/pending/`.

---

### Vector Search 4× QPS + Correctness (2026-04-07)

**4x search QPS, 4.1x lower latency, 2.56x faster than Qdrant on real MiniLM data.**

#### Performance (perf-profiled on GCloud c3-standard-8, Intel Xeon 8481C)
- 8-wide ILP unrolled `dist_bfs_budgeted` subcent path (the real hot loop, 90% of
  search time per perf profile). Loads 4 code bytes + 1 sign byte per iteration,
  8 independent f32 accumulators. Confirmed via objdump: parallel `vaddss` into
  xmm3-xmm8 (vs serial single-xmm0 chain before).
- 4-way unrolled `dist_bfs` non-subcent path with `unsafe` pointer arithmetic
- Pre-allocated ADC LUT in `SearchScratch` (eliminates 32-65KB heap alloc per query)
- Hoisted IVF `q_rotated` and `lut_buf` allocation out of per-segment loop

#### Correctness fixes
- **`FT.COMPACT` silent no-op**: split `try_compact` (threshold-gated) from
  `force_compact` (unconditional). Previously `FT.COMPACT` returned OK without
  compacting when `compact_threshold >= mutable_len`, leaving all vectors in
  brute-force O(n) mutable segment.
- **`key_hash_to_key` mapping restored** (lost in earlier refactor). `FT.SEARCH`
  now returns original Redis keys (`doc:N`) instead of `vec:<internal_id>`.
  Carried through `SearchResult.key_hash` and populated by `remap_to_global_ids`.
- **`FT.INFO num_docs`** now sums mutable + immutable segments (was 0 after compact)
- **Vector index recovery** metadata loads without `--disk-offload` flag
  (was gated behind `server_config.disk_offload_enabled()`)

#### Real MiniLM benchmarks (10K vectors, 384d, x86 Xeon 8481C)

| Metric | Mar 31 (M4 Pro) | Apr 7 (Xeon 8481C) | Δ |
|--------|---:|---:|---:|
| Recall@10 | 0.9250 | **0.9670** | +4.5% |
| QPS | 1,126 | **1,296** | +15% |
| p50 | 0.878 ms | **0.783 ms** | -11% |

| | Moon | Qdrant 1.12 FP32 | Ratio |
|---|---:|---:|---:|
| QPS (10K MiniLM) | 1,296 | 507 | **2.56x** |
| p50 | 0.783 ms | 1.79 ms | **2.29x lower** |
| Recall@10 | 0.967 | ~0.95 | **+1.7%** |

#### Infrastructure (for future segment merge work)
- `ImmutableSegment::decode_vector` / `iter_live_decoded`
- `MutableSegment::iter_live`

#### Attempted and reverted
Segment merge on `FT.COMPACT` via TQ4 decode → re-encode. Dropped recall from
0.73 → 0.0005 due to accumulated quantization error across 14 segments. Proper
fix requires retaining f32/f16 vectors alongside TQ codes in immutable segments.

#### Known limitation
TQ4 quantization at 384d with random Gaussian inputs hits ~0.73 recall floor
(curse of dimensionality — all points nearly equidistant). Real semantic
embeddings (clustered) achieve 0.92-0.97 recall with the same code.

---

### Disk Offload & x86_64 Performance (2026-04-06)

Tiered storage, crash recovery, and 2× Redis on x86_64 (Intel Xeon, io_uring).

### Added

#### Disk Offload (Tiered Storage)
- `--disk-offload enable` — evicted keys under maxmemory are spilled to NVMe instead of being deleted
- Async SpillThread: background pwrite via dedicated `std::thread` per shard (no event loop blocking)
- Cold read-through: GET transparently reads spilled keys from NVMe DataFiles
- ColdIndex: in-memory key→file mapping, updated immediately on eviction for consistent reads
- SpillThread channel capacity: 4096 bounded flume channel for burst absorption
- `--disk-offload-dir`, `--disk-offload-threshold` configuration flags

#### Crash Recovery
- V3 recovery falls back to appendonly.aof when WAL v3 has 0 commands
- V2 recovery falls back to appendonly.aof when shard WAL has 0 commands
- Automatic `--dir` creation before AOF writer starts (fixes silent write failure)
- Cold index rebuilt from manifest during v3 recovery
- Verified: 100% recovery (5000/5000 keys) across 7 persistence configurations after SIGKILL

#### Inline GET Optimization
- `read_db` + `get_if_alive` replaces `write_db` + triple-lookup `get()` — single DashTable probe
- Removed unnecessary write lock for timestamp refresh before inline dispatch
- Multi-shard inline dispatch: local keys bypass Frame construction via `key_to_shard()` check
- Cold storage fallback in `get_readonly` and inline GET dispatch paths

### Changed

- Connection handler eviction uses `try_evict_if_needed_async_spill` when disk offload enabled
- `spawn_monoio_connection` passes spill sender, file ID counter, and offload dir to handlers
- Event loop syncs `next_file_id` between `Rc<Cell<u64>>` (handlers) and local variable (timer tick)
- Inline dispatch `try_inline_dispatch` takes `now_ms` and `num_shards` parameters

### Fixed

- **Data loss under maxmemory**: evicted keys were silently deleted instead of spilled to disk (6 bugs)
- **Crash recovery = 0 keys**: appendonly.aof never tried as fallback source
- **AOF writer silent failure**: `--dir` directory not created before AOF writer task started
- **Cold read miss**: `get_if_alive` (read path) didn't check cold storage; `get_readonly` returned NULL for spilled keys
- **ColdIndex never initialized**: `cold_index` and `cold_shard_dir` were None on all databases at startup

### Performance (GCP c3-standard-8, Intel Xeon 8481C, CPU-pinned)

| Metric | Before | After |
|--------|--------|-------|
| c=1 p=1 GET vs Redis | 0.35x (47K) | **1.0x (47K)** — parity |
| c=10 p=64 GET | 2.29M | **4.71M** (2.06x Redis) |
| c=50 p=64 GET | 2.36M | **4.81M** (2.04x Redis) |
| Disk offload GET overhead | N/A | **<1%** vs no-persist |
| Recovery (SIGKILL) | 0/5000 | **5000/5000** (100%) |

---

## [0.1.2] - 2026-03-29

Multi-shard scaling milestone. Eliminated negative scaling, achieving 5M GET/s and 2.5M SET/s at 4 shards — both exceeding Redis 8.6.1.

### Added

#### Shared-Read Direct Access (Phase 49)
- `Arc<ShardDatabases>` with `parking_lot::RwLock<Database>` replaces `Rc<RefCell<Vec<Database>>>`
- Cross-shard read commands (GET, HGET, SCARD, ZRANGE, etc.) bypass SPSC channels entirely via `read_db()` + `dispatch_read()` — reduces cross-shard read latency from ~88μs to ~56ns
- Local read path uses shared `read_db()` lock instead of exclusive `write_db()` — eliminates RwLock contention between shards

#### Connection Affinity (Phase 50)
- `AffinityTracker` samples first 16 commands per connection to detect dominant shard
- Lazy FD migration: if ≥60% of keys target a non-local shard, migrates the TCP connection's file descriptor to the target shard via `ShardMessage::MigrateConnection`
- `MigratedConnectionState` preserves selected_db, client_name, protocol_version across migration
- Graceful fallback: if migration fails, connection stays on current shard with shared-read

#### Pre-Allocated Response Slots (Phase 51)
- `ResponseSlotPool` with lock-free `AtomicU8` state machine for zero-allocation cross-shard write dispatch (Tokio path)
- Eliminates per-dispatch `channel::oneshot()` heap allocation (~80-120ns savings per cross-shard write)

#### SO_REUSEPORT Per-Shard Accept (Phase 52)
- Each shard opens its own TCP listener with `SO_REUSEPORT` on Linux via `socket2` crate
- Kernel distributes connections across shard listeners using consistent 4-tuple hashing
- macOS/non-Linux: falls back to single-listener + MPSC round-robin (no behavior change)

#### jemalloc Production Tuning (Phase 53)
- `malloc_conf` static: `percpu_arena:percpu`, `background_thread:true`, `metadata_thp:auto`, `dirty_decay_ms:5000`, `muzzy_decay_ms:30000`, `abort_conf:true`
- Closes ~50% of allocation speed gap with mimalloc while retaining jemalloc's superior fragmentation behavior

#### New Commands (Phase 55)
- GETRANGE — return substring of stored string value
- SETRANGE — overwrite part of stored string at offset with zero-fill
- SUBSTR — alias for GETRANGE (Redis 1.x compatibility)

### Changed

- Custom `AtomicU8` oneshot channel replaced with `flume::bounded(1)` for cross-thread safety on monoio's `!Send` executor
- `pending_wakers` relay pattern: event loop locally wakes connection tasks after SPSC processing, bridging monoio's cross-thread waker limitation
- `write_db()` uses `try_write()` spin loop instead of blocking `write()` — prevents OS thread freeze on monoio when cross-shard readers hold locks
- Benchmark scripts: `scripts/bench-scaling.sh` for multi-shard test matrix, `scripts/bench-production.sh` updated

### Fixed

- ResponseSlot `UnsafeCell<Option<Waker>>` data race on ARM64 — replaced with `AtomicWaker`
- Local read path took exclusive write lock (`write_db()`) even for GET — split into `read_db()` + `dispatch_read()`
- Monoio local write path silently dropped responses (`responses.push(response)` missing after read/write split) — all write commands (SET, INCR, LPUSH, etc.) hung on monoio
- Pipeline ordering guard: `!remote_groups.contains_key(&target)` prevents stale reads when batch has pending writes for same shard

### Performance

| Metric | Before (v0.1.0) | After (v0.1.2) | Change |
|--------|:---------------:|:--------------:|:------:|
| Multi-shard GET p=16 | 688K (0.38x Redis) | **1,923K (1.17x Redis)** | **2.8x** |
| Multi-shard GET p=64 | N/A | **5,002K (1.60x Redis)** | New |
| Multi-shard SET p=16 | N/A | **1,515K (1.32x Redis)** | New |
| Multi-shard SET p=64 | N/A | **2,500K (1.55x Redis)** | New |
| Monoio 1s p=128 GET | 5,407K | **5,005K (1.25x Redis)** | Maintained |
| Negative scaling | -25% at 12 shards | **Zero at 1-8 shards** | Eliminated |
| Command coverage p=1 | Parity | **Monoio beats Redis 8/10** | Improved |

## [0.1.1] - 2026-03-28

Structural stability milestone. Codebase refactoring for maintainability — no feature changes, no performance changes.

### Changed

#### Error and State Foundations (Phase 44)
- Unified `MoonError` type hierarchy with structured `#[source]` on I/O variants carrying `PathBuf` context
- `ConnectionContext` struct for connection state (selected_db, authenticated, client_name, protocol_version)
- Criterion benchmark baseline (GET dispatch 69.1ns) to guard against regressions

#### Command Metadata Registry (Phase 45)
- `phf` static perfect hash map for O(1) command lookup (112 commands)
- `CommandMeta` struct: name, arity, flags (read/write/fast/admin), key positions, ACL categories
- `is_write()` classification via const bitflags — replaces duplicated match arms across codebase

#### Persistence Hardening (Phase 46)
- Eliminated server-crashing `unwrap()` calls in WAL, AOF, and RDB persistence code
- Corruption recovery: WAL uses per-block CRC32 log+skip, AOF seeks to next RESP `*` marker, RDB breaks on mid-stream corruption
- `WalWriter` methods remain `std::io::Result` (must-panic on flush = data loss prevention)

#### AOF Replay Decoupling (Phase 47)
- `CommandReplayEngine` trait breaks circular dependency between persistence and command dispatch
- `StorageEngine` trait boundary for persistence replay and Lua scripting
- `execute_command()` at command level (not individual get/set methods)

#### God-File Decomposition (Phase 48)
- `connection.rs` (5,102 lines) → 6 sub-modules in `conn/`: `handler_sharded.rs`, `handler_monoio.rs`, `handler_single.rs`, `shared.rs`, `blocking.rs`, `conn_state.rs`
- `shard/mod.rs` (2,004 lines) → 6 sub-modules: `event_loop.rs`, `spsc_handler.rs`, `persistence_tick.rs`, `conn_accept.rs`, `timers.rs`, `uring_handler.rs`
- Module facade pattern with `pub(crate)` re-exports preserving all external import paths
- No single file exceeds 800 lines

### Added
- Docker: optimized multi-stage build (113MB → 41MB)
- Mintlify documentation site
- Claude Code GitHub workflow for PR reviews
- `scripts/bench-resources.sh` for memory/CPU efficiency benchmarking

## [0.1.0] - 2026-03-27

Initial release. A Redis-compatible in-memory data store written in Rust, achieving 1.84-1.99x Redis throughput at 8 shards and 27-35% less memory for 1KB+ values.

### Added

#### Core Data Types (Phases 1-5)
- RESP2 protocol parser and serializer with inline command support
- TCP server with concurrent connections, graceful shutdown, and `redis-cli` compatibility
- String commands: GET, SET, MGET, MSET, INCR/DECR, APPEND, GETEX, GETDEL (17 commands)
- Hash commands: HSET, HGET, HGETALL, HINCRBY, HSCAN (14 commands)
- List commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LPOS (12 commands)
- Set commands: SADD, SREM, SINTER, SUNION, SDIFF, SPOP (15 commands)
- Sorted Set commands: ZADD, ZRANGE, ZRANGEBYSCORE, ZINCRBY, ZPOPMIN (18 commands)
- Key management: DEL, EXISTS, EXPIRE, TTL, SCAN, KEYS, RENAME (13 commands)
- Lazy + active key expiration with probabilistic sampling
- RDB persistence with point-in-time snapshots
- AOF persistence with configurable fsync (always/everysec/no)
- Pub/Sub messaging: SUBSCRIBE, PUBLISH, PSUBSCRIBE (4 commands)
- Transactions: MULTI/EXEC/DISCARD with WATCH optimistic locking
- LRU/LFU/random eviction policies with configurable maxmemory

#### Performance Architecture (Phases 6-15)
- SIMD-accelerated RESP parsing via memchr CRLF scanning and atoi
- CompactValue 16-byte SSO struct with embedded TTL delta
- DashTable segmented hash table with Swiss Table SIMD probing
- Thread-per-core shared-nothing architecture with per-shard event loops
- io_uring networking layer with multishot accept/recv and registered buffers
- Per-shard memory management with jemalloc and bumpalo arenas
- Forkless compartmentalized persistence (no COW memory spike)
- B+ tree sorted sets replacing BTreeMap for cache-friendly access
- Per-connection arena allocation with bumpalo

#### Protocol & Data Types (Phases 16-18)
- RESP3 protocol: Map, Set, Double, Boolean, VerbatimString, Push frames
- HELLO command for protocol negotiation
- Client-side caching invalidation via Push frames
- Blocking commands: BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX
- Streams data type: XADD, XREAD, XRANGE, XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM

#### Clustering & Replication (Phases 19-20, 26)
- PSYNC2-compatible replication with per-shard WAL streaming
- Partial resync support with replication backlog
- Cluster mode with 16,384 hash slots and gossip protocol
- MOVED/ASK redirections and live slot migration
- Majority consensus failover election with automatic promotion

#### Scripting & Security (Phases 21-22, 43)
- Lua 5.4 scripting via mlua: EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH
- Sandboxed Lua VM with Redis API bindings (redis.call, redis.pcall)
- ACL system: per-user command/key/channel permissions
- ACL SETUSER, GETUSER, DELUSER, LIST, WHOAMI, LOG, SAVE, LOAD
- TLS 1.3 via rustls + aws-lc-rs with dual-port support
- mTLS client authentication
- Protected mode (reject non-loopback when no password set)

#### Optimization (Phases 24-42)
- WAL v2 format: checksums, header, block framing, corruption isolation
- CompactKey SSO: 23-byte inline keys, eliminating heap allocation
- Response buffer pooling and adaptive pipeline batching
- Dual runtime: Tokio (all platforms) + Monoio (Linux io_uring / macOS kqueue)
- Full Monoio migration: channels, TCP, codec, spawn, persistence, replication
- Direct GET serialization bypassing Frame allocation
- Zero-copy argument slicing from parse buffer
- Lock-free oneshot channels (12% CPU reduction vs tokio::oneshot)
- CachedClock timestamp caching (4% throughput gain)
- HeapString for values (eliminates Arc overhead)
- Inline dispatch for single-shard commands

### Performance

| Benchmark | Result |
|-----------|--------|
| Peak GET throughput | 3.79M ops/sec (4 shards, p=64) |
| Peak SET with AOF | 2.78M ops/sec (AOF everysec, p=64) |
| vs Redis (pipeline=64) | 3.17x SET, 2.50x GET |
| vs Redis (8 shards, p=16) | 1.84-1.99x |
| vs Redis with AOF | 2.75x (per-shard WAL vs global) |
| Memory (1KB+ values) | 27-35% less than Redis |
| Memory (empty server) | Identical 7.0 MB baseline |
| p50 latency (8 shards) | 0.031ms (Redis: 0.26ms) |
| Data consistency | 132/132 tests pass |

### Technical Details

- **Language:** Rust (stable, edition 2024)
- **Lines of code:** ~54,000 across 96 files
- **Dependencies:** tokio, monoio, jemalloc, rustls, mlua, bumpalo, bytes, clap
- **Supported platforms:** Linux (io_uring via Monoio), macOS (kqueue via Monoio or Tokio)
- **Build time:** ~50s release build

[0.1.0]: https://github.com/pilotspace/moon/releases/tag/v0.1.0

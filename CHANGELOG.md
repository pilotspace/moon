# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- **Criterion benchmarks**: CSR 1-hop 923ps, edge insert 44ns, 2-hop BFS 7.15µs, CSR freeze 5.23ms.
- **New dependencies**: `slotmap` 1.x (generational indices), `boomphf` 0.6 (MPH), `logos` 0.14 (Cypher lexer, optional).

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

## [Earlier Unreleased] - Dispatch Hot-Path Recovery (2026-04-08)

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

## [Unreleased] - Vector Search 4x QPS + Correctness

### Vector Search Performance & Correctness (2026-04-07)

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

## [Earlier Unreleased] - Disk Offload & x86_64 Performance

Tiered storage, crash recovery, and 2x Redis on x86_64 (Intel Xeon, io_uring).

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

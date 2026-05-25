# Moon vs Valkey — Deep Architectural Comparison

*Reviewed: Moon v0.1.12 (~206K LOC Rust, traced from source) vs Valkey 9.1.0 (released 2026-05-19). Document date: 2026-05-25.*

---

## 0. Strategic Framing

These two projects are **not** the same kind of thing.

- **Valkey** is the *Linux-Foundation-stewarded continuation of Redis OSS* — a C codebase forked from Redis 7.2 after the SSPL relicense (March 2024). Its commercial DNA is "drop-in Redis replacement, blessed by hyperscalers, governed in the open." Every change is constrained by *preserving 30 years of Redis semantics* and the C single-threaded-command-execution mental model.
- **Moon** is a *clean-room rewrite in Rust* with a different bet: thread-per-core shared-nothing, in-process AI primitives (vector + BM25 + graph), and a converged data-platform surface. Wire-compatible, but the architectural premises diverge from the first byte. Status is still **experimental** — README leads with that warning.

The right mental model is **Valkey = horizontal moat (ecosystem + governance), Moon = vertical moat (architecture + integrated AI surface)**. They compete on different axes, not feature parity.

---

## 1. Headline Comparison Table

| Dimension | **Moon v0.1.12** | **Valkey 9.1.0** | Strategic note |
|---|---|---|---|
| **Language / age** | Rust edition 2024, MSRV 1.94, ~206K LOC, started 2025 | C99, ~150K LOC pre-fork + ~30K added, lineage 2009 (Redis) | Moon trades ecosystem maturity for memory safety + modern language. |
| **License / governance** | Apache-2.0, single-project repo | BSD-3-Clause, LF TSC, ~50 backers (AWS / GCP / Oracle / Alibaba / Tencent / Huawei…) | Valkey is the **safe-by-governance** choice; Moon has no foundation, no co-vendor, no module ecosystem. |
| **Threading model** | **Thread-per-core, shared-nothing.** Each `Shard` owns `databases[16]`, `pubsub_registry`, `vector_store` directly — *no Arc, no Mutex* per `src/shard/mod.rs`. Cross-shard = lock-free SPSC (`flume`) + `AtomicWaker` response slots. | **Single main thread executes all commands.** I/O threads (off by default; up to 9 used in published bench) parallelize parsing, response framing, `epoll_wait`. Comm model redesigned in 9.1 for +17 % throughput. Parallel read-execution on I/O threads (Issue #2022, 3.1M RPS prototype) is *TSC-pending, not merged*. | Moon's model is structurally further along the scaling curve; Valkey is incrementally retrofitting concurrency onto a single-threaded core. |
| **I/O driver** | Default **`monoio`** = io_uring on Linux / kqueue on macOS; **`tokio`** runtime alternative gated by feature. Same binary, dual codepath. SO_REUSEPORT per-shard listener. `MOON_NO_URING=1` runtime kill-switch. | **`epoll` (Linux) / `kqueue` (macOS) only.** *No io_uring* in any released or active PR (verified May 2026). No per-thread `SO_REUSEPORT`. RDMA exists as experimental compile flag. | Moon owns "modern Linux I/O." Valkey's design philosophy is portability over peak-Linux. |
| **In-memory data structure** | **`DashTable`** — custom segmented hash table = Dragonfly-style directory→segments→buckets + hashbrown-style Swiss-Table 16-way SIMD probing (`src/storage/dashtable/`). | **New open-addressing hashtable in 8.1** — 64-byte buckets, `serverObject` embeds key+value, replaces classic `dict`+`dictEntry`. Saves 20-30 B/KV vs Redis 7.2. | Both made the same architectural realization (cache-line-aligned, embedded entries). Moon's is per-shard; Valkey's is global. |
| **Per-key memory footprint** | `CompactKey` 23 B inline SSO; `CompactValue` 16 B SSO with inline TTL; `HeapString` for ≥24 B strings; `listpack` / `intset` / B+tree sorted sets — all custom. **27-35 % less RSS than Redis 8.6 at ≥1 KB values** (Moon's own bench). | Embstr threshold raised to **128 B** in 9.1 (PR #1726/#2516) — 20 % less for short strings. Listpack, intset, quicklist, ziplist retained from Redis. | Different optimizations: Moon wins on large values; Valkey 9.1 wins on tiny strings. Below 64 B Valkey may be tighter post-9.1 — needs head-to-head re-bench. |
| **Persistence — snapshot** | **Forkless compartmentalized snapshot** (`src/persistence/snapshot.rs`). Serializes DashTable segments one at a time as cooperative tasks in the shard event loop; segment-level COW via per-snapshot overflow buffer for keys modified in not-yet-serialized segments. RDB v2 format (`RRDSHARD` magic, last_lsn, created_at_unix_ms for PITR). | **`fork()` + COW** RDB child process. Memory spike during snapshot is the well-known production gotcha. RDB binary format. | **Clean architectural win for Moon.** No fork = no 2× RSS spike, predictable tail latencies during snapshot, works in container / non-MMU environments. |
| **Persistence — WAL/AOF** | **Per-shard WAL v2** (`src/persistence/wal.rs`) — `RRDWAL` magic, 32 B header, CRC32-framed blocks, batched 1 ms fsync via `FileIo` trait (uring on Linux). Multi-part AOF + RDB preamble + BGREWRITEAOF on monoio. 100 % crash-recovery validated. Three fsync policies. | **Single global AOF** written by one background thread via POSIX `write` / `fdatasync`. Multi-part AOF (base + incremental, since Redis 7.0). RDB preamble. Three fsync policies. | Moon's per-shard WAL eliminates the global write-serialization point — advantage grows with pipeline depth (2.75× over Redis at p=64 per Moon's bench). |
| **Tiered storage** | **Tiered NVMe disk-offload** with cold-index, async write-back, read-through. Eviction spills under `maxmemory` instead of deleting. | None in core; not on roadmap. (Redis Enterprise has Flash; Valkey OSS does not.) | Moon-only feature; high differentiator for large-working-set / cost-sensitive workloads. |
| **Replication** | PSYNC2 + per-shard shared replication backlog (`SharedBacklog` array; `evaluate_psync_shared` ANDs cover-check across shards). Async stream of WAL bytes. `WAIT` supported. No multi-master. | PSYNC2, single replication backlog, diskless RDB streaming, `WAIT`. No multi-master in OSS (CRDT only in Redis Enterprise). | Moon's per-shard backlog is the natural complement to per-shard WAL — but cross-shard PSYNC quorum logic is more complex. Both lack open-source active-active. |
| **Clustering** | Redis Cluster compatible: 16384 slots, gossip (PING / PONG / MEET / FailoverAuthReq+Ack — magic `0x52656469` = "Redi"), 2048-byte slot bitmap per node, MOVED/ASK, election. | Same 16384-slot model. **9.0 added atomic slot migration**, multi-DB-in-cluster. **9.1 added CLUSTERSCAN.** 2000-node target. | Valkey's clustering is more battle-tested and has more recent feature work (atomic slot migration is genuinely valuable). Moon has feature parity at the protocol level but less operational mileage. |
| **Command surface** | **230+ commands** per README, declared via `phf` static map in `src/command/metadata.rs`; ACL categories, flags (WRITE / READONLY / FAST / ADMIN / PUBSUB / …); FT.* (vector + text), GRAPH.* (Cypher subset, 14 cmds), TXN.* (cross-store ACID), TEMPORAL.*, WS.*, MQ.* added. | ~240 Redis 7.2 commands + new ones (`MSETEX`, `HGETDEL`, `HEXPIRE` family, `CLUSTERSCAN`, `DELIFEQ`, `CLIENT NO-TOUCH`). FT.*, JSON.*, BF.* live as **separate modules**. | Moon ships AI/transaction commands *in-core*. Valkey ships them as modules — a deliberate philosophy (small core, opt-in surface). |
| **Vector search** | **In-process HNSW + TurboQuant 1-8-bit quantization**, MVCC, IVF, GPU scaffold (`cudarc` feature), hybrid graph+vector, hexagonal HNSW, 2-stage search pipeline. Bench: **12.7K QPS / 0.92 R@10 at 384d MiniLM**, 8.2K insert/s. Auto-indexed on HSET. | **`valkey-search` 1.2.0** C++ module (March 2026). HNSW + FLAT, **no quantization** (FP32 only as of May 2026). Pre-filter vs inline-filter planner. Full-text + tag + numeric + vector hybrid in 1.2.0. "Single-digit ms latency, billions of vectors" — no published QPS/recall table. | **Moon's biggest single competitive advantage right now.** Quantization (1-8 bit) is the lever that drops memory 4-32× — Valkey has none. Moon vs Redis published 12.7K vs 3.8K QPS at equal recall. |
| **Full-text / BM25** | **Native in-core** BM25 + `FT.AGGREGATE` (GROUPBY / REDUCE / APPLY), Levenshtein fuzzy via FST automata, TAG / NUMERIC field types, HIGHLIGHT / SUMMARIZE, **three-way RRF hybrid fusion** (BM25 + dense + sparse). | In `valkey-search` 1.2.0 module: full-text added March 2026. Hybrid full-text + vector + tag + numeric. No reported maturity of relevance ranking yet. | Roughly comparable surface at the syntax level; Moon's RRF + sparse fusion is more advanced than what valkey-search has documented publicly. |
| **Graph database** | **First-class property graph engine**: 14 `GRAPH.*` commands, Cypher subset, CSR + SlotMap storage (`src/graph/csr/`), mmap'd CSR segments, SIMD-accelerated traversal, hybrid graph + vector queries, cross-shard traversal, bi-temporal `VALID_AT`. Bench: **23× FalkorDB insert, 2.4× Cypher QPS**. | **None.** No graph module, not on roadmap. FalkorDB remains a separate (Redis-module) project; not ported to Valkey. | **Moon-exclusive category.** No Valkey path to closing this in the next 12 months. |
| **Probabilistic DS** | HLL native (`hll.rs`, 30.5 KB). Bloom / cuckoo / count-min / top-k not in core (would need to be added). | HLL native (12× AVX2 speedup in 8.1). Bloom / cuckoo / CMS / top-k via `valkey-bloom` 1.0 module. | Valkey wider here. Easy add for Moon. |
| **JSON** | Not a native type (HSET-based modeling). | `valkey-json` 1.0 module — full JSONPath, available in valkey-bundle. | Valkey wider. Moon gap. |
| **Cross-store transactions** | **`TXN.BEGIN` / `COMMIT` / `ABORT`** — atomic writes across KV + vector + graph with undo-log rollback. | MULTI / EXEC (KV-only, no cross-module ACID). | Moon-only; meaningful for AI-pipeline use cases. |
| **Multi-tenancy** | **Workspaces** (`WS CREATE` / `WS AUTH`) — transparent key prefixing + per-shard registries + token-scoped auth. | Database-level ACLs (9.1 new) — restricts users to numbered DB indexes. | Different shapes. Workspaces are a richer multi-tenant primitive; ACL-per-DB is simpler. |
| **Message queue** | `MQ CREATE` / `PUSH` / `POP` / `ACK` — at-least-once, DLQ, debounced triggers, WAL-backed. | Streams (XADD / XREAD / XGROUP), no native DLQ semantics. | Different abstraction levels. Streams are lower-level; MQ is higher-level. |
| **Bi-temporal** | `TEMPORAL.SNAPSHOT_AT`, `TEMPORAL.INVALIDATE`, `FT.SEARCH AS_OF`, `GRAPH.QUERY VALID_AT`. | None. | Moon-exclusive. |
| **Modules** | **Closed core** (no module loader). All extension via Rust feature flags compiled in. | **Open module API v1** (C ABI, stable across versions). Rust SDK `valkey-module` crate. valkey-search / json / bloom / ldap as separate modules. | Trade-off: Moon gets cohesion + safety (no foreign code in-process) and loses extensibility. Valkey gets ecosystem at the cost of C-FFI surface area. |
| **Lua / scripting** | Lua 5.4 via `mlua` (vendored), lazy init. | Lua moved to **optional module in 9.1**. Future custom-engine API; WASM mentioned. | Both moving away from Lua-in-core. |
| **Pub / Sub** | Per-shard `PubSubRegistry`, zero-copy `Bytes` fan-out, sharded SPSC scatter. Bench: 8.9× faster `try_send`, 1.48× Redis at multi-shard p=16. | Standard PUBLISH / SUBSCRIBE + SHARDED PUB/SUB (cluster-aware). | Moon's design is more cache-friendly per shard; Valkey's sharded variant is a recent retrofit. |
| **Protocol** | RESP2 + RESP3 (zero-copy two-pass parser, `parse_frame_zerocopy` returns `Frame::Null` on any malformed input — no panic). | RESP2 + RESP3 + RDMA experimental transport. | Parity. Moon's parser defensiveness (fuzz-driven) is a hardening edge. |
| **TLS** | `rustls` + `aws-lc-rs`, mTLS, works on both runtimes (`tokio-rustls` / `monoio-rustls`). | OpenSSL-based, mTLS, **9.1 added auto cert reload + SAN URI mTLS**. | Valkey 9.1 has slightly nicer operator UX. Moon has memory-safe TLS stack. |
| **Observability** | Built-in **Prometheus exporter** (`metrics-exporter-prometheus`), admin HTTP port, OTel feature flag reserved (`otel` cargo feature). Embedded React **web console** at `/ui/` (Dashboard / Browser / Console / Vectors / Graph / Memory). | INFO (with main-thread + I/O-thread CPU breakdown in 9.1), LATENCY, COMMANDLOG (8.1), slowlog, JSON log format (9.1), keyspace notifications. **Valkey Admin GUI 1.0** released 2026-05-12 (separate tool). Server itself emits no OTel; OTel is client-side (GLIDE 2.0). | Both have admin GUIs now. Moon's is embedded (one binary), Valkey's is a separate desktop / k8s tool. Prometheus is in Moon by default; in Valkey it requires `oliver006/redis_exporter`. |
| **Memory safety** | 156 `unsafe` blocks with mandatory `// SAFETY:` audit (CI-enforced via `scripts/audit-unsafe.sh`). Unwrap ratchet. 7 fuzz targets (resp_parse, inline_parse, wal_v3_record, gossip_deser, acl_rule, …), 15 min/PR, 6 h nightly. Loom model tests. | C memory model. Long history of CVE exposure (any C codebase of this age). Module ABI also exposes C unsafety surface. | Architectural advantage Moon will own as long as the rewrite stays disciplined. |
| **GPU acceleration** | `cudarc` feature flag, kernel scaffold under `src/gpu/`, vector-distance kernels planned. CPU + SIMD fallback mandatory. | None. | Moon-only direction; not yet shipping production kernels. |
| **Published throughput** | 5.11M GET/s p=64, 3.50M SET/s p=64 (GCP c3-8, monoio, **single server**, Apr 2026). 2.20× Redis on ARM Neoverse-N1. | 2.1M RPS (Valkey 9.1, 9 I/O threads, p=10, 512 B, single server). 1B+ RPS on **2000-node cluster** (9.0 target). | Single-server: Moon ~2.4× Valkey 9.1 (caveat: different harnesses). Cluster-scale: Valkey has demonstrated multi-node scale; Moon has not yet published cluster-scale numbers. |
| **Production readiness** | README labels **experimental**. No managed cloud offering. Moon Console + Rust/Python SDKs published. | Production. **AWS ElastiCache, Google Memorystore, Oracle OCI, Alibaba, Tencent, Huawei, DigitalOcean, Aiven, Heroku** all GA. Bitnami + hyperspike + SAP Helm charts/operators. | Valkey wins the production-readiness axis by a continent. This is Moon's biggest non-technical risk. |
| **Client SDK story** | `moon-client` Rust on crates.io, `moondb` Python on PyPI. Wire-compatible with any Redis client. | All Redis clients work. Official `valkey-glide` (Rust core, Java / Python / Node / Go bindings, OTel built-in). | Valkey has multi-language first-party clients with OTel; Moon relies on protocol compatibility. |

---

## 2. Where Each Wins (Decisive)

### Moon wins decisively on

- **Vector search performance + quantization** — 12.7K QPS, 1-8 bit TurboQuant; Valkey has FP32 only, no public QPS.
- **Graph engine** — Valkey has no graph at all.
- **Forkless persistence** — no RSS spike, container-friendly.
- **Per-shard WAL** — AOF throughput advantage grows with pipeline depth (2.75× at p=64).
- **Thread-per-core scaling architecture** — Valkey is still moving execution onto I/O threads incrementally.
- **Memory safety** — Rust + audited unsafe + fuzz / loom.
- **Single-binary AI surface** — vector + BM25 + graph + transactions, no module loading dance.
- **Single-server peak throughput** — ~2.4× Valkey 9.1 on comparable hardware (caveat: vendor benches).
- **Tiered NVMe disk offload.**

### Valkey wins decisively on

- **Ecosystem & governance** — LF TSC, ~50 backers, every major cloud offers managed Valkey.
- **Production maturity** — multi-year track record at AWS / GCP scale, multi-thousand-node clusters demonstrated.
- **Module ecosystem** — JSON, Bloom, LDAP, Search ship as composable modules with stable C ABI + Rust SDK.
- **Atomic slot migration** (9.0) — operational property Moon's cluster code does not yet match.
- **Cluster-scale benchmarks** — 1B+ RPS, 2000 nodes demonstrated.
- **Multi-language official client** — GLIDE 2.0 with server-side-aware semantics + OTel.
- **Hash field expiration** (9.0) — `HEXPIRE` / `HTTL` family Moon does not implement.
- **Operator / Helm tooling** — Helm chart, operators, GUI, managed cloud everywhere.

### Effective parity

- Redis Cluster wire protocol (gossip, slots, MOVED / ASK).
- RESP2 / RESP3 + RESP3 typed replies.
- mTLS.
- PSYNC2 replication, diskless full sync.
- Listpack-class compact encodings.
- 230-240 command surface.

---

## 3. Architectural Philosophy in One Line Each

| Project | One-line thesis |
|---|---|
| **Valkey** | *"Preserve Redis semantics; squeeze more throughput by parallelizing I/O off the main thread; extend via modules."* |
| **Moon** | *"If you started over today, you'd use Rust, io_uring, thread-per-core, and put vector + graph + ACID transactions in the box."* |

The Valkey roadmap (atomic slot migration, parallel reads on I/O threads, JSON-format logs, RDMA, hash-field TTLs) tells you the team is *incrementally modernizing a 15-year-old codebase*. The Moon codebase tells you the team is *building the dataplane they wish Redis had been.*

Both are valid; they will likely coexist.

---

## 4. Risks for Moon (Project-Owner View)

1. **The "experimental" label is a moat for Valkey.** Until Moon has a managed offering, multi-year production reference, and an independent third-party benchmark validating the published numbers, Valkey wins every procurement conversation by default. **This is the single highest-leverage item.**
2. **No module API** is starting to look like a strategic constraint, not a clean-architecture win. Valkey's ability to ship JSON, Bloom, LDAP, Search as modules — and to expose a Rust module SDK — means *the community can extend Valkey faster than Moon's core team can extend Moon*. Reconsider whether a sandboxed (WASM?) module surface is worth opening.
3. **Cluster operational features** (atomic slot migration, CLUSTERSCAN, hash-field TTL, multi-DB-in-cluster) are real workload features Valkey shipped in 9.0 / 9.1 that Moon's cluster code does not yet match. These will surface in any serious operator evaluation.
4. **Valkey's vector module will add quantization.** It's the obvious next step and they have the contributor base. Moon's current TQ moat shrinks every quarter — lean into agentic / hybrid / temporal differentiation that's harder to copy.
5. **Multi-language client story.** GLIDE 2.0 is a real asset; Moon's two SDKs (Rust + Python) are insufficient for adoption at scale.
6. **Cluster-scale benchmark gap.** Moon has not published >1-node scaling numbers comparable to Valkey's 1B-RPS / 2000-node demonstration. The shared-nothing architecture *should* scale better than Valkey's main-thread bottleneck — but until that's shown publicly, it's a claim, not a fact.

---

## 5. Recommendation

The competitive narrative writes itself: **"Moon is what Redis would look like if rewritten today for AI workloads on modern Linux."** Moon's design choices are individually defensible and collectively coherent. The competitive risk is non-technical: governance, ecosystem, and managed-service distribution — exactly the axes where Valkey is unassailable.

Two strategic plays look high-value:

1. **Double down on the AI-platform vertical** (vector + graph + transactions + hybrid + temporal) where Valkey has no path to parity in 12 months.
2. **Cross the credibility chasm** with (a) one independent third-party benchmark, (b) a documented cluster-scale run (≥16 nodes), and (c) a stable storage-format guarantee. The "experimental" label is currently the biggest single drag on adoption.

---

## 6. Source References

### Moon (this repo, traced)

- `src/shard/mod.rs` — `Shard` struct, per-core ownership, "no Arc, no Mutex" comment.
- `src/storage/dashtable/mod.rs` — Dragonfly + Swiss-Table architecture.
- `src/storage/db.rs` — `Database`, `CompactKey`, `CompactValue`, `HeapString`, listpack/intset thresholds.
- `src/persistence/snapshot.rs` — forkless segment-COW snapshot engine.
- `src/persistence/wal.rs` — WAL v2 format, `RRDWAL` magic, CRC32 block frames.
- `src/persistence/aof.rs` — multi-part AOF, fsync policies.
- `src/replication/master.rs` — PSYNC2 with shared per-shard backlogs.
- `src/cluster/mod.rs`, `src/cluster/gossip.rs` — Cluster bus, gossip, MOVED / ASK.
- `src/command/metadata.rs` — PHF command table, flags, ACL categories.
- `src/vector/store.rs` — per-shard `VectorStore`, HNSW + TurboQuant.
- `src/graph/mod.rs` — graph engine (CSR, Cypher, hybrid, cross-shard).
- `README.md`, `BENCHMARK.md` — published benchmark tables.

### Valkey (external)

- <https://valkey.io/blog/valkey-9-1-delivers-improvements-in-security-performance-and-more/> — 9.1 release notes.
- <https://valkey.io/blog/introducing-valkey-9/> — 9.0 release notes (atomic slot migration, hash field TTL).
- <https://valkey.io/blog/unlock-one-million-rps/> — 8.0 async I/O threading + 1.19M RPS bench.
- <https://valkey.io/blog/new-hash-table/> — 8.1 hashtable redesign.
- <https://valkey.io/blog/introducing-valkey-search/> — valkey-search.
- <https://valkey.io/blog/valkey-search-1_2/> — full-text + hybrid (Mar 2026).
- <https://valkey.io/blog/valkey-modules-rust-sdk-updates/> — Rust module SDK.
- <https://valkey.io/blog/1-billion-rps/> — 2000-node cluster bench.
- <https://github.com/valkey-io/valkey/pull/758> — async I/O threads PR.
- <https://github.com/valkey-io/valkey/issues/2022> — parallel read on I/O threads design.
- <https://github.com/valkey-io/valkey/pull/1726>, <https://github.com/valkey-io/valkey/pull/2516> — embstr 128B / short-string −20% memory.
- <https://github.com/valkey-io/valkey-search> — vector module repo.
- <https://aws.amazon.com/blogs/database/announcing-valkey-9-0-for-amazon-elasticache/> — ElastiCache 9.0.
- <https://cloud.google.com/blog/products/databases/memorystore-for-valkey-9-0-is-now-ga> — GCP Memorystore.
- <https://www.linuxfoundation.org/press/valkey-9.0-delivers-performance-and-resiliency-for-real-time-workloads> — LF press.

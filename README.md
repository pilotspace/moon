<p align="center">
  <img src="assets/banner.png" alt="Moon Banner" width="100%">
</p>

<p align="center">
  <strong>A Redis-compatible in-memory data store, written from scratch in Rust.</strong>
</p>

<p align="center">
  <a href="https://github.com/pilotspace/moon/releases/tag/v0.1.0"><img src="https://img.shields.io/badge/version-v0.1.0-blue" alt="Version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License"></a>
  <img src="https://img.shields.io/badge/status-experimental-orange" alt="Status">
  <img src="https://img.shields.io/badge/rust-edition%202024-orange" alt="Rust">
  <img src="https://img.shields.io/badge/redis--compatible-RESP2%2FRESP3-red" alt="Protocol">
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> &bull;
  <a href="#why-moon">Why Moon</a> &bull;
  <a href="#benchmarks">Benchmarks</a> &bull;
  <a href="docs/index.mdx">Docs</a> &bull;
  <a href="CHANGELOG.md">Changelog</a>
</p>

---

> **⚠ Experimental.** Moon is under active development and **not** recommended for production. Storage formats, APIs, and config flags may change between releases. Please [open an issue](https://github.com/pilotspace/moon/issues) if something breaks.

---

Moon speaks the Redis wire protocol (RESP2/RESP3) and implements 200+ commands. It runs on a thread-per-core, shared-nothing architecture with optional `io_uring` I/O, per-shard WAL, tiered disk offload, and an in-process vector search engine. Any Redis client connects out of the box.

## Why Moon

- **Thread-per-core, zero shared state.** Each shard owns its own event loop, DashTable, WAL writer, and Pub/Sub registry. No global locks; cross-shard dispatch is a lock-free SPSC channel.
- **Dual runtime.** Monoio (`io_uring` on Linux, `kqueue` on macOS) for peak throughput; Tokio for portability and CI. Same binary, feature-gated.
- **Forkless persistence.** RDB snapshots iterate DashTable segments incrementally — no fork(), no COW memory spike. AOF is a per-shard WAL with batched fsync; the advantage over Redis grows with pipeline depth.
- **Tiered disk offload.** Keys evicted under `maxmemory` spill to NVMe instead of being deleted, with async write and read-through. 100% crash recovery across all tiers.
- **Memory-optimized types.** `CompactKey` (23-byte SSO), `CompactValue` (16-byte SSO with inline TTL), `HeapString`, B+ tree sorted sets, and per-request bumpalo arenas — **27–35% less RSS** than Redis at 1 KB+ values.
- **In-process vector search.** `FT.CREATE` / `FT.SEARCH` with HNSW + TurboQuant 4-bit quantization. **2.56× Qdrant QPS** at higher recall on real MiniLM embeddings.

<p align="center">
  <img src="assets/architecture-comparison.png" alt="Moon vs Redis Architecture" width="100%">
</p>

## Benchmarks

Measured vs Redis 8.6.1, co-located client and server, pipeline depth tuned per row. Full methodology and reproduction steps in [BENCHMARK.md](BENCHMARK.md) and [docs/benchmarks.mdx](docs/benchmarks.mdx).

### Peak throughput (GCP c3-standard-8, x86_64, monoio io_uring)

| Workload                         |   Moon | Redis |  Ratio |
|----------------------------------|-------:|------:|:------:|
| Peak GET (c=50, p=64)            | 4.81M  | 2.36M | **2.04×** |
| Peak SET (c=50, p=64)            | 3.60M  | 1.79M | **2.01×** |
| GET with AOF everysec            | 4.57M  | 2.24M | **2.04×** |
| GET with Disk Offload            | 4.81M  | 2.36M | **2.04×** |
| Single-conn GET (c=1, p=64)      | 2.08M  | 1.30M | **1.60×** |
| p99 latency (c=10, p=64)         | 0.079 ms | 0.263 ms | **3.3× lower** |
| Memory, values ≥ 1 KB            | —      | —     | **27–35% less** |
| Crash recovery (SIGKILL, 5K keys)| 100%   | 100%  | parity |

### Vector search (10K × 384d MiniLM, k=10)

|                    |   Moon x86 | Qdrant FP32 |
|--------------------|-----------:|------------:|
| Recall@10          | **0.9670** | ~0.9500     |
| Search QPS         | **1,296**  | 507         |
| Search p50         | **0.78 ms**| 1.79 ms     |
| Insert rate        | **11.3K/s**| ~2.6K/s     |
| Memory per vector  | **~3.2 KB**| ~4.0 KB     |

> **Caveat.** The x86_64 numbers above were measured before the PR #43 correctness changes were landed. A correctness-preserving dispatch hot-path recovery is in place on aarch64 — see the [dispatch recovery entry in CHANGELOG.md](CHANGELOG.md#unreleased---dispatch-hot-path-recovery-2026-04-08) for per-commit profiles. x86_64 peak numbers will be re-measured on the next release.

## Quick start

### Prerequisites

- [Rust](https://rustup.rs/) stable toolchain (edition 2024)
- `cmake` (required by `aws-lc-rs` for TLS)

### Build and run

```bash
git clone https://github.com/pilotspace/moon.git
cd moon
cargo build --release

# Defaults: bind 127.0.0.1:6379, shard count = CPU count
./target/release/moon

# Or with production flags
./target/release/moon \
  --port 6379 \
  --shards 8 \
  --appendonly yes --appendfsync everysec \
  --maxmemory 8g --maxmemory-policy allkeys-lfu
```

### Connect with any Redis client

```bash
redis-cli -p 6379
127.0.0.1:6379> SET hello world
OK
127.0.0.1:6379> GET hello
"world"
127.0.0.1:6379> HSET user:1 name Alice age 30
(integer) 2
127.0.0.1:6379> FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA emb VECTOR HNSW 6 DIM 384 TYPE FLOAT32 DISTANCE_METRIC COSINE
OK
```

### Docker

Multi-stage build with [cargo-chef](https://github.com/LukeMathWalker/cargo-chef) caching and a [distroless](https://github.com/GoogleContainerTools/distroless) runtime (~41 MB final image):

```bash
docker build -t moon .
docker run -d -p 6379:6379 -v moon-data:/data moon \
  moon --bind 0.0.0.0 --appendonly yes
```

See [docs/quickstart.mdx](docs/quickstart.mdx) for alternative build configs, TLS setup, and Docker Compose.

## Features at a glance

| Category | Highlights |
|---|---|
| **Data types** | Strings, lists, hashes, sets, sorted sets, streams, HyperLogLog, bitmaps, vectors |
| **Persistence** | Forkless RDB, per-shard AOF (`always`/`everysec`/`no`), WAL v2 framing, tiered disk offload |
| **Networking** | RESP2/RESP3, HELLO negotiation, TLS 1.3 (rustls + aws-lc-rs), mTLS, pipelining, client-side caching |
| **Clustering** | 16,384 hash slots, gossip, MOVED/ASK, live slot migration, PSYNC2 replication, majority-vote failover |
| **Scripting & security** | Lua 5.4 (EVAL/EVALSHA), ACL users/keys/channels/commands, protected mode |
| **Vector search** | `FT.CREATE`/`FT.SEARCH`, HNSW + TurboQuant 4-bit, auto-indexing on `HSET` |
| **Observability** | `INFO`, `SLOWLOG`, `COMMAND DOCS`, `OBJECT`, `DEBUG`, structured `tracing` logs |

Full command list: [docs/commands.mdx](docs/commands.mdx). Configuration flags: [docs/configuration.mdx](docs/configuration.mdx). Architecture deep-dive: [docs/architecture.mdx](docs/architecture.mdx).

## Development

```bash
# Unit tests (1,872 tests)
cargo test --lib

# Full CI matrix (Linux, via OrbStack on macOS)
cargo fmt --check && cargo clippy -- -D warnings && cargo test --release

# Data-consistency tests vs Redis as ground truth (132 tests, 1/4/12 shards)
./scripts/test-consistency.sh

# Throughput comparison vs Redis
./scripts/bench-production.sh

# Flamegraph a hot path
cargo flamegraph --bin moon -- --port 6399 --shards 1
```

Contribution guide and coding rules (unsafe policy, hot-path allocation rules, lock discipline) are in [CLAUDE.md](CLAUDE.md) and [UNSAFE_POLICY.md](UNSAFE_POLICY.md).

## Roadmap

Moon is pre-1.0 and **experimental**. Current focus:

- Correctness parity with Redis 8.x across the full command surface
- Tiered disk offload (RAM → NVMe) with crash recovery
- In-process vector search (HNSW + TurboQuant) with `FT.*` API compatibility
- Thread-per-core dispatch hot-path optimization (see [CHANGELOG.md](CHANGELOG.md))

Production readiness is **not** a v0.1 goal. Storage formats, APIs, and config flags may change between releases.

## Credits

Moon stands on the shoulders of systems research and an open-source ecosystem. Headline credits:

- **[Dragonfly](https://github.com/dragonflydb/dragonfly)**, **[ScyllaDB/Seastar](https://github.com/scylladb/seastar)**, **[Garnet](https://github.com/microsoft/garnet)** — thread-per-core shared-nothing architecture.
- **[Dash (VLDB 2020)](https://www.vldb.org/pvldb/vol13/p1147-lu.pdf)** — segmented hash table design behind `DashTable`.
- **[Swiss Table / Abseil](https://abseil.io/about/design/swisstables)** — SIMD control-byte probing within each segment.
- **[TurboQuant (arXiv 2411.04405)](https://arxiv.org/abs/2411.04405)** + **[HNSW (arXiv 1603.09320)](https://arxiv.org/abs/1603.09320)** — vector quantization and graph index for `FT.SEARCH`.
- **[Monoio (ByteDance)](https://github.com/bytedance/monoio)** — thread-per-core `io_uring` runtime.
- **[rustls](https://github.com/rustls/rustls)**, **[aws-lc-rs](https://github.com/aws/aws-lc-rs)**, **[mlua](https://github.com/mlua-rs/mlua)**, **[jemalloc (TiKV)](https://github.com/tikv/jemallocator)**, **[memchr](https://github.com/BurntSushi/memchr)**, **[bumpalo](https://github.com/fitzgen/bumpalo)**, **[bytes](https://github.com/tokio-rs/bytes)** — core runtime dependencies.
- **[Redis Protocol Spec (RESP2/RESP3)](https://redis.io/docs/latest/develop/reference/protocol-spec/)** + **[Redis Cluster Spec](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)** — the wire protocol and cluster semantics Moon implements.

Full list with per-dependency rationale, research paper summaries, and benchmarking methodology: **[docs/references.mdx](docs/references.mdx)**.

## License

[Apache License 2.0](LICENSE)

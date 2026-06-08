<p align="center">
  <img src="assets/banner.png" alt="Moon Banner" width="100%">
</p>

<p align="center">
  <strong>A Redis-compatible in-memory data store, written from scratch in Rust.</strong>
</p>

<p align="center">
  <a href="https://github.com/pilotspace/moon/releases/latest"><img src="https://img.shields.io/github/v/release/pilotspace/moon?label=version&color=blue" alt="Version"></a>
  <a href="https://crates.io/crates/moondb"><img src="https://img.shields.io/crates/v/moondb?label=moondb" alt="Rust SDK"></a>
  <a href="https://pypi.org/project/moondb/"><img src="https://img.shields.io/pypi/v/moondb?label=moondb" alt="Python SDK"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License"></a>
  <img src="https://img.shields.io/badge/single--node-production--grade-success" alt="Status">
  <img src="https://img.shields.io/badge/cluster-alpha-yellow" alt="Cluster status">
  <img src="https://img.shields.io/badge/rust-edition%202024-orange" alt="Rust">
  <img src="https://img.shields.io/badge/redis--compatible-RESP2%2FRESP3-red" alt="Protocol">
</p>

---

Moon is a clean-room Rust rewrite of a Redis-compatible in-memory data
store with first-class AI primitives. It speaks the Redis wire protocol
(RESP2/RESP3) and implements **230+ commands** — every standard Redis data
type plus native `FT.*` vector + BM25 search, `GRAPH.*` Cypher, `TXN.*`
cross-store ACID, workspaces, durable message queues, bi-temporal MVCC,
and an embedded web console. Any Redis client connects out of the box.

Primary target is **Linux** with io_uring (`monoio`); a `tokio` runtime is
available for portability. **macOS** is a first-class development platform
(kqueue via monoio); production should target Linux (see
[`docs/PRODUCTION-CONTRACT.md`](docs/PRODUCTION-CONTRACT.md) Tier 1/2).

> **Production-grade architecture, pre-1.0 maturity.** Single-node Moon is
> recommended for production caching, AI (vector / graph / feature-store)
> workloads, and Redis-compatible OLTP. **Multi-node clustering and
> multi-shard master PSYNC are alpha** — see
> [Production readiness](#production-readiness) for the honest GA matrix.
> Wire protocol and on-disk format are LTS as of v0.2
> ([`docs/STORAGE-FORMAT-V1.md`](docs/STORAGE-FORMAT-V1.md)); CLI flags may
> evolve until v1.0. [Open an issue](https://github.com/pilotspace/moon/issues)
> if something breaks.

## Contents

- [Why Moon](#why-moon) — the architecture in six bullets
- [Install](#install) — packages, one-liner, Docker, source
- [Quick start](#quick-start) — build, run, connect, config
- [Benchmarks](#benchmarks) — headline numbers ([full report →](BENCHMARK.md))
- [Moon vs Redis vs Valkey](#moon-vs-redis-vs-valkey) — pick the right one ([deep dive →](docs/comparison-valkey.md))
- [Features at a glance](#features-at-a-glance) — the full command surface
- [Web console](#web-console) — embedded 7-view UI
- [Production readiness](#production-readiness) — what's GA, what isn't
- [Documentation](#documentation) — the full doc map
- [Development](#development) · [Credits](#credits) · [License](#license)

## Why Moon

- **Thread-per-core, zero shared state.** Each shard owns its event loop, DashTable, WAL writer, and Pub/Sub registry. No global locks; cross-shard dispatch is a lock-free SPSC channel.
- **Dual runtime.** Monoio (`io_uring` on Linux, `kqueue` on macOS) for peak throughput; Tokio for portability and CI. Same binary, feature-gated.
- **Forkless persistence.** RDB snapshots iterate DashTable segments incrementally — no `fork()`, no COW memory spike. AOF is a per-shard WAL v3 with batched fsync; the advantage over Redis grows with pipeline depth.
- **Tiered disk offload.** Keys evicted under `maxmemory` spill to NVMe instead of being deleted, with async write and read-through. 100% crash recovery across all tiers.
- **Memory-optimized types.** `CompactKey` (23-byte SSO), `CompactValue` (16-byte SSO with inline TTL), `HeapString`, B+ tree sorted sets, and per-request bumpalo arenas — **27–35% less RSS** than Redis at 1 KB+ values.
- **AI-native, in-core.** Vector search (HNSW + TurboQuant), BM25 full-text with three-way RRF hybrid fusion, a Cypher property-graph engine, cross-store ACID, workspaces, durable queues, and bi-temporal MVCC — one binary, no module loader.

<p align="center">
  <img src="assets/architecture-comparison.png" alt="Moon vs Redis Architecture" width="100%">
</p>

## Install

Moon ships signed, checksummed packages for Linux, macOS, and Windows. All artifacts are listed on the [latest release page](https://github.com/pilotspace/moon/releases/latest).

**Linux / macOS — one-liner** (detects OS/arch, verifies SHA256 against the signed `SHA256SUMS.txt`, installs to `~/.local/bin`; Linux x86_64 gets the io_uring `monoio` build, aarch64/macOS get `tokio`):

```bash
curl -fsSL https://raw.githubusercontent.com/pilotspace/moon/main/install.sh | sh
# pin: VERSION=v0.2.0 INSTALL_DIR=/usr/local/bin sh install.sh
```

**Windows — PowerShell** (installs `moon.exe` and adds it to PATH; not yet Authenticode-signed, so SmartScreen may prompt "Run anyway"):

```powershell
irm https://raw.githubusercontent.com/pilotspace/moon/main/install.ps1 | iex
```

**Debian / RHEL** — `.deb` and `.rpm` (amd64 + arm64) ship a systemd unit and `/etc/moon/moon.conf`:

```bash
sudo dpkg -i moon_<version>_arm64.deb     # or: sudo rpm -i moon-<version>-1.arm64.rpm
sudo systemctl enable --now moon
```

**Docker:**

```bash
docker run -p 6379:6379 ghcr.io/pilotspace/moon:latest
```

**Cargo** — add the [`moondb`](https://crates.io/crates/moondb) Rust client library to your project (`moondb = "0.2"`); build the server itself [from source](#quick-start).

**Homebrew** — a tap is planned for v0.2.x (formula template in [`packaging/homebrew/`](packaging/homebrew/)).

**Verify downloads** — every artifact is checksummed in `SHA256SUMS.txt` and signed with keyless [cosign](https://docs.sigstore.dev/) (`.sig` + Fulcio `.crt`):

```bash
cosign verify-blob SHA256SUMS.txt \
  --signature SHA256SUMS.txt.sig --certificate SHA256SUMS.txt.crt \
  --certificate-identity-regexp 'github.com/pilotspace/moon' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Quick start

### Build from source

```bash
git clone https://github.com/pilotspace/moon.git
cd moon
cargo build --release          # needs Rust stable (edition 2024) + cmake (for TLS)

# Defaults: bind 127.0.0.1:6379, single shard (best non-pipelined throughput)
./target/release/moon

# Production flags (--shards 0 = auto-detect from CPU count; changing the
# count on an existing AOF dir needs a migration — see
# docs/runbooks/shard-count-change.md)
./target/release/moon \
  --port 6379 --shards 8 \
  --appendonly yes --appendfsync everysec \
  --maxmemory 8g --maxmemory-policy allkeys-lfu
```

> `--maxmemory` is a **whole-instance** cap: with `--shards M`, each shard
> evicts against `maxmemory / M`, so total RSS converges on the value you
> set. Omit it and Moon auto-caps at ~80% of RAM with `allkeys-lru`
> (`--maxmemory 0` for unlimited).

### Config file

Moon reads a Redis-style config file (precedence: **CLI flags → conf file → defaults**):

```bash
cp packaging/moon.conf.example /etc/moon/moon.conf
./target/release/moon /etc/moon/moon.conf          # redis convention
./target/release/moon --config /etc/moon/moon.conf # or explicit flag
./target/release/moon /etc/moon/moon.conf --check-config  # validate, don't start
```

### Connect with any Redis client

```bash
redis-cli -p 6379
127.0.0.1:6379> SET hello world
OK
127.0.0.1:6379> HSET user:1 name Alice age 30
(integer) 2
127.0.0.1:6379> HEXPIRE user:1 3600 FIELDS 1 age    # per-field TTL (Valkey 9.0 parity)
1) (integer) 1
127.0.0.1:6379> FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA emb VECTOR HNSW 6 DIM 384 TYPE FLOAT32 DISTANCE_METRIC COSINE
OK
```

### Docker

Multi-stage [cargo-chef](https://github.com/LukeMathWalker/cargo-chef) build on a [distroless](https://github.com/GoogleContainerTools/distroless) runtime (~41 MB image):

```bash
docker build -t moon .
docker run -d -p 6379:6379 -v moon-data:/data moon moon --bind 0.0.0.0 --appendonly yes
```

See [docs/quickstart.mdx](docs/quickstart.mdx) for TLS setup, Docker Compose, and client-library examples.

### Python SDK — 10 lines to vector search

```bash
pip install moondb
```

```python
from moondb import MoonClient, encode_vector

client = MoonClient(host="localhost", port=6379, decode_responses=True)
client.vector.create_index("docs", dim=384, metric="COSINE", prefix="doc:")
client.hset("doc:1", mapping={"title": "Hello", "vec": encode_vector([0.1] * 384)})

for r in client.vector.search("docs", [0.1] * 384, k=5):
    print(r.key, r.score, r.fields)
```

Full tutorials in [examples/](examples/): [RAG](examples/rag-quickstart/), [Semantic Cache](examples/semantic-cache/), [GraphRAG](examples/graphrag/), [AI Agent Tools](examples/ai-agent-tools/).

## Benchmarks

Headline numbers vs Redis 8.6.1, peak throughput, co-located client/server.
**Full methodology, ARM64, vector, graph, persistence, and latency tables
are in [BENCHMARK.md](BENCHMARK.md)** and [docs/benchmarks.mdx](docs/benchmarks.mdx).

### Peak throughput (GCloud c3-standard-8, x86_64, monoio io_uring)

| Workload                            |   Moon | Redis 8.6.1 |
|-------------------------------------|-------:|------------:|
| Peak GET (c=50, p=64)               | 5.11M  | 2.98M (1.72×) |
| Peak SET (c=50, p=64)               | 3.50M  | 1.82M (1.92×) |
| GET, production defaults (AOF + offload) | 4.76M | 2.46M (1.93×) |
| Memory, values ≥ 1 KB               |   —    | **27–35% less** |
| Crash recovery (SIGKILL, 5K keys)   | 100%   | 100% (parity) |

On **ARM64** (Neoverse-N1) Moon runs ~2.1–2.2× Redis on the same harness.
For **vector** (12.7K search QPS @ 384d), **graph** (23× FalkorDB bulk
insert, 2.4× Cypher QPS), and **hash-field TTL** (Valkey-parity)
benchmarks, see [BENCHMARK.md](BENCHMARK.md).

> Valkey is not yet head-to-head benched on this harness; its vendor-published
> 2.1M RPS (9 I/O threads, p=10) is quoted for context in the comparison below.

## Moon vs Redis vs Valkey

Three Redis-protocol servers, three bets. Moon competes on a **vertical
moat** — thread-per-core architecture and an AI-native in-core surface.
Valkey competes on a **horizontal moat** — Linux Foundation governance,
every major cloud, drop-in compatibility. Redis OSS is the upstream
reference but ships under SSPL since 2024. Full traced review:
[`docs/comparison-valkey.md`](docs/comparison-valkey.md).

| Dimension                  | **Moon v0.2.0**                  | **Valkey 9.1.0**            | **Redis 8.6.1 (OSS)**     |
|----------------------------|----------------------------------|-----------------------------|----------------------------|
| Language / license         | Rust 2024 / Apache-2.0           | C99 / BSD-3 (LF TSC)        | C99 / **SSPL** since 2024 |
| Threading                  | Thread-per-core, shared-nothing  | Main thread + ≤9 I/O threads | Single-threaded core      |
| I/O driver (Linux)         | io_uring (`monoio`)              | epoll                       | epoll                     |
| Snapshot                   | **Forkless** (segment COW)       | `fork()` + COW              | `fork()` + COW            |
| Vector / BM25 / graph      | **In-core** (HNSW+TQ, BM25, Cypher) | `valkey-search` module   | RediSearch module / none  |
| Cross-store ACID           | `TXN.BEGIN/COMMIT/ABORT`         | None                        | None                      |
| Hash-field TTL             | **Yes** (Valkey-parity)          | **Yes** (9.0+)              | No                        |
| Tiered NVMe offload        | **Yes** (under `maxmemory`)      | No (OSS)                    | No (OSS)                  |
| Multi-node cluster (GA)    | **Alpha** (single-node GA today) | **Production**              | **Production**            |
| Peak single-server GET     | **5.11M/s** (c3-8 x86_64)        | 2.1M RPS (vendor, p=10)     | 2.98M/s (same harness)    |

- **Choose Moon** for single-node peak throughput, ≥1 KB memory efficiency, forkless snapshots, or AI-native workloads (vector / GraphRAG / hybrid retrieval) with cross-store ACID.
- **Choose Valkey** for proven multi-node clusters, managed-cloud-only deployments, or strict Redis 7.2 module-ecosystem compatibility under LF governance.
- **Stay on Redis OSS** for existing RediSearch/RedisJSON/RedisBloom investments or Redis Enterprise features (CRDT active-active, Redis Flash).

## Features at a glance

| Category | Highlights |
|---|---|
| **Data types** | Strings, lists, hashes, sets, sorted sets, streams, HyperLogLog, bitmaps, vectors |
| **Persistence** | Forkless RDB v2, per-shard AOF (`always`/`everysec`/`no`), WAL v3 framing, tiered disk offload |
| **Networking** | RESP2/RESP3, HELLO negotiation, TLS 1.3 (rustls + aws-lc-rs), mTLS, pipelining, client-side caching |
| **Clustering** | 16,384 hash slots, gossip, MOVED/ASK, live slot migration, PSYNC2 replication ([clustering guide](docs/guides/clustering.mdx#replication)), majority-vote failover |
| **Scripting & security** | Lua 5.4 (EVAL/EVALSHA), ACL users/keys/channels/commands, protected mode |
| **Vector search** | `FT.CREATE`/`FT.SEARCH`/`FT.AGGREGATE`, HNSW + TurboQuant 1–8-bit, auto-indexing on `HSET`, hybrid dense+sparse+BM25 |
| **Full-text search** | BM25 inverted index, typo tolerance (Levenshtein-automata), TAG/NUMERIC fields, HIGHLIGHT/SUMMARIZE, three-way RRF fusion |
| **Graph engine** | 14 `GRAPH.*` commands, Cypher subset (MATCH/WHERE/RETURN/CREATE/DELETE/SET/MERGE), hybrid graph+vector, CSR segments, SIMD cosine, temporal-decay traversal |
| **Transactions** | `MULTI`/`EXEC` (Redis compat) + `TXN.BEGIN`/`COMMIT`/`ABORT` (cross-store ACID with undo-log rollback) |
| **Workspaces** | `WS CREATE`/`AUTH`/`LIST` — multi-tenant namespace isolation with transparent key prefixing |
| **Message queues** | `MQ CREATE`/`PUSH`/`POP`/`ACK` — durable queues with dead-letter, triggers, WAL recovery |
| **Temporal / CDC** | Bi-temporal MVCC (`TEMPORAL.SNAPSHOT_AT`), PITR (`--recovery-target-lsn`), `CDC.READ` change stream |
| **Web console** | 7-view React app embedded in the binary, served at `/ui/` |
| **Observability** | `INFO`, `SLOWLOG`, `COMMAND DOCS`, `OBJECT`, `DEBUG`, `MEMORY`, Prometheus metrics, structured `tracing` logs |

Full command list: [docs/commands.mdx](docs/commands.mdx) · Config flags: [docs/configuration.mdx](docs/configuration.mdx) · Architecture: [docs/architecture.mdx](docs/architecture.mdx).

## Web console

Moon ships an embedded web console — no separate install.

```bash
./target/release/moon --port 6379 --admin-port 9100 --shards 4
# open http://localhost:9100/ui/
```

| View | What it does |
|---|---|
| **Dashboard** | Real-time QPS, P50/P99 latency, memory, clients, keyspace (SSE @ 1 Hz) |
| **Browser** | Namespace tree, virtual-scrolled key list, type-specific editors |
| **Console** | Monaco editor with RESP + Cypher syntax, 233-command autocomplete, multi-tab |
| **Vectors** | 3D UMAP projection, HNSW layer overlay, KNN search with distance rings |
| **Graph** | Force-directed 3D layout, Cypher editor, node/edge inspector |
| **Memory** | Keyspace treemap, slowlog table, command stats |
| **Help** | Getting-started guide with seed examples |

Populate all views with
`python3 scripts/seed-console-fixtures.py --resp-port 6379 --admin-port 9100`
(seeds KV + 50K vectors + 10K graph nodes). For production auth, pass
`--console-auth-required --console-auth-secret … --console-cors-origin …
--console-rate-limit …`.

## Production readiness

Honest matrix of where Moon is today. Read alongside
[`docs/PRODUCTION-CONTRACT.md`](docs/PRODUCTION-CONTRACT.md) (machine-checkable
GA exit criteria) and [`docs/OPERATOR-GUIDE.md`](docs/OPERATOR-GUIDE.md)
(memory accounting, sizing, runbooks).

**Recommended for production today**

- **Single-node deployments** — Linux aarch64 (Tier 1) or x86_64 (Tier 2), `--shards N` master.
- **Read replication** — `--shards 1` master with any `--shards N` replica topology (single-shard PSYNC2, wired since v0.1.10).
- **AI workloads** — vector, BM25, GraphRAG, semantic cache, hybrid retrieval. All in-core, all RDB/WAL durable, crash-recovery validated.
- **Cache + feature store** — honest durability modes (`always`/`everysec`/`no`), forkless snapshots, tiered NVMe offload under `maxmemory`.
- **Crash recovery** — 100% survived across 7 persistence configs and 5K-key SIGKILL workloads (RDB v2 + WAL v3 + multi-part AOF + cold tier).

**Not yet GA — avoid for production**

- **Multi-node clustering** (16K-slot gossip, MOVED/ASK, failover) — protocol code exists but **PSYNC2 atomic slot migration is not soak-tested**. Scheduled for a v0.2.x follow-up.
- **Multi-shard master PSYNC** — single-shard only today ([RFC](.planning/rfcs/multi-shard-replication-design.md)).
- **`CDC.SUBSCRIBE` push channel** and **zero-snapshot PITR (P3c)** — `CDC.READ` polling is ready; push/live-LSN are deferred.
- **GPU vector acceleration** (`gpu-cuda`) — kernel scaffold only.
- **Performance SLOs** in [`docs/PRODUCTION-CONTRACT.md`](docs/PRODUCTION-CONTRACT.md) are `[provisional]` until the 24-h HDR-histogram rig validates them. Treat the benchmarks above as point-in-time measurements, not committed SLOs.

**Operator gotchas** — multi-shard needs load (`clients ≥ 25 × shards` on
pipeline ≥ 16); bill on **RSS, not VSZ** (see
[OPERATOR-GUIDE memory accounting](docs/OPERATOR-GUIDE.md#memory-accounting));
for fair Redis/Valkey comparisons add `--disk-offload disable --appendonly no`.
Full list in [CLAUDE.md](CLAUDE.md) "Gotchas".

### Roadmap

| Milestone | Focus | Status |
|---|---|---|
| **v0.2.0** (shipped) | Hash-field TTL, PITR, `CDC.READ`, signed multi-platform packaging | **GA** |
| **v0.2.x** | Multi-node cluster soak (PSYNC2 + atomic slot migration), CDC push, GPU vectors, SLO lock-in | planned |
| **v1.0** | Every [`PRODUCTION-CONTRACT.md`](docs/PRODUCTION-CONTRACT.md) GA box ticked | gate |

Full release history: [CHANGELOG.md](CHANGELOG.md).

## Documentation

📖 **Full documentation site: [pilotspace.github.io/moon](https://pilotspace.github.io/moon/)** — searchable, with every guide, runbook, and reference.

Start at [docs/index.md](docs/index.md), then follow the trail:

- **Get running** — [Quick start](docs/quickstart.md) · [Configuration](docs/configuration.md) · [Docker](docs/guides/docker.md) · [TLS](docs/guides/tls.md)
- **Understand it** — [Architecture](docs/architecture.md) · [Commands](docs/commands.md) · [Benchmarks](BENCHMARK.md) · [Storage format](docs/STORAGE-FORMAT-V1.md)
- **Build with it** — [SDKs](docs/guides/sdk.md) · [Full-text + vector search](docs/guides/full-text-search.md) · [Transactions](docs/guides/transactions.md) · [Workspaces](docs/guides/workspaces.md) · [Message queues](docs/guides/message-queues.md) · [Temporal](docs/guides/temporal.md)
- **Operate it** — [Operator guide](docs/OPERATOR-GUIDE.md) · [Production contract](docs/PRODUCTION-CONTRACT.md) · [Persistence](docs/guides/persistence.md) · [PITR](docs/guides/pitr.md) · [CDC](docs/guides/cdc.md) · [Clustering](docs/guides/clustering.md) · [Monitoring](docs/guides/monitoring.md) · [Runbooks](docs/runbooks/)
- **Trust it** — [Threat model](docs/THREAT-MODEL.md) · [Unsafe policy](UNSAFE_POLICY.md) · [References & papers](docs/references.md)

## Development

```bash
cargo test --lib                                            # unit tests
cargo fmt --check && cargo clippy -- -D warnings            # lint gate
cargo test --release                                        # full suite
./scripts/test-consistency.sh                               # 132 consistency tests vs Redis (1/4/12 shards)
./scripts/bench-production.sh                               # throughput vs Redis
cargo flamegraph --bin moon -- --port 6399 --shards 1       # profile a hot path
```

Coding rules — unsafe policy, hot-path allocation rules, lock discipline —
are in [CLAUDE.md](CLAUDE.md) and [UNSAFE_POLICY.md](UNSAFE_POLICY.md).
Contribution guide: [CONTRIBUTING.md](CONTRIBUTING.md).

## Credits

Moon stands on systems research and an open-source ecosystem. Headline credits:

- **[Dragonfly](https://github.com/dragonflydb/dragonfly)**, **[ScyllaDB/Seastar](https://github.com/scylladb/seastar)**, **[Garnet](https://github.com/microsoft/garnet)** — thread-per-core shared-nothing architecture.
- **[Dash (VLDB 2020)](https://www.vldb.org/pvldb/vol13/p1147-lu.pdf)** + **[Swiss Table / Abseil](https://abseil.io/about/design/swisstables)** — segmented hash table + SIMD probing behind `DashTable`.
- **[HNSW (arXiv 1603.09320)](https://arxiv.org/abs/1603.09320)** + **[TurboQuant (arXiv 2411.04405)](https://arxiv.org/abs/2411.04405)** — vector graph index and quantization for `FT.SEARCH`.
- **[BM25 (Robertson & Zaragoza 2009)](https://doi.org/10.1561/1500000019)** + **[RRF (Cormack et al., SIGIR 2009)](https://doi.org/10.1145/1571941.1572114)** — full-text ranking and hybrid fusion.
- **[HyperLogLog (Flajolet et al. 2007)](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)** + **[Ertl estimator (2017)](https://arxiv.org/abs/1702.01284)** — cardinality estimation (`PFCOUNT`).
- **[Monoio (ByteDance)](https://github.com/bytedance/monoio)** + **[io_uring (Axboe)](https://kernel.dk/io_uring.pdf)** — thread-per-core io_uring runtime.
- **[Redis Protocol Spec (RESP2/RESP3)](https://redis.io/docs/latest/develop/reference/protocol-spec/)** + **[Redis Cluster Spec](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)** — wire protocol and cluster semantics.

Full list with per-dependency rationale and paper summaries: **[docs/references.mdx](docs/references.mdx)**.

## License

[Apache License 2.0](LICENSE)

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
  <img src="https://img.shields.io/badge/cluster-v0.2%20alpha-yellow" alt="Cluster status">
  <img src="https://img.shields.io/badge/rust-edition%202024-orange" alt="Rust">
  <img src="https://img.shields.io/badge/redis--compatible-RESP2%2FRESP3-red" alt="Protocol">
</p>

<p align="center">
  <a href="#install">Install</a> &bull;
  <a href="#quick-start">Quick start</a> &bull;
  <a href="#why-moon">Why Moon</a> &bull;
  <a href="#benchmarks">Benchmarks</a> &bull;
  <a href="#moon-vs-redis-vs-valkey">Moon vs Redis vs Valkey</a> &bull;
  <a href="#production-readiness">Production readiness</a> &bull;
  <a href="docs/index.mdx">Docs</a> &bull;
  <a href="CHANGELOG.md">Changelog</a>
</p>

---

> **Production-grade architecture, pre-1.0 maturity.** Single-node Moon
> (`--shards N` master, `--shards 1` for replication-eligible workloads)
> is recommended for production caching, vector / graph / feature-store
> workloads, and Redis-compatible OLTP. Multi-node clustering and
> multi-shard master PSYNC are **alpha in v0.2** — see
> [Production readiness](#production-readiness) for the honest matrix of
> what is and isn't yet GA. Wire protocol and on-disk format are LTS as of
> v0.2 (`docs/STORAGE-FORMAT-V1.md`); CLI flags may still evolve until
> v1.0. [Open an issue](https://github.com/pilotspace/moon/issues) if
> something breaks.

---

Moon is a clean-room Rust rewrite of a Redis-compatible in-memory data
store with first-class AI primitives. It speaks the Redis wire protocol
(RESP2/RESP3) and implements 230+ commands — every standard Redis data
type plus native `FT.*` vector + BM25 search, `GRAPH.*` Cypher, `TXN.*`
cross-store ACID, workspaces, durable message queues, bi-temporal MVCC,
and an embedded web console. Primary target is **Linux** with io_uring
(`monoio`); a `tokio` runtime is available for portability. **macOS** is a
supported development platform (kqueue via monoio); production
deployments should target Linux (see
[`docs/PRODUCTION-CONTRACT.md`](docs/PRODUCTION-CONTRACT.md) Tier 1/2).
Any Redis client connects out of the box.

## Why Moon

- **Thread-per-core, zero shared state.** Each shard owns its own event loop, DashTable, WAL writer, and Pub/Sub registry. No global locks; cross-shard dispatch is a lock-free SPSC channel.
- **Dual runtime.** Monoio (`io_uring` on Linux, `kqueue` on macOS) for peak throughput; Tokio for portability and CI. Same binary, feature-gated.
- **Forkless persistence.** RDB snapshots iterate DashTable segments incrementally — no fork(), no COW memory spike. AOF is a per-shard WAL with batched fsync; the advantage over Redis grows with pipeline depth.
- **Tiered disk offload.** Keys evicted under `maxmemory` spill to NVMe instead of being deleted, with async write and read-through. 100% crash recovery across all tiers.
- **Memory-optimized types.** `CompactKey` (23-byte SSO), `CompactValue` (16-byte SSO with inline TTL), `HeapString`, B+ tree sorted sets, and per-request bumpalo arenas — **27–35% less RSS** than Redis at 1 KB+ values.

> **Operator note:** Moon's resident memory (RSS) is what you bill for; virtual
> memory (VSZ) reserved by the allocator is not. See
> [docs/OPERATOR-GUIDE.md#memory-accounting](docs/OPERATOR-GUIDE.md#memory-accounting)
> for the full VSZ-vs-RSS guide, `MEMORY DOCTOR` interpretation, and
> `--memory-arenas-cap` / `mimalloc-alt` tuning knobs.

- **In-process vector search.** `FT.CREATE` / `FT.SEARCH` with HNSW + TurboQuant 4/8-bit quantization. **12.7K search QPS** at 384d COSINE on GCloud x86_64.
- **BM25 full-text search.** `FT.AGGREGATE` with GROUPBY/REDUCE, typo tolerance (Levenshtein fuzzy), TAG and NUMERIC field types, HIGHLIGHT/SUMMARIZE, and three-way RRF hybrid fusion (BM25 + dense + sparse).
- **Property graph engine.** 14 `GRAPH.*` commands with Cypher subset, hybrid graph+vector queries, SIMD-accelerated traversal, and memory-mapped CSR segments. **23× FalkorDB insert, 2.4× Cypher QPS.**
- **Cross-store ACID transactions.** `TXN.BEGIN` / `TXN.COMMIT` / `TXN.ABORT` for atomic writes across KV, vector, and graph stores with undo-log rollback.
- **Workspace partitioning.** `WS CREATE` / `WS AUTH` — multi-tenant namespace isolation with transparent key prefixing and per-shard registries.
- **Durable message queues.** `MQ CREATE` / `MQ PUSH` / `MQ POP` / `MQ ACK` — at-least-once delivery with dead-letter queues, debounced triggers, and WAL-backed crash recovery.
- **Bi-temporal MVCC.** `TEMPORAL.SNAPSHOT_AT` / `TEMPORAL.INVALIDATE` — point-in-time queries across KV (`FT.SEARCH AS_OF`) and graph (`GRAPH.QUERY VALID_AT`), plus temporal-decay traversal scoring (`GRAPH.QUERY --decay`, `FT.NAVIGATE DECAY`) that biases path finding toward recently created edges.
- **Embedded web console.** 7-view React UI (Dashboard, Browser, Console, Vectors, Graph, Memory) served at `/ui/` — zero deployment, one binary. REST + WebSocket + SSE gateway with Bearer auth and rate limiting.

<p align="center">
  <img src="assets/architecture-comparison.png" alt="Moon vs Redis Architecture" width="100%">
</p>

## Benchmarks

Measured vs Redis 8.6.1 (peak throughput) and Redis 8.0.2 + Valkey 9.1.0
(hash-TTL surface, the only workload where all three were head-to-head
benchmarked). Co-located client and server, pipeline depth tuned per
row. Full methodology and reproduction steps in
[BENCHMARK.md](BENCHMARK.md) and [docs/benchmarks.mdx](docs/benchmarks.mdx).
Valkey peak-throughput columns are intentionally blank — a head-to-head
peak-RPS bench on identical hardware has not yet been run; the
[Moon vs Redis vs Valkey](#moon-vs-redis-vs-valkey) section quotes
Valkey's vendor-published 2.1M RPS (9 I/O threads, p=10) for context.

### Peak throughput (GCloud c3-standard-8, x86_64, monoio io_uring)

| Workload                                       |   Moon | Redis 8.6.1 | Valkey 9.1.0 |
|------------------------------------------------|-------:|------------:|-------------:|
| Peak GET (c=50, p=64)                          | 5.11M  | 2.98M (1.72×) | not yet benched |
| Peak SET (c=50, p=64)                          | 3.50M  | 1.82M (1.92×) | not yet benched |
| GET, production defaults (AOF + disk-offload)  | 4.76M  | 2.46M (1.93×) | not yet benched |
| GET, max durability (`fsync=always`)           | 4.85M  | 2.45M (1.98×) | not yet benched |
| Memory, values ≥ 1 KB                          | —      | **27–35 % less** | not yet benched* |
| Crash recovery (SIGKILL, 5K keys)              | 100 %  | 100 % (parity)| 100 % (parity, vendor-claimed) |

*Valkey 9.1 raised the embstr threshold to 128 B; below ~64 B Valkey
9.1 may be tighter than Moon. A head-to-head re-bench across the value
size curve is on the v0.2 roadmap.

### ARM64 (GCloud t2a-standard-8, Neoverse-N1)

| Workload                         |   Moon | Redis |  Ratio |
|----------------------------------|-------:|------:|:------:|
| Peak GET (c=50, p=64)            | 3.47M  | 1.58M | **2.20×** |
| Peak SET (c=50, p=64)            | 2.42M  | 1.15M | **2.10×** |
| GET, production defaults          | 3.45M  | 1.61M | **2.14×** |

### Vector search (50K × 384d, COSINE, GCloud x86_64)

|                    |   Moon  | Redis (RediSearch) | Qdrant |
|--------------------|--------:|-------------------:|-------:|
| Insert rate        | **8.2K/s** | ~4.0K/s         | ~6.6K/s |
| Search QPS         | **12.7K**  | 3.8K             | 982    |
| Recall@10 (MiniLM) | 0.92    | 0.95              | 0.96   |

### Graph engine (2K nodes, 6K edges, GCloud x86_64)

|                    |   Moon  | FalkorDB |
|--------------------|--------:|---------:|
| Cypher QPS         | **2.4×** | 1×      |
| Native API QPS     | **19×**  | N/A     |
| Bulk insert        | **23×**  | 1×      |

### Hash-field TTL — Valkey 9.0/9.1 parity

> **Status:** ships in **v0.2.0**. Not present in `v0.1.12` or earlier.

OrbStack moon-dev, n=200K c=50, median of 3. Three-way comparison on the per-field TTL surface added in v0.2.0. Full methodology + 26 scenarios in [docs/perf/2026-05-27-hash-ttl-3way-bench.md](docs/perf/2026-05-27-hash-ttl-3way-bench.md); reproducible via [scripts/bench-hash-ttl-3way.sh](scripts/bench-hash-ttl-3way.sh).

| Command          | Pipeline | Moon  | Redis 8.0.2 | Valkey 9.1.0 |
|------------------|----------|------:|------------:|-------------:|
| `HGET` plain     | p=16     | 2.11M | 2.08M       | 2.11M        |
| `HSET` plain     | p=16     | 1.56M | 1.89M       | 1.77M        |
| `HEXPIRE`        | p=16     | 1.53M | N/A         | 1.64M        |
| `HTTL`           | p=16     | 1.57M | N/A         | 1.75M        |
| `HGETEX EX`      | p=1      | 250K  | N/A         | 251K         |
| `HGETEX` no-mode | p=1      | 250K  | N/A         | 253K         |

Plain Hash HGET p=16 ties Redis (1.01×) and Valkey (1.00×). HEXPIRE-family Moon vs Valkey: 0.90–0.99× across the surface (Valkey leads HEXPIRE p=16 by 7 %, HTTL p=16 by 10 %; HGETEX hits 0.99× parity). Redis 8.x has no HEXPIRE-family — Moon is the only Redis-compatible alternative aside from Valkey. The internal `HashWithTtl` HGET / HLEN paths use a cached `min_expiry_ms` for an O(1) fast path that brings them to 1.03× of plain `Hash` (was 80× slower pre-fix; see PR #126).

## Moon vs Redis vs Valkey

Three Redis-protocol-compatible servers, three different bets. Moon
competes on a **vertical moat** — thread-per-core architecture and an
AI-native in-core surface. Valkey competes on a **horizontal moat** —
Linux Foundation governance, every major cloud, drop-in compatibility.
Redis OSS continues as the upstream reference but ships under SSPL since
March 2024. The deep architectural review is in
[`docs/comparison-valkey.md`](docs/comparison-valkey.md) (~22 KB,
traced to source).

| Dimension                            | **Moon v0.1.12 / 0.2.0-alpha** | **Valkey 9.1.0**            | **Redis 8.6.1 (OSS)**     |
|--------------------------------------|--------------------------------|-----------------------------|----------------------------|
| Language / license                   | Rust 2024 / Apache-2.0         | C99 / BSD-3-Clause (LF TSC) | C99 / **SSPL** since 2024 |
| Threading model                      | Thread-per-core, shared-nothing | Single main thread + ≤9 I/O threads | Single-threaded core (+ I/O threads) |
| I/O driver (Linux)                   | io_uring (`monoio`)            | epoll only                  | epoll only                |
| Snapshot                             | **Forkless** (segment-level COW) | `fork()` + COW              | `fork()` + COW            |
| AOF / WAL                            | Per-shard WAL v3 + **per-shard AOF** | Single global AOF         | Single global AOF         |
| Tiered NVMe disk offload             | **Yes** (under `maxmemory`)    | No (OSS)                    | No (OSS — Redis Flash is Enterprise) |
| Vector search                        | **In-core** HNSW + TurboQuant 1-8 bit | `valkey-search` module, FP32 only | RediSearch module |
| Full-text BM25                       | **In-core**                    | `valkey-search` module      | RediSearch module         |
| Property graph (Cypher)              | **In-core** 14 `GRAPH.*` cmds  | None                        | None (FalkorDB separate)  |
| Cross-store ACID                     | `TXN.BEGIN/COMMIT/ABORT`       | None                        | None                      |
| Hash-field TTL (`HEXPIRE`-family)    | **Yes** (Valkey-parity, v0.2-alpha) | **Yes** (9.0+)         | No                        |
| PITR + CDC                           | `--recovery-target-lsn` + `CDC.READ` (v0.2-alpha) | None | None |
| Embedded web console                 | **Yes** (7-view React, in-binary) | Valkey Admin GUI 1.0 (separate) | Redis Insight (separate) |
| Managed cloud offerings              | None (yet)                     | AWS, GCP, Oracle, Aiven, … | Redis Cloud (vendor)      |
| Multi-node cluster, soak-tested      | **v0.2 alpha** (single-node GA today) | **Production**         | **Production**            |
| Atomic slot migration                | Planned (v0.2)                 | Yes (9.0)                   | No                        |
| Peak single-server throughput        | **5.11M GET/s** (c3-8 x86_64)  | 2.1M RPS (9 I/O threads, p=10, vendor) | 2.98M GET/s (c3-8, same harness as Moon) |

### When to choose Moon

- Single-node Redis-compatible workloads where peak throughput,
  memory efficiency at ≥1 KB values, or **forkless snapshots** matter.
- AI-native applications: vector search, GraphRAG, semantic cache,
  hybrid BM25 + dense + sparse retrieval — all in one binary, no module
  loader, with cross-store ACID across KV / vector / graph.
- Workloads that benefit from **tiered NVMe offload** under `maxmemory`
  instead of LRU-eviction-then-rebuild.

### When to choose Valkey

- Multi-node clusters with proven 1000+ node operational mileage.
- Managed-cloud-only deployments (every major cloud offers Valkey).
- Strict drop-in compatibility with the Redis 7.2 module ecosystem
  (`valkey-json`, `valkey-bloom`, `valkey-search`, `valkey-ldap`).
- Risk-averse environments where Linux Foundation governance is a
  procurement requirement.

### When to stay on Redis OSS

- Existing investments in RediSearch / RedisJSON / RedisBloom under
  RLEC, or pre-SSPL-tolerance OSS Redis.
- Specific Redis Enterprise features (CRDT active-active, Redis Flash)
  that Moon and Valkey OSS do not match.

## Install

### Linux / macOS — one-liner

```bash
curl -fsSL https://raw.githubusercontent.com/pilotspace/moon/main/install.sh | sh
```

Detects OS/arch, downloads the latest release tarball, verifies its SHA256
against the signed `SHA256SUMS.txt`, and installs to `~/.local/bin`
(`/usr/local/bin` when run as root). Linux x86_64 gets the io_uring
(`monoio`) build; Linux aarch64 and macOS get the `tokio` build.
Overrides: `VERSION=v0.2.0 INSTALL_DIR=/opt/bin sh install.sh`.

### Windows — PowerShell

```powershell
irm https://raw.githubusercontent.com/pilotspace/moon/main/install.ps1 | iex
```

Installs `moon.exe` to `%LOCALAPPDATA%\moon\bin` (or `%ProgramFiles%\moon\bin`
as Administrator) and adds it to your user PATH. The binary is not yet
Authenticode-signed — SmartScreen may prompt "Run anyway" (signing is
planned for v0.3.0).

### Debian / RHEL packages

`.deb` and `.rpm` packages (amd64 + arm64) ship with a systemd unit and
`/etc/moon/moon.conf`:

```bash
# grab the package for your arch from the latest release page:
# https://github.com/pilotspace/moon/releases/latest
sudo dpkg -i moon_<version>_arm64.deb     # or: sudo rpm -i moon-<version>-1.arm64.rpm
sudo systemctl enable --now moon
```

### Docker

```bash
docker run -p 6379:6379 ghcr.io/pilotspace/moon:latest
```

### Verify downloads

Every artifact is checksummed in `SHA256SUMS.txt` and signed with
[cosign](https://docs.sigstore.dev/) (keyless). To verify:

```bash
cosign verify-blob --signature SHA256SUMS.txt.sig SHA256SUMS.txt \
  --certificate-identity-regexp 'github.com/pilotspace/moon' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

### From source

See [Quick start](#quick-start) below.

---

## Quick start

### Prerequisites

- [Rust](https://rustup.rs/) stable toolchain (edition 2024)
- `cmake` (required by `aws-lc-rs` for TLS)

### Build and run

```bash
git clone https://github.com/pilotspace/moon.git
cd moon
cargo build --release

# Defaults: bind 127.0.0.1:6379, single shard (best non-pipelined throughput)
./target/release/moon

# Or with production flags (--shards 0 = auto-detect from CPU count;
# changing the count on an existing AOF dir requires a migration — see
# docs/runbooks/shard-count-change.md)
./target/release/moon \
  --port 6379 \
  --shards 8 \
  --appendonly yes --appendfsync everysec \
  --maxmemory 8g --maxmemory-policy allkeys-lfu
```

> **`--maxmemory` is a whole-instance cap.** With `--shards M`, each
> shard enforces eviction against `maxmemory / M`, so total RSS converges
> on the value you set regardless of shard count. `CONFIG GET maxmemory`
> and `INFO` report the whole-instance value (Redis-compatible); a startup
> log line shows the resolved per-shard budget. If `--maxmemory` is omitted,
> Moon auto-caps at ~80% of detected RAM with `allkeys-lru` (pass
> `--maxmemory 0` for unlimited).

### Config file

Moon supports a Redis-style configuration file so you can move all flags out of
your systemd unit / launchd plist:

```bash
# Use the bundled example as a starting point
cp packaging/moon.conf.example /etc/moon/moon.conf
$EDITOR /etc/moon/moon.conf

# Pass the conf file as the first argument (redis convention)
./target/release/moon /etc/moon/moon.conf

# Or with --config
./target/release/moon --config /etc/moon/moon.conf
```

**Precedence (highest → lowest): CLI flags → conf file → built-in defaults.**
CLI flags always win, so you can override any conf option at the command line:

```bash
# conf says port 6379, but this instance listens on 7380
./target/release/moon /etc/moon/moon.conf --port 7380
```

Validate your conf file without starting the server:

```bash
./target/release/moon /etc/moon/moon.conf --check-config
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
127.0.0.1:6379> HEXPIRE user:1 3600 FIELDS 1 age   # Valkey 9.0 per-field TTL (v0.2.0-alpha; build from `main`)
1) (integer) 1
127.0.0.1:6379> HTTL user:1 FIELDS 1 age           # v0.2.0-alpha
1) (integer) 3600
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

### Client SDKs

**Python** — [`moondb`](https://pypi.org/project/moondb/) on PyPI:

```bash
pip install moondb
```

**Rust** — [`moondb`](https://crates.io/crates/moondb) on crates.io:

```toml
[dependencies]
moondb = "0.1"
tokio = { version = "1", features = ["full"] }
```

### Python SDK (10 lines to vector search)

```bash
pip install moondb
```

```python
from moondb import MoonClient, encode_vector

client = MoonClient(host="localhost", port=6379, decode_responses=True)

# Create a vector index (HNSW, 384 dimensions, cosine similarity)
client.vector.create_index("docs", dim=384, metric="COSINE", prefix="doc:")

# Store a document -- auto-indexed on HSET
client.hset("doc:1", mapping={"title": "Hello", "vec": encode_vector([0.1] * 384)})

# Search by vector similarity
results = client.vector.search("docs", [0.1] * 384, k=5)
for r in results:
    print(r.key, r.score, r.fields)
```

See [examples/](examples/) for complete tutorials: [RAG Quickstart](examples/rag-quickstart/), [Semantic Cache](examples/semantic-cache/), [GraphRAG](examples/graphrag/), [AI Agent Tools](examples/ai-agent-tools/).

## Moon Console (Web UI)

Moon ships an embedded web console at `http://localhost:9100/ui/` — no separate install needed.

```bash
# Start Moon with the admin port enabled
./target/release/moon --port 6379 --admin-port 9100 --shards 4
```

Open `http://localhost:9100/ui/` in your browser. The console has 7 views:

| View | What it does |
|---|---|
| **Dashboard** | Real-time QPS, latency P50/P99, memory, clients, keyspace — driven by SSE at 1 Hz |
| **Browser** | Namespace tree, virtual-scrolled key list, type-specific editors for all 6 data types |
| **Console** | Monaco editor with RESP + Cypher syntax, 233-command autocomplete, multi-tab, history |
| **Vectors** | 3D UMAP projection of vector indexes, HNSW layer overlay, KNN search with distance rings |
| **Graph** | Force-directed 3D layout of graph data, Cypher query editor, node/edge inspector |
| **Memory** | Keyspace treemap, slowlog table, command stats |
| **Help** | Getting Started guide with seed examples |

### Seed data to explore all views

Use the built-in seed script to populate KV, vector, and graph data:

```bash
# Install Python deps (one-time)
pip install redis numpy

# Seed 2000 KV keys, 50K vectors (384d), 10K graph nodes + 30K edges
python3 scripts/seed-console-fixtures.py \
  --resp-port 6379 \
  --admin-port 9100 \
  --kv-count 2000 \
  --vector-count 50000 \
  --graph-nodes 10000 \
  --graph-edges 30000
```

Or seed manually via the Console view (Cmd+Enter to execute):

```
# KV data with namespaces
SET user:1:name "Alice"
SET user:1:email "alice@example.com"
HSET session:1 user_id 1 created_at 1712345678 ttl 3600
LPUSH queue:emails "welcome-alice" "verify-alice"
SADD tags:user:1 "admin" "early-adopter" "beta"
ZADD leaderboard 9500 "alice" 8200 "bob" 7100 "charlie"

# Vector index (auto-indexed on HSET)
FT.CREATE embeddings ON HASH PREFIX 1 doc: SCHEMA v VECTOR HNSW 6 DIM 384 TYPE FLOAT32 DISTANCE_METRIC COSINE

# Graph data
GRAPH.CREATE social
GRAPH.ADDNODE social alice Person '{"name":"Alice","age":30}'
GRAPH.ADDNODE social bob Person '{"name":"Bob","age":25}'
GRAPH.ADDEDGE social alice bob FOLLOWS '{"since":"2024"}'
GRAPH.QUERY social "MATCH (a:Person)-[:FOLLOWS]->(b) RETURN a.name, b.name"
```

### Auth and CORS (production)

```bash
# Enable Bearer auth + CORS allowlist
./target/release/moon \
  --port 6379 \
  --admin-port 9100 \
  --console-auth-required \
  --console-auth-secret "your-secret-key" \
  --console-cors-origin "https://your-domain.com" \
  --console-rate-limit 100 \
  --console-rate-burst 200
```

## Features at a glance

| Category | Highlights |
|---|---|
| **Data types** | Strings, lists, hashes, sets, sorted sets, streams, HyperLogLog, bitmaps, vectors |
| **Persistence** | Forkless RDB, per-shard AOF (`always`/`everysec`/`no`), WAL v2 framing, tiered disk offload |
| **Networking** | RESP2/RESP3, HELLO negotiation, TLS 1.3 (rustls + aws-lc-rs), mTLS, pipelining, client-side caching |
| **Clustering** | 16,384 hash slots, gossip, MOVED/ASK, live slot migration, PSYNC2 replication (v0.1.x: `--shards 1` master only — see [clustering guide](docs/guides/clustering.mdx#replication)), majority-vote failover |
| **Scripting & security** | Lua 5.4 (EVAL/EVALSHA), ACL users/keys/channels/commands, protected mode |
| **Vector search** | `FT.CREATE`/`FT.SEARCH`/`FT.AGGREGATE`, HNSW + TurboQuant 4-bit, auto-indexing on `HSET`, hybrid dense+sparse+BM25 |
| **Full-text search** | BM25 inverted index, typo tolerance (Levenshtein), TAG and NUMERIC fields, HIGHLIGHT/SUMMARIZE, three-way RRF fusion |
| **Graph engine** | 14 `GRAPH.*` commands, Cypher subset (MATCH/WHERE/RETURN/CREATE/DELETE/SET/MERGE), hybrid graph+vector queries, CSR segments, SIMD cosine |
| **Transactions** | `MULTI`/`EXEC` (Redis compat) + `TXN.BEGIN`/`TXN.COMMIT`/`TXN.ABORT` (cross-store ACID with undo-log rollback) |
| **Workspaces** | `WS CREATE`/`WS AUTH`/`WS LIST` — multi-tenant namespace isolation with transparent key prefixing |
| **Message queues** | `MQ CREATE`/`MQ PUSH`/`MQ POP`/`MQ ACK` — durable queues with dead-letter, triggers, WAL recovery |
| **Temporal** | `TEMPORAL.SNAPSHOT_AT`/`TEMPORAL.INVALIDATE` — bi-temporal MVCC for KV and graph point-in-time queries |
| **Web console** | Dashboard, Browser, Console, Vector Explorer, Graph Explorer, Memory view — embedded in binary, served at `/ui/` |
| **Observability** | `INFO`, `SLOWLOG`, `COMMAND DOCS`, `OBJECT`, `DEBUG`, `MEMORY`, Prometheus metrics, structured `tracing` logs |

Full command list: [docs/commands.mdx](docs/commands.mdx). Configuration flags: [docs/configuration.mdx](docs/configuration.mdx). Architecture deep-dive: [docs/architecture.mdx](docs/architecture.mdx). Guides: [transactions](docs/guides/transactions.mdx), [workspaces](docs/guides/workspaces.mdx), [message queues](docs/guides/message-queues.mdx), [temporal queries](docs/guides/temporal.mdx), [full-text search](docs/guides/full-text-search.mdx).

## Development

```bash
# Unit tests (2,797+ tests)
cargo test --lib

# Full CI matrix (native macOS + Linux via OrbStack)
cargo fmt --check && cargo clippy -- -D warnings && cargo test --release

# Data-consistency tests vs Redis as ground truth (132 tests, 1/4/12 shards)
./scripts/test-consistency.sh

# Throughput comparison vs Redis
./scripts/bench-production.sh

# Flamegraph a hot path
cargo flamegraph --bin moon -- --port 6399 --shards 1
```

Contribution guide and coding rules (unsafe policy, hot-path allocation rules, lock discipline) are in [CLAUDE.md](CLAUDE.md) and [UNSAFE_POLICY.md](UNSAFE_POLICY.md).

## Production readiness

Honest matrix of where Moon is today (v0.2.0). Read alongside
[`docs/PRODUCTION-CONTRACT.md`](docs/PRODUCTION-CONTRACT.md) (the
machine-checkable GA exit criteria) and
[`docs/OPERATOR-GUIDE.md`](docs/OPERATOR-GUIDE.md) (memory accounting,
sizing, runbooks).

### Recommended for production today

- **Single-node deployments** — Linux aarch64 (Tier 1) or Linux x86_64
  (Tier 2). `--shards N` master, one process, one node.
- **Read replication** — `--shards 1` master with any `--shards N`
  replica topology. Single-shard PSYNC2 is wired end-to-end since
  v0.1.10.
- **AI workloads** — vector search (HNSW + TurboQuant), BM25 full-text,
  GraphRAG, semantic caching, hybrid retrieval. All in-core, all
  RDB / WAL durable, all crash-recovery validated.
- **Cache + feature store** — durability modes are honest
  (`always` / `everysec` / `no` with documented recovery bounds), forkless
  snapshots remove the Redis fork-COW RSS spike, tiered NVMe offload
  under `maxmemory` keeps working sets larger than RAM.
- **Crash recovery** — 100 % survived across 7 persistence
  configurations and 5 K-key SIGKILL workloads. RDB v2 + WAL v3 +
  multi-part AOF + tiered cold tier all participate.

### Not yet GA — avoid for production

- **Multi-node clustering** (16 K-slot gossip, MOVED/ASK, failover) —
  protocol-compatible code exists but **PSYNC2 atomic slot migration
  is not soak-tested**. Valkey 9.0 shipped this; Moon has not.
  Scheduled for v0.2.
- **Multi-shard master PSYNC** — single-shard only today; multi-shard
  master replication is RFC'd in
  [`.planning/rfcs/multi-shard-replication-design.md`](.planning/rfcs/multi-shard-replication-design.md).
- **PITR live-snapshot LSN wiring** (P3c) and **`CDC.SUBSCRIBE` push
  channel** (C3b) — `CDC.READ` polling is alpha-ready; push and
  zero-snapshot PITR are deferred to v0.2 follow-ups.
- **GPU vector acceleration** (`gpu-cuda` feature) — kernel scaffold
  exists; production kernels not yet shipping.
- **macOS native** — first-class development platform with full feature
  set minus io_uring, but production deployments should target Linux
  per
  [`docs/PRODUCTION-CONTRACT.md`](docs/PRODUCTION-CONTRACT.md) tiers.
- **Performance SLO numbers in
  [`docs/PRODUCTION-CONTRACT.md`](docs/PRODUCTION-CONTRACT.md)** — marked
  `[provisional]` until the Phase 97 24-h HDR-histogram rig validates
  them on reference hardware. Use the benchmarks above as point-in-time
  measurements, not committed SLOs.

### Operator gotchas worth knowing before you deploy

- **Multi-shard scaling needs load.** Aim for `clients ≥ 25 × shards`
  on pipeline ≥ 16 workloads; below that, multiple shards under-subscribe
  the dispatch loop and a single shard wins. Random keyspaces with
  `p=1` benefit less than `{tag}`-co-located keys (see
  [`CLAUDE.md`](CLAUDE.md) "Gotchas" + the v0.1.12 multi-shard memo).
- **Fairness flags for benchmarking against Redis / Valkey** —
  `--disk-offload disable --appendonly no` removes Moon's durability
  overhead (~26 % on SET p=64) so comparisons are apples-to-apples.
- **Memory accounting** — bill on RSS, not VSZ. `MEMORY DOCTOR`,
  `moon_memory_bytes{kind=…}` Prometheus gauge, and
  [`docs/OPERATOR-GUIDE.md#memory-accounting`](docs/OPERATOR-GUIDE.md#memory-accounting)
  cover the full VSZ-vs-RSS guide and tuning knobs
  (`--memory-arenas-cap`, `mimalloc-alt`).
- **RDB hash-field TTL trailer** is RDB v2 only — older v1 readers stop
  after the hash body and silently drop per-field TTLs. Pin storage
  format to `v1` (the umbrella covering v2/v3 sub-formats) per
  [`docs/STORAGE-FORMAT-V1.md`](docs/STORAGE-FORMAT-V1.md).

### Roadmap

| Milestone        | Focus                                                                                       | Status      |
|------------------|---------------------------------------------------------------------------------------------|-------------|
| **v0.2.0**       | Multi-node clustering soak (PSYNC2 + atomic slot migration); PITR P3c; CDC push (`SUBSCRIBE`); packaged installs (macOS/Linux/Windows) | released    |
| **v0.2.x**       | GPU vector acceleration (`gpu-cuda`); operator runbooks; full SLO lock-in (`PERF-01..05`)    | planned     |
| **v1.0**         | Every [`PRODUCTION-CONTRACT.md`](docs/PRODUCTION-CONTRACT.md) GA exit-criteria box ticked    | gate        |

What's in `v0.2.0` (v0.1.0 → v0.2.0, 14 months of work):

> Hash-field TTL, PITR + CDC, and packaged installers ship in **v0.2.0**
> (the latest tag). Everything below was already in `v0.1.12` GA.

**Shipped in v0.1.12 (single-node production-grade):**

- Forkless persistence (RDB v2 + per-shard WAL v3 + multi-part AOF).
- Tiered disk offload (RAM → NVMe) with 100 % crash recovery.
- In-process vector search (HNSW + TurboQuant 1–8-bit) — `FT.*` surface.
- BM25 full-text search + three-way RRF hybrid fusion (BM25 + dense + sparse).
- Property graph engine with Cypher subset (14 `GRAPH.*` commands).
- Cross-store ACID (`TXN.BEGIN` / `COMMIT` / `ABORT`) across KV, vector,
  graph.
- Workspaces, durable message queues, bi-temporal MVCC.
- Web console (7-view React app, embedded in binary).
- Thread-per-core dispatch optimization (5.11M GET/s on x86_64).

**Added in v0.2.0-alpha (on `main`, untagged):**

- Hash-field TTL — Valkey 9.0 / 9.1 parity, O(1) HGET / HLEN fast path
  (`HEXPIRE`, `HEXPIREAT`, `HPEXPIRE`, `HPEXPIREAT`, `HEXPIRETIME`,
  `HPEXPIRETIME`, `HTTL`, `HPTTL`, `HPERSIST`, `HGETEX`, `HGETDEL`).
- PITR — `--recovery-target-lsn` deterministic WAL replay-to-LSN.
- CDC — `CDC.READ` pull-mode change stream (push-mode planned for v0.2.0
  GA).
- Multi-node clustering soak (PSYNC2 + atomic slot migration).

## Production Readiness Contract

Moon's v1.0 promises — per-command-class SLOs, durability modes, supported
platforms, security guarantees, and a machine-checkable GA exit-criteria
checklist — live in
**[`docs/PRODUCTION-CONTRACT.md`](docs/PRODUCTION-CONTRACT.md)**. Every
v0.1.3+ hardening phase ticks off items on that checklist; nothing
promotes to `v1.0-rc1` until every box is green. The contract is the
single source of truth for what Moon owes you in production.

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

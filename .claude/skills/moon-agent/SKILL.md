---
name: moon-agent
description: Use Moon from an AI agent. Two modes — OPERATE (start a local instance, pick flags, health-check, shut down) and USE (drive it as an agent data plane: KV+TTL scratch state, FT.* vector search for semantic recall, session-aware retrieval, semantic cache, full-text, Cypher graph, pub/sub events). Use when an agent needs Moon as memory or a vector store, when wiring Moon into an agent app, or when starting/inspecting a local Moon. Args: operate | use | smoke (default: use).
---

Moon is a Redis-compatible server with a native vector engine, graph engine, and full-text
index. For an agent that means one process covers scratch state, semantic recall, relations,
and events — no separate vector DB.

Talk to it three ways, in order of preference:

| Layer | Use for |
|---|---|
| `moondb` Python SDK (`sdk/python`) | everything — it wraps the FT.*/GRAPH wire commands in typed helpers |
| `redis-cli` | health probes, one-off inspection, debugging |
| any `redis` client | if the SDK is unavailable; Moon speaks RESP2/RESP3 |

---

## Mode: OPERATE

Start an instance. **Always pass an explicit `--dir`** — an empty `--dir` means the process CWD,
which reloads whatever stale indexes are sitting there.

```bash
# agent-facing default: single shard, durable, isolated data dir
./target/release/moon --port 6399 --shards 1 --dir /tmp/moon-agent --appendonly yes

# throughput/cache mode: no durability
./target/release/moon --port 6399 --shards 1 --dir /tmp/moon-cache --appendonly no
```

No binary yet? Build natively — `cargo build --release`, or `--profile release-fast`
(thin LTO, ~3-5x faster to compile) when you just need something to run against.

**Pick `--shards` deliberately.** `--shards 1` is the right default for an agent workload:
single-shard wins on non-pipelined traffic, and **vector `FILTER` expressions only work at
`--shards 1`** — a multi-shard instance answers `FILTER not supported in multi-shard mode yet`
(measured, see Gotchas). Add shards only for pipelined/AOF throughput.

Health-check before using it, and confirm you reached *your* server rather than some other
listener on the port:

```bash
redis-cli -p 6399 -t 3 PING                       # -> PONG
redis-cli -p 6399 INFO server | grep -E 'moon_version|process_id|tcp_port'
```

`moon_version` present means it is Moon, not a stray `redis-server`. Bound every `redis-cli`
with `-t` so a wedged server cannot hang the agent.

Shut down with `redis-cli -p 6399 SHUTDOWN NOSAVE` or `SIGTERM` — both exit sub-second
(measured, `--shards 2`). Wait on the pid with a **timeout and a `kill -9` fallback**; never
block forever on a graceful stop.

Other flags worth knowing: `--protected-mode no` (accept non-loopback), `--maxmemory <bytes>`
plus `--maxmemory-policy allkeys-lru` for a pure cache, `--profile standalone` for a
latency-tuned preset. Full knobs: `docs/guides/tuning.md`, `docs/internal/env-knobs.md`.

---

## Mode: USE

```bash
pip install moondb          # or: pip install -e sdk/python  (from this repo)
```

```python
from moondb import MoonClient, encode_vector
c = MoonClient(host="127.0.0.1", port=6399)
```

`MoonClient` **is** a redis-py client — every standard command works directly — plus five
namespaces: `c.vector`, `c.graph`, `c.session`, `c.cache`, `c.text`.

### Scratch state — KV + TTL

```python
c.set("agent:s1:step", "planning")
c.setex("agent:s1:lease", 60, "held")        # lease that self-expires
c.hset("agent:s1:ctx", mapping={"goal": "ship", "tries": "1"})
c.incr("agent:s1:tries")
```

Give every key a TTL unless it is meant to outlive the run. Namespace by
`agent:<session>:<field>` so a session can be dropped with one prefix scan.

### Semantic recall — vector index

```python
c.vector.create_index("agentmem", prefix="mem:", field_name="vec",
                      dim=384, metric="COSINE")      # also: dtype, m, ef_construction, ef_runtime

c.hset("mem:1", mapping={"vec": encode_vector(embedding),   # MUST be encode_vector()
                         "text": "what the agent learned",
                         "kind": "note"})

hits = c.vector.search("agentmem", query_embedding, k=5, return_fields=["text"])
for h in hits:
    print(h.key, h.score, h.fields)
```

- The vector field must be a **binary f32 blob** — `encode_vector(list_of_floats)`. A plain list
  or string silently fails to index.
- `search` returns `SearchResult(key, score, fields, graph_hops, cache_hit)`.
- **`score` is a distance, not a similarity: lower is closer**, and results come back ascending.
  Quantization means an exact self-match scores near-zero, not exactly zero (measured 0.0102).
- `c.vector.index_info(name).num_docs` confirms documents actually indexed — check it after a
  bulk load rather than trusting the `hset` return.
- `filter_expr="@kind:{note}"` narrows by TAG — **single-shard only**.
- Also available: `recommend(index, positive_keys)`, `navigate`, `expand`, `compact`.

### Session-aware retrieval

Search that accounts for what this session already saw:

```python
c.session.search("agentmem", "sess:a", query_embedding, k=5)
c.session.history("sess:a")          # [] when nothing recorded yet
c.session.set_ttl("sess:a", 300)     # returns False if the session key does not exist
c.session.reset("sess:a")
```

### Semantic cache — skip repeat LLM calls

```python
# the cache key MUST fall under the index PREFIX, or it is never indexed
c.cache.store("agentmem:cache:q1", query_embedding, answer="42", ttl=300)
r = c.cache.lookup("agentmem", "agentmem:cache:", query_embedding, threshold=0.5)
if r.cache_hit:
    use(r.results[0])
```

`lookup` returns `CacheSearchResult(results, cache_hit)`. **Branch on `.cache_hit`** — it still
returns fallback nearest-neighbour `results` on a miss, so a truthiness check on `results`
alone treats every miss as a hit.

**Two things will bite you here:**

1. **The cache key must live under the index `PREFIX`.** `FT.CACHESEARCH` only sees indexed
   hashes. An entry at `cache:q1` with an index on `agentmem:` is never found — `cache_hit` is
   permanently `False` and no error is raised.

2. **`cache_hit` is inverted on `COSINE` / `INNER_PRODUCT` indexes** — moon#748, open as of
   0.8.7. `cache_hit` is true when the nearest entry is *farther* than `threshold`, so a
   near-identical query misses and an unrelated query hits. At the SDK default
   `threshold=0.95` a COSINE cache is wrong for essentially every query.
   **Until it is fixed: build the semantic cache on an `L2` index** (verified correct), or
   ignore `cache_hit` and apply your own distance cut-off on `r.results[0].score`.

### Full-text

```python
c.text.create_text_index(
    "agentdocs",
    [("body", "TEXT", {}), ("kind", "TAG", {"SORTABLE": True})],   # (name, type, opts) tuples
    prefix="doc:",
)
c.hset("doc:1", mapping={"body": "vector search with hnsw graphs", "kind": "note"})
hits = c.text.text_search("agentdocs", "vector", limit=5)   # -> [TextSearchHit(id, score, fields, highlights)]
```

`schema` is a **sequence of 3-tuples**, not a dict — passing a dict raises
`ValueError: too many values to unpack`. Indexing is asynchronous; allow a moment before
searching freshly written docs. `hybrid_search(...)` blends text + dense + sparse vectors.

### Graph — relations between memories

```python
c.graph.create("agentkg")
c.graph.query("agentkg", "CREATE (a:Task {name:'ship'})-[:BLOCKS]->(b:Task {name:'test'}) RETURN a.name")
res = c.graph.query("agentkg", "MATCH (a:Task)-[:BLOCKS]->(b:Task) RETURN a.name, b.name")
res.headers   # ['a.name', 'b.name']
res.rows      # [['ship', 'test']]
```

Cypher via `query` / `ro_query`; `explain` and `profile` for plans. Typed helpers `add_node`
/ `add_edge` / `neighbors` / `vsearch` (vector search seeded from a start node, N hops out).

### Events — pub/sub

```python
p = c.pubsub(); p.subscribe("agent:events")
c.publish("agent:events", "step-done")
msg = p.get_message(timeout=1)       # skip the subscribe confirmation frame first
```

---

## Failure handling

Moon is IO — design for it failing:

- **Timeouts on every call.** `MoonClient(..., socket_timeout=2, socket_connect_timeout=2)`;
  `-t 3` on every `redis-cli`.
- **Retry only idempotent reads.** A retried `INCR` double-counts.
- **Degrade, don't crash.** Semantic cache and recall are optimizations — on
  `redis.exceptions.ConnectionError` the agent should fall through to the uncached path.
- **`MOONERR diskfull`** means the data dir's filesystem is under 5% free. Moon pauses writes.
  Free space or point `--dir` elsewhere; for throwaway test instances `--disk-free-min-pct 0`
  disables the guard.
- **OOM:** with an evicting `--maxmemory-policy` Moon drops keys; under `noeviction` writes
  return `-OOM`. Treat an `-OOM` reply as backpressure, not a bug.

## Gotchas

| Symptom | Cause |
|---|---|
| `FILTER not supported in multi-shard mode yet` | vector `filter_expr` needs `--shards 1` |
| `ValueError: too many values to unpack (expected 3)` | text schema passed as dict; use `(name, type, opts)` tuples |
| Vectors index but never match | field not wrapped in `encode_vector()` |
| Top hit has a *low* score | expected — score is a distance, ascending |
| Cache "hits" every time | branched on `results` instead of `.cache_hit` |
| Cache never hits, no error | cache key not under the index `PREFIX` |
| COSINE cache hits on the *wrong* query | moon#748 — `cache_hit` inverted for COSINE/IP; use `L2` |
| Stale indexes on start | empty `--dir` defaults to CWD; always pass one |
| Search finds nothing right after write | text indexing is async; brief settle needed |

## Verify

`scripts/smoke.py` exercises every flow above against a live instance and reports pass/fail:

```bash
python .claude/skills/moon-agent/scripts/smoke.py 6399
```

Run it after changing Moon or the SDK — it is the fastest way to tell whether a break is in the
server, the SDK, or the calling code.

> Behaviour here was verified against **moon 0.8.7 / moondb 0.1.1 / redis-py 8.1.0** on macOS
> arm64 (2026-08-27): 26 of 29 flows passed first run; the three failures became the
> single-shard `FILTER` constraint and the text-schema tuple shape documented above.
> Probing the cache path then surfaced moon#748 (inverted `cache_hit` on COSINE/IP).

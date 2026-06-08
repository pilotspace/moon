---
title: "Temporal queries"
description: "Bi-temporal MVCC for point-in-time KV and graph queries."
---

# Temporal queries

Moon provides bi-temporal MVCC for point-in-time queries across KV and graph stores. Record wall-clock snapshots, invalidate graph entities at specific times, and query historical state via `AS_OF` and `VALID_AT` clauses.

## Quick start

```bash
redis-cli -p 6379

# Record a temporal snapshot (binds current wall-clock to WAL LSN)
127.0.0.1:6379> TEMPORAL.SNAPSHOT_AT
OK

# ... make data changes ...

# Query vectors at a historical point in time
127.0.0.1:6379> FT.SEARCH idx "*=>[KNN 5 @v $q]" AS_OF 1713394800000 \
    PARAMS 2 q <query_vector> DIALECT 2

# Query graph at a historical point in time
127.0.0.1:6379> GRAPH.QUERY social "MATCH (a:Person) RETURN a.name" \
    VALID_AT 1713394800000

# Invalidate a graph entity (set valid_to timestamp)
127.0.0.1:6379> TEMPORAL.INVALIDATE 42 NODE mygraph
OK
127.0.0.1:6379> TEMPORAL.INVALIDATE 99 EDGE social
OK
```

## Commands

| Command | Description |
|---------|-------------|
| `TEMPORAL.SNAPSHOT_AT` | Record current wall-clock → WAL LSN binding. Takes no arguments |
| `TEMPORAL.INVALIDATE <entity_id> <NODE\|EDGE> <graph_name>` | Set `valid_to` on a graph entity, making it invisible to future temporal queries |

## Temporal query clauses

| Clause | Used in | Description |
|--------|---------|-------------|
| `AS_OF <unix_ms>` | `FT.SEARCH` | Search vectors using the index state at the given timestamp |
| `VALID_AT <unix_ms>` | `GRAPH.QUERY` | Execute Cypher query against graph state valid at the given timestamp |
| `--decay <λ> [--time-weight <w>]` | `GRAPH.QUERY` | Bias `shortestPath()` toward recently created edges (temporal-decay scoring) |
| `DECAY <λ>` | `FT.NAVIGATE` | Penalize graph-expanded hits discovered over stale edges |

## Temporal-decay traversal scoring

Moon stamps every graph edge with its wall-clock creation time (from the
shard-cached clock — no syscall on the insert path). The decay knobs turn
that stamp into a recency bias for traversal — the primitive behind
"prefer what the agent learned recently" in agent memory graphs:

```bash
# shortestPath() cost per edge: |weight| + λ × w × edge_age_seconds
127.0.0.1:6379> GRAPH.QUERY social \
    "MATCH p = shortestPath((a:Person {name: 'A'})-[*..5]->(c:Person {name: 'C'})) RETURN p" \
    --decay 0.1

# Scale the age term independently of λ
127.0.0.1:6379> GRAPH.QUERY social "..." --decay 0.1 --time-weight 2.0

# FT.NAVIGATE: final_score += λ × age_seconds of the discovery edge
127.0.0.1:6379> FT.NAVIGATE idx "*=>[KNN 5 @vec $q]" PARAMS 2 q <vec> HOPS 2 DECAY 0.1
```

- `λ` is a decay rate in **1/seconds**; both surfaces validate it strictly
  (finite, non-negative). `--time-weight` requires `--decay`, and `--decay`
  is rejected on write queries (CREATE, SET, DELETE, MERGE) — it biases
  read-path traversal only.
- The two surfaces apply decay differently: `GRAPH.QUERY --decay` **steers**
  the traversal itself (the age term is part of the Dijkstra edge cost, so
  decay changes which paths get explored), while `FT.NAVIGATE DECAY`
  **re-ranks** hits the graph expansion already discovered — the BFS frontier
  itself is not decay-aware, so nodes reachable only through stale edges
  beyond the expansion budget are penalized, not replaced by fresher
  alternatives. KNN (hop-0) hits are never decay-penalized.
- **Decay off (no flag) is exact distance-only behavior** — the age term
  contributes zero to every edge cost, so path choice is identical to
  pre-decay Moon.
- Edges with an unknown creation time (created before the upgrade, or loaded
  from a pre-v3 segment file) are **neutral**: they pay no age penalty rather
  than being treated as maximally old.
- Stamps survive the full segment lifecycle: mutable graph → frozen CSR
  segment (format v3 stores a per-edge `created_ms` array) → disk → mmap →
  compaction merges.
- This is **transaction-time recency** (when the edge was created), distinct
  from the user-owned `valid_from`/`valid_to` bi-temporal valid-time used by
  `VALID_AT` and `TEMPORAL.INVALIDATE`.

## How it works

### Temporal registry

`TEMPORAL.SNAPSHOT_AT` captures the current Unix millisecond timestamp and the current WAL LSN (Log Sequence Number), storing the binding in a `TemporalRegistry` backed by a BTreeMap for O(log n) range lookups.

### Bi-temporal fields

Graph entities (nodes and edges) carry `valid_from` and `valid_to` timestamps in their metadata. `TEMPORAL.INVALIDATE` sets `valid_to` on a specific entity, and `VALID_AT` queries filter entities whose valid interval contains the requested timestamp.

### Vector temporal queries

`FT.SEARCH ... AS_OF <timestamp>` resolves the timestamp to a WAL LSN via the temporal registry, then searches the vector index using only data that existed at that LSN.

### WAL records

- `TemporalUpsert` (0x35): Versioned KV entries with `valid_from` timestamp
- `GraphTemporal` (0x36): Temporal graph modifications (invalidation, version bumps)

Both record types are replayed on startup to reconstruct the temporal registry and restore entity validity windows.

## Use cases

- **Audit trails**: Query the exact state of data at any historical point.
- **Regulatory compliance**: Prove what data was visible at a specific time.
- **ML reproducibility**: Reproduce feature vectors and graph state used for a past model inference.
- **Soft deletes with temporal visibility**: Invalidate entities instead of deleting them — they remain queryable for historical analysis.

## Limitations

- Temporal snapshots consume memory proportional to the number of snapshots recorded. No automatic GC policy yet.
- `AS_OF` requires at least one `TEMPORAL.SNAPSHOT_AT` recorded at or before the requested timestamp.
- Bi-temporal fields are currently limited to graph entities (nodes/edges). KV temporal versioning uses a sparse index.
- Decay stamps on edges still in the mutable (not yet frozen) segment are re-stamped to replay time after a restart — only CSR-resident edges keep their exact creation time across restarts. Newest edges look new either way, so the bias direction is preserved.

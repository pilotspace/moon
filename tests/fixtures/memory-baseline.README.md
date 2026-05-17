# Memory Baseline Fixture

**File:** `memory-baseline.json`

## Capture Details

- **Date:** 2026-04-27
- **Host:** macOS aarch64 (Apple Silicon)
- **Features:** `runtime-tokio,jemalloc,graph,text-index`
- **Build:** debug profile (unoptimized)
- **Shards:** 1

## Workload

| Category | Count | Details |
|----------|-------|---------|
| String keys | 1,000,000 | via redis-benchmark (random keys) |
| Vector docs | 10,000 | 16-dim FLOAT32, HNSW index |
| Graph nodes | 100 | GRAPH.ADDNODE with :N label |
| Steady-state wait | 60s | After all data loaded |

## 7 Subsystem Kinds

1. `dashtable` - DashTable structural overhead + entry data
2. `hnsw` - Mutable vector segments (brute-force buffer)
3. `csr` - Graph CSR storage + MemGraph SlotMap
4. `wal` - WAL writer buffers (0 when --appendonly no)
5. `sealed` - Immutable vector segments (0 without FT.COMPACT)
6. `replication_backlog` - Replication backlog ring buffer (0 when no replicas)
7. `allocator_overhead` - max(0, RSS - sum(other 6))

## Regeneration

To regenerate the baseline after an intentional memory change:

```bash
bash scripts/bench-memory-steady-state.sh --write-baseline tests/fixtures/memory-baseline.json
```

**Commit convention:** The commit subject MUST include `[memory-baseline-update]` followed by the reason:

```
chore(190-04): [memory-baseline-update] reason: added per-entry metadata field
```

## CI Gate

The `memory-steady-state` job in `.github/workflows/ci.yml` compares every PR's
memory profile against this baseline with a +/-5% per-kind tolerance. If any kind
exceeds the threshold, the job fails with the offending kind and delta percentage.

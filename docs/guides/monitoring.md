---
title: "Monitoring"
description: "Scrape Moon's Prometheus metrics and set up alerting."
---

# Monitoring

Moon exposes a Prometheus-compatible metrics endpoint on its admin HTTP port. This guide covers enabling the admin port, scraping metrics, and setting up basic alerting.

## Enable the admin port

Start Moon with `--admin-port` to expose the HTTP endpoints:

```bash
./target/release/moon --admin-port 9100
```

This serves three endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /metrics` | Prometheus metrics in exposition format |
| `GET /healthz` | Health check -- returns `200 OK` when the server is running |
| `GET /readyz` | Readiness check -- returns `200 OK` when the server is accepting commands |

Verify it is working:

```bash
curl http://127.0.0.1:9100/metrics
curl http://127.0.0.1:9100/healthz
```

## Prometheus configuration

Add Moon as a scrape target in your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: "moon"
    scrape_interval: 15s
    static_configs:
      - targets: ["127.0.0.1:9100"]
        labels:
          instance: "moon-primary"
```

For multiple Moon instances or sharded deployments, list each instance:

```yaml
scrape_configs:
  - job_name: "moon"
    scrape_interval: 15s
    static_configs:
      - targets:
          - "moon-1:9100"
          - "moon-2:9100"
          - "moon-3:9100"
```

## Key metrics

Moon exposes standard Redis-compatible INFO metrics through the Prometheus endpoint. Key metrics to monitor include:

- **`moon_connected_clients`** -- current number of connected clients
- **`moon_used_memory_bytes`** -- the logical memory ledger reported as
  `INFO`'s `used_memory` field, matching real Redis's `used_memory`
  semantics: KV (+ its ColdIndex bookkeeping) + vector/text/graph resident
  bytes, **plus** the Lua script cache and the replication backlog ring.
  This is NOT quite the same figure `--maxmemory` eviction gates on (see
  the elastic-budget note below) -- it is deliberately wider, because real
  Redis's `used_memory` counts Lua scripts and the replication backlog too
  even though neither is data eviction can reclaim.
- **Elastic budget (eviction gate)** -- `ShardDatabases::recompute_elastic_budget`,
  not directly exposed as its own gauge, is the NARROWER figure
  `--maxmemory` eviction actually acts on: KV+ColdIndex+vector+text+graph
  only, with NO Lua/replication-backlog terms. In the overwhelming majority
  of deployments (small script cache, small/no replication backlog) this is
  indistinguishable from `moon_used_memory_bytes`; it only diverges when
  either of those two subsystems grows large, in which case
  `moon_used_memory_bytes` can legitimately read above what eviction is
  bounding -- `MEMORY DOCTOR` prints both figures side by side for exactly
  this reason.
- **`moon_rss_bytes`** -- the process's true OS-level resident set size.
  Under disk-offload this is expected to run noticeably ABOVE
  `moon_used_memory_bytes` -- the remaining gap (RSS minus `used_memory`,
  which now already includes Lua + replication backlog) is allocator arena
  fragmentation, mmap'd page-cache frames serving cold-tier reads, and the
  binary image and thread stacks. **Do not alert on `moon_rss_bytes /
  <maxmemory>` and expect it to track eviction health** -- use
  `moon_used_memory_bytes` for that; use `moon_rss_bytes` (and
  `moon_memory_bytes{kind="allocator_overhead"}` /
  `{kind="pagecache"}`) to watch total OS footprint / capacity planning.
- **`moon_memory_bytes{kind="..."}`** -- the same breakdown `MEMORY DOCTOR`
  prints, per subsystem: `dashtable`, `hnsw` (vector), `text`, `csr`
  (graph), `wal`, `sealed`, `replication_backlog`, `lua_scripts`,
  `pagecache`, `allocator_overhead`. The first six (dashtable/hnsw/text/csr
  + replication_backlog + lua_scripts) sum to `moon_used_memory_bytes`;
  `pagecache` and `allocator_overhead` are the remaining components that
  explain the `moon_rss_bytes` gap.
- **`moon_commands_processed_total`** -- total commands processed (rate = ops/sec)
- **`moon_keyspace_hits_total`** -- successful key lookups
- **`moon_keyspace_misses_total`** -- failed key lookups (cache miss rate)
- **`moon_evicted_keys_total`** -- keys evicted due to maxmemory
- **`moon_expired_keys_total`** -- keys removed by expiration

### `used_memory` vs RSS under disk-offload

A disk-offloaded deployment intentionally keeps most of its dataset off the
hot ledger (on disk, in `heap-*.mpf` files), read back on demand. Three
figures that are easy to conflate but answer different questions:

| Field | Answers | Gated by `--maxmemory`? |
|---|---|---|
| Elastic budget (`recompute_elastic_budget`, shown in `MEMORY DOCTOR` only) | "How much of my logical dataset does the eviction system currently think is resident?" | Yes -- this is the exact number eviction reads |
| `used_memory` (INFO) / `moon_used_memory_bytes` | "How much allocator-attributed memory does Redis-parity `used_memory` report?" (elastic budget + Lua scripts + replication backlog) | Mostly -- diverges from the elastic budget only when the script cache or replication backlog is large |
| `used_memory_rss` (INFO) / `moon_rss_bytes` | "How much physical RAM does this process actually hold?" | No -- always some amount above `used_memory` |

Before task #56, `used_memory` was implemented as raw process RSS, so all
three rows above were identical and a healthy disk-offload deployment
permanently looked 1.5-3x over its configured cap. If you see this gap in an
older build, it is a reporting bug, not a memory leak -- upgrade rather than
raise `--maxmemory` to compensate.

**Why `used_memory` includes Lua scripts and the replication backlog but
`--maxmemory` eviction does not (adversarial-review finding, task #56):**
real Redis's `used_memory` is "total allocator-attributed memory", not
"memory eviction can reclaim" -- it counts the Lua script cache and the
replication backlog even though eviction never touches either. Moon matches
that semantics for the reported figure (both terms were already tracked as
separate `moon_memory_bytes{kind=...}` gauges, so folding them into
`used_memory` cost nothing new), while keeping the actual eviction gate
(`recompute_elastic_budget`) unchanged and narrower -- eviction has no
mechanism to reclaim Lua bytecode or replication-backlog bytes, so gating on
them would not free anything. If your script cache or replication backlog
is large enough for the two figures to diverge meaningfully, `SCRIPT FLUSH`
and reviewing `repl-backlog-size` are the levers, not raising `--maxmemory`.

## Grafana dashboard

Import the metrics into Grafana for visualization. A minimal dashboard should include:

1. **Operations rate** -- `rate(moon_commands_processed_total[5m])`
2. **Hit rate** -- `moon_keyspace_hits_total / (moon_keyspace_hits_total + moon_keyspace_misses_total)`
3. **Memory usage** -- `moon_used_memory_bytes`
4. **Connected clients** -- `moon_connected_clients`
5. **Eviction rate** -- `rate(moon_evicted_keys_total[5m])`

## Health check integration

Use the `/healthz` and `/readyz` endpoints with your orchestrator:

### Kubernetes

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 9100
  initialDelaySeconds: 5
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /readyz
    port: 9100
  initialDelaySeconds: 5
  periodSeconds: 5
```

### Docker Compose

```yaml
services:
  moon:
    image: moon:latest
    command: ["--port", "6379", "--admin-port", "9100"]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9100/healthz"]
      interval: 10s
      timeout: 5s
      retries: 3
```

## Alerting rules

Example Prometheus alerting rules:

```yaml
groups:
  - name: moon_alerts
    rules:
      - alert: MoonDown
        expr: up{job="moon"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Moon instance {{ $labels.instance }} is down"

      - alert: MoonHighMemory
        # Moon does not (yet) publish a `--maxmemory` gauge -- substitute
        # your instance's configured byte value here. Use
        # `moon_used_memory_bytes` (the gated logical ledger), NOT
        # `moon_rss_bytes` (always higher under disk-offload by design --
        # see "used_memory vs RSS under disk-offload" above).
        expr: moon_used_memory_bytes / <your-maxmemory-bytes> > 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Moon instance {{ $labels.instance }} is above 90% memory"

      - alert: MoonHighEvictionRate
        expr: rate(moon_evicted_keys_total[5m]) > 100
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Moon instance {{ $labels.instance }} is evicting >100 keys/sec"
```

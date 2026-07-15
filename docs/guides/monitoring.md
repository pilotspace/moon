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
- **`moon_used_memory_bytes`** -- the logical memory ledger `--maxmemory`
  eviction actually gates on (KV + its ColdIndex bookkeeping, plus
  vector/text/graph resident bytes). This is the number to compare against
  your configured `--maxmemory`, and the one `INFO`'s `used_memory` field
  also reports.
- **`moon_rss_bytes`** -- the process's true OS-level resident set size.
  Under disk-offload this is expected to run noticeably ABOVE
  `moon_used_memory_bytes` -- the gap is real, legitimate, resident memory
  that `--maxmemory` does not (and should not) gate on: allocator arena
  fragmentation, mmap'd page-cache frames serving cold-tier reads, the
  binary image and thread stacks, the Lua script cache (intentionally
  unbounded -- `SCRIPT FLUSH` is its only reclaim path), and the
  replication backlog ring. **Do not alert on `moon_rss_bytes /
  <maxmemory>` and expect it to track eviction health** -- use
  `moon_used_memory_bytes` for that; use `moon_rss_bytes` (and
  `moon_memory_bytes{kind="allocator_overhead"}` /
  `{kind="pagecache"}`) to watch total OS footprint / capacity planning.
- **`moon_memory_bytes{kind="..."}`** -- the same breakdown `MEMORY DOCTOR`
  prints, per subsystem: `dashtable`, `hnsw` (vector), `text`, `csr`
  (graph), `wal`, `sealed`, `replication_backlog`, `lua_scripts`,
  `pagecache`, `allocator_overhead`. The first four sum to
  `moon_used_memory_bytes`; the rest are the legitimately-outside-the-cap
  components that explain the `moon_rss_bytes` gap.
- **`moon_commands_processed_total`** -- total commands processed (rate = ops/sec)
- **`moon_keyspace_hits_total`** -- successful key lookups
- **`moon_keyspace_misses_total`** -- failed key lookups (cache miss rate)
- **`moon_evicted_keys_total`** -- keys evicted due to maxmemory
- **`moon_expired_keys_total`** -- keys removed by expiration

### `used_memory` vs RSS under disk-offload

A disk-offloaded deployment intentionally keeps most of its dataset off the
hot ledger (on disk, in `heap-*.mpf` files), read back on demand. Two figures
that are easy to conflate but answer different questions:

| Field | Answers | Gated by `--maxmemory`? |
|---|---|---|
| `used_memory` (INFO) / `moon_used_memory_bytes` | "How much of my logical dataset does the eviction system currently think is resident?" | Yes -- this is the number eviction reads |
| `used_memory_rss` (INFO) / `moon_rss_bytes` | "How much physical RAM does this process actually hold?" | No -- always some amount above `used_memory` |

Before task #56, `used_memory` was implemented as raw process RSS, so the two
rows above were identical and a healthy disk-offload deployment permanently
looked 1.5-3x over its configured cap. If you see this gap in an older
build, it is a reporting bug, not a memory leak -- upgrade rather than raise
`--maxmemory` to compensate.

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

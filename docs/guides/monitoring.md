# Monitoring with Prometheus

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
- **`moon_used_memory_bytes`** -- total memory used by the server
- **`moon_commands_processed_total`** -- total commands processed (rate = ops/sec)
- **`moon_keyspace_hits_total`** -- successful key lookups
- **`moon_keyspace_misses_total`** -- failed key lookups (cache miss rate)
- **`moon_evicted_keys_total`** -- keys evicted due to maxmemory
- **`moon_expired_keys_total`** -- keys removed by expiration

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
        expr: moon_used_memory_bytes / moon_maxmemory_bytes > 0.9
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

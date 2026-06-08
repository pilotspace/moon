**Status:** Active (v0.1.13 Wave 1) · **Owner:** Storage/Reclamation · **Version:** v0.1.13

# Reclamation SLO Contract

This document is the operator-facing contract for Moon's storage reclamation subsystem. It defines
six SLOs, the `INFO reclamation` fields used to measure them, alert thresholds that indicate a
breach is imminent or in progress, and remediation pointers into the
[Reclamation Runbook](./reclamation-runbook.md).

Every field listed here is emitted by `INFO reclamation` in v0.1.13. Fields marked
**Wave-2 placeholder** emit `0` in v0.1.13 and are populated by the Wave-2 autovacuum
daemon (v0.1.14). Do **not** alert on a `0` value from a Wave-2 placeholder field — configure
alert rules to fire only when the field is `> 0` and breaches the threshold.

**Related documents:**

- [Reclamation Runbook](./reclamation-runbook.md) — step-by-step triage for SLO breaches
- [PRODUCTION-CONTRACT.md](../PRODUCTION-CONTRACT.md) — overall Moon SLOs (durability, latency, throughput)
- [OPERATOR-GUIDE.md](../OPERATOR-GUIDE.md) — memory accounting and jemalloc tuning
- TODO.md — Wave 1/2 roadmap and wave-acceptance criteria

---

## Quick Reference

| # | SLO Name | Target | Primary INFO Field | Alert Threshold | Runbook Scenario | v0.1.13 Status |
|---|---|---|---|---|---|---|
| 1 | Bloat | dead fraction P99 ≤ 30% | `reclamation_dead_fraction_max` | > 0.35 | [Compaction Not Keeping Up](./reclamation-runbook.md#scenario-4-compaction-not-keeping-up) | Live |
| 2 | WAL size | total ≤ 2× `--max-wal-size` for ≥ 60 s | `reclamation_wal_bytes` | > 2× configured limit for 60 s | [Disk Filling Up](./reclamation-runbook.md#scenario-1-disk-filling-up) | Live |
| 3 | Manifest commit | P99 ≤ 5 ms with ≤ 100 K entries | `reclamation_manifest_tombstones` | tombstones > 80 K | [Query Latency Cliffed](./reclamation-runbook.md#scenario-2-query-latency-cliffed) | Live |
| 4 | Read amplification | vector query fanout P99 ≤ 16 | `reclamation_read_amp_p99` | > 20 | [Query Latency Cliffed](./reclamation-runbook.md#scenario-2-query-latency-cliffed) | Wave-2 placeholder |
| 5 | MVCC pinning | oldest snapshot age ≤ `--mvcc-old-snapshot-threshold-secs` | `reclamation_mvcc_oldest_snapshot_age_secs` | > configured threshold | [OOM / Memory Growth](./reclamation-runbook.md#scenario-3-oom--memory-growth) | Live |
| 6 | Request impact | autovacuum overhead P50 ≤ +5%, P99 ≤ +10% | `reclamation_autovacuum_throttled_due_to_load` | throttled continuously for > 300 s | [Compaction Not Keeping Up](./reclamation-runbook.md#scenario-4-compaction-not-keeping-up) | Wave-2 placeholder |

---

## SLO 1 — Bloat (dead fraction)

**Definition:** The fraction of dead (tombstoned, unreferenced) records in the hottest shard's
segment store must not exceed 30% at P99 when the ingest rate is at or below the compaction
throughput ceiling.

| Attribute | Value |
|---|---|
| **Target** | `reclamation_dead_fraction_max` ≤ 0.30 when ingest ≤ compaction throughput |
| **INFO field** | `reclamation_dead_fraction_max` |
| **Alert threshold** | `> 0.35` for 5 consecutive minutes |
| **Escalation threshold** | `> 0.50` for 2 consecutive minutes (compaction critically behind) |
| **Remediation** | [Scenario 4 — Compaction Not Keeping Up](./reclamation-runbook.md#scenario-4-compaction-not-keeping-up) |
| **v0.1.13 status** | **Live** — emitted by P10 metrics surface |

**Why this matters:** At dead fraction > 30%, vector query read amplification climbs (more
segments must be probed), and graph traversal must skip increasing numbers of dead edges.
At > 50%, a compaction backlog has formed and disk growth is unbounded until compaction catches up.

**Supporting fields:**

```
reclamation_immutable_segments:<n>      # immutable segment count (compaction input)
reclamation_warm_segments:<n>           # segments in warm tier
reclamation_cold_segments:<n>           # segments in cold tier
reclamation_compaction_pending_bytes:<n> # bytes queued for compaction
```

**Alert rule (Prometheus / VictoriaMetrics):**

```promql
moon_reclamation_dead_fraction_max > 0.35
```

If Moon's admin HTTP port is not enabled, poll via:

```bash
redis-cli -p 6399 INFO reclamation | grep reclamation_dead_fraction_max
```

---

## SLO 2 — WAL Size

**Definition:** The combined WAL byte size across all shards must not exceed twice the
configured `--max-wal-size` limit for 60 or more consecutive seconds under any workload.

| Attribute | Value |
|---|---|
| **Target** | `reclamation_wal_bytes` ≤ 2 × `--max-wal-size` (default 2 × 256 MiB = 512 MiB) for any 60-second window |
| **INFO fields** | `reclamation_wal_bytes`, `reclamation_wal_segments` |
| **Alert threshold** | `reclamation_wal_bytes` > 1.5 × `--max-wal-size` for 30 s |
| **Escalation threshold** | `reclamation_write_stall_active` = `true` |
| **Remediation** | [Scenario 1 — Disk Filling Up](./reclamation-runbook.md#scenario-1-disk-filling-up) |
| **v0.1.13 status** | **Live** — emitted by P10 metrics surface |

**Config flags involved:**

```
--max-wal-size              default: 256mb    per-shard WAL size cap
--wal-segment-size          default: 16mb     individual WAL segment size
--wal-max-checkpoint-lag-ms default: 10000    max ms before a checkpoint is forced
--disk-free-min-pct         default: 5        write-stall trigger (% free disk)
```

**Supporting fields:**

```
reclamation_disk_free_bytes:<n>          # free disk on persistence partition
reclamation_write_stall_active:<bool>    # true when writes are stalled
reclamation_write_stall_threshold_pct:<f> # --disk-free-min-pct in effect
```

**What write-stall means:** When `reclamation_write_stall_active` is `true`, write commands
return an error (`WRITE_STALL disk usage too high`) and block new writes until disk is freed.
This is intentional: fail-fast is safer than OOM. See [Scenario 1](./reclamation-runbook.md#scenario-1-disk-filling-up).

---

## SLO 3 — Manifest Commit Latency

**Definition:** The manifest commit latency (dual-root atomic swap) must stay below 5 ms at
P99 when the combined manifest entry count (active + tombstoned) is at or below 100 000.

| Attribute | Value |
|---|---|
| **Target** | Manifest commit P99 ≤ 5 ms with ≤ 100 K entries |
| **INFO fields** | `reclamation_manifest_active`, `reclamation_manifest_tombstones` |
| **Alert threshold** | `reclamation_manifest_tombstones` > 80 000 |
| **Escalation threshold** | `reclamation_manifest_tombstones` > 100 000 |
| **Remediation** | [Scenario 2 — Query Latency Cliffed](./reclamation-runbook.md#scenario-2-query-latency-cliffed) |
| **v0.1.13 status** | **Live** — emitted by P10 metrics surface |

**Config flags involved:**

```
--manifest-tombstone-retain-epochs   default: 2    epochs before tombstone GC
--manifest-tombstone-retain-secs     default: 300  seconds before tombstone GC
```

**Supporting fields:**

```
reclamation_manifest_active:<n>         # active (live) manifest entries
reclamation_manifest_tombstones:<n>     # pending-GC tombstone entries
```

**Root cause when breached:** Tombstones accumulate when segment GC is slower than segment
creation. This typically co-occurs with high dead fraction (SLO 1) and an active compaction
backlog. Reduce `--manifest-tombstone-retain-epochs` to `1` as a short-term mitigation.

---

## SLO 4 — Read Amplification (Vector Queries)

**Definition:** The P99 number of segments probed per vector query (HNSW search fanout)
must stay at or below 16 with default configuration.

| Attribute | Value |
|---|---|
| **Target** | `reclamation_read_amp_p99` ≤ 16 |
| **INFO fields** | `reclamation_read_amp_p99`, `reclamation_segment_fanout_p99`, `reclamation_segment_fanout_p50` |
| **Alert threshold** | `reclamation_read_amp_p99` > 20 |
| **Escalation threshold** | `reclamation_read_amp_p99` > 32 (2× budget — recall degrades measurably) |
| **Remediation** | [Scenario 2 — Query Latency Cliffed](./reclamation-runbook.md#scenario-2-query-latency-cliffed) |
| **v0.1.13 status** | **Wave-2 placeholder** — emits `0` in v0.1.13; populated by v0.1.14 autovacuum daemon |

> **Wave-2 note:** In v0.1.13 `reclamation_read_amp_p99` always emits `0`. Configure your
> alert to fire only when the field is `> 0 AND > 20`. The underlying cause of read
> amplification growth — too many immutable segments — is observable now via
> `reclamation_immutable_segments > 16`.

**Proxy metric available now (v0.1.13):**

```bash
redis-cli -p 6399 INFO reclamation | grep reclamation_immutable_segments
# Alert when > 16 — each immutable segment adds one full probe per FT.SEARCH call
```

**Root cause:** Immutable segment count grows when compaction (Wave-2 `P2`) has not run.
Manual `FT.COMPACT <index>` forces compaction. See [Scenario 2](./reclamation-runbook.md#scenario-2-query-latency-cliffed).

---

## SLO 5 — MVCC Pinning

**Definition:** An alarm must fire when the oldest active snapshot is older than the configured
threshold (`--mvcc-old-snapshot-threshold-secs`, default 600 s). The MVCC subsystem must not
hold unbounded memory due to a single stuck snapshot.

| Attribute | Value |
|---|---|
| **Target** | `reclamation_mvcc_oldest_snapshot_age_secs` ≤ `--mvcc-old-snapshot-threshold-secs` (default 600 s) |
| **INFO fields** | `reclamation_mvcc_oldest_snapshot_age_secs`, `reclamation_mvcc_oldest_snapshot_lag`, `reclamation_mvcc_active`, `reclamation_mvcc_committed` |
| **Alert threshold** | `reclamation_mvcc_oldest_snapshot_age_secs` > configured threshold |
| **Escalation threshold** | `> 2 × threshold` (MVCC committed set likely growing unbounded) |
| **Remediation** | [Scenario 3 — OOM / Memory Growth](./reclamation-runbook.md#scenario-3-oom--memory-growth) |
| **v0.1.13 status** | **Live** — emitted by P10 metrics surface |

**Config flags involved:**

```
--mvcc-old-snapshot-threshold-secs   default: 600   seconds before snapshot is "old"
--mvcc-committed-prune-margin        default: 1000  LSN prune margin
```

**Supporting fields:**

```
reclamation_mvcc_committed:<n>              # committed version entries in RoaringTreemap
reclamation_mvcc_active:<n>                # live (open) snapshots
reclamation_mvcc_oldest_snapshot_lag:<n>   # LSN lag of oldest snapshot
reclamation_mvcc_oldest_snapshot_age_secs:<n>
reclamation_mvcc_zombies_swept_total:<n>   # cumulative zombie sweep count
reclamation_delete_pending_visible_lsn:<n> # LSN below which tombstones are safe to GC
```

**What "MVCC pinning" means:** A long-running `FT.SEARCH` or graph query holds an open
snapshot. The MVCC manager cannot prune committed versions below that snapshot's LSN.
Memory grows proportionally to write throughput × snapshot age. One stuck client can pin
hundreds of megabytes. The `VACUUM MVCC` command (Wave-1 P8) forces a sweep and reports
bytes reclaimed.

---

## SLO 6 — Request Impact (Autovacuum Overhead)

**Definition:** When the autovacuum daemon (Wave-2, v0.1.14) is running, the request latency
overhead it introduces must not exceed +5% on P50 or +10% on P99 relative to a no-vacuum
baseline.

| Attribute | Value |
|---|---|
| **Target** | Autovacuum-induced P50 overhead ≤ +5%, P99 ≤ +10% |
| **INFO fields** | `reclamation_autovacuum_throttled_due_to_load`, `reclamation_autovacuum_last_run_ts`, `reclamation_autovacuum_segments_compacted_total` |
| **Alert threshold** | `reclamation_autovacuum_throttled_due_to_load` = `1` continuously for > 300 s |
| **Escalation threshold** | Throttled for > 1 800 s (30 min) — compaction is materially behind and autovacuum cannot catch up |
| **Remediation** | [Scenario 4 — Compaction Not Keeping Up](./reclamation-runbook.md#scenario-4-compaction-not-keeping-up) |
| **v0.1.13 status** | **Wave-2 placeholder** — emits `0` in v0.1.13; autovacuum daemon ships in v0.1.14 |

> **Wave-2 note:** In v0.1.13, autovacuum is not running and all `reclamation_autovacuum_*`
> fields emit `0`. Compaction is triggered manually via `VACUUM` / `FT.COMPACT`. The SLO
> target applies from v0.1.14 onward. Configure alerts to fire only when
> `reclamation_autovacuum_last_run_ts > 0`.

---

## Measuring the SLOs

### Getting current values

```bash
# Full reclamation section
redis-cli -p 6399 INFO reclamation

# Targeted field check
redis-cli -p 6399 INFO reclamation | grep -E \
  'reclamation_dead_fraction_max|reclamation_wal_bytes|reclamation_manifest_tombstones|reclamation_read_amp_p99|reclamation_mvcc_oldest_snapshot_age_secs|reclamation_autovacuum_throttled_due_to_load'
```

### Recommended alert rules (condensed)

```yaml
# Prometheus / VictoriaMetrics alert examples
# Adjust instance label and scrape target to match your deployment.

- alert: MoonBloatHigh
  expr: moon_reclamation_dead_fraction_max > 0.35
  for: 5m
  annotations:
    summary: "Moon dead fraction exceeds 35% — compaction backlog forming"
    runbook: "https://your-docs/docs/operations/reclamation-runbook.md#scenario-4"

- alert: MoonWalOversize
  expr: moon_reclamation_wal_bytes > moon_config_max_wal_bytes * 1.5
  for: 30s
  annotations:
    summary: "Moon WAL bytes > 1.5× limit — approaching write-stall"
    runbook: "https://your-docs/docs/operations/reclamation-runbook.md#scenario-1"

- alert: MoonManifestTombstoneHigh
  expr: moon_reclamation_manifest_tombstones > 80000
  for: 5m
  annotations:
    summary: "Moon manifest tombstones > 80K — manifest GC is behind"
    runbook: "https://your-docs/docs/operations/reclamation-runbook.md#scenario-2"

- alert: MoonImmutableSegmentsHigh
  # Proxy for SLO 4 until v0.1.14 populates reclamation_read_amp_p99
  expr: moon_reclamation_immutable_segments > 16
  for: 2m
  annotations:
    summary: "Moon immutable segment count > 16 — FT.SEARCH read amplification elevated"
    runbook: "https://your-docs/docs/operations/reclamation-runbook.md#scenario-2"

- alert: MoonMvccSnapshotOld
  expr: moon_reclamation_mvcc_oldest_snapshot_age_secs > 600
  for: 1m
  annotations:
    summary: "Moon MVCC oldest snapshot > 600 s — possible stuck client pinning memory"
    runbook: "https://your-docs/docs/operations/reclamation-runbook.md#scenario-3"
```

---

## SLO Compliance Boundaries

These SLOs apply under the following conditions. Behavior outside these bounds is unspecified:

| Condition | Assumption |
|---|---|
| Shard count | 1–16 shards |
| Ingest rate | ≤ compaction throughput ceiling (not an overload scenario) |
| Snapshot age | No snapshot older than `--mvcc-old-snapshot-threshold-secs` |
| Disk headroom | `reclamation_disk_free_bytes` > 10% of partition (above `--disk-free-min-pct`) |
| Platform | Linux aarch64 / x86_64, kernel ≥ 6.1 (Tier 1/2 per [PRODUCTION-CONTRACT.md](../PRODUCTION-CONTRACT.md)) |

**Out-of-scope:** macOS (dev-only), tokio runtime (CI parity only), WAL sync mode (`appendfsync=always`).

---

## Versioning

| Version | Changes |
|---|---|
| v0.1.13 | Initial publication. SLOs 1-3, 5 are live. SLOs 4, 6 are Wave-2 placeholders. |
| v0.1.14 (planned) | SLOs 4, 6 become live when autovacuum daemon and read-amp metrics ship. |

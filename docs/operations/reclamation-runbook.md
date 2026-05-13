**Status:** Active (v0.1.13 Wave 1) · **Owner:** Storage/Reclamation · **Version:** v0.1.13

# Reclamation Runbook

This runbook covers the four reclamation-induced failure modes that operators are most likely to
encounter. Each scenario opens with violated SLOs, shows which `INFO reclamation` fields to check,
provides a decision tree, and lists concrete mitigation commands.

**Companion documents:**

- [Reclamation SLO Contract](./reclamation-slo.md) — SLO definitions, targets, and alert rules
- [Disk Full During WAL Rotation](../runbooks/disk-full-during-wal-rotation.md) — WAL-rotation-specific disk ENOSPC
- [OOM During Snapshot](../runbooks/oom-during-snapshot.md) — BGSAVE memory spike
- [PRODUCTION-CONTRACT.md](../PRODUCTION-CONTRACT.md) — overall durability and uptime guarantees
- [TODO.md](../../TODO.md) — Wave 1/2 roadmap and acceptance criteria

**Wave-1 commands available (v0.1.13 P8):**

The manual reclamation commands below ship as part of Wave-1 P8 in the same v0.1.13 release:

```
VACUUM                           # reclaim across all subsystems, returns bytes reclaimed per category
VACUUM VECTOR <index> [VERBOSE]  # compact a specific vector index
VACUUM GRAPH [VERBOSE]           # compact graph dead edges
VACUUM MVCC [VERBOSE]            # sweep MVCC zombies and prune committed versions
KILL SNAPSHOT <snapshot-id>      # force-close a stuck snapshot (use with care — see SLO 5)
```

---

## How to Start Any Diagnosis

Before diving into a scenario, run these two commands. They give you the full picture in under
30 seconds:

```bash
# 1. Snapshot the reclamation section
redis-cli -p 6399 INFO reclamation

# 2. Identify which SLO is breached (scan the key fields)
redis-cli -p 6399 INFO reclamation | grep -E \
  'disk_free_bytes|wal_bytes|write_stall_active|dead_fraction_max|manifest_tombstones|immutable_segments|read_amp_p99|mvcc_oldest_snapshot_age_secs|compaction_pending_bytes|autovacuum_throttled'
```

Then match the output to the scenario table below:

| What you see | Go to |
|---|---|
| `reclamation_write_stall_active:true` OR `reclamation_disk_free_bytes` critically low | [Scenario 1](#scenario-1-disk-filling-up) |
| `reclamation_manifest_tombstones` > 80 K OR `reclamation_immutable_segments` > 16 | [Scenario 2](#scenario-2-query-latency-cliffed) |
| `reclamation_mvcc_oldest_snapshot_age_secs` > 600 OR process RSS growing while keyspace is stable | [Scenario 3](#scenario-3-oom--memory-growth) |
| `reclamation_dead_fraction_max` > 0.35 OR `reclamation_compaction_pending_bytes` high but not stalled | [Scenario 4](#scenario-4-compaction-not-keeping-up) |

---

## Scenario 1: Disk Filling Up

**Violated SLOs:** [SLO 2 — WAL Size](./reclamation-slo.md#slo-2--wal-size)

**Symptoms:**

- Write commands return `ERR WRITE_STALL disk usage too high`
- `reclamation_write_stall_active:true` in `INFO reclamation`
- Disk partition at > 95% usage (i.e., below `--disk-free-min-pct`, default 5%)
- Moon logs: `write stall activated` at `WARN` level

This scenario is the **reclamation lens** on disk pressure: the disk is filling because
compaction has fallen behind and dead bytes have not been reclaimed. This is distinct from
WAL-rotation ENOSPC (see [Disk Full During WAL Rotation](../runbooks/disk-full-during-wal-rotation.md),
which covers hard ENOSPC before write-stall kicks in).

### Step 1: Confirm write-stall and free headroom

```bash
# Check write-stall flag and free bytes
redis-cli -p 6399 INFO reclamation | grep -E 'write_stall|disk_free_bytes|wal_bytes|wal_segments'

# Check OS-level disk usage on the persistence partition
df -h <persistence-dir>    # --persistence-dir value
```

**Decision:**

```
reclamation_write_stall_active:true?
├── YES → write-stall is active, new writes are rejected
│   ├── reclamation_disk_free_bytes < 2 GB → CRITICAL: proceed to Step 2 immediately
│   └── reclamation_disk_free_bytes ≥ 2 GB → stall may be a threshold misconfiguration; see Step 4
└── NO → write-stall not yet active but disk is trending full → proceed to Step 3 proactively
```

### Step 2: Immediate space recovery (write-stall active)

```bash
# Force WAL checkpoint to seal and archive segments (frees in-flight WAL space)
redis-cli -p 6399 DEBUG RELOAD

# Run VACUUM to reclaim dead bytes across all subsystems
redis-cli -p 6399 VACUUM
# Output: "reclaimed: vector=<N>MB graph=<N>MB mvcc=<N>MB manifest=<N>MB total=<N>MB"

# If vector indexes are large, compact them explicitly
redis-cli -p 6399 VACUUM VECTOR <index-name> VERBOSE

# Recheck disk free
redis-cli -p 6399 INFO reclamation | grep disk_free_bytes
```

Write-stall clears automatically when `reclamation_disk_free_bytes` rises above the threshold.
You do **not** need to restart Moon.

### Step 3: Proactive mitigation (disk trending full, no stall yet)

```bash
# How much WAL is outstanding?
redis-cli -p 6399 INFO reclamation | grep -E 'wal_bytes|wal_segments'

# Run compaction to free dead bytes before stall kicks in
redis-cli -p 6399 VACUUM

# Reduce WAL retention if checkpoint lag is acceptable
# (increase checkpoint frequency → smaller WAL footprint)
# Edit config and SIGHUP or restart with:
#   --wal-max-checkpoint-lag-ms 5000   (default 10000)
```

### Step 4: If write-stall threshold is misconfigured

```bash
# Check configured threshold
redis-cli -p 6399 INFO reclamation | grep write_stall_threshold_pct
# If threshold is too aggressive (e.g. 20%) and disk is not actually critical:
# Restart with a lower threshold, e.g.:
#   --disk-free-min-pct 5   (default)
```

### Step 5: Prevent recurrence

- Alert at `reclamation_disk_free_bytes` < 20% of partition (before stall triggers at 5%).
- Place WAL and vector segment directories on a dedicated partition with monitoring.
- If compaction consistently falls behind ingest, this is [Scenario 4](#scenario-4-compaction-not-keeping-up) — plan for Wave-2 autovacuum.

**See also:** [Disk Full During WAL Rotation](../runbooks/disk-full-during-wal-rotation.md)
for ENOSPC that occurs before write-stall activates.

---

## Scenario 2: Query Latency Cliffed

**Violated SLOs:** [SLO 3 — Manifest Commit Latency](./reclamation-slo.md#slo-3--manifest-commit-latency), [SLO 4 — Read Amplification](./reclamation-slo.md#slo-4--read-amplification-vector-queries)

**Symptoms:**

- `FT.SEARCH` latency has increased step-function (a "cliff", not gradual degradation)
- P99 read latency is elevated but P50 is normal — indicates multi-segment probe overhead
- `reclamation_immutable_segments` > 16
- `reclamation_manifest_tombstones` > 80 000 (manifest commit adds latency to every write)
- `reclamation_read_amp_p99` > 20 (v0.1.14+ only; in v0.1.13 use `immutable_segments` as proxy)

### Step 1: Identify the root cause

```bash
redis-cli -p 6399 INFO reclamation | grep -E \
  'immutable_segments|warm_segments|cold_segments|graph_segments|manifest_tombstones|manifest_active|read_amp'
```

**Decision tree:**

```
reclamation_immutable_segments > 16?
├── YES → vector query read amplification is elevated
│   └── FT.SEARCH query is hitting N segments instead of ≤ 16 → goto Step 2 (vector compaction)
└── NO
    └── reclamation_manifest_tombstones > 80000?
        ├── YES → manifest commit latency is elevated → goto Step 3 (manifest GC)
        └── NO → latency cliff may not be reclamation-related
                 Check: redis-cli SLOWLOG GET 10 and server CPU/memory
```

### Step 2: Compact vector indexes (too many immutable segments)

```bash
# List indexes (if you have multiple)
redis-cli -p 6399 FT.INFO <index-name>

# Compact a specific index — forces mutable → immutable segment flush and HNSW graph build
redis-cli -p 6399 FT.COMPACT <index-name>

# Or use VACUUM VECTOR for each index with verbose output
redis-cli -p 6399 VACUUM VECTOR <index-name> VERBOSE
# Output shows segments merged and recall delta

# Recheck segment count
redis-cli -p 6399 INFO reclamation | grep immutable_segments
```

**Caution:** `FT.COMPACT` is a no-op if the mutable segment has fewer entries than
`COMPACT_THRESHOLD`. If the command is silent, either set `COMPACT_THRESHOLD` to match
your dataset size or use `VACUUM VECTOR` which bypasses this gate.

After compaction, segment count should drop to 1–3. Re-run the query to confirm latency returned.

### Step 3: Force manifest GC (too many tombstones)

```bash
# Check tombstone count and retention config
redis-cli -p 6399 INFO reclamation | grep -E 'manifest_tombstones|manifest_active'

# Run VACUUM to trigger manifest GC (tombstones below retain threshold are pruned)
redis-cli -p 6399 VACUUM

# If tombstones are not dropping, the retain-epochs or retain-secs window is too long.
# Restart with a shorter window (hot-path config — requires restart):
#   --manifest-tombstone-retain-epochs 1   (default 2)
#   --manifest-tombstone-retain-secs 60    (default 300)
```

### Step 4: Confirm latency restored

```bash
# Run a representative FT.SEARCH and observe response time
time redis-cli -p 6399 FT.SEARCH <index-name> "*" LIMIT 0 10

# Recheck read-amp proxy
redis-cli -p 6399 INFO reclamation | grep -E 'immutable_segments|read_amp_p99'
```

### Step 5: Prevent recurrence

- Alert on `reclamation_immutable_segments > 16` (available v0.1.13).
- Alert on `reclamation_manifest_tombstones > 80000`.
- Plan for Wave-2 `P2` (immutable segment merge daemon) which automates this compaction.
  Until then, schedule `FT.COMPACT` during off-peak windows if ingest is continuous.

---

## Scenario 3: OOM / Memory Growth

**Violated SLOs:** [SLO 5 — MVCC Pinning](./reclamation-slo.md#slo-5--mvcc-pinning)

**Symptoms:**

- Process RSS growing steadily while keyspace size is stable
- `reclamation_mvcc_committed` count is rising without bound
- `reclamation_mvcc_oldest_snapshot_age_secs` > 600 (default threshold)
- In extreme cases: OOM killer terminates Moon (check `dmesg | grep -i oom`)

This scenario is distinct from the BGSAVE memory spike (see [OOM During Snapshot](../runbooks/oom-during-snapshot.md)).
Here the growth is gradual and driven by MVCC version accumulation behind a pinned snapshot.

### Step 1: Confirm MVCC pinning

```bash
redis-cli -p 6399 INFO reclamation | grep -E \
  'mvcc_committed|mvcc_active|mvcc_oldest_snapshot_age_secs|mvcc_oldest_snapshot_lag|mvcc_zombies_swept_total|delete_pending_visible_lsn'
```

**Decision tree:**

```
reclamation_mvcc_oldest_snapshot_age_secs > 600?
├── YES → at least one snapshot is older than the threshold
│   ├── reclamation_mvcc_active > 0 → open snapshots exist (expected; check age)
│   └── reclamation_mvcc_committed growing → versions are accumulating behind pinned snapshot
│       → goto Step 2 (identify and close the stuck snapshot)
└── NO
    └── reclamation_mvcc_committed very large but age < threshold?
        → high write throughput with short-lived snapshots is normal; check ingest rate
        → if committed count > 1M, goto Step 3 (force prune)
```

### Step 2: Identify and close the stuck snapshot

```bash
# Get the age and lag of the oldest snapshot
redis-cli -p 6399 INFO reclamation | grep -E 'mvcc_oldest|delete_pending_visible_lsn'

# If the snapshot is from a long-running FT.SEARCH or graph query,
# identify the client connection holding it:
redis-cli -p 6399 CLIENT LIST | grep -v "cmd=ping\|cmd=client"
# Look for a client with a very old "age" or an in-flight FT.SEARCH

# Force-close the stuck snapshot (Wave-1 P8 command)
# WARNING: this aborts the query associated with the snapshot.
# Use the snapshot ID from the MVCC manager log or INFO output.
redis-cli -p 6399 KILL SNAPSHOT <snapshot-id>
```

### Step 3: Force MVCC sweep

```bash
# Trigger a MVCC zombie sweep and committed-set prune
redis-cli -p 6399 VACUUM MVCC VERBOSE
# Output: "swept=<N> zombies, pruned=<N> committed entries, freed=<N>MB"

# Recheck committed count
redis-cli -p 6399 INFO reclamation | grep mvcc_committed
```

The `VACUUM MVCC` command runs the same sweep that Wave-2 autovacuum will run automatically.
In v0.1.13, it must be triggered manually.

### Step 4: Monitor RSS after the fix

```bash
# Check process RSS on Linux
grep VmRSS /proc/$(pgrep moon)/status

# On macOS (dev only)
ps -o pid,rss -p $(pgrep moon)
```

RSS should stabilize within a few minutes of closing the pinned snapshot.

### Step 5: Prevent recurrence

- Alert on `reclamation_mvcc_oldest_snapshot_age_secs > 600`.
- Add a client-side timeout on `FT.SEARCH` and graph queries — long-running queries are the
  primary source of stuck snapshots.
- Tune `--mvcc-old-snapshot-threshold-secs` down to match your P99 query latency SLO.
  A value 3× your expected max query duration is a reasonable starting point.
- Plan for Wave-2 `MA2` (old-snapshot detection daemon) which auto-kills zombied snapshots.

**If Moon was OOM-killed:** restart it. Moon recovers from WAL v3 on startup. After restart,
run `VACUUM MVCC` immediately to prune any MVCC state that accumulated pre-crash.

---

## Scenario 4: Compaction Not Keeping Up

**Violated SLOs:** [SLO 1 — Bloat](./reclamation-slo.md#slo-1--bloat-dead-fraction), [SLO 6 — Request Impact](./reclamation-slo.md#slo-6--request-impact-autovacuum-overhead)

**Symptoms:**

- `reclamation_dead_fraction_max` > 0.35 and rising
- `reclamation_compaction_pending_bytes` large but not triggering a stall
- Disk usage growing even though no new keys are being added (dead bytes accumulating)
- Optionally: `reclamation_autovacuum_throttled_due_to_load:1` continuously (v0.1.14+)

This is the slow-burn failure mode: compaction exists but runs too slowly for the ingest
rate. The system is not yet stalled, but it will be if nothing changes.

### Step 1: Quantify the backlog

```bash
redis-cli -p 6399 INFO reclamation | grep -E \
  'dead_fraction_max|compaction_pending_bytes|compaction_throughput_bps|immutable_segments|warm_segments|cold_segments|graph_segments|autovacuum'
```

**Decision tree:**

```
reclamation_dead_fraction_max > 0.35?
├── YES — bloat is above SLO threshold
│   ├── reclamation_compaction_pending_bytes > 0 → compaction backlog exists
│   │   ├── reclamation_write_stall_active:true → URGENT: this is Scenario 1, go there first
│   │   └── write_stall_active:false → compaction is behind but not stalled → goto Step 2
│   └── compaction_pending_bytes ≈ 0 → dead fraction is high but no pending work
│       → dead bytes are in immutable segments awaiting merge (Wave-2 P2)
│       → manual FT.COMPACT is the only recourse until Wave-2 ships → goto Step 3
└── NO (≤ 0.35) → bloat is within SLO; compaction may be slow for a different reason
    → check reclamation_autovacuum_throttled_due_to_load (v0.1.14+) → goto Step 4
```

### Step 2: Manually drive compaction

```bash
# Run a full VACUUM across all subsystems
redis-cli -p 6399 VACUUM
# Check returned bytes per category and total

# If vector indexes dominate the dead fraction:
redis-cli -p 6399 VACUUM VECTOR <index-name> VERBOSE

# If graph dead-edge fraction is high:
redis-cli -p 6399 VACUUM GRAPH VERBOSE

# Recheck dead fraction after each pass
redis-cli -p 6399 INFO reclamation | grep dead_fraction_max
```

Run `VACUUM` in a loop during off-peak hours until `dead_fraction_max` < 0.20. This is
manual work in v0.1.13 — Wave-2 `P4` ships the autovacuum daemon.

### Step 3: Compact immutable segments (dead fraction in immutable tier)

```bash
# Force compact each vector index
# Note: FT.COMPACT is a no-op if mutable_len < compact_threshold.
# To force: set COMPACT_THRESHOLD to 1 temporarily or use VACUUM VECTOR.
redis-cli -p 6399 VACUUM VECTOR <index-name> VERBOSE

# Check if immutable segment count drops
redis-cli -p 6399 INFO reclamation | grep immutable_segments
```

Dead bytes in immutable segments can only be reclaimed by the Wave-2 segment merge (`P2`).
If the dead fraction is dominated by immutable-segment tombstones, the mitigation is to
limit ingest rate until Wave-2 ships, or to accept elevated disk usage as a known gap.

### Step 4: Autovacuum throttling (v0.1.14+)

```bash
# Check if autovacuum is throttled due to high request load
redis-cli -p 6399 INFO reclamation | grep autovacuum_throttled_due_to_load
# 1 = throttled, 0 = running at full budget

# If throttled continuously: autovacuum cannot catch up under current load
# Options:
#   A. Schedule manual VACUUM during a low-traffic window
#   B. Increase autovacuum budget (v0.1.14 flag: --autovacuum-max-budget-ms)
#   C. Reduce ingest rate temporarily
```

### Step 5: Prevent the backlog from growing

- Alert on `reclamation_dead_fraction_max > 0.35`.
- If dead fraction is consistently above 0.20 at steady-state, the ingest rate exceeds
  the single-pass compaction budget. This is a capacity planning signal: either the dataset
  is growing beyond the compaction SLO boundary, or Wave-2 weighted compaction (MA4) is needed.
- Review vector index configuration: high `COMPACT_THRESHOLD` delays compaction, allowing
  the mutable segment (brute-force search) to grow and degrade query performance before
  compaction fires.
- Check the [OPERATOR-GUIDE.md](../OPERATOR-GUIDE.md) memory accounting section for guidance
  on RSS growth that may accompany a large compaction backlog.

---

## When to Escalate

Escalate to the on-call engineering lead when:

1. **Write-stall persists > 10 minutes** after running `VACUUM` and freeing disk space.
   Indicates a compaction bug or WAL checkpoint failure.

2. **MVCC committed count growing unbounded** (> 10M entries) after `VACUUM MVCC` and
   `KILL SNAPSHOT`. Indicates a snapshot handle leak in the MVCC manager.

3. **Moon OOM-killed more than once in 24 hours** on the same instance with stable keyspace
   size. Indicates a memory leak outside the MVCC path.

4. **`reclamation_dead_fraction_max` > 0.70** for > 30 minutes. Compaction is critically
   behind — risk of write-stall cascading into disk full.

5. **`FT.SEARCH` latency did not improve** after `FT.COMPACT` / `VACUUM VECTOR`.
   The read amplification cliff may have a different root cause (HNSW graph corruption,
   TQ4 quantization degradation on low-dimensional embeddings — see [CLAUDE.md](../../CLAUDE.md) gotchas).

6. **Manifest tombstones not dropping** after `VACUUM` with `--manifest-tombstone-retain-epochs 1`.
   Indicates a manifest GC bug.

---

## On-Call Links

| Resource | Location |
|---|---|
| Reclamation SLO contract | [docs/operations/reclamation-slo.md](./reclamation-slo.md) |
| WAL disk-full runbook | [docs/runbooks/disk-full-during-wal-rotation.md](../runbooks/disk-full-during-wal-rotation.md) |
| OOM / snapshot runbook | [docs/runbooks/oom-during-snapshot.md](../runbooks/oom-during-snapshot.md) |
| AOF corruption recovery | [docs/runbooks/corrupted-aof-recovery.md](../runbooks/corrupted-aof-recovery.md) |
| Memory accounting guide | [docs/OPERATOR-GUIDE.md](../OPERATOR-GUIDE.md) |
| Production contract | [docs/PRODUCTION-CONTRACT.md](../PRODUCTION-CONTRACT.md) |
| Wave 1/2 roadmap | [TODO.md](../../TODO.md) |
| Coding rules and gotchas | [CLAUDE.md](../../CLAUDE.md) |

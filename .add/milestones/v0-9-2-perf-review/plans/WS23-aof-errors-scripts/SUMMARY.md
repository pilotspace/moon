# WS23-aof-errors-scripts SUMMARY

Branch `perf/ws23`, base main `273e6bc`; integrated as `6590ad0` (moon#1272) and `3f08174` (moon#1276).
Personas: storage-durability-engineer (moon#1272), ci-test-integrity-engineer (moon#1276).
All results: **Linux container (4 vCPU x86_64), not merge bar.** redis-server / redis-cli 7.0.15, GNU coreutils `timeout` 9.4, bash 5.2.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1272 | FIXED | `8ada82d` (= `6590ad0`) | `tests/aof_backpressure_reply_1272.rs` (`--shards 1`, `MOON_TEST_AOF_FSYNC_STALL_MS=1500`, `--aof-fsync-timeout-ms 100`): red on 273e6bc on both runtimes (monoio 15, tokio 14 refusals, each `ERR AOF fsync failed; write not durable`); green on both (~2.6 s). Checks the exact `AOF_BACKLOG_ERR` text, INFO `aof_append_backpressure_refusals` == refusals seen, `/metrics` `moon_aof_append_backpressure_refusals_total` == INFO, `aof_fsync_failures:0`, a refused key reads back, ≤ 2 WARN lines. Unit: `persistence::aof::refusal::tests` (4), `server::conn::single_aof_log::tests` (2, tokio-only module). Regression suites green on both runtimes: 838 (3), 769 `--ignored` (3), 831 (3), `aof_append_status_heals_on_rewrite` (2), `aof_fsync_err_subscribe_ordering --ignored` (2). | `aof_delayed_fsync` not exposed (moon never writes past a slow fsync, so it would read 0). Pre-existing: a zero-length `always` barrier refused as `ChannelFull` calls `record_append_dropped` (false hole mark). The overflow-cap refusal has no dedicated test (needs a 256 MiB spill); it shares the mapping and its counter call. |
| moon#1276 | FIXED | `f169d9e` (= `3f08174`) | Red on 273e6bc (redis-cli 7.0.15): the moon#600 leg exited 1 with no output and leaked its server; `test-commands.sh --category eviction` stopped after `=== Eviction (volatile-ttl) ===` and leaked its server. Green: full `test-consistency.sh --shards 1` reached its summary (422 s, 1371 pass / 79 fail — every fail a documented 7.0-oracle diff); `test-commands.sh --category eviction` 4/4, `--category vector` 114/114. Induced failures (SIGTERM mid-leg; oracle SIGKILLed mid-leg) print the reason and leave nothing running. Probe paths: 7.0.15 → `timeout`; no `timeout` → loud WARNING, unbounded; `-t`-capable shim → native. | TEMPORAL / FT sections need python3's `redis` module (now a loud death, not silent). Not run under macOS bash 3.2 (idioms chosen for it). |

### moon#1272 design notes
- Reply: `-MOONERR AOF backpressure: write applied in memory but not queued for persistence; the AOF writer is backlogged` (`persistence::aof::AOF_BACKLOG_ERR`, module `persistence::aof::refusal`).
- MOONERR, not BUSY: MOONERR already prefixes moon's other writer-backlog replies (`AOF_APPEND_LOST_ERR`, moon#769); BUSY is redis's script-busy code (Jedis `JedisBusyException`, Lettuce `RedisBusyException`). redis-py maps only LOADING, so BUSY would not have misfired there. `ERR` is indistinguishable from any generic error.
- "applied in memory", not "not applied, retry": every producer mapped through `append_refusal_reply` applies the write before its record is refused; a retry hint would invite a double INCR.
- `fsync_barrier` (`always`) failures and routed-script barriers keep `AOF_FSYNC_ERR`. Cancel safety is unchanged: a refusal never acks a record that did not reach the writer.
- Docs: `docs/guides/persistence.md` (everysec policy), `docs/guides/monitoring.md` (metric).

## Measurements
No performance claim: only refusal paths changed (static bytes, one relaxed `fetch_add`, a rate-limited log). Manual run (`redis-benchmark -t set -n 400000 -P 64`, stall 1500 ms, timeout 100 ms): 700 refusals, `aof_append_backpressure_refusals:700` == `aof_backpressure_dropped:700`, `aof_fsync_failures:0`, one WARN line (the old code logged one per refusal).

## Cross-ownership edits
`src/admin/metrics_setup/recorders.rs` (one recorder), `src/command/connection.rs` (one INFO field), `docs/guides/{persistence,monitoring}.md`, `scripts/README.md`, test string matchers in `tests/common/slow_host.rs`, `tests/default_config_aof_backpressure_838.rs`, `tests/aof_everysec_backpressure_769.rs`.

## Risks / things to re-check at integration
1. `tests/common/slow_host.rs::AOF_REFUSAL` is now the prefix `MOONERR AOF backpressure`: a real fsync failure is no longer retried as backpressure.
2. `persist_txn_aof` → `Result<(), &'static [u8]>`, `persist_local_leg` → `Result<bool, AofAck>`: a new caller must map with `append_refusal_reply`.
3. Scripts source `scripts/lib/harness-guard.sh`; `set -E` + ERR trap; new auxiliary servers must use `aux_start`; `kill_port_servers` needs the exact argv[0].

## Review round 2b (report: `../WS26-review-round/REVIEW-round2b.md`)
- **MINOR-3 (moon#1272):** the `appendfsync always` fsync barrier still answered "fsync failed" on writer backlog (11 sites). `0ad8b17` + `4c8a16d`: a barrier refusal for backlog answers the new `AOF_BARRIER_BACKLOG_ERR` ("write applied in memory and queued, but not confirmed durable"; same `MOONERR AOF backpressure` prefix), counts in `aof_append_backpressure_refusals`, and is not counted as a dropped record. Unit test `pool_tests::fsync_barrier_always_on_full_channel_is_a_backpressure_refusal`.
- **MINOR-2 (moon#1276):** `redis-cli -t` is a connect timeout only and arrived in 7.4, not 7.2. `387402b`: the guard prefers `timeout`/`gtimeout` (whole-command bound), falls back to `-t` with a NOTE. Verified: a listener that accepts and never answers is cut at 2 s (rc 124).

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.92 · Practicality 0.93 · Optimization 0.92 · Edge cases 0.90 · Self-evaluation 0.91

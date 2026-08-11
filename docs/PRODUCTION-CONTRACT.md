---
title: "Production Contract"
description: "Moon's v1.0 promises: SLOs, durability modes, supported platforms, and GA exit criteria — a living, evidence-linked ledger"
---

# Moon Production Contract

**Status:** living document — refreshed to post-replication-GA main (v0.7.0 release prep); the ledger
below is what the `v1.0` release gate checks.
**Last updated:** 2026-07-14
**Milestone:** v0.7.0 Replication GA for multi-shard masters (tag gated on the 24h replication soak)
**Current release:** v0.6.0 (2026-07-08, PR #249) — see [`RELEASES.md`](https://github.com/pilotspace/moon/blob/main/RELEASES.md).
Main additionally carries the full v0.6.1 hygiene scope (H-1…H-7) and the v0.7 replication
workstreams (R0/R0.5/R1/R2 + plane replication Waves A/B), untagged pending the soak gate.
**Roadmap:** [`docs/roadmap/ROADMAP.md`](roadmap/ROADMAP.md) §1 (proven strengths / reality check) is the
authoritative source this ledger was verified against.

## What this document is

This is the contract every Moon `v1.0` user is entitled to. It is a **checked ledger**: every row is
either ✅ (shipped, with a verifiable evidence link — a file, a CI job, a script) or ⬜ (not yet
shipped). **No aspirational ticks** — a row is only ✅ if the linked evidence was read and confirmed to
exist and do what the row claims, as of this refresh.

`scripts/check-production-contract.sh` greps this file and is wired into
[`.github/workflows/release.yml`](https://github.com/pilotspace/moon/blob/main/.github/workflows/release.yml):

- On any `v0.x` tag it **reports** unticked GA-blocking rows but does not fail the build — Moon is
  pre-GA and the whole point of this ledger is to track the gap honestly.
- On a `v1.0*` tag it **hard-fails** the release if any row marked `GA` in the Blocking column is
  still ⬜. That is the promotion gate: v1.0 cannot ship with a known, unticked promise in this file.

Rows marked `—` in the Blocking column are explicitly out of v1.0 scope (see [§ Out of
Scope](#out-of-scope)) and never block any tag.

## Supported Platforms

| Tier | Platform | Runtime | Guarantees |
|---|---|---|---|
| **1 — Primary** | Linux aarch64 + x86_64 | monoio + io_uring (kqueue/epoll via `MOON_NO_URING`) | Full feature set: io_uring, `SO_REUSEPORT` per-shard, O_DIRECT, connection migration |
| **1 — Primary** | macOS aarch64 (Apple Silicon) + x86_64 | monoio + kqueue, or tokio | Full feature set minus io_uring/O_DIRECT (pread fallback), no connection migration |
| **2 — CI / dev / portability** | Any of the above, or Windows x86_64 | tokio (`MOON_NO_URING=1` on Linux/macOS; tokio is Windows' default runtime) | Functional correctness only — production benchmarks are not taken on this tier |

**Rationale:** this supersedes the v0.1.3-era table, which treated macOS as OrbStack-only and
excluded Windows entirely. Both are now built and tested in CI (`check-macos`, `check-windows` jobs
in `.github/workflows/ci.yml`); production **benchmark numbers** still MUST come from Linux
(OrbStack or GCE) per `CLAUDE.md` — macOS/Windows numbers are dev-only.

## Performance

Numbers are tracked in [`BENCHMARK.md`](https://github.com/pilotspace/moon/blob/main/BENCHMARK.md) (KV pipelined/p=1, vector iso-recall vs
Qdrant/RediSearch, graph vs FalkorDB), refreshed per release rather than pinned as static SLOs in
this document — no automated 24h HDR rig or Criterion CI regression gate exists yet (`PERF-01`/`PERF-02`
below are unticked). Headline, evidence-cited numbers as of v0.6.0:

| Area | Standing | Source |
|---|---|---|
| Pipelined KV throughput | 1.7–2.6× Redis at P=64 across x86/ARM/macOS | `BENCHMARK.md` §1 |
| p=1 single-op | Wins x86 +4.7% (n=3); ARM needs `--io-busy-poll-us` | `tmp/KV-FULLPROOF.md` |
| Vector vs Qdrant | 8.9–10.9× ingest, 2.5–3.4× search QPS at iso-recall ≥0.999 | `BENCHMARK.md` §10.7–10.9 |
| Graph vs FalkorDB | 21–26× build; wins/ties point queries | `BENCHMARK.md` §11 |
| Vector 384d recall/QPS | Trails RediSearch ~16× QPS (quantization codebook mis-fit at low d) | `BENCHMARK.md`; diagnosed, fix candidate is TQ⁺ calibration |

## Durability Modes

Moon ships three `appendfsync` modes and a disk-offload cold tier.

| `appendfsync` | Process crash (SIGKILL) | OS crash / power loss | Disk full |
|---|---|---|---|
| `always` | RPO = 0 | RPO = 0 | Graceful `MOONERR diskfull`; no silent loss |
| `everysec` (default) | RPO ≤ last buffered batch | RPO ≤ 1 s | Graceful `MOONERR diskfull` |
| `no` | RPO = OS flush window | RPO = OS flush window (minutes) | Graceful `MOONERR diskfull` |

`appendfsync=no` is cache-mode only — do not use for primary storage. Recovery order: RDB snapshot →
WAL v3 segments → AOF tail (`src/persistence/`, checksum-guarded record/page formats:
`src/persistence/page.rs`, `kv_page.rs`, `clog.rs`, `manifest.rs`).

## GA Exit Ledger

Legend: ✅ shipped (evidence verified) · ⬜ not shipped · Blocking `GA` = must tick before `v1.0` tag
per the CI gate · Blocking `—` = tracked but never blocks a tag (see Out of Scope).

### A. Toolchain & Release Hygiene

| Status | ID | Item | Evidence | Blocking |
|---|---|---|---|---|
| ✅ | RUST-01 | MSRV 1.94 (edition 2024), clippy clean on default + tokio feature sets | `.github/workflows/ci.yml` jobs `Clippy (default)`, `Clippy (tokio)`, `MSRV (1.94)` | GA |
| ✅ | FMT-01 | `cargo fmt --check` enforced | `ci.yml` job `Lint` | GA |
| ✅ | CHANGELOG-01 | CHANGELOG.md CI gate on every PR (skip-changelog label escape hatch) | `ci.yml` step `CHANGELOG check` | GA |
| ✅ | REL-LEDGER-01 | Release tag requires a matching `RELEASES.md` entry | `release.yml` step `Require RELEASES.md entry for this tag` | GA |
| ✅ | CONTRACT-01 | This document is a checked ledger with a CI-wired gate | `scripts/check-production-contract.sh`, `release.yml` | GA |
| ✅ | SUPPLY-01 | `cargo audit` + `cargo deny` CI-blocking | Shipped (task #63, PR #326): `.github/workflows/supply-chain.yml` runs `cargo audit` + `cargo deny` on push/PR with `deny.toml` policy. Folded into v0.7.0. | GA |

### B. Correctness Hardening

| Status | ID | Item | Evidence | Blocking |
|---|---|---|---|---|
| ✅ | FUZZ-01 | cargo-fuzz targets, 15 min/target on PR (`ci-fuzz` label) + nightly, 24h cumulative clean | `fuzz/fuzz_targets/` has all **12** declared targets wired in `.github/workflows/fuzz.yml` — the missing 12th target found by the 2026-07-10 audit (`graph_props_record.rs`) was restored (task #10) and the nightly matrix shard is green again. | GA |
| ✅ | LOOM-01 | Loom model tests for lock-free/atomic state machines | `tests/loom_response_slot.rs`, `tests/loom_wal_sync_agent.rs` | GA |
| ✅ | SEC-08 | ACL glob-pattern fuzzing | `fuzz/fuzz_targets/acl_rule.rs`, wired in `fuzz.yml` | GA |
| ✅ | UNSAFE-01 | 100% `// SAFETY:` comment coverage, CI-enforced | `scripts/audit-unsafe.sh`, called from `ci.yml` `Lint` job | GA |
| ✅ | UNWRAP-01 | `unwrap`/`expect` ratchet (no new unannotated unwraps on hot paths) | `scripts/audit-unwrap.sh`, called from `ci.yml` `Lint` job | GA |
| ✅ | SEC-05 | `docs/security/unsafe-audit.md` published | `docs/security/unsafe-audit.md` | GA |
| ✅ | ACL-REG-01 | `TXN.*`/`WS.*`/`MQ.*`/`TEMPORAL.*`/`CDC.*` command families registered in the phf ACL/metadata table (early-intercept bypass) | Fixed by ROADMAP `H-3` (PR #258): families registered in `src/command/metadata.rs` with ACL categories, monoio early-intercept reordered behind the ACL check (the original bypass let `CDC.READ` skip ACL entirely on the production runtime), pubsub check added; denied-user tests included. | GA |

### C. Durability Proof

| Status | ID | Item | Evidence | Blocking |
|---|---|---|---|---|
| ✅ | WAL-01 | WAL v3 unified log, per-shard group commit, checksum-guarded records | `src/persistence/page.rs`, `kv_page.rs`, `clog.rs`, `manifest.rs`; `fuzz/fuzz_targets/wal_v3_record.rs` | GA |
| ✅ | CRASH-01 | Scripted crash-injection matrix (per-shard AOF, cold-tier DEL resurrection, disk-offload, graph, vacuum, vector durability) | `tests/crash_matrix_per_shard_aof.rs`, `crash_matrix_per_shard_bgrewriteaof.rs`, `crash_recovery_cold_del_resurrection.rs`, `crash_recovery_disk_offload_no_aof.rs`, `crash_recovery_graph_durability.rs`, `crash_recovery_vacuum.rs`, `crash_recovery_vector_durability.rs`; run by `.github/workflows/integration-tests.yml` jobs `Durability Tests` + `Crash Matrix (per-shard AOF)`. **2026-07-14 audit caveats:** the graph-durability suite's g1–g3 never actually ran until PRs #322/#324 (harness polled the deleted WAL v2 flat file AND the legacy-mode replay it guards was a silent no-op — both fixed, g1–g5 green 3× macOS / 5× Linux); `crash_recovery_disk_offload_no_aof` was harness rot, not a data-loss regression — its 65%-eviction-throughput floor predated PR #273's intentional plain-drop write path and was structurally unreachable; fixed (task #44) by asserting against ground truth read directly from the manifest instead, green 10×+ macOS (monoio + tokio). | GA |
| ✅ | CRASH-02 | Cross-plane kill-9 crash matrix — every plane pair (KV/vector/graph/FTS/WS/MQ/temporal/txn) survives kill -9 with zero synced-write loss, both legacy and disk-offload modes | `tests/crash_matrix_cross_plane/` (grown to **46 cells**; PR #298, v0.8 storage-kernel G1 gate). The matrix's 4 RED root-cause groups were each fixed with the cell as regression proof: cross-store MULTI/EXEC graph leg (task #52), checkpoint-Finalize graph total-loss window (task #53), eviction plain-drop under write pressure (task #57), legacy graph WAL replay no-op (task #60, PR #322). **2026-07-16:** all 46 cells green with zero `red_guard` sites (2/2 consecutive full runs + 5-iteration probabilistic-cell soak, fresh ELF @ `ec084556`); wired into scheduled CI — nightly full matrix + Saturday ITERS=20 soak (`.github/workflows/crash-matrix.yml`, PR #352). | GA |
| ✅ | MEM-10X-01 | 10× RAM datasets under disk-offload: bounded resident set, startup readiness not O(spilled-keys) | v0.8 storage-kernel G2 acceptance run (2026-07-13): all 3 criteria PASS — restart readiness 157s → 3.7s at 236K spilled files (PR #319 crash-orphan sweep off the critical path), unified resident-bytes accounting + elastic budget (PR #297), allocator-overhead + PageCache observability (PR #320). **2026-07-16 re-run @ `4dcfd533`** (`docs/perf/2026-07-16-g2-10x-ram-rerun.md`): spill batching lands — **840 files vs ~236K** for the same 2.6 GB spill (PR #350); `used_memory` truthful — 1.00× cap at steady state, ≤5 s post-restart drain to under-cap (task #56, PR #349); cold-GET-during-spill worst tail 1,910 ms → **205 ms** (task #59 fairness work still open for sub-10 ms); restart now AOF-replay-bound (16.9 s at 3.3 GB unrewritten incr AOF — file count out of the boot path), 500/500 integrity. Known follow-up: `DBSIZE` counts only resident keys under offload (~24K reported vs ~164K logical). | GA |
| ✅ | JEPSEN-01 | Jepsen-lite linearizability suite | `tests/jepsen_lite.rs`, run by `integration-tests.yml` job `Durability Tests` | GA |
| ✅ | BACKUP-01 | BGSAVE → restore correctness tests | `tests/durability/backup_restore.rs` | GA |
| ✅ | COLD-TTL-01 | Cold-tier TTL reclaim (expired ColdIndex entries never swept — unbounded RAM+disk growth) | Fixed by ROADMAP `H-2`: `ColdIndex::sweep_expired` (bounded per-tick reclaim, `src/storage/tiered/cold_index.rs`) + expired-on-read reclaim, observable via `src/command/info_reclamation.rs` counters (`cold_expired_reclaimed`) and `src/admin/reclamation_schedule.rs`. | GA |
| ✅ | RSS-01 | Memory steady-state regression gate (per-kind RSS vs baseline, ±5%, self-test injects +6% to prove the gate catches it) | `ci.yml` job `Memory steady-state gate`, `scripts/bench-memory-steady-state.sh` | GA |
| ⬜ | PERF-01 | Criterion regression gate blocking PRs on >5% hot-path regression | not found — no Criterion CI job exists; `benches/*.rs` are run manually only | GA |
| ⬜ | PERF-02 | 24h HDR histogram rig on reference hardware, numbers promoted from provisional | not found | GA |

### D. Replication & HA

| Status | ID | Item | Evidence | Blocking |
|---|---|---|---|---|
| ✅ | REPL-01 | Single-shard-master PSYNC2 (full + partial resync), replica promotion | `src/replication/`, `tests/replication_hardening.rs`, `tests/replication_test.rs`, run by `integration-tests.yml` job `Replication Tests` | GA |
| ✅ | REPL-MULTISHARD-01 | Multi-shard master replication (a `--shards N>1` master can be replicated at all) | R2 (task #20): `ShardMessage::PrepareReplicaSync` per-shard atomic snapshot legs + merged Redis-format RDB + per-record SELECT framing on the merged wire. monoio only; replicas run `--shards 1`; partial resync degrades to full at N>1. `tests/replication_multishard.rs` (2/4/8-shard resync, interleaved multi-db parity, graph, partial→full). | GA |
| ✅ | WAIT-01 | `WAIT` reflects real replica ACK state | R1 (task #19, PR #282): replica 1s `REPLCONF ACK` ticker on the split PSYNC socket; master `ack_read_loop` + `drain_ack_offsets` record into `ReplicaInfo.ack_offsets`; connection-layer `try_handle_wait` blocks until ACK ≥ target or timeout. `wait_returns_acked_replica_count` e2e; exact on multi-shard masters too (summed snapshot offset). | GA |
| ✅ | REPL-PLANES-01 | Every write plane replicates, not just KV: eviction/expiry DELs, Lua effects, graph, vector/text index defs+contents, WS.*, MQ.*, TEMPORAL.* | Wave A (PR #285): eviction/expiry DELs + Lua effects to both planes (EVAL was previously durable in neither). Wave B (PR #294 + task #34): WS/MQ deterministic records + replica apply + PSYNC registry blob. Graph plane (task #25): live GRAPH.* streaming + snapshot backfill. Suites: `tests/replication_planes.rs`, `replication_graph.rs`, `replication_mq.rs`, `replication_readonly_ws_mq.rs`. Unified poison-record policy for replica apply (task #48). | GA |
| ✅ | REPL-SOAK-01 | 24h replication soak: kill -9 either side under WAIT-confirmed load, zero acked-write loss | **PASSED 2026-07-15** — `SOAK-PASS duration=86400s cycles=114 acked=82044 inflight=7 master_kills=57 replica_kills=57`; 82,044 WAIT-acked writes preserved across 114 alternating kill-9 cycles, zero acked-loss. Run dir `moon-soak/runs/20260714-141946`, RC `e2d87893`. Gates the v0.7.0 tag (task #65). | GA |
| ⬜ | KEYSPACE-NOTIF-01 | `notify-keyspace-events` keyspace notifications | No implementation found in `src/`. ROADMAP v0.7.0 workstream R5 — deferred to a later release (v0.7.1 was a patch: SQ8 CPU-storm + replica TTL determinism). | GA |
| ⬜ | MONITOR-01 | `MONITOR` command | No implementation found in `src/command/`. ROADMAP v0.7.0 workstream R5. | GA |
| ⬜ | XSHARD-READ-01 | Lock-free cross-shard read path (retire the shardslice waiver) | Waiver **expires 2026-08-01** per `RELEASES.md` v0.6.0 entry and ROADMAP §5; L4 redesign (`tmp/MULTISHARD-REDESIGN.md`) unstarted. ROADMAP v0.7.0 workstream R4. | GA |

### E. Cluster Mode

| Status | ID | Item | Evidence | Blocking |
|---|---|---|---|---|
| ⬜ | CLUSTER-01 | Cluster bus + gossip run on the production (monoio) runtime | `src/main.rs`: gossip/bus spawn only inside the tokio startup block; the monoio branch's own comment reads *"Monoio listener: simplified startup. Cluster bus and gossip not yet supported under monoio."* Cluster mode today only runs under `runtime-tokio`, i.e. **not** the production default. ROADMAP v0.8.0 task `C-1`. | GA |
| ⬜ | CLUSTER-02 | Slot migration atomicity soak (MIGRATING/IMPORTING under load, crash mid-migration) | Not found; ROADMAP v0.8.0 task `C-2` | GA |
| ⬜ | CLUSTER-03 | `CLUSTER SHARDS`, `cluster-announce-ip/port`, `cluster-config-file` parity | Not verified present; ROADMAP v0.8.0 task `C-4` | GA |

### F. Security

| Status | ID | Item | Evidence | Blocking |
|---|---|---|---|---|
| ✅ | SEC-06a | TLS 1.3 mandatory (1.2 opt-in), mTLS via `--tls-ca-cert-file` | `src/tls.rs`, `src/config.rs` (`tls_ca_cert_file` doc: *"Path to CA certificate for client authentication (mTLS)"*) | GA |
| ✅ | SEC-06b | TLS hot-reload on `SIGHUP` | `src/tls.rs`, `src/server/embedded.rs` | GA |
| ✅ | SEC-08b | ACL enforcement on every command dispatch (category + key-pattern) | `src/acl/` (`mod.rs`, `rules.rs`, `table.rs`, `io.rs`, `log.rs`) — see the caveat at `ACL-REG-01` above for the early-intercept command families still bypassing this table | GA |
| ✅ | SEC-04 | `docs/security/lua-sandbox.md` published | `docs/security/lua-sandbox.md` | GA |
| ✅ | SEC-02 | SBOM (CycloneDX) generated per release; artifacts signed via cosign (keyless, Fulcio cert) | `release.yml` jobs `sign`: `cargo cyclonedx` (3 variant SBOMs), `cosign sign-blob --output-certificate` over every artifact + `SHA256SUMS.txt` | GA |
| ✅ | THREAT-01 | `docs/THREAT-MODEL.md` published | `docs/THREAT-MODEL.md` | GA |
| ✅ | SEC-07 | `SECURITY.md` disclosure policy is accurate | Fixed by ROADMAP `H-4`: `SECURITY.md` now states the release-agnostic policy — only the latest released minor line gets security fixes pre-1.0; 18-month LTS begins at v1.0.0. No per-release staleness possible anymore. | GA |
| ⬜ | ENCRYPT-01 | Encryption at rest (WAL/AOF/RDB) | No `encryption`/`AES` feature or config surface found. ROADMAP v0.9.0 task `E-1`. | GA |

`SUPPLY-01` (`cargo audit`/`cargo deny` not CI-wired) also belongs here conceptually — tracked once,
in the Toolchain & Release Hygiene table above, to avoid double-counting the same gap.

### G. Observability

| Status | ID | Item | Evidence | Blocking |
|---|---|---|---|---|
| ✅ | METRICS-01 | Prometheus `/metrics` on the admin port | `src/admin/metrics_setup.rs`, `src/admin/http_server.rs` | GA |
| ✅ | SLOWLOG-01 | Redis-compatible `SLOWLOG` | `src/admin/slowlog.rs`, dispatch entries in `src/command/mod.rs` / `metadata.rs` | GA |
| ✅ | HEALTH-01 | `/healthz` + `/readyz` endpoints | `src/admin/http_server.rs` | GA |
| ✅ | CONFIG-01 | `moon --check-config` validator | `src/config.rs` (`check_config` flag), `src/main.rs`, `src/config/conf_file.rs` | GA |
| ⬜ | OTEL-01 | OTLP trace export | Feature namespace `otel` reserved in `Cargo.toml` but unwired (no `tracing-opentelemetry`/`opentelemetry-otlp` dependency yet). ROADMAP v0.9.0 task `E-5`. | GA |
| ⬜ | LOGFMT-01 | `--log-format json` structured logs | Not found. ROADMAP v0.9.0 task `E-5`. | GA |

### H. Compatibility & Ecosystem

| Status | ID | Item | Evidence | Blocking |
|---|---|---|---|---|
| ✅ | COMPAT-03 | `docs/redis-compat.md` published | `docs/redis-compat.md` | GA |
| ⬜ | COMPAT-01 | Broad client-library CI matrix | Only redis-py (`.github/workflows/console-integration.yml`) and the Rust-native `redis`/internal test clients are CI-exercised today; Java (jedis/lettuce), Node (ioredis), .NET (StackExchange.Redis) are not. ROADMAP v0.9.0 task `E-4`. | GA |
| ⬜ | FT-PARITY-01 | `FT.ALTER` + `FT.AGGREGATE` parity closure | Half closed: `FT.AGGREGATE` is implemented (`src/command/vector_search/ft_aggregate.rs`). `FT.ALTER` still has no implementation in `src/command/`. Remainder is ROADMAP §8.4 task `E-6`. | GA |
| ⬜ | VEC-384-01 | 384d recall/QPS parity with RediSearch (TQ⁺ low-d calibration) | Diagnosed gap, ~16× QPS trail at 384d; fix candidate not yet implemented. ROADMAP v0.9.0 task `E-6`. | GA |

### I. Release Engineering

| Status | ID | Item | Evidence | Blocking |
|---|---|---|---|---|
| ✅ | REL-01 | `docs/versioning.md` + on-disk format version fields (RDB/WAL v3/AOF manifest) | `docs/versioning.md` | GA |
| ✅ | REL-02 | Upgrade path test | `tests/upgrade_test.rs`, `docs/runbooks/upgrade-to-v0.6.0.md` | GA |
| ✅ | REL-03 | Release artifacts: Linux musl aarch64+x86_64, deb, rpm, Docker, systemd unit, Windows zip | `release.yml` build matrix (7 Linux/macOS targets + Windows), `package` job (nfpm deb/rpm), `docker` job, `packaging/moon.service` | GA |
| ⬜ | REL-03b | Homebrew tap | `release.yml` job `homebrew-tap` exists but is gated behind `vars.HOMEBREW_TAP_ENABLED` (default off — no tap repo/token provisioned yet) | — |
| ✅ | REL-05 | Operator runbooks | `docs/runbooks/` (corrupted-aof-recovery, disk-full-during-wal-rotation, multi-shard-aof-rewrite, oom-during-snapshot, replica-fell-behind, rolling-restart, shard-count-change, tls-cert-rotation, upgrade-to-v0.6.0) | GA |
| ✅ | REL-06 | User docs (getting-started, config, commands, tuning, migration) | `docs/quickstart.md`, `docs/configuration.md`, `docs/commands.md`, `docs/production-guide.md`, `docs/OPERATOR-GUIDE.md` | GA |
| ✅ | REL-07 | Tag-triggered release pipeline | `.github/workflows/release.yml` (`on: push: tags: 'v[0-9]*'`) | GA |
| ⬜ | REL-08 | Performance regression suite pinned in CI (bench-compare vs recorded baselines) | Not found — see `PERF-01` above | GA |

## Out of Scope

Never blocks any tag (deliberate, standing non-goals — see ROADMAP §7):

| Excluded capability | Reason |
|---|---|
| C module ABI (`MODULE *`) | Moon builds features natively — modules conflict with the thread-per-core ownership model |
| Sentinel protocol | HA is cluster mode + orchestrator (k8s/systemd) |
| Multi-master / active-active, cross-region replication | Not in Moon's architectural scope; single-datacenter async replication only |
| Redis dense/sparse HLL internals (`PFDEBUG`/`PFSELFTEST`) | Not reimplemented |
| Decode+re-encode vector segment merging | Recall collapse (0.73→0.0005, measured); GraphUnion merge only |
| GPU vector acceleration (`gpu-cuda`) | Feature-gated, not on the default path |
| DiskANN GA, HexaHNSW GA | Experimental; recall gains not validated on real datasets at production scale |
| Redis Functions (scripting v2) | Deferred; EVAL/EVALSHA covers scripting needs |
| Kubernetes operator GA, Helm chart GA | Alpha targeted for v0.8/v0.9 per ROADMAP; GA-hardening is post-v1.0 |
| Moon Enterprise Edition (SSO/OIDC/LDAP, multi-DC DR, compliance/SOC2, support tiers) | Commercial layer maintained privately (open-core: CE never loses features) |

## Revision history

| Date | Change |
|---|---|
| 2026-04-08 | Initial publication — provisional SLO numbers from v0.1.2 benchmark memory; full checklist structure locked (Phase 87) |
| 2026-07-10 | Full refresh to v0.6.0 reality (ROADMAP `H-5`): converted to an evidence-linked checked ledger, dropped the defunct Phase-87..100 numbering in favor of the v0.6.1→v1.0 release train, added `scripts/check-production-contract.sh` as a CI gate (soft-report pre-v1.0, hard-fail at `v1.0*` tags). Verification pass found two undocumented defects: `SUPPLY-01` (`deny.toml` claims a CI job that does not exist) and a broken 12th fuzz target (`graph_props_record`, `FUZZ-01`) — both left unticked rather than assumed. |
| 2026-07-14 | v0.7.0-prep reconciliation: re-ticked `FUZZ-01` (12th target restored), `ACL-REG-01` (PR #258), `COLD-TTL-01` (`ColdIndex::sweep_expired`), `SEC-07` (release-agnostic policy); updated `FT-PARITY-01` (AGGREGATE shipped, ALTER open). Added honesty caveats to `CRASH-01` (g1–g3 harness never ran pre-#322/#324; task #44 residual red). New rows for shipped-but-previously-untracked guarantees: `CRASH-02` (37-cell cross-plane kill-9 matrix, v0.8 G1), `MEM-10X-01` (10× RAM acceptance, v0.8 G2), `REPL-PLANES-01` (all-plane replication, Waves A/B), and the pending `REPL-SOAK-01` gate for the v0.7.0 tag. |
| 2026-07-14 | task #44 closed: `crash_recovery_disk_offload_no_aof` was harness rot — its fixed 65%-of-`PROBE_COUNT` eviction-throughput floor predated PR #273's policy-aware plain-drop write path and was structurally unreachable (ground-truth-verified 0-1/200 durably spilled under the old pipelined-`SET` burst), not a data-loss regression (the boot-time `disk_offload_spill_inert` WARN already documents this exact config as non-durable on the write path). Fixed test-only: `write_filler` now drives the periodic manifest-backed memory-pressure tick via a single `MSET` instead of racing it with per-write plain-drop, and the assertion reads ground truth directly from each shard's manifest + `heap-*.mpf` files instead of a fixed floor. Verified the rewrite still catches a manually-reintroduced #22 regression; `CRASH-01` caveat updated. |
| 2026-07-16 | v0.8 storage-kernel close-out evidence refresh: `CRASH-02` — matrix grown 37→46 cells, all green ungated (zero `red_guard` sites), wired into scheduled CI (nightly full + Saturday ITERS=20 soak, PR #352). `MEM-10X-01` — G2 re-run @ `4dcfd533` (`docs/perf/2026-07-16-g2-10x-ram-rerun.md`): spill batching 840 files vs ~236K (PR #350), `used_memory` truthful at 1.00× cap steady-state with ≤5 s post-restart drain (task #56, PR #349), cold-GET-during-spill tail 1,910→205 ms, 500/500 kill-9 integrity. New follow-up filed: `DBSIZE` counts only resident keys under offload. |

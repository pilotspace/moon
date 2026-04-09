---
title: "Production Contract"
description: "Moon's v1.0 promises: SLOs, durability modes, supported platforms, and GA exit criteria"
---

# Moon Production Contract

**Status:** provisional — SLO numbers lock in at Phase 97 (Performance SLO Lock-In) and the full checklist locks at Phase 100 (GA Gate).
**Last updated:** 2026-04-08 (Phase 87 initial publication)
**Milestone:** v0.1.3 Production Readiness
**Architectural baseline:** [`.planning/MOON-DATAFLOW-WALKTHROUGH.md`](../.planning/MOON-DATAFLOW-WALKTHROUGH.md)

## What this document is

This is the contract every Moon v1.0 user is entitled to and every v0.1.3 phase tests against. If a number appears here without an automated test or benchmark behind it, the number is wrong. Phases 88-100 of the v0.1.3 milestone each tick items off the GA Exit Criteria checklist at the bottom of this document. The checklist is the gate — nothing promotes to `v1.0-rc1` until every box is ticked green.

Aspirational numbers do not belong in this document. Provisional numbers do, and they are marked `[provisional — Phase N]` until the phase that verifies them lands.

## Supported Platforms

| Tier | Platform | Runtime | Guarantees |
|---|---|---|---|
| **1 — Primary** | Linux aarch64, kernel ≥ 6.1 | monoio + io_uring | Full SLOs in this document |
| **2 — Secondary** | Linux x86_64, kernel ≥ 6.1 | monoio + io_uring | Full SLOs contingent on `PERF-04` fix (x86_64 monoio accept-loop regression closed in Phase 97) |
| **3 — CI / dev** | Linux any arch, any kernel | tokio (`MOON_NO_URING=1`) | Functional correctness only — no SLO commitment |
| **Unsupported** | macOS native, Windows, WSL1, kernel < 6.1, x86_64 without AVX2, aarch64 without NEON | — | Dev-only via OrbStack for macOS; others not built |

**Rationale for exclusions:**
- **macOS native** — kqueue integration with the monoio cold tier is not validated; OrbStack VM gives Linux parity on macOS hosts without forking the I/O layer.
- **Windows / WSL1** — io_uring unavailable; tokio fallback would create a two-tier support story we cannot staff.
- **Kernel < 6.1** — io_uring feature floor; `MOON_NO_URING=1` works but cannot hit the SLOs below.

## Performance SLOs

All numbers are per single node, single process. Pipelining and multi-key commands use separate tables. Reference hardware: **aarch64 Apple M-series via OrbStack for development, Google Cloud `c3-standard-8` for CI/reference benches**.

### Single-key, non-pipelined (Tier 1/2)

| Command | Load | p50 | p99 | p99.9 | Verified by |
|---|---|---|---|---|---|
| `GET` | 1 M QPS | ≤ 50 µs | ≤ 500 µs | ≤ 2 ms | `PERF-01`, `PERF-02` [provisional — Phase 97] |
| `SET` `appendfsync=everysec` | 300 K QPS | ≤ 80 µs | ≤ 800 µs | ≤ 3 ms | `PERF-01`, `PERF-02` [provisional — Phase 97] |
| `SET` `appendfsync=always` | 50 K QPS | ≤ 500 µs | ≤ 5 ms | ≤ 20 ms | `PERF-01`, `PERF-02` [provisional — Phase 97] |
| `HSET` | 300 K QPS | ≤ 100 µs | ≤ 1 ms | ≤ 4 ms | `PERF-01` [provisional — Phase 97] |
| `ZADD` | 200 K QPS | ≤ 150 µs | ≤ 1.5 ms | ≤ 6 ms | `PERF-01` [provisional — Phase 97] |
| `LPUSH` | 300 K QPS | ≤ 100 µs | ≤ 1 ms | ≤ 4 ms | `PERF-01` [provisional — Phase 97] |

### Pipelined

| Command | Pipeline depth | Throughput (absolute) | Ratio vs Redis 8.x | Verified by |
|---|---|---|---|---|
| `GET` | p=16 | ≥ 4 M QPS | ≥ 1.7× | `PERF-01` [provisional] |
| `GET` | p=128 | ≥ 5.5 M QPS | ≥ 2.3× | `PERF-01` [provisional — x86_64 only until `PERF-04`] |
| `SET` `everysec` | p=16 | ≥ 1.9 M QPS | ≥ 1.5× | `PERF-01` [provisional] |
| `SET` `everysec` | p=64 | ≥ 2.2 M QPS | ≥ 1.9× | `PERF-01` [provisional] |

### Vector search (HNSW + TurboQuant)

| Operation | Dataset | k | Metric | Target | Verified by |
|---|---|---|---|---|---|
| `FT.SEARCH` | 1 M × 768-d | 10 | p99 latency @ 100 QPS | ≤ 5 ms | `PERF-01` [provisional — Phase 97] |
| `FT.SEARCH` | 1 M × 768-d | 10 | recall@10 | ≥ 0.92 | Phase 96 benchmark reruns |
| `HSET` (indexed) | 768-d vector | — | Throughput | ≥ 30 K inserts/s | Phase 96 benchmark reruns |

### What "meets SLO" means operationally

1. The number is measured over a **24 h HDR histogram** run on reference hardware (`PERF-02`).
2. The number is the **steady-state** observation — first 5 minutes of warmup excluded.
3. CI Criterion gate (`PERF-01`) blocks any PR that regresses a listed target by > 5 %.
4. Failure to hit an SLO after measurement does **not** cause Moon to abort — it causes the release to be blocked, the SLO to be relaxed, or the regression to be fixed. Users are not surprised at runtime; operators are warned at CI time.

## Durability Modes

Moon ships three fsync modes and a disk-offload cold tier. Each has a documented recovery bound.

| `appendfsync` | Process crash (SIGKILL) | OS crash / power loss | Disk full | RTO for 10 GB |
|---|---|---|---|---|
| `always` | RPO = 0 (all committed writes survive) | RPO = 0 | Graceful OOM error; no silent loss | ≤ 10 s |
| `everysec` (default) | RPO ≤ last buffered batch (~ 1 ms) | RPO ≤ 1 s | Graceful OOM error | ≤ 10 s |
| `no` | RPO = OS flush window | RPO = OS flush window (minutes) | Graceful OOM error | ≤ 10 s |

**Enforcement:**
- Per-crash-class behavior is proven by the `CRASH-01` scripted crash-injection matrix in Phase 94. Every `{fsync-mode × failure-class × write-phase}` cell must pass in CI.
- Torn writes (partial sector) are detected by CRC32 on WAL v3 records and truncated at the last durable offset — `TORN-01` in Phase 94.
- Disk-offload cold tier uses a 6-phase recovery protocol (`OFFLOAD-01` in Phase 94) with v2 fallback on corruption.
- Recovery order: RDB snapshot → WAL v3 segments → AOF tail. Proven by integration tests.
- `appendfsync=no` is documented as **cache-mode only — do not use for primary storage.**

## Availability & Replication Guarantees

- **Single-node availability:** graceful shutdown on SIGTERM, drains in-flight requests before close. Recovery < RTO above.
- **Async replication (REPLICAOF):** eventually consistent. Replication lag is exposed via `/metrics` (Phase 92) and bounded by backlog size and replica ACK cadence.
- **PSYNC2 partial resync:** on replica reconnect inside backlog window — no full retransfer. Proven by `REPL-01` (Phase 95).
- **Full resync:** on reconnect outside backlog window — replica rebuilds from master snapshot. Proven by `REPL-02`.
- **Network partition:** master continues accepting writes (availability over consistency — Redis-semantics). Replica diverges and is reconciled on heal. Proven by `REPL-03`.
- **Replica promotion:** `REPLICAOF NO ONE` promotes a replica to master. Client reconfiguration is the client's responsibility. Proven by `REPL-06`.
- **Cluster mode:** not in v0.1.3 scope. Deferred to v0.2+.

## Security Guarantees

- **TLS version floor:** TLS 1.3 mandatory when TLS is enabled. TLS 1.2 permitted via explicit opt-in (`--tls-version 1.2`) for legacy clients.
- **Cipher allowlist:** frozen in code and audited in Phase 98 (`SEC-06`).
- **mTLS:** supported when `--tls-ca-cert-file` provided; client cert required for connection. Proven by TLS integration tests.
- **ACL enforcement:** every command dispatch checks the user's category + key-pattern rules before execution. Proven by `SEC-08` ACL fuzzing (Phase 89).
- **Lua sandbox scope:** no fs, net, os, debug, or package access. Audited in `SEC-04` (Phase 98). Any sandbox finding is a P0.
- **Unsafe code:** every `unsafe` block carries a `// SAFETY:` comment. Enforced by `UNSAFE-01` xtask audit in CI.
- **CVE disclosure:** [SECURITY.md](../SECURITY.md) (Phase 98 `SEC-07`) documents the 90-day embargo disclosure process and GPG key.
- **Supply chain:** `cargo audit` + `cargo deny` block PR merges. SBOM (CycloneDX) published per release. Release artifacts signed via cosign with provenance attestation. All enforced in Phase 98 (`SEC-01`, `SEC-02`).

## Out of Scope

Explicitly **not** promised by this contract:

| Excluded capability | Reason |
|---|---|
| Cluster mode GA | Deferred to v0.2+ — Jepsen-grade cluster testing is a milestone on its own |
| Multi-master / active-active | Not in Moon's architectural scope |
| Cross-region replication | Single-datacenter async replication only |
| Redis Modules API | Moon builds features natively — modules conflict with thread-per-core ownership model |
| Sentinel | Cluster mode (when GA) subsumes HA coordination |
| macOS native runtime | Dev-only via OrbStack VM; kqueue integration with cold tier not validated |
| Windows | io_uring unavailable; not staffed |
| WSL1 | io_uring unavailable |
| Kernel < 6.1 | io_uring feature floor below SLO viability |
| GPU vector acceleration | Feature-gated (`gpu-cuda`), not on default path |
| DiskANN | Not stabilized in v0.1.3 |
| HexaHNSW GA | Still experimental — recall gains not validated on real datasets |
| Redis Functions (scripting v2) | Deferred; EVAL/EVALSHA covers scripting needs |
| Client-side caching (invalidation tracking) | Deferred to v0.2+ |

## GA Exit Criteria Checklist

Every box below must be ticked green before `v0.1.3` promotes to `v1.0-rc1`. Each line links to a REQ-ID in [`.planning/REQUIREMENTS.md`](../.planning/REQUIREMENTS.md) and the phase that closes it.

### Production Contract
- [ ] `CONTRACT-01` — this document published (Phase 87) ← **you are here**

### Toolchain
- [ ] `RUST-01` — MSRV and CI on Rust 1.94.*, clippy clean on both feature sets, no Criterion regression > 2 % (Phase 88)

### Correctness Hardening
- [ ] `FUZZ-01` — cargo-fuzz targets for RESP/RDB/WAL/cluster/ACL parsers; 24 h cumulative clean (Phase 89)
- [ ] `LOOM-01` — loom model tests for ResponseSlot, SPSC drain+notify, pending-wakers (Phase 89)
- [ ] `SEC-08` — ACL glob pattern + key-bypass fuzzing clean (Phase 89)
- [ ] `UNSAFE-01` — 100 % `// SAFETY:` coverage, CI-enforced (Phase 90)
- [ ] `PANIC-01` — zero `unwrap`/`expect`/`panic` on hot-path modules, module-scoped clippy deny (Phase 90)
- [ ] `SEC-05` — `docs/security/unsafe-audit.md` published (Phase 90)

### Code Hygiene
- [ ] `HYGIENE-01` — all files ≤ 1500 lines per project rule (Phase 91)
- [ ] `HYGIENE-02` — unified `ConnectionCore` state machine; three handlers reduced to thin adapters (Phase 91)
- [ ] `HYGIENE-03` — `src/lib.rs` `#![allow(...)]` list audited and justified (Phase 91)

### Observability
- [ ] `METRICS-01` — Prometheus `/metrics` on admin port with full metric set (Phase 92)
- [ ] `SLOWLOG-01` — Redis-compatible SLOWLOG commands (Phase 92)
- [ ] `HEALTH-01` — `/healthz` + `/readyz` endpoints (Phase 92)
- [ ] `TRACE-01` — structured tracing spans with sampling (Phase 92)
- [ ] `INFO-01` — INFO parity with Redis 7.x (Phase 92)
- [ ] `CONFIG-01` — `moon --check-config` validator (Phase 92)
- [ ] `CONFIG-02` — TLS SIGHUP hot-reload (Phase 92)

### Durability Proof
- [ ] `OFFLOAD-02` — `feat/disk-offload` merged; recovery v3 validated (Phase 93)
- [ ] `CRASH-01` — crash-injection matrix green (Phase 94)
- [ ] `TORN-01` — torn-write replay clean (Phase 94)
- [ ] `OFFLOAD-01` — disk-offload SIGKILL crash test clean (Phase 94)
- [ ] `JEPSEN-01` — Jepsen-lite linearizability green (Phase 94)
- [ ] `BACKUP-01` — BGSAVE → restore `DEBUG DIGEST` parity; RTO recorded in runbook (Phase 94)

### Replication Hardening
- [ ] `REPL-01..06` — PSYNC partial + full, partition, kill-restart, lag metric, promotion (Phase 95)

### Compatibility Matrix
- [ ] `COMPAT-01` — 8-client CI matrix green (Phase 96)
- [ ] `COMPAT-02` — vector client smoke tests (Phase 96)
- [ ] `COMPAT-03` — `docs/redis-compat.md` published (Phase 96)
- [ ] `COMPAT-04` — Redis TCL subset in CI (Phase 96)

### Performance SLO Lock-In
- [ ] `PERF-01` — Criterion regression gate active in CI (Phase 97)
- [ ] `PERF-02` — 24 h HDR histogram rig on reference hardware; numbers above promoted from `[provisional]` (Phase 97)
- [ ] `PERF-03` — RSS-per-1M-keys memory gate (Phase 97)
- [ ] `PERF-04` — x86_64 monoio accept-loop regression closed (Phase 97)
- [ ] `PERF-05` — 7-day soak clean (Phase 97)

### Security Hardening
- [ ] `SEC-01` — `cargo audit` + `cargo deny` CI blocking (Phase 98)
- [ ] `SEC-02` — SBOM + cosign signing (Phase 98)
- [ ] `SEC-03` — `docs/THREAT-MODEL.md` published (Phase 98)
- [ ] `SEC-04` — `docs/security/lua-sandbox.md` published (Phase 98)
- [ ] `SEC-06` — TLS cipher allowlist frozen + cert rotation tested (Phase 98)
- [ ] `SEC-07` — `SECURITY.md` disclosure policy published (Phase 98)

### Release Engineering
- [ ] `REL-01` — `docs/versioning.md` + `MOON_FORMAT_VERSION` (Phase 99)
- [ ] `REL-02` — Upgrade/downgrade tests (Phase 99)
- [ ] `REL-03` — Artifacts: musl aarch64+x86_64, deb, rpm, Docker, systemd (Phase 99)
- [ ] `REL-04` — CHANGELOG CI gate (Phase 99)
- [ ] `REL-05` — Operator runbooks ([`runbooks/`](./runbooks/)) (Phase 99)
- [ ] `REL-06` — User docs (getting-started, config, commands, tuning, migration) (Phase 99)
- [ ] `REL-07` — Tag-triggered release pipeline (Phase 99)

### GA Gate
- [ ] `GA-01` — `v0.1.3-rc1` tagged + 4-week RC soak with 0 P0 bugs (Phase 100)
- [ ] `GA-02` — Every checkbox above ticked green → `v0.1.3` final → v1.0 candidate (Phase 100)

## Revision history

| Date | Change | Phase |
|---|---|---|
| 2026-04-08 | Initial publication — provisional SLO numbers from v0.1.2 benchmark memory; full checklist structure locked | 87 |

---
phase: 6
slug: setup-and-benchmark-real-use-cases
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-23
---

# Phase 6 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Shell scripts + file existence checks |
| **Config file** | None |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test && bash -n bench.sh` |
| **Estimated runtime** | ~15 seconds (tests only; benchmarks are manual) |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib`
- **After every plan wave:** Run `cargo test`
- **Before `/gsd:verify-work`:** Full suite green + BENCHMARK.md populated
- **Max feedback latency:** 20 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Deliverable | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 06-01-01 | 01 | 1 | README, Dockerfile, CI | file check | `test -f README.md && test -f Dockerfile && test -f .github/workflows/ci.yml` | ❌ W0 | ⬜ pending |
| 06-01-02 | 01 | 1 | Cargo.toml metadata | metadata | `cargo metadata --format-version 1` | ✅ | ⬜ pending |
| 06-02-01 | 02 | 1 | bench.sh | script check | `test -x bench.sh && bash -n bench.sh` | ❌ W0 | ⬜ pending |
| 06-03-01 | 03 | 2 | BENCHMARK.md | file check | `test -f BENCHMARK.md && grep -q "ops/sec" BENCHMARK.md` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] README.md — project documentation
- [ ] Dockerfile — containerized build
- [ ] .github/workflows/ci.yml — CI pipeline
- [ ] bench.sh — benchmark runner script
- [ ] BENCHMARK.md — results document

---

## Manual-Only Verifications

| Behavior | Deliverable | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Docker image builds and runs | Dockerfile | Requires Docker daemon | `docker build -t rust-redis . && docker run -p 6379:6379 rust-redis` |
| Benchmarks produce real numbers | BENCHMARK.md | Requires redis-benchmark + server running | Run `./bench.sh` and verify BENCHMARK.md has numeric results |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 20s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending

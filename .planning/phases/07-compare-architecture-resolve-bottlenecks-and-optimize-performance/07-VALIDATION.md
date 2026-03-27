---
phase: 7
slug: compare-architecture-resolve-bottlenecks-and-optimize-performance
status: draft
nyquist_compliant: false
wave_0_complete: true
created: 2026-03-23
---

# Phase 7 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test (built-in) + criterion 0.8 (benchmarks) |
| **Config file** | Cargo.toml [dev-dependencies] |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~20 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib`
- **After every plan wave:** Run `cargo test`
- **Before `/gsd:verify-work`:** Full `cargo test` green + `./bench.sh --rust-only` shows improvement
- **Max feedback latency:** 25 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 07-01-01 | 01 | 1 | OPT-02, OPT-05 | unit | `cargo test --lib storage::entry` | ✅ (update) | ⬜ pending |
| 07-01-02 | 01 | 1 | OPT-03, OPT-04 | regression | `cargo test` | ✅ | ⬜ pending |
| 07-02-01 | 02 | 2 | OPT-01, OPT-06, OPT-07 | regression+int | `cargo test` | ✅ | ⬜ pending |
| 07-03-01 | 03 | 3 | OPT-01 | benchmark | `./bench.sh --rust-only` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

*None — existing test infrastructure covers all requirements. Tests updated inline with Entry struct changes.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Benchmark improvement visible | OPT-01 | Requires full benchmark run | Run `./bench.sh --rust-only`, compare to baseline in BENCHMARK.md |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 25s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending

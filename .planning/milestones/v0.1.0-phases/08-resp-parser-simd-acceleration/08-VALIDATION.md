---
phase: 8
slug: resp-parser-simd-acceleration
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-24
---

# Phase 8 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test (unit + integration) + Criterion 0.8 (benchmarks) |
| **Config file** | Cargo.toml (bench config exists) |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~5 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib`
- **After every plan wave:** Run `cargo test`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 5 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 08-01-01 | 01 | 1 | memchr CRLF scanning | unit | `cargo test --lib protocol` | ✅ | ⬜ pending |
| 08-01-02 | 01 | 1 | atoi integer parsing | unit | `cargo test --lib protocol` | ✅ | ⬜ pending |
| 08-02-01 | 02 | 1 | bumpalo arena integration | unit | `cargo test --lib server` | ❌ W0 | ⬜ pending |
| 08-03-01 | 03 | 2 | Criterion benchmark >=4x | bench | `cargo bench --bench resp_parsing` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Existing test infrastructure covers all phase requirements (438 unit tests + 49 integration tests already exist)
- Criterion benchmark harness already configured in Cargo.toml

*Existing infrastructure covers all phase requirements.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| SIMD path activation | memchr uses SSE2/AVX2 | Runtime CPU detection, not testable in unit tests | Check `RUSTFLAGS="-C target-cpu=native"` enables AVX2 path |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 5s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending

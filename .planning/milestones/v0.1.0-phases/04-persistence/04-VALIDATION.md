---
phase: 4
slug: persistence
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-23
---

# Phase 4 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test + integration tests |
| **Config file** | Cargo.toml (test profile inherits) |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~15 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib`
- **After every plan wave:** Run `cargo test`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 20 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 04-01-01 | 01 | 1 | PERS-01, PERS-03 | unit | `cargo test --lib persistence::rdb` | ❌ W0 | ⬜ pending |
| 04-02-01 | 02 | 2 | PERS-02, PERS-03, PERS-05 | unit | `cargo test --lib persistence::aof` | ❌ W0 | ⬜ pending |
| 04-02-02 | 02 | 2 | PERS-04 | unit | `cargo test --lib command` | ❌ W0 | ⬜ pending |
| 04-03-01 | 03 | 3 | PERS-01..05 | integration | `cargo test --test integration` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `src/persistence/mod.rs` — module declaration
- [ ] `src/persistence/rdb.rs` — RDB save/load with unit tests
- [ ] `src/persistence/aof.rs` — AOF writer/replay with unit tests
- [ ] Integration tests for BGSAVE, BGREWRITEAOF, startup restore

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Server restarts and data persists | PERS-03 | Requires process restart | Start server, SET keys, stop, restart, verify GET returns values |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 20s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending

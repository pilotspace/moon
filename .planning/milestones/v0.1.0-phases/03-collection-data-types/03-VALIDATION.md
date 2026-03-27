---
phase: 3
slug: collection-data-types
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-23
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in `#[test]` + `#[tokio::test]` |
| **Config file** | Cargo.toml (test profile is default) |
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
| 03-01-01 | 01 | 1 | HASH-01..08 | unit | `cargo test --lib command::hash` | ❌ W0 | ⬜ pending |
| 03-02-01 | 02 | 1 | LIST-01..06 | unit | `cargo test --lib command::list` | ❌ W0 | ⬜ pending |
| 03-03-01 | 03 | 1 | SET-01..06 | unit | `cargo test --lib command::set` | ❌ W0 | ⬜ pending |
| 03-04-01 | 04 | 2 | ZSET-01..09 | unit | `cargo test --lib command::sorted_set` | ❌ W0 | ⬜ pending |
| 03-05-01 | 05 | 2 | EXP-02, KEY-07, KEY-09, CONN-06 | unit+int | `cargo test --lib expiration && cargo test --test integration` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `cargo add ordered-float@5.1 rand@0.10` — new dependencies
- [ ] `src/command/hash.rs` — inline `#[cfg(test)] mod tests`
- [ ] `src/command/list.rs` — inline tests
- [ ] `src/command/set.rs` — inline tests
- [ ] `src/command/sorted_set.rs` — inline tests
- [ ] `src/server/expiration.rs` — inline tests for expire cycle
- [ ] `tests/integration.rs` — extend with AUTH, SCAN, collection tests

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| redis-cli HSCAN/SSCAN/ZSCAN interactive | KEY-07 | Real cursor interaction | Start server, use redis-cli to SCAN through collection |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 20s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending

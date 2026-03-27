---
phase: 43
slug: rdb-snapshot-hardening-redis-rdb-format-compatibility-async-i-o-completion-tracking-and-streaming-snapshot-for-replication
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-26
---

# Phase 43 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust `#[cfg(test)]` + `cargo test` |
| **Config file** | Cargo.toml (test section) |
| **Quick run command** | `cargo test --lib persistence::snapshot --lib persistence::rdb -- --nocapture` |
| **Full suite command** | `cargo test --lib -- --nocapture` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib persistence::snapshot --lib persistence::rdb -- --nocapture`
- **After every plan wave:** Run `cargo test --lib -- --nocapture`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 43-01-01 | 01 | 1 | BGSAVE completion | unit | `cargo test --lib command::persistence` | ✅ | ⬜ pending |
| 43-01-02 | 01 | 1 | SAVE/LASTSAVE cmds | unit | `cargo test --lib command::persistence` | ✅ | ⬜ pending |
| 43-01-03 | 01 | 1 | Async finalize | unit | `cargo test --lib persistence::snapshot` | ✅ | ⬜ pending |
| 43-02-01 | 02 | 2 | Redis RDB export | unit | `cargo test --lib persistence::rdb` | ✅ | ⬜ pending |
| 43-02-02 | 02 | 2 | Streaming RDB | unit | `cargo test --lib persistence::rdb` | ❌ W0 | ⬜ pending |
| 43-02-03 | 02 | 2 | INFO persistence | unit | `cargo test --lib command::persistence` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Existing infrastructure covers all phase requirements — `cargo test` framework already in place
- New test files will be added alongside implementation

*If none: "Existing infrastructure covers all phase requirements."*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| BGSAVE completes across shards | completion tracking | Requires multi-shard runtime | Start server, BGSAVE, check LASTSAVE updates |
| Replication full resync uses streaming RDB | streaming snapshot | Requires master+replica pair | Start master, connect replica, verify FULLRESYNC |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending

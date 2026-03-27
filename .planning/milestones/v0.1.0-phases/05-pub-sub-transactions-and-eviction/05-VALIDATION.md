---
phase: 5
slug: pub-sub-transactions-and-eviction
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-23
---

# Phase 5 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test (built-in) + redis crate 0.27 for integration |
| **Config file** | Cargo.toml `[dev-dependencies]` |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~20 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib`
- **After every plan wave:** Run `cargo test`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 25 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 05-01-01 | 01 | 1 | PUB-01..04 | unit+int | `cargo test --lib pubsub && cargo test --test integration pubsub` | ❌ W0 | ⬜ pending |
| 05-02-01 | 02 | 1 | TXN-01, TXN-02 | unit+int | `cargo test --lib command::transaction && cargo test --test integration` | ❌ W0 | ⬜ pending |
| 05-03-01 | 03 | 2 | EVIC-01..03 | unit+int | `cargo test --lib storage::eviction && cargo test --test integration` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `src/pubsub/mod.rs` — PubSubRegistry unit tests
- [ ] `src/storage/eviction.rs` — eviction unit tests
- [ ] `tests/integration.rs` — Pub/Sub, MULTI/EXEC/WATCH, CONFIG, eviction integration tests

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Two redis-cli sessions Pub/Sub | PUB-01 | Real interactive test | Open two terminals, SUBSCRIBE in one, PUBLISH in other |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 25s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending

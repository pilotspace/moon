---
phase: 2
slug: working-server
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-23
---

# Phase 2 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test (built-in) + tokio::test for async |
| **Config file** | None — see Wave 0 |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~10 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib`
- **After every plan wave:** Run `cargo test`
- **Before `/gsd:verify-work`:** Full suite must be green + manual redis-cli smoke test
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 02-01-01 | 01 | 1 | PROTO-03, NET-01 | integration | `cargo test --test integration` | ❌ W0 | ⬜ pending |
| 02-01-02 | 01 | 1 | NET-02, NET-03 | integration | `cargo test --test integration shutdown` | ❌ W0 | ⬜ pending |
| 02-01-03 | 01 | 1 | CONN-01, CONN-02, CONN-03 | unit | `cargo test --lib command::connection` | ❌ W0 | ⬜ pending |
| 02-01-04 | 01 | 1 | CONN-04, CONN-05, CONN-07 | unit | `cargo test --lib command::connection` | ❌ W0 | ⬜ pending |
| 02-02-01 | 02 | 2 | STR-01, STR-02 | unit | `cargo test --lib command::string` | ❌ W0 | ⬜ pending |
| 02-02-02 | 02 | 2 | STR-03, STR-04, STR-05 | unit | `cargo test --lib command::string` | ❌ W0 | ⬜ pending |
| 02-02-03 | 02 | 2 | STR-06, STR-07, STR-08, STR-09 | unit | `cargo test --lib command::string` | ❌ W0 | ⬜ pending |
| 02-03-01 | 03 | 2 | KEY-01, KEY-02, KEY-03, KEY-04 | unit | `cargo test --lib command::key` | ❌ W0 | ⬜ pending |
| 02-03-02 | 03 | 2 | KEY-05, KEY-06, KEY-08 | unit | `cargo test --lib command::key` | ❌ W0 | ⬜ pending |
| 02-03-03 | 03 | 2 | EXP-01 | unit | `cargo test --lib storage::test_lazy_expiry` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/integration.rs` — integration tests with TCP connections using redis client crate
- [ ] Unit test modules in each `command/*.rs` file — inline `#[cfg(test)]` modules
- [ ] Unit test module in `storage/db.rs` — Database operations and lazy expiry
- [ ] Unit test module in `server/codec.rs` — Decoder/Encoder round-trip
- [ ] `Cargo.toml` dev-dependency: `redis = { version = "0.27", features = ["tokio-comp"] }`
- [ ] `Cargo.toml` dev-dependency: `tokio = { version = "1.50", features = ["test-util"] }`

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| redis-cli connects and works interactively | NET-01, CONN-01 | Real redis-cli binary interaction | Start server, run `redis-cli -p 6379`, type `PING`, verify `PONG` |
| Graceful shutdown from terminal | NET-02 | Requires real signal delivery | Start server, press Ctrl+C, verify clean exit |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending

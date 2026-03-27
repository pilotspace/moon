---
phase: 1
slug: protocol-foundation
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-23
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test framework (cargo test) + criterion 0.8 for benchmarks |
| **Config file** | None — Rust test framework is zero-config |
| **Quick run command** | `cargo test --lib -q` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~5 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib -q`
- **After every plan wave:** Run `cargo test -q`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 10 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 01-01-01 | 01 | 1 | PROTO-01 | unit | `cargo test protocol::parse::tests -q` | ❌ W0 | ⬜ pending |
| 01-01-02 | 01 | 1 | PROTO-01 | unit | `cargo test protocol::serialize::tests -q` | ❌ W0 | ⬜ pending |
| 01-01-03 | 01 | 1 | PROTO-01 | unit | `cargo test protocol::tests::roundtrip -q` | ❌ W0 | ⬜ pending |
| 01-01-04 | 01 | 1 | PROTO-01 | unit | `cargo test protocol::parse::tests::incomplete -q` | ❌ W0 | ⬜ pending |
| 01-01-05 | 01 | 1 | PROTO-01 | unit | `cargo test protocol::parse::tests::null -q` | ❌ W0 | ⬜ pending |
| 01-01-06 | 01 | 1 | PROTO-01 | unit | `cargo test protocol::parse::tests::invalid -q` | ❌ W0 | ⬜ pending |
| 01-01-07 | 01 | 1 | PROTO-01 | unit | `cargo test protocol::parse::tests::limits -q` | ❌ W0 | ⬜ pending |
| 01-02-01 | 02 | 1 | PROTO-02 | unit | `cargo test protocol::inline::tests -q` | ❌ W0 | ⬜ pending |
| 01-02-02 | 02 | 1 | PROTO-02 | unit | `cargo test protocol::inline::tests::edge_cases -q` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `Cargo.toml` — project initialization with bytes, thiserror, tikv-jemallocator, criterion
- [ ] `src/lib.rs` — module re-exports
- [ ] `src/main.rs` — jemalloc global allocator setup placeholder
- [ ] `src/protocol/mod.rs` — module structure
- [ ] `src/protocol/frame.rs` — Frame enum and helpers
- [ ] `src/protocol/parse.rs` — parser with inline tests
- [ ] `src/protocol/serialize.rs` — serializer with inline tests
- [ ] `src/protocol/inline.rs` — inline command parser with inline tests
- [ ] `benches/resp_parsing.rs` — criterion benchmark harness

---

## Manual-Only Verifications

*All phase behaviors have automated verification.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending

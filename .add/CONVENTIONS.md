# CONVENTIONS  (living documentation — set once, kept for the whole project)
<!-- evidence-grounded: CLAUDE.md "Coding Rules", Cargo.toml, .github/workflows -->

Language/framework: Rust 1.94 (edition 2024, MSRV enforced in CI) · runtimes: monoio (default, io_uring) + tokio (portability/CI)
Folders: production code in `src/<module>/` (directory-module convention: `mod.rs` + split read/write files); tests in `tests/` (integration, real server instances — no mocking) + `#[cfg(test)]` in `mod.rs`; benches in `benches/` (Criterion); fuzz targets in `fuzz/fuzz_targets/`. ADD task files: `.add/tasks/<slug>/TASK.md` (task tests/src point INTO the repo tree, declared per-task).
Naming: snake_case files/functions · PascalCase types · SCREAMING_SNAKE consts · crate:: imports in submodules (never super::super)
Lint/format: `cargo fmt --check` · `cargo clippy -- -D warnings` (default + tokio,jemalloc feature sets) · unsafe audit (`scripts/audit-unsafe.sh`) · unwrap ratchet (`scripts/audit-unwrap.sh`) — all enforced in CI
Errors: command errors are `Frame::Error(Bytes)` with Redis-compatible `-ERR …` text on dispatch paths (no Result); library code uses `thiserror`; `anyhow` only in main.rs/tests; no unwrap/expect in library code
Architecture: thread-per-core shared-nothing shards; per-shard locks only (parking_lot, never std::sync; never held across .await); no global locks on the write path; no allocation on hot paths (command dispatch, protocol parse, event loop, io drivers); cross-shard communication via flume/SPSC messages only; dual-runtime: all runtime-specific code compiles under both feature sets; Linux-only code behind `#[cfg(target_os = "linux")]` with non-Linux fallback; files ≤1500 lines (split into directory modules)
Verification: red/green TDD per task; full local CI parity via OrbStack `moon-dev` before push; benchmark numbers from Linux VM only; new parsers get fuzz targets; new atomic state machines get loom models

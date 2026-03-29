---
name: dual-runtime-check
description: Verify code compiles and tests pass under both runtime-tokio and runtime-monoio feature sets. Run before committing runtime-adjacent changes.
---

Dual-runtime compilation and test verification for moon.

## Steps

1. **Check tokio compilation**:
   ```bash
   cargo check --no-default-features --features runtime-tokio,jemalloc
   ```

2. **Check monoio compilation**:
   ```bash
   cargo check --no-default-features --features runtime-monoio,jemalloc
   ```

3. **Clippy tokio**:
   ```bash
   cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings
   ```

4. **Clippy monoio**:
   ```bash
   cargo clippy --no-default-features --features runtime-monoio,jemalloc -- -D warnings
   ```

5. **Test tokio** (CI-compatible):
   ```bash
   MOON_NO_URING=1 cargo test --no-default-features --features runtime-tokio,jemalloc
   ```

6. **MSRV check**:
   ```bash
   cargo +1.85 check --no-default-features --features runtime-tokio
   ```

## Report

For each step: PASS/FAIL with error details.

If any step fails, identify whether the issue is:
- Missing feature gate (`#[cfg(feature = "...")]`)
- Platform-specific code lacking `#[cfg(target_os = "...")]`
- API difference between tokio and monoio
- MSRV violation (using features from Rust > 1.85)

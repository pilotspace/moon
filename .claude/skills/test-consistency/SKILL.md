---
name: test-consistency
description: Run the rust-redis consistency test suite, comparing output against Redis as ground truth. Requires redis-server on PATH. Args: --shards N (default 1), --skip-build, --port-rust N.
---

Run the consistency test suite for rust-redis.

## Steps

1. Confirm `redis-server` is available:
   ```bash
   redis-server --version
   ```
   If not found, tell the user to install Redis first.

2. Build the release binary (unless `--skip-build` passed):
   ```bash
   cargo build --release --no-default-features --features runtime-tokio
   ```

3. Run the consistency tests:
   ```bash
   ./scripts/test-consistency.sh [args]
   ```
   Pass through any args the user provided (e.g. `--shards 4`, `--skip-build`).

4. Report the result:
   - Show PASS/FAIL counts
   - If any failures, show the failing test descriptions and expected vs actual output
   - The target is 132/132 tests passing

## Notes

- Tests compare rust-redis output against Redis on all data types: strings (SSO inline, heap small, heap large, binary), hash, list, set, zset
- Uses port 6399 for Redis and 6400 for rust-redis by default
- If ports are in use, suggest `--port-rust 6401`

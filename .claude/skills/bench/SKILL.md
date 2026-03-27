---
name: bench
description: Run rust-redis benchmarks. Args: micro|production|resources|profile|all (default: micro). Optional: --shards N, --duration SEC, --scenario NAME.
---

Run the specified benchmark for rust-redis.

## Argument Handling

Parse the user's args to determine which benchmark to run:

- `micro` (default) — Criterion micro-benchmarks: `cargo bench`
- `production` — Full production workload vs Redis: `./scripts/bench-production.sh`
- `resources` — Memory/resource benchmark: `./scripts/bench-resources.sh`
- `profile` — CPU + memory profiling suite: `./scripts/profile.sh`
- `all` — Run all of the above sequentially

Pass through any extra flags (e.g. `--shards 4`, `--duration 60`, `--scenario session-store`) to the appropriate script.

## Steps

1. Check that the release binary exists at `./target/release/rust-redis`. If not, build first:
   ```bash
   cargo build --release --no-default-features --features runtime-tokio
   ```

2. Run the selected benchmark:

   **micro**:
   ```bash
   RUSTFLAGS="-C target-cpu=native" cargo bench
   ```

   **production**:
   ```bash
   ./scripts/bench-production.sh [extra args]
   ```

   **resources**:
   ```bash
   ./scripts/bench-resources.sh [extra args]
   ```

   **profile**:
   ```bash
   ./scripts/profile.sh [extra args]
   ```

3. Summarize results. For production/resources, the script writes a Markdown report — show the key throughput table and ratios.

## Notes

- Peak advantage is at `--shards 1` and `-c 8` to `-c 16` parallelism
- Always use `redis-benchmark 8.x` (required for `\r` response parsing)
- Use `-r` flag for unique keys to avoid hot-path cache effects
- For fair per-key memory comparison, use `--shards 1`

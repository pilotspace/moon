# OrbStack `moon-dev` — Linux-parity environment

> Moved out of `CLAUDE.md` (2026-08-27): day-to-day development is native on the
> macOS host. This VM is still required by `scripts/ci-local.sh` in its default
> (`--full`) mode — the merge bar — and by every published benchmark number.

OrbStack is used for Linux-parity builds, production benchmarks, and io_uring testing on macOS hosts.

## Machine: `moon-dev`

- **OS:** Ubuntu 24.04 (kernel 6.17+, full io_uring support)
- **Arch:** aarch64 (matches Apple Silicon host)
- **Rust:** 1.94.1 (MSRV-pinned)
- **Tools:** build-essential, pkg-config, libssl-dev, redis-server

OrbStack auto-mounts the macOS filesystem (including `/Volumes/`) into the VM at the same paths — edit on macOS, compile on Linux. No rsync or Docker volumes needed. `orb run` preserves the caller's working directory, so commands run from the repo need no `cd` at all.

> **⚠ Stale checkout trap:** `/Users/tindang/workspaces/tind-repo/moon` is an OLD second checkout (stuck at hash-ttl era). The live repo is `/Volumes/Games/tindang-repo/moon` — never `cd` to the old path in VM commands, and pin `MOON_BIN` explicitly for integration tests that spawn a server binary (`find_moon_binary()` falls back to `target/release/moon`, whose provenance is unknown).

## Commands

```bash
# Build (release)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && cargo build --release'

# Test (all)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && cargo test --release'

# Test (tokio runtime, CI parity)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && cargo test --no-default-features --features runtime-tokio,jemalloc'

# Clippy
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && cargo clippy -- -D warnings'

# Run server
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && ./target/release/moon --port 6399 --shards 4'

# Benchmark (redis-benchmark from macOS can reach moon-dev via OrbStack networking)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && cargo bench'

# Interactive shell
orb run -m moon-dev bash
```

## Recreating the Machine

If the machine is lost or corrupted:
```bash
orb delete moon-dev
orb create ubuntu moon-dev
orb run -m moon-dev bash -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.94.1'
orb run -m moon-dev bash -c 'sudo apt-get update -qq && sudo apt-get install -y -qq build-essential pkg-config libssl-dev redis-server'
```

## OrbStack Rules for Claude Code

- **`cargo build`/`cargo test` on macOS is now fully supported** — macOS is a first-class target.
- Use `orb run -m moon-dev` for Linux-specific testing (io_uring, O_DIRECT, connection migration).
- All **benchmark numbers** MUST come from the Linux VM (or GCloud instances).
- The VM path to the repo is the same as macOS: `/Volumes/Games/tindang-repo/moon`.
- Use `source ~/.cargo/env &&` prefix in every `orb run` command.
- Use `CARGO_TARGET_DIR=target-linux` for VM builds of the shared checkout so Linux ELF and macOS Mach-O artifacts never clobber each other (`/target-linux/` is gitignored).
- **Diskfull guard:** Moon pauses writes (`MOONERR diskfull`) when the data dir's filesystem has <5% free. `/Volumes/Games` hovers near that line — run server-spawning suites (e.g. `scripts/test-consistency.sh`) from a VM-local clone (`git clone --depth 1 file:///Volumes/Games/tindang-repo/moon ~/moon-consistency`) or pass a fresh `--dir` on VM /tmp.
- Don't edit sources on macOS while a VM build of the same checkout is compiling (shared fs → spurious compile errors).

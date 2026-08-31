# OrbStack `moon-dev` — Linux-parity environment

> Moved out of `CLAUDE.md` (2026-08-27): day-to-day development is native on the
> macOS host. This VM is still required by `scripts/ci-local.sh` in its default
> (`--full`) mode — the merge bar — and by every published benchmark number.

OrbStack is used for Linux-parity builds, production benchmarks, and io_uring testing on macOS hosts.

## Machine: `moon-dev`

- **OS:** Ubuntu 26.04.1 LTS (kernel 6.17+, full io_uring support). `orb create ubuntu`
  tracks the current release — it produced 24.04 when this doc was written and 26.04.1 on
  2026-08-31. The apt `redis-server` is 8.0.5, so the compat oracle version is unaffected.
- **Arch:** aarch64 (matches Apple Silicon host)
- **Rust:** stable via rustup, but `rust-toolchain.toml` pins **1.94.1** inside the repo, so
  no toolchain pinning is needed at install time — `rustc --version` reads 1.94.1 in-tree
  and the newer stable everywhere else.
- **vCPU/RAM:** 6 / 12288 MiB, from OrbStack's **global** config. A wipe has reset it to 2
  before now; verify with `orb config show`, and never raise it without asking — see
  `CLAUDE.md` on the deliberate cap.
- **Tools:** build-essential, pkg-config, libssl-dev, redis-server, **git**,
  **python3-redis**, **libicu-dev**, curl, **cargo-nextest**. The last four are not
  optional: `git` (VM-local `file://` clones fail with exit 127), `python3-redis`
  (`scripts/test-consistency.sh` dies mid-suite with `ModuleNotFoundError`), `libicu-dev`
  (GitHub Actions runner dependency), and nextest (every `ci-local` VM leg runs
  `cargo nextest run --profile ci`; without it the legs fall back to a slower
  `cargo test` with no flake retries).

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

**The machine really does vanish.** `orbctl list` has come back empty five times
(2026-07-10, 08-19, 08-30, 08-31), twice *mid-run*. Two things break at once and only one
of them is obvious:

- `scripts/ci-local.sh` cannot run its VM legs. Since #781 it **refuses outright with
  exit 4** rather than running the macOS suite for another ~20 minutes and then reporting a
  failure that reads like a test failure. If you see that message, you are here.
- **The self-hosted Actions runner dies with the VM**, so the hosted `check-monoio` and
  `client-compat` legs queue forever with no error. Reinstalling the VM is not enough;
  step 5 below is not optional.

Full recreate, ~15 minutes end to end. The first `ci-local` afterwards is a cold build
(VM-local `~/ci-target` caches are gone with the machine).

```bash
# 1. Machine. (No `orb delete` needed when it has already vanished.)
orb create ubuntu moon-dev
orb config show | grep -E '^cpu|^memory_mib'   # expect 6 / 12288 — a wipe resets this

# 2. System packages — all of these, see "Tools" above for why.
orb run -m moon-dev bash -lc 'sudo apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive \
  apt-get install -y -qq build-essential pkg-config libssl-dev redis-server \
                        git python3-redis libicu-dev curl'

# 3. Rust. `rust-toolchain.toml` pins 1.94.1 in-tree, so install plain stable.
orb run -m moon-dev bash -lc 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs \
  | sh -s -- -y --default-toolchain stable'

# 4. nextest — every ci-local VM leg uses it.
orb run -m moon-dev bash -lc 'source ~/.cargo/env && \
  curl -LsSf https://get.nexte.st/latest/linux-arm | tar zxf - -C ~/.cargo/bin'

# 5. GitHub Actions runner. Do this AFTER any VM suite finishes — a starting runner
#    immediately drains the queued backlog and competes with whatever is running.
TOKEN=$(gh api -X POST repos/pilotspace/moon/actions/runners/registration-token -q .token)
orb run -m moon-dev bash -lc "mkdir -p ~/actions-runner && cd ~/actions-runner && \
  curl -sL -o r.tar.gz https://github.com/actions/runner/releases/download/v2.336.0/actions-runner-linux-arm64-2.336.0.tar.gz && \
  tar xzf r.tar.gz && rm r.tar.gz && \
  sudo ./bin/installdependencies.sh && \
  ./config.sh --url https://github.com/pilotspace/moon --token $TOKEN \
              --name moon-dev-vm --labels moon-dev --unattended --replace && \
  sudo ./svc.sh install \$(whoami) && sudo ./svc.sh start"

# 6. Verify.
gh api repos/pilotspace/moon/actions/runners -q '.runners[]|"\(.name) \(.status)"'
```

`installdependencies.sh` must run **before** `config.sh`, not after. After an `orb stop`/
`start` GitHub may show the runner offline while systemd reports the service active —
`sudo systemctl restart actions.runner.pilotspace-moon.moon-dev-vm.service` re-registers it.

Once the runner comes online it picks up everything that queued while the VM was down.
Check for a long nightly (crash-matrix, fuzz) that would resume into working hours and
hold the machine for hours before you start your own gates.

## OrbStack Rules for Claude Code

- **`cargo build`/`cargo test` on macOS is now fully supported** — macOS is a first-class target.
- Use `orb run -m moon-dev` for Linux-specific testing (io_uring, O_DIRECT, connection migration).
- All **benchmark numbers** MUST come from the Linux VM (or GCloud instances).
- The VM path to the repo is the same as macOS: `/Volumes/Games/tindang-repo/moon`.
- Use `source ~/.cargo/env &&` prefix in every `orb run` command.
- Use `CARGO_TARGET_DIR=target-linux` for VM builds of the shared checkout so Linux ELF and macOS Mach-O artifacts never clobber each other (`/target-linux/` is gitignored).
- **Diskfull guard:** Moon pauses writes (`MOONERR diskfull`) when the data dir's filesystem has <5% free. `/Volumes/Games` hovers near that line — run server-spawning suites (e.g. `scripts/test-consistency.sh`) from a VM-local clone (`git clone --depth 1 file:///Volumes/Games/tindang-repo/moon ~/moon-consistency`) or pass a fresh `--dir` on VM /tmp.
- Don't edit sources on macOS while a VM build of the same checkout is compiling (shared fs → spurious compile errors).

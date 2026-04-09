# Contributing to Moon

Moon is a high-performance Redis-compatible server in Rust. This guide covers the rules, conventions, and contracts that every contributor (human or AI) must follow.

For build/run/test commands and architecture details, see [CLAUDE.md](CLAUDE.md) and [README.md](README.md).

## Golden Rules

1. **Never crash on client input.** Protocol parsers, command handlers, and any code reachable from a client connection MUST return errors, not panic. No `.unwrap()` on data derived from the wire.
2. **No allocations on the hot path.** See [CLAUDE.md § Allocations on Hot Paths](CLAUDE.md#allocations-on-hot-paths). Use `itoa`, `SmallVec`, `Vec::with_capacity`, `Bytes::slice`, or `write!` to pre-allocated buffers.
3. **Every unsafe block has a `// SAFETY:` comment.** CI enforces this via `scripts/audit-unsafe.sh`. No exceptions.
4. **Both runtimes must compile.** `cargo clippy -- -D warnings` AND `cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings` must pass.
5. **No file exceeds 1500 lines.** Split into submodules proactively.

## Before You Start

```bash
# Clone and verify the toolchain auto-installs
git clone git@github.com:pilotspace/moon.git && cd moon
rustc --version  # Should show 1.94.0 (from rust-toolchain.toml)

# Run local CI parity (requires OrbStack moon-dev VM for Linux)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Users/$(whoami)/workspaces/tind-repo/moon && \
  cargo fmt --check && \
  cargo clippy -- -D warnings && \
  cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings && \
  cargo test --release && \
  cargo test --no-default-features --features runtime-tokio,jemalloc'
```

## PR Checklist

Every PR must satisfy:

- [ ] `cargo fmt --check` — no formatting diffs
- [ ] `cargo clippy -- -D warnings` — zero warnings (default features)
- [ ] `cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings` — zero warnings (tokio)
- [ ] `scripts/audit-unsafe.sh` — 100% SAFETY coverage on unsafe blocks
- [ ] `scripts/audit-unwrap.sh` — unwrap count does not exceed baseline
- [ ] All existing tests pass (no regressions)
- [ ] New code has tests (unit tests for logic, fuzz targets for parsers)
- [ ] No file exceeds 1500 lines
- [ ] CHANGELOG.md `[Unreleased]` section updated (or `skip-changelog` label with justification)

## Code Conventions

### Error Handling Contract

| Context | Pattern | Example |
|---|---|---|
| Command handler | Return `Frame::Error(Bytes)` | `Frame::Error(Bytes::from_static(b"ERR wrong number of arguments"))` |
| Protocol parser (pass 2) | Return `Frame::Null` on any failure | `let Some(crlf) = find_crlf(buf, pos) else { return Frame::Null; };` |
| Library code | `thiserror` typed errors | `#[derive(Error)] enum WalError { ... }` |
| Application code (main.rs) | `anyhow` with `.context()` | `load_config().context("failed to load config")?` |
| Startup/init | `.expect("reason")` is acceptable | `ctrlc::set_handler(...).expect("failed to set Ctrl-C handler")` |
| Post-insert get | `#[allow(clippy::unwrap_used)]` + comment | `// Key just inserted above` |

### Unwrap Policy

**Forbidden** (will fail CI ratchet):
```rust
// BAD — panics on client input
let count = atoi::atoi::<i64>(line).unwrap();
let member = members.choose(&mut rng).unwrap();
```

**Allowed** (with annotation):
```rust
// OK — guarded by atomic state; see fill()
#[allow(clippy::unwrap_used)]
let data = unsafe { (*self.data.get()).take().unwrap() };

// OK — key was just inserted on line above
#[allow(clippy::unwrap_used)]
let entry = map.get_mut(&key).unwrap();
```

### Hot-Path Allocation Rules

**Forbidden in `src/{command,protocol,shard/event_loop,io}/`:**
- `Vec::new()` — use `Vec::with_capacity()`
- `String::new()`, `to_string()`, `format!()` — use `itoa::Buffer` for integers, `write!` to pre-allocated buf
- `Box::new()`, `Arc::new()` — pre-allocate or use arenas
- `.clone()` on large types — borrow instead

**Allowed:**
- `Vec::with_capacity()` for result building
- `Bytes::slice()` (Arc refcount bump, no copy)
- `Bytes::from_static()` for constant responses
- `Bytes::copy_from_slice()` for small computed values
- `itoa::Buffer::new()` (stack-allocated, no heap)

### Module Split Convention

When a file approaches 1500 lines, split it:

```
src/command/hash.rs (1500+ lines)
  → src/command/hash/
      mod.rs          (shared helpers + tests + re-exports)
      hash_read.rs    (GET-style operations)
      hash_write.rs   (SET-style operations)
```

**Rules:**
- `mod.rs` declares `mod hash_read; mod hash_write;` and `pub use hash_read::*; pub use hash_write::*;`
- Submodule files use `crate::` imports, not `super::super::`
- Shared helpers live in `mod.rs`, not duplicated
- All test code stays in `mod.rs`
- The parent module (`src/command/mod.rs`) still says `pub mod hash;` — unchanged

### Unsafe Code Contract

1. Never introduce `unsafe` without explicit approval
2. Every `unsafe {}` block MUST have `// SAFETY:` on the line(s) immediately above
3. CI script `scripts/audit-unsafe.sh` enforces this — PRs without SAFETY comments fail
4. Prefer safe abstractions. If unsafe is needed, isolate behind a safe public API
5. Approved categories (see `docs/security/unsafe-audit.md`):
   - Tagged pointer unions (CompactValue, CompactKey)
   - Swiss Table SIMD probing (DashTable segment)
   - Atomic state machines (ResponseSlot)
   - io_uring kernel interface
   - FD ownership transfer (from_raw_fd after dup/accept)
   - HNSW pointer arithmetic
   - SIMD intrinsics behind target_feature gates

### Fuzzing Contract

Every function that deserializes untrusted input MUST have a `cargo-fuzz` target:

```
fuzz/fuzz_targets/
  resp_parse.rs              # RESP2/RESP3 protocol parser
  resp_parse_differential.rs # Two-parse determinism invariant
  inline_parse.rs            # Telnet-style command parser
  wal_v3_record.rs           # WAL v3 record decoder
  rdb_load.rs                # RDB snapshot loader
  gossip_deser.rs            # Cluster gossip + roundtrip
  acl_rule.rs                # ACL rule string parser
```

**Adding a new fuzz target:**
1. Create `fuzz/fuzz_targets/your_target.rs`
2. Add `[[bin]]` entry to `fuzz/Cargo.toml`
3. Add target name to `.github/workflows/fuzz.yml` matrix
4. Create seed corpus in `fuzz/corpus/your_target/`
5. Run locally: `cargo +nightly fuzz run your_target -- -max_total_time=60`

**Fuzz target requirements:**
- Use bounded configs to prevent OOM (e.g., `max_array_depth: 4`, `max_bulk_string_size: 64*1024`)
- Must not panic on ANY input — the function under test should return errors, not crash
- Differential targets: same input → two code paths → same result

### Lock-Free Data Structure Contract

Any lock-free or atomic state machine MUST have:
1. `// SAFETY:` comments on all unsafe access
2. A `loom` model test verifying the state machine under all interleavings
3. `Send + Sync` impls with documented invariants

Current loom-tested structures:
- `ResponseSlot` (EMPTY → FILLED → EMPTY cycle, concurrent fill+take)

## CI Pipeline

| Job | What it checks | Blocks merge? |
|---|---|---|
| Format | `cargo fmt --check` | Yes |
| Clippy (default) | `cargo clippy -- -D warnings` | Yes |
| Clippy (tokio) | Same with `--no-default-features --features runtime-tokio,jemalloc` | Yes |
| Test | `cargo test --no-default-features --features runtime-tokio,jemalloc` | Yes |
| MSRV | `cargo build` with Rust 1.94 | Yes |
| Safety Audit | `scripts/audit-unsafe.sh` + `scripts/audit-unwrap.sh` | Yes |
| Fuzz (PR) | 15 min/target on 7 fuzz targets (nightly compiler) | Yes (crash = fail) |
| Fuzz (nightly) | 6h/target, corpus archived | No (advisory) |
| CodeQL | Security analysis | Yes |

## Production Contract

Moon's v1.0 promises live in [docs/PRODUCTION-CONTRACT.md](docs/PRODUCTION-CONTRACT.md). Every hardening phase ticks items off the GA Exit Criteria checklist. The contract is the single source of truth for what Moon guarantees.

## Commit Message Convention

```
<type>(<scope>): <description>

<body>
```

Types: `feat`, `fix`, `refactor`, `docs`, `style`, `build`, `test`, `perf`, `ci`
Scope: module name, phase number, or component (`phase-89`, `parse`, `ci`)

## License

By contributing, you agree your contributions are licensed under [Apache License 2.0](LICENSE).

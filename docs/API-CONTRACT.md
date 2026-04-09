# Moon API Stability Contract

This document defines which interfaces are stable, which may change, and the process for making changes. It applies to all contributors including AI assistants.

## Stability Tiers

### Tier 1: Stable (breaking change = major version bump)

These interfaces are committed to Redis compatibility. Changing them requires a migration path.

| Interface | Scope | Governed by |
|---|---|---|
| RESP2/RESP3 wire protocol | Frame parsing + serialization | Redis Protocol Spec |
| Command behavior | All 200+ commands in the phf dispatch table | Redis 7.x command reference |
| RDB file format | `MOON` magic, version byte, type tags, CRC32 | `src/persistence/rdb.rs` header |
| WAL v3 record format | Record layout, CRC32C, LZ4 compression flag | `src/persistence/wal_v3/record.rs` |
| CLI flags | `--port`, `--shards`, `--appendonly`, `--dir`, etc. | `src/config.rs` (clap derive) |
| ACL rule syntax | `+`, `-`, `~`, `&`, `>`, `<`, `#`, `!`, `%R~`, `%W~` | `src/acl/rules.rs` |
| Cluster gossip protocol | Magic, version, message types, slot bitmap | `src/cluster/gossip.rs` |
| PSYNC2 replication handshake | REPLCONF, PSYNC, RDB transfer | `src/replication/handshake.rs` |

### Tier 2: Versioned (change requires `MOON_FORMAT_VERSION` bump)

These have on-disk or on-wire representations that must be forward/backward compatible within a major version.

| Interface | Version field | Location |
|---|---|---|
| RDB snapshot | Version byte in header | `rdb.rs:load()` magic check |
| WAL v3 segments | Record type byte enum | `wal_v3/record.rs:WalRecordType` |
| Disk-offload cold tier | Segment format version | `storage/tiered/segment.rs` |
| AOF manifest | Manifest schema | `persistence/manifest.rs` |

**Rule:** never load a file with a format version higher than the running binary understands. Return a clear error with the expected and found versions.

### Tier 3: Internal (may change without notice)

These are implementation details. No stability guarantee.

| Interface | Why internal |
|---|---|
| `ShardMessage` enum variants | Cross-shard dispatch protocol — single-process only |
| `ResponseSlot` state machine | Lock-free implementation detail |
| DashTable segment layout | Storage engine internals |
| CompactKey/CompactValue bit layout | Memory optimization — may change for better SSO |
| SPSC channel mesh topology | Shard communication internals |
| `Frame` enum variants beyond RESP2/3 spec | Internal representation |
| Criterion benchmark names | Perf testing infrastructure |
| Planning artifacts (`.planning/`) | Development process, not shipped |

## Change Process

### Adding a new command

1. Add handler function in the appropriate `src/command/<type>/` module
2. Add to phf dispatch table in `src/command/mod.rs`
3. Add ACL category annotation
4. Add unit test in the module's `mod.rs` test section
5. Add entry in `scripts/test-commands.sh` and `scripts/test-consistency.sh`
6. Update CHANGELOG `[Unreleased]` under `### Added`
7. If the command touches hot paths, add a Criterion benchmark

### Adding a new fuzz target

1. Create `fuzz/fuzz_targets/<name>.rs` with `#![no_main]` and `fuzz_target!` macro
2. Add `[[bin]]` to `fuzz/Cargo.toml`
3. Add to `.github/workflows/fuzz.yml` matrix (both `fuzz-pr` and `fuzz-nightly`)
4. Create seed corpus directory `fuzz/corpus/<name>/` with 2-5 valid inputs
5. Run locally for 60s to verify no immediate crashes
6. Update this document's fuzz target inventory

### Modifying on-disk/on-wire formats (Tier 1-2)

1. **Never** change existing format without a version bump
2. Add new record types/fields additively (old reader ignores unknown types)
3. Bump `MOON_FORMAT_VERSION` in the relevant header
4. Add upgrade test: write with N-1, read with N
5. Add downgrade test: refuse to load N+1 format with clear error message
6. Document in CHANGELOG under `### Changed` with migration steps

### Modifying protocol behavior (Tier 1)

1. Check Redis 7.x behavior first — Moon aims for compatibility
2. If diverging from Redis: document in `docs/redis-compat.md` with rationale
3. If adding Redis-incompatible extensions: gate behind a feature flag or config option
4. Add compatibility tests against real Redis client libraries

## Crate Public API

Moon is a binary crate, not a library. However, `pub` items in `src/lib.rs` are used by:
- Fuzz targets (`fuzz/fuzz_targets/*.rs`)
- Integration tests (`tests/*.rs`)
- Benchmarks (`benches/*.rs`)

**Rule:** do not remove or rename `pub` items in these modules without updating all consumers:
- `protocol::{parse, ParseConfig, Frame, ParseError, inline, serialize, resp3}`
- `persistence::{rdb, wal_v3::record}`
- `cluster::gossip::{serialize_gossip, deserialize_gossip}`
- `acl::{rules::apply_rule, table::AclUser}`
- `storage::db::Database`
- `server::response_slot::ResponseSlot`

## Versioning

Moon follows SemVer with data-store semantics:
- **Major** (1.0 → 2.0): on-disk format change requiring migration, or breaking Redis command behavior change
- **Minor** (1.0 → 1.1): new commands, new config options, new metrics, additive format extensions
- **Patch** (1.0.0 → 1.0.1): bug fixes only — wire + disk compatible, no new features

Pre-1.0 (current): all interfaces may change between minor versions, but we still aim for minimal churn and document every change in CHANGELOG.

## Contract Enforcement

| Rule | Enforced by |
|---|---|
| SAFETY comments on unsafe | `scripts/audit-unsafe.sh` (CI) |
| Unwrap ratchet | `scripts/audit-unwrap.sh` (CI) |
| No panics on fuzz input | `cargo-fuzz` CI (15 min PR, 6h nightly) |
| Clippy clean | `cargo clippy -- -D warnings` (CI) |
| Format clean | `cargo fmt --check` (CI) |
| Both runtimes compile | Clippy + test on tokio features (CI) |
| File size limit | Code review (not yet automated) |
| CHANGELOG updated | `skip-changelog` label policy (Phase 99, REL-04) |

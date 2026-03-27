# Technology Stack

**Project:** Rust Redis
**Researched:** 2026-03-23
**Overall Confidence:** HIGH

## Recommended Stack

### Async Runtime and Networking

| Technology | Version | Purpose | Why | Confidence |
|------------|---------|---------|-----|------------|
| tokio | 1.49 | Async runtime, TCP listener, task spawning | Battle-tested (387M+ downloads), LTS releases, powers production infrastructure at scale. Redis-like servers need non-blocking I/O and tokio is the undisputed standard. Use `current_thread` runtime flavor to match Redis's single-threaded data path model. | HIGH |
| tokio-util | 0.7.18 | Codec framework for RESP framing | Provides `Encoder`/`Decoder` traits for protocol framing over TCP streams. Avoids hand-rolling buffered read/write state machines. | HIGH |
| bytes | 1.10 | Zero-copy byte buffer management | Reference-counted `Bytes`/`BytesMut` enable zero-copy slicing for RESP parsing. Critical for a protocol-heavy server -- avoids copying data between parsing stages. Maintained by tokio team. | HIGH |

### Core Data Structures (Hand-Written)

The project explicitly requires hand-written core data structures for control over memory layout. These are NOT external crates -- they are implemented within the project.

| Structure | Purpose | Implementation Notes |
|-----------|---------|---------------------|
| RESP Parser/Serializer | Protocol compatibility | Zero-copy parser using `bytes::Bytes`. Incremental/streaming parse to handle partial reads. Support both RESP2 and RESP3 frames. |
| String Store | GET/SET/INCR etc. | Store as `Bytes` directly -- avoids String overhead and copies from network buffer. |
| Hash Map (keys) | Main keyspace | Use `std::collections::HashMap` with a fast hasher (see below). The std HashMap already uses hashbrown/SwissTable internally since Rust 1.36. |
| Linked List | Redis Lists | Doubly-linked list with arena allocation for nodes. Supports O(1) push/pop at both ends. |
| Hash Table | Redis Hashes | Nested HashMap per key. Field-level operations. |
| Hash Set | Redis Sets | `HashSet<Bytes>` per key. |
| Skip List + Hash Map | Redis Sorted Sets (ZSET) | Hand-written skip list paired with a hash map -- mirrors Redis's actual implementation. The skip list provides O(log n) range queries while the hash map provides O(1) score lookups by member. |
| TTL/Expiration | Key expiration | Combination of lazy expiration (check on access) and active expiration (periodic sweep via a min-heap or sorted structure of expiry timestamps). |

### Supporting Libraries

| Library | Version | Purpose | Why | Confidence |
|---------|---------|---------|-----|------------|
| hashbrown | 0.16 | Direct dependency for raw hash table API | While std HashMap uses hashbrown internally, importing hashbrown directly gives access to `RawTable` API for custom hash table implementations (e.g., Redis hash type with ziplist-like compact encoding). Use `foldhash` (default hasher) which is faster than SipHash. | HIGH |
| ahash | 0.8 | Fast non-cryptographic hasher | Alternative hasher for the main keyspace HashMap. ~2-10x faster than SipHash for short keys. Redis keys are typically short strings, so hasher speed directly impacts throughput. | HIGH |
| compact_str | 0.8 | Small string optimization | Stores strings up to 24 bytes inline (on the stack) instead of heap-allocating. Redis keys and small values are frequently under 24 bytes. Reduces allocator pressure significantly. | MEDIUM |
| smallvec | 1.15 | Inline small vectors | Avoids heap allocation for small collections (e.g., command argument lists, multi-bulk arrays). Most Redis commands have fewer than 8 arguments. `SmallVec<[Bytes; 8]>` keeps argument parsing allocation-free for typical commands. | HIGH |
| slab | 0.4 | Pre-allocated client connection storage | Efficient O(1) insert/remove for managing client connections by index. Used by tokio internally. Avoids HashMap overhead for connection tracking. | HIGH |

### Memory Allocator

| Technology | Version | Purpose | Why | Confidence |
|------------|---------|---------|-----|------------|
| tikv-jemallocator | 0.6 | Global memory allocator | jemalloc reduces fragmentation in long-running server processes with many small allocations -- exactly the Redis workload pattern. Redis itself uses jemalloc. 10-30% throughput improvement over system allocator in allocation-heavy workloads. Note: jemalloc upstream development has slowed, but for this workload profile it remains the best choice. Not available on MSVC/Windows. | HIGH |

### Error Handling

| Library | Version | Purpose | Why | Confidence |
|---------|---------|---------|-----|------------|
| thiserror | 2.0 | Structured error types for library/core code | Derive macro for clean error enums. Use for RESP parse errors, command errors, persistence errors. Callers need to match on error variants (e.g., wrong type, syntax error, out of memory). | HIGH |
| anyhow | 1.0 | Application-level error propagation | Use only in the server binary entry point and CLI. NOT in core library code. Provides context chaining for startup/config errors. | HIGH |

### Observability

| Library | Version | Purpose | Why | Confidence |
|---------|---------|---------|-----|------------|
| tracing | 0.1.41 | Structured logging and diagnostics | The standard for async Rust applications. Structured spans work naturally with tokio's task model. Supports log levels, structured fields, and subscriber-based output. | HIGH |
| tracing-subscriber | 0.3 | Log output formatting | Provides `fmt` subscriber for human-readable logs and JSON output. Filter by module/level at runtime. | HIGH |

### Persistence

| Library | Version | Purpose | Why | Confidence |
|---------|---------|---------|-----|------------|
| crc | 3.2 | CRC-64 checksums for RDB files | Redis RDB format uses CRC-64 checksums. The `crc` crate supports CRC-64/XZ (which Redis uses). Lightweight, no-frills. | MEDIUM |
| lz4_flex | 0.11 | LZ4 compression for RDB | Pure Rust LZ4 implementation. Redis 5+ supports LZ4 compression in RDB files. Safe Rust, no C bindings needed. | MEDIUM |

### CLI and Configuration

| Library | Version | Purpose | Why | Confidence |
|---------|---------|---------|-----|------------|
| clap | 4.6 | Command-line argument parsing | Derive-based CLI definition. Handles port, bind address, config file path, persistence options, log level. The standard choice -- 725M+ downloads. | HIGH |
| toml | 0.8 | Configuration file parsing | Redis uses its own config format, but for a Rust project, TOML is idiomatic and Cargo-native. Simpler than reimplementing Redis's config parser. | MEDIUM |

### Testing and Benchmarking

| Library | Version | Purpose | Why | Confidence |
|---------|---------|---------|-----|------------|
| criterion | 0.8 | Statistical micro-benchmarking | Measures performance of RESP parsing, data structure operations, command execution. Detects regressions with statistical confidence. | HIGH |
| tokio (test utils) | 1.49 | Async test runtime | `#[tokio::test]` macro for integration tests. Test TCP server with real client connections. | HIGH |
| redis (client crate) | 0.27 | Integration testing | Use the official Redis Rust client to test protocol compatibility. Confirms that real Redis clients can talk to our server. Dev-dependency only. | MEDIUM |

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Async Runtime | tokio | async-std, smol | tokio has overwhelmingly larger ecosystem, LTS releases, better tooling (tokio-console). async-std development has stagnated. smol is lighter but lacks the codec/framing ecosystem. |
| RESP Parser | Hand-written | redis-protocol, kresp | The project explicitly requires hand-written protocol handling for learning and control. Existing RESP crates are also mostly thin wrappers -- the protocol is simple enough that a dependency adds more weight than value. |
| Memory Allocator | jemalloc | mimalloc, system | mimalloc suffers with larger allocations (which sorted sets and large strings will produce). jemalloc matches Redis's own allocator choice and excels at the mixed small/large allocation pattern of a key-value store. |
| Hash Map | std HashMap + hashbrown raw API | dashmap | DashMap provides concurrent access, but Redis's architecture is single-threaded for data operations. Concurrent maps add overhead (sharding, locks) with zero benefit here. Use single-threaded HashMap. |
| String Type | Bytes + compact_str | String, SmolStr | `Bytes` is reference-counted and zero-copy from network buffers. `compact_str` for internal strings under 24 bytes. Standard `String` always heap-allocates and copies. SmolStr is immutable-only which is too restrictive. |
| Error Handling | thiserror | snafu, error-chain | thiserror is simpler, more widely adopted, and sufficient. snafu is more powerful but heavier for this use case. error-chain is deprecated. |
| Config Format | TOML | Redis-native config | TOML is idiomatic Rust, well-tooled, and avoids reimplementing a custom parser. Redis config compatibility is not a project goal. |
| Logging | tracing | log, slog | tracing is the modern standard for async Rust, provides spans (not just events), and integrates with tokio ecosystem. `log` is too simple. `slog` is powerful but less ecosystem support in 2025. |
| Skip List | Hand-written | skiplist crate | The `skiplist` crate (0.5) works but doesn't match Redis's specific skip list variant (which stores span information for O(log n) rank operations). Hand-writing allows matching Redis's exact semantics. |

## What NOT to Use

| Technology | Why Not |
|------------|---------|
| `dashmap` | Concurrent HashMap adds overhead from sharding/locking. Redis model is single-threaded data path -- no concurrent access to data structures. |
| `serde` / `serde_json` | Not needed. RESP protocol is simple binary-safe format, not JSON. Hand-written parser is both faster and simpler for RESP. Serde's derive overhead is wasted here. |
| `async-std` | Ecosystem has stagnated. Tokio won the async runtime war. |
| `crossbeam` | Channel-based concurrency is unnecessary for single-threaded data path. Tokio's `mpsc` channels suffice for the I/O thread to data thread communication pattern. |
| `parking_lot` | Faster mutexes are irrelevant -- the data path should be lock-free (single-threaded). Only networking uses async, not mutexes. |
| `redis-protocol` crate | Hand-written RESP parser gives zero-copy control and matches project goals. The crate adds a dependency for something that is ~200 lines of code. |
| `rocksdb` / `sled` | These are on-disk databases. This project is an in-memory store with optional persistence. Completely different architecture. |

## Dependency Philosophy

The project constraint states: "Minimal external crates -- tokio for async I/O, but core data structures and protocol handling are hand-written."

This means the dependency tree should be:
- **External crates:** Runtime (tokio), buffer management (bytes), allocator (jemalloc), error handling (thiserror), logging (tracing), CLI (clap), benchmarking (criterion)
- **Hand-written:** RESP parser/serializer, all Redis data structures (strings, lists, hashes, sets, sorted sets), expiration engine, RDB/AOF persistence format, command dispatch, pub/sub

Total production dependencies should stay under 15 direct crates. The Cargo.toml transitive dependency count will be higher (tokio alone brings ~30), but that is acceptable for a battle-tested runtime.

## Installation

```bash
# Create project
cargo init rust-redis

# Core dependencies
cargo add tokio --features full
cargo add tokio-util --features codec
cargo add bytes
cargo add hashbrown
cargo add ahash
cargo add compact_str
cargo add smallvec
cargo add slab
cargo add tikv-jemallocator
cargo add thiserror
cargo add anyhow
cargo add tracing
cargo add tracing-subscriber --features fmt,env-filter
cargo add crc
cargo add clap --features derive
cargo add toml

# Dev dependencies
cargo add --dev criterion --features html_reports
cargo add --dev redis  # for integration tests
cargo add --dev tokio-test
```

## Cargo.toml Features Configuration

```toml
[dependencies]
tokio = { version = "1.49", features = ["rt", "net", "io-util", "macros", "signal", "time", "fs"] }
tokio-util = { version = "0.7", features = ["codec"] }
bytes = "1.10"
hashbrown = "0.16"
ahash = "0.8"
compact_str = "0.8"
smallvec = "1.15"
slab = "0.4"
thiserror = "2.0"
anyhow = "1.0"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["fmt", "env-filter"] }
crc = "3.2"
clap = { version = "4.6", features = ["derive"] }
toml = "0.8"

[target.'cfg(not(target_env = "msvc"))'.dependencies]
tikv-jemallocator = "0.6"

[dev-dependencies]
criterion = { version = "0.8", features = ["html_reports"] }
redis = { version = "0.27", features = ["tokio-comp"] }
tokio = { version = "1.49", features = ["test-util"] }

[[bench]]
name = "resp_parsing"
harness = false

[[bench]]
name = "data_structures"
harness = false
```

## Rust Toolchain

| Tool | Purpose |
|------|---------|
| `rustup` stable (latest) | Compile with stable Rust. No nightly features needed. |
| `cargo clippy` | Lint for idiomatic Rust, catch common mistakes |
| `cargo fmt` | Consistent formatting |
| `cargo bench` | Run criterion benchmarks |
| `cargo test` | Unit + integration tests |
| `cargo-watch` | Auto-rebuild on file changes during development |
| `tokio-console` | Runtime diagnostics for async task inspection |

## Sources

- [tokio crate - crates.io](https://crates.io/crates/tokio) - Version 1.49.0 confirmed
- [bytes crate - lib.rs](https://lib.rs/crates/bytes) - Version 1.10.1 confirmed
- [hashbrown - GitHub](https://github.com/rust-lang/hashbrown) - SwissTable, version 0.16.1
- [criterion - crates.io](https://crates.io/crates/criterion) - Version 0.8.1 confirmed
- [tracing - crates.io](https://crates.io/crates/tracing) - Version 0.1.41 confirmed
- [thiserror - crates.io](https://crates.io/crates/thiserror) - Version 2.0.17 confirmed
- [tikv-jemallocator - crates.io](https://crates.io/crates/tikv-jemallocator) - Version 0.6
- [clap - crates.io](https://crates.io/crates/clap) - Version 4.6.0 confirmed
- [compact_str - GitHub](https://github.com/ParkMyCar/compact_str) - 24-byte inline SSO
- [smallvec - crates.io](https://crates.io/crates/smallvec) - Version 1.15.1
- [slab - GitHub](https://github.com/tokio-rs/slab) - Tokio project maintained
- [crc - crates.io](https://crates.io/crates/crc) - CRC-64 support confirmed
- [Redis Sorted Sets internals](https://jothipn.github.io/2023/04/07/redis-sorted-set.html) - Skip list + hash map architecture
- [jemalloc vs mimalloc comparison](https://medium.com/@syntaxSavage/the-power-of-jemalloc-and-mimalloc-in-rust-and-when-to-use-them-820deb8996fe) - Allocator tradeoffs
- [tokio framing tutorial](https://tokio.rs/tokio/tutorial/framing) - Codec pattern for protocol servers

# Getting Started with Moon

Moon is a high-performance Redis-compatible server written in Rust. This guide walks you through installing, running, and connecting to Moon.

## Prerequisites

- [Rust](https://rustup.rs/) stable toolchain (1.85+, edition 2024)
- cmake (required by aws-lc-rs for TLS support)
- Linux recommended (aarch64 primary, x86_64 secondary); macOS works for development

## Build from source

```bash
git clone https://github.com/pilotspace/moon.git
cd moon
cargo build --release
```

The default build uses the Monoio runtime (io_uring on Linux) with jemalloc. For Tokio runtime:

```bash
cargo build --release --no-default-features --features runtime-tokio,jemalloc
```

## Start the server

```bash
# Default: binds to 127.0.0.1:6379, auto-detects CPU count for shards
./target/release/moon

# Custom port and shard count
./target/release/moon --port 6399 --shards 4
```

## Connect with redis-cli

Moon speaks the Redis protocol (RESP2/RESP3), so any Redis client works out of the box:

```bash
redis-cli -p 6379
```

## Basic operations

```
127.0.0.1:6379> SET greeting "hello moon"
OK
127.0.0.1:6379> GET greeting
"hello moon"
127.0.0.1:6379> SET counter 0
OK
127.0.0.1:6379> INCR counter
(integer) 1
127.0.0.1:6379> INCR counter
(integer) 2
127.0.0.1:6379> HSET user:1 name "Alice" age "30"
(integer) 2
127.0.0.1:6379> HGETALL user:1
1) "name"
2) "Alice"
3) "age"
4) "30"
127.0.0.1:6379> LPUSH queue task1 task2 task3
(integer) 3
127.0.0.1:6379> RPOP queue
"task1"
```

## Enable persistence

Moon supports AOF (append-only file) persistence with per-shard WAL:

```bash
./target/release/moon --appendonly yes --dir /var/lib/moon
```

See the [configuration guide](configuration.md) for all available flags.

## Next steps

- [Configuration reference](configuration.md) -- all CLI flags and defaults
- [Monitoring with Prometheus](monitoring.md) -- set up metrics collection
- [Persistence guide](../persistence.mdx) -- AOF, RDB, and crash recovery
- [TLS setup](../tls.mdx) -- encrypted connections with mTLS

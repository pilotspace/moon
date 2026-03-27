# Rust Redis

## What This Is

A high-performance, in-memory key-value store written in Rust from scratch, inspired by Redis's architecture and design principles. Scoped to Redis's most essential and impactful features — the core data structures, persistence, and protocol that make Redis indispensable. Built with functional coding patterns, zero-copy where possible, and a focus on simplicity over feature completeness.

## Core Value

Simple, easy to use, high performance, and effectively memory-efficient — a Redis-compatible server that does fewer things but does them exceptionally well.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] RESP protocol parser and serializer (Redis Serialization Protocol)
- [ ] TCP server accepting concurrent client connections
- [ ] Core data types: Strings, Lists, Hashes, Sets, Sorted Sets
- [ ] Key expiration (TTL) with lazy + active expiration
- [ ] RDB persistence (snapshot to disk)
- [ ] AOF persistence (append-only file for durability)
- [ ] Pipelining support for batched commands
- [ ] Pub/Sub messaging
- [ ] LRU/LFU eviction policies for memory management
- [ ] CLI client compatibility (redis-cli should work)

### Out of Scope

- Cluster mode / sharding — complexity explosion, single-node focus for v1
- Lua scripting engine — significant complexity, not core to key-value operations
- Redis Modules API — extensibility deferred, core must be solid first
- Streams data type — complex, added in Redis 5.0, not essential for v1
- ACL / role-based access control — simplicity first, security hardening later
- Sentinel / high availability — single-node focus
- Redis Functions — newer feature, not core
- Client-side caching — optimization layer, not fundamental

## Context

- Redis is the de facto standard for in-memory data stores, but its C codebase has grown complex over 15+ years
- Rust provides memory safety without GC, making it ideal for a high-performance data store where every microsecond and byte matters
- The project follows functional coding patterns — pure functions, immutable-by-default, explicit state management, composition over inheritance
- Target: Redis protocol compatibility so existing tools (redis-cli, client libraries) work out of the box
- The architect (user) has 15+ years of software engineering experience and wants clean, idiomatic Rust with clear module boundaries

## Constraints

- **Language**: Rust (stable toolchain) — memory safety, zero-cost abstractions, no GC pauses
- **Protocol**: RESP2/RESP3 compatible — must work with redis-cli and standard Redis client libraries
- **Architecture**: Single-threaded event loop for data operations (like Redis) with I/O threads for networking — avoids lock contention on data structures
- **Coding style**: Functional patterns — pure functions, minimal mutation, composition, explicit error handling with Result types
- **Dependencies**: Minimal external crates — tokio for async I/O, but core data structures and protocol handling are hand-written
- **Memory**: Efficient memory layout — custom allocators where beneficial, arena allocation for small objects, zero-copy parsing

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Single-threaded data path | Eliminates lock contention, matches Redis's proven model | — Pending |
| Functional coding patterns | Testability, composability, explicit state transitions | — Pending |
| RESP2/RESP3 protocol | Ecosystem compatibility with existing clients and tools | — Pending |
| Hand-written core data structures | Control over memory layout and performance characteristics | — Pending |
| Tokio for async I/O | Battle-tested async runtime, but only for networking layer | — Pending |

---
*Last updated: 2026-03-23 after initialization*

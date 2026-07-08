---
title: "Redis Compatibility"
description: "Moon's Redis protocol and command compatibility matrix"
---

# Redis Compatibility

Moon implements a large subset of the Redis command surface with wire-level compatibility for RESP2 and RESP3. This document tracks known incompatibilities.

## Protocol Compatibility

| Protocol | Status |
|---|---|
| RESP2 | Full |
| RESP3 (HELLO 3) | Full |
| Inline commands | Full |
| Pipelining | Full |
| MULTI/EXEC | Full |
| Pub/Sub (RESP2 push) | Full |
| Pub/Sub (RESP3 push framing) | Partial — RESP2 framing used even under RESP3 |

## Client Compatibility Matrix

| Client | Language | Status | Notes |
|---|---|---|---|
| redis-py | Python | Tested in CI | Basic ops, pipelines, INFO parsing |
| go-redis | Go | Tested in CI | Basic ops, hash, pipelines |
| redis-rs | Rust | Used in integration tests | Full coverage |
| jedis | Java | Planned | |
| lettuce | Java | Planned | |
| ioredis | Node.js | Planned | |
| StackExchange.Redis | C# | Planned | |
| hiredis | C | Planned | |

## Known Incompatibilities

The command registry (`src/command/metadata.rs`) is the source of truth: 258
commands as of this writing, each with an arity, read/write flag, and ACL
category, checked by `cargo test command::metadata::tests`. The table below
is regenerated against that registry — previous editions of this doc
incorrectly listed `WAIT` and `FUNCTION *` as unimplemented; both are live.

### Commands

| Command | Status | Detail |
|---|---|---|
| `DEBUG DIGEST` | Not implemented | Use DBSIZE for parity checks |
| `DEBUG OBJECT` | Implemented | Redis-compatible one-line summary (encoding/refcount/serializedlength) |
| `ACL LOG` | Implemented | Real entries pushed from every command-dispatch path (single/sharded/monoio handlers) on auth/perm failures; `ACL LOG RESET` clears the ring |
| `CLIENT LIST` / `CLIENT INFO` | Implemented, some fields are placeholders | Full Redis field set is present (`id`, `addr`, `laddr`, `fd`, `name`, `age`, `idle`, `flags`, `db`, `sub`, `psub`, `ssub`, `multi`, `watch`, `qbuf*`, `argv-mem`, `multi-mem`, `tot-net-in/out`, `rbs`, `rbp`, `obl`, `oll`, `omem`, `tot-mem`, `events`, `cmd`, `user`, `redir`, `resp`, `lib-name`, `lib-ver`) so key=value parsers never choke on a missing key, but `laddr`, `tot-net-in/out`, `rbs`/`rbp`/`obl`/`oll`/`omem`, `cmd`, and `events` are not yet wired to live per-connection data (honest placeholder values, not real telemetry). `redir` and the tracking flag char track `CLIENT TRACKING` state landing separately. |
| `WAIT` | Implemented | Single-node: returns immediately (no replicas to wait for) |
| `OBJECT HELP` | Implemented | |
| `BITFIELD_RO` | Implemented | GET-only; rejects SET/INCRBY/OVERFLOW |
| `SORT_RO` | Implemented | Rejects STORE |
| `GEORADIUS_RO` | Implemented | Rejects STORE/STOREDIST (the base `GEORADIUS` doesn't implement STORE either — translates to GEOSEARCH internally) |
| `GEORADIUSBYMEMBER_RO` | Implemented | Rejects STORE/STOREDIST (same STORE caveat as `GEORADIUS_RO`) |
| `FUNCTION *` | Implemented | FCALL/FUNCTION LOAD/DELETE/LIST/DUMP/FLUSH/STATS via the Lua sandbox |

### Explicit Non-Goals

These commands are deliberately not implemented — not oversights:

| Command | Rationale |
|---|---|
| `PFDEBUG` | HyperLogLog internals-inspection command; no debugging surface to expose (Moon's HLL implementation isn't the dense/sparse Redis encoding this command introspects) |
| `PFSELFTEST` | Internal Redis HLL self-test with no external behavioral contract; nothing for a compatible server to reproduce |
| `FAILOVER` | Requires a primary/replica replication topology Moon doesn't have (single-node + cluster-mode sharding, no leader-initiated failover handshake) |
| `MODULE *` | Moon ships equivalent functionality (vector search, graph, JSON-like structures) as native compiled features rather than a dynamically loaded C module ABI |
| `SENTINEL *` | Moon's HA story is cluster mode + external orchestration (k8s/systemd), not Sentinel's gossip-based failover protocol |

### Behavior Differences

1. **RESP3 Pub/Sub push messages** — Moon uses RESP2 framing for pub/sub messages even when HELLO 3 is negotiated. Clients that strictly require RESP3 push framing for pub/sub may not work correctly.

2. **Cluster mode** — Available but not GA-hardened. Deferred to v0.2+.

3. **Persistence format** — Moon uses its own RDB format (magic `MOON`, not `REDIS`). Redis RDB files cannot be loaded directly; use RESP-based migration (e.g., `redis-cli --rdb` + replay).

4. **Memory reporting** — `INFO memory` sections may report different field names than Redis 7.x.

5. **CONFIG GET/SET** — Subset of Redis config parameters supported. Unrecognized parameters return empty rather than error.

## Vector Search (RediSearch Subset)

| Command | Status |
|---|---|
| `FT.CREATE` | Implemented (HNSW, TurboQuant) |
| `FT.DROPINDEX` | Implemented |
| `FT.INFO` | Implemented |
| `FT.SEARCH` | Implemented (KNN, hybrid filter) |
| `FT.COMPACT` | Implemented |
| `FT.AGGREGATE` | Not implemented |
| `FT.ALTER` | Not implemented |

---

*Last updated: 2026-07-08 — v0.6.0 WS1 command-parity audit (regenerated against `src/command/metadata.rs`; fixed stale WAIT/FUNCTION/ACL LOG/CLIENT LIST/OBJECT HELP claims, added BITFIELD_RO/SORT_RO/GEORADIUS_RO/GEORADIUSBYMEMBER_RO, documented explicit non-goals)*

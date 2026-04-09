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

### Commands

| Command | Status | Detail |
|---|---|---|
| `DEBUG DIGEST` | Not implemented | Use DBSIZE for parity checks |
| `DEBUG OBJECT` | Not implemented | |
| `ACL LOG` | Partial | Missing some subcommands |
| `CLIENT LIST` | Partial | Limited fields |
| `WAIT` | Not implemented | Single-node focus |
| `OBJECT HELP` | Not implemented | |
| `MODULE *` | Not implemented | Moon builds features natively |
| `SENTINEL *` | Not implemented | Cluster mode covers HA |
| `FUNCTION *` | Not implemented | Deferred to v0.2+ |

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

*Last updated: 2026-04-09 — Phase 96 of v0.1.3 Production Readiness*

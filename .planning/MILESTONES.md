# Milestones

## v0.1.0 Core Redis Server (Shipped: 2026-03-27)

**Phases completed:** 43 phases, 132 plans
**Timeline:** 4 days (2026-03-23 to 2026-03-27)
**Codebase:** ~54K lines Rust, 96 files, 254 commits
**Requirements:** 77/77 v1 requirements complete

**Key accomplishments:**
- Redis-compatible server with 200+ commands across all major data types
- Thread-per-core shared-nothing architecture with 1.84-1.99x Redis throughput
- Dual runtime (Tokio + Monoio) with io_uring support on Linux
- Forkless persistence with per-shard WAL (2.75x Redis with AOF)
- SIMD-accelerated parsing, DashTable with Swiss Table probing
- Memory-optimized: CompactKey SSO, HeapString, 27-35% less at 1KB+ values
- Full cluster mode with gossip protocol, failover, and slot migration
- PSYNC2 replication with partial resync
- Lua 5.4 scripting, ACL system, TLS 1.3, streams, blocking commands
- 132/132 data consistency tests pass across 1/4/12 shard configurations

**Archived:**
- [v0.1.0-ROADMAP.md](milestones/v0.1.0-ROADMAP.md)
- [v0.1.0-REQUIREMENTS.md](milestones/v0.1.0-REQUIREMENTS.md)
- [v0.1.0-phases/](milestones/v0.1.0-phases/) (43 phase directories)

---

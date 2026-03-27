# Phase 43: Security Hardening: TLS, ACL Bug Fixes, Protected Mode & Eviction Fixes - Context

**Gathered:** 2026-03-26
**Status:** Ready for planning

<domain>
## Phase Boundary

This phase delivers a production-ready security baseline: TLS 1.3 encrypted client connections via rustls, fixes for all ACL enforcement bugs across sharded handlers, Redis-compatible protected mode, and eviction enforcement in the sharded (production) code path. Pure infrastructure — no user-facing UI, no new data structures, no new Redis commands beyond CONFIG fields.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion

All implementation choices are at Claude's discretion — pure infrastructure phase. Key technical decisions validated during security audit:

- TLS integration via wrap-before-spawn pattern (TLS handshake in shard/mod.rs before handler spawn, transparent to handler internals)
- rustls + aws-lc-rs backend (FIPS 140-3 ready, 2.1x faster handshakes than OpenSSL)
- tokio-rustls for Tokio path, monoio-rustls for Monoio path (each runtime uses native TLS crate)
- Inline auth fix first (Phase 0 P0 security bug), refactor later
- Protected mode follows Redis 3.2+ behavior (reject non-loopback when no password set)
- Eviction wiring mirrors non-sharded handler pattern (call try_evict_if_needed before write dispatch)
- LRU clock fix: upgrade comparison to handle u16 wraparound via signed-distance comparison

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/acl/` — Full ACL subsystem (table.rs, rules.rs, io.rs, log.rs) — needs bug fixes, not rewrite
- `src/storage/eviction.rs` — All 8 eviction policies implemented, just not wired in sharded handlers
- `src/runtime/mod.rs` — TcpStream type aliases per runtime, TcpListener aliases
- `src/config.rs` — ServerConfig (clap-derived) + RuntimeConfig (mutable via CONFIG SET)
- `src/server/connection.rs` — 3 handler implementations (non-sharded, sharded Tokio, sharded Monoio)
- `src/shard/mod.rs` — Shard::run() event loop where connections are received and handlers spawned

### Established Patterns
- Feature-gated code: `#[cfg(feature = "runtime-tokio")]` / `#[cfg(feature = "runtime-monoio")]`
- Connection handoff: listener accepts TCP → sends via flume channel → shard receives and spawns handler
- Monoio cross-thread: `into_raw_fd()` → `std::net::TcpStream` (Send) → channel → `monoio::net::TcpStream::from_std()`
- Auth flow: `authenticated = requirepass.is_none()` → NOAUTH gate → AUTH command → ACL checks
- Config pattern: ServerConfig (static, clap) → RuntimeConfig (Arc<RwLock<>>, CONFIG SET)

### Integration Points
- **TLS wrapping:** shard/mod.rs lines 349-372 (Tokio) and 548-590 (Monoio) — after conn_rx.recv(), before handler spawn
- **requirepass bug:** shard/mod.rs line 359 (Tokio None), line 570 (Monoio — no requirepass param at all)
- **ACL channel enforcement:** connection.rs SUBSCRIBE/PSUBSCRIBE sections in all 3 handlers
- **Protected mode:** server/listener.rs accept loop — check peer addr before dispatching
- **Eviction wiring:** connection.rs sharded handlers — before write command dispatch sections
- **Config additions:** config.rs ServerConfig struct (new TLS fields, protected-mode flag)

### Critical Bugs Found (Validated)
1. **shard/mod.rs:359** — `None` hardcoded for requirepass in Tokio sharded handler → auth bypass
2. **shard/mod.rs:570** — Monoio handler call has no requirepass parameter → auth bypass
3. **connection.rs:3121** — Monoio handler: `_acl_table` unused, zero ACL enforcement
4. **connection.rs** — `check_channel_permission()` implemented but never called in any handler
5. **listener.rs** — `acl_table_from_config()` not used in non-sharded mode (aclfile ignored)
6. **eviction.rs** — `try_evict_if_needed` only called in non-sharded handler, never in sharded
7. **entry.rs** — LRU `last_access` stored as u16 (wraps every ~18.2 hours)

</code_context>

<specifics>
## Specific Ideas

No specific requirements — infrastructure phase. All decisions derive from the security blueprint validation and Redis compatibility requirements.

</specifics>

<deferred>
## Deferred Ideas

- kTLS kernel offload (requires unsafe FFI, platform-specific) — defer to future optimization phase
- FIPS 140-3 mode toggle (aws-lc-rs supports it, but needs testing infrastructure)
- mTLS client certificate → ACL role mapping (requires cert CN parsing, role system)
- Argon2id password hashing option (requires new dependency, Redis-incompatible)
- Hierarchical RBAC roles (Phase 44+ per security blueprint)
- Structured audit logging (Phase 44+ per security blueprint)
- Encryption at rest (Phase 44+ per security blueprint)

</deferred>

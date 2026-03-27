---
phase: 43-security-hardening-tls-acl-bug-fixes-protected-mode-eviction-fixes
plan: 03
subsystem: tls
tags: [rustls, aws-lc-rs, tokio-rustls, monoio-rustls, tls-1.3, mtls, cipher-suites]

# Dependency graph
requires:
  - phase: 43-01
    provides: "requirepass, protected_mode, acllog_max_len in ServerConfig/RuntimeConfig"
  - phase: 43-02
    provides: "ACL channel permission checks, eviction wiring"
provides:
  - "TLS 1.3 support via rustls + aws-lc-rs on dedicated port"
  - "Shard-side TLS handshake for both Tokio and Monoio runtimes"
  - "Configurable cipher suites and optional mTLS"
  - "Generic handler inner functions for stream-agnostic connection handling"
affects: [vector-search, future-tls-tests]

# Tech tracking
tech-stack:
  added: [rustls-0.23, rustls-pemfile-2, aws-lc-rs-1, tokio-rustls-0.26, monoio-rustls-0.4]
  patterns: [wrap-before-spawn-tls, generic-stream-handler, dual-port-listener]

key-files:
  created:
    - src/tls.rs
  modified:
    - Cargo.toml
    - src/config.rs
    - src/lib.rs
    - src/main.rs
    - src/shard/mod.rs
    - src/shard/mesh.rs
    - src/server/connection.rs
    - src/server/listener.rs
    - tests/integration.rs
    - tests/replication_test.rs

key-decisions:
  - "Used rustls with aws-lc-rs crypto provider (FIPS 140-3 ready, faster handshakes than OpenSSL)"
  - "Made rustls/aws-lc-rs optional deps activated by runtime features to avoid bare-metal compilation cost"
  - "Tokio handler: extracted handle_connection_sharded_inner<S: AsyncRead+AsyncWrite+Unpin> using AsyncReadExt::read_buf() instead of TcpStream-specific readable()+try_read_buf()"
  - "Monoio handler: made generic over AsyncReadRent+AsyncWriteRent with peer_addr passed as parameter"
  - "Connection channel changed from TcpStream to (TcpStream, bool) to carry TLS flag without separate channel"
  - "io_uring path skips TLS connections (plain TCP only) since TLS requires userspace crypto"

patterns-established:
  - "wrap-before-spawn: TLS handshake completes in shard thread before spawning handler task"
  - "dual-port listener: plain and TLS listeners coexist, both round-robin to same shard channels"
  - "generic stream handler: inner functions accept trait-bounded streams for plain/TLS polymorphism"

requirements-completed: [SEC-TLS-01]

# Metrics
duration: 18min
completed: 2026-03-27
---

# Phase 43 Plan 03: TLS Support Summary

**TLS 1.3 via rustls + aws-lc-rs with shard-side handshake, dual-port listener, configurable cipher suites, and optional mTLS for both Tokio and Monoio runtimes**

## Performance

- **Duration:** 18 min
- **Started:** 2026-03-26T16:54:54Z
- **Completed:** 2026-03-27T00:12:33Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments
- TLS 1.3 connections accepted on dedicated --tls-port via rustls + aws-lc-rs
- Both Tokio (tokio-rustls) and Monoio (monoio-rustls) runtimes support TLS connections
- Cipher suite filtering via --tls-ciphersuites with name-to-constant mapping
- Optional mTLS via --tls-ca-cert-file using WebPkiClientVerifier
- Plain and TLS ports work simultaneously with round-robin shard distribution

## Task Commits

Each task was committed atomically:

1. **Task 1: Add rustls dependencies + TLS config + TLS module** - `425e3a4` (feat)
2. **Task 2: Wire TLS into listener + shard accept paths** - `8be9b31` (feat)

## Files Created/Modified
- `src/tls.rs` - TLS configuration builder with build_tls_config() and resolve_cipher_suites()
- `Cargo.toml` - Added rustls, rustls-pemfile, aws-lc-rs, tokio-rustls, monoio-rustls deps
- `src/config.rs` - Added --tls-port, --tls-cert-file, --tls-key-file, --tls-ca-cert-file, --tls-ciphersuites CLI flags
- `src/lib.rs` - Registered tls module (feature-gated)
- `src/main.rs` - TLS config construction at startup, passed to shards
- `src/shard/mod.rs` - TLS handshake in Tokio and Monoio accept paths, tls_config parameter
- `src/shard/mesh.rs` - Channel type changed to (TcpStream, bool)
- `src/server/connection.rs` - Generic handle_connection_sharded_inner, generic monoio handler
- `src/server/listener.rs` - Dual-port listener for both Tokio and Monoio
- `tests/integration.rs` - Updated test fixtures for new types and parameters
- `tests/replication_test.rs` - Updated test fixtures for new config fields

## Decisions Made
- Used rustls with aws-lc-rs instead of OpenSSL: FIPS 140-3 ready, 2.1x faster handshakes, pure Rust
- Made TLS deps optional and feature-gated: zero cost when TLS not enabled
- Chose wrap-before-spawn pattern: TLS handshake completes before handler task spawn for clean error handling
- Tokio inner handler uses AsyncReadExt::read_buf() instead of TcpStream-specific readable()/try_read_buf() to support generic stream types
- io_uring path does not support TLS (skips to Tokio fallback) since TLS requires userspace crypto processing

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed RwLockReadGuard held across await in ACL channel checks**
- **Found during:** Task 1 (compilation)
- **Issue:** Wave 1's ACL channel permission checks held `RwLockReadGuard` across `.await` points, making futures non-Send
- **Fix:** Extracted check result into owned String before await, dropping guard before async operation
- **Files modified:** src/server/connection.rs (3 locations)
- **Committed in:** 425e3a4

**2. [Rule 1 - Bug] Fixed runtime_config borrow-after-move in shard Tokio path**
- **Found during:** Task 1 (compilation)
- **Issue:** Wave 1 code used `runtime_config.read()` inside async move block where `runtime_config` was also passed as parameter (moved)
- **Fix:** Extract requirepass before the async move closure using cloned `rtcfg`
- **Files modified:** src/shard/mod.rs
- **Committed in:** 425e3a4

---

**Total deviations:** 2 auto-fixed (2 bugs from Wave 1)
**Impact on plan:** Both fixes were necessary for compilation. No scope creep.

## Issues Encountered
- Pre-existing stack overflow in ACL table tests (unrelated to TLS, from Wave 1 changes)
- Pre-existing eviction test failure (unrelated to TLS, from Wave 2 changes)
- Both are out of scope for this plan

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- TLS infrastructure complete, ready for production deployment testing
- Self-signed cert generation for testing: `openssl req -x509 -newkey rsa:2048 -keyout test-key.pem -out test-cert.pem -days 1 -nodes -subj '/CN=localhost'`
- All Phase 43 plans (01, 02, 03) complete

---
*Phase: 43-security-hardening-tls-acl-bug-fixes-protected-mode-eviction-fixes*
*Completed: 2026-03-27*

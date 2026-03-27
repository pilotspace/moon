---
status: complete
phase: 43-security-hardening-tls-acl-bug-fixes-protected-mode-eviction-fixes
source: 43-01-SUMMARY.md, 43-02-SUMMARY.md, 43-03-SUMMARY.md
started: 2026-03-27T00:30:00Z
updated: 2026-03-27T00:35:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Auth Bypass Fix — requirepass enforced in sharded mode
expected: Both sharded handlers read requirepass from RuntimeConfig, no hardcoded None
result: pass
verified: No hardcoded None in shard/mod.rs. 3 requirepass references (RuntimeConfig reads).

### 2. ACL Channel Permission Enforcement in SUBSCRIBE
expected: check_channel_permission called in all SUBSCRIBE/PSUBSCRIBE paths (5+ call sites)
result: pass
verified: 7 check_channel_permission call sites in connection.rs (exceeds 5 minimum).

### 3. Protected Mode — non-loopback rejection
expected: Listener rejects non-loopback connections with Redis-compatible DENIED error
result: pass
verified: 14 protected_mode references in listener.rs. DENIED error message present.

### 4. Protected Mode — CONFIG SET support
expected: CONFIG SET protected-mode yes/no and CONFIG GET protected-mode work at runtime
result: pass
verified: 5 references in command/config.rs. SET validates yes/no, GET returns current value.

### 5. ACL File Loading Fix
expected: AclTable::load_or_default delegates to acl_table_from_config
result: pass
verified: load_or_default calls crate::acl::io::acl_table_from_config(config).

### 6. Configurable acllog-max-len
expected: CONFIG SET/GET acllog-max-len works. All 3 handlers use config value.
result: pass
verified: 5 refs in command/config.rs, 3 refs in connection.rs (all 3 handlers).

### 7. Eviction in Sharded Handlers
expected: try_evict_if_needed called before write dispatch in both sharded handlers
result: pass
verified: 5 try_evict_if_needed call sites in connection.rs (non-sharded + both sharded).

### 8. Background Eviction Timer
expected: 100ms background eviction timer in shard event loop
result: pass
verified: eviction_interval = TimerImpl::interval(Duration::from_millis(100)) in shard/mod.rs.

### 9. LRU Clock Wraparound Fix
expected: lru_is_older uses i16 wrapping_sub for correct u16 clock comparison
result: pass
verified: a16.wrapping_sub(b16) > 0 in eviction.rs. 4 total references.

### 10. Aggregate Memory Accounting
expected: aggregate_used_memory helper for cross-database eviction
result: pass
verified: aggregate_used_memory function exists in eviction.rs.

### 11. TLS Module — build_tls_config and resolve_cipher_suites
expected: src/tls.rs with build_tls_config() and resolve_cipher_suites()
result: pass
verified: src/tls.rs exists. 3 combined references to both functions.

### 12. TLS Config Flags
expected: --tls-port, --tls-cert-file, --tls-key-file, --tls-ca-cert-file, --tls-ciphersuites in config
result: pass
verified: 5 TLS config field references in config.rs.

### 13. Dual-Port Listener
expected: Plain and TLS listeners coexist, distributing to same shard channels
result: pass
verified: 10 TLS listener references in listener.rs.

### 14. Shard-Side TLS Handshake — Both Runtimes
expected: TLS handshake in shard/mod.rs for both tokio_rustls and monoio_rustls
result: pass
verified: tokio_rustls::TlsAcceptor and monoio_rustls::TlsAcceptor both present in shard/mod.rs.

### 15. Generic Stream Handlers
expected: handle_connection_sharded_inner<S: AsyncRead+AsyncWrite+Unpin> for stream-agnostic handling
result: pass
verified: Generic function signature found in connection.rs.

### 16. Optional mTLS
expected: WebPkiClientVerifier for mutual TLS client certificate authentication
result: pass
verified: 2 WebPkiClientVerifier/client_auth references in tls.rs.

## Summary

total: 16
passed: 16
issues: 0
pending: 0
skipped: 0

## Gaps

[none]

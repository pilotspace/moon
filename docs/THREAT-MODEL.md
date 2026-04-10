---
title: "Threat Model"
description: "Moon's threat model — attacker classes, assets, trust boundaries"
---

# Moon Threat Model

**Version:** v0.1.3 Production Readiness
**Last updated:** 2026-04-09

## Assets

| Asset | Value | Protection |
|---|---|---|
| **User data** (keys, values, streams) | Primary — data loss or corruption is P0 | Persistence (WAL, AOF, RDB), access control (ACL) |
| **Credentials** (ACL passwords, TLS keys) | High — compromise grants full access | ACL hashed passwords (SHA-256), TLS key file permissions |
| **Server availability** | High — outage impacts all clients | Graceful shutdown, crash recovery, replication |
| **Memory safety** | Critical — memory corruption → RCE potential | Rust ownership model, unsafe audit, fuzzing |

## Attacker Classes

### 1. Network Attacker (untrusted network)

**Capabilities:** Send arbitrary bytes to Moon's RESP port. Observe/modify traffic (if no TLS).

**Threats:**
- Malformed RESP frames → parser crash (DoS) or memory corruption (RCE)
- Connection flood → FD exhaustion (DoS)
- Traffic sniffing → credential/data theft

**Mitigations:**
- Two-pass RESP parser with bounds checking + cargo-fuzz
- Connection limits (SO_REUSEPORT per-shard)
- TLS 1.3 with rustls (no OpenSSL, no C dependencies)
- Protected mode (rejects non-loopback when no password set)

### 2. Authenticated Client (valid credentials, limited ACL)

**Capabilities:** Execute commands within their ACL permissions. Send any RESP frame.

**Threats:**
- ACL bypass via key pattern escape
- Resource exhaustion via large allocations (huge bulk strings, deeply nested arrays)
- Timing side-channels on password comparison

**Mitigations:**
- ACL key patterns with glob matching (fuzzed)
- ParseConfig limits: max_bulk_string_size, max_array_depth, max_array_length
- Constant-time password comparison (SHA-256 hash comparison)

### 3. Malicious Lua Script (via EVAL)

**Capabilities:** Execute arbitrary Lua code within the sandbox.

**Threats:**
- Sandbox escape → filesystem/network/OS access
- CPU exhaustion (infinite loop)
- Memory exhaustion (large table allocation)

**Mitigations:**
- Lua sandbox: no `io`, `os`, `debug`, `package`, `loadfile`, `dofile`
- Script timeout (configurable)
- Memory limits via Lua allocator hooks
- All bindings audited (Phase 98 SEC-04)

### 4. Replica Impersonator (network attacker posing as replica)

**Capabilities:** Initiate PSYNC handshake, receive full dataset.

**Threats:**
- Data exfiltration via unauthorized replication
- Corrupted replication stream injection

**Mitigations:**
- Replication requires AUTH if password is set
- TLS for replication traffic (when TLS enabled)
- PSYNC2 replication ID verification

### 5. Local User (access to host filesystem)

**Capabilities:** Read/write persistence files, config, process signals.

**Threats:**
- Data theft via RDB/AOF file read
- Data corruption via file modification
- Process manipulation via signals

**Mitigations:**
- File permissions (0600 for persistence files)
- CRC32C checksums on WAL records, CRC32 on RDB
- Signal handling (SIGTERM → graceful shutdown, SIGHUP → config reload)

## Trust Boundaries

```
┌─────────────────────────────────────────────────────┐
│                    UNTRUSTED                        │
│  Network attackers, port scanners, botnets          │
└─────────────┬───────────────────────────────────────┘
              │ TLS + Protected Mode
┌─────────────▼───────────────────────────────────────┐
│                 SEMI-TRUSTED                         │
│  Authenticated clients (ACL-limited)                 │
│  Lua scripts (sandboxed)                            │
└─────────────┬───────────────────────────────────────┘
              │ ACL + Sandbox + Resource Limits
┌─────────────▼───────────────────────────────────────┐
│                    TRUSTED                           │
│  Admin users (full ACL), replication peers           │
│  Operator (filesystem, signals, config)              │
└─────────────────────────────────────────────────────┘
```

## Risk Matrix

| Threat | Likelihood | Impact | Risk | Mitigation Status |
|---|---|---|---|---|
| RESP parser crash | Medium | Critical (DoS) | **High** | Fuzzing active (Phase 89) |
| ACL key pattern bypass | Low | High (data leak) | Medium | Fuzz target (SEC-08) |
| Lua sandbox escape | Low | Critical (RCE) | **High** | Audit pending (SEC-04) |
| TLS downgrade | Low | High (data leak) | Medium | TLS 1.3 floor enforced |
| Replica impersonation | Low | High (data theft) | Medium | AUTH + TLS available |
| Memory corruption via unsafe | Very Low | Critical (RCE) | Medium | 156 blocks audited + fuzzed |
| Supply chain (dep compromise) | Low | Critical | Medium | cargo audit + deny in CI |

---

*This threat model is a living document. Update when new features, attack surfaces, or mitigations are added.*

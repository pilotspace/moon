# Security Implementation Plan — Enterprise Production Grade

**Date:** 2026-03-26
**Author:** Security architecture review against rust-redis codebase (commit a6aa1b2)
**Status:** Validated against source code — all claims verified

---

## Critical Security Bugs (P0)

### Bug 1 — Sharded Tokio handler bypasses `requirepass`

**Severity:** CRITICAL
**File:** `src/shard/mod.rs:359`
**Root cause:** Hardcoded `None` passed as `requirepass` parameter.

```rust
// src/shard/mod.rs:359
None, // requirepass: TODO wire from shard config
```

**Impact chain:**
1. `handle_connection_sharded()` receives `requirepass: None`
2. `src/server/connection.rs:1370` evaluates `authenticated = requirepass.is_none()` → `true`
3. All sharded Tokio connections skip the NOAUTH gate at line 1440
4. Every client is pre-authenticated regardless of `--requirepass` config

**Scope:** All deployments using sharded mode with `runtime-tokio` feature and `--requirepass`.

**Note:** ACL command/key permission checks at lines 2034-2057 still fire (acl_table IS wired correctly), but the pre-auth gate that blocks unauthenticated access is completely bypassed.

### Bug 2 — Monoio handler has zero security enforcement

**Severity:** CRITICAL
**File:** `src/server/connection.rs:3104-3323`
**Root cause:** All security parameters are underscore-prefixed (unused) and no auth/ACL logic exists.

```rust
// src/server/connection.rs:3121
_acl_table: Arc<RwLock<crate::acl::AclTable>>,  // unused
```

**Evidence:**
- No `authenticated` variable declared anywhere in the handler (lines 3124-3323)
- No NOAUTH gate
- No `check_command_permission()` call
- No `check_key_permission()` call
- No `auth_acl()` call
- The handler processes ALL commands from ANY client with zero restrictions

**Scope:** Since `Cargo.toml:56` sets `default = ["runtime-monoio"]`, this is the **default production runtime** on Linux and macOS. Every connection has full unrestricted access.

---

## Current Security State Audit

### What Exists (Validated)

| Feature | Location | Status |
|---------|----------|--------|
| `requirepass` config | `src/config.rs:21` | Works in non-sharded Tokio handler only |
| ACL system | `src/acl/` (table.rs, rules.rs, io.rs, log.rs) | Full Redis-compatible ACL: users, passwords (SHA-256), command categories, key patterns (R/W), channel patterns |
| AUTH command | `src/command/connection.rs` auth_acl() | Legacy + ACL-aware auth, HELLO inline auth |
| ACL commands | `src/command/acl.rs` | LIST, WHOAMI, GETUSER, SETUSER, DELUSER, CAT, LOG, SAVE, LOAD |
| NOAUTH gate | `connection.rs:357-416` (non-sharded), `connection.rs:1440-1497` (sharded Tokio) | Blocks unauthenticated commands except AUTH/HELLO/QUIT |
| ACL permission gate | `connection.rs:889-920` (non-sharded), `connection.rs:2034-2067` (sharded Tokio) | command + key permission checks with AclLog on denial |
| ACL file persistence | `src/acl/io.rs` | Atomic save (tmp+rename), load, config bootstrap |
| In-memory ACL log | `src/acl/log.rs` | Ring buffer (128 entries), failed auth + NOPERM entries |

### What Is Missing (Validated)

| Feature | Evidence | Impact |
|---------|----------|--------|
| Protected mode | No `protected_mode` in config.rs or any handler | Accidental internet exposure risk |
| Brute-force protection | No failed-auth counters, no backoff, no AUTH rate limit | Credential brute-force possible |
| Connection limits (maxclients) | No `max_connections` in ServerConfig | Resource exhaustion / DoS |
| Client idle timeout | No idle timeout in handlers | Connection leak / resource waste |
| Auth timeout | No deadline forcing AUTH before disconnect | Slowloris-style attack vector |
| Strong password hashing | Only `sha2` in Cargo.toml; no argon2/bcrypt/scrypt | Offline brute-force against stolen hashes |
| TLS encryption | No rustls/tokio-rustls/openssl in Cargo.toml or src/ | All data + credentials transmitted in cleartext |
| Audit logging | Only in-memory AclLog ring buffer; no persistent/structured logging | Zero compliance (PCI-DSS, HIPAA, SOC 2) |
| Hierarchical RBAC | Flat AclUser with no roles/inheritance/groups | Doesn't scale past ~20 users |
| Rate limiting | No per-client or per-tenant throttling | Resource exhaustion / noisy neighbor |
| Encryption at rest | RDB/AOF written in plaintext | Data exposure on disk compromise |
| serde dependency | Not in Cargo.toml | Needed for structured audit JSON |

### Handler Security Matrix

| Handler | File:Line | I/O Model | AUTH Gate | ACL Checks | requirepass |
|---------|-----------|-----------|-----------|------------|-------------|
| Non-sharded (Tokio) | connection.rs:98 | `Framed<TcpStream, RespCodec>` | Yes | Yes | Wired correctly |
| Sharded (Tokio) | connection.rs:1333 | Direct `BytesMut` + `try_read_buf` | Yes (code exists) | Yes | **BUG: hardcoded None** |
| Sharded (Monoio) | connection.rs:3104 | Ownership `AsyncReadRent` + `Vec<u8>` | **NONE** | **NONE** | Not wired |

---

## Implementation Phases

### Phase 0: Fix Critical Auth Bypasses [P0 — Week 1]

| Task | Description | Files | LOC |
|------|-------------|-------|-----|
| Wire requirepass to sharded Tokio | Pass `config.requirepass.clone()` instead of `None` at shard/mod.rs:359 | shard/mod.rs | ~5 |
| Port AUTH/ACL gates to Monoio handler | Add `authenticated` bool, NOAUTH gate, `check_command_permission()`, `check_key_permission()` mirroring the Tokio sharded handler | connection.rs (monoio section) | ~200 |
| Integration tests | Verify NOAUTH enforcement in both runtimes, sharded and non-sharded | tests/ | ~150 |

**Decision:** Inline auth logic into Monoio handler first, refactor into shared struct in Phase 1.
**Rationale:** Security holes must close immediately. Architectural elegance must not block a P0 fix.

### Phase 1: Auth Hardening & Connection Security [Must-Have — Weeks 2-3]

#### 1.1 ConnectionSecurity Middleware Refactor

Extract duplicated auth/ACL logic from three handlers into a shared struct:

```rust
pub struct ConnectionSecurity {
    authenticated: bool,
    current_user: String,
    acl_log: AclLog,
    peer_addr: SocketAddr,
}

impl ConnectionSecurity {
    pub fn check_pre_auth(&self, cmd: &[u8]) -> Result<(), Frame>;
    pub fn check_acl(&self, cmd: &[u8], keys: &[&[u8]], acl: &AclTable) -> Result<(), Frame>;
    pub fn handle_auth(&mut self, args: &[Frame], acl: &AclTable, requirepass: &Option<String>) -> Frame;
}
```

This is feasible because auth/ACL logic operates on parsed `Frame`s, not raw I/O. The struct sits between frame parsing and command dispatch — same position in all three handlers.

| Advantage | Disadvantage |
|-----------|-------------|
| Single source of truth — impossible handler-specific auth gaps | Adds indirection on hot path (~1 function call) |
| Testable in isolation without network | Initial refactor touches all 3 handlers (~1500 lines) |
| New security features (rate limit, audit) added once | Must work across Tokio and Monoio ownership models |

#### 1.2 Protected Mode

When no password and no ACL file configured, reject non-loopback connections:
```
-DENIED Redis is running in protected mode because protected mode is enabled and
no password is set for the default user. Use requirepass or ACL to set a password.
```

| Advantage | Disadvantage |
|-----------|-------------|
| Prevents accidental internet exposure | Trivially disabled — defense in depth only |
| Redis-compatible behavior | Additional code path to test for both runtimes |

#### 1.3 Brute-Force Protection

| Decision | Details |
|----------|---------|
| Password hashing | Keep SHA-256 default (Redis compat), add Argon2id opt-in via `auth-hash-algorithm` config |
| Rate limiting | Per-IP failed auth counter with exponential backoff (1s, 2s, 4s... cap 30s) |
| AUTH throttle | Max 20 attempts/minute per IP |

| Option | Advantage | Disadvantage |
|--------|-----------|-------------|
| SHA-256 only (Redis parity) | Fast auth, compatible tooling | Weak against offline brute-force |
| Argon2id only | OWASP-recommended, GPU-resistant | Breaks Redis AUTH compat, ~50ms per auth |
| **SHA-256 default + Argon2id opt-in** | Compatible by default, secure when configured | Two code paths, config complexity |

#### 1.4 Connection Limits & Timeouts

New `ServerConfig` fields:

```rust
pub max_connections: usize,         // default 10_000
pub max_connections_per_ip: usize,  // default 100
pub tcp_keepalive: u32,             // default 300s
pub timeout: u64,                   // idle timeout, default 0 (disabled)
pub auth_timeout: u64,              // default 10s — close if no AUTH within this
```

**Connection tracking decision:** Atomic counter in listener thread before dispatch to shards.

| Approach | Advantage | Disadvantage |
|----------|-----------|-------------|
| **Atomic in listener (chosen)** | Accurate, simple | Minimal contention on accept() |
| Per-shard counters + aggregation | Zero contention | Approximate, complex |

### Phase 2: TLS via rustls [Must-Have — Weeks 3-5]

#### Architecture Decision

| Option | Advantage | Disadvantage |
|--------|-----------|-------------|
| **rustls + aws-lc-rs (chosen)** | 2.1x faster handshakes than OpenSSL, 5.3x less memory, FIPS 140-3 ready, memory-safe | Requires aws-lc-sys build (cmake), larger compile time |
| rustls + ring | Simpler build, pure Rust | No FIPS, slower, ring maintenance concerns |
| OpenSSL via tokio-openssl | Widest compat | C dependency, CVE surface, 36% throughput loss |

#### New Dependencies

```toml
rustls = { version = "0.23", default-features = false, features = ["aws-lc-rs", "tls12", "logging"] }
tokio-rustls = { version = "0.26", optional = true }
monoio-rustls = { version = "0.4", optional = true }
rustls-pemfile = "2"
```

#### Config Additions

```rust
pub tls_port: Option<u16>,              // default None (disabled)
pub tls_cert_file: Option<PathBuf>,
pub tls_key_file: Option<PathBuf>,
pub tls_ca_cert_file: Option<PathBuf>,  // for mTLS client verification
pub tls_auth_clients: TlsAuthClients,   // no | optional | yes
pub tls_protocols: Vec<TlsProtocol>,    // [tls1.2, tls1.3]
pub tls_replication: bool,              // encrypt replication channel
pub tls_cluster: bool,                  // encrypt cluster bus
```

#### Integration Approach (Revised After Validation)

Original plan proposed enum dispatch to abstract TLS across runtimes. **Validation revealed this is infeasible:**

- Tokio uses `AsyncRead`/`AsyncWrite` traits
- Monoio uses `AsyncReadRent`/`AsyncWriteRent` (ownership-based, fundamentally different)
- No common trait can abstract both

**Revised approach — wrap-before-spawn (simpler, ~500 fewer LOC):**

1. **Tokio path:** `tokio_rustls::TlsAcceptor::accept(tcp_stream)` → `TlsStream` → pass to `handle_connection_sharded()`
2. **Monoio path:** `monoio_rustls::TlsAcceptor::accept(tcp_stream)` → `TlsStream` → pass to `handle_connection_sharded_monoio()`
3. TLS handshake happens in `shard/mod.rs` before spawning the handler task
4. Handlers receive a TLS stream that implements their native I/O traits — **zero handler changes needed**

| Advantage | Disadvantage |
|-----------|-------------|
| No stream abstraction needed | Handshake in shard thread blocks other connections briefly |
| Handlers unchanged — TLS transparent | Must handle handshake timeout to prevent slowloris |
| Each runtime uses native TLS crate | Two TLS crate dependencies |

#### Phase 2 Decisions

| Decision | Tradeoff |
|----------|----------|
| TLS optional (not mandatory by default) | Redis compat vs. security-by-default. Operators expect `--tls-port 6380` opt-in |
| Support TLS 1.2 | Some enterprises have legacy clients. ~200 LOC for cipher config |
| No kTLS in v1 | Requires kernel 4.13+, platform-specific setsockopt, unsafe FFI. Defer to Phase 6. 15-25% overhead acceptable for v1 |
| mTLS client cert → ACL role mapping | Map `CN=app-reader` → ACL user `app-reader`. Eliminates password management |
| 0-RTT only for read-only commands | Replay attack risk on writes |

### Phase 3: Structured Audit Logging [Must-Have — Weeks 5-7]

#### Why Before RBAC

Audit logging is required by PCI-DSS (12 months), HIPAA (6 years), SOC 2. Without it, no compliance team approves deployment. RBAC without audit is unverifiable — can't prove who did what.

#### Event Structure

```rust
#[derive(Serialize)]
pub struct AuditEvent {
    pub timestamp: u64,
    pub event_type: AuditEventType,
    pub actor: String,
    pub source_ip: IpAddr,
    pub action: String,
    pub resource: Option<String>,
    pub result: AuditResult,
    pub detail: Option<String>,
    pub hash: String,  // tamper-evident chain
}

pub enum AuditEventType {
    AuthSuccess, AuthFailure, CommandDenied, ConfigChange,
    ConnectionOpen, ConnectionClose, ReplicationEvent, AclChange,
}
```

#### Async Pipeline (Zero Data-Path Impact)

```
Command handler → lock-free SPSC ring buffer → audit worker thread → serde_json → output sink
```

Uses existing `ringbuf::HeapProd`/`HeapCons` pattern already in the codebase.

| Sink | Use Case | Tradeoff |
|------|----------|----------|
| File (JSONL) — default | Simple, local | Rotation/retention management needed |
| TCP (SIEM) — opt-in | Splunk/Datadog/ELK | Network failures must not block commands |

If ring buffer is full: drop oldest events, increment `audit_events_dropped` counter.

| Advantage | Disadvantage |
|-----------|-------------|
| Zero-latency impact on hot path (lock-free ring) | Events can drop under extreme load |
| Tamper-evident hash chains satisfy SOC 2 | Hash chain verification requires full log read |
| Structured JSON enables automated SIEM analysis | ~2-5% throughput cost from serialization on audit thread |

#### Config

```rust
pub audit_enabled: bool,                // default false
pub audit_events: Vec<AuditEventType>,  // which events to capture
pub audit_file: Option<PathBuf>,        // JSONL output
pub audit_siem_endpoint: Option<String>,// tcp://host:port
pub audit_key_patterns: Vec<String>,    // only audit matching keys (e.g., "pii:*")
```

### Phase 4: Hierarchical RBAC [Must-Have — Weeks 7-9]

#### Design

```rust
pub struct Role {
    pub name: String,
    pub permissions: Permissions,
    pub inherits: Vec<String>,  // parent roles (DAG, not tree)
}

pub struct Permissions {
    pub commands: CommandPermissions,  // reuse existing AllAllowed/Specific
    pub key_patterns: Vec<KeyPattern>,
    pub channels: Vec<ChannelPattern>,
    pub max_memory: Option<u64>,
    pub max_connections: Option<u32>,
}
```

#### New Commands (namespaced, Redis-incompatible)

```
ROLE CREATE cache-reader +@read ~cache:*
ROLE CREATE cache-writer INHERIT cache-reader +@write ~cache:*
ROLE CREATE cache-admin INHERIT cache-writer +@admin +@dangerous
ACL SETUSER app1 ROLE cache-reader
```

#### Resolution Order

User-specific rules override role rules. Multiple roles merge with most-permissive wins for commands, union for key patterns. Matches Kubernetes/AWS IAM semantics.

| Decision | Advantage | Disadvantage |
|----------|-----------|-------------|
| Extend existing ACL, don't replace | Backward-compatible with Redis ACL commands | Two permission models coexist |
| Role inheritance via DAG | Flexible composition | Cycle detection needed |
| Roles stored in extended ACL file format | Single config source | Redis tooling can't parse extended format |

#### Phase 4b: External Identity (OIDC/LDAP) — Deferred [Should-Have]

Deferred because it requires async HTTP client, LDAP library, token caching — significant complexity. The role system must work standalone first.

### Phase 5: Rate Limiting & Resource Protection [Must-Have — Weeks 9-11]

#### 5.1 Per-Tenant Token Bucket

```rust
pub struct TokenBucket {
    tokens: AtomicU64,
    last_refill: AtomicU64,
    rate: u64,        // tokens per second
    burst: u64,       // max burst
}
```

**Decision: Per-tenant (authenticated username), not per-connection.**

| Approach | Advantage | Disadvantage |
|----------|-----------|-------------|
| **Per-tenant (chosen)** | Fair multi-tenant isolation | Requires cross-shard aggregation |
| Per-connection | Simple | Unfair — more connections = more throughput |

#### 5.2 Command Complexity Gates

Classify by Big-O. Estimate cost before execution. Reject if exceeds configurable limit.

```rust
pub enum CommandComplexity {
    O1,                    // GET, SET — always allowed
    ON { key: Bytes },     // LRANGE, SMEMBERS — check collection size
    ONLogN { key: Bytes }, // SORT — require elevated privilege
    ONM,                   // SUNION with multiple large sets
}
```

Default: reject commands with estimated cost > 100,000 elements.

#### 5.3 Memory Bomb Protection

```rust
pub max_key_size: usize,           // default 512 bytes
pub max_value_size: usize,         // default 512 MB
pub max_collection_elements: u64,  // default 1_000_000
pub per_tenant_memory_limit: u64,  // default 0 (unlimited)
```

### Phase 6: Encryption at Rest [Should-Have — Weeks 11-14]

Envelope encryption: per-file DEK (AES-256-GCM) encrypted by KEK from external KMS.

```rust
#[async_trait]
pub trait KmsProvider: Send + Sync {
    async fn encrypt_dek(&self, plaintext_dek: &[u8]) -> Result<Vec<u8>>;
    async fn decrypt_dek(&self, encrypted_dek: &[u8]) -> Result<Vec<u8>>;
}
```

Implementations: `LocalFileKms` (dev), `VaultKms`, `AwsKms`.

RDB/AOF files already use atomic write (tmp+rename). Encrypt entire file with per-file DEK, store encrypted DEK in file header. Key rotation = new DEK on next save.

### Phase 7: WASM Extension Sandboxing [Nice-to-Have — Weeks 14-18]

Deferred. Lua scripting works and is Redis-compatible. WASM added as new extension mechanism (`WASM.CALL`, `WASM.LOAD`) alongside Lua, not replacing it.

---

## Priority Matrix

| Feature | Priority | Justification |
|---------|----------|---------------|
| Fix auth bypasses (Phase 0) | **P0 CRITICAL** | Active vulnerability in default production runtime |
| Protected mode + brute-force (Phase 1) | **Must-Have** | Prevents accidental exposure, OWASP baseline |
| Connection limits + timeouts (Phase 1) | **Must-Have** | DDoS resistance, resource management |
| ConnectionSecurity refactor (Phase 1) | **Must-Have** | Eliminates class of handler-specific auth bugs |
| TLS via rustls (Phase 2) | **Must-Have** | Zero enterprise deployments without TLS |
| mTLS client auth (Phase 2) | **Must-Have** | Certificate-based auth eliminates passwords |
| Audit logging (Phase 3) | **Must-Have** | PCI-DSS, HIPAA, SOC 2 all require it |
| Hierarchical RBAC (Phase 4) | **Must-Have** | Flat ACL doesn't scale past ~20 users |
| Rate limiting (Phase 5) | **Must-Have** | Multi-tenant isolation, DDoS protection |
| Command complexity gates (Phase 5) | **Must-Have** | Prevents `KEYS *` taking down production |
| OIDC/LDAP integration (Phase 4b) | **Should-Have** | Enterprise SSO; manual user management works initially |
| Encryption at rest (Phase 6) | **Should-Have** | Required for HIPAA/PCI with sensitive data on disk |
| FIPS 140-3 mode (Phase 2 flag) | **Should-Have** | Government/finance; nearly free with aws-lc-rs |
| kTLS offload | **Should-Have** | Reduces TLS overhead from ~20% to <10% |
| WASM extensions (Phase 7) | **Nice-to-Have** | Competitive differentiator, not blocking adoption |
| eBPF XDP filtering | **Nice-to-Have** | Wire-speed DDoS; iptables works for v1 |
| Hardware memory encryption (SEV-SNP) | **Nice-to-Have** | Cloud-specific, customers manage at infra layer |

---

## Competitive Position After Implementation

| Feature | Redis OSS | Dragonfly | Garnet | **rust-redis** |
|---------|-----------|-----------|--------|----------------|
| Memory safety | No (50+ CVEs) | No (C++) | Partial (.NET) | **Yes (Rust)** |
| Multi-threaded TLS | 8.0+ only | Yes | Yes | **Yes (rustls 2.1x faster)** |
| FIPS 140-3 | No | No | No | **Yes (aws-lc-rs)** |
| RBAC with roles | No | No | No | **Yes** |
| External IdP | No | No | Azure AD only | **Yes (pluggable)** |
| Encryption at rest | No | No | No | **Yes** |
| Structured audit log | No | Basic | No | **Yes (tamper-evident)** |
| Per-tenant rate limit | No | No | No | **Yes** |

---

## Estimated Effort

| Phase | New LOC | New Dependencies | Files Touched |
|-------|---------|------------------|---------------|
| 0 | ~350 | None | 2 (shard/mod.rs, connection.rs) + tests |
| 1 | ~1,500 | `argon2` (opt-in) | 5-8 (ConnectionSecurity, config, handlers) |
| 2 | ~2,500 | `rustls`, `tokio-rustls`, `monoio-rustls`, `rustls-pemfile` | 6-10 (shard/mod.rs, listener, config) |
| 3 | ~2,000 | `serde`, `serde_json` | 4-6 (new audit module, config, handlers) |
| 4 | ~1,500 | None | 3-5 (extend acl/, new role system) |
| 5 | ~1,200 | None | 4-6 (new rate_limit module, command dispatch) |
| 6 | ~2,500 | `aes-gcm` (or reuse aws-lc-rs) | 6-8 (encryption module, RDB/AOF) |
| **Total** | **~11,550** | **5-6 crates** | |

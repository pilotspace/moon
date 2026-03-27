# Security architecture blueprint for an enterprise-grade Redis replacement in Rust

**Rust can eliminate roughly 60% of all Redis CVEs by construction**, making it the strongest foundation for a secure Redis replacement. Redis has accumulated 50+ CVEs since 2015, with the majority caused by C memory safety bugs — integer overflows, heap corruption, use-after-free — that are impossible in safe Rust. The October 2025 discovery of CVE-2025-49844 (CVSS 10.0), a use-after-free hiding in the Lua parser for **13 years**, underscores the structural nature of this problem. Beyond memory safety, Redis lacks encryption at rest, audit logging, RBAC, FIPS compliance, and multi-tenant isolation — all table stakes for enterprise deployments. This blueprint defines a security architecture that addresses every one of these gaps while leveraging Rust's type system, modern TLS via rustls, and WebAssembly sandboxing to deliver a fundamentally more secure system.

---

## Redis's security model was designed for trusted networks and it shows

Redis's own documentation states plainly: "Redis is designed to be accessed by trusted clients inside trusted environments." This perimeter-based philosophy means security is bolt-on, not built-in — and the CVE record reflects it.

**Authentication evolved slowly.** Before Redis 6.0 (May 2020), the entire authentication model was a single shared password via `requirepass`. Redis 6.0 introduced ACLs with named users, command categories (`@read`, `@write`, `@admin`, `@dangerous`), key patterns (`~prefix:*`), and pub/sub channel restrictions. Redis 7.0 added selectors (multiple rule sets per user) and `ACL DRYRUN` for testing. But the model remains **flat** — no roles, no groups, no inheritance, no LDAP/OIDC integration. Each user must be individually configured, and passwords are stored as SHA-256 hashes rather than bcrypt/scrypt, a deliberate design choice that trades brute-force resistance for speed.

**TLS arrived late and costs dearly.** Native TLS was added in Redis 6.0 as a compile-time option atop OpenSSL. Before that, users relied on stunnel proxies. Redis supports TLS 1.2/1.3, mTLS, and cluster bus encryption (`tls-cluster yes`), but the single-threaded architecture makes TLS painful: benchmarks show a **36–38% throughput reduction** with TLS enabled. Redis 8.0 finally added I/O threading support for TLS, years after competitors like KeyDB demonstrated 7× TLS throughput gains via multi-threading.

**Protected mode is a band-aid.** Added in Redis 3.2 (2016), protected mode blocks remote connections when no password is set. It is trivially disabled (`CONFIG SET protected-mode no`) and does nothing against localhost attacks, SSRF, or any scenario where authentication exists but is weak. `RENAME-COMMAND`, which hides dangerous commands like `CONFIG` and `FLUSHALL`, is officially deprecated in favor of ACLs and provides no per-user granularity.

### The Lua scripting engine is the single largest attack surface

The two CVSS 10.0 vulnerabilities in Redis history both involve Lua. **CVE-2022-0543** exploited Debian's dynamic linking of Lua, exposing `package.loadlib()` inside the sandbox — a single function that enabled arbitrary code execution on every Debian/Ubuntu Redis installation. It was actively exploited by Muhstik, HeadCrab, P2PInfect, and Redigo botnets. **CVE-2025-49844** (dubbed "RediShell") exploited a use-after-free in `lparser.c` that existed for 13 years; triggering garbage collection during parsing left a dangling reference that enabled full sandbox escape and host compromise. The fundamental problem is architectural: Lua runs in the same address space as Redis with a denylist-based sandbox. Any missed function or exposed global is an escape vector, and this pattern has produced critical vulnerabilities repeatedly over a decade.

### CVE categorization reveals a clear pattern

Analysis of 50+ Redis CVEs shows distinct vulnerability classes:

- **Memory safety (buffer overflows, integer overflows, use-after-free):** ~25–30 CVEs, or **60% of total**. This is the dominant category and includes the most critical bugs. Integer overflow leading to heap corruption is the single most common pattern (CVE-2021-29477, CVE-2021-32627/28/87, CVE-2021-41099, CVE-2022-35951, CVE-2023-25155, CVE-2025-46817, CVE-2025-62507).
- **Lua sandbox escapes:** CVE-2022-0543, CVE-2025-49844, CVE-2022-24834 — the highest-severity bugs.
- **Denial of service:** CVE-2022-36021 (pattern matching CPU exhaustion), CVE-2024-31228 (unbounded recursion), CVE-2025-21605 (output buffer growth from unauthenticated clients).
- **ACL/auth bypasses:** CVE-2023-41053 (SORT_RO bypassed key ACLs), CVE-2023-45145 (Unix socket permission bypass), CVE-2025-46818 (metatable injection across users).

The 2025 crop alone includes a CVSS 10.0 UAF, a stack buffer overflow in XACKDEL (CVE-2025-62507), an integer overflow in Lua unpack (CVE-2025-46817), and a heap/stack OOB write in HyperLogLog (CVE-2025-32023). Redis's C codebase continues to produce critical memory safety vulnerabilities at a steady rate.

### Known attack vectors form a well-documented playbook

Roughly **330,000 Redis instances** are exposed to the internet as of late 2025, with approximately 60,000 lacking any authentication. Attackers exploit these through several proven techniques: writing SSH keys via `CONFIG SET dir /root/.ssh` + `SAVE`; injecting crontab reverse shells; writing webshells to web-accessible directories; SSRF via `SLAVEOF`/`REPLICAOF` using gopher:// protocols; and loading malicious modules via rogue master replication. Organized campaigns — HeadCrab (2,300+ servers compromised by 2024), P2PInfect (Rust-based P2P worm), Migo, Muhstik — actively hunt for exposed instances.

---

## Enterprise features Redis never built

An enterprise-grade replacement must close critical gaps that Redis leaves to commercial products or external tooling.

**RBAC with role hierarchy.** Redis's flat ACL model requires per-user configuration with no concept of roles, groups, or inheritance. The replacement should implement named roles (e.g., `cache-reader`, `session-writer`, `cluster-admin`) with composition/inheritance, LDAP/OIDC group mapping, cross-database scope, and temporal access grants. Redis Enterprise offers this commercially; Azure Cache supports Microsoft Entra ID for data-plane auth. The open-source world has nothing.

**Encryption at rest.** Redis stores and persists data in plaintext. AWS ElastiCache uses AES-256 via encrypted EBS volumes with KMS integration. Azure Cache encrypts persistence data with Azure Storage encryption (FIPS 140-2). Google Memorystore encrypts by default with CMEK support. The replacement should implement transparent encryption of RDB/AOF equivalents using envelope encryption: a data encryption key (DEK) per file encrypted by a key encryption key (KEK) managed by an external KMS (Vault, AWS KMS, Azure Key Vault). For in-memory data, leverage AMD SEV-SNP or Intel TDX for hardware memory encryption where available.

**Audit logging for compliance.** Redis has no audit trail. `MONITOR` captures all commands but with devastating performance impact and no structured output. The replacement needs structured JSON audit events covering authentication (success/failure with source IP), ACL violations, data access on classified key patterns, configuration changes, replication events, and administrative operations. Events should be forwarded asynchronously via TCP to external SIEM systems (Splunk, Datadog, ELK) to minimize data-path impact. Implement tamper-evident logs using hash chains where each entry includes `hash(timestamp || actor || action || resource || previous_hash)`, anchored to write-once storage (S3 Object Lock, Azure Immutable Blob).

**Compliance requirements are specific and demanding.** HIPAA mandates 6-year audit retention and encryption of ePHI at rest (addressable but must be documented if not implemented). PCI-DSS 4.0 requires rendering PANs unreadable wherever stored, 12-month log retention with 3 months immediately available, and individual user action tracking. SOC 2 requires RBAC, periodic access reviews, and real-time alerting on unauthorized access. FIPS 140-2/140-3 mandates validated cryptographic modules — Redis OSS has no FIPS mode whatsoever.

---

## Rust eliminates entire vulnerability classes by design

The ownership system — single ownership, borrowing rules (one mutable reference XOR any number of immutable references), lifetime tracking — eliminates at compile time the bugs that dominate Redis's CVE history. Microsoft Security Response Center found that **~70% of all CVEs they assign are memory safety issues** that Rust prevents. The Rust Foundation's 2024 analysis found that less than 1% of actual code on crates.io is `unsafe`.

### Specific Redis CVEs impossible in safe Rust

**CVE-2025-49844 (CVSS 10.0, UAF):** The Lua parser's `luaY_parser` adds a `Tstring` object that garbage collection frees while a reference persists. Rust's ownership model makes this structurally impossible — when a value is dropped, no references can exist, enforced by the borrow checker. **CVE-2021-29477 (integer overflow → heap corruption):** Rust panics on integer overflow in debug mode and offers `checked_add()`, `saturating_add()` for explicit handling; release builds can enable overflow checks via `overflow-checks = true`. **CVE-2019-10192/10193 (stack/heap buffer overflows in HyperLogLog):** Rust's bounds-checked array/slice access prevents writing past buffer ends at runtime. **CVE-2025-62507 (stack buffer overflow in XACKDEL):** Stack-allocated arrays in Rust have compile-time known sizes with runtime bounds checking.

### Thread safety as a language guarantee

Redis chose single-threaded execution to avoid concurrency bugs — sacrificing performance for safety. A Rust KV store can safely use multi-threaded, shared-nothing architectures because the `Send` and `Sync` auto-traits, combined with the borrow checker, prevent data races at compile time. For shared state, `Arc<RwLock<T>>` provides multiple-reader/single-writer semantics with poisoning on panic. For high-throughput paths, `DashMap` offers lock-free concurrent hash maps, or shard `RwLock<HashMap>` by key hash to minimize contention.

### Strategic unsafe containment

Genuine `unsafe` is needed in a high-performance KV store for io_uring bindings (`io-uring` crate), SIMD operations (`std::arch`), custom allocators (`tikv-jemallocator`), memory-mapped I/O, and FFI to crypto libraries. The architecture should enforce `#![forbid(unsafe_code)]` at the crate root for application logic, storage protocol, and networking layers, allowing `unsafe` only in dedicated low-level crates with documented `// SAFETY:` invariants. Audit with **cargo-geiger** (counts unsafe in dependency tree), **Miri** (detects undefined behavior at runtime), and **cargo-careful** (extra stdlib debug assertions). Enable the `unsafe_op_in_unsafe_fn` lint to require explicit `unsafe` blocks even inside `unsafe fn`.

### Cryptographic hygiene through the type system

- **`subtle` crate:** Constant-time comparison for authentication tokens using `ConstantTimeEq`, preventing timing attacks. Uses volatile reads as optimization barriers.
- **`zeroize` crate:** Zeroes secrets on drop via `core::ptr::write_volatile` + `compiler_fence(SeqCst)`, guaranteed not optimized away. `ZeroizeOnDrop` derive macro automates cleanup.
- **`secrecy` crate:** `SecretBox<T>` wrapper that prints `[REDACTED]` in Debug and prevents accidental serialization. Pure safe Rust (`#![forbid(unsafe_code)]`).
- **`memsecurity` crate:** Encrypts secrets in memory using Ascon128a with per-run keys stored in `mlock`'d pages — defense against cold boot and microarchitectural attacks.

### Supply chain and binary hardening

Run `cargo audit` (RustSec Advisory Database), `cargo deny` (license/advisory/ban policy), and `cargo vet` (Mozilla's supply chain audit tracking) in CI. Vendor dependencies for reproducible, air-gapped builds. Rust binaries get PIE/ASLR (since 2014), NX/DEP, Full RELRO, and heap corruption protection by default. Stack canaries (`-Z stack-protector=strong`) and LLVM CFI (`-Z sanitizer=cfi`) are available on nightly. Production config should use `panic = "abort"` (prevents observing broken invariants through `catch_unwind`), `overflow-checks = true`, `lto = true`, and `codegen-units = 1`.

---

## TLS architecture: rustls + aws-lc-rs for performance and FIPS

The TLS stack is a load-bearing security component. The right choice determines handshake latency, throughput overhead, FIPS compliance, and post-quantum readiness.

### Why rustls wins

Benchmarks on Xeon E-2386G (rustls 0.23.15, October 2024) show rustls dramatically outperforming alternatives:

| Metric | rustls | OpenSSL 3.3.2 | rustls advantage |
|--------|--------|----------------|-----------------|
| Full TLS 1.3 ECDSA handshakes/s | 8,326 | 3,938 | **2.1×** |
| Resumed TLS 1.2 handshakes/s | 71,150 | 22,268 | **3.2×** |
| TLS 1.3 AES-256-GCM send (MB/s) | 7,094 | 5,982 | **1.2×** |
| Memory per session (peak) | ~13 KiB | ~69 KiB | **5.3×** less |

At C10K scale, rustls uses **132 MiB** versus OpenSSL's **688 MiB**. On 80-core ARM (Ampere Altra), rustls maintains flat per-thread scaling while OpenSSL degrades severely from lock contention. The `aws-lc-rs` backend (now rustls's default since 0.23) provides **FIPS 140-3 Level 1 validation** — the first NIST-certified module to include ML-KEM in its FIPS boundary — plus AVX-512IFMA acceleration for 2× faster RSA and default post-quantum key exchange (X25519MLKEM768).

### TLS overhead budget and mitigation

Single-threaded Redis with TLS loses ~36% throughput. A well-designed multi-threaded Rust KV store can reduce this to **15–25%** through parallelized per-thread TLS processing (as Dragonfly and KeyDB demonstrate). The ultimate optimization is **kernel TLS (kTLS)**: rustls handles the handshake, then session keys are installed into the kernel via `setsockopt(SOL_TCP, TCP_ULP, "tls")`. The kernel then handles record-layer encryption, enabling zero-copy `sendfile()` through TLS for RDB transfers. With kTLS + hardware AES-NI, overhead drops below **10%**. For io_uring integration, the architecture is: `io_uring TCP → rustls handshake → kTLS record layer → io_uring sendfile`.

### mTLS and certificate management

Implement mTLS using rustls's native `ClientConfig::with_client_auth_cert()` and `ServerConfig::with_client_cert_verifier()`. Map client certificate CN/SAN fields to internal ACL roles (e.g., CN=`kv-admin` → admin role). Use short-lived certificates (24-hour TTL) issued by SPIFFE/SPIRE or HashiCorp Vault PKI for automatic rotation without downtime. Enable OCSP stapling for revocation checking without client-side latency. For session resumption, enable TLS 1.3 PSK tickets with short rotation periods; support 0-RTT only for read-only operations due to replay attack risk.

---

## WebAssembly replaces Lua for extension sandboxing

The decade-long pattern of Lua sandbox escapes demonstrates a fundamental architectural flaw: denylist-based sandboxing in a shared address space cannot be made reliably secure. WASM provides a ground-up secure alternative.

**WASM's security model is foundational, not bolted on.** Modules execute in fault-isolated linear memory with no ambient authority — they cannot access host memory, other modules, or the call stack. All capabilities (KV operations, logging) must be explicitly imported by the host. There is no equivalent to Lua's `package.loadlib()`. Indirect calls go through typed function tables validated at load time, providing control-flow integrity. Out-of-bounds memory access traps rather than corrupts.

**Wasmtime is the right runtime.** Maintained by the Bytecode Alliance, wasmtime provides 2GB guard regions preceding linear memories, memory zeroing after instance completion, copy-on-write instantiation (microsecond startup), and formal verification of critical code. Resource control uses **epoch interruption** (~10% overhead) for CPU time limiting and the `ResourceLimiter` trait for memory caps. Fuel metering offers deterministic instruction counting when needed. The Component Model and WASI enable strongly-typed, capability-based I/O — modules receive only the KV operations they're authorized to call.

**Defense-in-depth from Cloudflare's model.** Cloudflare Workers layers V8 isolate memory isolation, Linux namespace + seccomp sandboxing, trust-based scheduling (cordons), hardware memory protection keys (MPK), and separate TLS termination. The KV store extension system should similarly layer WASM isolation + process-level sandboxing + capability-based APIs, assuming sandbox escapes will eventually happen.

**eBPF complements WASM for infrastructure.** Use the `aya` crate for eBPF-based XDP packet filtering (DDoS mitigation at wire speed), connection tracking, and observability probes. eBPF's kernel verifier guarantees bounded execution and valid memory access. But eBPF is too restrictive for user-facing extensions — it's limited to 512 bytes of stack, no heap, bounded loops, and requires privileged access. Use WASM for application-level extensions, eBPF for infrastructure.

---

## Distributed security: zero-trust for every channel

A distributed KV store must treat every node, client, and request as potentially hostile, regardless of network location.

### Cluster communication security

Assign each node a cryptographic identity via SPIFFE/SPIRE (e.g., `spiffe://cluster.example.com/node/region-us-east/shard-3`). All inter-node communication — gossip, replication, resharding — uses mTLS with short-lived X.509-SVIDs. Authenticate gossip messages with HMAC using a shared cluster secret derived from a key-derivation chain. Require a quorum of authenticated gossip reports before acting on failure detection (SWIM-style protocol with authenticated probes). New nodes must present a single-use, time-limited join token plus a valid mTLS certificate; existing nodes vote on admission before slot assignment. When a node is removed, its certificate goes on a CRL distributed via gossip.

During resharding, use a two-phase commit: migrate and verify at destination, then source deletes only after cryptographic acknowledgment. Both source and destination must enforce consistent ACLs during the migration window. Replication streams use TLS with application-layer HMAC integrity checks over data chunks — never write unencrypted data to disk during full sync.

### Service mesh integration and when to avoid it

A 2024 arxiv study measured service mesh overhead on latency-sensitive workloads: Istio sidecar adds **+166% P99 latency** at 3,200 RPS, while Istio Ambient mode adds only **+8%** and raw mTLS without a mesh adds **~3%**. For sub-millisecond KV operations, traditional sidecar proxies are unacceptable. Implement **application-level mTLS** for the hot data path (client ↔ node, node ↔ node replication) and use service mesh only for the control plane, admin interfaces, and ingress policy enforcement.

### DDoS and resource exhaustion defense

Redis has no per-IP connection limiting, no rate limiting, no command complexity estimation, and no per-tenant memory quotas. The replacement must implement:

- **Connection limits:** Per-IP (e.g., 100), per-tenant (e.g., 10,000), global maximum, with strict phase timeouts (TCP handshake: 5s, auth: 10s, idle: configurable).
- **Command complexity gates:** Classify commands by Big-O complexity. Reject or require elevated privileges for O(n)+ commands. Estimate cost before execution based on collection size; abort commands exceeding configurable wall-clock time (e.g., 100ms).
- **Memory bomb protection:** Enforce max key size (512 bytes), max value size (configurable per tenant), max collection cardinality (1M elements), and per-tenant memory quotas with eviction policies.
- **Rate limiting:** Token bucket per tenant implemented internally (not depending on the KV store itself), with configurable refill rate and burst size per tier.

### Timing and side-channel defenses

True constant-time key lookup is impractical for hash tables — memory access patterns inherently leak through cache timing. Practical mitigations include: constant-time password/token comparison via the `subtle` crate; random microsecond jitter on response times; cryptographic internal key hashing to prevent cache-line inference; batched response delivery (return all results together to prevent per-key timing analysis); and CPU pinning with hyperthreading disabled on security-critical deployments to mitigate Spectre-class attacks.

For multi-tenant isolation, deploy tenant shards in separate processes (not just threads) to leverage hardware-enforced memory isolation. On AMD SEV-SNP or Intel TDX platforms, each tenant VM gets per-VM AES-128-XTS encryption with hardware-managed keys, rendering physical memory attacks useless. Disable swap entirely on KV nodes, pin all pages with `mlockall()`, disable core dumps via `prctl(PR_SET_DUMPABLE, 0)`, and mark sensitive pages with `madvise(MADV_DONTDUMP)`.

---

## How Dragonfly, Garnet, and KeyDB compare on security

Understanding competitors' security posture reveals both gaps to exploit and lessons to learn.

| Feature | Redis | Dragonfly (C++) | Garnet (C#/.NET) | KeyDB (C) |
|---------|-------|-----------------|-------------------|-----------|
| Memory safety | ❌ | ❌ | **✅ Managed runtime** | ❌ |
| Multi-threaded TLS | ❌ (until 8.0) | ✅ | ✅ | ✅ (7× Redis) |
| External identity provider | ❌ | ❌ | **✅ Azure AD/Entra ID** | ❌ |
| Encryption at rest | ❌ | ❌ | ❌ | ❌ |
| Audit logging | Basic | ✅ Per-thread ACL log | ❌ | ❌ |
| SOC 2 compliance | Via Redis Cloud | ✅ Dragonfly Cloud | Via Azure | N/A |
| Known CVEs | 50+ | 1 (DoS) | 0 | Inherits Redis |

**Garnet** offers the closest to memory safety via C#/.NET managed runtime, and uniquely supports Azure AD authentication — a real enterprise differentiator. However, it deliberately manages the main store's memory outside the GC for performance (weakening the safety guarantee in the hot path), explicitly acknowledges no encryption for disk-spilled data, and remains a research project. **Dragonfly's** shared-nothing architecture provides implicit per-shard isolation and forkless snapshots, plus the most developed audit logging among alternatives. Its C++ codebase remains vulnerable to memory corruption. **KeyDB** provides excellent multi-threaded TLS performance (7× Redis) but is stuck at Redis 6.2.6 parity, falling behind on security patches from Redis 7.x and 8.x.

None of the three provide encryption at rest, comprehensive audit logging, RBAC, FIPS compliance, or WASM-based secure extensions. A Rust replacement addressing all of these simultaneously would occupy a unique position in the market.

---

## The complete security architecture

The following layered architecture addresses every gap identified in this analysis.

### Layer 1: Language-level safety (compile-time)
`#![forbid(unsafe_code)]` on all application crates. Strategic `unsafe` only in dedicated low-level crates (io_uring, SIMD, allocator). Audit with cargo-geiger, Miri, cargo-careful. `panic = "abort"` + `overflow-checks = true` in release. Full supply chain pipeline: cargo-audit + cargo-deny + cargo-vet + vendored dependencies.

### Layer 2: Cryptographic foundation
rustls 0.23+ with aws-lc-rs FIPS backend. Mandatory TLS 1.3 on all channels (client, replication, cluster bus, admin). mTLS with short-lived SPIFFE certificates for inter-node auth, client certificate-to-role mapping for external clients. kTLS offload for data-plane encryption. All secrets wrapped in `SecretBox<T>` (prevents logging), `ZeroizeOnDrop` (clears on drop), `mlock` (prevents swapping).

### Layer 3: Authentication and authorization
Hierarchical RBAC: roles → permissions (commands, key patterns, channels) with inheritance and composition. OIDC/LDAP integration for external identity federation. HashiCorp Vault integration for dynamic credential issuance with configurable TTLs. Constant-time token comparison via `subtle`. Certificate-based auth as primary mechanism; passwords as fallback. Per-connection authorization state with re-evaluation on ACL changes.

### Layer 4: Multi-tenant isolation
Per-tenant key namespaces enforced at the storage layer (not just ACL patterns). Per-tenant memory quotas, connection limits, command rate limits (token bucket), and command complexity budgets. Separate processes for highest-isolation tenants. Network namespace isolation via Kubernetes NetworkPolicies or Cilium eBPF.

### Layer 5: Extension sandboxing
Wasmtime-based WASM execution with capability-based imports (`kv_get`, `kv_set`, `kv_delete`). Epoch interruption for CPU limits. `ResourceLimiter` for memory caps. Per-request instance creation via CoW (microsecond overhead). No host filesystem/network access. Extensions cannot access TLS keys or raw sockets.

### Layer 6: Observability and compliance
Structured JSON audit events for every security-relevant operation, forwarded asynchronously via TCP to SIEM. Tamper-evident hash chains anchored to immutable storage. FIPS 140-3 via aws-lc-rs for all crypto. GDPR deletion support via `SCAN`-based key enumeration with TTL policies and deletion logging. Security metrics: failed auth rate, ACL violation rate, anomalous command patterns, connection source monitoring — all exported via Prometheus.

### Layer 7: Network and infrastructure defense
Connection limits per IP/tenant/global with phase timeouts. Command complexity classification and pre-execution cost estimation. eBPF XDP for wire-speed DDoS filtering. Memory bomb protection via configurable key/value/collection size caps. Gossip protocol authentication via HMAC. Hardware memory encryption (SEV-SNP/TDX) for physical attack resistance. No swap, no core dumps, `prctl(PR_SET_DUMPABLE, 0)`.

---

## Conclusion: what makes this architecture fundamentally different

The core insight is that Redis's security problems are not implementation bugs to be patched — they are **structural consequences of design decisions** made in 2009: a C codebase with manual memory management, a trusted-network security model, a denylist-based Lua sandbox, and single-threaded execution that makes TLS prohibitively expensive.

A Rust replacement doesn't just fix individual CVEs; it eliminates the vulnerability classes that produce them. The **60% of Redis CVEs attributable to memory safety** become compile-time errors. The Lua sandbox escapes become impossible when WASM provides hardware-enforced linear memory isolation with zero ambient authority. The TLS performance penalty shrinks from 36% to under 10% with multi-threaded rustls + kTLS offload. And enterprise features — RBAC, encryption at rest, audit logging, FIPS 140-3 — move from "requires commercial product" to "built into the open-source core."

Three architectural bets are particularly high-leverage. First, **aws-lc-rs as the crypto foundation** provides FIPS 140-3 validation, post-quantum readiness, and 2× handshake performance versus OpenSSL in a single dependency. Second, **WASM via wasmtime** for extensions replaces a decade of Lua sandbox failures with a formally specified isolation model that assumes hostile code. Third, **SPIFFE-based node identity** with mTLS everywhere replaces Redis's trust-the-network model with cryptographically verified, automatically rotated identities.

The result is a system where security is a compile-time guarantee, not a runtime hope — where the type system prevents accidental secret exposure, the borrow checker prevents use-after-free, the WASM sandbox prevents code execution escapes, and the audit log captures every security-relevant event for compliance. That is the delta between a Redis replacement and a Redis successor.
---
title: "moon:// / moons:// URI scheme"
description: "Authoritative spec for Moon's native connection URI scheme — a superset of redis:// / rediss://."
---

# `moon://` / `moons://` connection URI scheme

**Status:** SPEC — accepted design, doc-only (v0.6.1 task **H-7**). No Rust implementation
ships with this document. The shared parser (`src/uri.rs`), server-side `--announce-url`
plumbing, and `tests/uri_scheme.rs` conformance matrix are **v0.7.0 Workstream R6**
(see [ROADMAP.md §8.2](../roadmap/ROADMAP.md#82-v070--replication-ga) and
[§8.5](../roadmap/ROADMAP.md#85-cross-cutting-native-moon--moons-connection-uri-scheme)).
This document is the source of truth that R6 must implement against and that
`tests/uri_scheme.rs` must be written to conform to.

Until R6 ships, `moon://` / `moons://` strings are **not understood by any Moon binary** —
`moon-cli -u`, client libraries, `REPLICAOF`, and `CLUSTER MEET` all still take
`redis://` / `rediss://` or bare `host port` today. See
[Conformance](#conformance-v070-r6-implementation-gate) for the exact behavior that must
exist before this scheme is considered "live."

## Motivation

Moon is Redis-wire-compatible, so `redis://` / `rediss://` connection strings work today
and **must keep working** — this spec changes nothing about that. But Moon is a
multi-model engine with first-class multi-tenancy ([workspaces](../guides/workspaces.md)),
TLS 1.3, and (from v0.7.0) multi-shard replication. It deserves a native, self-branding
URL scheme the way Redis has its own — `moon://` / `moons://` are that scheme: a strict
superset of the Redis URI understood by clients, replication, cluster redirects, and
`--announce-url`. `moons://` is the TLS variant, exactly like `rediss://` is to `redis://`
and `https://` is to `http://`.

**Backward compatibility (non-negotiable).** The scheme is a *client-side transport +
routing* convention only — **zero wire-protocol change**. `redis://` / `rediss://` remain
fully accepted and semantically identical for every overlapping field. Any client that
already allows a scheme override keeps working unmodified; `moon(s)://` is additive.

## Grammar (ABNF)

```abnf
moon-uri    = scheme "://" [ userinfo "@" ] host [ ":" port ] [ "/" db-index ] [ "?" query ]

scheme      = "moon" / "moons"                ; "moons" = TLS 1.3 transport (rustls, aws-lc-rs)

userinfo    = [ username ] [ ":" password ]   ; maps to AUTH [user] pass
username    = *( unreserved / pct-encoded / sub-delims )
password    = *( unreserved / pct-encoded / sub-delims / ":" )

host        = IP-literal / IPv4address / reg-name / unix-path-encoded
              ; IP-literal, IPv4address, reg-name per RFC 3986 §3.2.2
unix-path-encoded = "unix" ; reserved reg-name value — see "Unix sockets" below

port        = 1*DIGIT
              ; moon:  default 6379 if omitted (matches --port default)
              ; moons: NO implicit default — port MUST be given explicitly and
              ;        MUST equal the server's configured --tls-port. There is no
              ;        well-known "TLS port" the way 443 is to 80; guessing one
              ;        would silently connect to the wrong listener or hang.

db-index    = 1*DIGIT                          ; SELECT <db-index> on connect

query       = param *( "&" param )
param       = key "=" value
key         = 1*( ALPHA / DIGIT / "_" )
value       = *( unreserved / pct-encoded / sub-delims )

unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
pct-encoded = "%" HEXDIG HEXDIG
sub-delims  = "!" / "$" / "'" / "(" / ")" / "*" / "+" / "," / ";"
```

Notes:

- `scheme` is case-insensitive at parse time (`Moon://`, `MOONS://` are accepted) but Moon
  tooling always **emits** lowercase.
- `host` follows RFC 3986 host productions unchanged — IPv6 literals use bracket form
  (`moon://[::1]:6379`).
- Percent-decoding of `userinfo` and `value` happens **after** field splitting, so a literal
  `@`, `:`, `/`, `&`, or `=` inside a username/password/value MUST be percent-encoded.
- Unrecognized query keys are a parse **error**, not silently ignored — see
  [Design-for-failure](#design-for-failure).
- **Unix sockets** are a reserved future extension (`moon://unix/path/to/socket`, mirroring
  how some Redis clients overload `host=unix`). Not part of this spec's normative grammar
  beyond reserving the token; Moon has no Unix-socket listener today. If/when one ships,
  it gets its own ROADMAP entry and an addendum here — do not implement against this
  paragraph alone.

## `redis(s)://` parity table

Every field below behaves **identically** whether the scheme is `redis(s)` or `moon(s)`,
except where the "Moon-native" rows call out an extension. This table is normative for
the R6 parser: any divergence from it is a bug.

| Concern | `redis(s)://` behavior | `moon(s)://` behavior |
|---|---|---|
| Transport | `redis` = plaintext, `rediss` = TLS | `moon` = plaintext, `moons` = TLS 1.3 (rustls + aws-lc-rs) — **identical selection rule** |
| Default port | `redis://` → 6379 | `moon://` → 6379; `moons://` → **no default**, port is mandatory and must equal `--tls-port` |
| Auth | `userinfo` → `AUTH [user] pass` | same |
| DB select | `/N` → `SELECT N` | same |
| TLS peer verification | `?ssl_cert_reqs=` | same key accepted (alias, see [below](#tls-query-options)) |
| TLS CA bundle | `?ssl_ca_certs=` | same key accepted (alias) |
| Socket read/write timeout | `?socket_timeout=` | same |
| Connect timeout | `?socket_connect_timeout=` | same |
| **Workspace selection** | *(none — client issues `WS AUTH <ws_id>` after connecting)* | `?workspace=<id-or-name>` selects the [Moon workspace](../guides/workspaces.md) **before the first application command** — multi-tenancy is a shipped guarantee, not bolted on post-connect |
| **Server self-announce** | *(n/a)* | server emits `moon(s)://` URIs in `INFO replication` (`master_announce_url`), `CLUSTER SHARDS`/`MOVED`/`ASK` redirects, and replica handshake metadata |

## Query-parameter reference

### Parity parameters (shared with `redis(s)://`) {#tls-query-options}

| Parameter | Type | Default | Example | Semantics |
|---|---|---|---|---|
| `ssl_cert_reqs` | enum `none` \| `optional` \| `required` | `required` on `moons`/`rediss`; n/a on plaintext | `?ssl_cert_reqs=required` | Peer certificate verification mode. `none` disables verification (dev-only; client MUST warn). Server-side mTLS is independently controlled by `--tls-ca-cert-file`; this parameter is about the **client's** verification of the server cert. |
| `ssl_ca_certs` | path (string, percent-encoded) | *(system trust store)* | `?ssl_ca_certs=%2Fetc%2Fmoon%2Fca.pem` | Path to a PEM CA bundle the client uses to verify the server certificate. |
| `ssl_certfile` | path | *(none)* | `?ssl_certfile=%2Fetc%2Fmoon%2Fclient.crt` | Client certificate for mTLS (paired with `--tls-ca-cert-file` on the server). |
| `ssl_keyfile` | path | *(none)* | `?ssl_keyfile=%2Fetc%2Fmoon%2Fclient.key` | Client private key for mTLS. |
| `socket_timeout` | duration, seconds (float) | client-library default | `?socket_timeout=5` | Per-operation read/write timeout after the connection is established. |
| `socket_connect_timeout` | duration, seconds (float) | client-library default | `?socket_connect_timeout=2` | Bounds the initial TCP + (if `moons`/`rediss`) TLS handshake. See [Design-for-failure](#design-for-failure) — this is the knob that makes a `moons://` fail-fast guarantee concrete. |

### Moon-native parameters

| Parameter | Type | Default | Example | Semantics |
|---|---|---|---|---|
| `workspace` | string — UUID (v7, from `WS CREATE`) **or** workspace name | *(none — connection is unbound, matching current `WS AUTH` behavior)* | `?workspace=0193a9f2-e456-7890-abcd-ef1234567890` or `?workspace=myapp` | Sent as `WS AUTH <workspace>` immediately after `AUTH`/`SELECT`, before the first application command. **Implementation note (R6c):** the wire command `WS AUTH <ws_id>` today (`src/command/workspace.rs`) accepts **only** a UUID; it does not resolve names even though `WorkspaceRegistry::get_by_name` (`src/workspace/registry.rs:58`) already exists server-side. R6c must do ONE of: (a) extend `WS AUTH` to accept a name and resolve it server-side via `get_by_name`, or (b) restrict this query parameter's accepted grammar to UUID-only and document that name resolution is a client-side, pre-connect lookup. Recommendation: (a) — the registry lookup already exists, and requiring callers to know UUIDs defeats the ergonomic point of a human-readable tenant name in a connection string. This spec does not pick a winner; R6c's PR description must record the decision. |

## Design-for-failure

Per this repo's IO-failure design rule (timeouts, retries, circuit breakers, no silent
degradation), the URI scheme has hard failure semantics — there is no "best effort" mode:

- **No opportunistic downgrade.** A `moons://` target that answers in plaintext (or fails
  the TLS handshake in a way indistinguishable from "this port speaks plaintext") is a
  **hard connection error**, never a silent fallback to unencrypted `moon://` semantics.
  This closes the STARTTLS-strip downgrade vector (an on-path attacker cannot force a
  client down to plaintext by intercepting the handshake).
- **No auto-upgrade**, symmetrically. `moon://` never opportunistically negotiates TLS even
  if the target happens to also accept it on the same port. Scheme selects transport;
  transport is never inferred from the peer's behavior.
- **Fail fast — never hang.** `moons://` dialed against a server with no `--tls-port`
  configured (or the wrong port) must fail within `?socket_connect_timeout=` (or the
  client's default) with a diagnostic equivalent to:

  ```text
  TLS required by scheme but server has no TLS listener
  ```

  This is a **connect-time** classification, not a post-handshake timeout: the client
  should not need to wait out a full TLS handshake timeout to learn the port doesn't speak
  TLS at all when that can be determined earlier (e.g. the peer resets/closes on a raw
  ClientHello, or responds with a plaintext RESP error/greeting).
- **Unknown scheme → immediate parse error.** Any scheme other than `moon`, `moons`,
  `redis`, `rediss` is rejected at parse time, before any socket is opened. No guessing,
  no "try both."
- **Bounded connect, always.** `?socket_connect_timeout=` (or the client's configured
  default when the query parameter is absent) bounds the dial for **both** schemes and
  **both** transports. Retry/backoff policy is unchanged by the scheme — it is the client's
  existing policy, not something the URI itself encodes (there is deliberately no
  `?retries=`/`?backoff=` parameter; conflating connection addressing with retry policy
  has caused ambiguity bugs in other ecosystems' URI schemes and is out of scope here).
- **Malformed percent-encoding, missing mandatory `moons://` port, or an unrecognized query
  key are all parse errors** raised before any I/O — never partially-applied, never
  defaulted-and-continue.

## Server participation

Once R6a lands, the server is not just a URI *target* — it advertises and consumes its own
scheme:

- **`--announce-url moon(s)://host:port`** — a new config flag (not present in `src/config.rs`
  today) giving the server a canonical externally-reachable URL. When set, it takes
  precedence over the existing `--announce-ip`/discovered-address logic for anything that
  currently emits a bare `host:port` pair.
- **`INFO replication`** gains a `master_announce_url` field carrying this value verbatim
  (today `INFO replication` reports `role:master` / `role:slave` plus host/port fields —
  see `src/replication/*.rs` — with no scheme-qualified URL).
- **Cluster redirects** (`MOVED`, `ASK`, `CLUSTER SHARDS`) surface `moon(s)://` alongside the
  existing bare-address form once cluster mode understands the scheme (v0.8.0 — out of
  scope for R6, tracked so this doc doesn't over-promise).
- **`REPLICAOF` / `CLUSTER MEET`-adjacent inputs** accept `moon(s)://host:port` as an
  alternative to the current bare `REPLICAOF host port` form (`src/command/connection.rs:639`).
  `moons://` selects the TLS replication connector, `moon://` the plaintext one — the same
  no-downgrade/no-upgrade rule from [Design-for-failure](#design-for-failure) applies to
  inter-node replication links, not just client connections.

## Worked examples

```text
# Plaintext, default port, no auth, no db-select
moon://localhost/

# Plaintext, explicit port, db 3
moon://localhost:6399/3

# TLS 1.3, explicit tls-port (mandatory — no implicit default)
moons://cache.internal:6380/

# Auth (user + password) + db-select
moon://appuser:s3cr3t@cache.internal:6379/2

# Password-only auth (Redis single-arg AUTH form)
moon://:s3cr3t@cache.internal:6379/

# TLS with explicit peer verification + CA bundle
moons://cache.internal:6380/?ssl_cert_reqs=required&ssl_ca_certs=%2Fetc%2Fmoon%2Fca.pem

# mTLS: client cert + key, CA bundle, bounded connect
moons://cache.internal:6380/?ssl_ca_certs=%2Fetc%2Fmoon%2Fca.pem&ssl_certfile=%2Fetc%2Fmoon%2Fclient.crt&ssl_keyfile=%2Fetc%2Fmoon%2Fclient.key&socket_connect_timeout=2

# Moon-native: workspace selection by UUID, plaintext
moon://appuser:s3cr3t@cache.internal:6379/0?workspace=0193a9f2-e456-7890-abcd-ef1234567890

# Moon-native: workspace selection by name, TLS
moons://appuser:s3cr3t@cache.internal:6380/0?workspace=myapp

# Server self-announce (--announce-url), as it would appear in INFO replication
master_announce_url:moons://replica-2.internal:6380

# redis:// / rediss:// keep working unmodified — parity, not deprecation
redis://appuser:s3cr3t@cache.internal:6379/0
rediss://cache.internal:6380/?ssl_cert_reqs=required
```

## Conformance (v0.7.0 R6 implementation gate)

The R6 implementation (`src/uri.rs` + call sites) and `tests/uri_scheme.rs` MUST satisfy
every item below before this scheme is considered shipped. This list is the acceptance
checklist for R6, not aspirational:

- [ ] `moon://` and `redis://` parse to byte-identical internal representations for every
      overlapping field (transport=plaintext, host, port, auth, db, shared query params).
- [ ] `moons://` and `rediss://` parse identically for every overlapping field
      (transport=TLS) **except** default-port behavior, which is intentionally different
      (see grammar) and must be a distinct, explicitly-tested case.
- [ ] Round-trip: parse → re-serialize → parse is stable (idempotent) for every worked
      example above, for both `moon(s)` and `redis(s)` families.
- [ ] `moons://` with no port present is a **parse error**, not a fallback to any default.
- [ ] `moons://host:<port-not-equal-to-tls-port>` connects, completes TCP, and fails the
      TLS handshake or is rejected — verified as the exact "no opportunistic downgrade"
      error, not a hang and not a silent plaintext fallback.
- [ ] `moons://` against a server started without `--tls-port` fails within
      `?socket_connect_timeout=` (test asserts wall-clock bound, not just eventual
      failure) with the diagnostic text from
      [Design-for-failure](#design-for-failure).
- [ ] `moon://` against a `moons`-only listener does not upgrade — either connection
      refused/reset or a decodable-but-hard error, never treated as a successful plaintext
      session against a TLS port.
- [ ] Unknown scheme (`redi://`, `moon2://`, empty scheme) is a parse error raised before
      any socket syscall — assert via a mock/no-network unit test, not an integration test
      (must not depend on network reachability to prove "never dials").
- [ ] Unrecognized query key is a parse error (not silently dropped) — one test per family.
- [ ] `?workspace=<uuid>` lands the session in that workspace (`WS AUTH` observably applied
      — e.g. a subsequent `SET`/`GET` round-trips through the workspace-prefixed keyspace)
      before any application command the caller issues is processed.
- [ ] `?workspace=<name>` behaves per whichever of the two options in the
      [workspace parameter row](#moon-native-parameters) R6c actually implements — the
      chosen behavior must itself be covered by a test, and the PR description must state
      which option was chosen (this doc intentionally leaves it open).
- [ ] `moon-cli -u moon://…` and `moon-cli -u moons://…` parse-and-dial parity with the
      existing `-u redis://…` / `-u rediss://…` paths (same flag, wider scheme set).
- [ ] `src/uri.rs` has a `cargo-fuzz` target (any new parser needs one per CLAUDE.md) —
      added to `fuzz/fuzz_targets/` and wired into the 15-min-per-target PR fuzz job.
- [ ] `--announce-url moon(s)://host:port` is validated at startup (scheme ∈ {moon, moons},
      port present) and rejected with a startup error (not accepted-then-silently-ignored)
      if malformed.
- [ ] `INFO replication` exposes `master_announce_url` only when `--announce-url` is set;
      absent otherwise (no empty-string field).

## Implementation-notes appendix

Pointers into the current codebase for whoever picks up R6 — verified against this
checkout, not the roadmap prose:

- **TLS flags** (`src/config.rs`; confirmed via `#[arg(long = ...)]` clap attributes):
  - `--tls-port` (u16, default `0` = disabled) — `src/config.rs:264`
  - `--tls-cert-file` — `src/config.rs:268`
  - `--tls-key-file` — `src/config.rs:272`
  - `--tls-ca-cert-file` (enables mTLS / client-cert verification when set) — `src/config.rs:276`
  - `--tls-ciphersuites` (comma-separated; defaults to a frozen AEAD-only, PFS-required
    allowlist if omitted — see `DEFAULT_CIPHER_SUITES` in `src/tls.rs`) — `src/config.rs:280`
- **TLS engine**: pure-Rust, `rustls` + `aws-lc-rs` crypto provider (no OpenSSL dependency),
  TLS 1.3 by default via `with_safe_default_protocol_versions()`; TLS 1.2 cipher suites are
  in the resolver (`src/tls.rs:9-57`) for interop but the default allowlist favors TLS 1.3.
  See `docs/guides/tls.md` for the operator-facing setup guide and
  `docs/runbooks/tls-cert-rotation.md` for rotation.
- **mTLS**: `build_tls_config()` in `src/tls.rs:64-175` builds a `WebPkiClientVerifier` from
  `--tls-ca-cert-file` when present; omit that flag and the server accepts any client
  (`with_no_client_auth()`).
- **Hot reload**: `SharedTlsConfig = Arc<ArcSwap<rustls::ServerConfig>>`
  (`src/tls.rs:182`); SIGHUP re-reads cert/key/CA from disk and atomically swaps
  (`src/tls.rs:189-…`, plus the signal-handling thread wiring later in the same file).
  In-flight handshakes keep the old config; new connections see the reload immediately.
  This is why a `moons://` client observing a mid-session cert rotation is expected —
  it is not a downgrade, the transport never changed.
- **Workspaces**: command surface is `WS CREATE|DROP|AUTH|INFO|LIST`
  (`src/command/workspace.rs`), intercepted before normal dispatch
  (same pattern as `TXN.*`/`TEMPORAL.*`). `WS AUTH <ws_id>` currently requires a UUID
  (`validate_ws_auth`, `src/command/workspace.rs:106`); `WorkspaceRegistry::get_by_name`
  (`src/workspace/registry.rs:58`) already exists but is not wired to `WS AUTH` — this is
  exactly the R6c open decision flagged in the
  [workspace parameter row](#moon-native-parameters) above. See
  [Workspaces guide](../guides/workspaces.md) for full command semantics and key-rewriting
  behavior.
- **`--announce-url` / `src/uri.rs` / and the `REPLICAOF host port`-only surface
  (`src/command/connection.rs:639`) confirm this is entirely prospective work** — a repo
  search for `announce_url`, `announce-url`, and `src/uri.rs` returns no matches in this
  checkout. This document is the spec R6 must be built against; do not treat any mention of
  these names elsewhere as already-implemented until R6 lands.

## Discrepancies vs. ROADMAP.md §8.5

Flagged during authoring, for the R6 implementer's awareness — none of these change the
decided design, they are precision gaps in the roadmap's prose that this doc resolves:

1. **`?workspace=<tenant>` wire semantics were unspecified.** §8.5's parity table says the
   parameter "selects the Moon workspace before the first command" but doesn't say what
   value the client sends over the wire. The actual `WS AUTH` command
   (`src/command/workspace.rs`) takes a UUID only, not a name, even though `WS CREATE`
   returns UUIDs from human-readable names and the registry already supports
   name→metadata lookup. Resolved above by making `?workspace=` accept either form in the
   URI grammar and calling out the two implementation options for R6c explicitly, rather
   than silently assuming name resolution "just works" today.
2. **`--announce-url`, `INFO replication`'s `master_announce_url`, and `src/uri.rs` are
   described in §8.5's "Deliverables" as R6 work but the wording could be misread as
   already-partial.** Confirmed via repo search: none exist in the current tree. This doc
   treats them as 100% prospective.

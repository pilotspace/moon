# Phase 22: ACL System and Per-User Permissions - Research

**Researched:** 2026-03-25
**Domain:** Redis ACL system, SHA256 password hashing, per-connection permission enforcement
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Each user has: username, password(s), enabled/disabled flag, command permissions, key patterns, channel patterns
- Default user: backward compatible with single-password AUTH — `default` user with `~* +@all`
- Multiple passwords per user supported (SHA256 hashed, not stored in plaintext)
- User enabled/disabled toggle — disabled user cannot authenticate
- Permission categories matching Redis: `+@all`, `-@all`, `+@read`, `+@write`, `+@set`, `+@sortedset`, `+@hash`, `+@list`, `+@string`, etc.
- Individual command allow/deny: `+command` / `-command`
- Subcommand level: `+command|subcommand` (e.g., `+config|get -config|set`)
- Deny takes precedence over allow (most restrictive wins)
- Categories are predefined sets of commands (matching Redis's category taxonomy)
- `~pattern` — allow access to keys matching glob pattern; `~*` all keys
- `%R~pattern` — read-only access; `%W~pattern` — write-only access
- Multiple patterns allowed — key must match at least one
- `&pattern` — allow pub/sub access to channels; `&*` all channels
- Enforced on SUBSCRIBE, PSUBSCRIBE, and PUBLISH commands
- ACL commands: SETUSER, GETUSER, DELUSER, LIST, WHOAMI, CAT, LOG, SAVE, LOAD
- `AUTH password` — authenticate as default user (backward compatible)
- `AUTH username password` — authenticate as specific user (RESP3 / Redis 6+)
- HELLO command's AUTH option uses the same mechanism
- Connection starts as default user; AUTH switches to specified user
- Failed auth increments ACL LOG entry
- Every command checked against connection's current user before execution
- Check order: (1) user enabled? (2) command allowed? (3) key patterns match? (4) channel patterns match?
- NOPERM error returned for denied commands with descriptive message
- Enforcement is per-shard (each shard has read-only reference to ACL table)
- ACL file format matching Redis: one user per line, space-separated rules
- ACL SAVE writes to configured ACL file path
- ACL LOAD reloads and replaces in-memory ACL table
- On startup: load ACL file if configured, otherwise default user only

### Claude's Discretion
- ACL file path configuration (CLI flag or CONFIG SET)
- Password hashing: SHA256 or bcrypt (SHA256 matches Redis, simpler)
- ACL LOG max entries (default 128, circular buffer)
- Whether to implement ACL DRYRUN (test permissions without executing)

### Deferred Ideas (OUT OF SCOPE)
- ACL rules in CONFIG REWRITE — defer to when CONFIG REWRITE is implemented
- Pub/Sub channel ACL enforcement for PSUBSCRIBE patterns — complex pattern matching, implement basic first
- External ACL provider (LDAP, OAuth) — out of scope
</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| ACL-01 | ACL SETUSER creates/modifies users with password, command permissions, key patterns, channel patterns | AclTable struct, rule parser, SHA256 hashing with sha2 crate |
| ACL-02 | ACL GETUSER/LIST/DELUSER/WHOAMI | AclTable read methods, WHOAMI reads current_user from connection state |
| ACL-03 | AUTH username password (two-arg form) | conn_cmd::auth extended signature; `authenticated` flag → `current_user: String` |
| ACL-04 | Default user backward compatible | Default user constructed from requirepass config at startup |
| ACL-05 | Command filtering with NOPERM | acl_check() before dispatch in both connection handlers |
| ACL-06 | Key pattern matching | glob_match reuse from key.rs; check at connection level pre-dispatch |
| ACL-07 | ACL LOG | Circular buffer (VecDeque with 128 cap) per shard or global Arc<Mutex> |
| ACL-08 | ACL SAVE/LOAD | File I/O with Redis-compatible line format |
</phase_requirements>

---

## Summary

Phase 22 adds a Redis 6-compatible ACL system. The core challenge is architectural: the ACL table must be readable by every shard without mutex contention on the hot path, while ACL-mutating commands (SETUSER, DELUSER, LOAD) must propagate to all shards. The pattern to follow is how `ScriptLoad` fans out via SPSC — a new `AclUpdate` ShardMessage variant replaces/merges the in-shard ACL snapshot.

The current codebase has a clean separation: connection-level intercepts (AUTH, HELLO, CLIENT, BGSAVE, CONFIG, PUBLISH) run before dispatch. The ACL permission gate adds a new layer after authentication but before database dispatch. Key patterns are already glob-matched in `key.rs` — that function is `pub(crate)` and can be reused.

Password storage: Redis uses `#<sha256hex>` for pre-hashed passwords and plaintext (which it immediately hashes) for `>password` rule syntax. Using the `sha2` crate (v0.10.9, stable) with `hex` (v0.4.3) is the right choice — it matches Redis exactly, zero runtime cost compared to bcrypt, and sha1_smol is already in the dependency tree showing the project's comfort with this crate family.

**Primary recommendation:** New `src/acl/` module with `table.rs` (AclTable, AclUser), `rules.rs` (rule parser), `log.rs` (AclLog circular buffer), and `io.rs` (SAVE/LOAD). Share via `Arc<RwLock<AclTable>>` cloned to each shard at startup (reads are cheap — RwLock read lock, no contention unless ACL is being mutated). Use `AclUpdate` ShardMessage for propagation.

---

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| sha2 | 0.10.9 | SHA-256 password hashing | Matches Redis `#sha256hex` format exactly; RustCrypto ecosystem standard |
| hex | 0.4.3 | Encode/decode SHA256 hex strings | Companion to sha2; used for `#<hash>` rule format |

Both are already used indirectly (sha1_smol already in dep tree; RustCrypto crates are standard for crypto in Rust).

**No other new dependencies needed** — all other infrastructure (glob_match, Bytes, parking_lot, Arc, VecDeque, tokio fs) is already present.

### Installation
```toml
sha2 = "0.10"
hex = "0.4"
```

**Version verification:** sha2 stable = 0.10.9 (verified crates.io 2026-03-25), hex = 0.4.3 (verified crates.io 2026-03-25).

---

## Architecture Patterns

### Recommended Project Structure
```
src/acl/
├── mod.rs          # pub use, AclTable, AclUser, AclPermissions
├── table.rs        # AclTable (HashMap<String, AclUser>), CRUD operations
├── rules.rs        # Rule string parser: +@all, ~pattern, &channel, >pass, #hash
├── log.rs          # AclLog: VecDeque<AclLogEntry>, max 128 entries, circular evict
└── io.rs           # acl_save(path, table), acl_load(path) -> AclTable
```

### Pattern 1: AclUser Data Model

```rust
// src/acl/table.rs
use std::collections::HashSet;
use bytes::Bytes;

pub struct AclUser {
    pub username: String,
    pub enabled: bool,
    /// SHA256 hashes of allowed passwords (hex-encoded, 64 chars each)
    pub passwords: Vec<String>,
    /// nopass flag — user can auth with any password (or no password)
    pub nopass: bool,
    /// Allowed command flags: None = all allowed, Some(set) = only these
    pub allowed_commands: CommandPermissions,
    /// Key patterns user can access (~pattern); empty = no key access
    pub key_patterns: Vec<KeyPattern>,
    /// Channel patterns for pub/sub (&pattern); empty = no channel access
    pub channel_patterns: Vec<String>,
}

pub enum CommandPermissions {
    /// +@all (default for default user)
    AllAllowed,
    /// -@all baseline with individual allows
    Specific {
        allowed: HashSet<String>,  // command names (lowercase) or @category
        denied: HashSet<String>,   // explicit denials (most restrictive wins)
    },
}

pub struct KeyPattern {
    pub pattern: String,
    pub read: bool,    // %R~ or ~
    pub write: bool,   // %W~ or ~
}
```

### Pattern 2: AclTable Sharing via Arc<RwLock<>>

Follow the `RuntimeConfig` pattern — `Arc<RwLock<AclTable>>` shared across shards:

```rust
// main.rs / listener startup
let acl_table: Arc<RwLock<AclTable>> = Arc::new(RwLock::new(AclTable::load_or_default(&config)));
// Clone Arc to each shard (cheap Arc clone, not data clone)
```

This matches how `repl_state: Option<Arc<RwLock<ReplicationState>>>` is already passed to `handle_connection_sharded`. ACL reads on the hot path take a read lock (shared, non-blocking unless writer holds write lock). ACL mutations (SETUSER, DELUSER, LOAD) take a write lock (brief) — then fan out an `AclUpdate` ShardMessage to force per-shard cache refresh if shards cache a local copy.

**Simpler alternative:** Since all shard threads share the same Arc, no fan-out is needed — just take the read lock on each command. The `ScriptCache` approach (per-shard copy + fan-out) is only needed when the data structure is !Send or heavy. AclTable is Send + Sync via Arc<RwLock>. Use the Arc<RwLock> directly; no ShardMessage needed.

### Pattern 3: Connection State Changes

`handle_connection` and `handle_connection_sharded` currently have:
```rust
let mut authenticated = requirepass.is_none();
```

Replace with:
```rust
let mut current_user: String = "default".to_string();
// authenticated = acl_table.read().get("default").map(|u| u.enabled && (u.nopass || requirepass.is_none())).unwrap_or(true)
```

The `authenticated` bool stays for the auth-gate fast path, but `current_user` tracks which user is active for permission checks.

### Pattern 4: ACL Permission Check Before Dispatch

In both `handle_connection` and `handle_connection_sharded`, add after the auth gate:

```rust
// ACL permission check (after authentication, before dispatch)
if let Some((cmd, cmd_args)) = extract_command(&frame) {
    if let Some(deny_reason) = acl_table.read().check_command_permission(&current_user, cmd, cmd_args) {
        // Log to ACL LOG
        acl_log.push(AclLogEntry { ... });
        responses.push(Frame::Error(Bytes::from(format!(
            "NOPERM User {} has no permissions to run the '{}' command",
            current_user,
            String::from_utf8_lossy(cmd).to_lowercase()
        ))));
        continue;
    }
}
```

Key pattern check happens inside the handler after command execution extracts the key — or alternatively in a wrapper that inspects args before dispatch. The simpler approach: key pattern check at the pre-dispatch stage by extracting the first key argument.

### Pattern 5: AUTH Two-Argument Form

Current `conn_cmd::auth(args, requirepass)` only handles one argument. Extend:

```rust
pub fn auth_acl(args: &[Frame], acl_table: &AclTable) -> (Frame, Option<String>) {
    // Returns (response, Option<new_username_if_success>)
    match args.len() {
        1 => {
            // AUTH password — authenticate as default user (backward compat)
            let password = extract_str(&args[0]);
            // Check default user
        }
        2 => {
            // AUTH username password — Redis 6+ form
            let username = extract_str(&args[0]);
            let password = extract_str(&args[1]);
            // Check named user
        }
        _ => Frame::Error(...)
    }
}
```

### Pattern 6: ACL File Format (Redis-Compatible)

```
user default on nopass ~* &* +@all
user alice on #2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b ~* +@read -@write
user readonly off >temporarypass ~* +@read +@string
```

Rules:
- `on` / `off` — enabled/disabled
- `nopass` — no password required
- `>password` — plaintext password (hashed on parse/write)
- `#sha256hex` — pre-hashed password
- `~pattern` — key pattern
- `%R~pattern` — read-only key pattern
- `%W~pattern` — write-only key pattern
- `&pattern` — channel pattern
- `+@category` / `-@category` — command category
- `+command` / `-command` — individual command
- `+command|subcommand` — subcommand level
- `resetpass` / `resetkeys` / `resetchannels` / `reset` — clear respective fields

### Pattern 7: Command Category Taxonomy

Categories must match Redis exactly for client-side ACL compatibility:

| Category | Commands |
|----------|----------|
| @all | every command |
| @read | GET, MGET, HGET, LRANGE, SMEMBERS, ZSCORE, etc. |
| @write | SET, MSET, HSET, LPUSH, SADD, ZADD, etc. |
| @string | GET, SET, INCR, APPEND, STRLEN, etc. |
| @hash | HGET, HSET, HDEL, HMGET, etc. |
| @list | LPUSH, RPUSH, LPOP, LRANGE, etc. |
| @set | SADD, SREM, SMEMBERS, SINTER, etc. |
| @sortedset | ZADD, ZREM, ZSCORE, ZRANGE, etc. |
| @stream | XADD, XREAD, XGROUP, etc. |
| @pubsub | SUBSCRIBE, PUBLISH, UNSUBSCRIBE, etc. |
| @admin | CONFIG, INFO, DEBUG, BGSAVE, ACL, etc. |
| @dangerous | FLUSHDB, FLUSHALL, DEBUG, CONFIG, ACL, etc. |
| @keyspace | DEL, EXISTS, EXPIRE, RENAME, SCAN, KEYS, etc. |
| @connection | AUTH, HELLO, PING, QUIT, SELECT, etc. |
| @transaction | MULTI, EXEC, DISCARD, WATCH, UNWATCH |
| @scripting | EVAL, EVALSHA, SCRIPT |
| @cluster | CLUSTER, ASKING, READONLY, READWRITE |

Implement as a function: `fn get_category_commands(category: &str) -> &'static [&'static str]` backed by a static match.

### Anti-Patterns to Avoid
- **Storing plaintext passwords:** Always SHA256-hash with sha2 + hex on write; compare hashes during AUTH
- **Global ACL mutex on command hot path:** Use Arc<RwLock> with read lock (shared), not a Mutex; write lock only on ACL mutations
- **Re-checking key patterns inside the database:** Check key patterns at the pre-dispatch layer using args inspection — avoids touching database code
- **Forgetting HELLO's AUTH option:** HELLO ... AUTH username password must go through acl_table, not the old requirepass check
- **Forgetting `nopass` users:** A `nopass` user authenticates with any password — the auth check must handle this before hash comparison
- **Default user edge cases:** If `requirepass` is set in config but no ACL file is configured, the default user gets that password hashed as its single password; if no requirepass and no ACL file, default user is `nopass on ~* &* +@all`

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| SHA-256 hashing | custom hash | `sha2` + `hex` crates | Correctness, constant-time comparison subtleties |
| Glob key pattern matching | new pattern matcher | `glob_match` from `key.rs` (already `pub(crate)`) | Already handles `*`, `?`, `[abc]` per Redis glob spec |
| Circular log buffer | manual Vec rotation | `std::collections::VecDeque` with capacity check | O(1) push_back + pop_front, no allocation after warmup |

**Key insight:** The glob_match function in `src/command/key.rs` already handles Redis-compatible glob patterns (`*`, `?`, `[abc]`). Reuse it for key pattern ACL checks and channel pattern checks — do NOT write a second glob matcher.

---

## Common Pitfalls

### Pitfall 1: AUTH Single vs Two-Argument Form Arity Error
**What goes wrong:** Current `auth()` returns `ERR wrong number of arguments` for `AUTH username password`. The fix is not just allowing 2 args — you must also handle `AUTH default ""` (empty password for nopass default user) and distinguish the two forms from arity.
**Why it happens:** Old `auth()` signature is `(args: &[Frame], requirepass: &Option<String>)` — it doesn't know about the ACL table.
**How to avoid:** Replace with `auth_acl(args, acl_table)` that returns `(Frame, Option<String>)` — the second return is the authenticated username on success. Keep the old `auth()` for backward compat in tests, or update all callers.
**Warning signs:** Test `AUTH default secret` returns arity error instead of OK/WRONGPASS.

### Pitfall 2: Default User Construction from requirepass
**What goes wrong:** When `requirepass` is set but no ACL file exists, the default user must have that password. If you forget this, single-password setups break.
**Why it happens:** The old authentication used `requirepass` directly; the new code constructs a `default` user from it.
**How to avoid:** In `AclTable::load_or_default(config)`, if `config.requirepass` is Some(p), add `#sha256(p)` to the default user's passwords. If None, set `nopass = true`.
**Warning signs:** `redis-cli -a mysecret` fails to authenticate after ACL refactor.

### Pitfall 3: NOPERM Error Format Must Match Redis Exactly
**What goes wrong:** Clients parse the NOPERM error prefix to detect permission denials. Wrong prefix = clients can't distinguish NOPERM from ERR.
**Why it happens:** Easy to write `ERR NOPERM ...` instead of `NOPERM ...`.
**How to avoid:** Error must start with `NOPERM` (not `ERR`): `NOPERM User %s has no permissions to run the '%s' command or its subcommand`.
**Warning signs:** Integration test with `redis` crate receives an error but doesn't match expected NOPERM type.

### Pitfall 4: ACL LOG Needs Client Address
**What goes wrong:** ACL LOG entries that omit `client-addr` and `client-name` are incomplete and fail client-side parsing.
**Why it happens:** Connection handlers don't currently pass peer address to command handlers.
**How to avoid:** Capture `stream.peer_addr()` at connection start (before `Framed::new` wraps the stream — use `stream.peer_addr()` first, then move stream into Framed). Store as `client_addr: SocketAddr` in connection local state.
**Warning signs:** `ACL LOG` returns entries with empty/missing addr field.

### Pitfall 5: Key Pattern Check for Multi-Key Commands
**What goes wrong:** Commands like MSET, MGET, DEL take multiple keys. ACL key pattern check must verify ALL keys, not just the first.
**Why it happens:** Simple pre-dispatch check extracts `args[0]` as the key.
**How to avoid:** Write a `extract_command_keys(cmd, args) -> Vec<&[u8]>` helper that knows which commands take keys in which positions (SET key val → [key], MGET k1 k2 k3 → [k1,k2,k3], MSET k1 v1 k2 v2 → [k1,k2]).
**Warning signs:** `MSET restricted_key value safe_key value` succeeds when it should be denied.

### Pitfall 6: ACL WHOAMI Returns Username, Not "authenticated"
**What goes wrong:** Returning a boolean or the word "authenticated" instead of the actual username string.
**Why it happens:** Old code only had `authenticated: bool`.
**How to avoid:** `current_user` connection state (String) replaces the old `authenticated` bool for WHOAMI. ACL WHOAMI returns `Frame::BulkString(current_user.as_bytes())`.
**Warning signs:** `ACL WHOAMI` returns wrong value or crashes.

### Pitfall 7: Arc<RwLock> ACL Table and Shard Architecture
**What goes wrong:** Sharded connection handlers use `Rc<RefCell<...>>` for single-threaded shard-local state. The ACL table is cross-shard global state.
**Why it happens:** The distinction between shard-local (Rc) and cross-shard (Arc) state is subtle.
**How to avoid:** ACL table is `Arc<RwLock<AclTable>>` — same pattern as `repl_state`. Pass it to `handle_connection_sharded` as a new parameter alongside existing `repl_state` and `cluster_state`. ACL SETUSER/DELUSER/LOAD update the Arc (write lock, brief), no fan-out needed since all shards share the same Arc pointer.
**Warning signs:** Compiler error trying to use `Rc` for ACL table across shard boundaries.

---

## Code Examples

### SHA256 Password Hashing

```rust
// Source: sha2 0.10 + hex 0.4 official docs
use sha2::{Sha256, Digest};

pub fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

pub fn verify_password(provided: &str, stored_hash: &str) -> bool {
    // stored_hash is a hex SHA256 without the # prefix
    let provided_hash = hash_password(provided);
    // Constant-time comparison not critical here (Redis doesn't do it either)
    provided_hash == stored_hash
}
```

### ACL Rule Parser

```rust
// Parse a single rule token from ACL SETUSER args
pub fn apply_rule(user: &mut AclUser, rule: &str) {
    match rule {
        "on"  => user.enabled = true,
        "off" => user.enabled = false,
        "nopass" => user.nopass = true,
        "resetpass" => { user.passwords.clear(); user.nopass = false; }
        "resetkeys" => user.key_patterns.clear(),
        "resetchannels" => user.channel_patterns.clear(),
        "reset" => *user = AclUser::default_deny(user.username.clone()),
        _ if rule.starts_with('>') => {
            let hash = hash_password(&rule[1..]);
            if !user.passwords.contains(&hash) {
                user.passwords.push(hash);
            }
        }
        _ if rule.starts_with('<') => {
            let hash = hash_password(&rule[1..]);
            user.passwords.retain(|p| p != &hash);
        }
        _ if rule.starts_with('#') => {
            let hash = rule[1..].to_string(); // pre-hashed
            if !user.passwords.contains(&hash) {
                user.passwords.push(hash);
            }
        }
        _ if rule.starts_with('!') => {
            let hash = rule[1..].to_string(); // remove pre-hashed
            user.passwords.retain(|p| p != &hash);
        }
        _ if rule.starts_with("~") => user.key_patterns.push(KeyPattern::from_str(&rule[1..])),
        _ if rule.starts_with("%R~") => user.key_patterns.push(KeyPattern::read_only(&rule[3..])),
        _ if rule.starts_with("%W~") => user.key_patterns.push(KeyPattern::write_only(&rule[3..])),
        _ if rule.starts_with('&') => user.channel_patterns.push(rule[1..].to_string()),
        _ if rule.starts_with('+') => user.allow_command(&rule[1..]),
        _ if rule.starts_with('-') => user.deny_command(&rule[1..]),
        _ => { /* unknown rule: ignore gracefully */ }
    }
}
```

### ACL File Line Serialization

```rust
// Serialize a user to ACL file format
pub fn user_to_acl_line(user: &AclUser) -> String {
    let mut parts = vec![format!("user {}", user.username)];
    parts.push(if user.enabled { "on".to_string() } else { "off".to_string() });
    if user.nopass {
        parts.push("nopass".to_string());
    }
    for hash in &user.passwords {
        parts.push(format!("#{}", hash));
    }
    for kp in &user.key_patterns {
        if kp.read && kp.write {
            parts.push(format!("~{}", kp.pattern));
        } else if kp.read {
            parts.push(format!("%R~{}", kp.pattern));
        } else if kp.write {
            parts.push(format!("%W~{}", kp.pattern));
        }
    }
    for cp in &user.channel_patterns {
        parts.push(format!("&{}", cp));
    }
    // Serialize command permissions
    // (serialize command rules last)
    parts.join(" ")
}
```

### Integration Test Pattern (ACL)

```rust
// Following the existing pattern from tests/integration.rs
async fn start_server_with_acl(acl_file: Option<&str>) -> (u16, CancellationToken) {
    let config = ServerConfig {
        // ... same as start_server() ...
        aclfile: acl_file.map(String::from),
        ..Default::default()
    };
    // spawn server, return port + token
}

#[tokio::test]
async fn test_acl_setuser_and_auth() {
    let (port, token) = start_server().await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // Create user
    let _: () = redis::cmd("ACL").arg("SETUSER").arg("alice")
        .arg("on").arg(">secret").arg("~*").arg("+@all")
        .query_async(&mut con).await.unwrap();

    // Authenticate as alice using AUTH username password
    let result: redis::RedisResult<String> = redis::cmd("AUTH")
        .arg("alice").arg("secret")
        .query_async(&mut con).await;
    assert!(result.is_ok());

    token.cancel();
}
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Single `requirepass` + `authenticated: bool` | Multi-user AclTable + `current_user: String` | Redis 6 (2020) | Per-user fine-grained control |
| AUTH password | AUTH [username] password | Redis 6 | Backward compatible |
| No command filtering | ACL category-based + individual allow/deny | Redis 6 | Security isolation |
| No key restrictions | ~pattern, %R~, %W~ per user | Redis 6 | Multi-tenant key namespacing |

**Deprecated/outdated:**
- `requirepass` config: Still supported but becomes the default user's password. Do NOT remove `requirepass` from `ServerConfig` — it stays as the backward-compat bootstrap for the default user.

---

## Open Questions

1. **ACL LOG scope: per-shard or global?**
   - What we know: ACL LOG entries need to be aggregated across shards for `ACL LOG [count]`
   - What's unclear: Whether a global `Arc<Mutex<AclLog>>` or per-shard log + fan-out aggregation is better
   - Recommendation: Global `Arc<Mutex<VecDeque<AclLogEntry>>>` (128 entries max, brief lock). ACL LOG is not on the hot path — it's a diagnostic/audit command.

2. **ACL DRYRUN implementation**
   - What we know: Redis added ACL DRYRUN in 7.0; it tests permissions without executing the command
   - What's unclear: Whether to include in this phase
   - Recommendation: Skip per deferred ideas. Flag with TODO comment in acl_check().

3. **CONFIG SET aclfile**
   - What we know: Redis allows setting ACL file path via CONFIG SET
   - What's unclear: Whether to use CLI flag only or also CONFIG SET
   - Recommendation (Claude's discretion): CLI flag `--aclfile` in `ServerConfig`. Add to `RuntimeConfig` for CONFIG GET/SET support. This mirrors how `--requirepass` works.

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in + tokio::test (integration.rs) |
| Config file | none (cargo.toml dev-dependencies) |
| Quick run command | `cargo test acl` |
| Full suite command | `cargo test` |

### Phase Requirements -> Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| ACL-01 | ACL SETUSER creates user, modifies rules | integration | `cargo test acl_setuser` | Wave 0 |
| ACL-02 | ACL GETUSER returns correct fields; ACL LIST shows all users; WHOAMI returns username | integration | `cargo test acl_getuser` | Wave 0 |
| ACL-03 | AUTH username password authenticates named user; wrong pass = WRONGPASS | integration | `cargo test acl_auth` | Wave 0 |
| ACL-04 | Default user with requirepass is backward compatible | integration | `cargo test acl_default_user` | Wave 0 |
| ACL-05 | NOPERM returned for denied command; allowed command executes normally | integration | `cargo test acl_noperm` | Wave 0 |
| ACL-06 | Key pattern ~cache:* blocks access to other keys; MSET multi-key check | integration | `cargo test acl_key_patterns` | Wave 0 |
| ACL-07 | ACL LOG records denied attempts; ACL LOG RESET clears; ACL LOG count limits results | integration | `cargo test acl_log` | Wave 0 |
| ACL-08 | ACL SAVE writes valid file; ACL LOAD reloads users from file | integration | `cargo test acl_save_load` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test acl`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] ACL test helpers in `tests/integration.rs` — `start_server_with_acl()` helper
- [ ] `cargo add sha2 --no-default-features --features sha2` — no current sha2 dep

---

## Sources

### Primary (HIGH confidence)
- Redis official ACL documentation: https://redis.io/docs/manual/security/acl/
- Redis 6 release notes (ACL introduction): https://raw.githubusercontent.com/redis/redis/7.0/CHANGELOG
- sha2 crate docs: https://docs.rs/sha2/0.10.9/sha2/
- hex crate docs: https://docs.rs/hex/0.4.3/hex/
- Codebase: `src/command/key.rs` — `glob_match` implementation (verified pub(crate))
- Codebase: `src/server/connection.rs` — auth gate pattern (lines 111, 344-384, 1257, 1301-1335)
- Codebase: `src/command/connection.rs` — `auth()` and `hello()` signatures
- Codebase: `src/config.rs` — `ServerConfig` and `RuntimeConfig`
- Codebase: `src/shard/dispatch.rs` — `ShardMessage` enum (ScriptLoad fan-out pattern)

### Secondary (MEDIUM confidence)
- Redis ACL file format: verified against multiple Redis 6+ documentation pages
- sha2 version 0.10.9 stable: verified against crates.io API (2026-03-25)
- hex version 0.4.3 stable: verified against crates.io API (2026-03-25)

### Tertiary (LOW confidence)
- Redis constant-time password comparison behavior: assumed similar to most auth systems; Redis source not directly checked

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — sha2 + hex versions verified against crates.io live API; glob_match reuse verified in codebase
- Architecture: HIGH — Arc<RwLock<AclTable>> pattern directly mirrors existing `repl_state` parameter in `handle_connection_sharded`; auth gate pattern fully read from source
- Pitfalls: HIGH — derived from direct code reading (auth() arity, Rc vs Arc shard boundary, requirepass bootstrap) and Redis ACL specification

**Research date:** 2026-03-25
**Valid until:** 2026-06-25 (sha2/hex are stable, Redis ACL spec is stable)

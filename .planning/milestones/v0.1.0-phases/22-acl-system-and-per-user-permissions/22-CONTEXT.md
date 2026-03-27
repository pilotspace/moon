# Phase 22: ACL System and Per-User Permissions - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Implement Redis 6+ ACL system with per-user command permissions, key pattern restrictions, and channel pattern restrictions. Replace the single-password AUTH with fine-grained access control. Users created/managed via ACL commands, persisted to ACL file, and enforced on every command execution.

</domain>

<decisions>
## Implementation Decisions

### User model
- [auto] Each user has: username, password(s), enabled/disabled flag, command permissions, key patterns, channel patterns
- Default user: backward compatible with single-password AUTH — `default` user with `~* +@all`
- Multiple passwords per user supported (SHA256 hashed, not stored in plaintext)
- User enabled/disabled toggle — disabled user cannot authenticate

### Command permissions
- [auto] Permission categories matching Redis:
  - `+@all` / `-@all` — all commands
  - `+@read`, `+@write`, `+@set`, `+@sortedset`, `+@hash`, `+@list`, `+@string`, etc.
  - `+command` / `-command` — individual command allow/deny
  - `+command|subcommand` — subcommand level (e.g., `+config|get -config|set`)
- Deny takes precedence over allow (most restrictive wins)
- Categories are predefined sets of commands (matching Redis's category taxonomy)

### Key pattern restrictions
- [auto] `~pattern` — allow access to keys matching glob pattern
- `~*` — all keys (default for unrestricted users)
- `%R~pattern` — read-only access to matching keys
- `%W~pattern` — write-only access to matching keys
- Multiple patterns allowed — key must match at least one

### Channel pattern restrictions
- [auto] `&pattern` — allow pub/sub access to channels matching pattern
- `&*` — all channels
- Enforced on SUBSCRIBE, PSUBSCRIBE, and PUBLISH commands

### ACL commands
- [auto] `ACL SETUSER username [rule ...]` — create or modify user
- `ACL GETUSER username` — get user's permissions
- `ACL DELUSER username [username ...]` — delete user(s)
- `ACL LIST` — list all users with full rule strings
- `ACL WHOAMI` — return current connection's username
- `ACL CAT [category]` — list command categories or commands in a category
- `ACL LOG [count|RESET]` — security log of denied commands
- `ACL SAVE` — persist ACL to file
- `ACL LOAD` — reload ACL from file

### AUTH integration
- [auto] `AUTH password` — authenticate as default user (backward compatible)
- `AUTH username password` — authenticate as specific user (RESP3 / Redis 6+)
- HELLO command's AUTH option uses the same mechanism
- Connection starts as default user; AUTH switches to specified user
- Failed auth increments ACL LOG entry

### Enforcement
- [auto] Every command checked against connection's current user before execution
- Check order: (1) user enabled? (2) command allowed? (3) key patterns match? (4) channel patterns match?
- NOPERM error returned for denied commands with descriptive message
- Enforcement is per-shard (each shard has read-only reference to ACL table)

### ACL persistence
- [auto] ACL file format matching Redis: one user per line, space-separated rules
- ACL SAVE writes to configured ACL file path
- ACL LOAD reloads and replaces in-memory ACL table
- On startup: load ACL file if configured, otherwise default user only

### Claude's Discretion
- ACL file path configuration (CLI flag or CONFIG SET)
- Password hashing: SHA256 or bcrypt (SHA256 matches Redis, simpler)
- ACL LOG max entries (default 128, circular buffer)
- Whether to implement ACL DRYRUN (test permissions without executing)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase dependencies
- `.planning/phases/16-resp3-protocol-support/16-CONTEXT.md` — RESP3 HELLO supports AUTH with username
- `.planning/phases/11-thread-per-core-shared-nothing-architecture/11-CONTEXT.md` — Per-shard ACL enforcement

### Current implementation
- `src/server/connection.rs` — AUTH handling (line ~connection auth gate) — extend for username-based auth
- `src/command/connection.rs` — AUTH command (346 lines) — extend for two-argument AUTH
- `src/config.rs` — ServerConfig with `requirepass` — add ACL configuration

### REQUIREMENTS.md
- v2 out-of-scope noted "Full ACL system — Disproportionate complexity" — now in scope for v2 roadmap

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- AUTH command handler — extend for username + password
- `requirepass` config — becomes the default user's password
- Connection `authenticated` flag — becomes `current_user: Option<String>`

### Established Patterns
- Per-connection state (authenticated, db_index) — add current_user
- Command dispatch pre-checks (auth gate) — add ACL permission gate

### Integration Points
- Command dispatch in connection.rs — add ACL check before every command execution
- CONFIG SET/GET — add ACL-related config parameters
- INFO command — add ACL section (users, denied commands count)
- Replication — ACL changes propagated to replicas

</code_context>

<specifics>
## Specific Ideas

- Default user ensures 100% backward compatibility — existing single-password setups work unchanged
- ACL categories match Redis exactly — clients that understand Redis ACL work out of the box
- ACL LOG is critical for security monitoring — shows denied attempts with timestamp, client, command

</specifics>

<deferred>
## Deferred Ideas

- ACL rules in CONFIG REWRITE — defer to when CONFIG REWRITE is implemented
- Pub/Sub channel ACL enforcement for PSUBSCRIBE patterns — complex pattern matching, implement basic first
- External ACL provider (LDAP, OAuth) — out of scope

</deferred>

---

*Phase: 22-acl-system-and-per-user-permissions*
*Context gathered: 2026-03-24*

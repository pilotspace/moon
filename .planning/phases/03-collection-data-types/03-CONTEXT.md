# Phase 3: Collection Data Types - Context

**Gathered:** 2026-03-23
**Status:** Ready for planning

<domain>
## Phase Boundary

All five Redis data types operational with full command coverage, plus active expiration (background sweep), cursor-based SCAN iteration, password authentication (AUTH), and non-blocking delete (UNLINK). This phase adds ~60 commands across Hash, List, Set, Sorted Set, SCAN family, AUTH, and UNLINK.

</domain>

<decisions>
## Implementation Decisions

### Data Structure Representations
- Extend `RedisValue` enum with 4 new variants:
  - `Hash(HashMap<Bytes, Bytes>)` — field-value pairs
  - `List(VecDeque<Bytes>)` — O(1) push/pop at both ends, better cache locality than LinkedList
  - `Set(HashSet<Bytes>)` — unique members
  - `SortedSet { members: HashMap<Bytes, f64>, scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()> }` — dual structure for O(log n) range queries + O(1) score lookup
- Type checking enforced: commands on wrong type return `WRONGTYPE Operation against a key holding the wrong kind of value` (matches Redis exactly)
- Each collection type gets its own command module file (command/hash.rs, command/list.rs, command/set.rs, command/sorted_set.rs)

### Sorted Set Design
- BTreeMap keyed by `(OrderedFloat<f64>, Bytes)` — BTreeMap provides ordered range queries by score
- HashMap<Bytes, f64> for O(1) member-to-score lookup (ZSCORE, ZRANK)
- Score ties broken by lexicographic ordering of member bytes — matches Redis behavior
- Accept +inf/-inf as valid scores, reject NaN with error — matches Redis
- ZRANGE with BYSCORE/BYLEX/REV/LIMIT uses BTreeMap range queries
- OrderedFloat from the `ordered-float` crate for total ordering (f64 doesn't impl Ord)

### Active Expiration Engine
- Tokio spawned background task running every 100ms
- Redis algorithm: sample 20 random keys from the expiring set, delete expired ones, repeat if >25% were expired (1ms budget per cycle)
- Random sampling: collect keys with TTL into a Vec, sample random indices using `rand` crate
- Rebuild sampling pool periodically (every N cycles) to account for new/removed TTL keys
- Background task holds `Arc<Mutex<Vec<Database>>>` — same shared state as connection handlers
- Task uses `tokio::time::interval` with graceful shutdown via the existing `CancellationToken`

### SCAN Cursor Strategy
- Stateless cursor — no server-side cursor storage
- Cursor is a position index; client resends it each call
- HashMap iteration order is non-deterministic but stable between rehashes
- SCAN may return duplicates, guarantees eventual completeness — matches Redis contract
- COUNT hint controls approximate batch size (default 10)
- MATCH pattern applied as post-filter on iterated keys
- TYPE filter (Redis 6.0+) applied as post-filter
- HSCAN/SSCAN/ZSCAN follow same pattern on collection internals

### AUTH Implementation
- Per-connection `authenticated: bool` field in Connection struct (default true if no password set)
- If `requirepass` is configured, all commands except AUTH return `-NOAUTH Authentication required.` until authenticated
- AUTH command checks password, sets `authenticated = true` on match
- Password stored in `ServerConfig` — single password, not ACL
- Add `requirepass: Option<String>` to ServerConfig and CLI args

### UNLINK (Async Delete)
- UNLINK removes key from HashMap immediately (O(1)) but defers value drop to a background task
- Spawn `tokio::task::spawn_blocking` to drop large values off the event loop
- For small values, behaves identically to DEL (not worth spawning a task)

### Claude's Discretion
- Threshold for UNLINK async vs sync drop (e.g., collections > 64 elements)
- Exact SCAN cursor encoding (integer index vs hash-based)
- Internal helper methods on Database for type-checked access
- Test organization for the 4 new command modules
- Whether to add `ordered-float` or implement a wrapper type

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Existing Server Code (built in Phase 2)
- `src/storage/entry.rs` — RedisValue enum (currently only String variant), Entry struct
- `src/storage/db.rs` — Database struct with HashMap<Bytes, Entry>, lazy expiration, get/set/remove
- `src/command/mod.rs` — Command dispatch with match on uppercase command name
- `src/command/string.rs` — Pattern for command handlers: pure functions `(&mut Database, &[Frame]) -> Frame`
- `src/command/key.rs` — Key management handlers including glob_match
- `src/server/connection.rs` — Connection struct, per-connection handler loop
- `src/server/shutdown.rs` — CancellationToken-based graceful shutdown
- `src/config.rs` — ServerConfig with CLI args

### Project Research
- `.planning/research/FEATURES.md` — Command lists by data type, dependency notes
- `.planning/research/ARCHITECTURE.md` — Approximated LRU, sorted set dual structure
- `.planning/research/PITFALLS.md` — Pitfall #3 (naive expiration), Pitfall #10 (sorted set performance cliff), Pitfall #1 (blocking event loop)

### Redis Command Reference
- No local spec — use official Redis command reference for exact command semantics

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `Database::get()` / `Database::get_mut()` — lazy expiration already wired in
- `Database::set()` / `Database::remove()` — key lifecycle operations
- `glob_match()` in command/key.rs — can be reused for KEYS/SCAN MATCH patterns
- Command dispatch pattern: add arms to match in `command/mod.rs`
- `Entry::new_string()` / `Entry::new_string_with_expiry()` — pattern for new Entry constructors
- `CancellationToken` in server/shutdown.rs — reuse for active expiration task shutdown
- `ServerConfig` with clap derive — extend for `requirepass`

### Established Patterns
- Command handlers: `pub fn cmd_name(db: &mut Database, args: &[Frame]) -> Frame`
- Error responses: `Frame::Error(Bytes::from_static(b"ERR ..."))`
- Argument extraction: pattern match on `Frame::BulkString`, validate arity
- `Bytes` for all string payloads, `std::time::Instant` for TTL
- Inline `#[cfg(test)] mod tests` in each module

### Integration Points
- `RedisValue` enum in entry.rs — add 4 new variants
- `command/mod.rs` dispatch — add ~60 new command routes
- `Connection` struct — add `authenticated` field for AUTH
- `ServerConfig` — add `requirepass` field
- `server/listener.rs` — spawn active expiration background task alongside TCP listener

</code_context>

<specifics>
## Specific Ideas

- Each data type should be testable in isolation via Database methods before wiring command handlers
- SCAN should work identically across all types (SCAN/HSCAN/SSCAN/ZSCAN) — shared cursor logic
- Active expiration task should log statistics (keys sampled, expired) via tracing for observability
- Integration tests should verify AUTH blocks commands and type errors are returned correctly

</specifics>

<deferred>
## Deferred Ideas

- Blocking commands (BLPOP/BRPOP/BZPOPMIN/BZPOPMAX) — Phase 5 or v2
- Memory-efficient encodings (ziplist/listpack equivalents) — v2
- Skip list replacement for BTreeMap in sorted sets — optimize if benchmarks warrant
- OBJECT ENCODING command — v2

</deferred>

---

*Phase: 03-collection-data-types*
*Context gathered: 2026-03-23*

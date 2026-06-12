# BUILD CONTEXT — consistency-dispatch-gaps (contract §3 FROZEN @ v2)

Read `.add/tasks/consistency-dispatch-gaps/TASK.md` in full first. §3 is the frozen
contract; §4 names the red suite. NEVER edit `tests/wire_reachability_red.rs`,
TASK.md, or any assertion to make the build pass — that inverts the method.

## Goal

Make `cargo test --test wire_reachability_red` go green (3 tests: cdg1 registry
sweep, cdg2 value asserts, cdg3 purity probes) WITHOUT touching the test file,
while keeping every existing test green and the hot path byte-identical.

## The four fixes (all empirically verified, see TASK.md §1)

### 1. 25 new `dispatch_read` arms — src/command/mod.rs (`dispatch_read_inner`)

Commands: XRANGE XREAD XREVRANGE XINFO XPENDING XLEN · ZDIFF ZINTER ZUNION
ZINTERCARD ZRANDMEMBER ZMSCORE · GEOPOS GEODIST GEOHASH GEOSEARCH · LCS ·
RANDOMKEY EXPIRETIME PEXPIRETIME TOUCH · LOLWUT TIME WAIT SLOWLOG.

Convention (copy the existing 62 arms): each arm calls a `*_readonly(db:
&Database, args: &[Frame], now_ms: u64) -> Frame` twin in the command's own
module. Twins use `db.get_if_alive(key, now_ms)` /
`db.get_sorted_set_if_alive(...)` (db.rs:1669) — NEVER `db.get()` (lazy-expires,
needs &mut). Expired keys are invisible (missing-form reply), never deleted.
DO NOT modify any existing `&mut Database` impl's behavior — extract a shared
core fn where the body is big (LCS DP, GEOSEARCH, XREAD, XPENDING, zset setops
via a `collect_source_sets_readonly` twin of sorted_set_read.rs's
`collect_source_sets`), or write a small standalone twin where it's tiny (XLEN).

Per-command notes:
- Streams (stream/stream_read.rs): Database has NO &self stream accessor — add
  `get_stream_if_alive(&self, key, now_ms) -> Result<Option<&StreamData>, Frame>`
  to src/storage/db.rs next to get_stream (db.rs:1875), built on get_if_alive
  (WRONGTYPE on non-stream, like get_stream). XREAD arm is NON-blocking: BLOCK
  parsed-and-ignored exactly like today's `xread` (stream_read.rs:144).
- TOUCH: count of alive keys (get_if_alive per key). Read twin must NOT update
  access metadata (contract: no LRU write under shared lock).
- EXPIRETIME/PEXPIRETIME: -2 missing/expired, -1 no TTL, else absolute secs/ms.
  Must match the &mut twins' replies byte-for-byte on live keys.
- RANDOMKEY: must return only ALIVE keys; with an all-expired keyspace return
  Null (cdg3 asserts this). One pass, reservoir-sample alive keys (O(1) memory)
  or mirror whatever &mut randomkey (key.rs:341) does minus the expiry delete.
- LOLWUT / TIME (key.rs:373 `time()`, takes no db): db-independent — reuse the
  same fn from the read arm.
- WAIT: reply Integer(0) (no replication). Mirror handler_single.rs:819's reply.
- SLOWLOG: arm calls `crate::admin::slowlog::handle_slowlog(
  crate::admin::metrics_setup::global_slowlog(), args)` exactly like the
  dispatch arm at mod.rs:692. SLOWLOG RESET mutating the global ring is a NAMED
  contract exception (it is internally synchronized, never touches Database).
- GEO commands store as zset — twins via get_sorted_set_if_alive; replies must
  match the &mut twins on live keys (GEODIST float formatting etc.).

### 2. `is_dispatch_read_supported` buckets — src/command/mod.rs (~line 990)

Add (len, first-byte-lowercased) buckets for all 25. Invariant: gate(cmd)==true
⇒ dispatch_read has an arm (the in-file unit test `test_dispatch_read_parity_
with_prefilter` and the wire sweep pin this). The OTHER in-file unit test
(~mod.rs:1935) currently asserts b"WAIT" and b"RANDOMKEY" are NOT supported —
that pins the PRE-fix gate; MOVING those entries to the supported side is
contract-mandated (frozen contract v2 adds them), not test-weakening. The
frozen suite is tests/wire_reachability_red.rs ONLY — never touch that one.

### 3. `extract_primary_key` routing — src/server/conn/shared.rs:244

Follow the OBJECT precedent at shared.rs:289 (comment included):
- XGROUP, XINFO: key is args[1] (subcommand-first). XINFO HELP / missing args →
  None (execute locally), like OBJECT HELP.
- ZDIFF, ZINTER, ZUNION, ZINTERCARD: key is args[1] (numkeys-first; args[0] is
  the numkeys literal — hashing "2" routes to an arbitrary shard).
- XREAD: first key AFTER the STREAMS token (scan args case-insensitively);
  no STREAMS → None.
- Everything else byte-identical. This function is hot-path: no allocation, no
  to_vec, only slice scans.

### 4. CDC.READ monoio port + script fixes

- handler_monoio (src/server/conn/handler_monoio/mod.rs): add a CDC.READ
  special case mirroring `try_handle_slowlog`-style placement of
  handler_sharded/dispatch.rs:239's CDC.READ handling (same backend fn, same
  reply shape). It reads WAL files from a path argument; it must NOT hold any
  Database lock while doing file IO.
- scripts/test-consistency.sh: (a) the SETRANGE case currently sends SETRANGE
  only to moon then compares GETs — send it to BOTH servers via the script's
  `both` helper; (b) the moon-only FT.CREATE uses `VECTOR FLAT 6` — Moon is
  HNSW-only; change to HNSW. NO other script line may change.

## Hard rules (from CLAUDE.md + frozen contract)

- No allocations on hot paths (command dispatch, protocol parsing): no
  Box::new/Vec::new/format!/to_string/clone in dispatch match arms themselves;
  Vec::with_capacity for result building inside command impls is fine.
- No new `unsafe`. No unwrap/expect outside tests. parking_lot only (n/a here).
- The existing 62 read arms and the &mut dispatch arms: byte-identical replies,
  unchanged order (M6 / fastpath_regression reject).
- Both runtimes must compile: `cargo check` AND
  `cargo check --no-default-features --features runtime-tokio,jemalloc`.
- `cargo fmt` + `cargo clippy -- -D warnings` clean (default features), plus
  clippy on the tokio feature set.

## Verification sequence (run all locally, macOS)

1. `cargo test --test wire_reachability_red` → 3/3 green.
2. `cargo test --lib command::` (or the full `cargo test --lib`) → green.
3. `cargo check --no-default-features --features runtime-tokio,jemalloc`.
4. `cargo clippy -- -D warnings` and
   `cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings`.
5. `cargo fmt`.

Do NOT commit — the orchestrator reviews the diff and commits.
Write a summary of what you changed (files, line counts, any deviation or named
exception) to `.add/tasks/consistency-dispatch-gaps/BUILD-SUMMARY.md`.

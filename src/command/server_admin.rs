//! Server administration commands: FLUSHALL, FLUSHDB, DEBUG, MEMORY USAGE,
//! VACUUM, and DEBUG RECLAMATION.
//!
//! These are routed through the main `dispatch()` function (keyless, broadcast
//! to all shards via the console gateway). The handlers operate on the
//! per-shard `Database` passed in by the event loop.
//!
//! ## Semantics
//!
//! * `FLUSHDB` clears the currently-selected database on this shard.
//! * `FLUSHALL` clears EVERY database on this shard; the console gateway
//!   broadcasts the command to every shard, so one client call empties the
//!   whole keyspace. `flushall` below can only reach the one database
//!   `dispatch` hands it, so the other databases are cleared by
//!   [`flush_every_database`] / [`flush_every_database_locked`], called from
//!   the sites that hold the full set. Until moon#677 that second half did
//!   not exist and `FLUSHALL` was a synonym for `FLUSHDB`.
//! * `DEBUG OBJECT <key>` returns the redis-compatible one-line summary
//!   (`Value at:0x0 refcount:N encoding:... serializedlength:N lru:0 lru_seconds_idle:0`)
//!   so downstream tooling (redis-cli, RDBTools, Prometheus exporters) can
//!   parse the encoding + length fields without changes.
//! * `DEBUG SLEEP <seconds>` blocks the current shard thread for up to 30s,
//!   which is deliberate: it unlocks slowlog-testing and deterministic
//!   latency benchmarks. Capped to prevent accidental DoS on ACL-less builds.
//! * `MEMORY USAGE <key> [SAMPLES n]` returns a conservative estimate of
//!   bytes consumed by the entry. SAMPLES is accepted and ignored (Moon
//!   always walks the entire value — there is no probabilistic sampling).
//! * `VACUUM [FILES | (VERBOSE) | (FREEZE) | VECTOR <idx> | GRAPH <name>]` —
//!   manual reclamation passes across manifest, MVCC, and WAL subsystems.
//!   Returns counts of resources reclaimed per subsystem. Postgres-style manual
//!   escape hatch shipped before autovacuum (Wave 2 P2/P7).
//! * `DEBUG RECLAMATION` — verbose per-subsystem diagnostic dump.

use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::command::helpers::{err_wrong_args, extract_bytes};
use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::Entry;

// Type aliases to avoid long paths in function signatures.
type ShardManifest = crate::persistence::manifest::ShardManifest;
type WalWriterV3 = crate::persistence::wal_v3::segment::WalWriterV3;

/// Default MVCC committed-prune margin used when VACUUM is dispatched through
/// a connection handler that does not have access to `ServerConfig`.
/// Matches the `mvcc_committed_prune_margin` config default (1000).
///
/// Used by handler_single, handler_sharded, and handler_monoio. The spsc_handler
/// path reads the real value from `server_config.mvcc_committed_prune_margin`.
pub const DEFAULT_VACUUM_PRUNE_MARGIN: u64 = 1000;

// ---------------------------------------------------------------------------
// FLUSHDB / FLUSHALL
// ---------------------------------------------------------------------------

/// `FLUSHDB [ASYNC|SYNC]`
///
/// Clears the currently-selected database. ASYNC/SYNC are accepted for
/// compatibility but treated identically (Moon has no background flush).
pub fn flushdb(db: &mut Database, args: &[Frame]) -> Frame {
    if !check_flush_args(args) {
        return Frame::Error(Bytes::from_static(b"ERR syntax error"));
    }
    db.clear();
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// `FLUSHALL [ASYNC|SYNC]`
///
/// Clears the currently-selected database on this shard. The console gateway
/// broadcasts the command to every shard, so a single client call fans out to
/// the full cluster.
///
/// This is only half of `FLUSHALL`. `dispatch` hands every command a single
/// `&mut Database`, so this entry point cannot express "every database" no
/// matter what it does — the caller completes it with
/// [`flush_every_database`] (or [`flush_every_database_locked`]), which is
/// the only place the whole set is in scope. A caller that forgets makes
/// `FLUSHALL` a synonym for `FLUSHDB`, which is exactly what moon#677 was.
pub fn flushall(db: &mut Database, args: &[Frame]) -> Frame {
    if !check_flush_args(args) {
        return Frame::Error(Bytes::from_static(b"ERR syntax error"));
    }
    db.clear();
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// moon#677: the rest of `FLUSHALL` — clear the databases `flushall` above
/// could not see.
///
/// `selected` is the database `dispatch` already cleared, so it is skipped.
/// Re-clearing it would be harmless, but skipping keeps one flush to one
/// `record_keyspace_change` per database.
///
/// `Database::clear` is what makes this complete rather than cosmetic: it
/// drops the cold-tier index and the in-flight spill record as well as the
/// hot table, so a flushed key cannot come back through a read-through or a
/// spill that lands after the flush.
pub fn flush_every_database(databases: &mut [Database], selected: usize) {
    for (idx, database) in databases.iter_mut().enumerate() {
        if idx != selected {
            database.clear();
        }
    }
}

/// Same, for the handler that owns `Vec<RwLock<Database>>` rather than a
/// shard-local slice.
///
/// Takes **one lock at a time** and never holds two. That is not incidental:
/// `MOVE` and `COPY ... DB n` hold two database locks at once, so a helper
/// that held `selected` while reaching for another index could sit on the
/// other side of that pair and deadlock. The caller therefore drops its own
/// guard first and this loop clears every database including `selected` —
/// which also means the result does not depend on `flushall` having run.
pub fn flush_every_database_locked(databases: &[parking_lot::RwLock<Database>]) {
    for database in databases {
        database.write().clear();
    }
}

fn check_flush_args(args: &[Frame]) -> bool {
    match args.len() {
        0 => true,
        1 => match extract_bytes(&args[0]) {
            Some(s) => s.eq_ignore_ascii_case(b"ASYNC") || s.eq_ignore_ascii_case(b"SYNC"),
            None => false,
        },
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// HOTKEYS
// ---------------------------------------------------------------------------

/// `HOTKEYS [COUNT n]` — top sampled keys on this database (Moon extension).
///
/// Returns an array of `[key, sampled_count]` pairs sorted by count
/// descending. Counts are 1-in-64 samples of keyed commands; multiply by 64
/// for an approximate command rate. In multi-shard mode the connection
/// handler merges per-shard results, so clients always see the global view.
pub fn hotkeys(db: &Database, args: &[Frame]) -> Frame {
    let count = match parse_hotkeys_count(args) {
        Ok(n) => n,
        Err(e) => return e,
    };
    let top = db.hot_keys().top(count);
    let mut out: Vec<Frame> = Vec::with_capacity(top.len());
    for (key, sampled) in top {
        out.push(Frame::Array(framevec![
            Frame::BulkString(key),
            Frame::Integer(sampled as i64),
        ]));
    }
    Frame::Array(out.into())
}

/// Parse `HOTKEYS [COUNT n]` arguments. Shared by the local command and the
/// cross-shard coordinator path so both reject identically.
pub fn parse_hotkeys_count(args: &[Frame]) -> Result<usize, Frame> {
    match args {
        [] => Ok(crate::storage::hotkey::HOTKEY_DEFAULT_COUNT),
        [subcmd, n] => {
            let valid = extract_bytes(subcmd).is_some_and(|s| s.eq_ignore_ascii_case(b"COUNT"));
            if !valid {
                return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
            }
            match extract_bytes(n).and_then(|s| atoi::atoi::<usize>(s)) {
                Some(n) if (1..=crate::storage::hotkey::HOTKEY_CAPACITY).contains(&n) => Ok(n),
                _ => Err(Frame::Error(Bytes::from_static(
                    b"ERR COUNT must be an integer between 1 and 128",
                ))),
            }
        }
        _ => Err(err_wrong_args("HOTKEYS")),
    }
}

// ---------------------------------------------------------------------------
// DEBUG OBJECT / SLEEP / HELP
// ---------------------------------------------------------------------------

/// `DEBUG <subcommand> [args...]`
///
/// Supported subcommands:
/// * `OBJECT <key>` — encoding/refcount/serializedlength summary.
/// * `SLEEP <seconds>` — blocking sleep on the current shard (0..=30s).
/// * `HELP` — list subcommands.
pub fn debug(db: &mut Database, args: &[Frame]) -> Frame {
    match classify_debug(args) {
        Ok(DebugCall::Object(rest)) => debug_object(db, rest),
        Ok(DebugCall::Sleep(rest)) => debug_sleep(rest),
        Ok(DebugCall::Panic) => debug_panic(),
        Ok(DebugCall::Help) => debug_help(),
        Err(e) => e,
    }
}

/// Read-only variant used by `dispatch_read()` on the shared-read path.
///
/// DEBUG is flagged as ADMIN (not WRITE and not READONLY) which steers the
/// connection handler into the read-dispatch branch. None of the supported
/// subcommands mutate `Database`, so exposing a `&Database` overload here
/// keeps the command working without forcing a WRITE reclassification
/// (which would incorrectly AOF-log DEBUG SLEEP).
pub fn debug_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    match classify_debug(args) {
        Ok(DebugCall::Object(rest)) => debug_object_readonly(db, rest, now_ms),
        Ok(DebugCall::Sleep(rest)) => debug_sleep(rest),
        Ok(DebugCall::Panic) => debug_panic(),
        Ok(DebugCall::Help) => debug_help(),
        Err(e) => e,
    }
}

enum DebugCall<'a> {
    Object(&'a [Frame]),
    Sleep(&'a [Frame]),
    Panic,
    Help,
}

fn classify_debug(args: &[Frame]) -> Result<DebugCall<'_>, Frame> {
    if args.is_empty() {
        return Err(err_wrong_args("DEBUG"));
    }
    let sub = match extract_bytes(&args[0]) {
        Some(s) => s,
        None => return Err(err_wrong_args("DEBUG")),
    };
    if sub.eq_ignore_ascii_case(b"OBJECT") {
        Ok(DebugCall::Object(&args[1..]))
    } else if sub.eq_ignore_ascii_case(b"SLEEP") {
        Ok(DebugCall::Sleep(&args[1..]))
    } else if sub.eq_ignore_ascii_case(b"PANIC") {
        Ok(DebugCall::Panic)
    } else if sub.eq_ignore_ascii_case(b"HELP") {
        Ok(DebugCall::Help)
    } else if sub.eq_ignore_ascii_case(b"DIGEST") {
        // Recognised here ONLY so this path can refuse it loudly. The real
        // implementation is an intercept that can reach every database and
        // every shard; see `digest_off_path_error`.
        Err(digest_off_path_error())
    } else {
        Err(Frame::Error(Bytes::from(format!(
            "ERR DEBUG subcommand '{}' not supported",
            String::from_utf8_lossy(sub),
        ))))
    }
}

/// `DEBUG DIGEST` reached the ordinary command path instead of its intercept.
///
/// Fail LOUD rather than answering from the single database this path can
/// see. A digest over one db of one shard is forty perfectly plausible hex
/// characters that match nothing: a harness comparing two servers would report
/// a difference that is not there, with nothing in the reply to hint the
/// answer was partial. An error is recoverable; a confident wrong digest is
/// not.
///
/// In practice this is reached from the contexts that execute commands INLINE
/// and so cannot call an intercept — inside `MULTI`/`EXEC`, and from Lua.
/// Redis serves `DEBUG DIGEST` in both; moon does not, and the message says so
/// rather than blaming the caller or inviting a bug report for a known
/// limitation. Verified against redis 8.6.1: it answers inside MULTI.
fn digest_off_path_error() -> Frame {
    // ASCII only: this is a RESP simple error, which may contain no CR or LF
    // and is conventionally one plain line.
    Frame::Error(Bytes::from_static(
        b"ERR DEBUG DIGEST spans every database and shard and cannot be served \
          from an inline context; it is unavailable inside MULTI/EXEC and Lua. \
          Send it as a top-level command.",
    ))
}

/// This shard's UNFINALISED per-db partials, for the cross-shard fan-out.
///
/// Deliberately not a digest: finalising per shard would fold the db index in
/// once per shard instead of once per server, and the parts could no longer be
/// combined.
pub fn debug_digest_shard_partials() -> Frame {
    crate::command::debug_digest::partials_to_frame(&crate::command::debug_digest::local_partials())
}

fn debug_help() -> Frame {
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from_static(b"DEBUG OBJECT <key>")),
        Frame::BulkString(Bytes::from_static(
            b"  Show low-level info about a key's object.",
        )),
        Frame::BulkString(Bytes::from_static(b"DEBUG SLEEP <seconds>")),
        Frame::BulkString(Bytes::from_static(
            b"  Stall this shard for <seconds> (float, capped at 30).",
        )),
        Frame::BulkString(Bytes::from_static(b"DEBUG PANIC")),
        Frame::BulkString(Bytes::from_static(
            b"  Panic this shard thread (crash-handling test aid, as in Redis).",
        )),
        Frame::BulkString(Bytes::from_static(b"DEBUG DIGEST")),
        Frame::BulkString(Bytes::from_static(
            b"  SHA1 fingerprint of the whole dataset, every database and \
              shard. Byte-compatible with Redis, so two servers can be \
              compared in one round trip. Not available inside MULTI or Lua.",
        )),
        Frame::BulkString(Bytes::from_static(b"DEBUG HELP")),
        Frame::BulkString(Bytes::from_static(b"  Return subcommand help.")),
    ])
}

fn debug_object(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("DEBUG OBJECT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("DEBUG OBJECT"),
    };
    match db.get(key.as_ref()) {
        Some(entry) => debug_object_reply(entry),
        None => Frame::Error(Bytes::from_static(b"ERR no such key")),
    }
}

fn debug_object_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("DEBUG OBJECT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("DEBUG OBJECT"),
    };
    match db.get_if_alive_any_plane(key.as_ref(), now_ms) {
        Some(entry) => debug_object_reply(entry.entry()),
        None => Frame::Error(Bytes::from_static(b"ERR no such key")),
    }
}

fn debug_object_reply(entry: &Entry) -> Frame {
    let encoding = entry.as_redis_value().encoding_name();
    let slen = estimate_serialized_length(entry);
    // Redis format: Value at:0x<addr> refcount:N encoding:X serializedlength:N lru:N lru_seconds_idle:N
    // Tools parse the key/value pairs; the exact address is not meaningful.
    let body = format!(
        "Value at:0x0000000000000000 refcount:1 encoding:{} serializedlength:{} lru:0 lru_seconds_idle:0",
        encoding, slen,
    );
    Frame::SimpleString(Bytes::from(body))
}

/// `DEBUG PANIC` — deliberately panic the executing shard thread (Redis
/// parity: a crash-handling test aid). The process-level panic policy
/// (fail-fast abort, installed in main) is what a client observes; the
/// return type exists only for the signature.
fn debug_panic() -> Frame {
    panic!("DEBUG PANIC requested by client");
}

fn debug_sleep(args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("DEBUG SLEEP");
    }
    let secs_bytes = match extract_bytes(&args[0]) {
        Some(b) => b,
        None => return err_wrong_args("DEBUG SLEEP"),
    };
    let secs: f64 = match std::str::from_utf8(secs_bytes.as_ref())
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(v) if v.is_finite() && v >= 0.0 => v,
        _ => return Frame::Error(Bytes::from_static(b"ERR value is not a valid float")),
    };
    // Dispatch runs on the shard event loop; blocking sleep stalls that
    // shard only (by design — tests rely on this to populate slowlog). The
    // upper bound is 30s to avoid accidental DoS when ACLs are not used.
    let millis = (secs.min(30.0) * 1000.0) as u64;
    if millis > 0 {
        std::thread::sleep(std::time::Duration::from_millis(millis));
    }
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// Conservative byte estimate of an entry's payload, excluding per-key
/// accounting overhead (entry header, compact key bytes).
fn estimate_serialized_length(entry: &Entry) -> usize {
    match entry.as_redis_value() {
        RedisValueRef::String(s) => s.len(),
        RedisValueRef::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len() + 2).sum(),
        RedisValueRef::HashWithTtl { fields, ttls, .. } => {
            let f: usize = fields.iter().map(|(k, v)| k.len() + v.len() + 2).sum();
            let t: usize = ttls.iter().map(|(k, _)| k.len() + 8).sum();
            f + t
        }
        RedisValueRef::HashListpack(lp) => lp.total_bytes(),
        RedisValueRef::List(lst) => lst.iter().map(|e| e.len() + 1).sum(),
        RedisValueRef::ListListpack(lp) => lp.total_bytes(),
        RedisValueRef::Set(s) => s.iter().map(|m| m.len() + 1).sum(),
        RedisValueRef::SetListpack(lp) => lp.total_bytes(),
        RedisValueRef::SetIntset(is) => is.len() * 8,
        RedisValueRef::SortedSet { members, .. } => members.iter().map(|(m, _)| m.len() + 9).sum(),
        RedisValueRef::SortedSetBPTree { members, .. } => {
            members.iter().map(|(m, _)| m.len() + 9).sum()
        }
        RedisValueRef::SortedSetListpack(lp) => lp.total_bytes(),
        // Streams track their own size; the header is an acceptable lower
        // bound for tooling — `XINFO STREAM` gives a richer picture.
        RedisValueRef::Stream(_) => 64,
    }
}

// ---------------------------------------------------------------------------
// MEMORY USAGE / STATS / DOCTOR / HELP
// ---------------------------------------------------------------------------

/// `MEMORY <subcommand> [args...]`
pub fn memory(db: &mut Database, args: &[Frame]) -> Frame {
    match classify_memory(args) {
        Ok(MemoryCall::Usage(rest)) => memory_usage(db, rest),
        Ok(MemoryCall::Stats) => memory_stats(db.estimated_memory()),
        Ok(MemoryCall::Doctor) => memory_doctor(),
        Ok(MemoryCall::Help) => memory_help(),
        Err(e) => e,
    }
}

/// Read-only variant routed from `dispatch_read()`.
pub fn memory_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    match classify_memory(args) {
        Ok(MemoryCall::Usage(rest)) => memory_usage_readonly(db, rest, now_ms),
        Ok(MemoryCall::Stats) => memory_stats(db.estimated_memory()),
        Ok(MemoryCall::Doctor) => memory_doctor(),
        Ok(MemoryCall::Help) => memory_help(),
        Err(e) => e,
    }
}

enum MemoryCall<'a> {
    Usage(&'a [Frame]),
    Stats,
    Doctor,
    Help,
}

fn classify_memory(args: &[Frame]) -> Result<MemoryCall<'_>, Frame> {
    if args.is_empty() {
        return Err(err_wrong_args("MEMORY"));
    }
    let sub = match extract_bytes(&args[0]) {
        Some(s) => s,
        None => return Err(err_wrong_args("MEMORY")),
    };
    if sub.eq_ignore_ascii_case(b"USAGE") {
        Ok(MemoryCall::Usage(&args[1..]))
    } else if sub.eq_ignore_ascii_case(b"STATS") {
        Ok(MemoryCall::Stats)
    } else if sub.eq_ignore_ascii_case(b"DOCTOR") {
        Ok(MemoryCall::Doctor)
    } else if sub.eq_ignore_ascii_case(b"HELP") {
        Ok(MemoryCall::Help)
    } else {
        Err(Frame::Error(Bytes::from(format!(
            "ERR MEMORY subcommand '{}' not supported",
            String::from_utf8_lossy(sub),
        ))))
    }
}

fn memory_stats(used: usize) -> Frame {
    // Note: this reports the current estimated memory usage for the shard's
    // databases, NOT a peak/high-water mark. We don't track peak on the hot
    // path to avoid atomic overhead per operation. Naming mirrors Redis's
    // `used_memory` key from MEMORY STATS (not `peak.allocated`).
    Frame::Map(vec![(
        Frame::BulkString(Bytes::from_static(b"used_memory")),
        Frame::Integer(used as i64),
    )])
}

fn memory_doctor() -> Frame {
    use std::fmt::Write;

    let rss = crate::admin::metrics_setup::get_rss_bytes() as usize;
    let vsz = get_vsz_bytes();

    // ── Gather per-subsystem resident bytes (C5 / M4 — lock-free atomics) ──
    // KV and store memory are read from published per-shard atomics. Figures
    // lag at most one 100ms tick — acceptable for an on-demand diagnostic.
    use std::sync::atomic::Ordering;
    let dashtable_bytes: usize;
    let hnsw_bytes: usize;
    let sealed_bytes: usize = 0; // combined into hnsw_bytes from vector atomic
    #[cfg_attr(not(feature = "graph"), allow(unused_variables))]
    let csr_bytes: usize;
    let wal_bytes: usize = 0;
    let lua_bytes: usize;
    // K4 (kernel-m2-brief-2026-07-12 stage 2): text (FTS) resident bytes,
    // previously hard-coded 0 at the publish site.
    let text_bytes: usize;

    if let Some(shard_dbs) = crate::admin::metrics_setup::get_global_shard_databases() {
        // KV memory: sum of per-shard published atomics. Lock-free.
        dashtable_bytes = shard_dbs.read_memory_sum();

        // Store memory: sum published per-shard vector/text/graph atomics.
        let mut vec_total = 0usize;
        let mut text_total = 0usize;
        let mut csr_total = 0usize;
        let mut lua_total = 0usize;
        for mem in shard_dbs.store_memory_per_shard.iter() {
            vec_total += mem.vector.load(Ordering::Relaxed);
            text_total += mem.text.load(Ordering::Relaxed);
            csr_total += mem.graph.load(Ordering::Relaxed);
            // C4 (wave-5 hygiene): Lua script-cache byte estimate, plus
            // (moon#506) the `mlua` interpreter heap — ~25KB/shard the doctor
            // previously charged to allocator overhead instead of to Lua.
            lua_total += mem.lua.load(Ordering::Relaxed) + mem.lua_vm.load(Ordering::Relaxed);
        }
        hnsw_bytes = vec_total;
        text_bytes = text_total;
        csr_bytes = csr_total;
        lua_bytes = lua_total;
    } else {
        dashtable_bytes = 0;
        hnsw_bytes = 0;
        text_bytes = 0;
        csr_bytes = 0;
        lua_bytes = 0;
    }

    // Replication backlog via global state (same pattern as INFO replication).
    let repl_bytes = replication_backlog_bytes();

    // WAL writers live on event-loop stacks, not accessible from command path.
    // Report 0 with stable label — operators see the label exists.

    // ── Allocator metadata ───────────────────────────────────────────────
    let (allocator_name, arena_count) = allocator_info();

    // ── Computed overhead ────────────────────────────────────────────────
    let tracked_sum = dashtable_bytes
        + hnsw_bytes
        + text_bytes
        + csr_bytes
        + wal_bytes
        + sealed_bytes
        + repl_bytes
        + lua_bytes;
    let allocator_overhead = rss.saturating_sub(tracked_sum);

    // Task #56 (used_memory truthfulness) + adversarial-review finding #3
    // (parity delta): three figures, not two, now that `used_memory` and
    // the eviction gate have deliberately diverged:
    //
    //   1. `elastic_budget_bytes` -- KV (+ ColdIndex) + vector + text +
    //      graph, the EXACT terms `ShardDatabases::recompute_elastic_budget`
    //      gates `--maxmemory` eviction on. Nothing else is ever evicted to
    //      make room.
    //   2. `used_memory_reported` -- what `INFO`'s `used_memory` field and
    //      the `moon_used_memory_bytes` gauge actually report: the elastic
    //      budget PLUS the Lua script cache and replication backlog, to
    //      match real Redis's `used_memory` semantics (it also counts
    //      script cache + backlog as allocator-attributed memory, even
    //      though neither is evictable data). See
    //      `admin::metrics_setup::logical_used_memory_bytes`'s doc comment
    //      for the full reasoning.
    //   3. `rss` -- true OS-level footprint: adds allocator overhead, page
    //      cache, WAL writer buffers, sealed segments, the binary image,
    //      and thread stacks on top of (2).
    //
    // Called out here explicitly because conflating (1)/(2) with (3) is
    // exactly what made a healthy disk-offload deployment look permanently
    // over-budget in the original G2 acceptance run.
    let elastic_budget_bytes = dashtable_bytes + hnsw_bytes + text_bytes + csr_bytes;
    let used_memory_reported = elastic_budget_bytes + lua_bytes + repl_bytes;
    let outside_cap_bytes = rss.saturating_sub(used_memory_reported);

    // ── VSZ ratio recommendation ─────────────────────────────────────────
    let vsz_ratio = if rss > 0 { vsz / rss } else { 0 };
    let vsz_recommendation = if vsz_ratio > 100 {
        format!("VSZ-vs-RSS ratio is {vsz_ratio}x (high -- consider --memory-arenas-cap 8)")
    } else {
        format!("VSZ-vs-RSS ratio is {vsz_ratio}x (normal)")
    };

    // Check if any single kind exceeds 50% of RSS.
    let half_rss = rss / 2;
    let resident_recommendation = if dashtable_bytes > half_rss {
        "DashTable dominates RSS (>50%). Consider increasing --initial-keyspace-hint to reduce segment splits."
    } else if hnsw_bytes > half_rss {
        "HNSW (vector) dominates RSS (>50%). Consider compacting (FT.COMPACT) or reducing ef_construction."
    } else if text_bytes > half_rss {
        "Text (FTS) dominates RSS (>50%). Consider FT.COMPACT to build FST sidecars, or reviewing indexed field cardinality."
    } else if csr_bytes > half_rss {
        "CSR (graph) dominates RSS (>50%). Review graph index sizes."
    } else if allocator_overhead > half_rss {
        "Allocator overhead dominates RSS (>50%). Possible fragmentation -- consider MEMORY PURGE or restart."
    } else {
        "No issues detected in resident memory."
    };

    // ── Format output ────────────────────────────────────────────────────
    let now = chrono_iso8601_now();
    let mut out = String::with_capacity(1024);

    let _ = writeln!(out, "Sample of Moon memory usage at {now}");
    let _ = writeln!(out);
    let _ = writeln!(out, "Memory accounting (task #56):");
    let _ = writeln!(
        out,
        "  used_memory (INFO / moon_used_memory_bytes): {}  -- elastic budget \
         + Lua (VM + script cache) + replication backlog (Redis parity)",
        humanize_bytes(used_memory_reported)
    );
    let _ = writeln!(
        out,
        "  elastic budget (gated by --maxmemory):       {}  -- KV+ColdIndex+vector+text+graph \
         only; the exact terms eviction acts on",
        humanize_bytes(elastic_budget_bytes)
    );
    let _ = writeln!(
        out,
        "  used_memory_rss (process footprint):         {}",
        humanize_bytes(rss)
    );
    let _ = writeln!(
        out,
        "  outside used_memory (RSS - used_memory):     {}  -- allocator overhead, \
         page cache, WAL writer buffers, sealed segments, binary+stacks",
        humanize_bytes(outside_cap_bytes)
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "Process:");
    let _ = writeln!(out, "  RSS:                    {}", humanize_bytes(rss));
    let _ = writeln!(out, "  VSZ:                    {}", humanize_bytes(vsz));
    let _ = writeln!(out, "  Allocator:              {allocator_name}");
    let _ = writeln!(out, "  Arenas:                 {arena_count}");
    let _ = writeln!(out);
    let _ = writeln!(out, "Per-subsystem (resident):");
    let _ = writeln!(
        out,
        "  DashTable + entries:    {}  ({:.1}%)",
        humanize_bytes(dashtable_bytes),
        pct(dashtable_bytes, rss)
    );
    let _ = writeln!(
        out,
        "  HNSW (vector):          {}  ({:.1}%)",
        humanize_bytes(hnsw_bytes),
        pct(hnsw_bytes, rss)
    );
    let _ = writeln!(
        out,
        "  Text (FTS):             {}  ({:.1}%)",
        humanize_bytes(text_bytes),
        pct(text_bytes, rss)
    );
    let _ = writeln!(
        out,
        "  CSR (graph):            {}  ({:.1}%)",
        humanize_bytes(csr_bytes),
        pct(csr_bytes, rss)
    );
    let _ = writeln!(
        out,
        "  WAL writers:            {}  ({:.1}%)",
        humanize_bytes(wal_bytes),
        pct(wal_bytes, rss)
    );
    let _ = writeln!(
        out,
        "  Sealed segments:        {}  ({:.1}%)",
        humanize_bytes(sealed_bytes),
        pct(sealed_bytes, rss)
    );
    let _ = writeln!(
        out,
        "  Replication backlog:    {}  ({:.1}%)",
        humanize_bytes(repl_bytes),
        pct(repl_bytes, rss)
    );
    let _ = writeln!(
        out,
        "  Lua (VM + scripts):     {}  ({:.1}%)",
        humanize_bytes(lua_bytes),
        pct(lua_bytes, rss)
    );
    let _ = writeln!(
        out,
        "  Allocator overhead:     {}  ({:.1}%)",
        humanize_bytes(allocator_overhead),
        pct(allocator_overhead, rss)
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "Mapped regions:");
    let _ = writeln!(out, "  File-backed mmap:       n/a");
    let _ = writeln!(out, "  Anonymous mmap:         n/a");
    let _ = writeln!(out);
    let _ = writeln!(out, "Recommendations:");
    let _ = writeln!(out, "  - {vsz_recommendation}");
    let _ = write!(out, "  - {resident_recommendation}");

    Frame::BulkString(Bytes::from(out))
}

/// Human-readable byte formatting (cold path — allocation OK).
fn humanize_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;
    const GB: usize = 1024 * 1024 * 1024;
    const TB: usize = 1024 * 1024 * 1024 * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Percentage with divide-by-zero guard.
fn pct(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        0.0
    } else {
        (part as f64 / whole as f64) * 100.0
    }
}

/// Simple ISO-8601 timestamp without external crate dependency.
fn chrono_iso8601_now() -> String {
    use std::time::SystemTime;
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => {
            let secs = d.as_secs();
            // Simple UTC formatting: YYYY-MM-DDTHH:MM:SSZ
            let days = secs / 86400;
            let time_of_day = secs % 86400;
            let hours = time_of_day / 3600;
            let minutes = (time_of_day % 3600) / 60;
            let seconds = time_of_day % 60;

            // Compute year/month/day from days since epoch (1970-01-01).
            let (year, month, day) = days_to_ymd(days);
            format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
        }
        Err(_) => "1970-01-01T00:00:00Z".to_string(),
    }
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Algorithm from Howard Hinnant's chrono-compatible date library.
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Read replication backlog resident bytes via the global state.
fn replication_backlog_bytes() -> usize {
    if let Some(state) = crate::admin::metrics_setup::get_global_repl_state_arc() {
        return state.read().backlog_resident_bytes();
    }
    0
}

/// Read allocator name and arena count. Cold path — single mallctl OK.
fn allocator_info() -> (String, String) {
    #[cfg(feature = "jemalloc")]
    {
        // opt.narenas = configured cap (what we set via malloc_conf / MALLOC_CONF).
        // arenas.narenas = actual created count (can exceed opt.narenas).
        // Operators care about the configured limit, not the runtime count.
        let arena_count = tikv_jemalloc_ctl::opt::narenas::read()
            .map(|n| n.to_string())
            .unwrap_or_else(|_| "n/a".to_string());
        ("jemalloc".to_string(), arena_count)
    }
    #[cfg(not(feature = "jemalloc"))]
    {
        ("system".to_string(), "n/a".to_string())
    }
}

/// Read VSZ (virtual memory size) for the current process.
///
/// Uses safe `/proc/self/status` parsing — `VmSize:` line is the canonical
/// virtual size in KiB. Cold admin path, allocation is fine.
#[cfg(target_os = "linux")]
fn get_vsz_bytes() -> usize {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    status
        .lines()
        .find_map(|line| {
            let rest = line.strip_prefix("VmSize:")?;
            let kib = rest.split_whitespace().next()?.parse::<usize>().ok()?;
            kib.checked_mul(1024)
        })
        .unwrap_or(0)
}

#[cfg(target_os = "macos")]
fn get_vsz_bytes() -> usize {
    // Reuse the shared macOS task_info helper that handles MACH_TASK_BASIC_INFO
    // with TASK_VM_INFO fallback (flavor 20 returns KERN_INVALID_ARGUMENT on
    // macOS 15+ / kernel 24.x).
    crate::admin::metrics_setup::macos_task_memory_info().0 as usize
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn get_vsz_bytes() -> usize {
    0
}

fn memory_help() -> Frame {
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from_static(b"MEMORY USAGE <key> [SAMPLES <count>]")),
        Frame::BulkString(Bytes::from_static(
            b"  Estimate memory usage of the key in bytes.",
        )),
        Frame::BulkString(Bytes::from_static(b"MEMORY STATS")),
        Frame::BulkString(Bytes::from_static(
            b"  Return a map of memory usage counters.",
        )),
        Frame::BulkString(Bytes::from_static(b"MEMORY DOCTOR")),
        Frame::BulkString(Bytes::from_static(b"  Memory health report.")),
        Frame::BulkString(Bytes::from_static(b"MEMORY HELP")),
        Frame::BulkString(Bytes::from_static(b"  Return subcommand help.")),
    ])
}

fn memory_usage(db: &mut Database, args: &[Frame]) -> Frame {
    let key = match parse_memory_usage_args(args) {
        Ok(k) => k,
        Err(e) => return e,
    };
    match db.get(key.as_ref()) {
        Some(entry) => memory_usage_reply(key.as_ref(), entry),
        None => Frame::Null,
    }
}

fn memory_usage_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    let key = match parse_memory_usage_args(args) {
        Ok(k) => k,
        Err(e) => return e,
    };
    match db.get_if_alive_any_plane(key.as_ref(), now_ms) {
        Some(entry) => memory_usage_reply(key.as_ref(), entry.entry()),
        None => Frame::Null,
    }
}

/// Validate the `MEMORY USAGE key [SAMPLES n]` argument list and return the
/// key bytes on success, or an error frame on failure.
fn parse_memory_usage_args(args: &[Frame]) -> Result<Bytes, Frame> {
    if args.is_empty() {
        return Err(err_wrong_args("MEMORY USAGE"));
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return Err(err_wrong_args("MEMORY USAGE")),
    };
    // Accept (and ignore) SAMPLES <n> — Moon always visits every entry.
    if args.len() > 1 {
        if args.len() != 3 {
            return Err(err_wrong_args("MEMORY USAGE"));
        }
        match extract_bytes(&args[1]) {
            Some(flag) if flag.eq_ignore_ascii_case(b"SAMPLES") => {
                if extract_bytes(&args[2])
                    .and_then(|s| std::str::from_utf8(s.as_ref()).ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .is_none()
                {
                    return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
                }
            }
            _ => return Err(Frame::Error(Bytes::from_static(b"ERR syntax error"))),
        }
    }
    Ok(key)
}

fn memory_usage_reply(key: &[u8], entry: &Entry) -> Frame {
    // Entry header + compact key bytes + payload estimate.
    // `48` models the DashTable entry metadata + CompactKey inline
    // bytes for the common case (Moon's SSO caps at 23 bytes; the
    // constant is intentionally conservative — Redis's numbers
    // include jemalloc fragmentation that we do not).
    let payload = estimate_serialized_length(entry);
    let total = 48usize.saturating_add(key.len()).saturating_add(payload);
    Frame::Integer(total as i64)
}

// ---------------------------------------------------------------------------
// KILL SNAPSHOT — MA2 operator command
// ---------------------------------------------------------------------------

/// `KILL SNAPSHOT <txn_id>`
///
/// Forcibly marks an active MVCC snapshot as killed. The snapshot is excluded
/// from the `oldest_snapshot` watermark immediately, unblocking `prune_committed`
/// GC. The client that owns the snapshot will receive a
/// `MOONERR snapshot too old: <txn_id>` error on its next transactional read.
///
/// ## Syntax
/// ```text
/// KILL SNAPSHOT <txn_id>
/// ```
/// - `txn_id` — decimal u64 transaction ID (from `TXN.BEGIN` response or `INFO`).
///
/// ## Returns
/// - `+OK` on success.
/// - `ERR wrong number of arguments` if syntax is wrong.
/// - `ERR KILL subcommand '<sub>' not supported` for unknown subcommands.
/// - `MOONERR snapshot not found: <txn_id>` if txn_id is unknown or already killed.
pub fn kill_snapshot(
    vector_store: &mut crate::vector::store::VectorStore,
    args: &[Frame],
) -> Frame {
    // args[0] = subcommand (must be SNAPSHOT), args[1] = txn_id
    if args.len() != 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'KILL SNAPSHOT'",
        ));
    }
    let sub = match extract_bytes(&args[0]) {
        Some(s) => s,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid argument")),
    };
    if !sub.eq_ignore_ascii_case(b"SNAPSHOT") {
        return Frame::Error(Bytes::from(
            format!(
                "ERR KILL subcommand '{}' not supported; use KILL SNAPSHOT",
                String::from_utf8_lossy(sub)
            )
            .into_bytes(),
        ));
    }
    let txn_id_bytes = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid txn_id")),
    };
    let txn_id: u64 = match std::str::from_utf8(txn_id_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(id) => id,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR txn_id must be a non-negative integer",
            ));
        }
    };

    let mgr = vector_store.txn_manager_mut();
    if mgr.kill_snapshot(txn_id) {
        tracing::info!(txn_id, "KILL SNAPSHOT: operator killed snapshot");
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else {
        Frame::Error(Bytes::from(
            format!("MOONERR snapshot not found: {txn_id}").into_bytes(),
        ))
    }
}

// ---------------------------------------------------------------------------
// VACUUM — P8 manual reclamation command family
// ---------------------------------------------------------------------------

/// Counts returned by any VACUUM variant.
#[derive(Debug, Default)]
pub struct VacuumCounts {
    /// Manifest tombstone entries physically removed by `gc_tombstones`.
    pub manifest_pruned: u64,
    /// MVCC committed-set entries pruned below the oldest-snapshot floor.
    pub mvcc_committed_pruned: u64,
    /// MVCC vector write-intents swept (zombie intent removal).
    pub mvcc_zombies_swept: u64,
    /// MVCC graph write-intents swept (zombie graph intent removal).
    pub mvcc_graph_zombies_swept: u64,
    /// MVCC snapshots newly flagged as killed by `mark_old_snapshots_killed`.
    pub mvcc_snapshots_killed: u64,
    /// WAL segments recycled by `recycle_aggressive`.
    pub wal_segments_recycled: u64,
}

impl VacuumCounts {
    /// Serialize into a flat RESP2 array of alternating label/value bulk strings.
    ///
    /// Shape (12 elements):
    /// ```text
    /// ["manifest_pruned", N, "mvcc_committed_pruned", N,
    ///  "mvcc_zombies_swept", N, "mvcc_graph_zombies_swept", N,
    ///  "mvcc_snapshots_killed", N, "wal_segments_recycled", N]
    /// ```
    pub fn to_frame(&self) -> Frame {
        fn kv(k: &'static [u8], v: u64) -> [Frame; 2] {
            [
                Frame::BulkString(Bytes::from_static(k)),
                Frame::Integer(v as i64),
            ]
        }
        let pairs: Vec<Frame> = [
            kv(b"manifest_pruned", self.manifest_pruned),
            kv(b"mvcc_committed_pruned", self.mvcc_committed_pruned),
            kv(b"mvcc_zombies_swept", self.mvcc_zombies_swept),
            kv(b"mvcc_graph_zombies_swept", self.mvcc_graph_zombies_swept),
            kv(b"mvcc_snapshots_killed", self.mvcc_snapshots_killed),
            kv(b"wal_segments_recycled", self.wal_segments_recycled),
        ]
        .iter()
        .flat_map(|pair| pair.iter().cloned())
        .collect();
        Frame::Array(crate::protocol::FrameVec::from_vec(pairs))
    }

    /// Like `to_frame` but prefixes each subsystem with a verbose diagnostic
    /// line as a bulk string. Used by `VACUUM (VERBOSE)`.
    pub fn to_verbose_frame(&self) -> Frame {
        use std::fmt::Write as _;
        let mut out: Vec<Frame> = Vec::with_capacity(20);

        let mut push_section = |label: &str, count: u64| {
            let mut s = String::with_capacity(64);
            let _ = write!(s, "# {} reclaimed: {}", label, count);
            out.push(Frame::BulkString(Bytes::from(s.into_bytes())));
        };

        push_section("manifest_pruned", self.manifest_pruned);
        push_section("mvcc_committed_pruned", self.mvcc_committed_pruned);
        push_section("mvcc_zombies_swept", self.mvcc_zombies_swept);
        push_section("mvcc_graph_zombies_swept", self.mvcc_graph_zombies_swept);
        push_section("mvcc_snapshots_killed", self.mvcc_snapshots_killed);
        push_section("wal_segments_recycled", self.wal_segments_recycled);

        // Append the same key/value pairs for machine-parseable consumption.
        let kv_frame = self.to_frame();
        if let Frame::Array(inner) = kv_frame {
            out.extend(inner);
        }
        Frame::Array(crate::protocol::FrameVec::from_vec(out))
    }
}

/// Core reclamation passes shared by `VACUUM` and `VACUUM (VERBOSE)`.
///
/// - `manifest`: optional manifest reference; when `None` (no persistence_dir),
///   the manifest pass is skipped and `manifest_pruned` stays 0.
/// - `wal`: optional WAL V3 writer; when `None`, the WAL pass is skipped.
/// - `freeze`: when `true`, calls `mark_old_snapshots_killed` with
///   `threshold = Duration::ZERO` (kills ALL non-system snapshots).
/// - `mvcc_prune_margin`: `oldest_snapshot - margin` is the GC floor.
/// - `disk_offload_dir` / `shard_id`: kernel M3 K2 review round 2 / P0-2.
///   Used ONLY to locate this shard's `ShardControlFile` for the WAL
///   recycle pass below — see that pass's own doc for why the unified
///   floor register must gate this exactly like autovacuum Pass C.
fn run_vacuum_passes(
    vector_store: &mut crate::vector::store::VectorStore,
    manifest: Option<&mut ShardManifest>,
    wal: Option<&mut WalWriterV3>,
    freeze: bool,
    mvcc_prune_margin: u64,
    disk_offload_dir: Option<&std::path::Path>,
    shard_id: usize,
) -> VacuumCounts {
    let now = Instant::now();
    let mut counts = VacuumCounts::default();

    // ── 1. Manifest physical GC (P1) ────────────────────────────────────────
    // Immediate removal: retain_epochs=0, retain_secs=0.
    if let Some(m) = manifest {
        counts.manifest_pruned = m.gc_tombstones(0, 0, now) as u64;
    }

    // ── 2. MVCC committed-set pruning (P3) ──────────────────────────────────
    {
        let mgr = vector_store.txn_manager_mut();
        counts.mvcc_committed_pruned = mgr.prune_committed(mvcc_prune_margin);
    }

    // ── 3. MVCC zombie intent sweep (P3) ────────────────────────────────────
    {
        let mgr = vector_store.txn_manager_mut();
        counts.mvcc_zombies_swept = mgr.sweep_zombies_mut() as u64;
    }

    // ── 4. MVCC graph zombie sweep (P3, graph feature) ──────────────────────
    #[cfg(feature = "graph")]
    {
        let mgr = vector_store.txn_manager_mut();
        counts.mvcc_graph_zombies_swept = mgr.sweep_graph_zombies_mut() as u64;
    }

    // ── 5. Mark old snapshots killed (MA2) ──────────────────────────────────
    {
        let threshold = if freeze {
            // FREEZE: kill ALL snapshots regardless of age.
            Duration::ZERO
        } else {
            // Plain VACUUM: use a very conservative threshold (24h) to
            // avoid killing healthy short-lived snapshots. Operators who
            // want aggressive snapshot removal should use VACUUM (FREEZE)
            // or configure mvcc_old_snapshot_threshold_secs.
            Duration::from_secs(86_400)
        };
        let mgr = vector_store.txn_manager_mut();
        counts.mvcc_snapshots_killed = mgr.mark_old_snapshots_killed(now, threshold) as u64;
    }

    // ── 6. WAL aggressive recycle (P6) ──────────────────────────────────────
    // Only runs when WAL is configured AND total WAL exceeds max_wal_bytes.
    //
    // Kernel M3 K2 review round 2 / P0-2: this used to recycle to
    // `w.current_lsn()` — the LIVE, uncheckpointed LSN counter — with NO
    // disk-offload gate at all. That is not a durable floor: in legacy
    // mode it deleted the sole durable copy of live KV/graph/plane data on
    // a routine client command (legacy has no checkpoint/snapshot
    // protocol whatsoever); in disk-offload mode it deleted
    // not-yet-checkpointed pages/graph writes recycle should never touch.
    // The "unified floor register" K2 claims to be does not exist while a
    // client-reachable command bypasses it — VACUUM must use EXACTLY the
    // same recycle floor as autovacuum Pass C
    // (`AutovacuumDaemon::run_tick`, `src/shard/autovacuum.rs`):
    // `min(control.last_checkpoint_lsn, control.graph_floor_lsn)` in
    // checkpoint-backed mode, refused entirely in legacy mode.
    if let Some(w) = wal {
        let should_recycle = w
            .stats()
            .map(|s| s.total_bytes > w.max_wal_bytes())
            .unwrap_or(false);
        if should_recycle {
            match disk_offload_dir {
                None => {
                    // Legacy (non-disk-offload) mode: no checkpoint or
                    // plane-snapshot protocol exists at all, so there is no
                    // durable floor to recycle against — mirror Pass C's
                    // skip-and-warn (task #43) exactly, including the same
                    // shared counter, so operators see WHY a routine
                    // VACUUM freed 0 WAL segments here (visible via `INFO`
                    // / `DEBUG RECLAMATION`'s `# Reclamation` section).
                    crate::command::info_reclamation::RECL_WAL_RECYCLE_BLOCKED_NO_CHECKPOINT_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::warn!(
                        "VACUUM: WAL recycle SKIPPED — this shard has no checkpoint \
                         floor to recycle against (legacy/non-disk-offload mode); \
                         recycling here would risk permanently losing graph/ \
                         workspace/MQ/temporal history that has no snapshot outside \
                         the WAL. Enable --disk-offload for bounded WAL growth."
                    );
                }
                Some(dir) => {
                    let shard_dir = dir.join(format!("shard-{shard_id}"));
                    let ctrl_path = crate::persistence::control::ShardControlFile::control_path(
                        &shard_dir, shard_id,
                    );
                    // Same-shape fallback as autovacuum's first tick
                    // (before any checkpoint has ever completed): no
                    // control file yet -> floor 0, i.e. recycle nothing
                    // this call. Maximally conservative, self-heals once
                    // Finalize runs.
                    let redo_lsn = crate::persistence::control::ShardControlFile::read(&ctrl_path)
                        .ok()
                        .map(|c| c.last_checkpoint_lsn.min(c.graph_floor_lsn))
                        .unwrap_or(0);
                    match w.recycle_aggressive(redo_lsn) {
                        Ok(stats) => counts.wal_segments_recycled = stats.segments_recycled as u64,
                        Err(e) => {
                            tracing::warn!("VACUUM: WAL recycle_aggressive failed: {e}");
                        }
                    }
                }
            }
        }
    }

    counts
}

/// `VACUUM [subcommand...]`
///
/// Manual reclamation across all Wave-1 subsystems. Postgres-style escape hatch
/// before autovacuum lands in Wave 2.
///
/// ## Subcommands
///
/// | Syntax | Description |
/// |---|---|
/// | `VACUUM` | Full pass: manifest GC + MVCC prune/sweep + WAL recycle |
/// | `VACUUM FILES` | Immediate manifest tombstone removal (gc_tombstones(0,0)) |
/// | `VACUUM (VERBOSE)` | Same as plain VACUUM, with per-subsystem diagnostic lines |
/// | `VACUUM (FREEZE)` | Forces `mark_old_snapshots_killed` with threshold=0 — **kills ALL client snapshots** |
/// | `VACUUM VECTOR <idx>` | Placeholder — returns `+OK pending` until Wave-2 segment merge |
/// | `VACUUM GRAPH <name>` | Placeholder — returns `+OK pending` until Wave-2 graph auto-merge |
///
/// ## Returns
/// Array of alternating `[label, count]` bulk strings (12 elements).
///
/// ## Dispatch-path contract
///
/// VACUUM always runs the MVCC passes (prune_committed, sweep_zombies,
/// mark_old_snapshots_killed) regardless of dispatch path. The manifest GC
/// and WAL recycle passes require the shard manifest and WAL writer, which
/// only exist in the shard event loop:
///
/// - **SPSC path** (console gateway, admin console): Full pass — manifest,
///   MVCC, and WAL. `manifest_pruned` and `wal_segments_recycled` reflect
///   real work when resources are available.
///
/// - **Direct dispatch path** (main-port connections via handler_single,
///   handler_sharded, handler_monoio): MVCC passes only. `manifest_pruned`
///   and `wal_segments_recycled` always return 0 because the manifest lives
///   only in the shard event loop (architecture constraint). The persistence
///   tick already handles manifest GC automatically; manual VACUUM on the main
///   port delivers the MVCC reclamation operators need most urgently.
///
/// For manifest/WAL reclamation via `VACUUM`, use the admin console, which
/// dispatches through the console-gateway SPSC path where manifest is in scope.
///
/// ## Edge cases
/// - No persistence_dir: manifest and WAL counts return 0.
/// - Under disk-pause: runs normally (VACUUM reclaims, does not write data).
/// - During active checkpoint: WAL recycle is idempotent (P6 `recycle_aggressive`
///   is a no-op if no segments are over the threshold).
/// - **Legacy mode (`--disk-offload disable`):** WAL recycle is REFUSED
///   entirely (kernel M3 K2 review round 2 / P0-2) — legacy mode has no
///   checkpoint/plane-snapshot protocol, so there is no durable floor to
///   recycle against; `wal_segments_recycled` stays 0 and
///   `RECL_WAL_RECYCLE_BLOCKED_NO_CHECKPOINT_TOTAL` increments (same
///   counter, same reasoning as autovacuum Pass C's legacy-mode skip).
/// - **Checkpoint-backed mode:** recycles to
///   `min(control.last_checkpoint_lsn, control.graph_floor_lsn)` — the
///   exact same floor Pass C uses — never the live, uncheckpointed
///   `wal.current_lsn()`.
///
/// ## FREEZE warning
/// `VACUUM (FREEZE)` forcibly kills ALL non-system MVCC snapshots on this shard,
/// breaking any in-flight transactional reads. Clients that hold active
/// `TXN.BEGIN` transactions will receive `MOONERR snapshot too old` on their
/// next transactional operation. Only use in emergencies (e.g. a stuck snapshot
/// is blocking GC from advancing for hours).
pub fn vacuum(
    vector_store: &mut crate::vector::store::VectorStore,
    manifest: Option<&mut ShardManifest>,
    wal: Option<&mut WalWriterV3>,
    args: &[Frame],
    mvcc_prune_margin: u64,
    // Kernel M3 K2 review round 2 / P0-2: `None` on the direct-dispatch
    // callers (handler_single/handler_sharded/handler_monoio — `wal` is
    // always `None` there too per this fn's own doc, so these are dead on
    // that path), `Some(..)`/real shard id on the SPSC/console-gateway
    // path where `wal` is real. See `run_vacuum_passes`'s WAL-recycle pass.
    disk_offload_dir: Option<&std::path::Path>,
    shard_id: usize,
) -> Frame {
    // Parse subcommand (optional first arg).
    let sub = args.first().and_then(|f| extract_bytes(f));

    match sub {
        // ── VACUUM FILES ────────────────────────────────────────────────────
        Some(s) if s.eq_ignore_ascii_case(b"FILES") => {
            if args.len() != 1 {
                return Frame::Error(Bytes::from_static(
                    b"ERR syntax error: VACUUM FILES takes no additional arguments",
                ));
            }
            let pruned = manifest
                .map(|m| m.gc_tombstones(0, 0, Instant::now()) as u64)
                .unwrap_or(0);
            let pairs = vec![
                Frame::BulkString(Bytes::from_static(b"manifest_pruned")),
                Frame::Integer(pruned as i64),
            ];
            Frame::Array(crate::protocol::FrameVec::from_vec(pairs))
        }

        // ── VACUUM (VERBOSE) ─────────────────────────────────────────────────
        Some(s) if s.eq_ignore_ascii_case(b"(VERBOSE)") => {
            if args.len() != 1 {
                return Frame::Error(Bytes::from_static(
                    b"ERR syntax error: VACUUM (VERBOSE) takes no additional arguments",
                ));
            }
            let counts = run_vacuum_passes(
                vector_store,
                manifest,
                wal,
                false,
                mvcc_prune_margin,
                disk_offload_dir,
                shard_id,
            );
            counts.to_verbose_frame()
        }

        // ── VACUUM (FREEZE) ──────────────────────────────────────────────────
        Some(s) if s.eq_ignore_ascii_case(b"(FREEZE)") => {
            if args.len() != 1 {
                return Frame::Error(Bytes::from_static(
                    b"ERR syntax error: VACUUM (FREEZE) takes no additional arguments",
                ));
            }
            tracing::warn!(
                "VACUUM (FREEZE): forcibly killing ALL active MVCC snapshots on this shard. \
                 In-flight TXN.BEGIN clients will receive 'snapshot too old' errors."
            );
            let counts = run_vacuum_passes(
                vector_store,
                manifest,
                wal,
                true,
                mvcc_prune_margin,
                disk_offload_dir,
                shard_id,
            );
            counts.to_frame()
        }

        // ── VACUUM VECTOR <index> ────────────────────────────────────────────
        Some(s) if s.eq_ignore_ascii_case(b"VECTOR") => {
            // Wave-2 P2 placeholder: segment merge not yet implemented.
            Frame::SimpleString(Bytes::from_static(b"OK pending implementation in v0.1.14"))
        }

        // ── VACUUM GRAPH <name> ──────────────────────────────────────────────
        Some(s) if s.eq_ignore_ascii_case(b"GRAPH") => {
            // Wave-2 P7 placeholder: graph auto-merge not yet implemented.
            Frame::SimpleString(Bytes::from_static(b"OK pending implementation in v0.1.14"))
        }

        // ── Plain VACUUM ─────────────────────────────────────────────────────
        None => {
            let counts = run_vacuum_passes(
                vector_store,
                manifest,
                wal,
                false,
                mvcc_prune_margin,
                disk_offload_dir,
                shard_id,
            );
            counts.to_frame()
        }

        // ── Unknown subcommand ───────────────────────────────────────────────
        Some(unknown) => Frame::Error(Bytes::from(
            format!(
                "ERR unknown VACUUM subcommand '{}'; \
                 use VACUUM, VACUUM FILES, VACUUM (VERBOSE), VACUUM (FREEZE), \
                 VACUUM VECTOR <idx>, or VACUUM GRAPH <name>",
                String::from_utf8_lossy(unknown)
            )
            .into_bytes(),
        )),
    }
}

// ---------------------------------------------------------------------------
// DEBUG RECLAMATION — P8 verbose diagnostic dump
// ---------------------------------------------------------------------------

/// `DEBUG RECLAMATION`
///
/// Verbose per-subsystem diagnostic dump — more detailed than the `# Reclamation`
/// INFO section. Returns INFO-style `key:value` lines as a RESP2 bulk string.
///
/// Covers:
/// - Manifest: active_entry_count, tombstone_count
/// - MVCC: committed_count, active_count, pruned_below, oldest_snapshot,
///   oldest_snapshot_age_secs, live_snapshot_count, killed_snapshot_count
/// - WAL: total_bytes, total_segments, max_wal_bytes, current_lsn
/// - Atomics snapshot (RECL_* counters for a complete picture)
pub fn debug_reclamation(
    vector_store: &crate::vector::store::VectorStore,
    manifest: Option<&ShardManifest>,
    wal: Option<&WalWriterV3>,
) -> Frame {
    use std::fmt::Write as _;
    use std::sync::atomic::Ordering::Relaxed;

    let mut buf = String::with_capacity(1024);
    let now = Instant::now();

    // ── Manifest ─────────────────────────────────────────────────────────────
    buf.push_str("# Manifest\r\n");
    if let Some(m) = manifest {
        let _ = write!(
            buf,
            "manifest_active_entries:{}\r\n",
            m.active_entry_count()
        );
        let _ = write!(buf, "manifest_tombstones:{}\r\n", m.tombstone_count());
    } else {
        buf.push_str("manifest_active_entries:0\r\n");
        buf.push_str("manifest_tombstones:0\r\n");
        buf.push_str("manifest_note:no_persistence_dir\r\n");
    }

    // ── WAL ───────────────────────────────────────────────────────────────────
    buf.push_str("# WAL\r\n");
    if let Some(w) = wal {
        let _ = write!(buf, "wal_current_lsn:{}\r\n", w.current_lsn());
        let _ = write!(buf, "wal_max_bytes:{}\r\n", w.max_wal_bytes());
        match w.stats() {
            Ok(s) => {
                let _ = write!(buf, "wal_total_bytes:{}\r\n", s.total_bytes);
                let _ = write!(buf, "wal_total_segments:{}\r\n", s.total_segments);
                let over = s.total_bytes > w.max_wal_bytes();
                let _ = write!(buf, "wal_over_ceiling:{}\r\n", if over { 1 } else { 0 });
            }
            Err(e) => {
                let _ = write!(buf, "wal_stats_error:{}\r\n", e);
            }
        }
    } else {
        buf.push_str("wal_current_lsn:0\r\n");
        buf.push_str("wal_note:no_persistence_dir\r\n");
    }

    // ── MVCC ─────────────────────────────────────────────────────────────────
    buf.push_str("# MVCC\r\n");
    {
        let mgr = vector_store.txn_manager();
        let _ = write!(buf, "mvcc_committed_count:{}\r\n", mgr.committed_count());
        let _ = write!(buf, "mvcc_active_count:{}\r\n", mgr.active_count());
        let _ = write!(buf, "mvcc_pruned_below:{}\r\n", mgr.pruned_below());
        let _ = write!(buf, "mvcc_oldest_snapshot:{}\r\n", mgr.oldest_snapshot());
        let _ = write!(buf, "mvcc_live_snapshots:{}\r\n", mgr.live_snapshot_count());
        let _ = write!(
            buf,
            "mvcc_killed_snapshots:{}\r\n",
            mgr.killed_snapshot_count()
        );
        let age_secs = mgr
            .oldest_snapshot_age(now)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let _ = write!(buf, "mvcc_oldest_snapshot_age_secs:{}\r\n", age_secs);
    }

    // ── Atomics snapshot (RECL_*) ─────────────────────────────────────────────
    buf.push_str("# Atomics\r\n");
    use crate::command::info_reclamation as R;
    let _ = write!(
        buf,
        "recl_manifest_active:{}\r\n",
        R::RECL_MANIFEST_ACTIVE.load(Relaxed)
    );
    let _ = write!(
        buf,
        "recl_manifest_tombstones:{}\r\n",
        R::RECL_MANIFEST_TOMBSTONES.load(Relaxed)
    );
    let _ = write!(
        buf,
        "recl_wal_bytes:{}\r\n",
        R::RECL_WAL_BYTES.load(Relaxed)
    );
    let _ = write!(
        buf,
        "recl_wal_segments:{}\r\n",
        R::RECL_WAL_SEGMENTS.load(Relaxed)
    );
    let _ = write!(
        buf,
        "recl_mvcc_committed:{}\r\n",
        R::RECL_MVCC_COMMITTED.load(Relaxed)
    );
    let _ = write!(
        buf,
        "recl_mvcc_active:{}\r\n",
        R::RECL_MVCC_ACTIVE.load(Relaxed)
    );
    let _ = write!(
        buf,
        "recl_mvcc_oldest_snapshot_age_secs:{}\r\n",
        R::RECL_MVCC_OLDEST_SNAPSHOT_AGE_SECS.load(Relaxed)
    );
    let _ = write!(
        buf,
        "recl_write_stall_active:{}\r\n",
        R::RECL_WRITE_STALL_ACTIVE.load(Relaxed)
    );
    let _ = write!(
        buf,
        "recl_segment_stall_active:{}\r\n",
        R::RECL_SEGMENT_STALL_ACTIVE.load(Relaxed)
    );
    let _ = write!(
        buf,
        "recl_disk_free_bytes:{}\r\n",
        R::RECL_DISK_FREE_BYTES.load(Relaxed)
    );

    Frame::BulkString(Bytes::from(buf.into_bytes()))
}

// ── VACUUM VECTOR <idx> (P2) ──────────────────────────────────────────────────

/// `VACUUM VECTOR <idx>` — merge immutable segments for a named vector index.
///
/// Forces a graph-union merge of all immutable segments in the named index.
/// Returns a human-readable summary with segment counts and live vector counts.
///
/// Return format:
///   "+Merged N segments into 1 (live_vectors=M)"  — merge ran
///   "+OK no merge needed (segments=N)"             — below trigger threshold
///   "+OK merge skipped (mode=none)"                — MERGE_MODE is NONE
///   error if index not found or merge fails
///
/// Wire this from the dispatch path that has access to `VectorStore`.
///
/// `db_index` (WS5a round 2, adversarial review finding 3): an index owned
/// by a different db is invisible — VACUUM VECTOR must not let a connection
/// merge/compact/probe (existence oracle) another db's index. Index names
/// are globally unique per shard (one name = exactly one db), so once the
/// name is confirmed to belong to `db_index` via the scoped lookups below,
/// the remaining unscoped `VectorStore` helpers (`needs_merge`,
/// `immutable_segment_count`, `force_merge_index`) are safe to call by name
/// — they cannot resolve to a different db's index.
pub fn vacuum_vector(
    vector_store: &mut crate::vector::store::VectorStore,
    args: &[Frame],
    db_index: u8,
) -> Frame {
    // Args: [index_name] or [index_name WEIGHT <n>]
    let name = match args.first() {
        Some(Frame::BulkString(b)) => b.clone(),
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR usage: VACUUM VECTOR <index_name> [WEIGHT <n>]",
            ));
        }
    };

    // W3-deep: intercept `VACUUM VECTOR <idx> WEIGHT <n>` before merge logic.
    // args[1] = "WEIGHT", args[2] = value
    if args.len() >= 3 {
        if let Some(Frame::BulkString(sub)) = args.get(1) {
            if sub.eq_ignore_ascii_case(b"WEIGHT") {
                let val_bytes = match args.get(2) {
                    Some(Frame::BulkString(b)) => b.as_ref(),
                    _ => {
                        return Frame::Error(Bytes::from_static(
                            b"ERR WEIGHT requires a numeric value",
                        ));
                    }
                };
                let parsed: f32 = match std::str::from_utf8(val_bytes)
                    .ok()
                    .and_then(|s| s.parse::<f32>().ok())
                {
                    Some(v) => v,
                    None => {
                        return Frame::Error(Bytes::from_static(b"ERR WEIGHT must be a number"));
                    }
                };
                let set_result = {
                    let idx = match vector_store.get_index_mut_for_db(name.as_ref(), db_index) {
                        Some(i) => i,
                        None => {
                            return Frame::Error(Bytes::from_static(b"ERR unknown vector index"));
                        }
                    };
                    idx.try_set_compaction_weight(parsed)
                    // `idx` borrow released here
                };
                return match set_result {
                    Ok(()) => {
                        // Persist the new weight so it survives a server restart.
                        vector_store.save_index_meta_sidecar();
                        let msg =
                            format!("OK weight set to {parsed} for index {:?}", name.as_ref());
                        Frame::SimpleString(Bytes::from(msg))
                    }
                    Err(e) => Frame::Error(Bytes::from(format!("ERR {e}").into_bytes())),
                };
            }
        }
    }

    // Check the index exists AND is owned by the caller's db.
    if vector_store
        .get_index_for_db(name.as_ref(), db_index)
        .is_none()
    {
        return Frame::Error(Bytes::from_static(b"ERR unknown vector index"));
    }

    // Check if merge mode is NONE.
    if let Some(idx) = vector_store.get_index_for_db(name.as_ref(), db_index) {
        if idx.meta.merge_mode == crate::vector::segment::compaction::MergeMode::None {
            return Frame::SimpleString(Bytes::from_static(b"OK merge skipped (mode=none)"));
        }
    }

    // Check if merge is needed.
    let needs = vector_store.needs_merge(name.as_ref()).unwrap_or(false);
    let seg_count = vector_store
        .immutable_segment_count(name.as_ref())
        .unwrap_or(0);

    if !needs && seg_count < 2 {
        let msg = format!("OK no merge needed (segments={seg_count})");
        return Frame::SimpleString(Bytes::from(msg));
    }

    // Run merge.
    match vector_store.force_merge_index(name.as_ref()) {
        Ok(stats) => {
            if stats.segments_merged == 0 {
                let msg = format!("OK no merge needed (segments={seg_count})");
                Frame::SimpleString(Bytes::from(msg))
            } else {
                let msg = format!(
                    "Merged {} segments into 1 (live_vectors={})",
                    stats.segments_merged, stats.live_vectors
                );
                Frame::SimpleString(Bytes::from(msg))
            }
        }
        Err(_) => Frame::Error(Bytes::from_static(b"ERR merge failed (check logs)")),
    }
}

// ---------------------------------------------------------------------------
// VACUUM GRAPH <name> — P7 graph segment auto-merge
// ---------------------------------------------------------------------------

/// `VACUUM GRAPH <name>`
///
/// Manually trigger a graph segment merge pass for a named graph.
///
/// Intercepted in `spsc_handler` before main dispatch because it needs
/// mutable `GraphStore` access (not available in `cmd_dispatch`).
///
/// ## Returns
/// - `+OK no merge needed (segments=N)` when no merge was triggered.
/// - `+Merged N segments into 1 (live_edges=E, dead_dropped=D)` on success.
/// - `-ERR unknown graph '<name>'` when the graph does not exist.
#[cfg(feature = "graph")]
pub fn vacuum_graph(
    graph_store: &mut crate::graph::store::GraphStore,
    args: &[Frame],
    graph_merge_max_segments: usize,
    graph_dead_edge_trigger: f64,
) -> Frame {
    let name = match args.first() {
        Some(Frame::BulkString(b)) => b.clone(),
        _ => return Frame::Error(Bytes::from_static(b"ERR usage: VACUUM GRAPH <graph_name>")),
    };

    if graph_store.get_graph(name.as_ref()).is_none() {
        return Frame::Error(Bytes::from(
            format!("ERR unknown graph '{}'", String::from_utf8_lossy(&name)).into_bytes(),
        ));
    }

    // Check current segment count before merge.
    let seg_count_before = graph_store
        .get_graph(name.as_ref())
        .map(|g| g.segments.load().immutable.len())
        .unwrap_or(0);

    let stats = crate::graph::compaction::run_graph_vacuum_pass(
        graph_store,
        &name,
        graph_merge_max_segments,
        graph_dead_edge_trigger,
    );

    if stats.segments_reclaimed == 0 {
        let msg = format!("OK no merge needed (segments={seg_count_before})");
        Frame::SimpleString(Bytes::from(msg))
    } else {
        let msg = format!(
            "Merged {} segments into 1 (live_edges={}, dead_dropped={})",
            stats.segments_reclaimed + 1,
            stats.live_edges,
            stats.dead_edges_dropped
        );
        Frame::SimpleString(Bytes::from(msg))
    }
}

// ---------------------------------------------------------------------------
// RECLAMATION SCHEDULE — MA5 maintenance-window scheduler commands
// ---------------------------------------------------------------------------

/// `RECLAMATION SCHEDULE <cron_expr> <multiplier>`
/// `RECLAMATION SCHEDULE LIST`
/// `RECLAMATION SCHEDULE CLEAR`
///
/// Manage the per-shard maintenance-window schedule that controls autovacuum
/// budget multipliers at different times of day/week.
///
/// ## Subcommands
///
/// | Syntax | Description |
/// |---|---|
/// | `RECLAMATION SCHEDULE <cron> <mult>` | Add a window |
/// | `RECLAMATION SCHEDULE LIST` | List all windows |
/// | `RECLAMATION SCHEDULE CLEAR` | Remove all windows |
///
/// `cron` is a 5-field UNIX cron expression (`* * * * *` format).
/// `mult` is a float multiplier (e.g. `2.0` for 2x budget, `0.1` for 10%).
///
/// ## Examples
/// ```text
/// RECLAMATION SCHEDULE "0 2 * * *" 2.0
/// RECLAMATION SCHEDULE "* 9-17 * * 1-5" 0.1
/// RECLAMATION SCHEDULE LIST
/// RECLAMATION SCHEDULE CLEAR
/// ```
pub fn reclamation_schedule(
    schedule: &mut crate::shard::maintenance_schedule::MaintenanceSchedule,
    args: &[Frame],
) -> Frame {
    let sub = match args
        .first()
        .and_then(|f| crate::command::helpers::extract_bytes(f))
    {
        Some(s) => s,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR usage: RECLAMATION SCHEDULE <cron> <mult> | LIST | CLEAR",
            ));
        }
    };

    if sub.eq_ignore_ascii_case(b"LIST") {
        // Return array of alternating [cron, multiplier_string] pairs.
        let windows = schedule.list();
        let mut pairs: Vec<Frame> = Vec::with_capacity(windows.len() * 2);
        for (expr, mult) in &windows {
            pairs.push(Frame::BulkString(Bytes::from(expr.clone())));
            pairs.push(Frame::BulkString(Bytes::from(format!("{mult}"))));
        }
        return Frame::Array(crate::protocol::FrameVec::from_vec(pairs));
    }

    if sub.eq_ignore_ascii_case(b"CLEAR") {
        schedule.clear();
        return Frame::SimpleString(Bytes::from_static(b"OK"));
    }

    // Add: RECLAMATION SCHEDULE <cron_expr> <multiplier>
    // args[0] = cron expression, args[1] = multiplier
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR usage: RECLAMATION SCHEDULE <cron> <mult>",
        ));
    }

    let cron_bytes = match crate::command::helpers::extract_bytes(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid cron expression")),
    };
    let mult_bytes = match crate::command::helpers::extract_bytes(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid multiplier")),
    };

    let cron_str = match std::str::from_utf8(cron_bytes.as_ref()) {
        Ok(s) => s,
        Err(_) => {
            return Frame::Error(Bytes::from_static(
                b"ERR cron expression is not valid UTF-8",
            ));
        }
    };

    let multiplier: f32 = match std::str::from_utf8(mult_bytes.as_ref())
        .ok()
        .and_then(|s| s.parse::<f32>().ok())
    {
        Some(v) if v.is_finite() && v >= 0.0 => v,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR multiplier must be a non-negative finite float",
            ));
        }
    };

    match schedule.add(cron_str, multiplier) {
        Ok(()) => Frame::SimpleString(Bytes::from_static(b"OK")),
        Err(e) => Frame::Error(Bytes::from(
            format!("ERR invalid cron expression: {e}").into_bytes(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn bulk(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn db_with_key() -> Database {
        let mut db = Database::new();
        // Use the public command entry point so the value goes through the
        // same insertion path the server uses at runtime.
        let _ = crate::command::string::set(&mut db, &[bulk(b"mykey"), bulk(b"helloworld")]);
        db
    }

    #[test]
    fn flushall_empties_db() {
        let mut db = db_with_key();
        assert_eq!(db.len(), 1);
        let f = flushall(&mut db, &[]);
        assert!(matches!(f, Frame::SimpleString(ref b) if b.as_ref() == b"OK"));
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn flushall_accepts_async() {
        let mut db = db_with_key();
        let f = flushall(&mut db, &[bulk(b"ASYNC")]);
        assert!(matches!(f, Frame::SimpleString(_)));
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn flushall_accepts_sync() {
        let mut db = db_with_key();
        let f = flushall(&mut db, &[bulk(b"sync")]);
        assert!(matches!(f, Frame::SimpleString(_)));
    }

    #[test]
    fn flushall_rejects_garbage() {
        let mut db = db_with_key();
        let f = flushall(&mut db, &[bulk(b"GARBAGE")]);
        match f {
            Frame::Error(b) => assert!(b.starts_with(b"ERR syntax")),
            _ => panic!("expected ERR, got {f:?}"),
        }
    }

    #[test]
    fn flushdb_clears_current() {
        let mut db = db_with_key();
        let f = flushdb(&mut db, &[]);
        assert!(matches!(f, Frame::SimpleString(_)));
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn debug_object_returns_encoding_refcount_serlen() {
        let mut db = db_with_key();
        let f = debug(&mut db, &[bulk(b"OBJECT"), bulk(b"mykey")]);
        let s = match f {
            Frame::SimpleString(b) => String::from_utf8(b.to_vec()).unwrap(),
            other => panic!("expected SimpleString, got {other:?}"),
        };
        assert!(s.contains("encoding:"), "missing encoding: in {s}");
        assert!(s.contains("refcount:1"), "missing refcount:1 in {s}");
        assert!(s.contains("serializedlength:"), "missing serlen in {s}");
    }

    #[test]
    fn debug_object_missing_key() {
        let mut db = Database::new();
        let f = debug(&mut db, &[bulk(b"OBJECT"), bulk(b"missing")]);
        match f {
            Frame::Error(b) => assert!(b.starts_with(b"ERR no such key")),
            _ => panic!("expected ERR, got {f:?}"),
        }
    }

    #[test]
    fn debug_sleep_zero_is_immediate() {
        let mut db = Database::new();
        let start = std::time::Instant::now();
        let f = debug(&mut db, &[bulk(b"SLEEP"), bulk(b"0")]);
        assert!(start.elapsed() < std::time::Duration::from_millis(50));
        assert!(matches!(f, Frame::SimpleString(_)));
    }

    #[test]
    fn debug_sleep_200ms() {
        let mut db = Database::new();
        let start = std::time::Instant::now();
        let f = debug(&mut db, &[bulk(b"SLEEP"), bulk(b"0.2")]);
        assert!(start.elapsed() >= std::time::Duration::from_millis(180));
        assert!(matches!(f, Frame::SimpleString(_)));
    }

    #[test]
    fn debug_sleep_rejects_negative() {
        let f = debug_sleep(&[bulk(b"-1")]);
        assert!(matches!(f, Frame::Error(_)));
    }

    #[test]
    fn debug_sleep_rejects_non_float() {
        let f = debug_sleep(&[bulk(b"abc")]);
        assert!(matches!(f, Frame::Error(_)));
    }

    /// `DEBUG DIGEST` must never be answered from the ordinary command path.
    ///
    /// That path holds ONE database of ONE shard, so any digest it produced
    /// would be forty plausible hex characters matching nothing. Redis does
    /// serve this inside MULTI; moon cannot, and refusing is the only honest
    /// option — a silently partial digest is worse than an error.
    #[test]
    fn debug_digest_is_refused_on_the_inline_path_rather_than_answered_partially() {
        let mut db = Database::new();
        db.set(
            bytes::Bytes::from_static(b"k"),
            crate::storage::Entry::new_string(bytes::Bytes::from_static(b"v")),
        );
        let args = [Frame::BulkString(Bytes::from_static(b"DIGEST"))];
        match debug(&mut db, &args) {
            Frame::Error(e) => {
                let msg = String::from_utf8_lossy(&e);
                assert!(
                    msg.starts_with("ERR "),
                    "must be a normal ERR reply, got {msg}"
                );
                assert!(
                    msg.contains("MULTI"),
                    "the message must name the actual limitation so a caller \
                     knows what to do, got {msg}"
                );
                // A 40-hex answer here would be the bug this guards. Check
                // the reply cannot BE a digest, not that it merely differs
                // from one.
                let hex_run = msg
                    .split(|c: char| !c.is_ascii_hexdigit())
                    .map(str::len)
                    .max()
                    .unwrap_or(0);
                assert!(
                    hex_run < 40,
                    "the refusal contains a 40-character hex run, which is \
                     exactly what a digest looks like: {msg}"
                );
            }
            other => panic!("DEBUG DIGEST was ANSWERED on the inline path: {other:?}"),
        }
    }

    /// A RESP simple error may contain no CR or LF, and moon's own harnesses
    /// parse these by line.
    #[test]
    fn the_off_path_error_is_a_single_ascii_line() {
        let args = [Frame::BulkString(Bytes::from_static(b"DIGEST"))];
        let mut db = Database::new();
        let Frame::Error(e) = debug(&mut db, &args) else {
            panic!("expected an error");
        };
        assert!(!e.contains(&b'\r'), "error frame contains CR");
        assert!(!e.contains(&b'\n'), "error frame contains LF");
        assert!(e.is_ascii(), "error frame is not ASCII");
    }

    /// The internal fan-out subcommand is NOT client surface: reaching the
    /// ordinary path with it means a client sent it, and it must look unknown.
    #[test]
    fn the_internal_shard_subcommand_is_not_advertised_to_clients() {
        let mut db = Database::new();
        let args = [Frame::BulkString(Bytes::from_static(b"DIGEST-SHARD"))];
        match debug(&mut db, &args) {
            Frame::Error(e) => assert!(
                String::from_utf8_lossy(&e).contains("not supported"),
                "DIGEST-SHARD should read as an unknown subcommand to clients"
            ),
            other => panic!("DIGEST-SHARD answered a client: {other:?}"),
        }
    }

    #[test]
    fn debug_help_lists_subcommands() {
        let mut db = Database::new();
        let f = debug(&mut db, &[bulk(b"HELP")]);
        match f {
            Frame::Array(v) => {
                // At least OBJECT + SLEEP + HELP (3 pairs of label + description = 6 entries).
                assert!(v.len() >= 6, "expected >=6 help lines, got {}", v.len());
                let joined: Vec<String> = v
                    .iter()
                    .filter_map(|e| match e {
                        Frame::BulkString(b) => Some(String::from_utf8_lossy(b).to_string()),
                        _ => None,
                    })
                    .collect();
                let blob = joined.join("\n");
                assert!(blob.contains("OBJECT"), "help missing OBJECT");
                assert!(blob.contains("SLEEP"), "help missing SLEEP");
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn debug_unknown_subcommand() {
        let mut db = Database::new();
        let f = debug(&mut db, &[bulk(b"NUKE")]);
        match f {
            Frame::Error(b) => assert!(b.starts_with(b"ERR DEBUG subcommand")),
            _ => panic!("expected ERR, got {f:?}"),
        }
    }

    #[test]
    fn memory_usage_existing_key() {
        let mut db = db_with_key();
        let f = memory_usage(&mut db, &[bulk(b"mykey")]);
        match f {
            Frame::Integer(n) => assert!(n >= 10, "expected >=10 bytes, got {n}"),
            other => panic!("expected Integer, got {other:?}"),
        }
    }

    #[test]
    fn memory_usage_missing_key_returns_null() {
        let mut db = Database::new();
        let f = memory_usage(&mut db, &[bulk(b"missing")]);
        assert!(matches!(f, Frame::Null));
    }

    #[test]
    fn memory_usage_samples_flag_accepted() {
        let mut db = db_with_key();
        let f = memory_usage(&mut db, &[bulk(b"mykey"), bulk(b"SAMPLES"), bulk(b"5")]);
        assert!(matches!(f, Frame::Integer(_)));
    }

    #[test]
    fn memory_usage_samples_zero_is_accepted() {
        // `SAMPLES 0` is VALID in Redis — it means "sample every nested
        // value" — so it must not be rejected as a bad argument. Pinned
        // because a stricter duplicate of this parser exists in
        // `key_extra::memory_usage` (currently unreferenced) which rejects
        // a zero count; if that one is ever wired up, this test fails
        // instead of silently breaking valid client syntax.
        let mut db = db_with_key();
        let f = memory_usage(&mut db, &[bulk(b"mykey"), bulk(b"SAMPLES"), bulk(b"0")]);
        assert!(
            matches!(f, Frame::Integer(_)),
            "MEMORY USAGE key SAMPLES 0 must return a size, got {f:?}"
        );
    }

    #[test]
    fn memory_usage_samples_rejects_non_integer() {
        let mut db = db_with_key();
        let f = memory_usage(&mut db, &[bulk(b"mykey"), bulk(b"SAMPLES"), bulk(b"abc")]);
        assert!(matches!(f, Frame::Error(_)));
    }

    #[test]
    fn memory_stats_returns_map() {
        let mut db = db_with_key();
        let f = memory(&mut db, &[bulk(b"STATS")]);
        assert!(matches!(f, Frame::Map(_)));
    }

    #[test]
    fn memory_help_lists_usage() {
        let mut db = Database::new();
        let f = memory(&mut db, &[bulk(b"HELP")]);
        assert!(matches!(f, Frame::Array(_)));
    }

    #[test]
    fn memory_unknown_subcommand() {
        let mut db = Database::new();
        let f = memory(&mut db, &[bulk(b"NUKE")]);
        match f {
            Frame::Error(b) => assert!(b.starts_with(b"ERR MEMORY subcommand")),
            _ => panic!("expected ERR, got {f:?}"),
        }
    }

    // ── KILL SNAPSHOT unit tests ───────────────────────────────────────────

    #[test]
    fn kill_snapshot_unknown_txn_id_returns_error() {
        let mut store = crate::vector::store::VectorStore::new();
        // txn_id 9999 was never started
        let f = kill_snapshot(&mut store, &[bulk(b"SNAPSHOT"), bulk(b"9999")]);
        match f {
            Frame::Error(b) => {
                let s = std::str::from_utf8(&b).unwrap_or("");
                assert!(
                    s.contains("not found") || s.contains("ERR"),
                    "must mention not found or ERR: {s:?}"
                );
            }
            _ => panic!("expected ERR, got {f:?}"),
        }
    }

    #[test]
    fn kill_snapshot_wrong_subcommand_returns_error() {
        let mut store = crate::vector::store::VectorStore::new();
        let f = kill_snapshot(&mut store, &[bulk(b"PROCESS"), bulk(b"1")]);
        match f {
            Frame::Error(_) => {}
            _ => panic!("expected ERR for wrong subcommand, got {f:?}"),
        }
    }

    #[test]
    fn kill_snapshot_missing_txn_id_returns_error() {
        let mut store = crate::vector::store::VectorStore::new();
        let f = kill_snapshot(&mut store, &[bulk(b"SNAPSHOT")]);
        match f {
            Frame::Error(_) => {}
            _ => panic!("expected ERR for missing txn_id, got {f:?}"),
        }
    }

    #[test]
    fn kill_snapshot_active_txn_returns_ok() {
        let mut store = crate::vector::store::VectorStore::new();
        let txn = store.txn_manager_mut().begin();
        let txn_id_str = txn.txn_id.to_string();
        let txn_id_bytes = txn_id_str.as_bytes();
        let f = kill_snapshot(&mut store, &[bulk(b"SNAPSHOT"), bulk(txn_id_bytes)]);
        match f {
            Frame::SimpleString(b) => assert_eq!(&*b, b"OK"),
            _ => panic!("expected +OK, got {f:?}"),
        }
        assert!(
            store.txn_manager().is_killed(txn.txn_id),
            "txn must be marked killed"
        );
    }

    // ── VACUUM unit tests ───────────────────────────────────────────────────

    /// P8-UNIT-1: plain VACUUM with no persistence returns zeros for
    /// manifest_pruned and wal_segments_recycled; MVCC counts are non-negative.
    #[test]
    fn vacuum_no_persistence_returns_array() {
        let mut store = crate::vector::store::VectorStore::new();
        let f = vacuum(&mut store, None, None, &[], 1000, None, 0);
        match f {
            Frame::Array(ref arr) => {
                assert_eq!(arr.len(), 12, "expect 6 key/value pairs = 12 elements");
                // First key must be manifest_pruned
                if let Frame::BulkString(ref k) = arr[0] {
                    assert_eq!(k.as_ref(), b"manifest_pruned");
                } else {
                    panic!("expected BulkString key at index 0");
                }
                // manifest_pruned value must be 0 (no manifest)
                if let Frame::Integer(v) = arr[1] {
                    assert_eq!(v, 0, "manifest_pruned must be 0 with no manifest");
                } else {
                    panic!("expected Integer at index 1");
                }
            }
            _ => panic!("expected Array from vacuum, got {f:?}"),
        }
    }

    /// P8-UNIT-2: VACUUM FILES with no manifest returns Array with manifest_pruned=0.
    #[test]
    fn vacuum_files_no_manifest_returns_zero() {
        let mut store = crate::vector::store::VectorStore::new();
        let f = vacuum(&mut store, None, None, &[bulk(b"FILES")], 1000, None, 0);
        match f {
            Frame::Array(ref arr) => {
                assert_eq!(arr.len(), 2);
                if let Frame::BulkString(ref k) = arr[0] {
                    assert_eq!(k.as_ref(), b"manifest_pruned");
                }
                if let Frame::Integer(v) = arr[1] {
                    assert_eq!(v, 0);
                }
            }
            _ => panic!("expected Array from VACUUM FILES, got {f:?}"),
        }
    }

    /// P8-UNIT-3: VACUUM (VERBOSE) returns Array with diagnostic prefix strings.
    #[test]
    fn vacuum_verbose_includes_diagnostic_lines() {
        let mut store = crate::vector::store::VectorStore::new();
        let f = vacuum(&mut store, None, None, &[bulk(b"(VERBOSE)")], 1000, None, 0);
        match f {
            Frame::Array(ref arr) => {
                // Must have at least 6 diagnostic lines + 12 kv pairs
                assert!(
                    arr.len() >= 18,
                    "verbose frame must have >= 18 elements, got {}",
                    arr.len()
                );
                // First element must be a diagnostic line starting with '#'
                if let Frame::BulkString(ref b) = arr[0] {
                    assert!(
                        b.starts_with(b"# "),
                        "first verbose element must start with '# ', got: {:?}",
                        std::str::from_utf8(b)
                    );
                } else {
                    panic!("expected BulkString diagnostic at index 0");
                }
            }
            _ => panic!("expected Array from VACUUM (VERBOSE), got {f:?}"),
        }
    }

    /// P8-UNIT-4: VACUUM (FREEZE) returns the same shape as plain VACUUM.
    #[test]
    fn vacuum_freeze_returns_kv_array() {
        let mut store = crate::vector::store::VectorStore::new();
        let f = vacuum(&mut store, None, None, &[bulk(b"(FREEZE)")], 1000, None, 0);
        match f {
            Frame::Array(ref arr) => {
                assert_eq!(arr.len(), 12, "FREEZE must return 12-element kv array");
            }
            _ => panic!("expected Array from VACUUM (FREEZE), got {f:?}"),
        }
    }

    /// P8-UNIT-5: VACUUM VECTOR returns +OK pending placeholder.
    #[test]
    fn vacuum_vector_returns_pending() {
        let mut store = crate::vector::store::VectorStore::new();
        let f = vacuum(
            &mut store,
            None,
            None,
            &[bulk(b"VECTOR"), bulk(b"myidx")],
            1000,
            None,
            0,
        );
        match f {
            Frame::SimpleString(ref b) => {
                assert!(
                    b.as_ref().starts_with(b"OK pending"),
                    "VACUUM VECTOR must return pending: {:?}",
                    std::str::from_utf8(b)
                );
            }
            _ => panic!("expected SimpleString from VACUUM VECTOR, got {f:?}"),
        }
    }

    /// P8-UNIT-6: VACUUM GRAPH returns +OK pending placeholder.
    #[test]
    fn vacuum_graph_returns_pending() {
        let mut store = crate::vector::store::VectorStore::new();
        let f = vacuum(
            &mut store,
            None,
            None,
            &[bulk(b"GRAPH"), bulk(b"g")],
            1000,
            None,
            0,
        );
        match f {
            Frame::SimpleString(ref b) => {
                assert!(
                    b.as_ref().starts_with(b"OK pending"),
                    "VACUUM GRAPH must return pending: {:?}",
                    std::str::from_utf8(b)
                );
            }
            _ => panic!("expected SimpleString from VACUUM GRAPH, got {f:?}"),
        }
    }

    /// P8-UNIT-7: VACUUM with unknown subcommand returns ERR.
    #[test]
    fn vacuum_unknown_subcommand_returns_error() {
        let mut store = crate::vector::store::VectorStore::new();
        let f = vacuum(&mut store, None, None, &[bulk(b"BOGUS")], 1000, None, 0);
        match f {
            Frame::Error(_) => {}
            _ => panic!("expected ERR for unknown VACUUM subcommand, got {f:?}"),
        }
    }

    /// P8-UNIT-8: VACUUM (FREEZE) with an active snapshot kills it and reflects
    /// in mvcc_snapshots_killed count.
    #[test]
    fn vacuum_freeze_kills_active_snapshots() {
        let mut store = crate::vector::store::VectorStore::new();
        let _txn = store.txn_manager_mut().begin();
        let f = vacuum(&mut store, None, None, &[bulk(b"(FREEZE)")], 1000, None, 0);
        // Extract mvcc_snapshots_killed from returned array.
        let killed = match &f {
            Frame::Array(arr) => {
                // Find "mvcc_snapshots_killed" key and read next Integer.
                let mut found = None;
                for i in (0..arr.len()).step_by(2) {
                    if let Frame::BulkString(ref k) = arr[i] {
                        if k.as_ref() == b"mvcc_snapshots_killed" {
                            if let Frame::Integer(v) = arr[i + 1] {
                                found = Some(v);
                            }
                        }
                    }
                }
                found.expect("mvcc_snapshots_killed key missing")
            }
            _ => panic!("expected Array from VACUUM (FREEZE)"),
        };
        assert_eq!(killed, 1, "VACUUM (FREEZE) must kill the 1 active snapshot");
    }

    /// P8-UNIT-9: DEBUG RECLAMATION returns BulkString with expected sections.
    #[test]
    fn debug_reclamation_returns_bulk_string_with_sections() {
        let store = crate::vector::store::VectorStore::new();
        let f = debug_reclamation(&store, None, None);
        match f {
            Frame::BulkString(ref b) => {
                let s = std::str::from_utf8(b).expect("debug output must be UTF-8");
                assert!(s.contains("# Manifest"), "must contain Manifest section");
                assert!(s.contains("# WAL"), "must contain WAL section");
                assert!(s.contains("# MVCC"), "must contain MVCC section");
                assert!(s.contains("# Atomics"), "must contain Atomics section");
                assert!(
                    s.contains("manifest_active_entries:"),
                    "manifest field missing"
                );
                assert!(s.contains("mvcc_committed_count:"), "mvcc field missing");
                assert!(s.contains("recl_wal_bytes:"), "atomic field missing");
            }
            _ => panic!("expected BulkString from DEBUG RECLAMATION, got {f:?}"),
        }
    }

    // ── Kernel M3 K2 review round 2 / P0-2: VACUUM floor unification ───────

    /// Count `*.wal` files under `wal_dir` (pattern:
    /// `shard::autovacuum::tests::count_wal_segments`).
    fn count_wal_segments(wal_dir: &std::path::Path) -> usize {
        std::fs::read_dir(wal_dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Build a `WalWriterV3` with enough sealed (non-active) segments to
    /// exceed a tiny `max_wal_bytes` ceiling, so the recycle-eligibility
    /// check in `run_vacuum_passes` fires (pattern:
    /// `shard::autovacuum::tests::wal_writer_over_ceiling`).
    fn wal_writer_over_ceiling(
        wal_dir: &std::path::Path,
    ) -> crate::persistence::wal_v3::segment::WalWriterV3 {
        use crate::persistence::wal_v3::record::WalRecordType;
        let mut writer =
            crate::persistence::wal_v3::segment::WalWriterV3::new(0, wal_dir, 512).unwrap();
        writer.set_wal_bounds(0, 256);
        for i in 0..80 {
            writer.append(WalRecordType::Command, b"vacuum-p0-2 EARLY-MARKER");
            if (i + 1) % 3 == 0 {
                writer.flush_sync().unwrap();
            }
        }
        writer.flush_sync().unwrap();
        assert!(
            writer.current_segment_sequence() >= 3,
            "test setup must produce several sealed segments"
        );
        writer
    }

    /// P0-2 (drop-resurrection sibling finding): `VACUUM` in legacy
    /// (non-disk-offload) mode used to recycle WAL segments against the
    /// live, uncheckpointed `wal.current_lsn()` — deleting the sole
    /// durable copy of unflushed data on a routine client command. Must
    /// now refuse entirely, mirroring autovacuum Pass C's task #43 fix,
    /// including the same shared blocked-counter.
    #[test]
    fn vacuum_legacy_mode_refuses_wal_recycle_and_counts_blocked() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        let mut writer = wal_writer_over_ceiling(&wal_dir);
        let before_segments = count_wal_segments(&wal_dir);
        let before_blocked =
            crate::command::info_reclamation::RECL_WAL_RECYCLE_BLOCKED_NO_CHECKPOINT_TOTAL
                .load(std::sync::atomic::Ordering::Relaxed);

        let mut store = crate::vector::store::VectorStore::new();
        // disk_offload_dir: None => legacy mode, no matter what `wal` holds.
        let counts = run_vacuum_passes(&mut store, None, Some(&mut writer), false, 1000, None, 0);

        assert_eq!(
            counts.wal_segments_recycled, 0,
            "legacy mode must recycle 0 segments via VACUUM"
        );
        let after_segments = count_wal_segments(&wal_dir);
        assert_eq!(
            after_segments, before_segments,
            "legacy mode must not delete any WAL segment via VACUUM (P0-2)"
        );
        let after_blocked =
            crate::command::info_reclamation::RECL_WAL_RECYCLE_BLOCKED_NO_CHECKPOINT_TOTAL
                .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            after_blocked > before_blocked,
            "legacy-mode VACUUM skip must be observable via \
             RECL_WAL_RECYCLE_BLOCKED_NO_CHECKPOINT_TOTAL (same counter as Pass C)"
        );
    }

    /// P0-2: `VACUUM` in checkpoint-backed (disk-offload) mode must recycle
    /// to exactly the same floor as autovacuum Pass C —
    /// `min(control.last_checkpoint_lsn, control.graph_floor_lsn)` read
    /// from this shard's `ShardControlFile` — never the live
    /// `wal.current_lsn()`. With both floors pinned to `u64::MAX` (a
    /// checkpoint that has covered everything written), recycling must
    /// actually happen.
    #[test]
    fn vacuum_disk_offload_mode_recycles_to_control_file_floor() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let wal_dir = dir.join("shard-0").join("wal-v3");
        let mut writer = wal_writer_over_ceiling(&wal_dir);
        let before_segments = count_wal_segments(&wal_dir);

        let shard_dir = dir.join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();
        let mut control = crate::persistence::control::ShardControlFile::new([0u8; 16]);
        control.last_checkpoint_lsn = u64::MAX;
        control.graph_floor_lsn = u64::MAX;
        let ctrl_path = crate::persistence::control::ShardControlFile::control_path(&shard_dir, 0);
        control.write(&ctrl_path).unwrap();

        let mut store = crate::vector::store::VectorStore::new();
        let counts = run_vacuum_passes(
            &mut store,
            None,
            Some(&mut writer),
            false,
            1000,
            Some(&dir),
            0,
        );

        assert!(
            counts.wal_segments_recycled > 0,
            "disk-offload mode with a control-file floor covering everything \
             must actually recycle via VACUUM (P0-2)"
        );
        let after_segments = count_wal_segments(&wal_dir);
        assert!(
            after_segments < before_segments,
            "recycled segments must actually be removed from disk"
        );
    }

    /// P0-2 edge case: disk-offload mode but no control file has ever been
    /// written yet (very first VACUUM before any checkpoint completes).
    /// Must be maximally conservative — floor 0, recycle nothing — not
    /// panic or fall back to the unsafe live-LSN behavior.
    #[test]
    fn vacuum_disk_offload_mode_no_control_file_yet_recycles_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let wal_dir = dir.join("shard-0").join("wal-v3");
        let mut writer = wal_writer_over_ceiling(&wal_dir);
        let before_segments = count_wal_segments(&wal_dir);

        let mut store = crate::vector::store::VectorStore::new();
        let counts = run_vacuum_passes(
            &mut store,
            None,
            Some(&mut writer),
            false,
            1000,
            Some(&dir),
            0,
        );

        assert_eq!(
            counts.wal_segments_recycled, 0,
            "no control file yet => floor 0 => recycle nothing"
        );
        assert_eq!(count_wal_segments(&wal_dir), before_segments);
    }
}

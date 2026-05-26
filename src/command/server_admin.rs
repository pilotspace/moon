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
//! * `FLUSHALL` clears the currently-selected database on this shard as well;
//!   the console gateway broadcasts the command to every shard, and a future
//!   extension can iterate every DB index before flushing. For v0.1.5 we
//!   match Redis behaviour with a single active DB.
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
pub fn flushall(db: &mut Database, args: &[Frame]) -> Frame {
    if !check_flush_args(args) {
        return Frame::Error(Bytes::from_static(b"ERR syntax error"));
    }
    db.clear();
    Frame::SimpleString(Bytes::from_static(b"OK"))
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
        Ok(DebugCall::Help) => debug_help(),
        Err(e) => e,
    }
}

enum DebugCall<'a> {
    Object(&'a [Frame]),
    Sleep(&'a [Frame]),
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
    } else if sub.eq_ignore_ascii_case(b"HELP") {
        Ok(DebugCall::Help)
    } else {
        Err(Frame::Error(Bytes::from(format!(
            "ERR DEBUG subcommand '{}' not supported",
            String::from_utf8_lossy(sub),
        ))))
    }
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
    match db.get_if_alive(key.as_ref(), now_ms) {
        Some(entry) => debug_object_reply(entry),
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

    // ── Gather per-subsystem resident bytes ──────────────────────────────
    let mut dashtable_bytes: usize = 0;
    let mut hnsw_bytes: usize = 0;
    let mut sealed_bytes: usize = 0;
    #[cfg_attr(not(feature = "graph"), allow(unused_mut))]
    let mut csr_bytes: usize = 0;
    let wal_bytes: usize = 0;

    if let Some(shard_dbs) = crate::admin::metrics_setup::get_global_shard_databases() {
        let num_shards = shard_dbs.num_shards();
        for shard_id in 0..num_shards {
            // Database + DashTable (DB 0 only — the hot database)
            let db_guard = shard_dbs.read_db(shard_id, 0);
            dashtable_bytes += db_guard.resident_bytes();
            dashtable_bytes += db_guard.data().resident_bytes();
            drop(db_guard);

            // VectorStore: (mutable/hnsw, immutable/sealed)
            let vs = shard_dbs.vector_store(shard_id);
            let (mutable, immutable) = vs.resident_bytes();
            hnsw_bytes += mutable;
            sealed_bytes += immutable;
            drop(vs);

            // GraphStore (CSR)
            #[cfg(feature = "graph")]
            {
                let gs = shard_dbs.graph_store_read(shard_id);
                csr_bytes += gs.resident_bytes();
            }
        }
    }

    // Replication backlog via global state (same pattern as INFO replication).
    let repl_bytes = replication_backlog_bytes();

    // WAL writers live on event-loop stacks, not accessible from command path.
    // Report 0 with stable label — operators see the label exists.

    // ── Allocator metadata ───────────────────────────────────────────────
    let (allocator_name, arena_count) = allocator_info();

    // ── Computed overhead ────────────────────────────────────────────────
    let tracked_sum =
        dashtable_bytes + hnsw_bytes + csr_bytes + wal_bytes + sealed_bytes + repl_bytes;
    let allocator_overhead = rss.saturating_sub(tracked_sum);

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
        if let Ok(guard) = state.read() {
            return guard.backlog_resident_bytes();
        }
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
    match db.get_if_alive(key.as_ref(), now_ms) {
        Some(entry) => memory_usage_reply(key.as_ref(), entry),
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
fn run_vacuum_passes(
    vector_store: &mut crate::vector::store::VectorStore,
    manifest: Option<&mut ShardManifest>,
    wal: Option<&mut WalWriterV3>,
    freeze: bool,
    mvcc_prune_margin: u64,
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
    if let Some(w) = wal {
        let should_recycle = w
            .stats()
            .map(|s| s.total_bytes > w.max_wal_bytes())
            .unwrap_or(false);
        if should_recycle {
            let redo_lsn = w.current_lsn();
            match w.recycle_aggressive(redo_lsn) {
                Ok(stats) => counts.wal_segments_recycled = stats.segments_recycled as u64,
                Err(e) => {
                    tracing::warn!("VACUUM: WAL recycle_aggressive failed: {e}");
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
            let counts = run_vacuum_passes(vector_store, manifest, wal, false, mvcc_prune_margin);
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
            let counts = run_vacuum_passes(vector_store, manifest, wal, true, mvcc_prune_margin);
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
            let counts = run_vacuum_passes(vector_store, manifest, wal, false, mvcc_prune_margin);
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
        let f = vacuum(&mut store, None, None, &[], 1000);
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
        let f = vacuum(&mut store, None, None, &[bulk(b"FILES")], 1000);
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
        let f = vacuum(&mut store, None, None, &[bulk(b"(VERBOSE)")], 1000);
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
        let f = vacuum(&mut store, None, None, &[bulk(b"(FREEZE)")], 1000);
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
        let f = vacuum(&mut store, None, None, &[bulk(b"GRAPH"), bulk(b"g")], 1000);
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
        let f = vacuum(&mut store, None, None, &[bulk(b"BOGUS")], 1000);
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
        let f = vacuum(&mut store, None, None, &[bulk(b"(FREEZE)")], 1000);
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
pub fn vacuum_vector(
    vector_store: &mut crate::vector::store::VectorStore,
    args: &[Frame],
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
                    let idx = match vector_store.get_index_mut(name.as_ref()) {
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

    // Check the index exists.
    if vector_store.get_index(name.as_ref()).is_none() {
        return Frame::Error(Bytes::from_static(b"ERR unknown vector index"));
    }

    // Check if merge mode is NONE.
    if let Some(idx) = vector_store.get_index(name.as_ref()) {
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

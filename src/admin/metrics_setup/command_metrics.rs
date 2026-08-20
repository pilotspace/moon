//! Per-command counters, latency histograms, and the cached-handle fast path.
//!
//! Split out of the former single-file `metrics_setup.rs` (moon#479, file-size
//! ceiling). Holds the label sanitizer (the Prometheus cardinality guard) and
//! every `record_command*` entry point, unchanged.

use std::sync::atomic::Ordering;

use metrics::{counter, histogram};

use crate::admin::metrics_setup::{METRICS_INITIALIZED, bump_total_commands};

// ── Command metrics ─────────────────────────────────────────────────────

/// Returns true if the Prometheus metrics exporter is active.
/// Use this to gate expensive timing operations on the hot path.
#[inline]
pub fn is_metrics_enabled() -> bool {
    METRICS_INITIALIZED.load(Ordering::Relaxed)
}

/// Sanitize a command name for use as a Prometheus label.
///
/// Prevents unbounded label cardinality (DoS vector): only ASCII-alpha
/// commands up to 20 chars (longest Redis command) are accepted. Everything
/// else maps to the static `"unknown"` label.
///
/// Zero-allocation: uses a stack buffer for case-insensitive matching
/// instead of `to_ascii_lowercase()` which allocates on every call.
#[inline]
fn sanitize_cmd_label(cmd: &str) -> &'static str {
    if cmd.len() > 20 || cmd.is_empty() {
        return "unknown";
    }
    if !cmd.bytes().all(|b| b.is_ascii_alphabetic() || b == b'.') {
        return "unknown";
    }
    // Stack-allocated lowercase: avoids heap allocation on the hot path.
    let mut buf = [0u8; 20];
    let bytes = cmd.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        buf[i] = b.to_ascii_lowercase();
    }
    // SAFETY: we validated all bytes are ASCII alphabetic or '.', so UTF-8 is guaranteed.
    let lowered = std::str::from_utf8(&buf[..cmd.len()]).unwrap_or("unknown");
    // Map to a static string to avoid per-call allocation.
    // The match covers all commands Moon dispatches; anything else is "unknown".
    match lowered {
        // String
        "get" => "get",
        "set" => "set",
        "mget" => "mget",
        "mset" => "mset",
        "append" => "append",
        "incr" => "incr",
        "incrby" => "incrby",
        "incrbyfloat" => "incrbyfloat",
        "decr" => "decr",
        "decrby" => "decrby",
        "getrange" => "getrange",
        "setrange" => "setrange",
        "strlen" => "strlen",
        "setnx" => "setnx",
        "setex" => "setex",
        "psetex" => "psetex",
        "msetnx" => "msetnx",
        "getset" => "getset",
        "getdel" => "getdel",
        "getex" => "getex",
        "substr" => "substr",
        "lcs" => "lcs",
        // Key
        "del" => "del",
        "exists" => "exists",
        "expire" => "expire",
        "expireat" => "expireat",
        "pexpire" => "pexpire",
        "pexpireat" => "pexpireat",
        "expiretime" => "expiretime",
        "pexpiretime" => "pexpiretime",
        "ttl" => "ttl",
        "pttl" => "pttl",
        "persist" => "persist",
        "type" => "type",
        "rename" => "rename",
        "renamenx" => "renamenx",
        "keys" => "keys",
        "scan" => "scan",
        "randomkey" => "randomkey",
        "unlink" => "unlink",
        "object" => "object",
        "dump" => "dump",
        "restore" => "restore",
        "sort" => "sort",
        "touch" => "touch",
        "copy" => "copy",
        "wait" => "wait",
        // Hash
        "hget" => "hget",
        "hset" => "hset",
        "hdel" => "hdel",
        "hexists" => "hexists",
        "hgetall" => "hgetall",
        "hincrby" => "hincrby",
        "hincrbyfloat" => "hincrbyfloat",
        "hkeys" => "hkeys",
        "hvals" => "hvals",
        "hlen" => "hlen",
        "hmget" => "hmget",
        "hmset" => "hmset",
        "hsetnx" => "hsetnx",
        "hrandfield" => "hrandfield",
        "hscan" => "hscan",
        // List
        "lpush" => "lpush",
        "rpush" => "rpush",
        "lpop" => "lpop",
        "rpop" => "rpop",
        "llen" => "llen",
        "lrange" => "lrange",
        "lindex" => "lindex",
        "lset" => "lset",
        "linsert" => "linsert",
        "lrem" => "lrem",
        "ltrim" => "ltrim",
        "rpoplpush" => "rpoplpush",
        "lmove" => "lmove",
        "lpos" => "lpos",
        "lmpop" => "lmpop",
        "lpushx" => "lpushx",
        "rpushx" => "rpushx",
        // Set
        "sadd" => "sadd",
        "srem" => "srem",
        "smembers" => "smembers",
        "sismember" => "sismember",
        "smismember" => "smismember",
        "scard" => "scard",
        "srandmember" => "srandmember",
        "spop" => "spop",
        "sunion" => "sunion",
        "sinter" => "sinter",
        "sdiff" => "sdiff",
        "sunionstore" => "sunionstore",
        "sinterstore" => "sinterstore",
        "sdiffstore" => "sdiffstore",
        "sintercard" => "sintercard",
        "sscan" => "sscan",
        "smove" => "smove",
        // Sorted Set
        "zadd" => "zadd",
        "zrem" => "zrem",
        "zscore" => "zscore",
        "zrank" => "zrank",
        "zrevrank" => "zrevrank",
        "zrange" => "zrange",
        "zrevrange" => "zrevrange",
        "zrangebyscore" => "zrangebyscore",
        "zrevrangebyscore" => "zrevrangebyscore",
        "zrangebylex" => "zrangebylex",
        "zrevrangebylex" => "zrevrangebylex",
        "zcard" => "zcard",
        "zcount" => "zcount",
        "zlexcount" => "zlexcount",
        "zincrby" => "zincrby",
        "zpopmin" => "zpopmin",
        "zpopmax" => "zpopmax",
        "zrandmember" => "zrandmember",
        "zrangestore" => "zrangestore",
        "zunionstore" => "zunionstore",
        "zinterstore" => "zinterstore",
        "zdiffstore" => "zdiffstore",
        "zmscore" => "zmscore",
        "zunion" => "zunion",
        "zinter" => "zinter",
        "zdiff" => "zdiff",
        "zscan" => "zscan",
        // Stream
        "xadd" => "xadd",
        "xlen" => "xlen",
        "xrange" => "xrange",
        "xrevrange" => "xrevrange",
        "xread" => "xread",
        "xinfo" => "xinfo",
        "xtrim" => "xtrim",
        "xack" => "xack",
        "xclaim" => "xclaim",
        "xdel" => "xdel",
        "xgroup" => "xgroup",
        "xreadgroup" => "xreadgroup",
        "xpending" => "xpending",
        "xautoclaim" => "xautoclaim",
        "xsetid" => "xsetid",
        // Pub/Sub
        "subscribe" => "subscribe",
        "unsubscribe" => "unsubscribe",
        "publish" => "publish",
        "psubscribe" => "psubscribe",
        "punsubscribe" => "punsubscribe",
        "ssubscribe" => "ssubscribe",
        "sunsubscribe" => "sunsubscribe",
        "pubsub" => "pubsub",
        // Server/Connection
        "ping" => "ping",
        "echo" => "echo",
        "quit" => "quit",
        "info" => "info",
        "dbsize" => "dbsize",
        "flushdb" => "flushdb",
        "flushall" => "flushall",
        "select" => "select",
        "auth" => "auth",
        "command" => "command",
        "config" => "config",
        "client" => "client",
        "debug" => "debug",
        "time" => "time",
        "slowlog" => "slowlog",
        "hello" => "hello",
        "reset" => "reset",
        "swapdb" => "swapdb",
        "lastsave" => "lastsave",
        "save" => "save",
        "bgsave" => "bgsave",
        "bgrewriteaof" => "bgrewriteaof",
        "multi" => "multi",
        "exec" => "exec",
        "discard" => "discard",
        "watch" => "watch",
        "unwatch" => "unwatch",
        // Scripting
        "eval" => "eval",
        "evalsha" => "evalsha",
        "script" => "script",
        // Vector search
        "ft.create" => "ft.create",
        "ft.dropindex" => "ft.dropindex",
        "ft.info" => "ft.info",
        "ft.search" => "ft.search",
        "ft.compact" => "ft.compact",
        "ft.cachesearch" => "ft.cachesearch",
        "ft.recommend" => "ft.recommend",
        "ft.navigate" => "ft.navigate",
        "ft.expand" => "ft.expand",
        // ACL
        "acl" => "acl",
        // Cluster
        "cluster" => "cluster",
        // Blocking
        "blpop" => "blpop",
        "brpop" => "brpop",
        "blmove" => "blmove",
        "blmpop" => "blmpop",
        "bzpopmin" => "bzpopmin",
        "bzpopmax" => "bzpopmax",
        _ => "unknown",
    }
}

/// Record a command execution.
#[inline]
pub fn record_command(cmd: &str, latency_us: u64) {
    bump_total_commands();
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    let label = sanitize_cmd_label(cmd);
    counter!("moon_commands_total", "cmd" => label).increment(1);
    histogram!("moon_command_duration_microseconds", "cmd" => label).record(latency_us as f64);
}

/// Record a command execution **without** latency sampling.
///
/// Hot-path variant for the 15/16 of commands that skip the `Instant::now()`
/// measurement under 1-in-16 sampling. Keeps `TOTAL_COMMANDS` + per-cmd
/// counter accurate (used by INFO) while avoiding the histogram record that
/// would otherwise bias the distribution with a zero value.
#[inline]
pub fn record_command_no_latency(cmd: &str) {
    bump_total_commands();
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    let label = sanitize_cmd_label(cmd);
    counter!("moon_commands_total", "cmd" => label).increment(1);
}

/// Record a command error.
#[inline]
pub fn record_command_error(cmd: &str) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_command_errors_total", "cmd" => sanitize_cmd_label(cmd)).increment(1);
}

/// Per-connection cached Prometheus metric handles.
///
/// The `metrics!` macros call `with_recorder(|rec| rec.register_counter(...))`
/// on every invocation, which for `metrics-exporter-prometheus` resolves to a
/// DashMap lookup keyed on `(name, labels)`. Under a steady single-command
/// workload (e.g. redis-benchmark -t set) the label is constant, so the lookup
/// is pure overhead. The flamegraph attributes ~6% of shard CPU to the
/// recorder backend on SET p=64.
///
/// This struct caches the last-seen command's counter / histogram / error
/// counter handles, keyed on the raw command bytes. Cache hit avoids both
/// `sanitize_cmd_label` and the registry lookup — the hot path collapses to
/// one atomic fetch + two atomic handle operations.
///
/// Held by `ConnectionState` (`!Send` because the handler is thread-pinned),
/// so there is no cross-thread synchronisation.
pub struct CachedMetricsHandles {
    /// Raw command bytes of the most recent call. Empty on init.
    // `pub(super)` restores exactly the visibility this field had while the
    // module was one file: reachable from `metrics_setup` (where the unit
    // tests live), nothing wider.
    pub(super) last_cmd: smallvec::SmallVec<[u8; 20]>,
    counter: metrics::Counter,
    histogram: metrics::Histogram,
    error_counter: metrics::Counter,
}

impl Default for CachedMetricsHandles {
    fn default() -> Self {
        Self {
            last_cmd: smallvec::SmallVec::new(),
            counter: metrics::Counter::noop(),
            histogram: metrics::Histogram::noop(),
            error_counter: metrics::Counter::noop(),
        }
    }
}

impl CachedMetricsHandles {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Ensure the cached handles refer to `cmd`. No-op when the previous
    /// call used the same bytes (cache hit).
    #[inline]
    pub(super) fn ensure(&mut self, cmd: &[u8]) {
        if self.last_cmd.as_slice() == cmd {
            return;
        }
        let cmd_str = std::str::from_utf8(cmd).unwrap_or("unknown");
        let label = sanitize_cmd_label(cmd_str);
        self.last_cmd.clear();
        self.last_cmd.extend_from_slice(cmd);
        self.counter = counter!("moon_commands_total", "cmd" => label);
        self.histogram = histogram!("moon_command_duration_microseconds", "cmd" => label);
        self.error_counter = counter!("moon_command_errors_total", "cmd" => label);
    }
}

/// Record a command execution with latency using a per-connection handle
/// cache. Functionally identical to [`record_command`] but avoids the
/// recorder-backend DashMap lookup on cache hit.
#[inline]
pub fn record_command_cached(cmd: &str, latency_us: u64, cache: &mut CachedMetricsHandles) {
    bump_total_commands();
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    cache.ensure(cmd.as_bytes());
    cache.counter.increment(1);
    cache.histogram.record(latency_us as f64);
}

/// Record a command execution without latency using a per-connection handle
/// cache. Functionally identical to [`record_command_no_latency`] but avoids
/// the recorder-backend DashMap lookup on cache hit.
#[inline]
pub fn record_command_no_latency_cached(cmd: &str, cache: &mut CachedMetricsHandles) {
    bump_total_commands();
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    cache.ensure(cmd.as_bytes());
    cache.counter.increment(1);
}

/// Record a command error using a per-connection handle cache.
/// Functionally identical to [`record_command_error`] but avoids the
/// recorder-backend DashMap lookup on cache hit.
#[inline]
pub fn record_command_error_cached(cmd: &str, cache: &mut CachedMetricsHandles) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    cache.ensure(cmd.as_bytes());
    cache.error_counter.increment(1);
}

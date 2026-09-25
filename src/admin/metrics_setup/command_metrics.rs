//! Per-command counters, latency histograms, and the cached-handle fast path.
//!
//! Split out of the former single-file `metrics_setup.rs` (moon#479, file-size
//! ceiling). Holds the label sanitizer (the Prometheus cardinality guard) and
//! every `record_command*` entry point, unchanged.

use std::sync::atomic::Ordering;

use metrics::histogram;

use crate::admin::metrics_setup::sharded::{bump_cmd_call, bump_cmd_error};
use crate::admin::metrics_setup::{METRICS_INITIALIZED, bump_total_commands};

// ── Command metrics ─────────────────────────────────────────────────────

/// Returns true if the Prometheus metrics exporter is active.
/// Use this to gate expensive timing operations on the hot path.
#[inline]
pub fn is_metrics_enabled() -> bool {
    METRICS_INITIALIZED.load(Ordering::Relaxed)
}

/// Number of distinct `cmd` labels, `"unknown"` included (moon#1178).
pub(super) const CMD_LABEL_COUNT: usize = 194;

/// Every label `moon_commands_total{cmd}` can carry, by label index; index 0
/// is `"unknown"`. The index is what the per-thread counters in `sharded.rs`
/// are keyed by, so a command's count never touches the recorder on the hot
/// path.
pub(super) static CMD_LABELS: [&str; CMD_LABEL_COUNT] = [
    "unknown",
    "get",
    "set",
    "mget",
    "mset",
    "append",
    "incr",
    "incrby",
    "incrbyfloat",
    "decr",
    "decrby",
    "getrange",
    "setrange",
    "strlen",
    "setnx",
    "setex",
    "psetex",
    "msetnx",
    "getset",
    "getdel",
    "getex",
    "substr",
    "lcs",
    "del",
    "exists",
    "expire",
    "expireat",
    "pexpire",
    "pexpireat",
    "expiretime",
    "pexpiretime",
    "ttl",
    "pttl",
    "persist",
    "type",
    "rename",
    "renamenx",
    "keys",
    "scan",
    "randomkey",
    "unlink",
    "object",
    "dump",
    "restore",
    "sort",
    "touch",
    "copy",
    "wait",
    "hget",
    "hset",
    "hdel",
    "hexists",
    "hgetall",
    "hincrby",
    "hincrbyfloat",
    "hkeys",
    "hvals",
    "hlen",
    "hmget",
    "hmset",
    "hsetnx",
    "hrandfield",
    "hscan",
    "lpush",
    "rpush",
    "lpop",
    "rpop",
    "llen",
    "lrange",
    "lindex",
    "lset",
    "linsert",
    "lrem",
    "ltrim",
    "rpoplpush",
    "lmove",
    "lpos",
    "lmpop",
    "lpushx",
    "rpushx",
    "sadd",
    "srem",
    "smembers",
    "sismember",
    "smismember",
    "scard",
    "srandmember",
    "spop",
    "sunion",
    "sinter",
    "sdiff",
    "sunionstore",
    "sinterstore",
    "sdiffstore",
    "sintercard",
    "sscan",
    "smove",
    "zadd",
    "zrem",
    "zscore",
    "zrank",
    "zrevrank",
    "zrange",
    "zrevrange",
    "zrangebyscore",
    "zrevrangebyscore",
    "zrangebylex",
    "zrevrangebylex",
    "zcard",
    "zcount",
    "zlexcount",
    "zincrby",
    "zpopmin",
    "zpopmax",
    "zrandmember",
    "zrangestore",
    "zunionstore",
    "zinterstore",
    "zdiffstore",
    "zmscore",
    "zunion",
    "zinter",
    "zdiff",
    "zscan",
    "xadd",
    "xlen",
    "xrange",
    "xrevrange",
    "xread",
    "xinfo",
    "xtrim",
    "xack",
    "xclaim",
    "xdel",
    "xgroup",
    "xreadgroup",
    "xpending",
    "xautoclaim",
    "xsetid",
    "subscribe",
    "unsubscribe",
    "publish",
    "psubscribe",
    "punsubscribe",
    "ssubscribe",
    "sunsubscribe",
    "pubsub",
    "ping",
    "echo",
    "quit",
    "info",
    "dbsize",
    "flushdb",
    "flushall",
    "select",
    "auth",
    "command",
    "config",
    "client",
    "debug",
    "time",
    "slowlog",
    "hello",
    "reset",
    "swapdb",
    "lastsave",
    "save",
    "bgsave",
    "bgrewriteaof",
    "multi",
    "exec",
    "discard",
    "watch",
    "unwatch",
    "eval",
    "evalsha",
    "script",
    "ft.create",
    "ft.dropindex",
    "ft.info",
    "ft.search",
    "ft.compact",
    "ft.cachesearch",
    "ft.recommend",
    "ft.navigate",
    "ft.expand",
    "acl",
    "cluster",
    "blpop",
    "brpop",
    "blmove",
    "blmpop",
    "bzpopmin",
    "bzpopmax",
];

/// The label index of `cmd` (see [`CMD_LABELS`]): the Prometheus cardinality
/// guard. Only ASCII-alpha (and `.`) names up to 20 bytes can map to a known
/// command; everything else is `"unknown"` (0).
///
/// Zero-allocation: lowercases into a stack buffer.
#[inline]
pub(super) fn cmd_label_index(cmd: &[u8]) -> usize {
    if cmd.len() > 20 || cmd.is_empty() {
        return 0;
    }
    if !cmd.iter().all(|&b| b.is_ascii_alphabetic() || b == b'.') {
        return 0;
    }
    let mut buf = [0u8; 20];
    for (d, s) in buf.iter_mut().zip(cmd) {
        *d = s.to_ascii_lowercase();
    }
    // Validated ASCII above, so this cannot fail; "" maps to "unknown".
    let lowered = std::str::from_utf8(&buf[..cmd.len()]).unwrap_or("");
    match lowered {
        "get" => 1,
        "set" => 2,
        "mget" => 3,
        "mset" => 4,
        "append" => 5,
        "incr" => 6,
        "incrby" => 7,
        "incrbyfloat" => 8,
        "decr" => 9,
        "decrby" => 10,
        "getrange" => 11,
        "setrange" => 12,
        "strlen" => 13,
        "setnx" => 14,
        "setex" => 15,
        "psetex" => 16,
        "msetnx" => 17,
        "getset" => 18,
        "getdel" => 19,
        "getex" => 20,
        "substr" => 21,
        "lcs" => 22,
        "del" => 23,
        "exists" => 24,
        "expire" => 25,
        "expireat" => 26,
        "pexpire" => 27,
        "pexpireat" => 28,
        "expiretime" => 29,
        "pexpiretime" => 30,
        "ttl" => 31,
        "pttl" => 32,
        "persist" => 33,
        "type" => 34,
        "rename" => 35,
        "renamenx" => 36,
        "keys" => 37,
        "scan" => 38,
        "randomkey" => 39,
        "unlink" => 40,
        "object" => 41,
        "dump" => 42,
        "restore" => 43,
        "sort" => 44,
        "touch" => 45,
        "copy" => 46,
        "wait" => 47,
        "hget" => 48,
        "hset" => 49,
        "hdel" => 50,
        "hexists" => 51,
        "hgetall" => 52,
        "hincrby" => 53,
        "hincrbyfloat" => 54,
        "hkeys" => 55,
        "hvals" => 56,
        "hlen" => 57,
        "hmget" => 58,
        "hmset" => 59,
        "hsetnx" => 60,
        "hrandfield" => 61,
        "hscan" => 62,
        "lpush" => 63,
        "rpush" => 64,
        "lpop" => 65,
        "rpop" => 66,
        "llen" => 67,
        "lrange" => 68,
        "lindex" => 69,
        "lset" => 70,
        "linsert" => 71,
        "lrem" => 72,
        "ltrim" => 73,
        "rpoplpush" => 74,
        "lmove" => 75,
        "lpos" => 76,
        "lmpop" => 77,
        "lpushx" => 78,
        "rpushx" => 79,
        "sadd" => 80,
        "srem" => 81,
        "smembers" => 82,
        "sismember" => 83,
        "smismember" => 84,
        "scard" => 85,
        "srandmember" => 86,
        "spop" => 87,
        "sunion" => 88,
        "sinter" => 89,
        "sdiff" => 90,
        "sunionstore" => 91,
        "sinterstore" => 92,
        "sdiffstore" => 93,
        "sintercard" => 94,
        "sscan" => 95,
        "smove" => 96,
        "zadd" => 97,
        "zrem" => 98,
        "zscore" => 99,
        "zrank" => 100,
        "zrevrank" => 101,
        "zrange" => 102,
        "zrevrange" => 103,
        "zrangebyscore" => 104,
        "zrevrangebyscore" => 105,
        "zrangebylex" => 106,
        "zrevrangebylex" => 107,
        "zcard" => 108,
        "zcount" => 109,
        "zlexcount" => 110,
        "zincrby" => 111,
        "zpopmin" => 112,
        "zpopmax" => 113,
        "zrandmember" => 114,
        "zrangestore" => 115,
        "zunionstore" => 116,
        "zinterstore" => 117,
        "zdiffstore" => 118,
        "zmscore" => 119,
        "zunion" => 120,
        "zinter" => 121,
        "zdiff" => 122,
        "zscan" => 123,
        "xadd" => 124,
        "xlen" => 125,
        "xrange" => 126,
        "xrevrange" => 127,
        "xread" => 128,
        "xinfo" => 129,
        "xtrim" => 130,
        "xack" => 131,
        "xclaim" => 132,
        "xdel" => 133,
        "xgroup" => 134,
        "xreadgroup" => 135,
        "xpending" => 136,
        "xautoclaim" => 137,
        "xsetid" => 138,
        "subscribe" => 139,
        "unsubscribe" => 140,
        "publish" => 141,
        "psubscribe" => 142,
        "punsubscribe" => 143,
        "ssubscribe" => 144,
        "sunsubscribe" => 145,
        "pubsub" => 146,
        "ping" => 147,
        "echo" => 148,
        "quit" => 149,
        "info" => 150,
        "dbsize" => 151,
        "flushdb" => 152,
        "flushall" => 153,
        "select" => 154,
        "auth" => 155,
        "command" => 156,
        "config" => 157,
        "client" => 158,
        "debug" => 159,
        "time" => 160,
        "slowlog" => 161,
        "hello" => 162,
        "reset" => 163,
        "swapdb" => 164,
        "lastsave" => 165,
        "save" => 166,
        "bgsave" => 167,
        "bgrewriteaof" => 168,
        "multi" => 169,
        "exec" => 170,
        "discard" => 171,
        "watch" => 172,
        "unwatch" => 173,
        "eval" => 174,
        "evalsha" => 175,
        "script" => 176,
        "ft.create" => 177,
        "ft.dropindex" => 178,
        "ft.info" => 179,
        "ft.search" => 180,
        "ft.compact" => 181,
        "ft.cachesearch" => 182,
        "ft.recommend" => 183,
        "ft.navigate" => 184,
        "ft.expand" => 185,
        "acl" => 186,
        "cluster" => 187,
        "blpop" => 188,
        "brpop" => 189,
        "blmove" => 190,
        "blmpop" => 191,
        "bzpopmin" => 192,
        "bzpopmax" => 193,
        _ => 0,
    }
}

thread_local! {
    /// This thread's `moon_command_duration_microseconds{cmd}` handles, by
    /// label index, registered on first use (moon#1178). The histogram is
    /// recorded for the 1-in-16 sampled commands only; caching its handle per
    /// thread means a mixed pipeline never re-registers it.
    static CMD_HISTOGRAMS: std::cell::RefCell<Vec<Option<metrics::Histogram>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// The duration histogram for label index `idx` on this thread.
#[inline]
fn cmd_histogram(idx: usize) -> metrics::Histogram {
    CMD_HISTOGRAMS.with_borrow_mut(|hs| {
        if hs.len() < CMD_LABEL_COUNT {
            hs.resize(CMD_LABEL_COUNT, None);
        }
        let label = CMD_LABELS.get(idx).copied().unwrap_or("unknown");
        hs[idx.min(CMD_LABEL_COUNT - 1)]
            .get_or_insert_with(|| histogram!("moon_command_duration_microseconds", "cmd" => label))
            .clone()
    })
}

/// Record a command execution.
#[inline]
pub fn record_command(cmd: &str, latency_us: u64) {
    bump_total_commands();
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    let idx = cmd_label_index(cmd.as_bytes());
    bump_cmd_call(idx);
    cmd_histogram(idx).record(latency_us as f64);
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
    bump_cmd_call(cmd_label_index(cmd.as_bytes()));
}

/// Record a command error.
#[inline]
pub fn record_command_error(cmd: &str) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    bump_cmd_error(cmd_label_index(cmd.as_bytes()));
}

/// Per-connection cache of the last command's label index and duration
/// histogram handle.
///
/// moon#1178: the counts no longer go through a recorder handle at all — they
/// land in this thread's slot of `sharded::CMD_COUNTS` and are published at
/// scrape — so this cache only saves the label match (one byte compare on a
/// hit) and the histogram handle for the sampled 1-in-16. It used to cache
/// ONE command's three registry handles, so every command switch in a mixed
/// pipeline re-registered all three (a registry lookup each), and the cached
/// counter was one `AtomicU64` every shard incremented.
///
/// Held by `ConnectionState` (`!Send` because the handler is thread-pinned),
/// so there is no cross-thread synchronisation.
pub struct CachedMetricsHandles {
    /// Raw command bytes of the most recent call. Empty on init.
    // `pub(super)` restores exactly the visibility this field had while the
    // module was one file: reachable from `metrics_setup` (where the unit
    // tests live), nothing wider.
    pub(super) last_cmd: smallvec::SmallVec<[u8; 20]>,
    /// Label index of `last_cmd` (see [`CMD_LABELS`]).
    idx: usize,
    histogram: metrics::Histogram,
}

impl Default for CachedMetricsHandles {
    fn default() -> Self {
        Self {
            last_cmd: smallvec::SmallVec::new(),
            idx: 0,
            histogram: metrics::Histogram::noop(),
        }
    }
}

impl CachedMetricsHandles {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Ensure the cache refers to `cmd`. No-op when the previous call used
    /// the same bytes (cache hit).
    #[inline]
    pub(super) fn ensure(&mut self, cmd: &[u8]) {
        if self.last_cmd.as_slice() == cmd {
            return;
        }
        self.last_cmd.clear();
        self.last_cmd.extend_from_slice(cmd);
        self.idx = cmd_label_index(cmd);
        self.histogram = cmd_histogram(self.idx);
    }

    /// One execution of `cmd`: bump its count and, when the call was a
    /// sampled one, record its duration. The single sink behind
    /// [`LatencyProbe::observe`](crate::admin::metrics_setup::LatencyProbe::observe);
    /// does NOT touch `total_commands_processed` (the probe batches that).
    #[inline]
    pub(super) fn observe(&mut self, cmd: &[u8], elapsed_us: Option<u64>) {
        if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
            return;
        }
        self.ensure(cmd);
        bump_cmd_call(self.idx);
        if let Some(us) = elapsed_us {
            self.histogram.record(us as f64);
        }
    }
}

/// Record a command execution with latency using a per-connection handle
/// cache. Functionally identical to [`record_command`].
#[inline]
pub fn record_command_cached(cmd: &str, latency_us: u64, cache: &mut CachedMetricsHandles) {
    bump_total_commands();
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    cache.ensure(cmd.as_bytes());
    bump_cmd_call(cache.idx);
    cache.histogram.record(latency_us as f64);
}

/// Record a command execution without latency using a per-connection handle
/// cache. Functionally identical to [`record_command_no_latency`].
#[inline]
pub fn record_command_no_latency_cached(cmd: &str, cache: &mut CachedMetricsHandles) {
    bump_total_commands();
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    cache.ensure(cmd.as_bytes());
    bump_cmd_call(cache.idx);
}

/// Record a command error using a per-connection handle cache.
/// Functionally identical to [`record_command_error`].
#[inline]
pub fn record_command_error_cached(cmd: &str, cache: &mut CachedMetricsHandles) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    cache.ensure(cmd.as_bytes());
    bump_cmd_error(cache.idx);
}

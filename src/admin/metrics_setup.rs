//! Prometheus metrics initialization and recording helpers.
//!
//! Uses the `metrics` facade crate so metric recording is a single atomic
//! operation on the hot path (counter increment or histogram observation).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use metrics::{counter, gauge, histogram};

static METRICS_INITIALIZED: AtomicBool = AtomicBool::new(false);
static SERVER_READY: AtomicBool = AtomicBool::new(false);

/// Mark the server as ready (called once after all shards are accepting).
pub fn set_server_ready() {
    SERVER_READY.store(true, Ordering::Release);
}

/// Check if the server is ready (for READYZ health check).
pub fn is_server_ready() -> bool {
    SERVER_READY.load(Ordering::Acquire)
}

// ── Lightweight atomic counters for INFO ────────────────────────────────
// These counters work even when the Prometheus exporter is disabled
// (admin_port=0), so INFO always returns meaningful stats.
static TOTAL_COMMANDS: AtomicU64 = AtomicU64::new(0);
static TOTAL_CONNECTIONS: AtomicU64 = AtomicU64::new(0);

/// Initialize the Prometheus metrics exporter and admin HTTP server.
///
/// Must be called once before any metrics recording. Spawns a custom admin
/// HTTP server on `addr` that serves `/metrics`, `/healthz`, and `/readyz`.
///
/// Returns an `Arc<AtomicBool>` readiness flag. Set it to `true` once all
/// shards have finished persistence recovery to make `/readyz` return 200.
pub fn init_metrics(admin_port: u16, bind: &str) -> Option<std::sync::Arc<AtomicBool>> {
    if admin_port == 0 {
        return None;
    }

    let addr_str = format!("{}:{}", bind, admin_port);
    let addr: std::net::SocketAddr = addr_str.parse().unwrap_or_else(|_| {
        tracing::warn!(
            "Invalid admin bind address '{}', using 0.0.0.0:{}",
            addr_str,
            admin_port
        );
        std::net::SocketAddr::from(([0, 0, 0, 0], admin_port))
    });

    // Build recorder without starting the built-in HTTP listener
    if METRICS_INITIALIZED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let prometheus_handle = recorder.handle();

        // Install as the global metrics recorder
        if let Err(e) = metrics::set_global_recorder(recorder) {
            tracing::error!("Failed to set global metrics recorder: {}", e);
            return None;
        }

        let ready = std::sync::Arc::new(AtomicBool::new(false));
        crate::admin::http_server::spawn_admin_server(addr, prometheus_handle, ready.clone());
        Some(ready)
    } else {
        None
    }
}

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
#[inline]
fn sanitize_cmd_label(cmd: &str) -> &'static str {
    if cmd.len() > 20 || cmd.is_empty() {
        return "unknown";
    }
    if !cmd.bytes().all(|b| b.is_ascii_alphabetic() || b == b'.') {
        return "unknown";
    }
    // Map to a static string to avoid per-call allocation.
    // The match covers all commands Moon dispatches; anything else is "unknown".
    match cmd.to_ascii_lowercase().as_str() {
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
    TOTAL_COMMANDS.fetch_add(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    let label = sanitize_cmd_label(cmd);
    counter!("moon_commands_total", "cmd" => label).increment(1);
    histogram!("moon_command_duration_microseconds", "cmd" => label).record(latency_us as f64);
}

/// Record a command error.
#[inline]
pub fn record_command_error(cmd: &str) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_command_errors_total", "cmd" => sanitize_cmd_label(cmd)).increment(1);
}

// ── Connection metrics ──────────────────────────────────────────────────

/// Record a new client connection.
#[inline]
pub fn record_connection_opened() {
    TOTAL_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_connections_total").increment(1);
    gauge!("moon_connected_clients").increment(1.0);
}

/// Record a client disconnection.
#[inline]
pub fn record_connection_closed() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    gauge!("moon_connected_clients").decrement(1.0);
}

// ── Keyspace metrics ────────────────────────────────────────────────────

/// Record keyspace hit/miss.
#[inline]
pub fn record_keyspace_hit() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_keyspace_hits_total").increment(1);
}

#[inline]
pub fn record_keyspace_miss() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_keyspace_misses_total").increment(1);
}

// ── Eviction metrics ────────────────────────────────────────────────────

/// Record an eviction event.
#[inline]
pub fn record_eviction() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_evicted_keys_total").increment(1);
}

// ── Persistence metrics ─────────────────────────────────────────────────

/// Record an AOF fsync duration.
#[inline]
pub fn record_aof_fsync(duration_us: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    histogram!("moon_aof_fsync_duration_microseconds").record(duration_us as f64);
}

/// Record a WAL segment rotation.
#[inline]
pub fn record_wal_rotation() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_wal_rotations_total").increment(1);
}

// ── Shard metrics ───────────────────────────────────────────────────────

/// Record SPSC queue drain batch size.
#[inline]
pub fn record_spsc_drain(shard_id: usize, count: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    let shard = itoa::Buffer::new().format(shard_id).to_string();
    histogram!("moon_spsc_drain_batch_size", "shard" => shard).record(count as f64);
}

// ── Pub/Sub metrics ─────────────────────────────────────────────────────

/// Record a pub/sub message published.
#[inline]
pub fn record_pubsub_published() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_pubsub_messages_published_total").increment(1);
}

/// Record a slow subscriber drop.
#[inline]
pub fn record_pubsub_slow_drop() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_pubsub_slow_subscriber_drops_total").increment(1);
}

// ── Replication metrics ─────────────────────────────────────────────

/// Record replication lag (byte offset and time-based).
///
/// Called periodically when replication is active. When no replicas are
/// connected, the gauges remain at their last-set values (or zero).
#[inline]
pub fn record_replication_lag(bytes: u64, ms: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    gauge!("moon_replication_lag_bytes").set(bytes as f64);
    gauge!("moon_replication_lag_ms").set(ms as f64);
}

// ── Memory metrics ──────────────────────────────────────────────────────

/// Update RSS gauge (called periodically by shard timer).
#[inline]
pub fn update_rss_bytes(rss: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    gauge!("moon_rss_bytes").set(rss as f64);
}

// ── INFO helpers ────────────────────────────────────────────────────────

/// Total commands processed since server start (for INFO Stats).
#[inline]
pub fn total_commands_processed() -> u64 {
    TOTAL_COMMANDS.load(Ordering::Relaxed)
}

/// Total connections received since server start (for INFO Stats).
#[inline]
pub fn total_connections_received() -> u64 {
    TOTAL_CONNECTIONS.load(Ordering::Relaxed)
}

/// Read process CPU usage via `getrusage(RUSAGE_SELF)`.
///
/// Returns `(used_cpu_sys, used_cpu_user)` in seconds (f64).
/// On non-Linux platforms returns `(0.0, 0.0)`.
#[cfg(target_os = "linux")]
pub fn get_cpu_usage() -> (f64, f64) {
    use std::mem::MaybeUninit;
    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` writes a valid `rusage` struct to the pointer on
    // success (returns 0). RUSAGE_SELF is always valid. We only read the
    // struct after confirming success.
    let ret = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if ret == 0 {
        // SAFETY: getrusage returned 0, so the struct is fully initialized.
        let ru = unsafe { usage.assume_init() };
        let sys = ru.ru_stime.tv_sec as f64 + ru.ru_stime.tv_usec as f64 / 1_000_000.0;
        let user = ru.ru_utime.tv_sec as f64 + ru.ru_utime.tv_usec as f64 / 1_000_000.0;
        (sys, user)
    } else {
        (0.0, 0.0)
    }
}

#[cfg(not(target_os = "linux"))]
pub fn get_cpu_usage() -> (f64, f64) {
    (0.0, 0.0)
}

// ── Global SLOWLOG ─────────────────────────────────────────────────────

/// Global slowlog instance accessible from any handler thread.
///
/// Initialized lazily with default thresholds. `init_global_slowlog` should
/// be called from main to apply user-configured values.
static GLOBAL_SLOWLOG: once_cell::sync::Lazy<crate::admin::slowlog::Slowlog> =
    once_cell::sync::Lazy::new(|| crate::admin::slowlog::Slowlog::new(128, 10_000));

/// Initialize the global slowlog with user-configured values.
///
/// Must be called before any command processing. If called after commands
/// have already been recorded, the old entries are lost (new instance).
/// In practice this is called once from main() before shards start.
pub fn init_global_slowlog(max_len: usize, threshold_us: u64) {
    // Force initialization of the Lazy with default, then reconfigure.
    // Since Slowlog fields are behind a Mutex, we just reset.
    let sl = global_slowlog();
    sl.reconfigure(max_len, threshold_us);
}

/// Get a reference to the global slowlog.
#[inline]
pub fn global_slowlog() -> &'static crate::admin::slowlog::Slowlog {
    &GLOBAL_SLOWLOG
}

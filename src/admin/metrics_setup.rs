//! Prometheus metrics initialization and recording helpers.
//!
//! Uses the `metrics` facade crate so metric recording is a single atomic
//! operation on the hot path (counter increment or histogram observation).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use metrics::{Unit, counter, describe_gauge, gauge, histogram};

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
static CONNECTED_CLIENTS: AtomicU64 = AtomicU64::new(0);

// ── P6: WAL aggressive reclamation counters (read by P10 INFO emitter) ───
// Incremented by WalWriterV3::recycle_aggressive(). P10 reads these via the
// public getters below to populate the `# Reclamation` INFO section.
// Using relaxed ordering: these are monotonic event counters for monitoring,
// not synchronisation primitives.
static WAL_AGGRESSIVE_RECYCLE_SEGMENTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static WAL_AGGRESSIVE_RECYCLE_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Initialize the Prometheus metrics exporter and admin HTTP server.
///
/// Must be called once before any metrics recording. Spawns a custom admin
/// HTTP server on `addr` that serves `/metrics`, `/healthz`, and `/readyz`.
///
/// Returns an `Arc<AtomicBool>` readiness flag. Set it to `true` once all
/// shards have finished persistence recovery to make `/readyz` return 200.
///
/// When the `console` feature is enabled, the three hardening policies
/// (`auth`, `cors`, `rate`) are threaded into the server via the extra
/// arguments. Callers build the policies from `ServerConfig` in `main.rs`.
pub fn init_metrics(
    admin_port: u16,
    bind: &str,
    #[cfg(feature = "console")] auth: std::sync::Arc<crate::admin::auth::AuthPolicy>,
    #[cfg(feature = "console")] cors: std::sync::Arc<crate::admin::cors::CorsPolicy>,
    #[cfg(feature = "console")] rate_limit_rps: f64,
    #[cfg(feature = "console")] rate_limit_burst: f64,
) -> Option<std::sync::Arc<AtomicBool>> {
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
        crate::admin::http_server::spawn_admin_server(
            addr,
            prometheus_handle,
            ready.clone(),
            #[cfg(feature = "console")]
            auth,
            #[cfg(feature = "console")]
            cors,
            #[cfg(feature = "console")]
            rate_limit_rps,
            #[cfg(feature = "console")]
            rate_limit_burst,
        );
        // Register per-subsystem memory gauge and prime all 7 labels so
        // disabled subsystems still surface a zero-valued series.
        describe_gauge!(
            "moon_memory_bytes",
            Unit::Bytes,
            "Resident bytes per subsystem; sum approximates RSS"
        );
        prime_moon_memory_bytes();

        Some(ready)
    } else {
        None
    }
}

/// Prime all 7 `moon_memory_bytes{kind=...}` series with `0.0` so they
/// appear in `/metrics` output from the first scrape, even when subsystems
/// are feature-gated off or not yet initialized.
///
/// NOTE: This scrape path intentionally does NOT call `mallctl("epoch")`.
/// See the documented jemalloc leak at the `get_rss_bytes()` doc-comment
/// (~1 MB / 20 s growth). `allocator_overhead` is computed as
/// `max(0, RSS − sum(other 6))` — the same formula MEMORY DOCTOR uses.
fn prime_moon_memory_bytes() {
    for kind in [
        "dashtable",
        "hnsw",
        "csr",
        "wal",
        "sealed",
        "replication_backlog",
        "allocator_overhead",
    ] {
        gauge!("moon_memory_bytes", "kind" => kind).set(0.0);
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
    TOTAL_COMMANDS.fetch_add(1, Ordering::Relaxed);
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
    TOTAL_COMMANDS.fetch_add(1, Ordering::Relaxed);
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
    last_cmd: smallvec::SmallVec<[u8; 20]>,
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
    fn ensure(&mut self, cmd: &[u8]) {
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
    TOTAL_COMMANDS.fetch_add(1, Ordering::Relaxed);
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
    TOTAL_COMMANDS.fetch_add(1, Ordering::Relaxed);
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

// ── Connection metrics ──────────────────────────────────────────────────

/// Record a new client connection.
#[inline]
pub fn record_connection_opened() {
    TOTAL_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
    CONNECTED_CLIENTS.fetch_add(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_connections_total").increment(1);
    gauge!("moon_connected_clients").increment(1.0);
}

/// Record a client disconnection.
#[inline]
pub fn record_connection_closed() {
    CONNECTED_CLIENTS.fetch_sub(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    gauge!("moon_connected_clients").decrement(1.0);
}

/// Current number of connected clients (for INFO command).
#[inline]
pub fn connected_clients() -> u64 {
    CONNECTED_CLIENTS.load(Ordering::Relaxed)
}

/// Try to open a connection if under the maxclients limit.
/// Returns true if the connection was accepted, false if at limit.
/// When maxclients is 0, the limit is disabled (unlimited).
#[inline]
pub fn try_accept_connection(maxclients: usize) -> bool {
    if maxclients == 0 {
        record_connection_opened();
        return true;
    }
    // CAS loop: only increment if under limit.
    // AcqRel on success ensures the counter increment is visible to other cores
    // before the connection handler runs (important on ARM/weak-memory archs).
    let mut current = CONNECTED_CLIENTS.load(Ordering::Acquire);
    loop {
        if current >= maxclients as u64 {
            return false;
        }
        match CONNECTED_CLIENTS.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                TOTAL_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
                if METRICS_INITIALIZED.load(Ordering::Relaxed) {
                    counter!("moon_connections_total").increment(1);
                    gauge!("moon_connected_clients").increment(1.0);
                }
                return true;
            }
            Err(actual) => current = actual,
        }
    }
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

/// Record segments and bytes freed by P6 aggressive WAL recycle.
///
/// Called from [`WalWriterV3::recycle_aggressive`] after each successful
/// segment deletion. The increments are also reflected in the Prometheus
/// counter `moon_wal_aggressive_recycle_segments_total` when the exporter
/// is enabled; the atomics are always updated so P10 INFO works without
/// Prometheus.
#[inline]
pub fn record_wal_aggressive_recycle(segments: u64, bytes: u64) {
    WAL_AGGRESSIVE_RECYCLE_SEGMENTS_TOTAL.fetch_add(segments, Ordering::Relaxed);
    WAL_AGGRESSIVE_RECYCLE_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_wal_aggressive_recycle_segments_total").increment(segments);
    counter!("moon_wal_aggressive_recycle_bytes_total").increment(bytes);
}

/// Return the total number of WAL segments freed by aggressive recycle since
/// process start. Used by P10 for the `reclamation_wal_segments` INFO field.
#[inline]
pub fn wal_aggressive_recycle_segments_total() -> u64 {
    WAL_AGGRESSIVE_RECYCLE_SEGMENTS_TOTAL.load(Ordering::Relaxed)
}

/// Return the total bytes freed by aggressive WAL recycle since process start.
/// Used by P10 for the `reclamation_wal_bytes` INFO field.
#[inline]
pub fn wal_aggressive_recycle_bytes_total() -> u64 {
    WAL_AGGRESSIVE_RECYCLE_BYTES_TOTAL.load(Ordering::Relaxed)
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

// ── Dispatch routing counters (Phase 177, Step 6) ───────────────────────
// Three-way split of the connection hot path so we can quantify what
// fraction of traffic is hitting the expensive cross-shard SPSC dispatch
// vs. the free local / shared-read fast paths. Ratio of these counters
// is the ground-truth signal for validating dispatch-layer optimizations
// (HotShardMessage split, outbox batching, waker relay fusion).

/// Command executed on the connection's own shard (no cross-thread hop).
#[inline]
pub fn record_dispatch_local() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_dispatch_path_total", "path" => "local").increment(1);
}

/// Batched variant of `record_dispatch_local`: one atomic increment per
/// caller instead of N. Used on the per-batch hot loop in the sharded and
/// monoio handlers to avoid per-command global-atomic cache-line bouncing
/// on `moon_dispatch_path_total{path="local"}`. Short-circuits on
/// `count == 0` so empty batches pay nothing.
#[inline]
pub fn record_dispatch_local_batch(count: u64) {
    if count == 0 || !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_dispatch_path_total", "path" => "local").increment(count);
}


/// Command deferred to cross-shard SPSC dispatch (the slow path).
/// Recorded when a command is enqueued into a `remote_groups` bucket that
/// will be flushed as a `PipelineBatchSlotted` message.
#[inline]
pub fn record_dispatch_cross_spsc() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_dispatch_path_total", "path" => "cross_spsc").increment(1);
}

/// Batched variant of `record_dispatch_cross_spsc`.
#[inline]
pub fn record_dispatch_cross_spsc_batch(count: u64) {
    if count == 0 || !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_dispatch_path_total", "path" => "cross_spsc").increment(count);
}

/// Command handled by the inline GET/SET fast path
/// (`try_inline_dispatch_loop` in `server/conn/blocking.rs`) — the hottest
/// local branch, which bypasses the standard frame-by-frame routing and
/// therefore the three counters above. Recorded in a single batch increment
/// per dispatch loop to keep the call site out of the per-command hot path.
#[inline]
pub fn record_dispatch_local_inline(count: u64) {
    if count == 0 || !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_dispatch_path_total", "path" => "local_inline").increment(count);
}

// ── Vector search metrics (v0.1.6) ─────────────────────────────────────

/// Record a cache hit for FT.CACHESEARCH.
#[inline]
pub fn record_cache_hit() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_cache_hits_total").increment(1);
}

/// Record a cache miss for FT.CACHESEARCH.
#[inline]
pub fn record_cache_miss() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_cache_misses_total").increment(1);
}

/// Update the document count gauge for a vector index.
/// Called after FT.CREATE, HSET auto-index, FT.DROPINDEX, and compaction.
#[inline]
pub fn update_vector_index_docs(index_name: &str, count: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    // Sanitize index name: only allow alphanumeric + underscore + hyphen, max 64 chars.
    if index_name.len() > 64
        || !index_name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return;
    }
    let label = index_name.to_string();
    gauge!("moon_vector_index_docs", "index" => label).set(count as f64);
}

/// Update total vector memory usage gauge (bytes across all indexes).
#[inline]
pub fn update_vector_memory_bytes(bytes: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    gauge!("moon_vector_memory_bytes").set(bytes as f64);
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

// ── Memory helpers ──────────────────────────────────────────────────────

/// Query jemalloc `stats.resident` via raw `mallctl` FFI.
///
/// Returns the total bytes of physical memory mapped by the allocator,
/// or 0 on failure. This is more accurate than `/proc/self/statm` which
/// can be inflated by `mmap` regions that the allocator doesn't own (e.g.
/// WAL segment files, io_uring buffers) and which reports incorrect values
/// inside certain container/VM environments (OrbStack, Docker with cgroups v2).
///
/// **Zero-allocation**: calls `mallctl` directly — no CString, no heap churn.
/// Requires `epoch` advance first so the stats snapshot is fresh.
/// Read process RSS from /proc/self/statm (Linux only).
/// Returns bytes, or 0 on failure / non-Linux.
///
/// **True zero-allocation**: uses raw `libc::open`/`read`/`close` with a
/// static path and stack buffer. Avoids `std::fs::File::open` which
/// allocates internally (`CString::new` for the syscall path).
///
/// Note: jemalloc `mallctl("stats.resident")` was tried but calling
/// `mallctl("epoch")` every second to refresh stats causes jemalloc to
/// allocate internal bookkeeping memory that grows unbounded (~1MB/20s).
#[cfg(target_os = "linux")]
pub fn get_rss_bytes() -> u64 {
    // SAFETY: c"/proc/self/statm" is a valid C string literal.
    // open() with O_RDONLY on /proc is always safe.
    let fd = unsafe { libc::open(c"/proc/self/statm".as_ptr(), libc::O_RDONLY) };
    if fd < 0 {
        return 0;
    }
    let mut buf = [0u8; 128];
    // SAFETY: buf is valid, fd is open, read() returns bytes written.
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len()) };
    // SAFETY: close() on a valid fd is always safe.
    unsafe { libc::close(fd) };
    if n <= 0 {
        return 0;
    }
    // statm format: "size resident shared text lib data dt" (pages)
    // Field 1 (resident) is what we need.
    let s = &buf[..n as usize];
    let mut fields = s.split(|&b| b == b' ');
    let _size = fields.next(); // skip field 0
    if let Some(rss_field) = fields.next() {
        // Parse ASCII digits directly — no String allocation.
        let mut pages: u64 = 0;
        for &b in rss_field {
            if b.is_ascii_digit() {
                pages = pages * 10 + (b - b'0') as u64;
            }
        }
        let page_size = page_size_cached();
        return pages * page_size;
    }
    0
}

/// Cached page size to avoid repeated syscall.
#[cfg(target_os = "linux")]
fn page_size_cached() -> u64 {
    use std::sync::atomic::AtomicU64;
    static PAGE_SIZE: AtomicU64 = AtomicU64::new(0);
    let cached = PAGE_SIZE.load(Ordering::Relaxed);
    if cached != 0 {
        return cached;
    }
    // SAFETY: sysconf(_SC_PAGESIZE) is always safe and returns a positive value.
    let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
    PAGE_SIZE.store(ps, Ordering::Relaxed);
    ps
}

/// Read process RSS on macOS via Mach `task_info` API.
/// Returns bytes, or 0 on failure.
#[cfg(target_os = "macos")]
pub fn get_rss_bytes() -> u64 {
    macos_task_memory_info().1
}

/// Returns (virtual_size, resident_size) for the current process on macOS.
///
/// Tries `MACH_TASK_BASIC_INFO` (flavor 20) first; falls back to
/// `TASK_VM_INFO` (flavor 22) which is available on macOS 10.9+ and works
/// on all tested kernel versions including 24.x (Sequoia).
#[cfg(target_os = "macos")]
pub fn macos_task_memory_info() -> (u64, u64) {
    // Mach kernel API types and functions for querying task memory info.
    // SAFETY: These are stable Mach kernel ABI functions available on all macOS versions.
    unsafe extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(target: u32, flavor: u32, info: *mut u8, count: *mut u32) -> i32;
    }

    // ── Try MACH_TASK_BASIC_INFO (flavor 20) ─────────────────────────────
    // Layout: policy(i32), pad(i32), virtual_size(u64), resident_size(u64), ...
    // Total = 10 natural_t (40 bytes).
    const MACH_TASK_BASIC_INFO: u32 = 20;
    const MACH_TASK_BASIC_INFO_COUNT: u32 = 10;

    let mut info = [0u8; 40];
    let mut count = MACH_TASK_BASIC_INFO_COUNT;
    // SAFETY: mach_task_self() returns current task port. info is 40 bytes,
    // task_info writes at most `count` natural_t values.
    let kr = unsafe {
        task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            info.as_mut_ptr(),
            &mut count,
        )
    };
    if kr == 0 {
        let vsz = u64::from_ne_bytes(info[8..16].try_into().unwrap_or([0; 8]));
        let rss = u64::from_ne_bytes(info[16..24].try_into().unwrap_or([0; 8]));
        return (vsz, rss);
    }

    // ── Fallback: TASK_VM_INFO (flavor 22) ───────────────────────────────
    // Available macOS 10.9+. Layout: virtual_size(u64) at offset 0,
    // phys_footprint(u64) at offset 16. Count = 68 natural_t (272 bytes).
    const TASK_VM_INFO: u32 = 22;
    const TASK_VM_INFO_COUNT: u32 = 68;

    let mut vm_info = [0u8; 272];
    let mut vm_count = TASK_VM_INFO_COUNT;
    // SAFETY: vm_info is 272 bytes = TASK_VM_INFO_COUNT × 4.
    let kr2 = unsafe {
        task_info(
            mach_task_self(),
            TASK_VM_INFO,
            vm_info.as_mut_ptr(),
            &mut vm_count,
        )
    };
    if kr2 == 0 {
        let vsz = u64::from_ne_bytes(vm_info[0..8].try_into().unwrap_or([0; 8]));
        let rss = u64::from_ne_bytes(vm_info[16..24].try_into().unwrap_or([0; 8]));
        return (vsz, rss);
    }

    (0, 0)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn get_rss_bytes() -> u64 {
    0
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
#[cfg(unix)]
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

#[cfg(not(unix))]
pub fn get_cpu_usage() -> (f64, f64) {
    (0.0, 0.0)
}

// ── Global replication state (for INFO) ────────────────────────────────

static GLOBAL_REPL_STATE: once_cell::sync::OnceCell<
    std::sync::Arc<std::sync::RwLock<crate::replication::state::ReplicationState>>,
> = once_cell::sync::OnceCell::new();

/// Register the global replication state for INFO queries.
pub fn set_global_repl_state(
    state: std::sync::Arc<std::sync::RwLock<crate::replication::state::ReplicationState>>,
) {
    let _ = GLOBAL_REPL_STATE.set(state);
}

/// Get the raw global replication state Arc (for MEMORY DOCTOR backlog query).
/// Returns None before replication is initialized.
pub fn get_global_repl_state_arc()
-> Option<&'static std::sync::Arc<std::sync::RwLock<crate::replication::state::ReplicationState>>> {
    GLOBAL_REPL_STATE.get()
}

/// Get replication info for INFO command: (role, connected_slaves, master_repl_offset, repl_id).
/// Also updates the Prometheus replication lag gauge as a side-effect.
pub fn get_replication_info() -> (&'static str, usize, u64, String) {
    if let Some(state) = GLOBAL_REPL_STATE.get() {
        if let Ok(guard) = state.read() {
            let role = match &guard.role {
                crate::replication::state::ReplicationRole::Master => "master",
                crate::replication::state::ReplicationRole::Replica { .. } => "slave",
            };
            let slaves = guard.replicas.len();
            let offset = guard.master_repl_offset.load(Ordering::Relaxed);
            let repl_id = guard.repl_id.clone();
            // Update Prometheus lag gauge: max lag across all replicas.
            if !guard.replicas.is_empty() {
                let max_lag_bytes = guard
                    .replicas
                    .iter()
                    .map(|r| {
                        let ack: u64 = r
                            .ack_offsets
                            .iter()
                            .map(|a| a.load(Ordering::Relaxed))
                            .sum();
                        offset.saturating_sub(ack)
                    })
                    .max()
                    .unwrap_or(0);
                record_replication_lag(max_lag_bytes, 0);
            }
            return (role, slaves, offset, repl_id);
        }
    }
    ("master", 0, 0, "0".repeat(40))
}

// ── Global ShardDatabases (for MEMORY DOCTOR / Prometheus per-kind) ───

static GLOBAL_SHARD_DBS: once_cell::sync::OnceCell<
    std::sync::Weak<crate::shard::shared_databases::ShardDatabases>,
> = once_cell::sync::OnceCell::new();

/// Register the global ShardDatabases handle for admin commands.
/// Called once from main after ShardDatabases::new().
pub fn set_global_shard_databases(
    dbs: &std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
) {
    let _ = GLOBAL_SHARD_DBS.set(std::sync::Arc::downgrade(dbs));
}

/// Get the global ShardDatabases handle (returns None before server init
/// or after shutdown when the Arc has been dropped).
pub fn get_global_shard_databases()
-> Option<std::sync::Arc<crate::shard::shared_databases::ShardDatabases>> {
    GLOBAL_SHARD_DBS.get().and_then(|w| w.upgrade())
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

// ── SSE metrics publisher (console feature) ────────────────────────────

/// Spawn a background task that publishes `MetricEvent` to the SSE
/// broadcast channel at ~1 Hz. Reads from existing atomic counters.
///
/// Must be called from within a tokio runtime context (the admin thread).
#[cfg(feature = "console")]
pub fn spawn_metrics_publisher() {
    use crate::admin::sse_stream::{MetricEvent, get_metrics_sender};

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(1000));
        let mut prev_ops: u64 = 0;
        let start = std::time::Instant::now();

        loop {
            interval.tick().await;

            let sender = match get_metrics_sender() {
                Some(s) => s,
                None => continue,
            };

            let total_ops = TOTAL_COMMANDS.load(Ordering::Relaxed);
            let ops_per_sec = total_ops.saturating_sub(prev_ops);
            prev_ops = total_ops;

            let event = MetricEvent {
                event: "server_stats",
                total_ops,
                ops_per_sec,
                total_memory: get_rss_bytes(),
                connected_clients: CONNECTED_CLIENTS.load(Ordering::Relaxed),
                uptime_seconds: start.elapsed().as_secs(),
                total_keys: 0,
            };

            // watch::send replaces the single stored value — no ring buffer,
            // no per-event allocation. Receivers see only the latest snapshot.
            let _ = sender.send(event);
        }
    });
}

// ── Per-subsystem memory gauge publisher ─────────────────────────────

/// Spawn a background task that updates the `moon_memory_bytes{kind=...}`
/// gauge every 15 seconds from the `resident_bytes()` accessors added in
/// Phase 190-01.
///
/// Must be called from within a tokio runtime context (the admin-http
/// thread). Requires `set_global_shard_databases()` to have been called
/// first — the function tolerates a missing handle by emitting 0 for all
/// subsystem kinds until the global is registered.
///
/// NOTE: This loop does NOT call `mallctl("epoch")` — see the documented
/// jemalloc leak at `get_rss_bytes()` (~1 MB / 20 s). `allocator_overhead`
/// is computed as `max(0, RSS − sum(other 6))`, the same formula MEMORY
/// DOCTOR uses.
pub fn spawn_moon_memory_publisher() {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));

        loop {
            interval.tick().await;

            if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
                continue;
            }

            update_moon_memory_bytes();
        }
    });
}

/// Collect per-subsystem resident bytes and emit all 7
/// `moon_memory_bytes{kind=...}` series plus `moon_rss_bytes`.
///
/// Called every 15 s by `spawn_moon_memory_publisher`. Allocation-free in
/// the steady state — all label values are `&'static str`.
fn update_moon_memory_bytes() {
    let rss = get_rss_bytes() as usize;

    let mut dashtable: usize = 0;
    let mut hnsw: usize = 0;
    let mut sealed: usize = 0;
    #[cfg_attr(not(feature = "graph"), allow(unused_mut))]
    let mut csr: usize = 0;
    let wal: usize = 0; // WalWriterV3 is stack-owned; not reachable here
    let mut backlog: usize = 0;

    if let Some(shard_dbs) = get_global_shard_databases() {
        let num_shards = shard_dbs.num_shards();
        for shard_id in 0..num_shards {
            // Database + DashTable (DB 0 — the hot database)
            let db_guard = shard_dbs.read_db(shard_id, 0);
            dashtable += db_guard.resident_bytes();
            dashtable += db_guard.data().resident_bytes();
            drop(db_guard);

            // VectorStore: (mutable/hnsw, immutable/sealed)
            let vs = shard_dbs.vector_store(shard_id);
            let (m, i) = vs.resident_bytes();
            hnsw += m;
            sealed += i;
            drop(vs);

            // GraphStore (CSR)
            #[cfg(feature = "graph")]
            {
                let gs = shard_dbs.graph_store_read(shard_id);
                csr += gs.resident_bytes();
            }
        }
    }

    // Replication backlog via global state.
    if let Some(state) = get_global_repl_state_arc() {
        if let Ok(guard) = state.read() {
            backlog = guard.backlog_resident_bytes();
        }
    }

    let other_sum = dashtable + hnsw + csr + wal + sealed + backlog;
    let alloc_overhead = rss.saturating_sub(other_sum);

    gauge!("moon_memory_bytes", "kind" => "dashtable").set(dashtable as f64);
    gauge!("moon_memory_bytes", "kind" => "hnsw").set(hnsw as f64);
    gauge!("moon_memory_bytes", "kind" => "csr").set(csr as f64);
    gauge!("moon_memory_bytes", "kind" => "wal").set(wal as f64);
    gauge!("moon_memory_bytes", "kind" => "sealed").set(sealed as f64);
    gauge!("moon_memory_bytes", "kind" => "replication_backlog").set(backlog as f64);
    gauge!("moon_memory_bytes", "kind" => "allocator_overhead").set(alloc_overhead as f64);

    // Update the existing RSS gauge in the same snapshot so the integration
    // test can compare moon_memory_bytes sum against moon_rss_bytes from the
    // same scrape (no drift between separate reads).
    update_rss_bytes(rss as u64);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Smoke tests for the dispatch-path counters added in Phase 177, Step 6.
    // A full assertion on the prometheus state would require initialising a
    // recorder and a scraping harness — out of scope here. These tests just
    // pin the contract that the helpers are safe to call on the hot path
    // before `init_metrics` has run and therefore never panic or allocate
    // unexpectedly when the exporter is disabled (admin_port = 0).

    #[test]
    fn dispatch_path_counters_no_op_before_init() {
        // METRICS_INITIALIZED starts false; all three helpers must early-return.
        // We just assert they do not panic. The absence of a global recorder
        // means counter!() would otherwise be a no-op, but the guard is what
        // we actually care about: no string allocation, no label churn.
        assert!(!METRICS_INITIALIZED.load(Ordering::Relaxed));
        record_dispatch_local();
        record_dispatch_cross_spsc();
        record_dispatch_local_inline(0); // count == 0 must short-circuit even when init
        record_dispatch_local_inline(7);
    }

    #[test]
    fn cached_metrics_skips_rebuild_on_same_cmd() {
        let mut cache = CachedMetricsHandles::new();
        assert!(cache.last_cmd.is_empty(), "fresh cache starts empty");

        cache.ensure(b"SET");
        assert_eq!(cache.last_cmd.as_slice(), b"SET", "first call populates");

        // Repeated SET: cache hit. We cannot observe the skip directly without a
        // mock recorder, but the stored bytes must remain and not churn.
        cache.ensure(b"SET");
        assert_eq!(cache.last_cmd.as_slice(), b"SET");

        // Different command: must rebuild and swap the buffer contents.
        cache.ensure(b"GET");
        assert_eq!(cache.last_cmd.as_slice(), b"GET");

        // Mixed case is treated as a distinct raw input; sanitize_cmd_label
        // will still normalise to "set" for the Prometheus label, but the
        // cache key is the raw bytes (pointer to the last call's payload).
        cache.ensure(b"set");
        assert_eq!(cache.last_cmd.as_slice(), b"set");
    }

    #[test]
    fn record_command_cached_no_op_before_init() {
        assert!(!METRICS_INITIALIZED.load(Ordering::Relaxed));
        let mut cache = CachedMetricsHandles::new();
        record_command_cached("set", 1, &mut cache);
        record_command_no_latency_cached("set", &mut cache);
        record_command_error_cached("set", &mut cache);
        // Must not panic, must not churn the cache on the hot path.
    }
}

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

/// Moon's own release version, taken from `Cargo.toml` at compile time.
///
/// Used in the INFO `moon_version` field, HELLO `version` field, and LOLWUT.
pub const MOON_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Redis compatibility version advertised to clients.
///
/// Clients gate features (e.g. RESP3, command-info, ACL) on `redis_version`.
/// Advertising 7.4.0 unlocks the full Redis 7 feature set in standard clients
/// while staying conservative enough to avoid enabling Redis 8 alpha paths.
/// The real moon version is always present in the `moon_version` INFO field.
pub const REDIS_COMPAT_VERSION: &str = "7.4.0";

/// Global monotonic client ID counter.
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// Allocate a unique client connection ID.
pub fn next_client_id() -> u64 {
    NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
}

/// CLIENT ID command: return the connection's unique ID.
pub fn client_id(id: u64) -> Frame {
    Frame::Integer(id as i64)
}

/// PING command handler.
///
/// No args: return PONG as SimpleString.
/// One arg: return the arg as BulkString.
/// More args: return arity error.
pub fn ping(args: &[Frame]) -> Frame {
    match args.len() {
        0 => Frame::SimpleString(Bytes::from_static(b"PONG")),
        1 => match &args[0] {
            Frame::BulkString(s) => Frame::BulkString(s.clone()),
            Frame::SimpleString(s) => Frame::BulkString(s.clone()),
            _ => Frame::BulkString(Bytes::from(format!("{:?}", args[0]))),
        },
        _ => Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'PING' command",
        )),
    }
}

/// ECHO command handler.
///
/// Exactly one arg: return it as BulkString.
/// Wrong arity: return error.
pub fn echo(args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'ECHO' command",
        ));
    }
    match &args[0] {
        Frame::BulkString(s) => Frame::BulkString(s.clone()),
        Frame::SimpleString(s) => Frame::BulkString(s.clone()),
        _ => Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'ECHO' command",
        )),
    }
}

/// SELECT command handler.
///
/// Parse arg as integer, validate range 0..db_count, set selected_db.
pub fn select(args: &[Frame], selected_db: &mut usize, db_count: usize) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'SELECT' command",
        ));
    }
    let index_str = match &args[0] {
        Frame::BulkString(s) => s,
        Frame::SimpleString(s) => s,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let index_str = match std::str::from_utf8(index_str) {
        Ok(s) => s,
        Err(_) => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let index: usize = match index_str.parse() {
        Ok(n) => n,
        Err(_) => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    if index >= db_count {
        return Frame::Error(Bytes::from_static(b"ERR DB index is out of range"));
    }
    *selected_db = index;
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// COMMAND command handler.
///
/// COMMAND (bare): return integer 0.
/// COMMAND DOCS: return empty array.
/// Any other subcommand: return empty array.
pub fn command(args: &[Frame]) -> Frame {
    if args.is_empty() {
        return Frame::Integer(0);
    }
    // Check for DOCS subcommand (case-insensitive, zero-alloc)
    if let Some(Frame::BulkString(sub)) | Some(Frame::SimpleString(sub)) = args.first() {
        if sub.eq_ignore_ascii_case(b"DOCS") || sub.eq_ignore_ascii_case(b"COUNT") {
            return Frame::Array(framevec![]);
        }
    }
    Frame::Array(framevec![])
}

/// HEALTHZ command — liveness check. Always returns +OK if the server is running.
pub fn healthz() -> Frame {
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// READYZ command — readiness check. Returns +OK when the server is fully
/// initialized (shards accepting, persistence loaded), -ERR otherwise.
pub fn readyz() -> Frame {
    if crate::admin::metrics_setup::is_server_ready() {
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else {
        Frame::Error(Bytes::from_static(b"ERR server not ready"))
    }
}

/// Format bytes as human-readable (e.g. "1.23M", "456.78K").
fn format_memory_human(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2}G", b / GB)
    } else if b >= MB {
        format!("{:.2}M", b / MB)
    } else if b >= KB {
        format!("{:.2}K", b / KB)
    } else {
        format!("{bytes}B")
    }
}

/// INFO command handler.
///
/// Returns a BulkString with server info sections matching Redis INFO format.
///
/// The `# Keyspace` section reports the CALLING shard's `db` as `db0` only —
/// the single-`Database` fallback for paths with no scatter access (generic
/// dispatch). The connection handlers pass cross-shard, all-db stats through
/// [`info_with_keyspace`] instead.
pub fn info(db: &Database, args: &[Frame]) -> Frame {
    let raw = info_raw(db, &InstanceFacts::default());
    crate::command::info_sections::finalize(&raw, None, args)
}

/// This process's `run_id`: 40 hex chars, stable for the process lifetime and
/// regenerated on every start.
///
/// Clients (and Sentinel) compare it across reconnects to decide whether they
/// are talking to the same server instance; deriving it from anything durable
/// — the data dir, the port, a config hash — would defeat exactly that check,
/// so it is seeded from process identity plus start time.
fn run_id() -> &'static str {
    use std::sync::OnceLock;
    static RUN_ID: OnceLock<String> = OnceLock::new();
    RUN_ID.get_or_init(|| {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        std::process::id().hash(&mut h);
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
            .hash(&mut h);
        (&RUN_ID as *const _ as usize).hash(&mut h);
        let a = h.finish();
        // Second independent round so the 40 chars are not a repeat of 16.
        a.hash(&mut h);
        let b = h.finish();
        b.hash(&mut h);
        let c = h.finish();
        format!("{a:016x}{b:016x}{c:016x}")[..40].to_string()
    })
}

/// Process start instant, captured once at startup by [`record_server_start`].
static SERVER_START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

/// Capture the server start instant. Called from `main` before the listener
/// binds, so `uptime_in_seconds` counts from a point that precedes the first
/// client rather than from whenever INFO was first asked.
pub fn record_server_start() {
    let _ = SERVER_START.set(std::time::Instant::now());
}

/// Seconds since [`record_server_start`].
///
/// Falls back to initialising on first read, which keeps a unit test or an
/// embedded harness that never calls `record_server_start` reporting a
/// monotonic uptime instead of a constant — the only wrong answer here is one
/// that never advances, because that is what makes a crash loop invisible.
fn server_uptime_secs() -> u64 {
    SERVER_START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_secs()
}

/// Build the full INFO payload with every section present.
///
/// Callers must pass this through [`crate::command::info_sections::finalize`],
/// which applies section selection and drops duplicate headers. Building
/// everything and filtering afterwards is deliberate: the `# Replication`
/// section here is a STUB that the connection handlers replace with the real
/// one, so a filter applied during assembly would either emit the stub or drop
/// the real section depending on where it ran.
fn info_raw(db: &Database, facts: &InstanceFacts) -> String {
    use std::fmt::Write as _;
    let mut sections = String::with_capacity(2048);

    sections.push_str("# Server\r\n");
    let _ = write!(sections, "redis_version:{REDIS_COMPAT_VERSION}\r\n");
    let _ = write!(sections, "moon_version:{MOON_VERSION}\r\n");
    sections.push_str("moon:true\r\n");
    let _ = write!(sections, "run_id:{}\r\n", run_id());
    // Read from the real cluster gate, not a literal. These two were hardcoded
    // `standalone` / `0`, and the comment above them promised the cluster
    // subsystem would say otherwise — nothing ever did. An SDK branches on
    // `redis_mode` BEFORE it ever calls CLUSTER SHARDS, so a server that
    // answers SHARDS correctly while reporting `standalone` is still
    // undiscoverable as a cluster.
    //
    // `cluster_enabled` belongs HERE and not in CLUSTER INFO — measured
    // against redis-server 8.6.1, which emits it in INFO and never there.
    if crate::cluster::cluster_enabled() {
        sections.push_str("redis_mode:cluster\r\n");
        sections.push_str("cluster_enabled:1\r\n");
    } else {
        sections.push_str("redis_mode:standalone\r\n");
        sections.push_str("cluster_enabled:0\r\n");
    }
    let _ = write!(sections, "process_id:{}\r\n", std::process::id());
    // The port this instance LISTENS on, not the port this connection arrived
    // on. Behind a container port map or a proxy they differ, and a client
    // handing a peer the arrival port would send it somewhere unreachable.
    let _ = write!(sections, "tcp_port:{}\r\n", facts.tcp_port);
    // Uptime is the field every restart-detector keys on: a drop to near zero
    // is how a dashboard learns the process died. Sourced from the start
    // instant captured before the listener binds, so it can never read as a
    // constant.
    let uptime_secs = server_uptime_secs();
    let _ = write!(sections, "uptime_in_seconds:{uptime_secs}\r\n");
    let _ = write!(sections, "uptime_in_days:{}\r\n", uptime_secs / 86_400);
    let _ = write!(sections, "os:{}\r\n", std::env::consts::OS);
    let _ = write!(
        sections,
        "arch_bits:{}\r\n",
        (std::mem::size_of::<usize>() * 8)
    );
    sections.push_str("\r\n");

    sections.push_str("# Clients\r\n");
    let _ = write!(
        sections,
        "connected_clients:{}\r\n",
        crate::admin::metrics_setup::connected_clients(),
    );
    // c1M task-exit parking: connections held only by a readiness watcher.
    // Counted separately rather than folded into connected_clients — a parked
    // connection is fully live (CLIENT LIST/KILL reach it), just not holding a
    // handler task or a working set.
    let _ = write!(
        sections,
        "parked_clients:{}\r\n",
        crate::client_registry::parked_clients(),
    );
    // A gauge, not a counter: an operator reads it to tell an idle server
    // apart from one whose every worker is parked on an empty queue.
    let _ = write!(
        sections,
        "blocked_clients:{}\r\n",
        crate::admin::metrics_setup::blocked_clients(),
    );
    sections.push_str("\r\n");

    sections.push_str("# Memory\r\n");
    // `phys_footprint`, not `resident_size`. On macOS the latter counts only
    // pages currently in RAM, so a process whose heap has been swapped out
    // reports a SMALL rss precisely when it is costing the most. Measured on
    // the live :6381 instance: resident 439 MB against a 10.1 GB footprint,
    // 9.9 GB of it swapped — INFO read `used_memory:4.21G` and
    // `used_memory_rss:0.4G`, which looks like excellent efficiency and is in
    // fact a machine about to thrash. Falls back to the old reading when the
    // platform cannot answer.
    let rss = {
        let fp = crate::admin::metrics_setup::process_footprint_bytes();
        if fp > 0 {
            fp
        } else {
            crate::admin::metrics_setup::get_rss_bytes()
        }
    };
    // Task #56 (used_memory truthfulness): `used_memory` reports the same
    // logical ledger `--maxmemory` eviction actually gates on (KV + its
    // ColdIndex overhead + vector/text/graph resident bytes) -- NOT raw
    // process RSS. Before this fix `used_memory` was literally `rss`,
    // which is why a disk-offload deployment at `--maxmemory 256MB` showed
    // `used_memory` in the 400-700MB range: RSS also carries the binary
    // image, thread stacks, allocator arena fragmentation, mmap'd cold-read
    // page-cache frames, the (intentionally unbounded) Lua script cache, and
    // the replication backlog ring -- none of which the eviction system
    // ever charges against the cap. Those components still get their own
    // truthful lines below (`used_memory_rss`, `allocator_overhead_bytes`,
    // `pagecache_bytes`) plus a full breakdown in `MEMORY DOCTOR` -- they are
    // not hidden, just no longer conflated with the gate-comparable figure.
    let used_memory = crate::admin::metrics_setup::logical_used_memory_bytes() as u64;
    // task #58: allocator_overhead_bytes is sampled continuously by shard 0's
    // 100ms tick (persistence_tick::run_eviction_tick), not recomputed here.
    // pagecache_bytes sums each shard's published PageCache resident-buffer
    // atomic (same cross-shard sum pattern as MEMORY DOCTOR). Both are
    // observability-only figures -- neither feeds eviction or budget gating.
    let allocator_overhead_bytes = crate::admin::metrics_setup::get_allocator_overhead_bytes();
    let pagecache_bytes =
        crate::admin::metrics_setup::get_global_shard_databases().map_or(0, |shard_dbs| {
            shard_dbs
                .store_memory_per_shard
                .iter()
                .map(|mem| mem.pagecache.load(std::sync::atomic::Ordering::Relaxed))
                .sum::<usize>()
        });
    // Bytes held by the per-shard Lua VMs, summed across shards. Each shard
    // publishes `mlua::Lua::used_memory()` on its periodic tick and the VM is
    // created lazily on first script use, so 0 before any EVAL is the truth,
    // not a placeholder. Tells an operator whether a runaway script is holding
    // memory the eviction cap never sees (the Lua plane is outside it).
    let used_memory_lua =
        crate::admin::metrics_setup::get_global_shard_databases().map_or(0, |shard_dbs| {
            shard_dbs
                .store_memory_per_shard
                .iter()
                .map(|mem| mem.lua.load(std::sync::atomic::Ordering::Relaxed))
                .sum::<usize>()
        });
    let _ = write!(
        sections,
        "used_memory_lua:{used_memory_lua}\r\n\
         used_memory:{used_memory}\r\n\
         used_memory_human:{human}\r\n\
         used_memory_rss:{rss}\r\n\
         used_memory_peak:{rss}\r\n\
         allocator_overhead_bytes:{allocator_overhead_bytes}\r\n\
         pagecache_bytes:{pagecache_bytes}\r\n\
         mem_fragmentation_ratio:{frag:.2}\r\n\
         maxmemory:{maxmemory}\r\n\
         maxmemory_policy:{maxmemory_policy}\r\n",
        used_memory = used_memory,
        human = format_memory_human(used_memory),
        rss = rss,
        allocator_overhead_bytes = allocator_overhead_bytes,
        pagecache_bytes = pagecache_bytes,
        // The gap operators currently cannot see, under Redis's field name so
        // existing dashboards pick it up without translation. Deliberately the
        // raw quotient of the two fields printed just above, NOT
        // `footprint_ratio` — that one is floored, baseline-subtracted and
        // clamped for use as an eviction divisor, and publishing a doctored
        // number under a standard field name would make INFO disagree with
        // itself.
        frag = if used_memory > 0 {
            rss as f64 / used_memory as f64
        } else {
            0.0
        },
        // Read from the same atomic the eviction gate enforces, so INFO can
        // never report a cap different from the one actually applied.
        maxmemory = crate::storage::eviction::maxmemory_bytes(),
        // Named from the same published atomic the gate reads, so INFO cannot
        // claim `noeviction` while the instance is in fact evicting.
        maxmemory_policy = crate::storage::eviction::maxmemory_policy_name(),
    );

    // Allocator counters, Redis's `allocator_*` field names so existing
    // dashboards and exporters read them without translation.
    //
    // These are the fields that make a used_memory-vs-RSS gap diagnosable
    // rather than a guess: `allocator_frag_bytes` is space lost to size-class
    // rounding, `allocator_unreturned_bytes` is dirty pages jemalloc is
    // holding instead of giving back, and whatever the OS charges beyond
    // `allocator_resident` belongs to something other than the allocator
    // (mmap'd segments, thread stacks, the binary image).
    //
    // Only present on `--features jemalloc-stats`; jemalloc's stats cost
    // bookkeeping on every allocation, so the default build does not pay it.
    // Absent rather than zero-filled: a zero here would read as "no
    // fragmentation", which is a worse answer than "not measured".
    #[cfg(feature = "jemalloc-stats")]
    if let Some(st) = crate::memory_ctl::jemalloc_stats() {
        let _ = write!(
            sections,
            "allocator_allocated:{}\r\n\
             allocator_active:{}\r\n\
             allocator_resident:{}\r\n\
             allocator_retained:{}\r\n\
             allocator_frag_bytes:{}\r\n\
             allocator_frag_ratio:{:.2}\r\n\
             allocator_unreturned_bytes:{}\r\n",
            st.allocated,
            st.active,
            st.resident,
            st.retained,
            st.frag_bytes(),
            st.frag_ratio(),
            st.unreturned_bytes(),
        );
    }
    sections.push_str("\r\n");

    sections.push_str("# Persistence\r\n");
    // #432: aof_enabled / aof_rewrite_in_progress / sizes are real state, not
    // hardcoded zeros. Sizes come from the auto-rewrite monitor's statics
    // (#433); refresh_current_size keeps `aof_current_size` honest when INFO
    // is read between monitor ticks (one directory walk — INFO is cold path).
    let aof_enabled = crate::persistence::aof::auto_rewrite::AOF_ENABLED
        .load(std::sync::atomic::Ordering::Relaxed);
    let aof_current_size = if aof_enabled {
        crate::persistence::aof::auto_rewrite::refresh_current_size()
    } else {
        0
    };
    sections.push_str(&format!(
        "loading:0\r\n\
         rdb_changes_since_last_save:{}\r\n\
         rdb_bgsave_in_progress:{}\r\n\
         rdb_last_save_time:{}\r\n\
         rdb_last_bgsave_status:{}\r\n\
         aof_last_write_status:{}\r\n\
         aof_last_bgrewrite_status:{}\r\n\
         aof_enabled:{}\r\n\
         aof_rewrite_in_progress:{}\r\n\
         aof_base_size:{}\r\n\
         aof_current_size:{}\r\n\
         aof_backpressure_dropped:{}\r\n\
         aof_last_fsync_status:{}\r\n\
         aof_fsync_failures:{}\r\n\
         aof_last_append_status:{}\r\n\
         aof_reason_del_dropped:{}\r\n\
         aof_rewrite_overflow_spilled:{}\r\n\
         spill_batches_flushed:{}\r\n\
         spill_completions_dropped:{}\r\n\
         spill_failed_reinserted:{}\r\n\
         spill_completion_superseded:{}\r\n\
         spill_last_heartbeat_ms:{}\r\n",
        // Keyspace mutations since the last COMPLETED save — the "is a save
        // worth doing" signal a backup script reads. A failed save does not
        // reset it: the dataset is still unpersisted.
        crate::admin::metrics_setup::rdb_changes_since_last_save(),
        if crate::command::persistence::SAVE_IN_PROGRESS.load(std::sync::atomic::Ordering::Relaxed)
        {
            1
        } else {
            0
        },
        crate::command::persistence::LAST_SAVE_TIME.load(std::sync::atomic::Ordering::Relaxed),
        if crate::command::persistence::BGSAVE_LAST_STATUS
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            "ok"
        } else {
            "err"
        },
        // Redis parity names for the two AOF statuses stock tooling
        // string-matches. `aof_last_write_status` shares its source with
        // Moon's own `aof_last_append_status` below — the same fact under the
        // name a redis-py/ioredis health check actually looks for.
        if crate::persistence::aof::AOF_LAST_APPEND_OK.load(std::sync::atomic::Ordering::Relaxed) {
            "ok"
        } else {
            "err"
        },
        if crate::persistence::aof::AOF_REWRITE_LAST_OK.load(std::sync::atomic::Ordering::Relaxed) {
            "ok"
        } else {
            "err"
        },
        u8::from(aof_enabled),
        u8::from(
            crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                .load(std::sync::atomic::Ordering::SeqCst)
        ),
        crate::persistence::aof::auto_rewrite::AOF_BASE_SIZE
            .load(std::sync::atomic::Ordering::Relaxed),
        aof_current_size,
        crate::persistence::aof::AOF_BACKPRESSURE_DROPPED
            .load(std::sync::atomic::Ordering::Relaxed),
        if crate::persistence::aof::aof_last_fsync_ok() {
            "ok"
        } else {
            "err"
        },
        crate::persistence::aof::AOF_FSYNC_FAILURES.load(std::sync::atomic::Ordering::Relaxed),
        if crate::persistence::aof::AOF_LAST_APPEND_OK.load(std::sync::atomic::Ordering::Relaxed) {
            "ok"
        } else {
            "err"
        },
        crate::persistence::aof::AOF_REASON_DEL_DROPPED.load(std::sync::atomic::Ordering::Relaxed),
        crate::persistence::aof::rewrite_overflow::AOF_REWRITE_OVERFLOW_SPILLED
            .load(std::sync::atomic::Ordering::Relaxed),
        crate::storage::tiered::spill_thread::spill_batches_flushed_total(),
        crate::storage::tiered::spill_thread::spill_completion_dropped_total(),
        crate::storage::tiered::spill_thread::spill_failed_reinserted_total(),
        crate::storage::tiered::spill_thread::spill_completion_superseded_total(),
        crate::storage::tiered::spill_thread::spill_last_heartbeat_ms(),
    ));
    sections.push_str("\r\n");

    sections.push_str("# Vector\r\n");
    sections.push_str(&format!(
        "vector_indexes:{}\r\n\
         vector_total_vectors:{}\r\n\
         vector_memory_bytes:{}\r\n\
         vector_search_total:{}\r\n\
         vector_search_latency_us:{}\r\n\
         vector_compaction_count:{}\r\n\
         vector_compaction_duration_ms:{}\r\n\
         vector_mutable_segment_bytes:{}\r\n",
        crate::vector::metrics::VECTOR_INDEXES.load(std::sync::atomic::Ordering::Relaxed),
        crate::vector::metrics::total_vectors(),
        crate::vector::metrics::VECTOR_MEMORY_BYTES.load(std::sync::atomic::Ordering::Relaxed),
        crate::vector::metrics::search_total(),
        crate::vector::metrics::VECTOR_SEARCH_LATENCY_US.load(std::sync::atomic::Ordering::Relaxed),
        crate::vector::metrics::VECTOR_COMPACTION_COUNT.load(std::sync::atomic::Ordering::Relaxed),
        crate::vector::metrics::VECTOR_COMPACTION_DURATION_MS
            .load(std::sync::atomic::Ordering::Relaxed),
        crate::vector::metrics::VECTOR_MUTABLE_SEGMENT_BYTES
            .load(std::sync::atomic::Ordering::Relaxed),
    ));
    sections.push_str("\r\n");

    sections.push_str("# MoonStore\r\n");
    let _ = write!(
        sections,
        "disk_offload_enabled:{}\r\n",
        crate::vector::metrics::MOONSTORE_DISK_OFFLOAD_ENABLED
            .load(std::sync::atomic::Ordering::Relaxed) as u8
    );
    sections.push_str("\r\n");

    // # Reclamation — observability foundation for Wave-1 production hardening (P10).
    crate::command::info_reclamation::write_reclamation_section(&mut sections);

    // # Stats
    sections.push_str("# Stats\r\n");
    let _ = write!(
        sections,
        "total_commands_processed:{}\r\n\
         total_connections_received:{}\r\n\
         total_dispatch_cross_spsc:{}\r\n\
         spsc_notify_wakes:{}\r\n\
         spsc_drain_renotify:{}\r\n\
         spsc_notify_skipped:{}\r\n\
         ft_search_cooperative_yields_total:{}\r\n",
        crate::admin::metrics_setup::total_commands_processed(),
        crate::admin::metrics_setup::total_connections_received(),
        crate::admin::metrics_setup::total_dispatch_cross_spsc(),
        crate::admin::metrics_setup::spsc_notify_wakes(),
        crate::admin::metrics_setup::spsc_drain_renotify(),
        crate::admin::metrics_setup::spsc_notify_skipped(),
        crate::admin::metrics_setup::ft_search_cooperative_yields(),
    );
    // Fields stock monitoring agents read. Backed by real counters — a field
    // Moon cannot answer truthfully is omitted rather than reported as a
    // constant, because a hardcoded zero is indistinguishable from a healthy
    // server on a dashboard.
    let _ = write!(
        sections,
        "keyspace_hits:{}\r\n\
         keyspace_misses:{}\r\n\
         expired_keys:{}\r\n\
         evicted_keys:{}\r\n\
         rejected_connections:{}\r\n\
         total_net_input_bytes:{}\r\n\
         total_net_output_bytes:{}\r\n\
         instantaneous_ops_per_sec:{}\r\n\
         sync_full:{}\r\n\
         sync_partial_ok:{}\r\n\
         sync_partial_err:{}\r\n\
         pubsub_channels:{}\r\n\
         pubsub_patterns:{}\r\n",
        crate::admin::metrics_setup::keyspace_hits(),
        crate::admin::metrics_setup::keyspace_misses(),
        crate::admin::metrics_setup::expired_keys(),
        crate::admin::metrics_setup::evicted_keys(),
        crate::admin::metrics_setup::rejected_connections(),
        crate::admin::metrics_setup::total_net_input_bytes(),
        crate::admin::metrics_setup::total_net_output_bytes(),
        crate::admin::metrics_setup::instantaneous_ops_per_sec(),
        // Replica-sync health: a climbing `sync_full` against a flat
        // `sync_partial_ok` means partial resync keeps failing and every
        // replica reconnect re-ships the whole dataset.
        crate::admin::metrics_setup::sync_full(),
        crate::admin::metrics_setup::sync_partial_ok(),
        crate::admin::metrics_setup::sync_partial_err(),
        facts.pubsub_channels,
        facts.pubsub_patterns,
    );
    sections.push_str("\r\n");

    // # CPU
    sections.push_str("# CPU\r\n");
    let (cpu_sys, cpu_user) = crate::admin::metrics_setup::get_cpu_usage();
    let _ = write!(
        sections,
        "used_cpu_sys:{:.6}\r\n\
         used_cpu_user:{:.6}\r\n",
        cpu_sys, cpu_user,
    );
    sections.push_str("\r\n");

    // # Replication
    sections.push_str("# Replication\r\n");
    let (role, slaves, offset, repl_id) = crate::admin::metrics_setup::get_replication_info();
    // task #48: unified poison-record counter, incremented by every replica
    // apply plane (RESP framing / graph / MQ / WS / temporal / snapshot
    // install) on a malformed/undecodable record — see
    // `replication::apply`'s "Unified poison-record policy" docs. Zero on a
    // master (a master never applies a replicated stream) and on a replica
    // that has never seen a corrupt record.
    let poison_total = crate::replication::apply::REPL_POISON_RECORDS_TOTAL
        .load(std::sync::atomic::Ordering::Relaxed);
    let _ = write!(
        sections,
        "role:{role}\r\n\
         connected_slaves:{slaves}\r\n\
         master_replid:{repl_id}\r\n\
         master_repl_offset:{offset}\r\n\
         replication_poison_records_total:{poison_total}\r\n",
    );
    sections.push_str("\r\n");

    // # Commandstats — placeholder section for Redis 7.x parity.
    // Per-command stats (calls, usec, usec_per_call) require a global registry;
    // the record_command() path already tracks per-label counters in Prometheus.
    // For now, emit the section header so redis-py parse_info recognizes it.
    sections.push_str("# Commandstats\r\n");
    sections.push_str("\r\n");

    sections.push_str("# Keyspace\r\n");
    // Logical count (hot + cold), matching DBSIZE (issue #355). `expires`
    // remains resident-only: cold TTLs live in ColdLocation.ttl_ms and are
    // swept by the proactive expiry pass, but counting them here would need
    // a second O(cold) scan for a field monitoring rarely consumes.
    let key_count = db.logical_len();
    let expires_count = db.expires_count();
    if key_count > 0 {
        let _ = write!(
            sections,
            "db0:keys={},expires={},avg_ttl=0\r\n",
            key_count, expires_count
        );
    }

    sections
}

/// INFO with an externally-gathered `# Keyspace` section: one `(keys,
/// expires)` entry per logical db, already summed across shards
/// (`coordinate_keyspace_info`). Lists every NON-EMPTY db like Redis —
/// previously the section always read `db0:` with the SELECTED db's local
/// count, so `SELECT 2; SET k v; INFO` reported the db-2 count as db0 and
/// every other db was invisible.
pub fn info_with_keyspace(db: &Database, args: &[Frame], keyspace: &[(u64, u64)]) -> Frame {
    info_with_keyspace_and_replication(db, args, keyspace, None)
}

/// As [`info_with_keyspace`], but also substitutes the authoritative
/// `# Replication` section.
///
/// The connection handlers own the replication state, so they used to APPEND
/// their section after `info()` had already written a stub — which is why INFO
/// emitted `# Replication` twice. Passing it in instead keeps one assembly
/// point, so section selection and de-duplication see the final section set.
pub fn info_with_keyspace_and_replication(
    db: &Database,
    args: &[Frame],
    keyspace: &[(u64, u64)],
    real_replication: Option<&str>,
) -> Frame {
    info_with_facts(
        db,
        args,
        keyspace,
        real_replication,
        &InstanceFacts::default(),
    )
}

/// Instance-wide facts INFO must report that a single shard's [`Database`]
/// cannot answer.
///
/// Pub/sub counts are the motivating case: a channel with subscribers on two
/// shard threads exists in two per-shard registries, so summing per-registry
/// counters would report it twice. The connection handlers already hold
/// `all_pubsub_registries` and already de-duplicate for `PUBSUB CHANNELS`, so
/// they compute the same way and pass the answer in — the alternative, a
/// process-global counter maintained at subscribe time, cannot dedupe.
///
/// Defaults to zeroes so a caller without handler context (Lua's `redis.call`,
/// unit tests) still gets a well-formed INFO rather than a missing field.
#[derive(Default, Clone, Copy)]
pub struct InstanceFacts {
    /// Distinct channels with at least one subscriber, across all shards.
    pub pubsub_channels: usize,
    /// Distinct subscribed patterns, across all shards.
    pub pubsub_patterns: usize,
    /// The port this instance's listener is bound to (`--port`).
    ///
    /// Deliberately the CONFIGURED port and not the local port of the socket
    /// INFO arrived on: behind a container port map or a proxy the two differ,
    /// and `tcp_port` exists so a client can hand a peer an address that
    /// actually reaches this server.
    pub tcp_port: u16,
}

/// As [`info_with_keyspace_and_replication`], plus the instance-wide facts
/// only a connection handler can gather.
pub fn info_with_facts(
    db: &Database,
    args: &[Frame],
    keyspace: &[(u64, u64)],
    real_replication: Option<&str>,
    facts: &InstanceFacts,
) -> Frame {
    use std::fmt::Write as _;
    let text = info_raw(db, facts);
    // Rebuild everything up to the fallback "# Keyspace" section, then emit
    // the accurate per-db lines. Filtering runs afterwards, so a request for a
    // single section still gets the ACCURATE keyspace numbers.
    let Some(cut) = text.find("# Keyspace\r\n") else {
        return crate::command::info_sections::finalize(&text, real_replication, args);
    };
    let mut sections = String::with_capacity(text.len() + keyspace.len() * 32);
    sections.push_str(&text[..cut]);
    sections.push_str("# Keyspace\r\n");
    for (db_idx, (keys, expires)) in keyspace.iter().enumerate() {
        if *keys > 0 {
            let _ = write!(
                sections,
                "db{db_idx}:keys={keys},expires={expires},avg_ttl=0\r\n"
            );
        }
    }
    crate::command::info_sections::finalize(&sections, real_replication, args)
}

/// INFO command handler (read-only variant for RwLock read path).
///
/// Identical to info() -- Database methods used (len, expires_count) are already &self.
pub fn info_readonly(db: &Database, args: &[Frame]) -> Frame {
    info(db, args)
}

/// AUTH command handler.
///
/// Authenticates the client with the configured password.
/// Returns OK on success, WRONGPASS on mismatch, or ERR if no password is configured.
pub fn auth(args: &[Frame], requirepass: &Option<String>) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'AUTH' command",
        ));
    }

    let password = match requirepass {
        Some(p) => p,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR Client sent AUTH, but no password is set",
            ));
        }
    };

    let provided = match &args[0] {
        Frame::BulkString(s) => s,
        Frame::SimpleString(s) => s,
        _ => {
            return Frame::Error(Bytes::from_static(b"ERR invalid password type"));
        }
    };

    if provided.as_ref() == password.as_bytes() {
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else {
        Frame::Error(Bytes::from_static(
            b"WRONGPASS invalid username-password pair or user is disabled.",
        ))
    }
}

/// AUTH command handler -- ACL-aware.
/// Handles 1-arg form (AUTH password -> authenticate as "default") and
/// 2-arg form (AUTH username password -> authenticate as named user).
/// Returns (response_frame, Option<authenticated_username>).
pub fn auth_acl(
    args: &[Frame],
    acl_table: &std::sync::Arc<std::sync::RwLock<crate::acl::AclTable>>,
) -> (Frame, Option<String>) {
    match args.len() {
        1 => {
            let password = match extract_bytes_ref(&args[0]) {
                Some(p) => String::from_utf8_lossy(p).to_string(),
                None => {
                    return (
                        Frame::Error(Bytes::from_static(b"ERR invalid password")),
                        None,
                    );
                }
            };
            // Fail closed: if the ACL lock is poisoned, deny authentication
            let Ok(table) = acl_table.read() else {
                return (
                    Frame::Error(Bytes::from_static(b"ERR internal ACL error")),
                    None,
                );
            };
            match table.authenticate("default", &password) {
                Some(username) => (
                    Frame::SimpleString(Bytes::from_static(b"OK")),
                    Some(username),
                ),
                None => (
                    Frame::Error(Bytes::from_static(
                        b"WRONGPASS invalid username-password pair or user is disabled.",
                    )),
                    None,
                ),
            }
        }
        2 => {
            let username = match extract_bytes_ref(&args[0]) {
                Some(u) => String::from_utf8_lossy(u).to_string(),
                None => {
                    return (
                        Frame::Error(Bytes::from_static(b"ERR invalid username")),
                        None,
                    );
                }
            };
            let password = match extract_bytes_ref(&args[1]) {
                Some(p) => String::from_utf8_lossy(p).to_string(),
                None => {
                    return (
                        Frame::Error(Bytes::from_static(b"ERR invalid password")),
                        None,
                    );
                }
            };
            // Fail closed: if the ACL lock is poisoned, deny authentication
            let Ok(table) = acl_table.read() else {
                return (
                    Frame::Error(Bytes::from_static(b"ERR internal ACL error")),
                    None,
                );
            };
            match table.authenticate(&username, &password) {
                Some(uname) => (Frame::SimpleString(Bytes::from_static(b"OK")), Some(uname)),
                None => (
                    Frame::Error(Bytes::from_static(
                        b"WRONGPASS invalid username-password pair or user is disabled.",
                    )),
                    None,
                ),
            }
        }
        _ => (
            Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'AUTH' command",
            )),
            None,
        ),
    }
}

/// HELLO command handler -- ACL-aware variant.
///
/// Like hello() but uses acl_table for AUTH option instead of requirepass.
/// Returns (response_frame, new_protocol_version, new_client_name, authenticated_username)
pub fn hello_acl(
    args: &[Frame],
    current_proto: u8,
    client_id: u64,
    acl_table: &std::sync::Arc<std::sync::RwLock<crate::acl::AclTable>>,
    authenticated: &mut bool,
    // (role, mode) read from ReplicationState/ClusterState by the caller.
    // Previously these were `Bytes::from_static` literals — "master" and
    // "standalone" — so a replica told HELLO it was a master while telling
    // INFO replication it was a slave, on the same connection.
    role_mode: (&'static str, &'static str),
) -> (Frame, u8, Option<Bytes>, Option<String>) {
    let mut proto = current_proto;
    let mut client_name: Option<Bytes> = None;
    let mut auth_user: Option<String> = None;
    let mut i = 0;

    // Parse optional protover
    if i < args.len() {
        if let Some(ver_bytes) = extract_bytes_ref(&args[i]) {
            if let Ok(ver_str) = std::str::from_utf8(ver_bytes) {
                if let Ok(ver) = ver_str.parse::<u8>() {
                    if ver != 2 && ver != 3 {
                        return (
                            Frame::Error(Bytes::from_static(
                                b"NOPROTO unsupported protocol version",
                            )),
                            current_proto,
                            None,
                            None,
                        );
                    }
                    proto = ver;
                    i += 1;
                }
            }
        }
    }

    // Parse optional AUTH and SETNAME
    while i < args.len() {
        if let Some(keyword) = extract_bytes_ref(&args[i]) {
            if keyword.eq_ignore_ascii_case(b"AUTH") {
                if i + 2 >= args.len() {
                    return (
                        Frame::Error(Bytes::from_static(
                            b"ERR Syntax error in HELLO option 'auth'",
                        )),
                        current_proto,
                        None,
                        None,
                    );
                }
                // AUTH username password
                let username = match extract_bytes_ref(&args[i + 1]) {
                    Some(u) => String::from_utf8_lossy(u).to_string(),
                    None => {
                        return (
                            Frame::Error(Bytes::from_static(b"ERR invalid username")),
                            current_proto,
                            None,
                            None,
                        );
                    }
                };
                let password = match extract_bytes_ref(&args[i + 2]) {
                    Some(p) => String::from_utf8_lossy(p).to_string(),
                    None => {
                        return (
                            Frame::Error(Bytes::from_static(b"ERR invalid password")),
                            current_proto,
                            None,
                            None,
                        );
                    }
                };
                // Fail closed: if the ACL lock is poisoned, deny authentication
                let Ok(table) = acl_table.read() else {
                    return (
                        Frame::Error(Bytes::from_static(b"ERR internal ACL error")),
                        current_proto,
                        None,
                        None,
                    );
                };
                match table.authenticate(&username, &password) {
                    Some(uname) => {
                        *authenticated = true;
                        auth_user = Some(uname);
                    }
                    None => {
                        return (
                            Frame::Error(Bytes::from_static(
                                b"WRONGPASS invalid username-password pair or user is disabled.",
                            )),
                            current_proto,
                            None,
                            None,
                        );
                    }
                }
                i += 3;
            } else if keyword.eq_ignore_ascii_case(b"SETNAME") {
                if i + 1 >= args.len() {
                    return (
                        Frame::Error(Bytes::from_static(
                            b"ERR Syntax error in HELLO option 'setname'",
                        )),
                        current_proto,
                        None,
                        None,
                    );
                }
                client_name = extract_bytes_owned(&args[i + 1]);
                i += 2;
            } else {
                return (
                    Frame::Error(Bytes::from(format!(
                        "ERR Unrecognized HELLO option: {:?}",
                        String::from_utf8_lossy(keyword)
                    ))),
                    current_proto,
                    None,
                    None,
                );
            }
        } else {
            break;
        }
    }

    // Build response Map
    let response = Frame::Map(vec![
        (
            Frame::BulkString(Bytes::from_static(b"server")),
            Frame::BulkString(Bytes::from_static(b"moon")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"version")),
            Frame::BulkString(Bytes::from_static(MOON_VERSION.as_bytes())),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"proto")),
            Frame::Integer(proto as i64),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"id")),
            Frame::Integer(client_id as i64),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"mode")),
            Frame::BulkString(Bytes::from_static(role_mode.1.as_bytes())),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"role")),
            Frame::BulkString(Bytes::from_static(role_mode.0.as_bytes())),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"modules")),
            Frame::Array(framevec![]),
        ),
    ]);

    (response, proto, client_name, auth_user)
}

/// Action to take after REPLICAOF command is parsed.
pub enum ReplicaofAction {
    /// Connect to master and start replication.
    StartReplication { host: String, port: u16 },
    /// Promote to master: copy repl_id to repl_id2, generate new repl_id.
    PromoteToMaster,
    /// Already master, REPLICAOF NO ONE is a no-op.
    NoOp,
}

/// REPLICAOF host port -- initiate replication from master at host:port.
/// REPLICAOF NO ONE -- promote this replica to master.
///
/// Returns (response_frame, optional action for the caller to execute).
pub fn replicaof(args: &[Frame]) -> (Frame, Option<ReplicaofAction>) {
    if args.len() != 2 {
        return (
            Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'REPLICAOF' command",
            )),
            None,
        );
    }
    let host = match extract_bytes_ref(&args[0]) {
        Some(h) => String::from_utf8_lossy(h).to_string(),
        None => return (Frame::Error(Bytes::from_static(b"ERR invalid host")), None),
    };
    let port_str = match extract_bytes_ref(&args[1]) {
        Some(p) => String::from_utf8_lossy(p).to_string(),
        None => return (Frame::Error(Bytes::from_static(b"ERR invalid port")), None),
    };

    // REPLICAOF NO ONE -- promote to master
    if host.eq_ignore_ascii_case("NO") && port_str.eq_ignore_ascii_case("ONE") {
        return (
            Frame::SimpleString(Bytes::from_static(b"OK")),
            Some(ReplicaofAction::PromoteToMaster),
        );
    }

    let port: u16 = match port_str.parse() {
        Ok(p) => p,
        Err(_) => {
            return (
                Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                )),
                None,
            );
        }
    };

    (
        Frame::SimpleString(Bytes::from_static(b"OK")),
        Some(ReplicaofAction::StartReplication { host, port }),
    )
}

/// REPLCONF -- replication configuration handshake.
///
/// Recognises the Redis 7 subcommand set. Unknown subcommands return an error
/// instead of silently OK-ing so that client/replica mistakes surface during
/// handshake rather than at the (much later) PSYNC step. State is not yet
/// persisted — that lives in `replication::state::ReplicaInfo` and will be
/// wired once master-side PSYNC is connected.
pub fn replconf(args: &[Frame]) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'REPLCONF' command",
        ));
    }
    // REPLCONF takes key/value pairs. Walk them; reject if any unknown.
    let mut i = 0;
    while i < args.len() {
        let Some(key) = extract_bytes_ref(&args[i]) else {
            return Frame::Error(Bytes::from_static(
                b"ERR syntax error in 'REPLCONF' command",
            ));
        };
        let known_pair_key = key.eq_ignore_ascii_case(b"listening-port")
            || key.eq_ignore_ascii_case(b"ip-address")
            || key.eq_ignore_ascii_case(b"capa")
            || key.eq_ignore_ascii_case(b"ack")
            || key.eq_ignore_ascii_case(b"getack")
            || key.eq_ignore_ascii_case(b"rdb-only")
            || key.eq_ignore_ascii_case(b"rdb-filter-only")
            || key.eq_ignore_ascii_case(b"version");
        if !known_pair_key {
            return Frame::Error(Bytes::from(format!(
                "ERR Unrecognized REPLCONF option: {}",
                String::from_utf8_lossy(key)
            )));
        }
        // Each known option takes one value argument.
        if i + 1 >= args.len() {
            return Frame::Error(Bytes::from(format!(
                "ERR missing value for REPLCONF {}",
                String::from_utf8_lossy(key)
            )));
        }
        i += 2;
    }
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// Extract a byte slice reference from a Frame argument (zero-alloc).
fn extract_bytes_ref(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.as_ref()),
        _ => None,
    }
}

/// Extract an owned Bytes from a Frame argument.
fn extract_bytes_owned(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
        _ => None,
    }
}

/// HELLO command handler.
///
/// HELLO [protover [AUTH username password] [SETNAME clientname]]
/// Returns server info Map. Sets protocol_version if protover given.
/// Returns (response_frame, new_protocol_version, new_client_name)
pub fn hello(
    args: &[Frame],
    current_proto: u8,
    client_id: u64,
    requirepass: &Option<String>,
    authenticated: &mut bool,
    // Same (role, mode) contract as `hello_acl`: kept in lockstep so the two
    // HELLO variants cannot report different identities.
    role_mode: (&'static str, &'static str),
) -> (Frame, u8, Option<Bytes>) {
    let mut proto = current_proto;
    let mut client_name: Option<Bytes> = None;
    let mut i = 0;

    // Parse optional protover
    if i < args.len() {
        if let Some(ver_bytes) = extract_bytes_ref(&args[i]) {
            if let Ok(ver_str) = std::str::from_utf8(ver_bytes) {
                if let Ok(ver) = ver_str.parse::<u8>() {
                    if ver != 2 && ver != 3 {
                        return (
                            Frame::Error(Bytes::from_static(
                                b"NOPROTO unsupported protocol version",
                            )),
                            current_proto,
                            None,
                        );
                    }
                    proto = ver;
                    i += 1;
                }
            }
        }
    }

    // Parse optional AUTH and SETNAME (can appear in any order after protover)
    while i < args.len() {
        if let Some(keyword) = extract_bytes_ref(&args[i]) {
            if keyword.eq_ignore_ascii_case(b"AUTH") {
                // Need username and password (2 more args)
                if i + 2 >= args.len() {
                    return (
                        Frame::Error(Bytes::from_static(
                            b"ERR Syntax error in HELLO option 'auth'",
                        )),
                        current_proto,
                        None,
                    );
                }
                // username is args[i+1] (we ignore it -- single-user mode)
                // password is args[i+2]
                let auth_result = auth(&[args[i + 2].clone()], requirepass);
                if matches!(&auth_result, Frame::Error(_)) {
                    return (auth_result, current_proto, None); // Auth failed, don't change proto
                }
                *authenticated = true;
                i += 3;
            } else if keyword.eq_ignore_ascii_case(b"SETNAME") {
                if i + 1 >= args.len() {
                    return (
                        Frame::Error(Bytes::from_static(
                            b"ERR Syntax error in HELLO option 'setname'",
                        )),
                        current_proto,
                        None,
                    );
                }
                client_name = extract_bytes_owned(&args[i + 1]);
                i += 2;
            } else {
                return (
                    Frame::Error(Bytes::from(format!(
                        "ERR Unrecognized HELLO option: {:?}",
                        String::from_utf8_lossy(keyword)
                    ))),
                    current_proto,
                    None,
                );
            }
        } else {
            break;
        }
    }

    // Build response Map
    let response = Frame::Map(vec![
        (
            Frame::BulkString(Bytes::from_static(b"server")),
            Frame::BulkString(Bytes::from_static(b"moon")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"version")),
            Frame::BulkString(Bytes::from_static(MOON_VERSION.as_bytes())),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"proto")),
            Frame::Integer(proto as i64),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"id")),
            Frame::Integer(client_id as i64),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"mode")),
            Frame::BulkString(Bytes::from_static(role_mode.1.as_bytes())),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"role")),
            Frame::BulkString(Bytes::from_static(role_mode.0.as_bytes())),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"modules")),
            Frame::Array(framevec![]),
        ),
    ]);

    (response, proto, client_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_with_keyspace_lists_every_nonempty_db() {
        let db = Database::new();
        let stats = vec![(0u64, 0u64), (0, 0), (7, 2), (0, 0), (1, 0)];
        let Frame::BulkString(b) = info_with_keyspace(&db, &[], &stats) else {
            panic!("expected bulk string");
        };
        let text = String::from_utf8_lossy(&b);
        assert!(text.contains("# Keyspace\r\n"));
        assert!(text.contains("db2:keys=7,expires=2,avg_ttl=0\r\n"));
        assert!(text.contains("db4:keys=1,expires=0,avg_ttl=0\r\n"));
        assert!(!text.contains("db0:"), "empty dbs must not be listed");
        assert!(!text.contains("db1:"));
        assert!(!text.contains("db3:"));
        // The fallback single-db section must have been replaced, not doubled.
        assert_eq!(text.matches("# Keyspace").count(), 1);
    }

    #[test]
    fn test_ping_no_args() {
        let result = ping(&[]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"PONG")));
    }

    #[test]
    fn test_ping_with_arg() {
        let result = ping(&[Frame::BulkString(Bytes::from_static(b"hello"))]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"hello")));
    }

    #[test]
    fn test_ping_too_many_args() {
        let result = ping(&[
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
        ]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_echo() {
        let result = echo(&[Frame::BulkString(Bytes::from_static(b"hello"))]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"hello")));
    }

    #[test]
    fn test_echo_wrong_arity() {
        let result = echo(&[]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_select_valid() {
        let mut selected = 0usize;
        let result = select(
            &[Frame::BulkString(Bytes::from_static(b"5"))],
            &mut selected,
            16,
        );
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(selected, 5);
    }

    #[test]
    fn test_select_out_of_range() {
        let mut selected = 0usize;
        let result = select(
            &[Frame::BulkString(Bytes::from_static(b"16"))],
            &mut selected,
            16,
        );
        assert!(
            matches!(result, Frame::Error(ref s) if s.as_ref() == b"ERR DB index is out of range")
        );
        assert_eq!(selected, 0); // unchanged
    }

    #[test]
    fn test_select_non_integer() {
        let mut selected = 0usize;
        let result = select(
            &[Frame::BulkString(Bytes::from_static(b"abc"))],
            &mut selected,
            16,
        );
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_command_bare() {
        let result = command(&[]);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_command_docs() {
        let result = command(&[Frame::BulkString(Bytes::from_static(b"DOCS"))]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    #[test]
    fn test_command_docs_lowercase() {
        let result = command(&[Frame::BulkString(Bytes::from_static(b"docs"))]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    #[test]
    fn test_info_basic() {
        let db = Database::new();
        let result = info(&db, &[]);
        match result {
            Frame::BulkString(s) => {
                let text = std::str::from_utf8(&s).unwrap();
                assert!(text.contains("# Server"));
                assert!(text.contains("redis_version:7.4.0"));
                assert!(text.contains("moon_version:"));
                assert!(text.contains("# Keyspace"));
            }
            _ => panic!("Expected BulkString"),
        }
    }

    #[test]
    fn test_info_with_keys() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"key1"),
            crate::storage::Entry::new_string(Bytes::from_static(b"val")),
        );
        let result = info(&db, &[]);
        match result {
            Frame::BulkString(s) => {
                let text = std::str::from_utf8(&s).unwrap();
                assert!(text.contains("db0:keys=1,expires=0,avg_ttl=0"));
            }
            _ => panic!("Expected BulkString"),
        }
    }

    #[test]
    fn test_auth_correct_password() {
        let pass = Some("secret123".to_string());
        let result = auth(
            &[Frame::BulkString(Bytes::from_static(b"secret123"))],
            &pass,
        );
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_auth_wrong_password() {
        let pass = Some("secret123".to_string());
        let result = auth(&[Frame::BulkString(Bytes::from_static(b"wrong"))], &pass);
        assert!(matches!(result, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
    }

    #[test]
    fn test_auth_no_password_configured() {
        let pass: Option<String> = None;
        let result = auth(&[Frame::BulkString(Bytes::from_static(b"anything"))], &pass);
        assert!(matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR Client sent AUTH")));
    }

    #[test]
    fn test_auth_wrong_arity() {
        let pass = Some("secret".to_string());
        let result = auth(&[], &pass);
        assert!(matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR wrong number")));
    }

    // === HELLO command tests ===

    fn get_proto_from_hello_response(frame: &Frame) -> Option<i64> {
        if let Frame::Map(entries) = frame {
            for (k, v) in entries {
                if let Frame::BulkString(key) = k {
                    if key.as_ref() == b"proto" {
                        if let Frame::Integer(n) = v {
                            return Some(*n);
                        }
                    }
                }
            }
        }
        None
    }

    #[test]
    fn test_hello_no_args_returns_current_proto() {
        let mut auth = true;
        let (resp, proto, name) = hello(&[], 2, 1, &None, &mut auth, ("master", "standalone"));
        assert!(matches!(resp, Frame::Map(_)));
        assert_eq!(get_proto_from_hello_response(&resp), Some(2));
        assert_eq!(proto, 2);
        assert!(name.is_none());
    }

    #[test]
    fn test_hello_upgrade_to_resp3() {
        let mut auth = true;
        let (resp, proto, _) = hello(
            &[Frame::BulkString(Bytes::from_static(b"3"))],
            2,
            1,
            &None,
            &mut auth,
            ("master", "standalone"),
        );
        assert_eq!(proto, 3);
        assert_eq!(get_proto_from_hello_response(&resp), Some(3));
    }

    #[test]
    fn test_hello_downgrade_to_resp2() {
        let mut auth = true;
        let (resp, proto, _) = hello(
            &[Frame::BulkString(Bytes::from_static(b"2"))],
            3,
            1,
            &None,
            &mut auth,
            ("master", "standalone"),
        );
        assert_eq!(proto, 2);
        assert_eq!(get_proto_from_hello_response(&resp), Some(2));
    }

    #[test]
    fn test_hello_with_auth_success() {
        let pass = Some("secret".to_string());
        let mut auth = false;
        let (resp, proto, _) = hello(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"AUTH")),
                Frame::BulkString(Bytes::from_static(b"default")),
                Frame::BulkString(Bytes::from_static(b"secret")),
            ],
            2,
            1,
            &pass,
            &mut auth,
            ("master", "standalone"),
        );
        assert_eq!(proto, 3);
        assert!(matches!(resp, Frame::Map(_)));
        assert!(auth); // authenticated
    }

    #[test]
    fn test_hello_with_auth_failure() {
        let pass = Some("secret".to_string());
        let mut auth = false;
        let (resp, proto, _) = hello(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"AUTH")),
                Frame::BulkString(Bytes::from_static(b"default")),
                Frame::BulkString(Bytes::from_static(b"wrong")),
            ],
            2,
            1,
            &pass,
            &mut auth,
            ("master", "standalone"),
        );
        // Auth failed: proto stays at current, response is error
        assert_eq!(proto, 2);
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
        assert!(!auth); // not authenticated
    }

    #[test]
    fn test_hello_with_setname() {
        let mut auth = true;
        let (_, _, name) = hello(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"SETNAME")),
                Frame::BulkString(Bytes::from_static(b"myclient")),
            ],
            2,
            1,
            &None,
            &mut auth,
            ("master", "standalone"),
        );
        assert_eq!(name, Some(Bytes::from_static(b"myclient")));
    }

    #[test]
    fn test_hello_noproto() {
        let mut auth = true;
        let (resp, proto, _) = hello(
            &[Frame::BulkString(Bytes::from_static(b"4"))],
            2,
            1,
            &None,
            &mut auth,
            ("master", "standalone"),
        );
        assert_eq!(proto, 2); // unchanged
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"NOPROTO")));
    }

    #[test]
    fn test_client_id_returns_integer() {
        let result = client_id(42);
        assert_eq!(result, Frame::Integer(42));
    }

    // === REPLICAOF command tests ===

    #[test]
    fn test_replicaof_start_replication() {
        let (resp, action) = replicaof(&[
            Frame::BulkString(Bytes::from_static(b"127.0.0.1")),
            Frame::BulkString(Bytes::from_static(b"6379")),
        ]);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert!(matches!(
            action,
            Some(ReplicaofAction::StartReplication { ref host, port })
            if host == "127.0.0.1" && port == 6379
        ));
    }

    #[test]
    fn test_replicaof_no_one() {
        let (resp, action) = replicaof(&[
            Frame::BulkString(Bytes::from_static(b"NO")),
            Frame::BulkString(Bytes::from_static(b"ONE")),
        ]);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert!(matches!(action, Some(ReplicaofAction::PromoteToMaster)));
    }

    #[test]
    fn test_replicaof_no_one_case_insensitive() {
        let (resp, action) = replicaof(&[
            Frame::BulkString(Bytes::from_static(b"no")),
            Frame::BulkString(Bytes::from_static(b"one")),
        ]);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert!(matches!(action, Some(ReplicaofAction::PromoteToMaster)));
    }

    #[test]
    fn test_replicaof_wrong_arity() {
        let (resp, action) = replicaof(&[Frame::BulkString(Bytes::from_static(b"host"))]);
        assert!(matches!(resp, Frame::Error(_)));
        assert!(action.is_none());
    }

    #[test]
    fn test_replicaof_invalid_port() {
        let (resp, action) = replicaof(&[
            Frame::BulkString(Bytes::from_static(b"localhost")),
            Frame::BulkString(Bytes::from_static(b"notaport")),
        ]);
        assert!(matches!(resp, Frame::Error(_)));
        assert!(action.is_none());
    }

    // === REPLCONF command tests ===

    #[test]
    fn test_replconf_listening_port() {
        let resp = replconf(&[
            Frame::BulkString(Bytes::from_static(b"listening-port")),
            Frame::BulkString(Bytes::from_static(b"6380")),
        ]);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_replconf_capa() {
        let resp = replconf(&[
            Frame::BulkString(Bytes::from_static(b"capa")),
            Frame::BulkString(Bytes::from_static(b"psync2")),
        ]);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_replconf_empty_args() {
        let resp = replconf(&[]);
        assert!(matches!(resp, Frame::Error(_)));
    }

    #[test]
    fn test_replconf_multi_pair_capa_handshake() {
        // This is the exact shape the replica sends during PSYNC2 handshake.
        let resp = replconf(&[
            Frame::BulkString(Bytes::from_static(b"capa")),
            Frame::BulkString(Bytes::from_static(b"eof")),
            Frame::BulkString(Bytes::from_static(b"capa")),
            Frame::BulkString(Bytes::from_static(b"psync2")),
        ]);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_replconf_ack_offset() {
        let resp = replconf(&[
            Frame::BulkString(Bytes::from_static(b"ACK")),
            Frame::BulkString(Bytes::from_static(b"12345")),
        ]);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_replconf_rejects_unknown_subcommand() {
        let resp = replconf(&[
            Frame::BulkString(Bytes::from_static(b"made-up")),
            Frame::BulkString(Bytes::from_static(b"value")),
        ]);
        match resp {
            Frame::Error(msg) => {
                assert!(
                    msg.as_ref()
                        .starts_with(b"ERR Unrecognized REPLCONF option"),
                    "got: {}",
                    String::from_utf8_lossy(&msg)
                );
            }
            other => panic!("expected error, got {:?}", other),
        }
    }

    #[test]
    fn test_replconf_missing_value_errors() {
        let resp = replconf(&[Frame::BulkString(Bytes::from_static(b"listening-port"))]);
        assert!(matches!(resp, Frame::Error(_)));
    }

    // === auth_acl tests ===

    fn make_acl_table() -> std::sync::Arc<std::sync::RwLock<crate::acl::AclTable>> {
        use crate::acl::{AclTable, AclUser};
        let mut table = AclTable::new();
        table.set_user("default".to_string(), AclUser::new_default_nopass());
        std::sync::Arc::new(std::sync::RwLock::new(table))
    }

    fn make_acl_table_with_password() -> std::sync::Arc<std::sync::RwLock<crate::acl::AclTable>> {
        use crate::acl::{AclTable, AclUser};
        let mut table = AclTable::new();
        table.set_user(
            "default".to_string(),
            AclUser::new_default_with_password("secret"),
        );
        std::sync::Arc::new(std::sync::RwLock::new(table))
    }

    #[test]
    fn test_auth_acl_1arg_nopass() {
        let table = make_acl_table();
        let (resp, user) = auth_acl(&[Frame::BulkString(Bytes::from_static(b"anypass"))], &table);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(user, Some("default".to_string()));
    }

    #[test]
    fn test_auth_acl_1arg_wrong_password() {
        let table = make_acl_table_with_password();
        let (resp, user) = auth_acl(&[Frame::BulkString(Bytes::from_static(b"wrong"))], &table);
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
        assert!(user.is_none());
    }

    #[test]
    fn test_auth_acl_1arg_correct_password() {
        let table = make_acl_table_with_password();
        let (resp, user) = auth_acl(&[Frame::BulkString(Bytes::from_static(b"secret"))], &table);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(user, Some("default".to_string()));
    }

    #[test]
    fn test_auth_acl_2arg_named_user() {
        let table = make_acl_table();
        // Create alice with password
        {
            let mut t = table.write().unwrap();
            t.apply_setuser("alice", &["on", ">alicepass", "~*", "+@all"]);
        }
        let (resp, user) = auth_acl(
            &[
                Frame::BulkString(Bytes::from_static(b"alice")),
                Frame::BulkString(Bytes::from_static(b"alicepass")),
            ],
            &table,
        );
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(user, Some("alice".to_string()));
    }

    #[test]
    fn test_auth_acl_2arg_wrong_password() {
        let table = make_acl_table();
        {
            let mut t = table.write().unwrap();
            t.apply_setuser("alice", &["on", ">alicepass", "~*", "+@all"]);
        }
        let (resp, user) = auth_acl(
            &[
                Frame::BulkString(Bytes::from_static(b"alice")),
                Frame::BulkString(Bytes::from_static(b"wrong")),
            ],
            &table,
        );
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
        assert!(user.is_none());
    }

    #[test]
    fn test_auth_acl_disabled_user() {
        let table = make_acl_table();
        {
            let mut t = table.write().unwrap();
            t.apply_setuser("alice", &["off", ">alicepass"]);
        }
        let (resp, user) = auth_acl(
            &[
                Frame::BulkString(Bytes::from_static(b"alice")),
                Frame::BulkString(Bytes::from_static(b"alicepass")),
            ],
            &table,
        );
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
        assert!(user.is_none());
    }

    #[test]
    fn test_auth_acl_wrong_arity() {
        let table = make_acl_table();
        let (resp, user) = auth_acl(&[], &table);
        assert!(matches!(resp, Frame::Error(_)));
        assert!(user.is_none());
    }

    // === hello_acl tests ===

    #[test]
    fn test_hello_acl_no_args() {
        let table = make_acl_table();
        let mut auth = true;
        let (resp, proto, name, user) =
            hello_acl(&[], 2, 1, &table, &mut auth, ("master", "standalone"));
        assert!(matches!(resp, Frame::Map(_)));
        assert_eq!(proto, 2);
        assert!(name.is_none());
        assert!(user.is_none());
    }

    #[test]
    fn test_hello_acl_with_auth_success() {
        let table = make_acl_table_with_password();
        let mut auth = false;
        let (resp, proto, _, user) = hello_acl(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"AUTH")),
                Frame::BulkString(Bytes::from_static(b"default")),
                Frame::BulkString(Bytes::from_static(b"secret")),
            ],
            2,
            1,
            &table,
            &mut auth,
            ("master", "standalone"),
        );
        assert_eq!(proto, 3);
        assert!(matches!(resp, Frame::Map(_)));
        assert!(auth);
        assert_eq!(user, Some("default".to_string()));
    }

    #[test]
    fn test_hello_acl_with_auth_failure() {
        let table = make_acl_table_with_password();
        let mut auth = false;
        let (resp, proto, _, user) = hello_acl(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"AUTH")),
                Frame::BulkString(Bytes::from_static(b"default")),
                Frame::BulkString(Bytes::from_static(b"wrong")),
            ],
            2,
            1,
            &table,
            &mut auth,
            ("master", "standalone"),
        );
        assert_eq!(proto, 2); // unchanged
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
        assert!(!auth);
        assert!(user.is_none());
    }
}

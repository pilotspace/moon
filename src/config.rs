use std::path::PathBuf;

use clap::Parser;

/// Server configuration parsed from command-line arguments.
#[derive(Parser, Debug, Clone)]
#[command(name = "moon", about = "A Redis-compatible server")]
pub struct ServerConfig {
    /// Bind address
    #[arg(long, default_value = "127.0.0.1")]
    pub bind: String,

    /// Port to listen on
    #[arg(long, short, default_value_t = 6379)]
    pub port: u16,

    /// Admin/metrics HTTP port (0 = disabled). Serves /metrics, /healthz, /readyz.
    #[arg(long, default_value_t = 0)]
    pub admin_port: u16,

    // ── Admin/console hardening (HARD-01/02/03, Phase 137) ──────────
    /// Require Bearer/HMAC auth on the admin/console HTTP port.
    /// When set, `/api/v1/*` rejects unauthenticated requests with 401.
    /// `/healthz`, `/readyz`, `/metrics`, and CORS preflight bypass auth.
    #[arg(long = "console-auth-required", default_value_t = false)]
    pub console_auth_required: bool,

    /// HMAC-SHA256 secret for --console-auth-required token verification.
    /// When auth is required and this is empty, an ephemeral 32-byte secret
    /// is generated at startup and logged once (tokens will not survive
    /// restart). Operators SHOULD set this for reproducible deploys.
    #[arg(long = "console-auth-secret", default_value = "")]
    pub console_auth_secret: String,

    /// CORS origin allowlist for the admin port (repeatable).
    /// Default: http://localhost:5173 and http://127.0.0.1:5173 (Vite dev).
    /// Wildcard "*" is only permitted when --console-auth-required is false.
    #[arg(
        long = "console-cors-origin",
        value_parser = clap::value_parser!(String),
        action = clap::ArgAction::Append,
    )]
    pub console_cors_origin: Vec<String>,

    /// Per-IP rate limit for the admin HTTP port in requests per second.
    /// 0 disables rate limiting entirely.
    #[arg(long = "console-rate-limit", default_value_t = 1000.0)]
    pub console_rate_limit: f64,

    /// Token-bucket burst capacity for the admin rate limiter.
    /// Default: 2x --console-rate-limit.
    #[arg(long = "console-rate-burst", default_value_t = 2000.0)]
    pub console_rate_burst: f64,

    /// Slowlog threshold in microseconds (commands slower than this are logged)
    #[arg(long = "slowlog-log-slower-than", default_value_t = 10000)]
    pub slowlog_log_slower_than: u64,

    /// Maximum entries in the slowlog
    #[arg(long = "slowlog-max-len", default_value_t = 128)]
    pub slowlog_max_len: usize,

    /// Validate configuration and exit without starting the server
    #[arg(long = "check-config")]
    pub check_config: bool,

    /// Number of databases
    #[arg(long, default_value_t = 16)]
    pub databases: usize,

    /// Require clients to authenticate with this password
    #[arg(long)]
    pub requirepass: Option<String>,

    /// Enable append-only file persistence (yes/no)
    #[arg(long, default_value = "yes")]
    pub appendonly: String,

    /// AOF fsync policy (always/everysec/no)
    #[arg(long, default_value = "everysec")]
    pub appendfsync: String,

    /// RDB auto-save rules (e.g., "3600 1 300 100")
    #[arg(long)]
    pub save: Option<String>,

    /// Directory for persistence files
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// RDB snapshot filename
    #[arg(long, default_value = "dump.rdb")]
    pub dbfilename: String,

    /// AOF filename
    #[arg(long, default_value = "appendonly.aof")]
    pub appendfilename: String,

    /// Maximum memory in bytes (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    pub maxmemory: usize,

    /// Eviction policy when maxmemory is reached
    #[arg(long, default_value = "noeviction")]
    pub maxmemory_policy: String,

    /// Number of random keys to sample for eviction
    #[arg(long, default_value_t = 5)]
    pub maxmemory_samples: usize,

    /// Number of shards (0 = auto-detect from CPU count)
    #[arg(long, default_value_t = 0)]
    pub shards: usize,

    /// Initial keyspace size hint (total entries across all shards, 0 = disabled).
    ///
    /// When non-zero, pre-sizes the default database (DB 0) on each shard to
    /// hold approximately `hint / shards` entries without triggering segment
    /// splits. Trades ~340 KB per shard of startup RSS per 60 K hinted entries
    /// for elimination of the 10 % `split_segment` CPU cost on write-heavy
    /// workloads that stay within the hint.
    ///
    /// Safe default is 0 (no pre-sizing). Typical values: 1_000_000 for a
    /// 1 M-key benchmark; `maxmemory / 128` for bounded deployments.
    #[arg(long = "initial-keyspace-hint", default_value_t = 0)]
    pub initial_keyspace_hint: usize,

    /// Path to ACL file (Redis-compatible format)
    #[arg(long)]
    pub aclfile: Option<String>,

    /// Enable cluster mode
    #[arg(long, default_value_t = false)]
    pub cluster_enabled: bool,

    /// Cluster node timeout in milliseconds (PFAIL detection threshold)
    #[arg(long, default_value_t = 15000)]
    pub cluster_node_timeout: u64,

    /// Enable protected mode (reject non-loopback connections when no password set)
    #[arg(long, default_value = "yes")]
    pub protected_mode: String,

    /// Maximum number of simultaneous client connections (0 = unlimited)
    #[arg(long, default_value_t = 10000)]
    pub maxclients: usize,

    /// Close connections idle for more than N seconds (0 = disabled)
    #[arg(long, default_value_t = 0)]
    pub timeout: u64,

    /// TCP keepalive interval in seconds (0 = disabled). Sets SO_KEEPALIVE on accepted sockets.
    #[arg(long = "tcp-keepalive", default_value_t = 300)]
    pub tcp_keepalive: u64,

    /// Maximum number of entries in the ACL log
    #[arg(long, default_value_t = 128)]
    pub acllog_max_len: usize,

    /// TLS port (0 = TLS disabled)
    #[arg(long, default_value_t = 0)]
    pub tls_port: u16,

    /// Path to TLS certificate file (PEM format)
    #[arg(long)]
    pub tls_cert_file: Option<String>,

    /// Path to TLS private key file (PEM format)
    #[arg(long)]
    pub tls_key_file: Option<String>,

    /// Path to CA certificate for client authentication (mTLS)
    #[arg(long)]
    pub tls_ca_cert_file: Option<String>,

    /// TLS 1.3 cipher suites (comma-separated, e.g., "TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256")
    #[arg(long)]
    pub tls_ciphersuites: Option<String>,

    // ── io_uring tuning ─────────────────────────────────────────────
    /// Enable io_uring SQPOLL mode with the given idle timeout in milliseconds.
    /// The kernel spins a dedicated SQ poll thread, eliminating io_uring_enter()
    /// syscalls on the submission path. Requires CAP_SYS_NICE or root; falls back
    /// gracefully if unprivileged. Linux-only; ignored on other platforms.
    #[arg(long = "uring-sqpoll")]
    pub uring_sqpoll_ms: Option<u32>,

    // ── MoonStore v2: Disk Offload ──────────────────────────────────
    /// Enable disk offload (tiered storage: RAM -> mmap -> NVMe)
    #[arg(long = "disk-offload", default_value = "enable")]
    pub disk_offload: String,

    /// Directory for disk offload files (default: same as --dir)
    #[arg(long = "disk-offload-dir")]
    pub disk_offload_dir: Option<PathBuf>,

    /// RAM pressure threshold to trigger disk offload (0.0-1.0).
    /// NOTE: Consumed by the memory pressure cascade (deferred to a future phase).
    /// Currently parsed and stored but not acted upon at runtime.
    #[arg(long = "disk-offload-threshold", default_value_t = 0.85)]
    pub disk_offload_threshold: f64,

    /// Seconds before sealed segments transition to warm tier
    #[arg(long = "segment-warm-after", default_value_t = 3600)]
    pub segment_warm_after: u64,

    // ── MoonStore v2: PageCache ─────────────────────────────────────
    /// PageCache memory budget (e.g., "256mb", "1gb"). Default: 25% of maxmemory.
    #[arg(long = "pagecache-size")]
    pub pagecache_size: Option<String>,

    // ── MoonStore v2: Checkpoint ────────────────────────────────────
    /// Checkpoint timeout in seconds
    #[arg(long = "checkpoint-timeout", default_value_t = 300)]
    pub checkpoint_timeout: u64,

    /// Fraction of checkpoint interval to spread dirty page flushes (0.0-1.0)
    #[arg(long = "checkpoint-completion", default_value_t = 0.9)]
    pub checkpoint_completion: f64,

    /// Maximum WAL size before triggering checkpoint (e.g., "256mb")
    #[arg(long = "max-wal-size", default_value = "256mb")]
    pub max_wal_size: String,

    // ── MoonStore v2: WAL v3 ────────────────────────────────────────
    /// Enable Full Page Images for torn page defense
    #[arg(long = "wal-fpi", default_value = "enable")]
    pub wal_fpi: String,

    /// FPI compression codec
    #[arg(long = "wal-compression", default_value = "lz4")]
    pub wal_compression: String,

    /// WAL segment file size (e.g., "16mb")
    #[arg(long = "wal-segment-size", default_value = "16mb")]
    pub wal_segment_size: String,

    /// P6: minimum milliseconds since last checkpoint before the WAL
    /// ceiling-trigger is allowed to force another checkpoint + aggressive
    /// recycle. Prevents thrashing when WAL is over max but a checkpoint
    /// just completed moments ago. Default: 10 000ms (10 seconds).
    #[arg(long = "wal-max-checkpoint-lag-ms", default_value_t = 10_000)]
    pub wal_max_checkpoint_lag_ms: u64,

    // ── MoonStore v2: Vector Warm Tier ──────────────────────────────
    /// mlock vector codes pages into RAM
    #[arg(long = "vec-codes-mlock", default_value = "enable")]
    pub vec_codes_mlock: String,

    /// Maximum resident bytes allowed across all warm-tier vector segments on
    /// this shard (e.g. "2gb", "512mb", "0"). When the total exceeds this
    /// limit the budget enforcer drops LRU warm segments from memory; they
    /// are reloaded from disk on next access. Set to "0" to disable.
    ///
    /// Default: "2gb". Tune down for cgroup-constrained containers.
    #[arg(long = "vec-warm-mmap-budget", default_value = "2gb")]
    pub vec_warm_mmap_budget: String,

    // ── Cold-tier / DiskANN config stubs (not yet consumed) ─────────
    /// Seconds after last access before a WARM segment is promoted to COLD.
    /// Not yet consumed — reserved for the WARM->COLD transition timer.
    #[arg(long = "segment-cold-after", default_value_t = 86_400)]
    pub segment_cold_after: u64,

    /// Minimum queries-per-second threshold; segments below this are COLD candidates.
    /// Not yet consumed — reserved for the WARM->COLD transition heuristic.
    #[arg(long = "segment-cold-min-qps", default_value_t = 0.1)]
    pub segment_cold_min_qps: f64,

    // ── Allocator tuning (PERF-10) ────────────────────────────────
    /// Override jemalloc narenas cap (default 8). Range 1-256.
    /// Reduces VSZ on multi-core hosts (4*ncpus default -> 8). No-op for
    /// non-jemalloc builds. Implemented via MALLOC_CONF env-var injection
    /// at process start (re-spawn before jemalloc init).
    #[arg(long = "memory-arenas-cap", value_name = "N", default_value_t = 8, value_parser = clap::value_parser!(u32).range(1..=256))]
    pub memory_arenas_cap: u32,

    /// DiskANN beam width for disk-resident vector search.
    /// Not yet consumed — reserved for the DiskANN search implementation.
    #[arg(long = "vec-diskann-beam-width", default_value_t = 8)]
    pub vec_diskann_beam_width: u32,

    /// Number of HNSW upper levels cached in memory for DiskANN hybrid search.
    /// Not yet consumed — reserved for the DiskANN cache layer.
    #[arg(long = "vec-diskann-cache-levels", default_value_t = 3)]
    pub vec_diskann_cache_levels: u32,

    // ── Wave-3 P9: Cold-tier orphan sweeper ────────────────────────
    /// Interval in seconds between cold-tier orphan sweep passes.
    ///
    /// The sweeper walks the cold index, identifies entries whose key is now
    /// present in the hot in-memory DashTable (hot-shadow orphans), deletes
    /// the on-disk DataFile, and tombstones the manifest entry.
    ///
    /// Set to 0 to disable the sweeper entirely.
    /// Default: 300 (5 minutes). Recommended range: 60–3600.
    #[arg(long = "cold-orphan-sweep-interval-secs", default_value_t = 300)]
    pub cold_orphan_sweep_interval_secs: u64,

    // ── MoonStore v2: Point-in-time recovery (PITR) ────────────────
    /// Stop WAL replay at this LSN during recovery. Records with LSN > target
    /// are skipped. Mutually exclusive with --recovery-target-time; if both
    /// are set the LSN takes precedence. Wired by P3 in recovery.rs.
    #[arg(long = "recovery-target-lsn", value_name = "LSN")]
    pub recovery_target_lsn: Option<u64>,

    /// Stop WAL replay at the first record whose timestamp exceeds this
    /// RFC3339 instant (e.g. "2026-05-12T08:30:00Z"). The recovery scanner
    /// resolves it to an LSN during P3. Mutually exclusive with
    /// --recovery-target-lsn (LSN wins if both are set).
    #[arg(long = "recovery-target-time", value_name = "RFC3339")]
    pub recovery_target_time: Option<String>,

    // ── P1: Manifest tombstone GC ───────────────────────────────────
    /// Minimum manifest epoch age before a tombstoned file entry is physically
    /// removed from the manifest. Each committed epoch is a new snapshot
    /// generation; retain_epochs=2 means a tombstone must survive two full
    /// manifest commits before GC can prune it. Guards readers holding old
    /// snapshot views opened before the tombstone was written.
    #[arg(long = "manifest-tombstone-retain-epochs", default_value_t = 2)]
    pub manifest_tombstone_retain_epochs: u64,

    /// Minimum wall-clock age in seconds before a tombstoned file entry is
    /// physically removed from the manifest. Must be ≥ the longest expected
    /// reader snapshot age. Default 300 s (5 min) covers most operational
    /// scan/backup windows without accumulating unbounded tombstone bloat.
    #[arg(long = "manifest-tombstone-retain-secs", default_value_t = 300)]
    pub manifest_tombstone_retain_secs: u64,

    // ── MA12: Disk free-space monitor ───────────────────────────────────────
    /// Pause writes when filesystem free space drops below this percentage.
    ///
    /// The disk monitor samples the WAL/data volume every 5 seconds.
    /// When free % < `disk_free_min_pct`, all write commands return
    /// `MOONERR diskfull: writes paused` until space recovers.
    /// Writes resume when free % > `disk_free_min_pct + 5` (hysteresis).
    ///
    /// Set to 0 to disable the monitor entirely.
    #[arg(long = "disk-free-min-pct", default_value_t = 5, value_parser = clap::value_parser!(u8).range(0..=95))]
    pub disk_free_min_pct: u8,

    // ── P3: MVCC committed-set prune margin ────────────────────────────────
    /// Number of LSN units to keep in the MVCC committed treemap above the
    /// oldest active snapshot watermark before pruning entries below.
    ///
    /// At the 1-second sweep tick, entries with txn_id < (oldest_snapshot - margin)
    /// are removed from the RoaringTreemap. Any txn_id below the resulting floor
    /// is considered globally committed (short-circuit in `is_committed`).
    ///
    /// Default 1000: at 100K commits/s, this retains ~10ms of history — more
    /// than enough for any in-flight snapshot to resolve its visibility window.
    ///
    /// Set to 0 to disable pruning (not recommended for long-running deployments).
    #[arg(long = "mvcc-committed-prune-margin", default_value_t = 1000)]
    pub mvcc_committed_prune_margin: u64,

    // ── MA1: Write-stall on immutable segment backlog ──────────────────────
    /// Maximum number of unflushed immutable vector/graph segments before
    /// foreground writes are stalled with `MOONERR busy: compaction backlog`.
    ///
    /// This is Moon's analog of RocksDB's `level0_stop_writes_trigger`.
    /// Background compaction (FT.COMPACT, GRAPH.COMPACT) is NOT affected.
    ///
    /// Default 20. Set to 0 to disable the stall guard.
    #[arg(long = "max-unflushed-immutable-segments", default_value_t = 20)]
    pub max_unflushed_immutable_segments: u64,

    // ── MA2: old_snapshot_threshold — stuck-snapshot kill ─────────────────
    /// Wall-clock age in seconds after which an active MVCC snapshot is
    /// forcibly killed by the 1-second sweep tick.
    ///
    /// Analog of PostgreSQL's `old_snapshot_threshold`. When a snapshot's age
    /// exceeds this threshold, its entry in the active map is flagged as killed.
    /// The killed snapshot is excluded from the `oldest_snapshot` watermark so
    /// `prune_committed` can advance past it and free the RoaringTreemap memory.
    ///
    /// Callers that attempt to use a killed snapshot receive:
    ///   `MOONERR snapshot too old: <txn_id>`
    ///
    /// Set to 0 to disable automatic threshold killing (KILL SNAPSHOT command
    /// still works for manual operator intervention).
    ///
    /// Default 600 (10 minutes). Covers most operational scan/backup windows.
    #[arg(long = "mvcc-old-snapshot-threshold-secs", default_value_t = 600)]
    pub mvcc_old_snapshot_threshold_secs: u64,

    // ── P4: Autovacuum daemon ──────────────────────────────────────────────
    /// Enable or disable the per-shard autovacuum daemon.
    ///
    /// When enabled, the daemon runs background reclamation passes (manifest
    /// tombstone GC, WAL recycle, vector compact) on a configurable interval.
    /// Use `disable` only for debugging or when manual VACUUM is preferred.
    ///
    /// Valid values: `enable` (default) | `disable`.
    #[arg(long = "autovacuum", default_value = "enable")]
    pub autovacuum: String,

    /// Minimum autovacuum time budget per tick in milliseconds.
    ///
    /// The Postgres-style AIMD throttle will never shrink the budget below
    /// this floor, even under sustained high-latency load. Setting too low
    /// means background work may lag indefinitely under load.
    ///
    /// Default: 5 ms.
    #[arg(long = "autovacuum-budget-ms-min", default_value_t = 5)]
    pub autovacuum_budget_ms_min: u64,

    /// Maximum autovacuum time budget per tick in milliseconds.
    ///
    /// The AIMD throttle will never grow the budget above this ceiling.
    /// Increasing this allows more aggressive background work when the server
    /// is idle, at the cost of occasional latency spikes on idle→busy transitions.
    ///
    /// Default: 200 ms.
    #[arg(long = "autovacuum-budget-ms-max", default_value_t = 200)]
    pub autovacuum_budget_ms_max: u64,

    /// Target P95 request latency in milliseconds for the autovacuum throttle.
    ///
    /// When observed P95 exceeds this target, the autovacuum daemon shrinks its
    /// time budget by 25 % (Postgres `vacuum_cost_delay` analogy). When P95
    /// drops below `target/2`, the budget grows by 25 %.
    ///
    /// Set to 0 to disable adaptive throttling (budget stays at initial value).
    ///
    /// Default: 10 ms.
    #[arg(long = "autovacuum-target-p95-ms", default_value_t = 10)]
    pub autovacuum_target_p95_ms: u64,

    /// Interval between autovacuum ticks in seconds.
    ///
    /// Each enabled shard runs one tick per interval. The tick examines all
    /// reclamation passes and runs those whose conditions are met, within
    /// the current time budget.
    ///
    /// Default: 30 s. Reduce to 1 s for testing.
    #[arg(long = "autovacuum-interval-secs", default_value_t = 30)]
    pub autovacuum_interval_secs: u64,

    // ── P7: Graph segment auto-merge ──────────────────────────────────────
    /// Maximum number of immutable CSR segments per graph before the autovacuum
    /// daemon triggers a merge pass (Pass E).
    ///
    /// When `immutable.len() > graph_merge_max_segments`, all immutable segments
    /// are merged into one via Rabbit Order compaction.
    ///
    /// Default: 8.
    #[arg(long = "graph-merge-max-segments", default_value_t = 8)]
    pub graph_merge_max_segments: usize,

    /// Dead-edge fraction threshold that triggers a graph segment merge.
    ///
    /// When `dead_edges / total_edges > graph_dead_edge_trigger` across all
    /// immutable segments for a graph, a merge is triggered even if the segment
    /// count is below `--graph-merge-max-segments`.
    ///
    /// Range: 0.0 (disabled) – 1.0. Default: 0.20 (20 % dead edges).
    #[arg(long = "graph-dead-edge-trigger", default_value_t = 0.20)]
    pub graph_dead_edge_trigger: f64,

    // ── MA4: Weighted compaction scheduling ───────────────────────────────
    /// Minimum seconds before a stale entity is forced to be scheduled by the
    /// autovacuum daemon regardless of its compaction weight (anti-starvation cap).
    ///
    /// Prevents hot indexes from starving cold ones indefinitely.
    /// Set to 0 to disable anti-starvation (pure priority-queue ordering).
    ///
    /// Default: 300 s (5 minutes).
    #[arg(long = "autovacuum-starvation-cap-secs", default_value_t = 300)]
    pub autovacuum_starvation_cap_secs: u64,
}

impl ServerConfig {
    /// Returns true when disk offload is enabled.
    pub fn disk_offload_enabled(&self) -> bool {
        self.disk_offload == "enable"
    }

    /// Returns true when WAL Full Page Images are enabled.
    pub fn wal_fpi_enabled(&self) -> bool {
        self.wal_fpi == "enable"
    }

    /// Returns true when vector codes pages should be mlocked.
    pub fn vec_codes_mlock_enabled(&self) -> bool {
        self.vec_codes_mlock == "enable"
    }

    /// Returns the warm-segment mmap budget in bytes.
    ///
    /// Parses `--vec-warm-mmap-budget` using [`Self::parse_size`].
    /// Returns `0` if the string is `"0"` or unparseable (disabling enforcement).
    /// Default is 2 GiB.
    pub fn vec_warm_mmap_budget_bytes(&self) -> u64 {
        Self::parse_size(&self.vec_warm_mmap_budget).unwrap_or(2 * 1024 * 1024 * 1024)
    }

    /// Returns the effective disk offload directory, falling back to --dir.
    pub fn effective_disk_offload_dir(&self) -> PathBuf {
        self.disk_offload_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(&self.dir))
    }

    /// Parse a size string like "256mb" or "1gb" into bytes.
    ///
    /// Supported suffixes: `kb`, `mb`, `gb` (case-insensitive). Plain integers
    /// are treated as raw byte counts.
    pub fn parse_size(s: &str) -> Option<u64> {
        let s = s.trim().to_lowercase();
        if let Some(num) = s.strip_suffix("gb") {
            num.trim()
                .parse::<u64>()
                .ok()
                .and_then(|n| n.checked_mul(1024 * 1024 * 1024))
        } else if let Some(num) = s.strip_suffix("mb") {
            num.trim()
                .parse::<u64>()
                .ok()
                .and_then(|n| n.checked_mul(1024 * 1024))
        } else if let Some(num) = s.strip_suffix("kb") {
            num.trim()
                .parse::<u64>()
                .ok()
                .and_then(|n| n.checked_mul(1024))
        } else {
            s.parse::<u64>().ok()
        }
    }

    /// Returns --max-wal-size parsed to bytes (default 256 MiB).
    pub fn max_wal_size_bytes(&self) -> u64 {
        Self::parse_size(&self.max_wal_size).unwrap_or(256 * 1024 * 1024)
    }

    /// Returns --wal-segment-size parsed to bytes (default 16 MiB).
    pub fn wal_segment_size_bytes(&self) -> u64 {
        Self::parse_size(&self.wal_segment_size).unwrap_or(16 * 1024 * 1024)
    }

    /// Returns --pagecache-size parsed to bytes, defaulting to 25% of maxmemory.
    pub fn pagecache_size_bytes(&self, maxmemory: u64) -> u64 {
        self.pagecache_size
            .as_ref()
            .and_then(|s| Self::parse_size(s))
            .unwrap_or(maxmemory / 4)
    }

    /// Create a RuntimeConfig from this server config, copying mutable parameters.
    pub fn to_runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig {
            maxmemory: self.maxmemory,
            maxmemory_policy: self.maxmemory_policy.clone(),
            maxmemory_samples: self.maxmemory_samples,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            save: self.save.clone(),
            appendonly: self.appendonly.clone(),
            appendfsync: self.appendfsync.clone(),
            aclfile: self.aclfile.clone(),
            dir: self.dir.clone(),
            requirepass: self.requirepass.clone(),
            protected_mode: self.protected_mode.clone(),
            acllog_max_len: self.acllog_max_len,
            client_pause_deadline_ms: 0,
            client_pause_write_only: false,
            lazyfree_threshold: 64,
            maxclients: self.maxclients,
            timeout: self.timeout,
            tcp_keepalive: self.tcp_keepalive,
        }
    }
}

/// Runtime-mutable configuration parameters.
///
/// These can be changed via CONFIG SET without server restart.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Maximum memory in bytes (0 = unlimited).
    pub maxmemory: usize,
    /// Eviction policy name (e.g., "noeviction", "allkeys-lru").
    pub maxmemory_policy: String,
    /// Number of random keys to sample for eviction.
    pub maxmemory_samples: usize,
    /// LFU logarithmic factor for probabilistic counter increment.
    pub lfu_log_factor: u8,
    /// LFU decay time in minutes.
    pub lfu_decay_time: u64,
    /// Save rules (copied from ServerConfig, mutable via CONFIG SET but no live effect).
    pub save: Option<String>,
    /// Appendonly setting (mutable via CONFIG SET but no live effect).
    pub appendonly: String,
    /// Appendfsync setting (mutable via CONFIG SET but no live effect).
    pub appendfsync: String,
    /// ACL file path (mutable via CONFIG SET).
    pub aclfile: Option<String>,
    /// Data directory for persistence files (snapshot, WAL).
    pub dir: String,
    /// Require clients to authenticate with this password (mutable via CONFIG SET).
    pub requirepass: Option<String>,
    /// Protected mode setting (mutable via CONFIG SET).
    pub protected_mode: String,
    /// Maximum number of entries in the ACL log (mutable via CONFIG SET).
    pub acllog_max_len: usize,
    /// CLIENT PAUSE deadline (epoch ms). 0 = not paused.
    /// Set by CLIENT PAUSE, cleared by CLIENT UNPAUSE or expiry.
    pub client_pause_deadline_ms: u64,
    /// CLIENT PAUSE mode: false = ALL (pause all), true = WRITE (pause writes only).
    pub client_pause_write_only: bool,
    /// Lazyfree threshold: collections with more elements than this are freed async.
    pub lazyfree_threshold: usize,
    /// Maximum number of simultaneous client connections (0 = unlimited).
    pub maxclients: usize,
    /// Close connections idle for more than N seconds (0 = disabled).
    pub timeout: u64,
    /// TCP keepalive interval in seconds (0 = disabled).
    pub tcp_keepalive: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            maxmemory: 0,
            maxmemory_policy: "noeviction".to_string(),
            maxmemory_samples: 5,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            save: None,
            appendonly: "yes".to_string(),
            appendfsync: "everysec".to_string(),
            aclfile: None,
            dir: ".".to_string(),
            requirepass: None,
            protected_mode: "yes".to_string(),
            acllog_max_len: 128,
            client_pause_deadline_ms: 0,
            client_pause_write_only: false,
            lazyfree_threshold: 64,
            maxclients: 10000,
            timeout: 0,
            tcp_keepalive: 300,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.bind, "127.0.0.1");
        assert_eq!(config.port, 6379);
        assert_eq!(config.databases, 16);
    }

    #[test]
    fn test_custom_port() {
        let config = ServerConfig::parse_from(["moon", "--port", "6380"]);
        assert_eq!(config.port, 6380);
    }

    #[test]
    fn test_custom_bind_and_databases() {
        let config = ServerConfig::parse_from(["moon", "--bind", "0.0.0.0", "--databases", "4"]);
        assert_eq!(config.bind, "0.0.0.0");
        assert_eq!(config.databases, 4);
    }

    #[test]
    fn test_requirepass() {
        let config = ServerConfig::parse_from(["moon", "--requirepass", "mysecret"]);
        assert_eq!(config.requirepass, Some("mysecret".to_string()));
    }

    #[test]
    fn test_requirepass_default_none() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.requirepass, None);
    }

    #[test]
    fn test_persistence_defaults() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.appendonly, "yes");
        assert_eq!(config.appendfsync, "everysec");
        assert_eq!(config.save, None);
        assert_eq!(config.dir, ".");
        assert_eq!(config.dbfilename, "dump.rdb");
        assert_eq!(config.appendfilename, "appendonly.aof");
    }

    #[test]
    fn test_persistence_custom_values() {
        let config = ServerConfig::parse_from([
            "moon",
            "--dir",
            "/data",
            "--dbfilename",
            "my.rdb",
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            "--save",
            "3600 1 300 100",
            "--appendfilename",
            "my.aof",
        ]);
        assert_eq!(config.dir, "/data");
        assert_eq!(config.dbfilename, "my.rdb");
        assert_eq!(config.appendonly, "yes");
        assert_eq!(config.appendfsync, "always");
        assert_eq!(config.save, Some("3600 1 300 100".to_string()));
        assert_eq!(config.appendfilename, "my.aof");
    }

    #[test]
    fn test_maxmemory_defaults() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.maxmemory, 0);
        assert_eq!(config.maxmemory_policy, "noeviction");
        assert_eq!(config.maxmemory_samples, 5);
    }

    #[test]
    fn test_maxmemory_custom() {
        let config = ServerConfig::parse_from([
            "moon",
            "--maxmemory",
            "1048576",
            "--maxmemory-policy",
            "allkeys-lru",
            "--maxmemory-samples",
            "10",
        ]);
        assert_eq!(config.maxmemory, 1048576);
        assert_eq!(config.maxmemory_policy, "allkeys-lru");
        assert_eq!(config.maxmemory_samples, 10);
    }

    #[test]
    fn test_to_runtime_config() {
        let config = ServerConfig::parse_from([
            "moon",
            "--maxmemory",
            "1024",
            "--maxmemory-policy",
            "allkeys-lfu",
        ]);
        let rt = config.to_runtime_config();
        assert_eq!(rt.maxmemory, 1024);
        assert_eq!(rt.maxmemory_policy, "allkeys-lfu");
        assert_eq!(rt.maxmemory_samples, 5);
        assert_eq!(rt.lfu_log_factor, 10);
        assert_eq!(rt.lfu_decay_time, 1);
    }

    #[test]
    fn test_runtime_config_default() {
        let rt = RuntimeConfig::default();
        assert_eq!(rt.maxmemory, 0);
        assert_eq!(rt.maxmemory_policy, "noeviction");
        assert_eq!(rt.maxmemory_samples, 5);
    }

    #[test]
    fn test_disk_offload_defaults() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert!(config.disk_offload_enabled());
        assert_eq!(config.disk_offload, "enable");
        assert_eq!(config.disk_offload_dir, None);
        assert!((config.disk_offload_threshold - 0.85).abs() < f64::EPSILON);
        assert_eq!(config.segment_warm_after, 3600);
        assert_eq!(config.checkpoint_timeout, 300);
        assert!((config.checkpoint_completion - 0.9).abs() < f64::EPSILON);
        assert_eq!(config.max_wal_size, "256mb");
        assert!(config.wal_fpi_enabled());
        assert_eq!(config.wal_compression, "lz4");
        assert_eq!(config.wal_segment_size, "16mb");
        assert!(config.vec_codes_mlock_enabled());
        assert_eq!(config.pagecache_size, None);
    }

    #[test]
    fn test_parse_size() {
        assert_eq!(ServerConfig::parse_size("256mb"), Some(268_435_456));
        assert_eq!(ServerConfig::parse_size("1gb"), Some(1_073_741_824));
        assert_eq!(ServerConfig::parse_size("16mb"), Some(16_777_216));
        assert_eq!(ServerConfig::parse_size("1024"), Some(1024));
        assert_eq!(ServerConfig::parse_size("64kb"), Some(65_536));
        assert_eq!(ServerConfig::parse_size("  2 GB  "), Some(2_147_483_648));
        assert_eq!(ServerConfig::parse_size("invalid"), None);
    }

    #[test]
    fn test_config_flag_parsing() {
        let config = ServerConfig::parse_from([
            "moon",
            "--disk-offload",
            "enable",
            "--disk-offload-dir",
            "/mnt/nvme",
            "--disk-offload-threshold",
            "0.75",
            "--segment-warm-after",
            "7200",
            "--pagecache-size",
            "512mb",
            "--checkpoint-timeout",
            "600",
            "--checkpoint-completion",
            "0.8",
            "--max-wal-size",
            "512mb",
            "--wal-fpi",
            "disable",
            "--wal-compression",
            "none",
            "--wal-segment-size",
            "32mb",
            "--vec-codes-mlock",
            "disable",
        ]);
        assert!(config.disk_offload_enabled());
        assert_eq!(
            config.disk_offload_dir,
            Some(std::path::PathBuf::from("/mnt/nvme"))
        );
        assert!((config.disk_offload_threshold - 0.75).abs() < f64::EPSILON);
        assert_eq!(config.segment_warm_after, 7200);
        assert_eq!(config.pagecache_size, Some("512mb".to_string()));
        assert_eq!(config.checkpoint_timeout, 600);
        assert!((config.checkpoint_completion - 0.8).abs() < f64::EPSILON);
        assert_eq!(config.max_wal_size_bytes(), 512 * 1024 * 1024);
        assert!(!config.wal_fpi_enabled());
        assert_eq!(config.wal_compression, "none");
        assert_eq!(config.wal_segment_size_bytes(), 32 * 1024 * 1024);
        assert!(!config.vec_codes_mlock_enabled());
    }

    #[test]
    fn test_effective_disk_offload_dir() {
        // Falls back to --dir when --disk-offload-dir not set
        let config = ServerConfig::parse_from(["moon", "--dir", "/data"]);
        assert_eq!(
            config.effective_disk_offload_dir(),
            std::path::PathBuf::from("/data")
        );

        // Uses explicit --disk-offload-dir when set
        let config =
            ServerConfig::parse_from(["moon", "--dir", "/data", "--disk-offload-dir", "/mnt/nvme"]);
        assert_eq!(
            config.effective_disk_offload_dir(),
            std::path::PathBuf::from("/mnt/nvme")
        );
    }

    #[test]
    fn test_pagecache_size_bytes() {
        // Explicit size
        let config = ServerConfig::parse_from(["moon", "--pagecache-size", "1gb"]);
        assert_eq!(config.pagecache_size_bytes(0), 1_073_741_824);

        // Default: 25% of maxmemory
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.pagecache_size_bytes(4_000_000_000), 1_000_000_000);
    }

    #[test]
    fn test_shards_default() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.shards, 0); // auto-detect
    }

    #[test]
    fn test_shards_custom() {
        let config = ServerConfig::parse_from(["moon", "--shards", "4"]);
        assert_eq!(config.shards, 4);
    }

    #[test]
    fn test_aclfile_default_none() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.aclfile, None);
    }

    #[test]
    fn test_aclfile_custom() {
        let config = ServerConfig::parse_from(["moon", "--aclfile", "/tmp/test.acl"]);
        assert_eq!(config.aclfile, Some("/tmp/test.acl".to_string()));
    }

    #[test]
    fn test_to_runtime_config_aclfile() {
        let config = ServerConfig::parse_from(["moon", "--aclfile", "/data/users.acl"]);
        let rt = config.to_runtime_config();
        assert_eq!(rt.aclfile, Some("/data/users.acl".to_string()));
    }

    #[test]
    fn test_cold_tier_defaults() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.segment_cold_after, 86_400);
        assert!((config.segment_cold_min_qps - 0.1).abs() < f64::EPSILON);
        assert_eq!(config.vec_diskann_beam_width, 8);
        assert_eq!(config.vec_diskann_cache_levels, 3);
    }

    #[test]
    fn test_cold_tier_custom() {
        let config = ServerConfig::parse_from([
            "moon",
            "--segment-cold-after",
            "3600",
            "--segment-cold-min-qps",
            "0.5",
            "--vec-diskann-beam-width",
            "16",
            "--vec-diskann-cache-levels",
            "5",
        ]);
        assert_eq!(config.segment_cold_after, 3600);
        assert!((config.segment_cold_min_qps - 0.5).abs() < f64::EPSILON);
        assert_eq!(config.vec_diskann_beam_width, 16);
        assert_eq!(config.vec_diskann_cache_levels, 5);
    }
}

use std::path::PathBuf;

use clap::{CommandFactory, FromArgMatches, Parser};

// NOTE: `src/config.rs` (this file) co-exists with `src/config/conf_file.rs`
// using the Rust "file + directory-sibling" layout.  We add `conf_file` as a
// submodule here rather than converting config.rs → config/mod.rs because the
// plan calls for a minimal diff (an *addition*, not a split).
pub mod conf_file;

/// Server configuration parsed from command-line arguments.
#[derive(Parser, Debug, Clone, Default)]
#[command(
    name = "moon",
    about = "A Redis-compatible server",
    args_override_self = true
)]
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

    /// TCP listen backlog per listening socket. With per-shard SO_REUSEPORT
    /// accept the kernel splits the SYN load across shards, so this is a
    /// per-socket bound (Redis single-socket default is 511; Moon's
    /// historical hardcoded value was 1024). Raise for connection-storm
    /// workloads alongside `ulimit -n` and net.core.somaxconn.
    #[arg(long = "tcp-backlog", default_value_t = 1024)]
    pub tcp_backlog: i32,

    /// Replication backlog capacity in bytes per shard (raw bytes, like
    /// --maxmemory). Bounds how far a disconnected replica can fall behind
    /// and still partial-resync; older stream bytes are evicted and force a
    /// full resync on reconnect. Matches Redis `repl-backlog-size`
    /// (default 1 MiB).
    #[arg(long = "repl-backlog-size", default_value_t = 1024 * 1024)]
    pub repl_backlog_size: usize,

    /// Require clients to authenticate with this password
    #[arg(long)]
    pub requirepass: Option<String>,

    /// Enable append-only file persistence (yes/no)
    #[arg(long, default_value = "yes")]
    pub appendonly: String,

    /// [DEPRECATED — will be removed in v0.2] This flag is now a no-op.
    ///
    /// Historically, `--shards >= 2 + --appendonly yes` lost ~50 % of
    /// writes on SIGKILL (verified 2026-05-26, HEAD `6e49050`). The flag
    /// was an escape hatch to acknowledge the risk.
    ///
    /// As of PR #129 the per-shard AOF architecture is fully crash-safe
    /// (CRASH-01-LITE: 200/200 SIGKILL recovery). The startup refusal gate
    /// has been lifted. Passing this flag now only emits a `[DEPRECATED]`
    /// warning at startup and has no other effect. Remove it from your
    /// launch command or systemd unit. See
    /// `docs/runbooks/multi-shard-aof-rewrite.md`.
    #[arg(long, default_value_t = false)]
    pub unsafe_multishard_aof: bool,

    /// [EXPERIMENTAL] Enable per-shard BGREWRITEAOF (compaction) for the
    /// `--shards >= 2 + --appendonly yes` PerShard layout.
    ///
    /// Default `false`: BGREWRITEAOF stays gated in PerShard mode (the
    /// shipped, crash-safe "append-only, no in-place compaction" behavior).
    /// When `true`, BGREWRITEAOF fans the rewrite out to every per-shard
    /// writer (synchronized seq bump + single manifest commit). This path is
    /// validated by `tests/crash_matrix_per_shard_bgrewriteaof.rs` and is
    /// opt-in until the both-runtime crash matrix is green by default.
    ///
    /// The flag only takes effect alongside `per_shard_aof_active`; it is a
    /// no-op for `--shards 1` (TopLevel rewrite already works) and for
    /// `--appendonly no`.
    #[arg(long, default_value_t = false)]
    pub experimental_per_shard_rewrite: bool,

    /// AOF fsync policy (always/everysec/no)
    #[arg(long, default_value = "everysec")]
    pub appendfsync: String,

    /// Max time (ms) a write may block awaiting the `appendfsync=always`
    /// fsync ack before the write is failed instead of parking the
    /// connection forever. Design-for-failure bound: a stalled disk must
    /// not turn write connections into zombies holding their buffers.
    /// 0 disables the bound (legacy unbounded await). Default 2000ms.
    #[arg(long = "aof-fsync-timeout-ms", default_value_t = 2000)]
    pub aof_fsync_timeout_ms: u64,

    /// RDB auto-save rules (e.g., "3600 1 300 100")
    #[arg(long)]
    pub save: Option<String>,

    /// Directory for persistence files. Empty (the default) auto-resolves
    /// at startup: the current directory when it already holds moon
    /// persistence data (pre-v0.2.0 default layout), otherwise the
    /// platform user-data directory (Linux: `$XDG_DATA_HOME/moon` or
    /// `~/.local/share/moon`; macOS: `~/Library/Application Support/moon`;
    /// Windows: `%LOCALAPPDATA%\moon`), created on first run. Pass an
    /// explicit path (e.g. `--dir .`) to opt out of auto-resolution.
    #[arg(long, default_value = "")]
    pub dir: String,

    /// RDB snapshot filename
    #[arg(long, default_value = "dump.rdb")]
    pub dbfilename: String,

    /// AOF filename
    #[arg(long, default_value = "appendonly.aof")]
    pub appendfilename: String,

    /// Maximum memory in bytes.
    ///
    /// G1 memory guardrail (design-for-failure against unbounded keyspace
    /// growth → OOM kill):
    /// - **flag omitted** (`None`) → Moon auto-caps at ~80% of the detected
    ///   memory limit (cgroup-aware on Linux, host RAM otherwise) and switches
    ///   a `noeviction` policy to `allkeys-lru`, logging a startup notice.
    /// - `--maxmemory 0` → explicitly UNLIMITED (Redis-compatible escape hatch).
    /// - `--maxmemory N` → exact cap in bytes (honored verbatim).
    ///
    /// Resolved once at startup by [`ServerConfig::apply_memory_guardrail`];
    /// downstream code reads the resolved `usize` from `RuntimeConfig`
    /// (`0 = unlimited`).
    #[arg(long)]
    pub maxmemory: Option<usize>,

    /// Eviction policy when maxmemory is reached
    #[arg(long, default_value = "noeviction")]
    pub maxmemory_policy: String,

    /// Number of random keys to sample for eviction
    #[arg(long, default_value_t = 5)]
    pub maxmemory_samples: usize,

    /// Per-logical-db memory quota (repeatable): `--db-maxmemory <db>:<bytes>`.
    ///
    /// Independent of and additive to `--maxmemory` (the whole-instance cap):
    /// a db-scoped quota bounds ONE `SELECT`-able db slot, while `--maxmemory`
    /// still bounds the whole instance. Enforced with the SAME eviction
    /// policy as `--maxmemory-policy` (mirrors global maxmemory behavior —
    /// `noeviction` rejects the write with a `MOONERR db maxmemory exceeded`
    /// error, an evicting policy sheds this db's own keys first). Malformed
    /// entries (bad format, non-numeric db/bytes) or an out-of-range db index
    /// (`>= --databases`) are logged as a startup warning and ignored — a
    /// typo in this flag must never refuse to start the server.
    /// Zero-cost when omitted: the fast path is a single empty-`Vec` check.
    #[arg(
        long = "db-maxmemory",
        value_parser = clap::value_parser!(String),
        action = clap::ArgAction::Append,
    )]
    pub db_maxmemory: Vec<String>,

    /// Number of shards (0 = auto-detect from CPU count).
    ///
    /// Defaults to 1: single-shard gives the best per-op latency for
    /// low-concurrency, non-pipelined workloads (a cross-shard hop costs
    /// ~10µs) and a deterministic persistence layout across hosts. Pin an
    /// explicit count (e.g. 4) for 8+ concurrent connections or pipelined
    /// traffic — measured 1.3-1.9x Redis at 8-64 conns on 4 shards — or
    /// pass `--shards 0` to auto-detect on a dedicated host. See
    /// docs/guides/tuning.md.
    #[arg(long, default_value_t = 1)]
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

    /// I/O driver for the monoio runtime. "auto" lets FusionDriver pick
    /// (io_uring on Linux when available, else epoll/kqueue); "epoll" forces
    /// the legacy poller. Measured on GCE ARM (c4a Axion, 2026-07): epoll is
    /// 2-4% faster than io_uring across ALL pipeline depths for KV workloads,
    /// while other platforms (e.g. OrbStack aarch64) favor io_uring — bench
    /// per platform. Equivalent env kill-switch: MOON_NO_URING=1.
    #[arg(long = "io-driver", default_value = "auto", value_parser = ["auto", "epoll"])]
    pub io_driver: String,

    /// Busy-poll the shard event loop for N microseconds before sleeping
    /// (0 = disabled). Implies `--io-driver epoll`. Poll-mode park: the shard
    /// thread spins on readiness (zero-timeout polls) instead of blocking, so
    /// the scheduler sleep+wake disappears from the request path. Best for
    /// low-pipeline request/response workloads on dedicated cores; costs up to
    /// ~N µs of CPU per idle park. Measured (GCE c1 GET p=1, 2026-07):
    /// ARM c4a 0.95→1.21× vs Redis, x86 c3 1.06→1.66×. monoio runtime only.
    ///
    /// Deploy-safe by default (O3): each shard thread watches its own
    /// involuntary-preemption rate (Linux) and self-gates the spin while its
    /// core is shared with other runnable threads — the shared-core
    /// regression that previously made this flag pinned-cores-only judgment
    /// no longer applies (one >25-preempts/s window gates the spin; 5
    /// consecutive quiet windows re-enable it). `MOON_SPIN_ADAPTIVE=0`
    /// restores unconditional spinning (bench A/B knob).
    #[arg(long = "io-busy-poll-us", default_value_t = 0)]
    pub io_busy_poll_us: u64,

    /// Apply a named tuning preset. A profile only fills flags the operator
    /// left at their default — any flag passed explicitly on the CLI (or via
    /// `moon.conf`) always wins over the preset's value.
    ///
    /// Currently supported: `standalone` — single dedicated instance, tuned
    /// for the "beat Redis at p=1" latency path (`--shards 1`,
    /// `--io-busy-poll-us 40`, implying `--io-driver epoll`). On jemalloc
    /// builds, passing `--profile standalone` on the **command line** also
    /// drops the arena cap to `2` (single-shard allocator footprint); that
    /// happens in the pre-clap allocator re-spawn, not here, so a conf-file
    /// `profile standalone` sets only the three flags above.
    /// Safe on any host as of the O3 contention governor: `--io-busy-poll-us`
    /// now auto-gates the busy-poll on shared/oversubscribed cores (per-shard
    /// involuntary-preemption sampling), so the preset no longer requires
    /// pinned CPUs — it simply delivers its full win on dedicated cores and
    /// costs at most ~one window of spin on a contended one. See
    /// docs/guides/tuning.md#profiles. Unknown profile names are a startup
    /// error.
    #[arg(long)]
    pub profile: Option<String>,

    /// FT.SEARCH intra-query worker threads: per-segment HNSW searches of one
    /// KNN query fan out across this pool, cutting single-query latency on
    /// multi-segment indexes (the pool also serves concurrent queries).
    /// Default 0 = disabled (serial per-segment loop; results are identical
    /// either way). Opt in on boxes with spare PHYSICAL cores — a good size is
    /// cores minus shards, capped at 8 (see search_pool::auto_workers).
    /// Measured (20k×384d clustered SQ8, single conn, R@10=1.0): macOS
    /// 10-core 473→2,321 QPS (4.9×); GCE c3-standard-8 (4 physical cores)
    /// REGRESSES at default ef — each segment pays the full resolved ef, so a
    /// pooled N-segment query does ~N× the CPU work for its latency win.
    #[arg(long = "ft-search-workers")]
    pub ft_search_workers: Option<usize>,

    // ── MoonStore v2: Disk Offload ──────────────────────────────────
    /// Enable disk offload (tiered storage: RAM -> mmap -> NVMe)
    #[arg(long = "disk-offload", default_value = "enable")]
    pub disk_offload: String,

    /// Directory for disk offload files (default: same as --dir)
    #[arg(long = "disk-offload-dir")]
    pub disk_offload_dir: Option<PathBuf>,

    /// RAM pressure threshold to trigger disk offload (0.0-1.0).
    /// Acted upon every 100ms eviction tick (`persistence_tick::run_eviction_tick`):
    /// once a shard's published KV memory crosses `threshold * per-shard-budget`,
    /// `handle_memory_pressure` runs the ordered cascade (PageCache clock-sweep
    /// eviction -> force HOT->WARM segment demotion -> proactive KV spill via the
    /// background `SpillThread` -> NoEviction warning) instead of waiting for the
    /// plain LRU/LFU eviction edge.
    #[arg(long = "disk-offload-threshold", default_value_t = 0.85)]
    pub disk_offload_threshold: f64,

    /// Seconds before sealed segments transition to warm tier (age-based).
    /// A segment also qualifies once it hits `--engine-offload-idle-secs` of
    /// no search traffic, whichever threshold is reached first — see that
    /// flag's doc comment.
    #[arg(long = "segment-warm-after", default_value_t = 3600)]
    pub segment_warm_after: u64,

    /// Seconds of no FT.SEARCH traffic before an immutable (HOT) vector
    /// segment becomes eligible for HOT->WARM demotion, regardless of its
    /// age. `0` disables this idle criterion — only `--segment-warm-after`
    /// (age since compaction) then applies, matching pre-existing behavior.
    /// Complements `--segment-warm-after`: a segment demotes as soon as
    /// EITHER threshold is met, so a segment that is old but still being
    /// queried heavily is not unloaded just because `--segment-warm-after`
    /// disagrees with treating hot data as cold — set this lower than
    /// `--segment-warm-after` to make idleness the effective trigger.
    /// Default 3600s (1 hour) is conservative; `0` = disabled.
    #[arg(long = "engine-offload-idle-secs", default_value_t = 3600)]
    pub engine_offload_idle_secs: u64,

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

    /// KV command logging into the per-shard WAL: auto | on | off.
    ///
    /// `auto` (default): KV records are skipped while the AOF is the crash-
    /// recovery authority (`--appendonly yes`) and no CDC subscriber is
    /// attached — startup recovery wipes WAL-replayed state and replays the
    /// AOF, so the WAL copy is pure write amplification (~2× disk writes at
    /// shards>=2). Logging resumes dynamically when a CDC subscriber attaches.
    /// `on`: always log (pre-0.6 behavior; required for point-in-time
    /// recovery or full CDC history alongside `--appendonly yes`).
    /// `off`: never log KV records (FPI/checkpoint/feature records still
    /// written). With `--appendonly no` this leaves NO KV durability log.
    #[arg(long = "wal-kv-log", default_value = "auto", value_parser = ["auto", "on", "off"])]
    pub wal_kv_log: String,

    // ── MoonStore v2: Vector Warm Tier ──────────────────────────────
    /// mlock vector codes pages into RAM
    #[arg(long = "vec-codes-mlock", default_value = "enable")]
    pub vec_codes_mlock: String,

    /// Maximum resident bytes allowed across all warm-tier vector segments
    /// on this INSTANCE (e.g. "2gb", "512mb", "0"), divided evenly across
    /// shards — matching `--maxmemory` semantics (A5, tiering-v2 D3). When a
    /// shard's share is exceeded the budget enforcer demotes LRU warm
    /// segments to reloadable COLD stubs; they reload from disk on next
    /// access. Set to "0" to disable.
    ///
    /// Default: "2gb". Tune down for cgroup-constrained containers.
    #[arg(long = "vec-warm-mmap-budget", default_value = "2gb")]
    pub vec_warm_mmap_budget: String,

    // ── DEPRECATED: DiskANN cold-tier (removed) ─────────────────────
    // The experimental "COLD-ann" tier (`SegmentList.cold`, `DiskAnnSegment`
    // — approximate serve-from-disk via PQ codes in RAM + Vamana graph on
    // NVMe, gated off by default behind `MOON_VEC_COLD_TIER=1`) was DELETED:
    // it never left experimental status, had no restart recovery story (no
    // PQ-codebook reload, no cold-segment delete) and ADC-only recall. The
    // M3-exit review decided delete-over-finish. See CHANGELOG.
    //
    // The four flags below are kept as parseable, inert no-ops (rather than
    // hard-removed) so existing `moon.conf` files / scripts that still pass
    // them do not fail to start — the DEFAULT COLD valve remains
    // `SegmentList.unloaded` (`UnloadedSegment`, exact, ~0 RAM,
    // reload-on-touch, always available) plus warm-tier byte-budget LRU
    // eviction (`--vec-warm-mmap-budget`), neither of which this section
    // ever gated. A future release may remove these flags entirely (clap
    // will then error on unknown args) — see `warn_deprecated_cold_tier_flags`
    // in `main.rs`, which logs once at startup if any of these four were set
    // away from their default.
    /// DEPRECATED, no-op: was consumed only by the removed DiskANN cold
    /// tier's transition timer.
    #[arg(long = "segment-cold-after", default_value_t = 86_400)]
    pub segment_cold_after: u64,

    /// DEPRECATED, no-op: was reserved for the removed DiskANN cold tier's
    /// EWMA frequency classifier (never implemented).
    #[arg(long = "segment-cold-min-qps", default_value_t = 0.1)]
    pub segment_cold_min_qps: f64,

    // ── Allocator tuning (PERF-10) ────────────────────────────────
    /// Override jemalloc narenas cap (default 8). Range 1-256.
    /// Reduces VSZ on multi-core hosts (4*ncpus default -> 8). No-op for
    /// non-jemalloc builds. Implemented via MALLOC_CONF env-var injection
    /// at process start (re-spawn before jemalloc init).
    /// CLI-only: a value in moon.conf cannot reach jemalloc (its config is
    /// read at process start, before the conf file is parsed) and triggers a
    /// startup warning instead of taking effect.
    #[arg(long = "memory-arenas-cap", value_name = "N", default_value_t = 8, value_parser = clap::value_parser!(u32).range(1..=256))]
    pub memory_arenas_cap: u32,

    /// Opt jemalloc into `thp:always` for the value heap (distinct from the
    /// always-on `metadata_thp:auto`, which only huge-pages jemalloc's own
    /// bookkeeping). Real-PMU measured on GCE (tmp/GCE-PMU-RESULTS.md,
    /// 2026-07-18): GET +24.4% (ARM Axion) / +12.1% (x86 Emerald Rapids),
    /// dTLB MPKI -35% / -98.4%, RSS +4.2% on both. No-op for non-jemalloc
    /// builds (warns). Implemented via the same MALLOC_CONF env-var re-spawn
    /// as `--memory-arenas-cap` -- passing both together produces exactly one
    /// re-exec with one composed conf string. **Linux-only in effect**:
    /// jemalloc has no THP support outside Linux, and with the baked-in
    /// `abort_conf:true` it does not silently ignore `thp:always` on other
    /// platforms -- it aborts at init (verified experimentally on macOS,
    /// 2026-07-18). This flag warns and no-ops on non-Linux jemalloc builds
    /// rather than risk that abort; `--memory-arenas-cap` still applies
    /// normally if both flags are given together. Permanently opt-in: the
    /// RSS-drift soak (2026-07-19) disqualified a default flip -- after
    /// mixed-size churn goes idle, khugepaged collapses the heap into 2M
    /// pages and re-materializes jemalloc's purged 4K holes, settling RSS
    /// ~+31% above the non-THP baseline at identical used_memory (bounded,
    /// but permanent -- jemalloc never re-purges those ranges), which also
    /// erodes maxmemory eviction headroom. Best for uniform-value-size
    /// fleets with RSS headroom; avoid under mixed-size churn.
    /// CLI-only: a value in moon.conf cannot reach jemalloc (its config is
    /// read at process start, before the conf file is parsed) and triggers a
    /// startup warning instead of taking effect.
    #[arg(long = "memory-thp", default_value_t = false)]
    pub memory_thp: bool,

    /// DEPRECATED, no-op: DiskANN beam width for the removed cold-tier
    /// disk-resident search path.
    #[arg(long = "vec-diskann-beam-width", default_value_t = 8)]
    pub vec_diskann_beam_width: u32,

    /// DEPRECATED, no-op: HNSW upper-level cache depth for the removed
    /// DiskANN cold-tier hybrid search path.
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
    /// Default: 60 (1 minute). Recommended range: 60–3600.
    ///
    /// Lowered from 300 → 60: at 300s the sweep never fired within a typical
    /// benchmark window, which both let cold orphans accumulate on disk for up
    /// to 5 minutes AND masked a batch-file shared-deletion data-loss bug (fixed
    /// by the per-file-liveness refcount in ColdIndex). 60s reclaims promptly;
    /// the sweep's per-file unlinks run off the hot path so a shorter interval
    /// keeps each batch small rather than churning under the shard lock.
    #[arg(long = "cold-orphan-sweep-interval-secs", default_value_t = 60)]
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
    ///
    /// Also settable via `MOON_DISK_FREE_MIN_PCT` (CLI flag wins). The env
    /// form exists for test harnesses and CI runners whose root filesystem
    /// legitimately sits below the 5% default (GitHub windows-latest images
    /// do): one exported var relaxes the guard for every server a test tree
    /// spawns, without threading the flag through each spawn helper.
    #[arg(long = "disk-free-min-pct", env = "MOON_DISK_FREE_MIN_PCT", default_value_t = 5, value_parser = clap::value_parser!(u8).range(0..=95))]
    pub disk_free_min_pct: u8,

    // ── Wave 3: proactive RSS memory watchdog ("mem-full guard") ───────────
    /// Pause writes when process RSS crosses this percentage of the detected
    /// system/cgroup memory limit.
    ///
    /// This is the memory analogue of `--disk-free-min-pct` (MA12): it fires
    /// on the ACTUAL RSS vs the detected limit (`detect_memory_limit_bytes`),
    /// not on the configured `--maxmemory` (which can be an unconfigured 0).
    /// The direction is INVERTED vs the disk guard: high RSS is bad, so
    /// writes pause once RSS% >= `mem_full_pct` and resume only once
    /// RSS% <= `mem_full_pct - 5` (hysteresis, prevents flapping).
    ///
    /// Read-only commands are never blocked. Like the diskfull guard,
    /// DEL/UNLINK/EXPIRE/FLUSHALL are write-flagged and are blocked too while
    /// paused — the same accepted trade-off as MA12; no allowlist.
    ///
    /// Set to 0 to disable the monitor entirely.
    #[arg(long = "mem-full-pct", default_value_t = 95, value_parser = clap::value_parser!(u8).range(0..=100))]
    pub mem_full_pct: u8,

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

    /// Default graph traversal timeout in milliseconds (0 = unlimited).
    ///
    /// Bounds how long a single Cypher traversal (variable-length expand,
    /// shortestPath, GRAPH.TRAVERSE hop loop) may run — and therefore how long
    /// it may hold a graph snapshot. A per-query `TIMEOUT <ms>` argument on
    /// GRAPH.QUERY / GRAPH.RO_QUERY / GRAPH.PROFILE / GRAPH.TRAVERSE overrides
    /// this for one query (RedisGraph parity).
    ///
    /// Default: 30_000 (30 s).
    #[arg(long = "graph-timeout-ms", default_value_t = 30_000)]
    pub graph_timeout_ms: u64,

    /// Cypher result-cache capacity: maximum cached query results per graph.
    ///
    /// The cache serves repeated read-only Cypher queries without
    /// re-executing them (doorkeeper-gated admission; invalidated on any
    /// write to the graph). Raising it helps dashboards that cycle through
    /// many distinct read queries; each graph gets its own cache.
    ///
    /// Default: 256 entries.
    #[arg(long = "graph-result-cache-entries", default_value_t = 256)]
    pub graph_result_cache_entries: usize,

    /// Cypher result-cache memory ceiling in bytes (per graph).
    ///
    /// Entries are evicted LRU when either this byte budget or
    /// `--graph-result-cache-entries` is exceeded.
    ///
    /// Default: 4194304 (4 MiB).
    #[arg(long = "graph-result-cache-bytes", default_value_t = 4_194_304)]
    pub graph_result_cache_bytes: usize,

    // ── Vector search-tuning defaults (FT.CREATE / FT.CONFIG initial values) ──
    /// Default HNSW search beam width (EF_RUNTIME) for indexes created
    /// without an explicit `EF_RUNTIME` in FT.CREATE. `0` keeps the per-query
    /// auto heuristic (max(k*20, 200) with dimension boost). Range: 10-4096.
    ///
    /// Per-index `FT.CONFIG SET <idx> EF_RUNTIME` still overrides this at
    /// runtime; the flag only sets the starting value for NEW indexes.
    #[arg(long = "vector-ef-runtime", default_value_t = 0)]
    pub vector_ef_runtime: u32,

    /// Default exact-rerank depth multiplier (RERANK_MULT) for new vector
    /// indexes: the top `mult × k` beam candidates are re-scored with true
    /// f16 sidecar distances before top-k truncation. Range: 1-64.
    ///
    /// Per-index `FT.CONFIG SET <idx> RERANK_MULT` still overrides this at
    /// runtime; the flag only sets the starting value for NEW indexes.
    #[arg(long = "vector-rerank-mult", default_value_t = 4)]
    pub vector_rerank_mult: u32,

    /// Default EXACT_BEAM state for new vector indexes: when set, the HNSW
    /// beam navigates with exact f16 sidecar distances instead of quantized
    /// ADC estimates — recall becomes graph-limited at a QPS cost that grows
    /// with dimension.
    ///
    /// Per-index `FT.CONFIG SET <idx> EXACT_BEAM OFF` still overrides this
    /// at runtime; the flag only sets the starting value for NEW indexes.
    #[arg(long = "vector-exact-beam", default_value_t = false)]
    pub vector_exact_beam: bool,

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

    // ── AOF v1→v2 migration ────────────────────────────────────────────
    /// Source directory containing a legacy single-file AOF (`appendonly.aof`
    /// or TopLevel manifest layout).  When this flag is set the server runs
    /// the migration tool, writes the v2 PerShard layout to `--migrate-aof-to`,
    /// and exits.  Do NOT combine with normal server startup flags.
    ///
    /// Example:
    ///   moon --migrate-aof-from /old/dir --migrate-aof-to /new/dir \
    ///        --migrate-aof-shards 4
    #[arg(long = "migrate-aof-from", value_name = "PATH")]
    pub migrate_aof_from: Option<PathBuf>,

    /// Destination directory for the v2 PerShard AOF layout produced by
    /// `--migrate-aof-from`.  The directory is created if absent; it must be
    /// empty (or non-existent) to prevent accidental overwrites.
    #[arg(long = "migrate-aof-to", value_name = "PATH")]
    pub migrate_aof_to: Option<PathBuf>,

    /// Number of target shards for the migration.  Must match the `--shards`
    /// value you will use when starting the server on the migrated data.
    /// Defaults to 0 (invalid — must be set when `--migrate-aof-from` is used).
    #[arg(long = "migrate-aof-shards", default_value_t = 0)]
    pub migrate_aof_shards: u16,
}

/// Filesystem markers that identify an existing moon persistence layout.
/// Used to keep pre-v0.2.0 deployments (default `dir = "."`) pointed at
/// their data after the default moved to the platform user-data directory.
const MOON_DATA_MARKERS: [&str; 4] = ["appendonlydir", "shard-0", "dump.rdb", "replication.state"];

/// True when `base` already contains moon persistence data.
fn dir_has_moon_data(base: &std::path::Path) -> bool {
    MOON_DATA_MARKERS.iter().any(|m| base.join(m).exists())
}

/// Platform user-data directory for moon, derived from the given
/// environment values (parameterized for testability — the runtime
/// wrapper is [`default_data_dir`]).
///
/// Linux/other unix: `$XDG_DATA_HOME/moon`, else `~/.local/share/moon`.
/// macOS: `~/Library/Application Support/moon`.
/// Windows: `%LOCALAPPDATA%\moon`.
fn data_dir_from_env(
    home: Option<&std::ffi::OsStr>,
    xdg_data_home: Option<&std::ffi::OsStr>,
    local_app_data: Option<&std::ffi::OsStr>,
) -> Option<std::path::PathBuf> {
    use std::path::Path;
    fn non_empty(v: Option<&std::ffi::OsStr>) -> Option<&std::ffi::OsStr> {
        v.filter(|s| !s.is_empty())
    }
    if cfg!(target_os = "windows") {
        non_empty(local_app_data).map(|d| Path::new(d).join("moon"))
    } else if cfg!(target_os = "macos") {
        non_empty(home).map(|h| {
            Path::new(h)
                .join("Library")
                .join("Application Support")
                .join("moon")
        })
    } else {
        // Linux + other unix: XDG, then conventional fallback.
        if let Some(x) = non_empty(xdg_data_home) {
            return Some(Path::new(x).join("moon"));
        }
        non_empty(home).map(|h| Path::new(h).join(".local").join("share").join("moon"))
    }
}

/// Runtime wrapper over [`data_dir_from_env`] reading the real environment.
fn default_data_dir() -> Option<std::path::PathBuf> {
    data_dir_from_env(
        std::env::var_os("HOME").as_deref(),
        std::env::var_os("XDG_DATA_HOME").as_deref(),
        std::env::var_os("LOCALAPPDATA").as_deref(),
    )
}

/// Resolved `--wal-kv-log` mode (see the flag docs on `ServerConfig::wal_kv_log`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalKvLogMode {
    /// Skip WAL KV records while the AOF is the recovery authority and no
    /// CDC subscriber is attached; log otherwise. The per-drain decision is
    /// made by the shard event loop (it owns the CDC registry).
    Auto,
    /// Always log KV records to the WAL (pre-0.6 behavior).
    On,
    /// Never log KV records to the WAL.
    Off,
}

/// Outcome of persistence-directory auto-resolution (pure decision —
/// IO happens in [`ServerConfig::resolve_dir`]).
#[derive(Debug, PartialEq, Eq)]
enum DirResolution {
    /// User passed `--dir` (or conf `dir`) — never touched.
    Explicit,
    /// Existing moon data found in the current directory (pre-v0.2.0
    /// default layout) — keep using it so upgrades don't silently boot
    /// with an empty keyspace away from their data.
    LegacyCwd,
    /// Fresh start — use the platform user-data directory.
    UserData(std::path::PathBuf),
    /// No usable user-data directory (no HOME/LOCALAPPDATA) — fall back
    /// to the current directory.
    FallbackCwd,
}

fn decide_dir(
    dir: &str,
    cwd_has_data: bool,
    data_dir: Option<std::path::PathBuf>,
) -> DirResolution {
    if !dir.is_empty() {
        return DirResolution::Explicit;
    }
    if cwd_has_data {
        return DirResolution::LegacyCwd;
    }
    match data_dir {
        Some(d) => DirResolution::UserData(d),
        None => DirResolution::FallbackCwd,
    }
}

/// Maximum allowed `--databases` count (WS5a round 2, adversarial review
/// finding 2). Vector/text index `db_index` tagging downcasts the
/// connection's SELECTed db to `u8` at ~20 call sites
/// (`conn.selected_db as u8`); `--databases 300` + `SELECT 256` would pass
/// SELECT's `index < db_count` range check, then silently wrap
/// `256 as u8 == 0`, aliasing db 256's indexes onto db 0's. `u8::MAX + 1`
/// keeps the full 0..=255 range addressable without widening `db_index` to
/// `u16` across every `IndexMeta`/`TextIndex`/SPSC payload site.
pub const MAX_DATABASES: usize = 256;

impl ServerConfig {
    /// Warn once at startup if an operator set any of the four deprecated
    /// DiskANN cold-tier flags (`--segment-cold-after`,
    /// `--segment-cold-min-qps`, `--vec-diskann-beam-width`,
    /// `--vec-diskann-cache-levels`) away from their historical default —
    /// the tier they configured was deleted (see CHANGELOG) and these flags
    /// are now pure no-ops kept only so existing `moon.conf` files / launch
    /// scripts do not fail to start.
    pub fn warn_deprecated_cold_tier_flags(&self) {
        let mut touched: Vec<&str> = Vec::new();
        if self.segment_cold_after != 86_400 {
            touched.push("--segment-cold-after");
        }
        if (self.segment_cold_min_qps - 0.1).abs() > f64::EPSILON {
            touched.push("--segment-cold-min-qps");
        }
        if self.vec_diskann_beam_width != 8 {
            touched.push("--vec-diskann-beam-width");
        }
        if self.vec_diskann_cache_levels != 3 {
            touched.push("--vec-diskann-cache-levels");
        }
        if !touched.is_empty() {
            tracing::warn!(
                "{} set but ignored: the experimental DiskANN cold tier was removed \
                 (delete-over-finish decision — incomplete restart recovery, ADC-only \
                 recall). WARM eviction (--vec-warm-mmap-budget) and the default \
                 COLD-stub reload-on-touch valve are unaffected.",
                touched.join(", ")
            );
        }
    }

    /// True when `--disk-offload enable` is configured but neither AOF
    /// (`--appendonly yes`) nor RDB (`--save`) durability is on (GCP
    /// benchmark finding, 2026-07-10).
    ///
    /// The durable-spill eviction path (`evict_batch_durable`) needs
    /// a `ShardManifest`, which today is only threaded through the
    /// tick-driven memory-pressure cascade
    /// (`shard::persistence_tick::handle_memory_pressure`) — itself gated on
    /// `persistence_dir`, which `main.rs` only constructs when
    /// `appendonly == "yes" || save.is_some()` (see `main.rs`'s
    /// `persistence_dir` binding). The inline per-connection write-path
    /// eviction gate has no manifest access and, under this combination,
    /// cannot durably spill — cold data is NOT tiered to disk regardless of
    /// policy. What happens to the *write* itself is now policy-aware
    /// (`src/storage/eviction.rs`, the `manifest is None` branch of
    /// `try_evict_if_needed_async_spill_with_total_budget`): an evicting
    /// policy (`allkeys-*`/`volatile-*`) still honors `--maxmemory` by
    /// DROPPING victims outright (Redis cache semantics, no tiering, no
    /// crash-durability claim needed since nothing needs to survive a
    /// restart); `noeviction` — and any evicting policy once no eligible
    /// victim remains (e.g. `volatile-*` with no TTL keys left) — rejects
    /// writes with OOM at the cap. This
    /// predicate stays orthogonal to `maxmemory_policy` — spill IS still
    /// inert either way — and exists only to make that degradation loud.
    pub fn disk_offload_spill_inert(&self) -> bool {
        self.disk_offload_enabled() && self.appendonly != "yes" && self.save.is_none()
    }

    /// Warn once at startup when [`disk_offload_spill_inert`] holds — see
    /// that method's docs for the full mechanism. Behavior is unchanged;
    /// this only surfaces the pre-existing silent degradation.
    ///
    /// [`disk_offload_spill_inert`]: Self::disk_offload_spill_inert
    pub fn warn_disk_offload_without_durability(&self) {
        if self.disk_offload_spill_inert() {
            tracing::warn!(
                "--disk-offload is enabled but persistence is off (appendonly=no and no \
                 --save). The disk-offload cold-spill tier requires a durability backstop \
                 to function: without one, cold data is NOT spilled to disk. Evicting \
                 policies (allkeys-*/volatile-*) fall back to DROPPING eligible \
                 victims with no tiering; noeviction — and any evicting policy once \
                 no eligible victim remains (e.g. volatile-* with no TTL keys) — \
                 rejects writes with OOM at the cap. Enable \
                 --appendonly yes or --save to activate durable spill."
            );
        }
    }

    /// Validate `--databases` fits the `u8` db_index tag used by vector/text
    /// index scoping (WS5a round 2). Returns an error message (not a
    /// `Frame`/`anyhow::Error` — kept plain so both `main.rs`'s early-boot
    /// path and unit tests can format/assert on it directly) when the
    /// configured count exceeds `MAX_DATABASES`.
    /// Validate the vector/graph tuning-default flags (startup error, not a
    /// silent clamp — a typo in a fleet-wide default must be loud). Mirrors
    /// the per-index FT.CONFIG ranges exactly so a value accepted here is
    /// accepted there and vice versa.
    pub fn validate_tuning_defaults(&self) -> Result<(), String> {
        if self.vector_ef_runtime != 0 && !(10..=4096).contains(&self.vector_ef_runtime) {
            return Err(format!(
                "--vector-ef-runtime {} out of range (10-4096, or 0 for the auto heuristic)",
                self.vector_ef_runtime
            ));
        }
        if !(1..=64).contains(&self.vector_rerank_mult) {
            return Err(format!(
                "--vector-rerank-mult {} out of range (1-64)",
                self.vector_rerank_mult
            ));
        }
        if self.graph_result_cache_entries == 0 {
            return Err(
                "--graph-result-cache-entries must be >= 1 (the cache cannot be sized to zero                  entries; it is admission-gated, not disableable by size)"
                    .to_string(),
            );
        }
        if self.graph_result_cache_bytes < 4096 {
            return Err(format!(
                "--graph-result-cache-bytes {} too small (minimum 4096)",
                self.graph_result_cache_bytes
            ));
        }
        Ok(())
    }

    pub fn validate_databases_bound(&self) -> Result<(), String> {
        if self.databases > MAX_DATABASES {
            return Err(format!(
                "--databases {} exceeds the maximum of {MAX_DATABASES} (vector/text index db \
                 scoping uses a u8 tag; higher values alias distinct dbs onto the same index)",
                self.databases
            ));
        }
        Ok(())
    }

    /// Resolve the persistence directory when `--dir` was not given.
    ///
    /// Must run once at startup (after conf-file merge, before any
    /// persistence component reads `self.dir`). See `decide_dir` for the
    /// resolution order. Creation failure of the user-data directory
    /// degrades to the current directory with a warning rather than
    /// refusing to start.
    pub fn resolve_dir(&mut self) {
        match decide_dir(
            &self.dir,
            dir_has_moon_data(std::path::Path::new(".")),
            default_data_dir(),
        ) {
            DirResolution::Explicit => {}
            DirResolution::LegacyCwd => {
                tracing::warn!(
                    "--dir not set and existing moon data found in the current \
                     directory; continuing to use '.'. Pass --dir explicitly to \
                     silence this warning."
                );
                self.dir = ".".to_owned();
            }
            DirResolution::UserData(d) => match std::fs::create_dir_all(&d) {
                Ok(()) => {
                    tracing::info!(dir = %d.display(), "--dir not set; using platform user-data directory");
                    self.dir = d.to_string_lossy().into_owned();
                }
                Err(e) => {
                    tracing::warn!(
                        dir = %d.display(), error = %e,
                        "cannot create user-data directory; falling back to current directory"
                    );
                    self.dir = ".".to_owned();
                }
            },
            DirResolution::FallbackCwd => {
                tracing::warn!(
                    "--dir not set and no HOME/XDG_DATA_HOME/LOCALAPPDATA in the \
                     environment; falling back to the current directory"
                );
                self.dir = ".".to_owned();
            }
        }
    }

    /// Returns true when disk offload is enabled.
    pub fn disk_offload_enabled(&self) -> bool {
        self.disk_offload == "enable"
    }

    /// Resolve `--wal-kv-log` into its mode. The CLI rejects unknown values
    /// at parse time (`value_parser`, fail-fast on typos — a silent `Auto`
    /// fallback could unexpectedly disable WAL KV history needed for
    /// PITR/CDC); the `_ => Auto` arm below only covers programmatic
    /// construction in tests.
    pub fn wal_kv_log_mode(&self) -> WalKvLogMode {
        match self.wal_kv_log.as_str() {
            "on" => WalKvLogMode::On,
            "off" => WalKvLogMode::Off,
            _ => WalKvLogMode::Auto,
        }
    }

    /// Returns true when WAL Full Page Images are enabled.
    pub fn wal_fpi_enabled(&self) -> bool {
        self.wal_fpi == "enable"
    }

    /// Returns true when the per-shard AOF layout is active.
    ///
    /// Per-shard AOF is selected whenever `--shards >= 2` and
    /// `--appendonly yes`. In this layout each shard owns its own
    /// `appendonlydir/shard-{N}/` directory and a dedicated
    /// `per_shard_aof_writer_task`. Operations that touch the single
    /// consolidated `appendonly.aof` file (e.g. BGREWRITEAOF) are not
    /// supported in this layout until the multi-part AOF rewrite ships.
    #[inline]
    pub fn per_shard_aof_active(&self, num_shards: usize) -> bool {
        num_shards >= 2 && self.appendonly == "yes"
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

    /// Per-shard share of `--vec-warm-mmap-budget` (accounting-spine A5,
    /// tiering-v2 D3): the flag is an INSTANCE-TOTAL cap divided across
    /// shards, matching `maxmemory_per_shard` semantics. Previously each
    /// shard applied the full value — an N-shard instance silently allowed
    /// N× the configured WARM memory. `0` still disables enforcement; a
    /// nonzero total floors at 1 byte per shard (0 would flip semantics to
    /// "unlimited", the unsafe direction). Division floor is fine otherwise:
    /// under-allocating a soft budget is the safe direction.
    pub fn vec_warm_mmap_budget_bytes_per_shard(&self) -> u64 {
        let total = self.vec_warm_mmap_budget_bytes();
        if total == 0 {
            return 0;
        }
        (total / self.shards.max(1) as u64).max(1)
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

    /// Resolve the G1 memory guardrail, mutating `self` in place, and return
    /// the [`GuardrailOutcome`] for the caller to log as a startup notice.
    ///
    /// MUST be called once at startup, after parsing and before
    /// [`Self::to_runtime_config`]. Idempotent in effect: once `maxmemory` is
    /// `Some`, re-calling returns `Explicit` and changes nothing.
    pub fn apply_memory_guardrail(&mut self) -> GuardrailOutcome {
        let detected = detect_memory_limit_bytes();
        let outcome = resolve_memory_guardrail(
            self.maxmemory,
            &self.maxmemory_policy,
            detected,
            MAXMEMORY_GUARDRAIL_PERCENT,
        );
        match &outcome {
            GuardrailOutcome::Applied {
                cap_bytes,
                policy_changed_to,
                ..
            } => {
                self.maxmemory = Some(*cap_bytes);
                if let Some(p) = policy_changed_to {
                    self.maxmemory_policy = p.clone();
                }
            }
            GuardrailOutcome::Explicit(_) => { /* operator set it; honor verbatim */ }
            GuardrailOutcome::Skipped => {
                // Omitted but no limit detectable → leave UNLIMITED but make it
                // concrete so downstream sees the `0` sentinel, not `None`.
                self.maxmemory = Some(0);
            }
        }
        outcome
    }

    /// Parse argv into a `ServerConfig`, also returning the `clap::ArgMatches`
    /// used to produce it.
    ///
    /// Callers need the `ArgMatches` alongside the parsed struct so
    /// [`Self::apply_profile`] can tell explicitly-passed flags apart from
    /// ones that only hold their `default_value`/`default_value_t` — a plain
    /// `ServerConfig::parse_from` throws that provenance away. Exits the
    /// process on parse error (or `--help`/`--version`), matching
    /// `clap::Parser::parse_from`'s behavior exactly.
    ///
    /// Uses [`FromArgMatches::from_arg_matches`] (the non-consuming variant,
    /// which clones internally) rather than `from_arg_matches_mut`: the
    /// `_mut` builder removes consumed entries from `ArgMatches` as it goes
    /// (an optimization to avoid extra clones), which would silently erase
    /// `value_source` provenance for every field before `apply_profile` ever
    /// gets to inspect it.
    pub fn parse_from_with_matches<I, T>(args: I) -> (Self, clap::ArgMatches)
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let matches = match Self::command().try_get_matches_from(args) {
            Ok(m) => m,
            Err(e) => e.exit(),
        };
        let config = match Self::from_arg_matches(&matches) {
            Ok(c) => c,
            Err(e) => e.exit(),
        };
        (config, matches)
    }

    /// Apply a named `--profile` tuning preset, mutating `self` in place.
    ///
    /// Fill-only semantics: a preset field is written ONLY when the
    /// corresponding flag's [`clap::ValueSource`] is not `CommandLine` (i.e.
    /// the operator did not pass it explicitly — on the real CLI or via the
    /// `moon.conf`-merged argv, both of which land in the same `ArgMatches`
    /// fed to clap). An explicitly-passed flag always wins over the profile.
    ///
    /// `matches` MUST be the `ArgMatches` produced by parsing the SAME argv
    /// that produced `self` (see [`Self::parse_from_with_matches`]).
    ///
    /// MUST be called once at startup, after parsing and before any code
    /// reads `shards` / `io_busy_poll_us` / `io_driver` (shard spawn,
    /// `to_runtime_config`, etc.).
    pub fn apply_profile(
        &mut self,
        matches: &clap::ArgMatches,
    ) -> Result<ProfileOutcome, ProfileError> {
        let Some(name) = self.profile.clone() else {
            return Ok(ProfileOutcome::None);
        };
        if name != "standalone" {
            return Err(ProfileError::Unknown(name));
        }

        let mut fields: Vec<&'static str> = Vec::new();
        if !Self::flag_was_explicit(matches, "shards") {
            self.shards = 1;
            fields.push("--shards=1");
        }
        if !Self::flag_was_explicit(matches, "io_busy_poll_us") {
            self.io_busy_poll_us = 40;
            fields.push("--io-busy-poll-us=40");
        }
        if !Self::flag_was_explicit(matches, "io_driver") {
            self.io_driver = "epoll".to_string();
            fields.push("--io-driver=epoll (implied by io-busy-poll-us)");
        }
        // NOTE: the standalone profile ALSO drops the jemalloc arena cap to 2
        // on jemalloc builds, but that is applied entirely in the pre-clap
        // allocator re-spawn (`malloc_respawn::scan_argv` sees `--profile
        // standalone` in the raw CLI argv and re-execs with narenas:2, long
        // before this method runs). It is deliberately NOT mirrored into
        // `self.memory_arenas_cap` here: this method cannot tell a CLI-sourced
        // `--profile` from a conf-file-sourced one (clap reports both as
        // `CommandLine` on the merged argv), but the re-spawn — which only sees
        // raw pre-merge argv — only honors the CLI form. Mirroring it here
        // would falsely claim "arena cap applied" for the conf-file form, where
        // jemalloc actually stays at 8 (and would even trip
        // `warn_if_conf_only_overrides`). The re-spawn is the single source of
        // truth for the arena cap; we stay out of it.
        Ok(ProfileOutcome::Applied {
            profile: name,
            fields,
        })
    }

    /// True when `id`'s value came from the parsed argv rather than a
    /// `default_value`/`default_value_t`/env fallback.
    fn flag_was_explicit(matches: &clap::ArgMatches, id: &str) -> bool {
        matches!(
            matches.value_source(id),
            Some(clap::parser::ValueSource::CommandLine)
        )
    }

    /// Create a RuntimeConfig from this server config, copying mutable parameters.
    ///
    /// `maxmemory` resolves `None`/`Some(0)` → `0` (the downstream "unlimited"
    /// sentinel). Call [`Self::apply_memory_guardrail`] BEFORE this if the G1
    /// auto-guardrail should populate an unset `--maxmemory`.
    pub fn to_runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig {
            maxmemory: self.maxmemory.unwrap_or(0),
            maxmemory_policy: self.maxmemory_policy.clone(),
            maxmemory_samples: self.maxmemory_samples,
            db_maxmemory: parse_db_maxmemory_entries(&self.db_maxmemory, self.databases),
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
            // Default to single-shard (no division). The server overwrites this
            // on the shared RuntimeConfig with the resolved shard count once it
            // is known (main.rs / embedded.rs), so the per-shard eviction budget
            // bounds aggregate RSS. Tests that call this directly run 1 shard.
            num_shards: 1,
        }
    }
}

/// Parse `--db-maxmemory <db>:<bytes>` entries into a dense `Vec<u64>` of
/// length `num_databases` (index = db number, `0` = unlimited).
///
/// Malformed entries (no `:`, non-numeric halves) or an out-of-range db
/// index (`>= num_databases`) are logged via `tracing::warn!` and skipped —
/// a typo in this repeatable flag must never refuse to start the server
/// (same fail-open philosophy as the parser-defensiveness rule for wire
/// protocol input). Later entries for the same db index win (clap
/// `ArgAction::Append` preserves argv order), matching `CONFIG SET`'s
/// last-wins semantics for the same key.
pub fn parse_db_maxmemory_entries(raw: &[String], num_databases: usize) -> Vec<u64> {
    let mut out = vec![0u64; num_databases];
    for entry in raw {
        match parse_one_db_maxmemory_entry(entry) {
            Ok((idx, bytes)) if idx < num_databases => out[idx] = bytes,
            Ok((idx, _)) => tracing::warn!(
                entry = %entry,
                idx,
                num_databases,
                "--db-maxmemory: db index out of range (>= --databases); ignoring entry"
            ),
            Err(reason) => tracing::warn!(
                entry = %entry,
                reason,
                "--db-maxmemory: ignoring malformed entry"
            ),
        }
    }
    out
}

/// Parse a single `<db>:<bytes>` token, shared by [`parse_db_maxmemory_entries`]
/// (batch, startup CLI) and `CONFIG SET db-maxmemory <db>:<bytes>` (single,
/// runtime). Returns `Err(reason)` — never panics — on malformed input; the
/// db-index range check (`>= --databases`) is the caller's responsibility
/// since only the caller knows `num_databases`.
pub fn parse_one_db_maxmemory_entry(entry: &str) -> Result<(usize, u64), &'static str> {
    let Some((idx_str, bytes_str)) = entry.split_once(':') else {
        return Err("expected '<db>:<bytes>' format");
    };
    let idx = idx_str
        .trim()
        .parse::<usize>()
        .map_err(|_| "non-numeric db index")?;
    let bytes = bytes_str
        .trim()
        .parse::<u64>()
        .map_err(|_| "non-numeric byte count")?;
    Ok((idx, bytes))
}

/// Validate `--db-maxmemory` entries strictly, for startup fail-fast.
///
/// `parse_db_maxmemory_entries` (used by [`ServerConfig::to_runtime_config`])
/// is deliberately fail-OPEN: a malformed entry there is logged and skipped
/// so a stray typo can never crash a server that's already running (mirrors
/// the wire-protocol parser-defensiveness rule). But `--db-maxmemory` itself
/// is trusted OPERATOR config supplied at process launch, not untrusted wire
/// input — silently ignoring a typo there means the operator believes a
/// quota is protecting a db when it is not, which is a worse failure mode
/// than refusing to start. `CONFIG SET db-maxmemory` already fails loud
/// (returns `Frame::Error`) for the identical malformed/out-of-range cases;
/// this closes the same gap for the CLI form. Call this BEFORE
/// `to_runtime_config()` at both startup entry points (`main.rs`,
/// `server/embedded.rs`) and exit non-zero on `Err` — matches this file's
/// existing "REFUSING TO START: ..." + `std::process::exit(2)` convention
/// for other trusted-config validation failures (see e.g. the AOF manifest
/// / shard-count guards in `main.rs`).
///
/// Returns `Err(message)` describing every invalid entry found (not just the
/// first) so an operator with several typos in one invocation sees all of
/// them in a single failed start, not one-at-a-time.
pub fn validate_db_maxmemory_cli(raw: &[String], num_databases: usize) -> Result<(), String> {
    let mut problems: Vec<String> = Vec::new();
    for entry in raw {
        match parse_one_db_maxmemory_entry(entry) {
            Ok((idx, _)) if idx >= num_databases => {
                problems.push(format!(
                    "'{entry}': db index {idx} is out of range (--databases is {num_databases})"
                ));
            }
            Ok(_) => {}
            Err(reason) => problems.push(format!("'{entry}': {reason}")),
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "invalid --db-maxmemory entr{plural}: {joined} \
             (expected '<db>:<bytes>' with db < --databases)",
            plural = if problems.len() == 1 { "y" } else { "ies" },
            joined = problems.join("; "),
        ))
    }
}

/// Fraction (percent) of the detected memory limit used as the G1 auto
/// guardrail cap when `--maxmemory` is omitted. 80% leaves headroom for
/// allocator fragmentation, page cache, and non-keyspace overhead.
pub const MAXMEMORY_GUARDRAIL_PERCENT: u64 = 80;

/// Result of resolving the G1 memory guardrail — drives the startup notice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardrailOutcome {
    /// Operator set `--maxmemory` explicitly (including `0` = unlimited).
    /// The value is honored verbatim; no guardrail applied.
    Explicit(usize),
    /// `--maxmemory` was omitted and a memory limit was detected: Moon
    /// auto-capped at `cap_bytes` (`~PERCENT%` of `detected_limit_bytes`).
    /// `policy_changed_to` is `Some` when a `noeviction` policy was switched
    /// to an evicting one so the cap actually sheds memory instead of OOM-ing.
    Applied {
        cap_bytes: usize,
        detected_limit_bytes: usize,
        policy_changed_to: Option<String>,
    },
    /// `--maxmemory` was omitted but no memory limit could be detected (e.g.
    /// non-Linux dev host, or `/proc`/`/sys` unreadable). Left UNLIMITED — the
    /// caller warns the operator to set `--maxmemory` explicitly.
    Skipped,
}

/// Result of resolving `--profile` — drives the startup transparency log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileOutcome {
    /// No `--profile` flag was given; nothing to do.
    None,
    /// `profile` was applied; `fields` lists exactly what it set (only
    /// fields the operator left unset — see [`ServerConfig::apply_profile`]).
    Applied {
        profile: String,
        fields: Vec<&'static str>,
    },
}

/// `--profile` resolution failure: an unrecognised profile name.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProfileError {
    /// `name` does not match any known preset.
    #[error("unknown --profile '{0}' (supported: standalone)")]
    Unknown(String),
}

/// Parse the `MemTotal:` line of `/proc/meminfo` contents into bytes.
/// Format: `MemTotal:       16384256 kB`. Pure for testability.
///
/// Only compiled where used: the Linux detector and the unit tests (avoids a
/// dead-code warning on non-Linux non-test builds).
#[cfg(any(target_os = "linux", test))]
fn parse_meminfo_memtotal(contents: &str) -> Option<usize> {
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: usize = rest.split_whitespace().next()?.parse().ok()?;
            return kb.checked_mul(1024);
        }
    }
    None
}

/// Parse a cgroup memory-limit file's contents into a byte cap.
///
/// Handles cgroup v2 (`memory.max`: a number or the literal `max`) and v1
/// (`memory.limit_in_bytes`: a number, with a near-`i64::MAX` "no limit"
/// sentinel). Returns `None` for "unlimited". Pure for testability.
#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_mem_max(contents: &str) -> Option<usize> {
    let t = contents.trim();
    if t.is_empty() || t == "max" {
        return None;
    }
    let v: usize = t.parse().ok()?;
    // cgroup v1 uses a huge page-rounded value (~i64::MAX) to mean "no limit".
    if v >= (1usize << 62) {
        return None;
    }
    Some(v)
}

/// Detect the effective memory limit in bytes: the minimum of the cgroup limit
/// (v2 then v1) and host RAM. Linux-only; returns `None` elsewhere or when
/// nothing is readable (the guardrail then fails open with a warning).
#[cfg(target_os = "linux")]
pub(crate) fn detect_memory_limit_bytes() -> Option<usize> {
    let host = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|c| parse_meminfo_memtotal(&c));
    let cgroup = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|c| parse_cgroup_mem_max(&c))
        .or_else(|| {
            std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
                .ok()
                .and_then(|c| parse_cgroup_mem_max(&c))
        });
    match (host, cgroup) {
        (Some(h), Some(c)) => Some(h.min(c)),
        (Some(h), None) => Some(h),
        (None, Some(c)) => Some(c),
        (None, None) => None,
    }
}

/// macOS (first-class target): probe physical RAM via `sysctl -n hw.memsize`.
/// A spawned command instead of `sysctlbyname` FFI keeps this free of new
/// unsafe blocks; it runs once at startup so the fork cost is irrelevant.
/// No container limit concept applies on macOS, so host RAM is the limit.
#[cfg(target_os = "macos")]
pub(crate) fn detect_memory_limit_bytes() -> Option<usize> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    std::str::from_utf8(&out.stdout).ok()?.trim().parse().ok()
}

/// Other platforms: no portable, dependency-free memory-limit probe. The
/// guardrail is skipped (operator sets `--maxmemory` explicitly). Production
/// targets Linux/macOS per the platform policy.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn detect_memory_limit_bytes() -> Option<usize> {
    None
}

/// Pure resolution of the guardrail decision (no I/O) for unit testing.
fn resolve_memory_guardrail(
    maxmemory: Option<usize>,
    policy: &str,
    detected_limit: Option<usize>,
    percent: u64,
) -> GuardrailOutcome {
    if let Some(explicit) = maxmemory {
        return GuardrailOutcome::Explicit(explicit);
    }
    match detected_limit {
        Some(limit) if limit > 0 => {
            // u128 intermediate avoids overflow on large-RAM hosts.
            let cap = ((limit as u128 * percent as u128) / 100) as usize;
            let policy_changed_to = (policy == "noeviction").then(|| "allkeys-lru".to_string());
            GuardrailOutcome::Applied {
                cap_bytes: cap,
                detected_limit_bytes: limit,
                policy_changed_to,
            }
        }
        _ => GuardrailOutcome::Skipped,
    }
}

/// Emit the G1 startup notice for a resolved guardrail outcome. Shared by the
/// binary entry (`main`) and the embedded entry so the message is identical.
pub fn log_memory_guardrail(outcome: GuardrailOutcome) {
    match outcome {
        GuardrailOutcome::Applied {
            cap_bytes,
            detected_limit_bytes,
            policy_changed_to,
        } => {
            let policy_note = policy_changed_to
                .map(|p| format!("; eviction policy set to '{p}'"))
                .unwrap_or_default();
            tracing::warn!(
                "Memory guardrail: --maxmemory not set; auto-capping at {} bytes \
                 (~{}% of detected {} bytes){}. Override with --maxmemory <bytes>, \
                 or --maxmemory 0 for unlimited.",
                cap_bytes,
                MAXMEMORY_GUARDRAIL_PERCENT,
                detected_limit_bytes,
                policy_note
            );
        }
        GuardrailOutcome::Skipped => {
            tracing::warn!(
                "Memory guardrail: --maxmemory not set and no memory limit could be \
                 detected on this platform; running UNLIMITED. Set --maxmemory <bytes> \
                 to bound keyspace growth and avoid OOM termination."
            );
        }
        GuardrailOutcome::Explicit(_) => { /* operator chose; stay silent */ }
    }
}

/// Emit a one-line startup notice when `maxmemory` is split across multiple shards.
///
/// `maxmemory` is a whole-instance cap, but each shard enforces eviction
/// independently against `maxmemory / num_shards` (see
/// [`RuntimeConfig::maxmemory_per_shard`]). Without this notice an operator
/// running e.g. `--maxmemory 8gb --shards 4` would silently get an 8 GB
/// (not 32 GB) effective ceiling and see "surprise" evictions with nothing
/// in the logs explaining why. Only fires when the division actually changes
/// the effective budget (`num_shards > 1` and a finite cap).
pub fn log_maxmemory_sharding(maxmemory: usize, num_shards: usize) {
    if maxmemory == 0 || num_shards <= 1 {
        return;
    }
    let per_shard = maxmemory.div_ceil(num_shards);
    tracing::info!(
        "maxmemory {} bytes is a whole-instance cap; each of {} shards enforces \
         eviction against a per-shard budget of {} bytes (maxmemory / shards). \
         CONFIG GET / INFO continue to report the whole-instance value.",
        maxmemory,
        num_shards,
        per_shard
    );
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
    /// Per-logical-db memory quota in bytes, indexed by db number
    /// (`db_maxmemory[i]`, `0` = unlimited for db `i`). Dense: length equals
    /// `--databases` so `CONFIG SET db-maxmemory <db> <bytes>` can index
    /// directly with a bounds check instead of resizing at runtime. Empty
    /// (or all-zero) is the common case and costs a single slice-index
    /// check on the write path — see [`crate::storage::db_quota`].
    pub db_maxmemory: Vec<u64>,
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
    /// Resolved shard count — used only to derive the per-shard eviction budget.
    ///
    /// `maxmemory` is a whole-instance cap (Redis-compatible: `CONFIG GET` /
    /// INFO report it verbatim). But each shard is shared-nothing and enforces
    /// eviction independently, so without dividing, an N-shard server would
    /// tolerate ~N×`maxmemory` before evicting. The per-shard threshold is
    /// therefore `maxmemory / num_shards` (see [`RuntimeConfig::maxmemory_per_shard`]).
    /// Defaults to `1` (single shard ⇒ no division, preserving prior behavior);
    /// the server overwrites it on the shared instance at startup with the
    /// resolved shard count.
    pub num_shards: usize,
}

impl RuntimeConfig {
    /// Per-shard eviction budget in bytes.
    ///
    /// `maxmemory` is a whole-instance cap. Because each shard enforces eviction
    /// independently (shared-nothing), the effective per-shard threshold is
    /// `maxmemory / num_shards`, so the aggregate across all shards converges on
    /// the configured whole-instance cap instead of overshooting it ~N×.
    ///
    /// Uses `div_ceil` so the summed per-shard budgets never undershoot the cap,
    /// and `max(1)` on the divisor guards against a mis-set `num_shards`. Returns
    /// `0` (unlimited) iff `maxmemory == 0`.
    #[inline]
    #[must_use]
    pub fn maxmemory_per_shard(&self) -> usize {
        if self.maxmemory == 0 {
            return 0;
        }
        self.maxmemory.div_ceil(self.num_shards.max(1))
    }

    /// Per-shard quota for logical db `db_index`, mirroring
    /// [`Self::maxmemory_per_shard`] exactly but keyed by db instead of the
    /// whole instance. Returns `0` (unlimited) when no quota is configured
    /// for this db, or `db_index` is out of range — the fast, allocation-free
    /// path taken on every write when `--db-maxmemory` was never set.
    #[inline]
    #[must_use]
    pub fn db_maxmemory_per_shard(&self, db_index: usize) -> u64 {
        let Some(&limit) = self.db_maxmemory.get(db_index) else {
            return 0;
        };
        if limit == 0 {
            return 0;
        }
        limit.div_ceil(self.num_shards.max(1) as u64)
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            maxmemory: 0,
            maxmemory_policy: "noeviction".to_string(),
            maxmemory_samples: 5,
            db_maxmemory: Vec::new(),
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
            num_shards: 1,
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
        // Single shard by default: best throughput for non-pipelined
        // workloads and a deterministic persistence layout. `--shards 0`
        // remains the explicit auto-detect opt-in.
        assert_eq!(config.shards, 1);
    }

    #[test]
    fn test_shards_zero_is_explicit_auto_detect() {
        let config = ServerConfig::parse_from(["moon", "--shards", "0"]);
        assert_eq!(config.shards, 0);
    }

    // ── Vector/graph tuning-default flags ─────────────────────────────────

    #[test]
    fn test_tuning_default_flags_parse_and_default() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.vector_ef_runtime, 0);
        assert_eq!(config.vector_rerank_mult, 4);
        assert!(!config.vector_exact_beam);
        assert_eq!(config.graph_result_cache_entries, 256);
        assert_eq!(config.graph_result_cache_bytes, 4_194_304);
        assert!(config.validate_tuning_defaults().is_ok());

        let config = ServerConfig::parse_from([
            "moon",
            "--vector-ef-runtime",
            "256",
            "--vector-rerank-mult",
            "16",
            "--vector-exact-beam",
            "--graph-result-cache-entries",
            "1024",
            "--graph-result-cache-bytes",
            "8388608",
        ]);
        assert_eq!(config.vector_ef_runtime, 256);
        assert_eq!(config.vector_rerank_mult, 16);
        assert!(config.vector_exact_beam);
        assert_eq!(config.graph_result_cache_entries, 1024);
        assert_eq!(config.graph_result_cache_bytes, 8_388_608);
        assert!(config.validate_tuning_defaults().is_ok());
    }

    #[test]
    fn test_tuning_default_flags_out_of_range_rejected() {
        // Same ranges as FT.CONFIG: ef 10-4096 (or 0), mult 1-64.
        for args in [
            vec!["moon", "--vector-ef-runtime", "5"],
            vec!["moon", "--vector-ef-runtime", "5000"],
            vec!["moon", "--vector-rerank-mult", "0"],
            vec!["moon", "--vector-rerank-mult", "65"],
            vec!["moon", "--graph-result-cache-entries", "0"],
            vec!["moon", "--graph-result-cache-bytes", "100"],
        ] {
            let config = ServerConfig::parse_from(args.clone());
            assert!(
                config.validate_tuning_defaults().is_err(),
                "expected startup rejection for {args:?}"
            );
        }
    }

    // ── WS5a round 2 (adversarial review finding 2): --databases bound ────

    #[test]
    fn test_validate_databases_bound_default_ok() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert!(config.validate_databases_bound().is_ok());
    }

    #[test]
    fn test_validate_databases_bound_at_max_ok() {
        let config = ServerConfig::parse_from(["moon", "--databases", "256"]);
        assert!(
            config.validate_databases_bound().is_ok(),
            "256 databases (u8::MAX + 1) must be the accepted ceiling"
        );
    }

    #[test]
    fn test_validate_databases_bound_over_max_rejected() {
        let config = ServerConfig::parse_from(["moon", "--databases", "257"]);
        let err = config
            .validate_databases_bound()
            .expect_err("257 databases must be rejected");
        assert!(
            err.contains("257") && err.contains("256"),
            "error must name both the offending value and the ceiling: {err}"
        );
    }

    #[test]
    fn test_validate_databases_bound_way_over_max_rejected() {
        // The exact scenario from the review: --databases 300 + SELECT 256
        // would silently wrap `256 as u8 == 0`, aliasing db 256's vector/text
        // indexes onto db 0's. Must be rejected at config-validation time,
        // not discovered later as a cross-db data leak.
        let config = ServerConfig::parse_from(["moon", "--databases", "300"]);
        assert!(config.validate_databases_bound().is_err());
    }

    /// `MOON_DISK_FREE_MIN_PCT` env override for `--disk-free-min-pct`.
    ///
    /// Test harnesses (and CI runners whose root disk legitimately sits
    /// below the 5% default — GitHub's windows-latest images do) need a way
    /// to relax the diskfull guard for a whole tree of spawned servers
    /// without threading a flag through every spawn helper. The explicit
    /// CLI flag must still win over the env var (clap semantics).
    ///
    /// SAFETY of env mutation in a test: no other config test reads this
    /// var, and `set_var`/`remove_var` happen strictly around the parses.
    #[test]
    fn test_disk_free_min_pct_env_override() {
        // SAFETY: single-threaded mutation scoped to this test; no other
        // test in this binary reads MOON_DISK_FREE_MIN_PCT.
        unsafe { std::env::set_var("MOON_DISK_FREE_MIN_PCT", "0") };
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(
            config.disk_free_min_pct, 0,
            "env var must override the default"
        );
        // Explicit CLI flag beats the env var.
        let config = ServerConfig::parse_from(["moon", "--disk-free-min-pct", "7"]);
        assert_eq!(config.disk_free_min_pct, 7, "CLI flag must beat env var");
        // SAFETY: see above.
        unsafe { std::env::remove_var("MOON_DISK_FREE_MIN_PCT") };
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.disk_free_min_pct, 5, "default restored without env");
    }

    #[test]
    fn test_io_driver_flag_parses_and_rejects_unknown() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.io_driver, "auto", "auto must stay the default");
        let config = ServerConfig::parse_from(["moon", "--io-driver", "epoll"]);
        assert_eq!(config.io_driver, "epoll");
        // clap-level validation: anything outside auto|epoll is a parse error.
        assert!(ServerConfig::try_parse_from(["moon", "--io-driver", "iouring"]).is_err());
    }

    #[test]
    fn test_ft_search_workers_flag() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(
            config.ft_search_workers, None,
            "default must be auto (None)"
        );
        let config = ServerConfig::parse_from(["moon", "--ft-search-workers", "4"]);
        assert_eq!(config.ft_search_workers, Some(4));
        let config = ServerConfig::parse_from(["moon", "--ft-search-workers", "0"]);
        assert_eq!(
            config.ft_search_workers,
            Some(0),
            "0 must parse (explicit off)"
        );
        assert!(ServerConfig::try_parse_from(["moon", "--ft-search-workers", "x"]).is_err());
    }

    #[test]
    fn test_io_busy_poll_flag_parses_with_zero_default() {
        let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        assert_eq!(config.io_busy_poll_us, 0, "busy-poll must default OFF");
        let config = ServerConfig::parse_from(["moon", "--io-busy-poll-us", "40"]);
        assert_eq!(config.io_busy_poll_us, 40);
        assert!(ServerConfig::try_parse_from(["moon", "--io-busy-poll-us", "x"]).is_err());
    }

    /// `--profile standalone` with no other flags fills the single-instance
    /// preset (shards=1, io-busy-poll-us=40, io-driver=epoll) and reports
    /// exactly what it set.
    #[test]
    fn test_profile_standalone_fills_unset_fields() {
        let (mut config, matches) =
            ServerConfig::parse_from_with_matches(["moon", "--profile", "standalone"]);
        let outcome = config.apply_profile(&matches).expect("known profile");
        assert_eq!(config.shards, 1);
        assert_eq!(config.io_busy_poll_us, 40);
        assert_eq!(config.io_driver, "epoll");
        // The jemalloc arena cap is NOT filled here (it is applied by the
        // pre-clap allocator re-spawn, not this method — see apply_profile's
        // NOTE and malloc_respawn::scan_argv coverage). apply_profile must
        // leave memory_arenas_cap at its parsed default so it never falsely
        // claims a cap that a conf-file-sourced profile can't actually apply.
        assert_eq!(config.memory_arenas_cap, 8);
        match outcome {
            ProfileOutcome::Applied { profile, fields } => {
                assert_eq!(profile, "standalone");
                assert!(!fields.is_empty(), "must report what it set");
                assert!(
                    !fields.iter().any(|f| f.contains("arenas-cap")),
                    "arena cap is owned by the re-spawn, not the profile fields log"
                );
            }
            ProfileOutcome::None => panic!("expected Applied outcome"),
        }
    }

    /// An explicitly-passed flag ALWAYS overrides the profile's value — the
    /// profile only fills fields left at their default.
    #[test]
    fn test_profile_standalone_explicit_flag_wins() {
        let (mut config, matches) = ServerConfig::parse_from_with_matches([
            "moon",
            "--profile",
            "standalone",
            "--shards",
            "4",
            "--io-busy-poll-us",
            "0",
        ]);
        config.apply_profile(&matches).expect("known profile");
        assert_eq!(config.shards, 4, "explicit --shards must beat the profile");
        assert_eq!(
            config.io_busy_poll_us, 0,
            "explicit --io-busy-poll-us must beat the profile"
        );
        // io_driver was NOT passed explicitly, so the profile still fills it.
        assert_eq!(config.io_driver, "epoll");
    }

    /// No `--profile` flag → no-op, `ProfileOutcome::None`.
    #[test]
    fn test_profile_absent_is_noop() {
        let (mut config, matches) = ServerConfig::parse_from_with_matches::<[&str; 0], &str>([]);
        let outcome = config.apply_profile(&matches).expect("no profile is Ok");
        assert_eq!(outcome, ProfileOutcome::None);
        assert_eq!(config.shards, 1, "unmodified default");
        assert_eq!(config.io_busy_poll_us, 0, "unmodified default");
    }

    /// Unknown profile name is a clear startup error, not a silent no-op.
    #[test]
    fn test_profile_unknown_name_errors() {
        let (mut config, matches) =
            ServerConfig::parse_from_with_matches(["moon", "--profile", "bogus"]);
        let err = config
            .apply_profile(&matches)
            .expect_err("unknown profile must error");
        let msg = err.to_string();
        assert!(
            msg.contains("bogus"),
            "error must name the offending profile: {msg}"
        );
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
        assert_eq!(config.dir, ""); // empty = auto-resolve (user data dir / legacy cwd)
        assert_eq!(config.dbfilename, "dump.rdb");
        assert_eq!(config.appendfilename, "appendonly.aof");
    }

    #[test]
    fn test_dir_explicit_dot_is_preserved() {
        // `--dir .` is an explicit opt-out of auto-resolution.
        let mut config = ServerConfig::parse_from(["moon", "--dir", "."]);
        config.resolve_dir();
        assert_eq!(config.dir, ".");
    }

    #[test]
    fn test_dir_explicit_path_is_preserved() {
        let mut config = ServerConfig::parse_from(["moon", "--dir", "/var/lib/moon"]);
        config.resolve_dir();
        assert_eq!(config.dir, "/var/lib/moon");
    }

    #[test]
    fn test_decide_dir_explicit_wins_over_everything() {
        assert_eq!(
            decide_dir("/x", true, Some("/y".into())),
            DirResolution::Explicit
        );
    }

    #[test]
    fn test_decide_dir_legacy_cwd_beats_user_data() {
        // Pre-v0.2.0 deployments keep their data: cwd markers win.
        assert_eq!(
            decide_dir("", true, Some("/y".into())),
            DirResolution::LegacyCwd
        );
    }

    #[test]
    fn test_decide_dir_user_data_on_fresh_start() {
        assert_eq!(
            decide_dir("", false, Some("/y".into())),
            DirResolution::UserData("/y".into())
        );
    }

    #[test]
    fn test_decide_dir_falls_back_to_cwd_without_env() {
        assert_eq!(decide_dir("", false, None), DirResolution::FallbackCwd);
    }

    #[test]
    fn test_dir_has_moon_data_markers() {
        let tmp = std::env::temp_dir().join(format!("moon-marker-test-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).expect("create temp base");
        assert!(!dir_has_moon_data(&tmp), "empty dir must have no markers");
        std::fs::create_dir_all(tmp.join("appendonlydir")).expect("create marker");
        assert!(
            dir_has_moon_data(&tmp),
            "appendonlydir marker must be detected"
        );
        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_data_dir_from_env_platform_layout() {
        use std::ffi::OsStr;
        let got = data_dir_from_env(
            Some(OsStr::new("/home/u")),
            Some(OsStr::new("/xdg/data")),
            Some(OsStr::new(r"C:\Users\u\AppData\Local")),
        )
        .expect("resolves on every supported platform");
        if cfg!(target_os = "windows") {
            assert!(got.ends_with("moon"));
            assert!(got.starts_with(r"C:\Users\u\AppData\Local"));
        } else if cfg!(target_os = "macos") {
            assert_eq!(
                got,
                std::path::Path::new("/home/u/Library/Application Support/moon")
            );
        } else {
            assert_eq!(got, std::path::Path::new("/xdg/data/moon"));
        }
    }

    #[test]
    fn test_data_dir_from_env_unix_ignores_empty_xdg() {
        // Empty XDG_DATA_HOME must fall back to ~/.local/share, never "/moon".
        use std::ffi::OsStr;
        let got = data_dir_from_env(Some(OsStr::new("/home/u")), Some(OsStr::new("")), None);
        if cfg!(all(unix, not(target_os = "macos"))) {
            assert_eq!(
                got.expect("home fallback"),
                std::path::Path::new("/home/u/.local/share/moon")
            );
        }
    }

    #[test]
    fn test_data_dir_from_env_none_without_env() {
        assert_eq!(data_dir_from_env(None, None, None), None);
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
        // G1: omitted flag parses to None (the auto-guardrail sentinel),
        // distinct from an explicit `--maxmemory 0` (= unlimited).
        assert_eq!(config.maxmemory, None);
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
        assert_eq!(config.maxmemory, Some(1048576));
        assert_eq!(config.maxmemory_policy, "allkeys-lru");
        assert_eq!(config.maxmemory_samples, 10);
    }

    #[test]
    fn test_maxmemory_explicit_zero_is_unlimited() {
        // The Redis escape hatch: explicit `--maxmemory 0` must stay unlimited
        // and NOT trigger the guardrail.
        let mut config = ServerConfig::parse_from(["moon", "--maxmemory", "0"]);
        assert_eq!(config.maxmemory, Some(0));
        let outcome = config.apply_memory_guardrail();
        assert_eq!(outcome, GuardrailOutcome::Explicit(0));
        assert_eq!(config.maxmemory, Some(0));
        assert_eq!(config.to_runtime_config().maxmemory, 0);
    }

    // ── G1 pure resolution + parsing ──

    #[test]
    fn guardrail_applies_percent_and_flips_noeviction() {
        let out = resolve_memory_guardrail(None, "noeviction", Some(1000), 80);
        assert_eq!(
            out,
            GuardrailOutcome::Applied {
                cap_bytes: 800,
                detected_limit_bytes: 1000,
                policy_changed_to: Some("allkeys-lru".to_string()),
            }
        );
    }

    #[test]
    fn guardrail_keeps_existing_evicting_policy() {
        // Operator already chose an evicting policy → don't override it.
        let out = resolve_memory_guardrail(None, "allkeys-lfu", Some(2000), 80);
        assert_eq!(
            out,
            GuardrailOutcome::Applied {
                cap_bytes: 1600,
                detected_limit_bytes: 2000,
                policy_changed_to: None,
            }
        );
    }

    #[test]
    fn guardrail_explicit_value_is_honored() {
        assert_eq!(
            resolve_memory_guardrail(Some(4096), "noeviction", Some(1 << 30), 80),
            GuardrailOutcome::Explicit(4096)
        );
    }

    #[test]
    fn guardrail_skipped_when_no_limit_detected() {
        assert_eq!(
            resolve_memory_guardrail(None, "noeviction", None, 80),
            GuardrailOutcome::Skipped
        );
        // Zero/invalid detection also skips (fail open, never cap at 0-by-math).
        assert_eq!(
            resolve_memory_guardrail(None, "noeviction", Some(0), 80),
            GuardrailOutcome::Skipped
        );
    }

    #[test]
    fn guardrail_skipped_outcome_leaves_unlimited() {
        let mut config = ServerConfig::parse_from::<[&str; 0], &str>([]);
        // Force the Skipped branch deterministically by resolving against a
        // None detection (mirrors non-Linux / unreadable /proc).
        let out = resolve_memory_guardrail(config.maxmemory, &config.maxmemory_policy, None, 80);
        assert_eq!(out, GuardrailOutcome::Skipped);
        // apply_* on Skipped must concretize to Some(0) = unlimited.
        if let GuardrailOutcome::Skipped = out {
            config.maxmemory = Some(0);
        }
        assert_eq!(config.to_runtime_config().maxmemory, 0);
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn macos_detects_memory_limit() {
        // macOS is a first-class target; booting UNLIMITED + noeviction by
        // default (guardrail Skipped) was RSS/CPU/OOM review item 4 — the
        // hw.memsize probe must feed the same 80% guardrail as Linux.
        let detected = detect_memory_limit_bytes();
        assert!(
            detected.is_some_and(|b| b > 1 << 30),
            "hw.memsize probe failed: {detected:?}"
        );
    }

    #[test]
    fn parse_meminfo_extracts_memtotal_bytes() {
        let sample = "MemTotal:       16384256 kB\nMemFree:    1000 kB\n";
        assert_eq!(parse_meminfo_memtotal(sample), Some(16_384_256 * 1024));
        assert_eq!(parse_meminfo_memtotal("MemFree: 10 kB\n"), None);
        assert_eq!(parse_meminfo_memtotal(""), None);
    }

    #[test]
    fn parse_cgroup_max_handles_v1_v2_sentinels() {
        assert_eq!(parse_cgroup_mem_max("2147483648\n"), Some(2_147_483_648));
        assert_eq!(parse_cgroup_mem_max("max\n"), None); // v2 unlimited
        assert_eq!(parse_cgroup_mem_max(""), None);
        // v1 near-i64::MAX "no limit" sentinel.
        assert_eq!(parse_cgroup_mem_max("9223372036854771712"), None);
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
        // to_runtime_config defaults num_shards to 1 (no per-shard division until
        // the server sets the resolved count on the shared instance).
        assert_eq!(rt.num_shards, 1);
        assert_eq!(rt.maxmemory_per_shard(), 1024);
    }

    #[test]
    fn maxmemory_per_shard_unlimited_stays_zero() {
        let mut rt = RuntimeConfig {
            maxmemory: 0,
            ..Default::default()
        };
        for n in [1, 2, 4, 16] {
            rt.num_shards = n;
            assert_eq!(
                rt.maxmemory_per_shard(),
                0,
                "unlimited (0) must stay 0 regardless of shard count"
            );
        }
    }

    #[test]
    fn maxmemory_per_shard_single_shard_is_whole_instance() {
        let rt = RuntimeConfig {
            maxmemory: 1_000,
            num_shards: 1,
            ..Default::default()
        };
        assert_eq!(rt.maxmemory_per_shard(), 1_000);
    }

    #[test]
    fn maxmemory_per_shard_divides_by_shard_count() {
        let rt = RuntimeConfig {
            maxmemory: 400,
            num_shards: 4,
            ..Default::default()
        };
        assert_eq!(rt.maxmemory_per_shard(), 100);
    }

    #[test]
    fn maxmemory_per_shard_div_ceil_never_undershoots() {
        // 10 / 3 = 3.33 -> ceil 4 so the summed per-shard budgets (12) >= cap (10).
        let rt = RuntimeConfig {
            maxmemory: 10,
            num_shards: 3,
            ..Default::default()
        };
        assert_eq!(rt.maxmemory_per_shard(), 4);
        assert!(rt.maxmemory_per_shard() * rt.num_shards >= rt.maxmemory);
    }

    #[test]
    fn maxmemory_per_shard_guards_zero_shard_count() {
        // A mis-set num_shards == 0 must not divide-by-zero; treat as 1 shard.
        let rt = RuntimeConfig {
            maxmemory: 500,
            num_shards: 0,
            ..Default::default()
        };
        assert_eq!(rt.maxmemory_per_shard(), 500);
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
    fn disk_offload_spill_inert_true_without_durability_backstop() {
        // disk-offload defaults to enabled; explicitly turning off both AOF
        // and RDB durability with no backstop leaves cold-spill inert.
        let config = ServerConfig::parse_from(["moon", "--appendonly", "no"]);
        assert!(config.disk_offload_spill_inert());
    }

    #[test]
    fn disk_offload_spill_inert_false_when_appendonly_yes() {
        let config = ServerConfig::parse_from(["moon", "--appendonly", "yes"]);
        assert!(!config.disk_offload_spill_inert());
    }

    #[test]
    fn disk_offload_spill_inert_false_when_save_configured() {
        let config = ServerConfig::parse_from(["moon", "--appendonly", "no", "--save", "3600 1"]);
        assert!(!config.disk_offload_spill_inert());
    }

    #[test]
    fn disk_offload_spill_inert_false_when_disk_offload_disabled() {
        let config =
            ServerConfig::parse_from(["moon", "--appendonly", "no", "--disk-offload", "disable"]);
        assert!(!config.disk_offload_spill_inert());
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
        assert_eq!(config.shards, 1); // single shard; `--shards 0` opts into auto-detect
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

    /// Accounting-spine A5 (tiering-v2 D3): `--vec-warm-mmap-budget` is an
    /// INSTANCE-TOTAL cap divided across shards, matching
    /// `maxmemory_per_shard` semantics. Previously each shard applied the
    /// full value — an N-shard instance silently allowed N× the configured
    /// WARM memory. RED until the per-shard accessor exists and divides.
    #[test]
    fn test_vec_warm_budget_divided_per_shard() {
        let mut config = ServerConfig::parse_from::<[&str; 0], &str>([]);

        // 4 shards × default "2gb" ⇒ 512 MiB per shard.
        config.shards = 4;
        assert_eq!(
            config.vec_warm_mmap_budget_bytes_per_shard(),
            512 * 1024 * 1024
        );

        // Single shard: unchanged (full 2 GiB).
        config.shards = 1;
        assert_eq!(
            config.vec_warm_mmap_budget_bytes_per_shard(),
            2 * 1024 * 1024 * 1024
        );

        // "0" disables enforcement regardless of shard count.
        config.vec_warm_mmap_budget = "0".to_string();
        config.shards = 8;
        assert_eq!(config.vec_warm_mmap_budget_bytes_per_shard(), 0);

        // A tiny budget over many shards must floor at 1 byte, NOT 0 —
        // 0 flips semantics to "unlimited", the unsafe direction.
        config.vec_warm_mmap_budget = "100".to_string();
        config.shards = 128;
        assert_eq!(config.vec_warm_mmap_budget_bytes_per_shard(), 1);
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

    /// FIX-W1-4: per_shard_aof_active must be true only when both
    /// num_shards >= 2 AND appendonly=yes are set, and false for every
    /// other combination. This predicate drives the BGREWRITEAOF gate in
    /// main.rs — a false negative silently allows the unsafe rewrite path.
    #[test]
    fn test_per_shard_aof_active_predicate() {
        // Base config: appendonly=yes, shards=2 → active
        let mut config = ServerConfig::parse_from(["moon", "--appendonly", "yes"]);
        assert!(
            config.per_shard_aof_active(2),
            "must be active with shards=2 and appendonly=yes"
        );
        assert!(
            config.per_shard_aof_active(4),
            "must be active with shards=4 and appendonly=yes"
        );

        // shards=1 → not active (single-shard uses TopLevel AOF)
        assert!(
            !config.per_shard_aof_active(1),
            "must be inactive with shards=1 even if appendonly=yes"
        );

        // appendonly=no → not active regardless of shard count
        config.appendonly = "no".to_string();
        assert!(
            !config.per_shard_aof_active(2),
            "must be inactive when appendonly=no"
        );
        assert!(
            !config.per_shard_aof_active(4),
            "must be inactive when appendonly=no with 4 shards"
        );

        // shards=0 (auto-detect placeholder) → not active
        config.appendonly = "yes".to_string();
        assert!(
            !config.per_shard_aof_active(0),
            "must be inactive when num_shards=0"
        );

        // disk_offload has no bearing on this predicate (FIX-W1-4 broadened
        // the gate to not require disk_offload).
        config.disk_offload = "enable".to_string();
        assert!(
            config.per_shard_aof_active(2),
            "must remain active with disk_offload=enable (predicate is orthogonal)"
        );
        config.disk_offload = "disable".to_string();
        assert!(
            config.per_shard_aof_active(2),
            "must remain active with disk_offload=disable"
        );
    }

    // --- db-maxmemory (WS5b) ---

    #[test]
    fn parse_db_maxmemory_entries_basic() {
        let raw = vec!["0:1024".to_string(), "3:2048".to_string()];
        let out = parse_db_maxmemory_entries(&raw, 16);
        assert_eq!(out.len(), 16);
        assert_eq!(out[0], 1024);
        assert_eq!(out[3], 2048);
        assert_eq!(out[1], 0, "unmentioned dbs stay unlimited");
    }

    #[test]
    fn parse_db_maxmemory_entries_empty_is_all_zero() {
        let out = parse_db_maxmemory_entries(&[], 16);
        assert_eq!(out, vec![0u64; 16]);
    }

    #[test]
    fn parse_db_maxmemory_entries_malformed_is_ignored_not_fatal() {
        let raw = vec![
            "not-a-pair".to_string(),
            "abc:1024".to_string(),
            "1:not-bytes".to_string(),
            "2:4096".to_string(), // the one well-formed entry
        ];
        let out = parse_db_maxmemory_entries(&raw, 16);
        assert_eq!(out[2], 4096, "the well-formed entry must still apply");
        assert_eq!(
            out[1], 0,
            "malformed entries must not panic or partially apply"
        );
    }

    #[test]
    fn parse_db_maxmemory_entries_out_of_range_index_is_ignored() {
        let raw = vec!["99:1024".to_string()];
        let out = parse_db_maxmemory_entries(&raw, 16);
        assert_eq!(
            out,
            vec![0u64; 16],
            "out-of-range db index must not resize or panic"
        );
    }

    #[test]
    fn parse_db_maxmemory_entries_later_entry_wins_for_same_db() {
        let raw = vec!["0:1024".to_string(), "0:9999".to_string()];
        let out = parse_db_maxmemory_entries(&raw, 4);
        assert_eq!(out[0], 9999, "last entry for the same db index must win");
    }

    #[test]
    fn parse_one_db_maxmemory_entry_roundtrip() {
        assert_eq!(parse_one_db_maxmemory_entry("7:123456"), Ok((7, 123456)));
        assert!(parse_one_db_maxmemory_entry("no-colon").is_err());
        assert!(parse_one_db_maxmemory_entry("x:123").is_err());
        assert!(parse_one_db_maxmemory_entry("1:notbytes").is_err());
    }

    #[test]
    fn validate_db_maxmemory_cli_accepts_well_formed_in_range_entries() {
        let raw = vec!["0:1024".to_string(), "15:2048".to_string()];
        assert!(validate_db_maxmemory_cli(&raw, 16).is_ok());
    }

    #[test]
    fn validate_db_maxmemory_cli_empty_is_ok() {
        assert!(validate_db_maxmemory_cli(&[], 16).is_ok());
    }

    #[test]
    fn validate_db_maxmemory_cli_rejects_malformed_entry() {
        let raw = vec!["not-a-pair".to_string()];
        let err = validate_db_maxmemory_cli(&raw, 16).unwrap_err();
        assert!(
            err.contains("not-a-pair"),
            "error must name the offending entry: {err}"
        );
    }

    #[test]
    fn validate_db_maxmemory_cli_rejects_out_of_range_index() {
        let raw = vec!["99:1024".to_string()];
        let err = validate_db_maxmemory_cli(&raw, 16).unwrap_err();
        assert!(
            err.contains("99") && err.contains("out of range"),
            "error must name the bad index and explain why: {err}"
        );
    }

    #[test]
    fn validate_db_maxmemory_cli_reports_every_bad_entry_not_just_first() {
        let raw = vec![
            "not-a-pair".to_string(),
            "99:1024".to_string(),
            "0:512".to_string(), // valid — must not appear as a problem
        ];
        let err = validate_db_maxmemory_cli(&raw, 16).unwrap_err();
        assert!(err.contains("not-a-pair"), "missing first bad entry: {err}");
        assert!(err.contains("99"), "missing second bad entry: {err}");
    }

    #[test]
    fn db_maxmemory_per_shard_unconfigured_is_zero() {
        let rt = RuntimeConfig::default();
        assert_eq!(rt.db_maxmemory_per_shard(0), 0);
        assert_eq!(
            rt.db_maxmemory_per_shard(999),
            0,
            "out-of-range db must not panic"
        );
    }

    #[test]
    fn db_maxmemory_per_shard_divides_like_global_maxmemory() {
        let rt = RuntimeConfig {
            db_maxmemory: vec![1000],
            num_shards: 4,
            ..Default::default()
        };
        assert_eq!(rt.db_maxmemory_per_shard(0), 250);
    }

    #[test]
    fn db_maxmemory_per_shard_zero_entry_is_unlimited() {
        let rt = RuntimeConfig {
            db_maxmemory: vec![0, 500],
            num_shards: 1,
            ..Default::default()
        };
        assert_eq!(rt.db_maxmemory_per_shard(0), 0);
        assert_eq!(rt.db_maxmemory_per_shard(1), 500);
    }
}

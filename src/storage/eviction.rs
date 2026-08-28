use std::path::{Path, PathBuf};

use bytes::Bytes;
use rand::RngExt;
use smallvec::SmallVec;
use tracing::warn;

use crate::config::RuntimeConfig;
use crate::persistence::kv_page::{ValueType, entry_flags};
use crate::persistence::manifest::ShardManifest;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::compact_key::CompactKey;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::{RedisValue, lfu_decay};
use crate::storage::tiered::kv_serde;
use crate::storage::tiered::kv_spill;
use crate::storage::tiered::spill_thread::SpillRequest;

/// Maximum number of victim candidates we will sample in a single
/// `find_victim_*` call. This bounds the inline storage of the SmallVec
/// returned by `sample_random_keys` and matches a generous upper bound on
/// the user-tunable `maxmemory-samples` (Redis default 5; we accept up to 16).
const MAX_VICTIM_SAMPLES: usize = 16;

/// Lock-free mirrors of `RuntimeConfig::{maxmemory, maxmemory_per_shard()}`
/// for the inline write fast path, which previously paid a
/// `parking_lot::RwLock` read pair per SET just to discover eviction had
/// nothing to do. `usize::MAX` = not yet published — fail safe: callers must
/// take the config lock and run the full path.
static MAXMEMORY_HINT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(usize::MAX);
static MAXMEMORY_PER_SHARD_HINT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(usize::MAX);

/// Process-global "is `maxmemory` nonzero?" flag, in bytes (0 = unset /
/// unlimited). This answers a single boolean question — distinct from
/// [`MAXMEMORY_HINT`] above, which additionally encodes the exact budget for
/// the inline fast path's skip-eviction arithmetic. Two call sites used to
/// answer this same question with a lock read per drain cycle
/// (`spsc_handler.rs`'s `drain_spsc_shared`) or a generation/`Cell` snapshot
/// (`LuaEvictionCtx::gate`); both now read this atomic instead.
static MAXMEMORY_GLOBAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Publish the current `maxmemory` byte count. MUST be called at every
/// production write site of `RuntimeConfig.maxmemory`: server startup
/// (after `ServerConfig` resolves the guardrail-capped value into
/// `RuntimeConfig`) and `CONFIG SET maxmemory`. A missed publish is a silent
/// eviction-gate bypass — `tests/oom_bypass_closure.rs` exercises both call
/// sites.
#[inline]
pub fn publish_maxmemory(bytes: u64) {
    MAXMEMORY_GLOBAL.store(bytes, std::sync::atomic::Ordering::Relaxed);
}

/// Process-global eviction policy, as an [`EvictionPolicy`] discriminant.
///
/// Published alongside [`MAXMEMORY_GLOBAL`] so INFO can name the policy
/// without taking the runtime-config lock, and — more to the point — so it
/// names the policy the gate will actually apply rather than a second copy
/// that can drift from it.
static MAXMEMORY_POLICY_GLOBAL: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Publish the current `maxmemory-policy`. Same contract as
/// [`publish_maxmemory`]: every production write site of
/// `RuntimeConfig.maxmemory_policy` must call it — startup and
/// `CONFIG SET maxmemory-policy`.
#[inline]
pub fn publish_maxmemory_policy(name: &str) {
    MAXMEMORY_POLICY_GLOBAL.store(
        EvictionPolicy::from_str(name).as_u8(),
        std::sync::atomic::Ordering::Relaxed,
    );
}

/// Canonical name of the published eviction policy, for INFO.
#[inline]
pub fn maxmemory_policy_name() -> &'static str {
    EvictionPolicy::from_u8(MAXMEMORY_POLICY_GLOBAL.load(std::sync::atomic::Ordering::Relaxed))
        .as_str()
}

/// The configured `maxmemory` in bytes, 0 when unlimited.
///
/// Reads the same published atomic the eviction gate uses, so INFO cannot
/// disagree with the value actually being enforced.
#[inline]
pub fn maxmemory_bytes() -> u64 {
    MAXMEMORY_GLOBAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// `true` iff `maxmemory` is currently nonzero (i.e. a limit is configured).
/// Lock-free: a single Relaxed atomic load, safe to call on every SPSC drain
/// cycle or Lua `redis.call`/`redis.pcall` invocation.
#[inline]
pub fn maxmemory_is_set() -> bool {
    MAXMEMORY_GLOBAL.load(std::sync::atomic::Ordering::Relaxed) != 0
}

/// Publish the maxmemory hints. MUST be called wherever `maxmemory` (or the
/// shard count it is divided by) changes: server startup after the resolved
/// `num_shards` is written, and `CONFIG SET maxmemory`. A missed publish is
/// safe-but-slow for lowered limits only if stale-high — so this is kept to
/// a single helper to make the write sites greppable.
pub fn publish_maxmemory_hints(config: &RuntimeConfig) {
    MAXMEMORY_PER_SHARD_HINT.store(
        config.maxmemory_per_shard(),
        std::sync::atomic::Ordering::Relaxed,
    );
    MAXMEMORY_HINT.store(config.maxmemory, std::sync::atomic::Ordering::Relaxed);
}

/// THE effective memory budget a write is held to — the single definition,
/// shared by every path that enforces `maxmemory`.
///
/// Both the locked slow path (`evict_to_budget`) and the lock-free inline
/// pre-gate (`inline_write_can_skip_eviction`) call this, so they cannot
/// disagree on the cap: they do not merely apply the same formula, they run the
/// same code. moon#475: they DID disagree. The pre-gate open-coded the budget
/// and omitted the footprint divide, so one server enforced two different
/// effective `maxmemory` caps depending on which dispatch path a command took —
/// a plain `SET` (inline) was capped at `maxmemory`, while `HSET`/`EVAL` (the
/// other two dispatch paths) were capped at `maxmemory / ratio`.
///
/// - `elastic`: the caller's budget override (`EvictionRun::budget`), capped at
///   the instance `maxmemory`. Zero means "no override" → use `per_shard`.
/// - `per_shard`: `RuntimeConfig::maxmemory_per_shard()`, the shared-nothing
///   split of the instance limit.
/// - `ratio`: an ALREADY-SAMPLED footprint correction (see
///   [`crate::admin::metrics_setup::footprint_correction`] — one Relaxed atomic
///   load). Never sample inside this function: PR #510 moved that sampling off
///   the write path after it measured -57% on SET at c=8 P=16. A `ratio` of NaN
///   or <= 1.0 is the identity, so an unsampled or nonsensical correction can
///   only widen the budget back to the uncorrected value, never collapse it.
///
/// Scaling by the correction is what makes `maxmemory` bound REAL process
/// footprint rather than the accounting figure — see the block comment in
/// `evict_to_budget` for the measurement that motivated it.
#[inline]
pub(crate) fn effective_budget(elastic: usize, mm: usize, per_shard: usize, ratio: f64) -> usize {
    let budget = if elastic > 0 {
        elastic.min(mm)
    } else {
        per_shard
    };
    if ratio > 1.0 {
        ((budget as f64) / ratio) as usize
    } else {
        budget
    }
}

/// Lock-free pre-gate for the inline write path: `true` iff the slow path
/// (`evict_to_budget`) would PROVABLY no-op, i.e. `maxmemory == 0` or
/// `estimated_memory <= budget`, where `budget` comes from the shared
/// [`effective_budget`] — the same function, on the same sampled ratio, that
/// the slow path calls, so the two cannot enforce different caps. Any
/// uncertainty (unpublished hints) returns `false` → caller takes the lock and
/// runs the existing full path.
///
/// Cost: three Relaxed atomic loads (two `maxmemory` hints plus the cached
/// footprint correction) and one predictable branch. NOTHING here samples the
/// platform — the correction is republished once a second by shard 0's chore,
/// because measuring it per write cost -57% on SET at c=8 P=16 (PR #510).
///
/// Staleness of all three reads is bounded by the same one-write-behind window
/// the 100ms elastic-budget tick already accepts.
///
/// moon#475: this used to omit the footprint correction while the slow path
/// applied it, so a plain `SET` (inline path) was capped at `maxmemory` while
/// `HSET`/`EVAL` (the other two dispatch paths) were capped at
/// `maxmemory / ratio`. One server, two effective limits, no error and no log
/// line. Regression test:
/// `test_inline_pre_gate_agrees_with_slow_path_under_footprint_correction`.
#[inline]
pub fn inline_write_can_skip_eviction(estimated_memory: usize, elastic_budget: usize) -> bool {
    can_skip_eviction(
        MAXMEMORY_HINT.load(std::sync::atomic::Ordering::Relaxed),
        MAXMEMORY_PER_SHARD_HINT.load(std::sync::atomic::Ordering::Relaxed),
        estimated_memory,
        elastic_budget,
        crate::admin::metrics_setup::footprint_correction(),
    )
}

/// Pure decision core of [`inline_write_can_skip_eviction`], separated from
/// the process-global hint statics so it can be tested deterministically
/// (the statics race with `CONFIG SET maxmemory` tests in the parallel
/// test binary). `usize::MAX` in either hint = unpublished → fail safe.
#[inline]
fn can_skip_eviction(
    mm: usize,
    per_shard: usize,
    estimated_memory: usize,
    elastic_budget: usize,
    footprint_ratio: f64,
) -> bool {
    if mm == usize::MAX {
        return false; // unpublished — fail safe
    }
    if mm == 0 {
        return true; // unlimited: slow path early-returns
    }
    if elastic_budget == 0 && per_shard == usize::MAX {
        return false; // per-shard split needed but unpublished — fail safe
    }
    // moon#475: the SAME function the slow path uses, footprint correction
    // included. Not a copied formula — one implementation, so the inline path
    // cannot drift back into enforcing a different effective cap.
    estimated_memory <= effective_budget(elastic_budget, mm, per_shard, footprint_ratio)
}

/// Reservoir-sample up to `samples` random keys from the database without
/// materializing the entire keyspace.
///
/// Algorithm: pick a random `Segment`, then reservoir-sample one slot inside
/// it (Algorithm R with reservoir size 1). Repeat until either `samples` keys
/// have been collected or the per-segment retry budget is exhausted (which
/// can happen if `volatile_only` is true and most segments contain no
/// volatile keys). The returned vector is bounded by `MAX_VICTIM_SAMPLES`.
///
/// Cost: each iteration touches one segment (≤ a few hundred slots), so the
/// total work per call is `O(samples × segment_capacity)` instead of
/// `O(total_keys)` — the previous implementation cloned every key in the
/// database into a `Vec<CompactKey>` per eviction loop iteration, which
/// dominated CPU cost on hot eviction.
fn sample_random_keys(
    db: &Database,
    samples: usize,
    volatile_only: bool,
) -> SmallVec<[CompactKey; MAX_VICTIM_SAMPLES]> {
    let table = db.data();
    let mut out: SmallVec<[CompactKey; MAX_VICTIM_SAMPLES]> = SmallVec::new();

    let seg_count = table.segment_count();
    if seg_count == 0 || table.is_empty() {
        return out;
    }
    let want = samples.min(MAX_VICTIM_SAMPLES);
    if want == 0 {
        return out;
    }

    let mut rng = rand::rng();
    // Per-segment retries: bounded so a sparse volatile keyspace cannot
    // turn this into an unbounded loop.
    let max_attempts = want.saturating_mul(8);
    let mut attempts = 0usize;

    while out.len() < want && attempts < max_attempts {
        attempts += 1;
        let seg_idx = rng.random_range(0..seg_count);
        let seg = table.segment(seg_idx);

        // Reservoir-sample one occupied slot from this segment with the
        // optional volatile filter applied. Algorithm R with k=1.
        let mut chosen: Option<&CompactKey> = None;
        let mut seen = 0u32;
        for (k, v) in seg.iter_occupied() {
            if volatile_only && !v.has_expiry() {
                continue;
            }
            seen += 1;
            if rng.random_range(0..seen) == 0 {
                chosen = Some(k);
            }
        }
        if let Some(k) = chosen {
            out.push(k.clone());
        }
    }

    out
}

/// Compare two LRU timestamps with u32 wraparound handling.
/// Uses signed-distance comparison: treats the 32-bit epoch-seconds clock as
/// circular, so verdicts are correct for access-time gaps up to ±68 years —
/// including across the year-2106 clock wrap. (The old 16-bit variant
/// inverted verdicts once two keys' access times differed by more than
/// ~9.1h, making LRU evict hot keys under diurnal workloads.)
/// Returns true if `a` is considered older (less recent) than `b`.
#[inline]
pub fn lru_is_older(a: u32, b: u32) -> bool {
    // Signed difference handles wraparound: if a was accessed before b,
    // the circular distance a-b is negative (a < b in time) → a is older.
    (a.wrapping_sub(b) as i32) < 0
}

/// Sum used_memory across all databases for aggregate eviction decisions.
pub fn aggregate_used_memory(databases: &[Database]) -> usize {
    databases.iter().map(|db| db.estimated_memory()).sum()
}

/// Eviction policy variants matching Redis maxmemory-policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    NoEviction,
    AllKeysLru,
    AllKeysLfu,
    AllKeysRandom,
    VolatileLru,
    VolatileLfu,
    VolatileRandom,
    VolatileTtl,
}

impl EvictionPolicy {
    /// Parse a policy name string (case-insensitive) into an EvictionPolicy.
    ///
    /// Allocation-free: called per `evict_to_budget` invocation (i.e. per
    /// write command under memory pressure), so it must not build a lowercase
    /// `String` on every call.
    pub fn from_str(s: &str) -> Self {
        const TABLE: [(&str, EvictionPolicy); 7] = [
            ("allkeys-lru", EvictionPolicy::AllKeysLru),
            ("allkeys-lfu", EvictionPolicy::AllKeysLfu),
            ("allkeys-random", EvictionPolicy::AllKeysRandom),
            ("volatile-lru", EvictionPolicy::VolatileLru),
            ("volatile-lfu", EvictionPolicy::VolatileLfu),
            ("volatile-random", EvictionPolicy::VolatileRandom),
            ("volatile-ttl", EvictionPolicy::VolatileTtl),
        ];
        for (name, policy) in TABLE {
            if s.eq_ignore_ascii_case(name) {
                return policy;
            }
        }
        EvictionPolicy::NoEviction
    }

    /// Return the canonical string name for this policy.
    pub fn as_str(&self) -> &'static str {
        match self {
            EvictionPolicy::NoEviction => "noeviction",
            EvictionPolicy::AllKeysLru => "allkeys-lru",
            EvictionPolicy::AllKeysLfu => "allkeys-lfu",
            EvictionPolicy::AllKeysRandom => "allkeys-random",
            EvictionPolicy::VolatileLru => "volatile-lru",
            EvictionPolicy::VolatileLfu => "volatile-lfu",
            EvictionPolicy::VolatileRandom => "volatile-random",
            EvictionPolicy::VolatileTtl => "volatile-ttl",
        }
    }

    /// Discriminant for the process-global publish. Written out rather than
    /// derived via `as` so that reordering the enum cannot silently change
    /// what a running process's published value means.
    pub fn as_u8(self) -> u8 {
        match self {
            EvictionPolicy::NoEviction => 0,
            EvictionPolicy::AllKeysLru => 1,
            EvictionPolicy::AllKeysLfu => 2,
            EvictionPolicy::AllKeysRandom => 3,
            EvictionPolicy::VolatileLru => 4,
            EvictionPolicy::VolatileLfu => 5,
            EvictionPolicy::VolatileRandom => 6,
            EvictionPolicy::VolatileTtl => 7,
        }
    }

    /// Inverse of [`Self::as_u8`]. An unknown byte means the publish and the
    /// read disagree about the encoding, which can only happen through a bug —
    /// report the safe policy rather than guess.
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => EvictionPolicy::AllKeysLru,
            2 => EvictionPolicy::AllKeysLfu,
            3 => EvictionPolicy::AllKeysRandom,
            4 => EvictionPolicy::VolatileLru,
            5 => EvictionPolicy::VolatileLfu,
            6 => EvictionPolicy::VolatileRandom,
            7 => EvictionPolicy::VolatileTtl,
            _ => EvictionPolicy::NoEviction,
        }
    }
}

/// OOM error frame returned when eviction cannot free enough memory.
fn oom_error() -> Frame {
    Frame::Error(Bytes::from_static(
        b"OOM command not allowed when used memory > 'maxmemory'",
    ))
}

/// Context for spilling evicted entries to disk instead of deleting them.
///
/// When provided to [`evict_to_budget`] via [`EvictionRun::sync_spill`],
/// evicted entries are serialized to KvLeafPage DataFiles before being
/// removed from RAM.
pub struct SpillContext<'a> {
    pub shard_dir: &'a Path,
    pub manifest: &'a mut ShardManifest,
    pub next_file_id: &'a mut u64,
    /// Logical database the CURRENT victim belongs to — stamped into each
    /// spill file's manifest `FileEntry` so per-db recovery attribution
    /// (#139) survives the sync path too. Callers that loop over databases
    /// with one shared context (`run_eviction`, the memory-pressure sync
    /// fallback) MUST restamp this per iteration; the async batch path
    /// carries the equivalent on `SpillRequest.db_index`.
    pub db_index: usize,
}

/// Where eviction victims go (W4: the one eviction entry point).
///
/// The former `try_evict_if_needed*` family encoded this choice — plus every
/// optional parameter — in 13 function names. The choice is now data.
pub enum EvictionSink<'s, 'm> {
    /// Drop victims with no tiering. An evicting policy (`allkeys-*` /
    /// `volatile-*`) still honors Redis semantics and reclaims the budget by
    /// DELETING victims; only `noeviction` rejects with OOM. Each dropped key
    /// is reported to [`EvictionRun::report`]'s sink (dual-plane DEL
    /// emission, task #34).
    Plain,
    /// Durable batched sync spill: pwrite + fsync + manifest commit BEFORE
    /// any hot value is dropped (`evict_batch_durable` — shared multi-entry
    /// files, one manifest commit per file). `None` behaves as `Plain`
    /// (kept as an `Option` because several call sites decide at runtime).
    SyncSpill(Option<&'s mut SpillContext<'m>>),
    /// AOF-backstopped async spill via the background `SpillThread`.
    ///
    /// ## Durability ordering (crash-window fix)
    ///
    /// Under `--appendonly yes`: queue the `SpillRequest`, then immediately
    /// free RAM — safe ONLY because the AOF durably records the original
    /// write, so a crash between "dropped from RAM" and "spilled to disk" is
    /// recovered by AOF replay.
    ///
    /// Under `--appendonly no` there is no second copy once a value leaves
    /// RAM, so with a `manifest` available this batches victims through
    /// [`evict_batch_durable`] (fully durable before drop); with no manifest
    /// reachable it falls back to plain-drop (cap still enforced, Redis
    /// semantics, `noeviction` OOMs) — the tick-driven memory-pressure
    /// cascade with its manifest picks up durable spilling within 100ms.
    AsyncSpill {
        sender: &'s flume::Sender<SpillRequest>,
        shard_dir: &'s Path,
        next_file_id: &'s mut u64,
        db_index: usize,
        manifest: Option<&'s mut ShardManifest>,
    },
}

/// Options for [`evict_to_budget`] (W4).
///
/// Construct with [`EvictionRun::plain`] / [`EvictionRun::sync_spill`] /
/// [`EvictionRun::async_spill`], then chain the optional knobs:
///
/// ```ignore
/// evict_to_budget(db, config, EvictionRun::plain().budget(elastic).report(&mut sink))?;
/// ```
pub struct EvictionRun<'s, 'm, 'r> {
    /// Memory usage to reclaim against. `None` = `db.estimated_memory()`
    /// (single-DB, Redis-compatible). The memory-pressure cascade passes the
    /// aggregate across all databases.
    total_memory: Option<usize>,
    /// Elastic per-shard budget override (GAP-1). `0` keeps the static
    /// `maxmemory / num_shards`; a non-zero value (from
    /// `ShardDatabases::recompute_elastic_budget`) lets a hot shard borrow
    /// headroom donated by under-budget siblings. Always capped at the
    /// instance `maxmemory`.
    budget_override: usize,
    sink: EvictionSink<'s, 'm>,
    /// Sink for every plain-dropped (non-spill) victim key — task #34: write
    /// paths emit a dual-plane DEL record for keys eviction deleted with no
    /// cold-tier copy. Fires ONLY for plain drops — a spilled entry stays
    /// cold-readable, not deleted, so it is never reported here.
    on_plain_drop: Option<&'r mut dyn FnMut(&[u8])>,
}

impl<'s, 'm, 'r> EvictionRun<'s, 'm, 'r> {
    /// Victims are dropped (no tiering).
    pub fn plain() -> Self {
        Self {
            total_memory: None,
            budget_override: 0,
            sink: EvictionSink::Plain,
            on_plain_drop: None,
        }
    }

    /// Victims spill durably via the batch writer; `None` = plain drop.
    pub fn sync_spill(spill: Option<&'s mut SpillContext<'m>>) -> Self {
        Self {
            total_memory: None,
            budget_override: 0,
            sink: EvictionSink::SyncSpill(spill),
            on_plain_drop: None,
        }
    }

    /// Victims spill via the background `SpillThread` (AOF-backstopped) —
    /// see [`EvictionSink::AsyncSpill`] for the `--appendonly no` fallbacks.
    pub fn async_spill(
        sender: &'s flume::Sender<SpillRequest>,
        shard_dir: &'s Path,
        next_file_id: &'s mut u64,
        db_index: usize,
        manifest: Option<&'s mut ShardManifest>,
    ) -> Self {
        Self {
            total_memory: None,
            budget_override: 0,
            sink: EvictionSink::AsyncSpill {
                sender,
                shard_dir,
                next_file_id,
                db_index,
                manifest,
            },
            on_plain_drop: None,
        }
    }

    /// Reclaim against this total instead of `db.estimated_memory()`
    /// (aggregate checking from the memory-pressure cascade).
    #[must_use]
    pub fn total(mut self, total_memory: usize) -> Self {
        self.total_memory = Some(total_memory);
        self
    }

    /// Elastic per-shard budget override (`0` = static per-shard split).
    #[must_use]
    pub fn budget(mut self, budget_override: usize) -> Self {
        self.budget_override = budget_override;
        self
    }

    /// Report every plain-dropped victim key (dual-plane DEL, task #34).
    #[must_use]
    pub fn report(mut self, on_plain_drop: &'r mut dyn FnMut(&[u8])) -> Self {
        self.on_plain_drop = Some(on_plain_drop);
        self
    }
}

/// Evict until memory is back under the shard budget (W4: the single
/// eviction entry point — replaces the 13-name `try_evict_if_needed*`
/// family; the victim destination and optional knobs are [`EvictionRun`]
/// data instead of name suffixes).
///
/// Returns `Ok(())` once within budget (or when `maxmemory` is 0);
/// `Err(Frame)` with an OOM error when eviction cannot free enough memory
/// (`noeviction` policy, no evictable victims, or a sink that cannot make
/// progress).
///
/// The budget compared against is PER-SHARD (`maxmemory / num_shards`, or
/// the elastic override capped at instance `maxmemory`) — `maxmemory` is a
/// whole-instance cap that each shard enforces independently.
pub fn evict_to_budget(
    db: &mut Database,
    config: &RuntimeConfig,
    run: EvictionRun<'_, '_, '_>,
) -> Result<(), Frame> {
    if config.maxmemory == 0 {
        return Ok(());
    }

    let policy = EvictionPolicy::from_str(&config.maxmemory_policy);

    // Scale the budget down by how far the OS-charged footprint exceeds what
    // the allocator says is live.
    //
    // Without this, `maxmemory` bounds `used_memory` — an accounting figure —
    // while the process actually costs the OS substantially more. Measured on
    // a live instance: used_memory 4.21 GB against a 9.7 GB phys_footprint, a
    // 2.3x gap, of which 9.6 GB had been pushed to swap. maxmemory was set to
    // 19.2 GB (75% of RAM), so eviction never fired and the OS reclaimed by
    // swapping instead — the worst outcome for a key-value store. A restart
    // did NOT help: a fresh heap returned to the same ratio within 30 minutes,
    // so this is steady-state overhead, not drift that can be reset.
    //
    // Dividing by the ratio means a 19.2 GB maxmemory at 2.3x starts evicting
    // once accounted memory passes ~8.3 GB, holding REAL footprint near the
    // configured limit — which is what the operator asked for.
    let budget = {
        // ONE relaxed load. This function is on the write path, so the
        // correction is READ here and COMPUTED once a second by shard 0's
        // chore (`admin::footprint::refresh_footprint_correction`). Computing
        // it here instead — the shape this shipped in originally — cost three
        // syscalls plus a global-lock read per SET, and measured -57% on SET
        // at c=8 P=16 against the release before it.
        //
        // The denominator inside that computation is the WHOLE instance's
        // accounted memory, not this shard's slice: the footprint covers the
        // entire process, so dividing it by one shard's `estimated_memory()`
        // would inflate the ratio by roughly the shard count and evict N times
        // too aggressively at `--shards N`.
        let ratio = crate::admin::metrics_setup::footprint_correction();
        effective_budget(
            run.budget_override,
            config.maxmemory,
            config.maxmemory_per_shard(),
            ratio,
        )
    };

    let mut sink = run.sink;
    let mut noop = |_: &[u8]| {};
    let on_plain_drop: &mut dyn FnMut(&[u8]) = match run.on_plain_drop {
        Some(f) => f,
        None => &mut noop,
    };

    let mut current_total = run.total_memory.unwrap_or_else(|| db.estimated_memory());
    // moon#600: consecutive iterations that claimed progress but changed
    // NOTHING observable. See `EVICTION_STALL_LIMIT`.
    let mut stalled = 0usize;
    while current_total > budget {
        if policy == EvictionPolicy::NoEviction {
            return Err(oom_error());
        }
        // moon#466: bytes already handed to the spill thread are resident RAM
        // (`estimated_memory` now counts them) that is ALREADY scheduled for
        // release. Once they cover the overshoot this tick has done its job —
        // selecting more victims would chase memory that cannot drop until
        // the completions land, draining the whole database in a runaway
        // loop. Checked AFTER the `noeviction` gate so a `noeviction` server
        // still surfaces OOM rather than silently accepting the write.
        //
        // One `usize` read plus one comparison per victim: no allocation, no
        // lock, nothing added to the 100 ms sweep's cost.
        let pending = db.pending_spill_bytes();
        if pending > 0 && current_total.saturating_sub(pending) <= budget {
            return Ok(());
        }
        // moon#600: the liveness probe. `progressed` is a claim by the sink;
        // this triple is the OBSERVATION. Three O(1) field reads, no
        // allocation, no lock — see `EVICTION_STALL_LIMIT` for why all three
        // are needed. `memory` is the same figure the byte accounting below
        // uses, so this replaces that read rather than adding one.
        let before_probe = EvictionProgress::probe(db);
        let before = before_probe.memory;
        let deficit = current_total.saturating_sub(budget);
        let progressed = match &mut sink {
            EvictionSink::Plain | EvictionSink::SyncSpill(None) => {
                evict_one_with_spill(db, config, &policy, None, on_plain_drop)
            }
            EvictionSink::SyncSpill(Some(ctx)) => {
                // W2: reclaim the whole deficit as durable batches (shared
                // multi-entry files, ONE manifest commit per file) instead of
                // one single-entry file + one blocking manifest fsync
                // round-trip per victim.
                evict_batch_durable(
                    db,
                    config,
                    &policy,
                    ctx.shard_dir,
                    ctx.next_file_id,
                    ctx.manifest,
                    ctx.db_index,
                    deficit,
                    on_plain_drop,
                ) > 0
            }
            EvictionSink::AsyncSpill {
                sender,
                shard_dir,
                next_file_id,
                db_index,
                manifest,
            } => {
                if config.appendonly != "yes" {
                    match manifest.as_deref_mut() {
                        // No AOF backstop but a manifest is reachable:
                        // durable batched spill before any drop.
                        Some(m) => {
                            evict_batch_durable(
                                db,
                                config,
                                &policy,
                                shard_dir,
                                next_file_id,
                                m,
                                *db_index,
                                deficit,
                                on_plain_drop,
                            ) > 0
                        }
                        // No manifest and no AOF backstop: durable spill is
                        // impossible here, but the cap is still enforced —
                        // evicting policies plain-drop, `noeviction` OOMs.
                        None => evict_one_with_spill(db, config, &policy, None, on_plain_drop),
                    }
                } else {
                    evict_one_async_spill(
                        db,
                        config,
                        &policy,
                        sender,
                        shard_dir,
                        next_file_id,
                        *db_index,
                        on_plain_drop,
                    )
                }
            }
        };
        if !progressed {
            return Err(oom_error());
        }
        // moon#600: a sink may return `true` having done nothing at all (the
        // original spin: a victim `db.remove` could not find). Terminate on
        // the observation, never on the claim.
        let after_probe = EvictionProgress::probe(db);
        if EvictionProgress::note_iteration(before_probe, after_probe, &mut stalled) {
            warn!(
                policy = %config.maxmemory_policy,
                budget,
                current_total,
                stalled,
                "eviction made no observable progress for \
                 EVICTION_STALL_LIMIT consecutive victims; answering OOM \
                 rather than spinning the shard thread"
            );
            return Err(oom_error());
        }
        let after = after_probe.memory;
        current_total = current_total.saturating_sub(before.saturating_sub(after));
    }

    Ok(())
}

/// Consecutive eviction iterations allowed to claim progress while changing
/// nothing observable, before [`evict_to_budget`] gives up with `OOM`
/// (moon#600).
///
/// ## Why a backstop exists at all
///
/// `evict_to_budget`'s loop condition is `current_total > budget`, and
/// `current_total` only moves when `estimated_memory()` moves. A sink that
/// returns `true` without touching the database therefore loops forever —
/// the shard thread spins, the instance stays over budget, and no client
/// ever sees an error. That is strictly worse than an `OOM` reply: an `OOM`
/// is a bounded, visible, retryable failure; a spin is an outage.
///
/// moon#600's index-only victim was one way to reach it. This bound is
/// deliberately sampler-agnostic so the NEXT one cannot hang a shard either.
///
/// ## Why the probe is a triple and not just bytes
///
/// A "did memory drop?" test alone would misfire on a legitimately slow
/// reclaim. Two real paths free ~0 bytes per victim yet are making progress:
///
/// * async spill (moon#466) moves a value from the hot plane to the pending
///   plane — `estimated_memory()` counts both, so bytes are flat while
///   `pending_spill_bytes()` rises and the hot key count falls;
/// * an eviction of a tiny value can be dwarfed by rounding in the estimate,
///   but the hot key count still falls.
///
/// Progress is therefore "any of the three moved". Only an iteration that
/// moved NONE of them counts as stalled, and the counter resets on the first
/// real one, so a slow-but-working reclaim can never be converted into a
/// rejection. 16 consecutive no-ops is far outside anything a working sink
/// produces.
const EVICTION_STALL_LIMIT: usize = 16;

/// Observable state of an eviction run, sampled between victims (moon#600).
///
/// Deliberately NOT a measure of "enough progress" — only of "any progress".
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct EvictionProgress {
    /// Accounted bytes, including bytes already handed to the spill thread.
    memory: usize,
    /// Bytes queued for spill: rises when the async sink accepts a victim.
    pending_spill: usize,
    /// Hot keys: falls whenever a key leaves the hot plane, however tiny.
    hot_keys: usize,
}

impl EvictionProgress {
    /// Fold one iteration's observation into the consecutive-stall counter,
    /// answering "must this run give up?" (moon#600).
    ///
    /// Split out of `evict_to_budget` so the backstop is reachable from a
    /// test WITHOUT a stalling sink. It is defence in depth for a sampler
    /// that does not exist yet — no sampler can reach it today, which is
    /// exactly why it would otherwise rot unnoticed and fail to catch the
    /// next index-only victim.
    ///
    /// Pins all three properties the constant's doc comment claims: it fires
    /// only on the `EVICTION_STALL_LIMIT`-th CONSECUTIVE no-op, ANY of the
    /// three fields moving counts as progress, and progress resets the count
    /// so a slow-but-working reclaim can never accumulate into a rejection.
    #[inline]
    fn note_iteration(before: Self, after: Self, stalled: &mut usize) -> bool {
        if after == before {
            *stalled += 1;
            *stalled >= EVICTION_STALL_LIMIT
        } else {
            *stalled = 0;
            false
        }
    }

    #[inline]
    fn probe(db: &Database) -> Self {
        Self {
            memory: db.estimated_memory(),
            pending_spill: db.pending_spill_bytes(),
            hot_keys: db.data().len(),
        }
    }
}

/// Compute a shard's elastic memory budget from a snapshot of every shard's
/// published usage (GAP-1: hot-shard memory pooling).
///
/// `base` is the static per-shard budget (`maxmemory / num_shards`). Shards
/// under `base` donate their headroom; shards at-or-over `base` split the
/// donated surplus evenly. The aggregate invariant holds by construction:
///
/// ```text
/// Σ_hot (base + surplus/H) + Σ_under used_u
///   ≤ H·base + Σ_under (base − used_u) + Σ_under used_u  = N·base ≈ maxmemory
/// ```
///
/// so the instance-wide cap is never exceeded (modulo one tick of staleness
/// in the snapshot — the same bound the static scheme already has between
/// eviction ticks). An under-budget shard always keeps its full `base`, so
/// a shard whose load returns is never squeezed below its static share.
///
/// Returns `base` when pooling cannot help: unlimited memory, single shard,
/// this shard under budget, or no hot shard in the snapshot.
pub fn compute_elastic_budget(shard_id: usize, base: usize, used: &[usize]) -> usize {
    if base == 0 || used.len() <= 1 || shard_id >= used.len() {
        return base;
    }
    let mut surplus = 0usize;
    let mut hot = 0usize;
    for &u in used {
        if u < base {
            surplus += base - u;
        } else {
            hot += 1;
        }
    }
    if used[shard_id] < base || hot == 0 {
        return base;
    }
    base.saturating_add(surplus / hot)
}

/// Seed value for a shard's spill `file_id` counter after recovery.
///
/// On restart, AOF/RDB replay re-populates the hot tier and the persistence
/// cascade re-evicts the excess. If the counter restarted at 1 it would mint
/// `heap-000001.mpf`, `heap-000257.mpf`, … — the *same names* recovery just
/// loaded — and atomically overwrite cold files the rebuilt `cold_index` still
/// references, silently corrupting cold read-through (the B-2 bug).
///
/// Scanning the on-disk `<shard_dir>/data/heap-NNNNNN.mpf` files (rather than
/// the manifest) is deliberate: a spill that wrote its `.mpf` but crashed
/// before the manifest commit leaves an orphan the manifest doesn't know about,
/// yet it still occupies a filename that must not be clobbered. The physical
/// files are the authority on what can be overwritten.
///
/// Returns `max(existing file_id) + 1`. Because each batch file is named after
/// its first request's `file_id` (gaps are harmless — recovery iterates
/// manifest entries, see `spill_thread`), any seed strictly above the current
/// max guarantees every future filename is new. Returns `1` when disk-offload
/// is off, the directory is absent, or it holds no heap files — identical to
/// the historical default, so a fresh server and the live hot path are
/// unchanged by this seeding.
#[must_use]
pub fn next_spill_file_id_seed(shard_dir: Option<&Path>) -> u64 {
    let Some(dir) = shard_dir else { return 1 };
    let data_dir = dir.join("data");
    let entries = match std::fs::read_dir(&data_dir) {
        Ok(e) => e,
        Err(e) => {
            // NotFound is the normal fresh-server case (data/ not created yet).
            // Anything else (permissions, I/O) is anomalous: fail safe to the
            // legacy default but surface it so a misseed can't hide silently.
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    dir = %data_dir.display(),
                    error = %e,
                    "spill file_id seed: could not scan cold dir; defaulting to 1"
                );
            }
            return 1;
        }
    };
    let mut max_id: Option<u64> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        if let Some(id) = name
            .to_str()
            .and_then(|n| n.strip_prefix("heap-"))
            .and_then(|r| r.strip_suffix(".mpf"))
            .and_then(|s| s.parse::<u64>().ok())
        {
            max_id = Some(max_id.map_or(id, |m| m.max(id)));
        }
    }
    match max_id {
        Some(m) => m + 1,
        None => 1,
    }
}

/// Maximum victims collected into a single durable batch by
/// [`evict_batch_durable`], mirroring `SpillThread`'s own
/// `FLUSH_ENTRY_CAP` (256) so the no-AOF-backstop path's on-disk batch size
/// matches the async path's.
const NO_AOF_BATCH_CAP: usize = 256;

/// Bounded retries when victim sampling re-picks a key already staged in the
/// current batch (possible because, unlike the single-victim paths, this
/// function does NOT remove a key from `db` between picks -- removal must
/// wait until the whole batch is durable). Random re-sampling (see
/// `sample_random_keys`) makes an infinite repeat vanishingly unlikely; this
/// only guards a pathological near-empty or heavily-duplicate keyspace.
const NO_AOF_BATCH_STALL_LIMIT: usize = 16;

/// Pick one eviction victim according to `policy` (W2: the single
/// implementation of the policy dispatch — previously copy-pasted in
/// `evict_batch_durable`, `evict_one_async_spill`, and
/// `evict_one_with_spill`).
///
/// **Postcondition (moon#600): a returned key is present in the hot plane.**
/// Every caller's only way to make progress is `db.remove`, so a victim that
/// is not in `data` is not a victim — it is an infinite loop. The sampling
/// pickers satisfy this by construction (`sample_random_keys` iterates
/// occupied DashTable slots); `volatile-ttl` reads a maintained index and
/// must therefore verify, which is why this takes `&mut Database`.
fn select_victim(
    db: &mut Database,
    config: &RuntimeConfig,
    policy: &EvictionPolicy,
) -> Option<CompactKey> {
    match policy {
        EvictionPolicy::NoEviction => None,
        EvictionPolicy::AllKeysLru => find_victim_lru(db, config.maxmemory_samples, false),
        EvictionPolicy::AllKeysLfu => {
            find_victim_lfu(db, config.maxmemory_samples, config.lfu_decay_time, false)
        }
        EvictionPolicy::AllKeysRandom => find_victim_random(db, false),
        EvictionPolicy::VolatileLru => find_victim_lru(db, config.maxmemory_samples, true),
        EvictionPolicy::VolatileLfu => {
            find_victim_lfu(db, config.maxmemory_samples, config.lfu_decay_time, true)
        }
        EvictionPolicy::VolatileRandom => find_victim_random(db, true),
        EvictionPolicy::VolatileTtl => find_victim_volatile_ttl(db),
    }
}

/// Remaining-TTL floor below which an eviction victim is DROPPED instead of
/// spilled to the cold tier (moon#553).
///
/// ## Why
///
/// Spilling costs a datafile write, a manifest commit and a cold-index entry.
/// Cold TTLs are lazy-only — nothing sweeps the cold tier — so a victim that
/// expires a moment later occupies that IO and that disk slot until something
/// happens to read it. `volatile-ttl` makes this pathological by
/// construction: it selects the NEAREST deadline in the database, i.e. the
/// victim least worth persisting, and hands it to the spill writer.
///
/// ## Why dropping is safe
///
/// Eviction is already a destructive operation in Redis: `allkeys-*` and
/// `volatile-*` DELETE victims. Moon's spill tier is a strict extension that
/// preserves more than Redis would; declining it for a key with under a
/// second to live gives up at most `SPILL_TTL_FLOOR_MS` of extra visibility
/// on a key that is about to become invisible anyway, and never contradicts
/// Redis semantics. `noeviction` never reaches here (`select_victim` returns
/// `None`), so no policy that forbids deletion can be affected.
///
/// The drop is reported through the same `on_plain_drop` sink a plain-drop
/// eviction uses, so the dual-plane `DEL` (replica + AOF, task #34) is
/// emitted — without it a replica would keep a key its master no longer has
/// in EITHER tier, and no later expiry sweep could ever correct it.
///
/// One second is deliberately conservative: long enough to cover the spill
/// round-trip (queue → batch → pwrite → fsync → manifest commit) plus the
/// 100ms eviction tick, short enough that no realistic reader loses a key it
/// could still have used.
pub const SPILL_TTL_FLOOR_MS: u64 = 1_000;

/// `true` when `entry` is so close to expiry that spilling it would be wasted
/// IO (moon#553). Reads the TTL already in hand — no clock syscall, no
/// allocation, no lookup.
#[inline]
fn expiring_too_soon_to_spill(entry: &crate::storage::Entry, now_ms: u64) -> bool {
    entry.has_expiry() && entry.expires_at_ms().saturating_sub(now_ms) < SPILL_TTL_FLOOR_MS
}

/// Serialize a victim entry's value for spill (W2: the single implementation
/// — previously copy-pasted at all three spill entry points).
///
/// Returns `(value_type, value_bytes, flags, ttl_ms)`, or `None` when the
/// value cannot be faithfully serialized (in-memory corruption, e.g. an
/// unparseable sorted-set listpack score). **`None` is fail-closed**: the
/// caller must retain the hot value and skip the victim — the pre-W2
/// `unwrap_or_default()` at every call site durably spilled an EMPTY value
/// body and evicted the key (silent data loss on reload).
fn build_spill_payload(
    entry: &crate::storage::Entry,
) -> Option<(ValueType, Bytes, u8, Option<u64>)> {
    let val_ref = entry.as_redis_value();
    let (value_type, value_bytes): (ValueType, Bytes) = match val_ref {
        RedisValueRef::String(s) => (ValueType::String, Bytes::copy_from_slice(s)),
        ref other => {
            let vt = kv_spill::value_type_of(other);
            // `None` = corrupt value (serialize_collection logs it loudly).
            let buf = kv_serde::serialize_collection(other)?;
            (vt, Bytes::from(buf))
        }
    };
    let mut flags: u8 = 0;
    let ttl_ms = if entry.has_expiry() {
        flags |= entry_flags::HAS_TTL;
        Some(entry.expires_at_ms())
    } else {
        None
    };
    Some((value_type, value_bytes, flags, ttl_ms))
}

/// Inverse of [`build_spill_payload`]: rebuild a hot [`Entry`] from a spill
/// payload that never reached disk (deep-review F1).
///
/// `evict_one_async_spill` removes the hot entry as soon as the request is
/// queued, on the promise that the background thread's completion lands the
/// key in the cold index. When the pwrite FAILS, that promise is broken: the
/// key is in neither plane and every read returns nil until an AOF-replay
/// restart. The completion handler re-inserts via this helper instead
/// (fail-closed, mirroring the enqueue-failure path which retains the hot
/// value).
///
/// Returns `None` only if the payload does not deserialize — impossible for
/// bytes produced by `build_spill_payload` unless memory was corrupted in
/// transit; the caller logs that loudly.
pub(crate) fn rehydrate_spill_payload(
    value_type: ValueType,
    value_bytes: &Bytes,
    ttl_ms: Option<u64>,
) -> Option<crate::storage::Entry> {
    let redis_value = match value_type {
        ValueType::String => RedisValue::String(value_bytes.clone()),
        _ => kv_serde::deserialize_collection(value_bytes, value_type)?,
    };
    let mut entry = crate::storage::Entry::new_string(Bytes::new());
    entry.value = crate::storage::compact_value::CompactValue::from_redis_value(redis_value);
    if let Some(ttl) = ttl_ms {
        entry.set_expires_at_ms(ttl);
    }
    Some(entry)
}

/// Batch-durable synchronous spill (W2: the ONE sync spill implementation).
///
/// Originally built for the no-AOF-backstop case (`--appendonly no`); since
/// W2 it also backs every `SpillContext` sync path (`evict_one_with_spill`,
/// the tick-driven `run_eviction`, the memory-pressure cascade's sync
/// fallback), which previously wrote one single-entry file + one blocking
/// manifest commit per victim through `kv_spill::spill_to_datafile`.
///
/// Collects up to [`NO_AOF_BATCH_CAP`] distinct victims (or until the
/// estimated bytes staged cover `deficit`, whichever comes first) WITHOUT
/// removing any of them from RAM yet, then writes them as ONE durable batch
/// by calling `spill_thread::flush_buffer` synchronously (the exact
/// inline/oversized routing and multi-page batching the background
/// `SpillThread` itself uses, just invoked directly instead of over a
/// channel) -- ONE fsync (or a small, bounded number for any oversized
/// entries) covers the whole batch, instead of one fsync per key. Only
/// entries in a file that both pwrite-succeeded AND whose manifest commit
/// succeeded are removed from RAM; any partial failure retains the
/// corresponding hot values (never a silent drop) and is logged.
///
/// Returns the number of keys durably evicted (0 if nothing could be
/// evicted -- the caller surfaces OOM).
#[allow(clippy::too_many_arguments)]
fn evict_batch_durable(
    db: &mut Database,
    config: &RuntimeConfig,
    policy: &EvictionPolicy,
    shard_dir: &Path,
    next_file_id: &mut u64,
    manifest: &mut ShardManifest,
    db_index: usize,
    deficit: usize,
    on_plain_drop: &mut dyn FnMut(&[u8]),
) -> usize {
    let mut seen: std::collections::HashSet<CompactKey> = std::collections::HashSet::new();
    let mut buffer: Vec<SpillRequest> = Vec::new();
    let mut staged_bytes = 0usize;
    let mut stall = 0usize;
    // moon#553: victims dropped instead of spilled because they were about to
    // expire. Counted as reclaimed — a plain drop frees the same RAM a spill
    // would have, and `evict_to_budget` needs the progress signal or it
    // answers OOM after a batch made entirely of drops.
    let mut dropped = 0usize;
    let now_ms = db.now_ms();

    while buffer.len() < NO_AOF_BATCH_CAP && staged_bytes < deficit {
        let key = match select_victim(db, config, policy) {
            Some(k) => k,
            None => break,
        };
        if seen.contains(&key) {
            stall += 1;
            if stall >= NO_AOF_BATCH_STALL_LIMIT {
                break;
            }
            continue;
        }
        stall = 0;
        seen.insert(key.clone());

        let Some(entry) = db.data().get(key.as_bytes()) else {
            // Race with expiry: nothing to spill for this key, but it is
            // already gone, so keep sampling for more victims.
            continue;
        };
        // moon#553: about to expire — drop it now rather than pay a datafile
        // write plus a manifest commit for a cold entry nothing will reclaim.
        if expiring_too_soon_to_spill(entry, now_ms) {
            let before = db.estimated_memory();
            db.remove(key.as_bytes());
            crate::admin::metrics_setup::record_expiring_spill_skipped();
            crate::admin::metrics_setup::record_eviction();
            on_plain_drop(key.as_bytes());
            // A drop reclaims RAM immediately, so it counts toward the same
            // deficit the staged (not-yet-written) victims are sized against.
            staged_bytes += before.saturating_sub(db.estimated_memory());
            dropped += 1;
            continue;
        }
        // Fail-closed: an unserializable (corrupt) value is retained hot and
        // skipped — its `seen` entry stops the sampler from re-picking it
        // into this batch, and staging continues with other victims.
        let Some((value_type, value_bytes, flags, ttl_ms)) = build_spill_payload(entry) else {
            warn!(
                key = %String::from_utf8_lossy(key.as_bytes()),
                "kv_spill: victim value cannot be serialized; retaining hot \
                 value and skipping it (fail-closed, no empty-body spill)"
            );
            continue;
        };

        staged_bytes += key.as_bytes().len() + value_bytes.len();
        let file_id = *next_file_id;
        *next_file_id += 1;
        buffer.push(SpillRequest {
            key: Bytes::copy_from_slice(key.as_bytes()),
            db_index,
            value_bytes,
            value_type,
            flags,
            ttl_ms,
            file_id,
            shard_dir: PathBuf::from(shard_dir),
        });
    }

    if buffer.is_empty() {
        return dropped;
    }

    // Durable write (pwrite + fsync[+ fsync dir]) for the whole batch, still
    // fully in RAM at this point -- write-then-durable-then-drop.
    let completions = crate::storage::tiered::spill_thread::flush_buffer(&mut buffer);
    let mut reclaimed = 0usize;
    for completion in completions {
        if !completion.success {
            warn!(
                file_id = completion.file_entry.file_id,
                "kv_spill: durable batch spill failed under appendonly=no; \
                 retaining hot values for this file's keys (no silent drop)"
            );
            continue;
        }

        let file_id = completion.file_entry.file_id;
        manifest.add_file(completion.file_entry);
        if let Err(e) = manifest.commit() {
            warn!(
                file_id,
                error = %e,
                "kv_spill: manifest commit failed for durable batch spill under \
                 appendonly=no; retaining hot values (spill file may be orphaned; \
                 the orphan sweep reclaims it)"
            );
            continue;
        }

        for entry in completion.entries {
            // `Database::remove` ALSO clears any cold copy of the key (D1:
            // DEL must not resurrect a stale cold entry via read-through) --
            // so the ColdIndex insert MUST come AFTER remove, never before,
            // or `remove` wipes out the very entry being added.
            db.remove(&entry.key);
            if let Some(ref mut ci) = db.cold_index {
                ci.insert(
                    entry.key,
                    crate::storage::tiered::cold_index::ColdLocation {
                        file_id,
                        page_idx: entry.page_idx,
                        slot_idx: entry.slot_idx,
                        ttl_ms: entry.ttl_ms,
                        value_type: entry.value_type,
                    },
                );
                // moon#585: TIERED, not evicted. The key is still readable
                // through the cold index and still counted by `logical_len`,
                // so `DBSIZE` deliberately does not move — counting it in
                // `evicted_keys` is what made a live instance report 456,018
                // evictions against a `DBSIZE` that never budged.
                crate::admin::metrics_setup::record_key_spilled();
            } else {
                // No cold index to receive the key: the spill file is
                // recovery-only, so as far as the LIVE keyspace is concerned
                // this key is gone. That is a real eviction, and `DBSIZE`
                // drops with it.
                crate::admin::metrics_setup::record_eviction();
            }
            reclaimed += 1;
        }
    }
    reclaimed + dropped
}

/// Evict a single key via the async spill path.
///
/// AOF-backstopped fast path only (see the durability-ordering doc comment
/// on [`EvictionSink::AsyncSpill`], which routes to
/// [`evict_batch_durable`] instead when `--appendonly no`). Extracts
/// the entry, removes it from DashTable (immediate RAM relief), then sends a
/// `SpillRequest` to the background thread for pwrite.
fn evict_one_async_spill(
    db: &mut Database,
    config: &RuntimeConfig,
    policy: &EvictionPolicy,
    sender: &flume::Sender<SpillRequest>,
    shard_dir: &Path,
    next_file_id: &mut u64,
    db_index: usize,
    on_plain_drop: &mut dyn FnMut(&[u8]),
) -> bool {
    // Find victim key using the shared policy dispatch (same as sync path)
    let key = match select_victim(db, config, policy) {
        Some(k) => k,
        None => return false,
    };

    // Build SpillRequest from the entry BEFORE removing it from DashTable.
    // This is CPU work only -- no I/O on the event loop.
    if let Some(entry) = db.data().get(key.as_bytes()) {
        // moon#553: about to expire — skip the queue/pwrite/manifest round
        // trip entirely and reclaim the RAM now.
        if expiring_too_soon_to_spill(entry, db.now_ms()) {
            db.remove(key.as_bytes());
            crate::admin::metrics_setup::record_expiring_spill_skipped();
            crate::admin::metrics_setup::record_eviction();
            on_plain_drop(key.as_bytes());
            return true;
        }
        // Fail-closed: a value that cannot be faithfully serialized must not
        // be evicted (pre-W2 this spilled an EMPTY body and dropped the key).
        // Bail; the caller surfaces OOM rather than losing data.
        let Some((value_type, value_bytes, flags, ttl_ms)) = build_spill_payload(entry) else {
            warn!(
                key = %String::from_utf8_lossy(key.as_bytes()),
                "kv_spill: async-spill victim cannot be serialized; retaining \
                 hot value (fail-closed, no empty-body spill)"
            );
            return false;
        };

        let file_id = *next_file_id;
        *next_file_id += 1;

        // Second handle on the SAME payload the request carries (refcount,
        // not a copy) so the in-flight plane below can answer reads from RAM
        // while the request is queued.
        let pending_bytes = value_bytes.clone();

        let req = SpillRequest {
            key: Bytes::copy_from_slice(key.as_bytes()),
            db_index,
            value_bytes,
            value_type,
            flags,
            ttl_ms,
            file_id,
            shard_dir: PathBuf::from(shard_dir),
        };

        // CRITICAL: queue the spill BEFORE freeing RAM. If try_send fails
        // (channel full or disconnected) we MUST NOT remove the entry — that
        // would lose data because no completion will arrive and the file will
        // not exist. Bail out and let the next eviction tick retry.
        if sender.try_send(req).is_err() {
            return false;
        }
        // Free RAM, then hand the key to the in-flight plane. `db.remove`
        // clears any in-flight record for the key (that is what makes DEL
        // final), so the mark MUST follow it — marking first would erase
        // itself. Both calls run synchronously on the shard thread with no
        // `.await` between them, so no reader can catch the key between
        // planes.
        db.remove(key.as_bytes());

        // NOTE: we still do NOT insert a tentative cold_index entry here.
        // Under batching the (page_idx, slot_idx) are unknown at evict time;
        // slot 0 would return a *different* key's value in the pre-flush
        // window. Instead the key stays readable from the in-flight plane
        // itself, which holds the payload in RAM until the completion lands
        // (#459). Before that fix this window was documented as "accept a
        // brief read-miss" with the AOF as backstop — but the AOF backstops
        // DURABILITY, not VISIBILITY: measured on 400 keys, GET and EXISTS
        // denied live keys, DEL answered :0 and was then undone by the
        // completion, and DBSIZE under-counted by 276.
        db.spill_inflight_mark(
            Bytes::copy_from_slice(key.as_bytes()),
            crate::storage::db::PendingSpill {
                req_id: file_id,
                value_type,
                value_bytes: pending_bytes,
                ttl_ms,
            },
        );
        // moon#585: TIERED, not evicted — same reasoning as the batch path.
        // The key is answered from the in-flight plane now and from the cold
        // index once the completion lands; it never leaves the keyspace.
        // (Before this, the async path recorded NOTHING while the batch path
        // recorded an eviction — two spill paths, two different answers.)
        crate::admin::metrics_setup::record_key_spilled();
    } else {
        // Entry disappeared (race with expiry), just remove
        db.remove(key.as_bytes());
    }

    true
}

/// Evict a single key, optionally spilling to disk before removal.
///
/// `pub(crate)` so [`crate::storage::db_quota`] can reuse the exact same
/// victim-selection + spill logic for per-db quota enforcement, keeping a
/// single policy implementation for both the whole-instance and per-db gates.
///
/// Write-then-durable-then-drop: when a `SpillContext` is provided,
/// [`kv_spill::spill_to_datafile`] performs pwrite + fsync + manifest commit
/// synchronously, so by construction the file is durable BEFORE this
/// function ever calls `db.remove`. On spill failure the hot value is
/// retained (this function returns `false` without touching `db`) and the
/// error is surfaced via the `warn!` below — never a silent drop. Previously
/// this fell through to `db.remove` unconditionally even on an I/O error,
/// which discarded the value with no copy anywhere (real, unconditional data
/// loss on every spill failure, independent of any crash).
pub(crate) fn evict_one_with_spill(
    db: &mut Database,
    config: &RuntimeConfig,
    policy: &EvictionPolicy,
    spill: Option<&mut SpillContext<'_>>,
    on_plain_drop: &mut dyn FnMut(&[u8]),
) -> bool {
    // W2: the SpillContext arm delegates to the ONE durable batch spiller
    // (deficit 1 byte stages exactly one victim). Everything the per-victim
    // path used to hand-roll — write-then-durable-then-drop ordering, the
    // task #34/#45 every-type spill guarantees, fail-closed retention on
    // I/O error, D1 remove-before-cold-index ordering — is inherited from
    // `evict_batch_durable` instead of being a second implementation that
    // can drift (the drift is exactly what caused #34 defect 1 and #45).
    if let Some(ctx) = spill {
        return evict_batch_durable(
            db,
            config,
            policy,
            ctx.shard_dir,
            ctx.next_file_id,
            ctx.manifest,
            ctx.db_index,
            1,
            on_plain_drop,
        ) > 0;
    }

    // Plain-drop path (no spill context): pick one victim and delete it.
    let key = match select_victim(db, config, policy) {
        Some(k) => k,
        None => return false,
    };
    // moon#585: count the eviction only when a key ACTUALLY left the
    // keyspace. `select_victim` samples the hot plane, so the miss is rare
    // (a concurrent lazy-expiry hide, a sampler reading a stale expiry pair)
    // — but an unconditional `record_eviction()` here would make
    // `evicted_keys` grow without `DBSIZE` shrinking, which is exactly the
    // divergence this issue is about. Same gate on `on_plain_drop`: no
    // dual-plane DEL record for a key this call did not delete.
    let removed = db.remove(key.as_bytes()).is_some();
    if removed {
        crate::admin::metrics_setup::record_eviction();
        on_plain_drop(key.as_bytes());
    }
    true
}

// ── Victim selection helpers ───────────────────────────

/// Find the victim key with the oldest last_access from a random sample.
fn find_victim_lru(db: &Database, samples: usize, volatile_only: bool) -> Option<CompactKey> {
    let sampled = sample_random_keys(db, samples, volatile_only);
    if sampled.is_empty() {
        return None;
    }

    let mut oldest_key: Option<CompactKey> = None;
    let mut oldest_access: Option<u32> = None;

    for key in sampled.iter() {
        if let Some(entry) = db.data().get(key.as_bytes()) {
            let la = entry.last_access();
            match oldest_access {
                None => {
                    oldest_key = Some(key.clone());
                    oldest_access = Some(la);
                }
                Some(oldest) => {
                    if lru_is_older(la, oldest) {
                        oldest_key = Some(key.clone());
                        oldest_access = Some(la);
                    }
                }
            }
        }
    }

    oldest_key
}

/// Find the victim key with the lowest LFU counter from a random sample.
fn find_victim_lfu(
    db: &Database,
    samples: usize,
    lfu_decay_time: u64,
    volatile_only: bool,
) -> Option<CompactKey> {
    let sampled = sample_random_keys(db, samples, volatile_only);
    if sampled.is_empty() {
        return None;
    }

    let mut evict_key: Option<CompactKey> = None;
    let mut lowest_counter: Option<u8> = None;
    let mut oldest_access_for_tie: Option<u32> = None;

    for key in sampled.iter() {
        if let Some(entry) = db.data().get(key.as_bytes()) {
            let effective_counter =
                lfu_decay(entry.access_counter(), entry.last_access(), lfu_decay_time);

            let la = entry.last_access();
            let should_evict = match lowest_counter {
                None => true,
                Some(lowest) => {
                    effective_counter < lowest
                        || (effective_counter == lowest
                            && oldest_access_for_tie.map_or(true, |t| lru_is_older(la, t)))
                }
            };

            if should_evict {
                evict_key = Some(key.clone());
                lowest_counter = Some(effective_counter);
                oldest_access_for_tie = Some(la);
            }
        }
    }

    evict_key
}

/// Find a random victim key.
fn find_victim_random(db: &Database, volatile_only: bool) -> Option<CompactKey> {
    sample_random_keys(db, 1, volatile_only).into_iter().next()
}

/// Bounded number of provably-stale expiry pairs [`find_victim_volatile_ttl`]
/// will reap in ONE call before giving up on this tick (moon#600).
///
/// This is a LATENCY bound, not a correctness one. Termination is already
/// guaranteed without it — every non-returning iteration removes the pair it
/// just peeked, so the loop is bounded by `expiry_index_len()` — but this
/// path runs on the shard event loop inside the 100 ms sweep, and a large
/// leaked backlog must not be walked in a single tick. Reaping is permanent,
/// so successive calls converge: 64 pairs retired per call, never re-seen.
const VOLATILE_TTL_STALE_REAP_LIMIT: usize = 64;

/// Find the victim with the globally nearest expiry — exact, via the
/// deadline-ordered expiry index (#541), O(log n). Replaces the old
/// random-sampling approximation (#551), which could miss the soonest
/// key entirely when it was buried among many far-future volatiles.
///
/// ## Why this verifies (moon#600)
///
/// This is the only sampler that reads a *maintained index* rather than
/// `data` itself, so it is the only one that can name a key the caller
/// cannot remove. `evict_to_budget`'s sole progress mechanism is
/// `db.remove`; handing it a pair whose hot entry is gone means
/// `before == after` forever and the shard thread spins — an unreachable
/// instance rather than an `OOM` reply.
///
/// The staleness predicate is byte-for-byte the one `expire_cycle` already
/// uses (`src/server/expiration.rs`): a pair is PROVABLY stale when the
/// entry is gone **or** carries a different deadline than the pair claims.
/// Anything else is left alone — in particular a pair that merely looks
/// "not due yet" is valid and must survive, which is why no clock is read
/// here at all. Reaping (rather than skipping) matches
/// [`Database::drop_expiry_index_pair`]'s stated contract: without it the
/// next peek walks the same head again.
///
/// Reaping is a repair, never an eviction: the key it names is already
/// absent from the keyspace, so `DBSIZE` cannot move and neither
/// `evicted_keys` nor `spilled_keys` is touched (moon#585/#599 accounting
/// is unaffected by construction — this function records nothing).
fn find_victim_volatile_ttl(db: &mut Database) -> Option<CompactKey> {
    let mut reaped = 0usize;
    while reaped < VOLATILE_TTL_STALE_REAP_LIMIT {
        let (ts, key) = db.peek_nearest_expiry()?;
        if db
            .data()
            .get(key.as_bytes())
            .is_some_and(|e| e.expires_at_ms() == ts)
        {
            return Some(key);
        }
        // Provably stale. `drop_expiry_index_pair` removes exactly the pair
        // just peeked, so `expiry_index_len()` strictly decreases: this loop
        // cannot repeat a pair and cannot run longer than the index is long.
        db.drop_expiry_index_pair(ts, &key);
        reaped += 1;
    }
    warn!(
        reaped = VOLATILE_TTL_STALE_REAP_LIMIT,
        remaining = db.expiry_index_len(),
        "volatile-ttl: expiry index disagrees with the keyspace; retired the \
         per-tick cap of stale pairs and yielded no victim this tick (a TTL \
         writer is leaking index pairs — see debug_expiry_index_consistent)"
    );
    None
}

#[cfg(test)]
mod tests {
    // Legacy wrappers used only in tests for backward-compatible assertions.
    fn evict_one_random(db: &mut super::Database, volatile_only: bool) -> bool {
        if let Some(key) = super::find_victim_random(db, volatile_only) {
            db.remove(key.as_bytes());
            true
        } else {
            false
        }
    }

    fn evict_one_volatile_ttl(db: &mut super::Database, _samples: usize) -> bool {
        if let Some(key) = super::find_victim_volatile_ttl(db) {
            db.remove(key.as_bytes());
            true
        } else {
            false
        }
    }

    use super::*;
    use crate::persistence::kv_page::read_datafile;
    use crate::persistence::manifest::ShardManifest;
    use crate::storage::entry::{Entry, current_secs, current_time_ms};

    /// moon#551: volatile-ttl must evict the GLOBALLY nearest-expiry key,
    /// not the nearest within a random sample. With one soon-expiring key
    /// buried among 5_000 far-future volatile keys, a 5-key sample picks
    /// it with probability ~0.1%; the deadline-index head always does.
    #[test]
    fn volatile_ttl_evicts_globally_nearest_expiry() {
        let mut db = super::Database::new();
        let far_ms = current_time_ms() + 3_600_000;
        for i in 0..5_000u32 {
            db.set(
                Bytes::from(format!("far_{i}")),
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), far_ms + u64::from(i)),
            );
        }
        // Volatile and clearly the soonest, but NOT expired.
        db.set(
            Bytes::from_static(b"soonest"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), current_time_ms() + 60_000),
        );

        assert!(evict_one_volatile_ttl(&mut db, 5));
        assert!(
            db.data().get(&b"soonest"[..]).is_none(),
            "the globally nearest-expiry key must be the victim"
        );
        assert_eq!(db.len(), 5_000, "exactly one key evicted");
    }

    #[test]
    fn spill_payload_round_trips_through_rehydrate() {
        // F1 (deep review): a failed background spill must be able to
        // resurrect the exact hot entry from its request payload.
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
        map.insert(Bytes::from_static(b"f2"), Bytes::from_static(b"v2"));
        let mut entry = Entry::new_string(Bytes::new());
        entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
            crate::storage::entry::RedisValue::Hash(map.clone()),
        );
        entry.set_expires_at_ms(current_time_ms() + 60_000);

        let (vt, bytes, _flags, ttl) = build_spill_payload(&entry).expect("serializable");
        let restored = rehydrate_spill_payload(vt, &bytes, ttl).expect("round-trip");
        match restored.as_redis_value() {
            RedisValueRef::Hash(h) => assert_eq!(*h, map),
            _ => panic!("expected hash after rehydrate"),
        }
        assert_eq!(restored.expires_at_ms(), entry.expires_at_ms());

        // String payloads round-trip too.
        let s = Entry::new_string(Bytes::from_static(b"hello"));
        let (vt, bytes, _f, ttl) = build_spill_payload(&s).expect("serializable");
        let restored = rehydrate_spill_payload(vt, &bytes, ttl).expect("round-trip");
        match restored.as_redis_value() {
            RedisValueRef::String(v) => assert_eq!(v, b"hello"),
            _ => panic!("expected string after rehydrate"),
        }
    }

    #[test]
    fn lru_is_older_valid_beyond_nine_hours() {
        // Regression: the old i16 comparison window (±32768s ≈ ±9.1h)
        // inverted verdicts for diurnal idle gaps — a key idle 10h compared
        // as NEWER than one touched a minute ago, so LRU evicted the hot key.
        let now: u32 = 1_754_000_000;
        let idle_10h = now - 36_000;
        let idle_1m = now - 60;
        assert!(lru_is_older(idle_10h, idle_1m));
        assert!(!lru_is_older(idle_1m, idle_10h));
        // Still correct at multi-day idle.
        let idle_3d = now - 259_200;
        assert!(lru_is_older(idle_3d, idle_10h));
    }

    // -----------------------------------------------------------------
    // compute_elastic_budget (GAP-1)
    // -----------------------------------------------------------------

    // -----------------------------------------------------------------
    // MAXMEMORY_GLOBAL atomic (Gap C)
    // -----------------------------------------------------------------

    #[test]
    fn maxmemory_publish_and_is_set_roundtrip() {
        // Unset (0) => maxmemory_is_set() is false.
        publish_maxmemory(0);
        assert!(!maxmemory_is_set());
        // Any nonzero byte count => true.
        publish_maxmemory(1);
        assert!(maxmemory_is_set());
        publish_maxmemory(64 * 1024 * 1024);
        assert!(maxmemory_is_set());
        // Un-publish (CONFIG SET maxmemory 0) flips it back off.
        publish_maxmemory(0);
        assert!(!maxmemory_is_set());
    }

    #[test]
    fn elastic_budget_balanced_load_keeps_base() {
        // Every shard at base — no surplus to borrow.
        assert_eq!(compute_elastic_budget(0, 100, &[100, 100, 100, 100]), 100);
    }

    #[test]
    fn elastic_budget_under_budget_shard_keeps_base() {
        // An under-budget shard never borrows (and never shrinks below base).
        assert_eq!(compute_elastic_budget(1, 100, &[100, 10, 100, 100]), 100);
    }

    #[test]
    fn elastic_budget_single_hot_shard_takes_all_surplus() {
        // Shard 0 hot, three idle siblings donate (100-10)*3 = 270.
        assert_eq!(compute_elastic_budget(0, 100, &[100, 10, 10, 10]), 370);
    }

    #[test]
    fn elastic_budget_two_hot_shards_split_surplus() {
        // Two hot shards split (100-10)*2 = 180 → +90 each.
        assert_eq!(compute_elastic_budget(0, 100, &[150, 10, 10, 120]), 190);
        assert_eq!(compute_elastic_budget(3, 100, &[150, 10, 10, 120]), 190);
    }

    #[test]
    fn elastic_budget_snapshot_aggregate_never_exceeds_total() {
        // Snapshot invariant: Σ_hot budget_i + Σ_under used_i ≤ N·base.
        // (A donor growing back is re-balanced on the next 100ms tick — the
        // hot shard's budget shrinks with the surplus and write-path
        // eviction pulls it back down, so transient overshoot is bounded by
        // one tick of donor write growth, same class as the static scheme's
        // between-tick slack.)
        let base = 100usize;
        for used in [
            vec![400, 0, 0, 0],
            vec![150, 150, 0, 0],
            vec![100, 100, 100, 100],
            vec![0, 0, 0, 0],
            vec![350, 99, 1, 0],
        ] {
            let n = used.len();
            let total: usize = (0..n)
                .map(|i| {
                    if used[i] >= base {
                        compute_elastic_budget(i, base, &used)
                    } else {
                        used[i]
                    }
                })
                .sum();
            assert!(
                total <= n * base,
                "snapshot aggregate {total} > cap {} for {used:?}",
                n * base
            );
        }
    }

    #[test]
    fn elastic_budget_degenerate_inputs_fall_back_to_base() {
        assert_eq!(compute_elastic_budget(0, 0, &[10, 10]), 0); // unlimited
        assert_eq!(compute_elastic_budget(0, 100, &[500]), 100); // single shard
        assert_eq!(compute_elastic_budget(7, 100, &[100, 0]), 100); // bad index
    }

    #[test]
    fn elastic_budget_override_widens_eviction_threshold() {
        // A db over the static budget but under the override must NOT evict;
        // override 0 must evict down to the static threshold.
        let mut db = Database::new();
        let mut config = make_config(20_000, "allkeys-lru");
        config.num_shards = 4; // static per-shard budget = 5000
        for i in 0..50 {
            let key = bytes::Bytes::from(format!("k{i}"));
            let entry = Entry::new_string(bytes::Bytes::from(vec![0u8; 100]));
            db.set(key, entry);
        }
        let used = db.estimated_memory();
        assert!(
            used > 5_000 && used < 20_000,
            "fixture must sit between static budget and maxmemory, got {used}"
        );

        // Elastic override above current usage: no eviction.
        let keys_before = db.len();
        evict_to_budget(
            &mut db,
            &config,
            EvictionRun::sync_spill(None)
                .total(used)
                .budget(used + 1024),
        )
        .expect("within elastic budget must not OOM");
        assert_eq!(db.len(), keys_before, "no eviction under elastic budget");

        // Static budget (override 0): must evict below 5000 bytes.
        let used_now = db.estimated_memory();
        evict_to_budget(
            &mut db,
            &config,
            EvictionRun::sync_spill(None).total(used_now).budget(0),
        )
        .expect("eviction should succeed");
        assert!(
            db.estimated_memory() <= 5_000,
            "static budget must evict down to maxmemory/num_shards"
        );
        assert!(db.len() < keys_before);
    }

    #[test]
    fn elastic_budget_override_is_capped_at_instance_maxmemory() {
        // Override beyond maxmemory clamps: usage above maxmemory must evict.
        let mut db = Database::new();
        let config = make_config(500, "allkeys-lru");
        for i in 0..50 {
            let key = bytes::Bytes::from(format!("k{i}"));
            let entry = Entry::new_string(bytes::Bytes::from(vec![0u8; 100]));
            db.set(key, entry);
        }
        let used = db.estimated_memory();
        assert!(used > 500);
        evict_to_budget(
            &mut db,
            &config,
            EvictionRun::sync_spill(None).total(used).budget(usize::MAX),
        )
        .expect("eviction should succeed");
        assert!(
            db.estimated_memory() <= 500,
            "override must clamp at instance maxmemory"
        );
    }

    fn make_config(maxmemory: usize, policy: &str) -> RuntimeConfig {
        RuntimeConfig {
            maxmemory,
            maxmemory_policy: policy.to_string(),
            maxmemory_samples: 5,
            db_maxmemory: Vec::new(),
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            save: None,
            appendonly: "no".to_string(),
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
            client_query_buffer_limit: 1024 * 1024 * 1024,
            client_query_buffer_limit_preauth: 64 * 1024,
            client_write_timeout_ms: 60_000,
            client_output_buffer_limit_normal: 256 * 1024 * 1024,
            timeout: 0,
            tcp_keepalive: 300,
            num_shards: 1,
        }
    }

    #[test]
    fn per_shard_budget_divides_maxmemory_at_enforcement() {
        // Regression for the multishard "zombie RAM" bug: maxmemory is a
        // whole-instance cap, but each shard enforces eviction independently.
        // Without per-shard division an N-shard server tolerates ~N×maxmemory.
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        db.set_string(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"));
        db.set_string(Bytes::from_static(b"k3"), Bytes::from_static(b"v3"));
        db.set_string(Bytes::from_static(b"k4"), Bytes::from_static(b"v4"));
        let total = db.estimated_memory();
        assert!(total > 0);

        // Whole-instance cap == current memory, single shard: per-shard budget
        // equals `total`, so nothing is evicted.
        let mut cfg1 = make_config(total, "allkeys-lru");
        cfg1.num_shards = 1;
        assert!(evict_to_budget(&mut db, &cfg1, EvictionRun::plain()).is_ok());
        assert_eq!(
            db.len(),
            4,
            "single shard: nothing evicted at budget == total"
        );

        // SAME whole-instance cap, 4 shards: per-shard budget = ceil(total/4) <<
        // total, so this shard MUST shed keys until it is under the per-shard
        // budget. Pre-fix (comparison vs raw maxmemory) this evicted nothing.
        let mut cfg4 = make_config(total, "allkeys-lru");
        cfg4.num_shards = 4;
        assert!(evict_to_budget(&mut db, &cfg4, EvictionRun::plain()).is_ok());
        assert!(
            db.len() < 4,
            "4 shards: per-shard budget must force eviction (got len {})",
            db.len()
        );
        assert!(
            db.estimated_memory() <= cfg4.maxmemory_per_shard(),
            "evicted below the per-shard budget"
        );
    }

    #[test]
    fn test_eviction_policy_from_str() {
        assert_eq!(
            EvictionPolicy::from_str("noeviction"),
            EvictionPolicy::NoEviction
        );
        assert_eq!(
            EvictionPolicy::from_str("allkeys-lru"),
            EvictionPolicy::AllKeysLru
        );
        assert_eq!(
            EvictionPolicy::from_str("allkeys-lfu"),
            EvictionPolicy::AllKeysLfu
        );
        assert_eq!(
            EvictionPolicy::from_str("allkeys-random"),
            EvictionPolicy::AllKeysRandom
        );
        assert_eq!(
            EvictionPolicy::from_str("volatile-lru"),
            EvictionPolicy::VolatileLru
        );
        assert_eq!(
            EvictionPolicy::from_str("volatile-lfu"),
            EvictionPolicy::VolatileLfu
        );
        assert_eq!(
            EvictionPolicy::from_str("volatile-random"),
            EvictionPolicy::VolatileRandom
        );
        assert_eq!(
            EvictionPolicy::from_str("volatile-ttl"),
            EvictionPolicy::VolatileTtl
        );
        assert_eq!(
            EvictionPolicy::from_str("ALLKEYS-LRU"),
            EvictionPolicy::AllKeysLru
        );
        assert_eq!(
            EvictionPolicy::from_str("unknown"),
            EvictionPolicy::NoEviction
        );
    }

    #[test]
    fn test_noeviction_returns_oom() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"value"));
        let config = make_config(1, "noeviction");
        let result = evict_to_budget(&mut db, &config, EvictionRun::plain());
        assert!(result.is_err());
        match result.unwrap_err() {
            Frame::Error(msg) => {
                assert!(msg.starts_with(b"OOM"));
            }
            _ => panic!("Expected error frame"),
        }
    }

    #[test]
    fn test_no_eviction_when_unlimited() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"value"));
        let config = make_config(0, "allkeys-lru");
        assert!(evict_to_budget(&mut db, &config, EvictionRun::plain()).is_ok());
    }

    #[test]
    fn test_no_eviction_when_under_limit() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"value"));
        let config = make_config(1_000_000, "allkeys-lru");
        assert!(evict_to_budget(&mut db, &config, EvictionRun::plain()).is_ok());
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn test_lru_evicts_oldest() {
        // `sample_random_keys` reservoir-samples `maxmemory_samples` victims
        // per eviction round using a non-deterministic RNG, so a single
        // eviction call over a tiny 3-key population is statistically flaky:
        // with probability ~(2/3)^5 ≈ 13% the oldest key is never sampled in
        // that round and a different key is evicted. We instead drive
        // eviction in a bounded loop, shrinking maxmemory after each round,
        // so "old" is eventually guaranteed to be sampled and picked
        // (worst case once the population shrinks to a single key).
        let mut db = Database::new();
        let mut entry1 = Entry::new_string(Bytes::from_static(b"val1"));
        entry1.set_last_access(current_secs() - 100);
        db.set(Bytes::from_static(b"old"), entry1);

        let mut entry2 = Entry::new_string(Bytes::from_static(b"val2"));
        entry2.set_last_access(current_secs() - 50);
        db.set(Bytes::from_static(b"medium"), entry2);

        let mut entry3 = Entry::new_string(Bytes::from_static(b"val3"));
        entry3.set_last_access(current_secs());
        db.set(Bytes::from_static(b"new"), entry3);

        // Drive eviction rounds until "old" is gone, bounded to prevent
        // infinite looping if the sampler is broken.
        for _ in 0..50 {
            if db.data().get(b"old" as &[u8]).is_none() {
                break;
            }
            let mem = db.estimated_memory();
            if mem == 0 {
                break;
            }
            let config = make_config(mem.saturating_sub(1), "allkeys-lru");
            let result = evict_to_budget(&mut db, &config, EvictionRun::plain());
            assert!(result.is_ok());
        }
        assert!(
            db.data().get(b"old" as &[u8]).is_none(),
            "LRU eviction failed to remove the oldest key within 50 rounds",
        );
    }

    #[test]
    fn test_allkeys_random_evicts() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        db.set_string(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"));
        db.set_string(Bytes::from_static(b"k3"), Bytes::from_static(b"v3"));

        let config = make_config(1, "allkeys-random");
        let result = evict_to_budget(&mut db, &config, EvictionRun::plain());
        assert!(result.is_ok());
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn test_volatile_only_skips_persistent() {
        let mut db = Database::new();
        db.set_string(
            Bytes::from_static(b"persistent"),
            Bytes::from_static(b"value"),
        );
        let future_ms = current_time_ms() + 3_600_000;
        db.set_string_with_expiry(
            Bytes::from_static(b"volatile"),
            Bytes::from_static(b"value"),
            future_ms,
        );

        let result = evict_one_random(&mut db, true);
        assert!(result);
        assert_eq!(db.len(), 1);
        assert!(db.data().contains_key(b"persistent" as &[u8]));
    }

    #[test]
    fn test_volatile_ttl_evicts_soonest() {
        // `sample_random_keys` reservoir-samples across DashTable segments
        // with a non-deterministic RNG, so over a tiny 2-key population a
        // single eviction round can fail to sample `soon` and pick `later`
        // instead. Same flake pattern as `test_lru_evicts_oldest`: drive
        // eviction in a bounded loop so `soon` is guaranteed to be sampled
        // (worst case once the population shrinks to one key).
        let mut db = Database::new();
        let now_ms = current_time_ms();
        db.set_string_with_expiry(
            Bytes::from_static(b"soon"),
            Bytes::from_static(b"v"),
            now_ms + 10_000,
        );
        db.set_string_with_expiry(
            Bytes::from_static(b"later"),
            Bytes::from_static(b"v"),
            now_ms + 3_600_000,
        );

        let mut evictions = 0;
        for _ in 0..50 {
            if db.data().get(b"soon" as &[u8]).is_none() {
                break;
            }
            if !evict_one_volatile_ttl(&mut db, 5) {
                break;
            }
            evictions += 1;
            // Re-insert the wrongly-evicted `later` so we always have two
            // candidates until the algorithm picks `soon`. This proves the
            // sampler eventually selects the soonest-expiring key.
            if db.data().get(b"later" as &[u8]).is_none()
                && db.data().get(b"soon" as &[u8]).is_some()
            {
                db.set_string_with_expiry(
                    Bytes::from_static(b"later"),
                    Bytes::from_static(b"v"),
                    now_ms + 3_600_000,
                );
            }
        }
        assert!(
            db.data().get(b"soon" as &[u8]).is_none(),
            "expected `soon` evicted within bounded rounds (evictions={evictions})"
        );
    }

    #[test]
    fn test_policy_as_str() {
        assert_eq!(EvictionPolicy::NoEviction.as_str(), "noeviction");
        assert_eq!(EvictionPolicy::AllKeysLru.as_str(), "allkeys-lru");
        assert_eq!(EvictionPolicy::VolatileTtl.as_str(), "volatile-ttl");
    }

    #[test]
    fn test_evict_with_spill_creates_datafile() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.set_string(
            Bytes::from_static(b"spill_key"),
            Bytes::from_static(b"spill_val"),
        );

        let config = make_config(1, "allkeys-lru");
        let mut ctx = SpillContext {
            shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 0,
        };

        let result = evict_to_budget(&mut db, &config, EvictionRun::sync_spill(Some(&mut ctx)));
        assert!(result.is_ok());
        assert_eq!(db.len(), 0);

        // Verify DataFile was created
        let file_path = shard_dir.join("data/heap-000001.mpf");
        assert!(file_path.exists(), "DataFile should have been created");

        // Verify contents
        let pages = read_datafile(&file_path).unwrap();
        assert_eq!(pages.len(), 1);
        let entry = pages[0].get(0).unwrap();
        assert_eq!(entry.key, b"spill_key");
        assert_eq!(entry.value, b"spill_val");

        // file_id should have been incremented
        assert_eq!(next_file_id, 2);
    }

    /// RED for the #139 follow-up (PR #383 adversarial review, confirmed
    /// defect): the SYNC spill path — `run_eviction`'s non-cascade branch →
    /// `evict_one_with_spill` → `kv_spill::spill_to_datafile` — hardcoded
    /// `db_index: 0` in the manifest `FileEntry`, so an ordinary maxmemory
    /// eviction of a db>0 victim produced a spill file that recovery's
    /// `rebuild_from_manifest_per_db` re-attached to db 0: exactly the #139
    /// wrong-database corruption, reintroduced through a path the async-spill
    /// fix didn't cover. GREEN: `SpillContext.db_index` (restamped per
    /// database by the `run_eviction` loop) is threaded through to the
    /// manifest entry.
    #[test]
    fn sync_spill_stamps_victims_database_into_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.set_string(
            Bytes::from_static(b"db2_key"),
            Bytes::from_static(b"db2_val"),
        );

        let config = make_config(1, "allkeys-lru");
        let mut ctx = SpillContext {
            shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 2,
        };

        let result = evict_to_budget(&mut db, &config, EvictionRun::sync_spill(Some(&mut ctx)));
        assert!(result.is_ok());
        assert_eq!(db.len(), 0, "victim must have been evicted");

        let files = manifest.files();
        assert_eq!(files.len(), 1, "exactly one spill file registered");
        assert_eq!(
            files[0].db_index, 2,
            "sync spill must attribute the file to the database the victim \
             was evicted from, not db 0 (#139 recovery corruption otherwise)"
        );
    }

    /// Fail-safe send-before-remove: when the async-spill request channel is
    /// FULL, eviction must NOT remove the victim from the hot tier (no data
    /// loss) and must surface OOM rather than pretend success. The live path
    /// (`evict_one_async_spill`) `try_send`s the `SpillRequest` BEFORE
    /// `db.remove`, so a failed send leaves the key resident for retry on the
    /// next eviction tick. This regression guard locks that ordering — the exact
    /// request-side data-loss scenario CodeRabbit raised on PR #144 (verified
    /// stale against the live code; this test keeps it that way).
    #[test]
    fn async_spill_full_channel_keeps_victim_in_ram_no_data_loss() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));

        // Per-shard budget of 1 byte forces an eviction attempt for k1.
        // appendonly=yes: this test exercises the AOF-backstopped fast path's
        // full-channel ordering specifically. Under appendonly=no (this
        // crate's `make_config` default) `evict_one_async_spill` now bails
        // before ever touching the channel (no manifest reachable here, no
        // AOF backstop) — see `async_spill_no_aof_backstop_durably_spills_before_drop`
        // below for that scenario's dedicated coverage.
        let mut config = make_config(1, "allkeys-lru");
        config.appendonly = "yes".to_string();

        // bounded(1) PRE-FILLED so the eviction's `try_send` observes a FULL
        // channel (the reviewer's backpressure scenario) rather than a
        // disconnect — both fail `try_send`, but full is the precise concern.
        let (tx, _rx) = flume::bounded::<SpillRequest>(1);
        tx.try_send(SpillRequest {
            key: Bytes::from_static(b"filler"),
            db_index: 0,
            value_bytes: Bytes::from_static(b"x"),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
            file_id: 0,
            shard_dir: shard_dir.to_path_buf(),
        })
        .expect("filler occupies the single channel slot");

        let result = evict_to_budget(
            &mut db,
            &config,
            EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, None),
        );

        // Could not enqueue the spill → OOM surfaced, never a false success.
        assert!(
            result.is_err(),
            "a full spill channel must surface OOM, never silently succeed"
        );
        // CRUCIAL: the victim is still resident — no acknowledged-write data loss.
        assert_eq!(
            db.len(),
            1,
            "victim must NOT be removed from RAM when the spill send fails"
        );
        assert!(
            db.data().get(&b"k1"[..]).is_some(),
            "k1 must remain in the hot tier for retry on the next eviction tick"
        );
    }

    // ── Crash-window fix: write-then-durable-then-drop under appendonly=no ──
    //
    // Without an AOF backstop the async fast path's "queue-then-drop"
    // ordering is a genuine, unconditional data-loss bug: the value would
    // exist nowhere (not in RAM, not yet on disk, no AOF/WAL record) for the
    // whole `SpillThread` batching window. Pre-fix, `evict_one_async_spill`
    // had no `--appendonly` check at all and no way to spill synchronously,
    // so a successful `try_send` was immediately followed by `db.remove` —
    // exactly the crash window these tests lock shut.

    /// GREEN: under `--appendonly no`, eviction must perform a fully durable
    /// synchronous spill (pwrite + fsync + manifest commit) BEFORE dropping
    /// the hot value, and must make the key immediately cold-read-through-able
    /// (there is no later `SpillCompletion` on this path to do it for us).
    #[test]
    fn async_spill_no_aof_backstop_durably_spills_before_drop() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        let total = db.estimated_memory();

        let mut config = make_config(1, "allkeys-lru");
        config.appendonly = "no".to_string(); // no AOF backstop

        // Channel is never touched by the durable path -- bounded(1) with
        // nothing pre-filled proves that if the fast path ran instead, this
        // assertion would still pass, so we additionally assert `_rx` stayed
        // empty below to prove the queue was never used.
        let (tx, rx) = flume::bounded::<SpillRequest>(4096);

        let result = evict_to_budget(
            &mut db,
            &config,
            EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, Some(&mut manifest))
                .total(total)
                .budget(0),
        );
        assert!(
            result.is_ok(),
            "durable synchronous spill must succeed: {result:?}"
        );
        assert_eq!(db.len(), 0, "hot value dropped only after durable spill");
        assert!(
            rx.try_recv().is_err(),
            "the durable fallback must bypass the SpillThread channel entirely"
        );

        // The .mpf file must already be fsynced on disk -- the durability
        // barrier that has to complete before RAM is freed.
        let file_path = shard_dir.join("data/heap-000001.mpf");
        assert!(file_path.exists(), "spill file must be durable before drop");
        let pages = read_datafile(&file_path).unwrap();
        let entry = pages[0].get(0).unwrap();
        assert_eq!(entry.key, b"k1");
        assert_eq!(entry.value, b"v1");

        // A FRESH manifest load from disk (simulating post-crash recovery)
        // must already see the file -- the commit landed before drop.
        let reloaded = crate::persistence::manifest::ShardManifest::open(&manifest_path)
            .expect("manifest must be openable");
        assert!(
            reloaded.files().iter().any(|f| f.file_id == 1),
            "manifest commit must be durable before the hot value is dropped"
        );

        // No SpillCompletion will ever arrive for this eviction (no
        // SpillThread involved) -- ColdIndex must be populated inline or the
        // key is unreadable until the next full restart.
        assert!(
            db.cold_index.as_ref().unwrap().lookup(b"k1").is_some(),
            "key must be immediately cold-read-through-able after a durable sync spill"
        );

        // Requirement (4) regression guard (#257 class): a DEL of a key that
        // was just durably spilled by this new path must tombstone the cold
        // copy too, never leave it resurrectable via read-through. This
        // pins the fix's `db.remove()`-before-`ci.insert()` ordering (an
        // earlier draft of this fix got that backwards: inserting into
        // ColdIndex before `db.remove` let `Database::remove`'s own cold-tier
        // cleanup wipe the entry right back out; this test would have caught
        // the opposite bug too -- a DEL failing to clear a cold entry that
        // durable-spill inserted).
        db.remove(b"k1");
        assert!(
            db.cold_index.as_ref().unwrap().lookup(b"k1").is_none(),
            "DEL after a durable sync spill must not leave the key resurrectable from cold"
        );
    }

    /// RED→GREEN (GCP benchmark finding, 2026-07-10 -- policy-aware
    /// fail-close): when no `ShardManifest` is reachable (e.g. the inline
    /// per-connection write-path eviction gate) AND there is no AOF
    /// backstop, durable spill (`evict_batch_durable`) is impossible.
    /// That must NOT mean the cap goes unenforced for an evicting policy,
    /// though -- Redis semantics require `allkeys-lru` (and friends) to
    /// honor `--maxmemory` by DROPPING victims (no tiering) when no durable
    /// spill path exists; only `noeviction` may reject. Before this fix, the
    /// `manifest is None` branch OOM'd unconditionally regardless of policy,
    /// which silently broke the documented "Pure cache (no durability)"
    /// recipe (`--appendonly no --maxmemory ... --maxmemory-policy
    /// allkeys-lru`) -- a classic LRU cache rejected writes at the cap
    /// instead of evicting. This test reproduces exactly that regression:
    /// it FAILS (OOM, value retained) on the pre-fix code and PASSES
    /// (evicted, `Ok`) after.
    #[test]
    fn async_spill_no_aof_backstop_no_manifest_allkeys_lru_evicts_pure_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        let total = db.estimated_memory();

        // budget of 1 byte: always over, for any non-empty value.
        let mut config = make_config(1, "allkeys-lru");
        config.appendonly = "no".to_string();

        let (tx, _rx) = flume::bounded::<SpillRequest>(4096);

        let result = evict_to_budget(
            &mut db,
            &config,
            EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, None)
                .total(total)
                .budget(0),
        );

        assert!(
            result.is_ok(),
            "an evicting policy must reclaim budget by dropping, not OOM: {result:?}"
        );
        assert_eq!(
            db.len(),
            0,
            "victim must be dropped (no spill, no manifest) to honor the cap"
        );
        assert!(
            !shard_dir.join("data").exists(),
            "this is a plain drop, not a spill -- no file should be written"
        );
        assert!(
            _rx.try_recv().is_err(),
            "the plain-drop path must bypass the SpillThread channel entirely"
        );
    }

    /// GREEN (unchanged): `noeviction` rejects writes with OOM regardless of
    /// manifest/AOF availability -- there is no policy under which
    /// `noeviction` may drop a key. The periodic memory-pressure tick (which
    /// does have a manifest, when a durability backstop exists) is
    /// irrelevant here since `noeviction` never evicts anywhere.
    #[test]
    fn async_spill_no_aof_backstop_no_manifest_noeviction_still_rejects() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));

        let mut config = make_config(1, "noeviction");
        config.appendonly = "no".to_string();

        let (tx, _rx) = flume::bounded::<SpillRequest>(4096);

        let result = evict_to_budget(
            &mut db,
            &config,
            EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, None),
        );

        assert!(
            result.is_err(),
            "noeviction must surface OOM, never silently evict"
        );
        assert_eq!(db.len(), 1, "hot value must be retained under noeviction");
        assert!(db.data().get(&b"k1"[..]).is_some());
        // Nothing was queued either -- no half-done spill left dangling.
        assert!(_rx.try_recv().is_err());
        assert!(
            !shard_dir.join("data").exists(),
            "no spill file should have been written"
        );
    }

    /// GREEN: `volatile-lru` must only evict keys carrying a TTL -- a
    /// persistent key must never be dropped to satisfy this policy's cap,
    /// matching Redis's documented "volatile-*" scope. When no volatile
    /// victim exists (only a persistent key remains) the cap cannot be
    /// honored at all, so this must OOM rather than drop the persistent key.
    #[test]
    fn async_spill_no_aof_backstop_no_manifest_volatile_lru_scoped_to_ttl_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"persistent"), Bytes::from_static(b"v"));
        // Budget the persistent key alone comfortably fits under, but the
        // persistent+volatile total exceeds -- so eviction must stop exactly
        // once the volatile key is gone, without needing a "drop everything"
        // budget that would otherwise force a second (impossible) eviction
        // attempt against the persistent-only remainder within the same
        // internal `while current_total > budget` loop.
        let persistent_only_memory = db.estimated_memory();

        let future_ms = current_time_ms() + 3_600_000;
        db.set_string_with_expiry(
            Bytes::from_static(b"volatile"),
            Bytes::from_static(b"v"),
            future_ms,
        );
        let total_with_both = db.estimated_memory();
        assert!(
            total_with_both > persistent_only_memory,
            "adding the volatile key must increase estimated memory"
        );

        let mut config = make_config(persistent_only_memory, "volatile-lru");
        config.appendonly = "no".to_string();

        let (tx, _rx) = flume::bounded::<SpillRequest>(4096);

        let result = evict_to_budget(
            &mut db,
            &config,
            EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, None)
                .total(total_with_both)
                .budget(0),
        );

        assert!(result.is_ok(), "{result:?}");
        assert_eq!(db.len(), 1, "only the volatile key may be dropped");
        assert!(
            db.data().contains_key(b"persistent" as &[u8]),
            "persistent key must survive volatile-lru eviction"
        );
        assert!(!db.data().contains_key(b"volatile" as &[u8]));

        // Only the persistent key remains now: tighten the budget so it's
        // over-budget with no volatile candidate left to satisfy it -- the
        // cap cannot be honored, so this must OOM, never drop a persistent
        // key under a `volatile-*` policy.
        let mut config2 = make_config(1, "volatile-lru");
        config2.appendonly = "no".to_string();
        let current = db.estimated_memory();
        let result2 = evict_to_budget(
            &mut db,
            &config2,
            EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, None).total(current),
        );
        assert!(
            result2.is_err(),
            "volatile-lru with no volatile candidate must OOM, not drop a persistent key"
        );
        assert_eq!(db.len(), 1);
    }

    /// GREEN (spill-failure retention, no-AOF path): if the durable
    /// synchronous spill itself fails (I/O error), the hot value must be
    /// retained -- never silently dropped. Simulated by pointing `shard_dir`
    /// at a path whose parent is a regular file, so `create_dir_all("data")`
    /// fails.
    #[test]
    fn async_spill_no_aof_backstop_spill_failure_retains_hot_value() {
        let tmp = tempfile::tempdir().unwrap();
        let blocker_file = tmp.path().join("not_a_dir");
        std::fs::write(&blocker_file, b"x").unwrap();
        // shard_dir/data cannot be created because `shard_dir` itself is a
        // regular file, not a directory.
        let shard_dir = blocker_file.as_path();
        let manifest_path = tmp.path().join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        let total = db.estimated_memory();

        let mut config = make_config(1, "allkeys-lru");
        config.appendonly = "no".to_string();

        let (tx, _rx) = flume::bounded::<SpillRequest>(4096);

        let result = evict_to_budget(
            &mut db,
            &config,
            EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, Some(&mut manifest))
                .total(total)
                .budget(0),
        );

        assert!(
            result.is_err(),
            "spill I/O failure must surface OOM, never a false success"
        );
        assert_eq!(
            db.len(),
            1,
            "hot value must be retained when the durable spill fails"
        );
        assert!(db.data().get(&b"k1"[..]).is_some());
    }

    /// GREEN (bug #1 regression guard, sync path): `evict_one_with_spill`
    /// previously fell through to `db.remove` unconditionally even when
    /// `spill_to_datafile` failed, discarding the value with no copy
    /// anywhere -- real, unconditional data loss on every spill I/O error,
    /// independent of any crash. Same blocker-file trick as above.
    #[test]
    fn sync_spill_failure_retains_hot_value_no_silent_drop() {
        let tmp = tempfile::tempdir().unwrap();
        let blocker_file = tmp.path().join("not_a_dir");
        std::fs::write(&blocker_file, b"x").unwrap();
        let shard_dir = blocker_file.as_path();
        let manifest_path = tmp.path().join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.set_string(
            Bytes::from_static(b"spill_key"),
            Bytes::from_static(b"spill_val"),
        );

        let config = make_config(1, "allkeys-lru");
        let mut ctx = SpillContext {
            shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 0,
        };

        let result = evict_to_budget(&mut db, &config, EvictionRun::sync_spill(Some(&mut ctx)));
        assert!(
            result.is_err(),
            "spill I/O failure must surface OOM, never a false success"
        );
        assert_eq!(
            db.len(),
            1,
            "hot value must be retained when the sync spill fails (no silent drop)"
        );
        assert!(db.data().get(&b"spill_key"[..]).is_some());
    }

    /// GREEN (task #45 fix, was RED as `sync_spill_non_string_victim_reports_plain_drop`
    /// under the old `is_string` gate): `evict_one_with_spill`'s spill body
    /// now durably spills every `RedisValueRef` type via
    /// `kv_spill::spill_to_datafile` (which has fully supported collections
    /// since the async write-path eviction gate started using it). A Hash
    /// victim picked here while a `SpillContext` is present must therefore
    /// (a) write real bytes to `shard_dir`, (b) register a `ColdIndex` entry
    /// so `EXISTS`/`HGETALL`/etc. can find it again via
    /// `promote_cold_if_present`, and (c) NOT be reported to `on_plain_drop`
    /// (a spilled entry stays cold-readable, not deleted). Before this fix a
    /// Hash/List/Set/ZSet victim under a live `SpillContext` was silently
    /// plain-dropped with no cold copy anywhere -- the tick/cascade root
    /// cause of the `cold_collection_visibility` stable-ghost flake (task
    /// #45): `EXISTS` on a key with a stale cold-index reference to an OLDER
    /// spill could read `1` while every content read came back empty forever.
    #[test]
    fn sync_spill_non_string_victim_is_durably_spilled_not_plain_dropped() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = tmp.path().join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        // Hash victim: get_or_create_hash + a field write, matching how HSET
        // populates a real hash entry (mirrors src/storage/db.rs's own test
        // fixture pattern).
        {
            let h = db.get_or_create_hash(b"hash_key").unwrap();
            h.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        }
        assert_eq!(db.len(), 1);

        let config = make_config(1, "allkeys-lru"); // over-budget by construction
        let mut ctx = SpillContext {
            shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 0,
        };

        let mut reported: Vec<Vec<u8>> = Vec::new();
        let evicted = evict_one_with_spill(
            &mut db,
            &config,
            &EvictionPolicy::AllKeysLru,
            Some(&mut ctx),
            &mut |key| reported.push(key.to_vec()),
        );

        assert!(evicted, "the hash victim must still be evicted from RAM");
        assert_eq!(db.len(), 0);
        assert!(
            shard_dir.join("data").exists(),
            "P0 fix: a Hash victim under a live SpillContext must be durably \
             spilled to disk, not plain-dropped"
        );
        assert!(
            reported.is_empty(),
            "a durably-spilled Hash victim must NOT be reported to \
             on_plain_drop -- it stays cold-readable, not deleted: {reported:?}"
        );
        assert!(
            db.cold_index
                .as_ref()
                .unwrap()
                .lookup(b"hash_key")
                .is_some(),
            "the spilled Hash key must be registered in ColdIndex so EXISTS/ \
             promote_cold_if_present can find it again"
        );
    }

    /// RED→GREEN (W2): the sync-spill eviction path must batch victims into
    /// shared multi-entry files (one manifest commit per file), not one
    /// single-entry file + one blocking manifest commit per key. Before W2,
    /// evicting 40 keys through the sync spill path produced 40
    /// `heap-*.mpf` files and 40 shard-thread-blocking manifest fsync
    /// round-trips; the async path had already solved this (FLUSH_ENTRY_CAP
    /// batching) — the sync path re-derived its own per-key layout, the exact
    /// divergence class behind tasks #34/#45/#139.
    #[test]
    fn sync_spill_batches_victims_into_shared_files() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = tmp.path().join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        for i in 0..40u8 {
            db.set_string(
                Bytes::copy_from_slice(format!("batch_key_{i:02}").as_bytes()),
                Bytes::from_static(b"batch_value_payload"),
            );
        }
        assert_eq!(db.len(), 40);

        let config = make_config(1, "allkeys-lru"); // budget 1 byte: evict all
        let mut ctx = SpillContext {
            shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 0,
        };

        let result = evict_to_budget(&mut db, &config, EvictionRun::sync_spill(Some(&mut ctx)));
        assert!(result.is_ok(), "eviction must succeed: {result:?}");
        assert_eq!(db.len(), 0, "all victims must be evicted from RAM");

        // Every key must remain cold-readable.
        let ci = db.cold_index.as_ref().unwrap();
        for i in 0..40u8 {
            let key = format!("batch_key_{i:02}");
            assert!(
                ci.lookup(key.as_bytes()).is_some(),
                "{key} must be registered in ColdIndex after sync spill"
            );
        }

        // The batching claim itself: 40 small same-db victims must share
        // files (the async path packs ~256/file; assert a generous bound so
        // the test pins "batched", not an exact packing constant).
        let file_count = std::fs::read_dir(shard_dir.join("data"))
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".mpf"))
            .count();
        assert!(
            file_count <= 4,
            "sync spill must batch victims into shared files: \
             40 keys produced {file_count} .mpf files (pre-W2: 40)"
        );
    }

    /// RED→GREEN (W2): a victim whose value cannot be faithfully serialized
    /// (in-memory corruption, e.g. a sorted-set listpack score that does not
    /// parse — see `ValueCodecError::CorruptScore`) must be retained hot,
    /// fail-closed. Before W2 all three spill sites did
    /// `serialize_collection(..).unwrap_or_default()`: the victim was
    /// "spilled" as an EMPTY value body and evicted — silent, durable data
    /// loss (reload of the empty body yields a decode miss).
    #[test]
    fn sync_spill_unserializable_victim_retained_fail_closed() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = tmp.path().join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        // Inject a corrupt SortedSetListpack (unparseable score) the way a
        // memory-corruption event would leave it — unreachable via ZADD.
        let mut lp = crate::storage::listpack::Listpack::new();
        lp.push_back(b"member");
        lp.push_back(b"not-a-number");
        let mut entry = Entry::new_string(Bytes::new());
        entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
            crate::storage::RedisValue::SortedSetListpack(lp),
        );
        db.insert_for_load(Bytes::from_static(b"corrupt_zset"), entry);
        db.recalculate_memory();
        assert_eq!(db.len(), 1);

        let config = make_config(1, "allkeys-lru");
        let mut ctx = SpillContext {
            shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 0,
        };

        let mut reported: Vec<Vec<u8>> = Vec::new();
        let evicted = evict_one_with_spill(
            &mut db,
            &config,
            &EvictionPolicy::AllKeysLru,
            Some(&mut ctx),
            &mut |key| reported.push(key.to_vec()),
        );

        assert!(
            !evicted,
            "an unserializable victim must NOT be evicted (fail-closed): \
             pre-W2 it was spilled as an EMPTY value body and dropped from RAM"
        );
        assert_eq!(
            db.len(),
            1,
            "the corrupt value must be retained hot — no durable copy exists"
        );
        assert!(
            db.cold_index
                .as_ref()
                .unwrap()
                .lookup(b"corrupt_zset")
                .is_none(),
            "no ColdIndex entry may point at an empty/absent spill body"
        );
    }

    // ── moon#553: about-to-expire victims are dropped, not spilled ───────

    /// Seed one string key carrying `ttl_ms` from now, with the exact shape
    /// the eviction paths expect (hot entry + indexed TTL).
    fn db_with_ttl_key(key: &'static [u8], remaining_ms: u64) -> Database {
        let mut db = Database::new();
        let mut entry = crate::storage::Entry::new_string(Bytes::from_static(b"a-value"));
        entry.set_expires_at_ms(db.now_ms() + remaining_ms);
        db.set(Bytes::from_static(key), entry);
        db
    }

    /// RED before #553: a victim two seconds from expiry was serialized,
    /// pwritten, fsynced and manifest-committed into the cold tier, where
    /// nothing sweeps it. It must be dropped instead — same RAM reclaimed,
    /// zero IO.
    #[test]
    fn sync_spill_drops_a_victim_that_is_about_to_expire() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = db_with_ttl_key(b"dying", SPILL_TTL_FLOOR_MS / 2);
        db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        let config = make_config(1, "allkeys-lru");

        let mut dropped: Vec<Vec<u8>> = Vec::new();
        let mut ctx = SpillContext {
            shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 0,
        };
        let mut sink = |k: &[u8]| dropped.push(k.to_vec());
        let result = evict_to_budget(
            &mut db,
            &config,
            EvictionRun::sync_spill(Some(&mut ctx)).report(&mut sink),
        );

        assert!(result.is_ok(), "the budget must still be reclaimed");
        assert_eq!(db.len(), 0, "the victim must leave hot RAM either way");
        assert_eq!(
            manifest.files().len(),
            0,
            "no datafile may be written for a key that expires in \
             under {SPILL_TTL_FLOOR_MS}ms"
        );
        assert!(
            db.cold_index
                .as_ref()
                .is_none_or(|ci| ci.iter().count() == 0),
            "and no cold-index entry either"
        );
        assert_eq!(
            dropped,
            vec![b"dying".to_vec()],
            "the drop MUST be reported, or a replica keeps a key its master \
             has in neither tier and no expiry sweep can ever correct it"
        );
    }

    /// The guard must be narrow: a victim with real time left still spills.
    #[test]
    fn sync_spill_still_spills_a_victim_with_a_long_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = db_with_ttl_key(b"living", SPILL_TTL_FLOOR_MS * 60);
        db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        let config = make_config(1, "allkeys-lru");

        let mut dropped: Vec<Vec<u8>> = Vec::new();
        let mut ctx = SpillContext {
            shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 0,
        };
        let mut sink = |k: &[u8]| dropped.push(k.to_vec());
        evict_to_budget(
            &mut db,
            &config,
            EvictionRun::sync_spill(Some(&mut ctx)).report(&mut sink),
        )
        .expect("eviction succeeds");

        assert_eq!(db.len(), 0);
        assert_eq!(
            manifest.files().len(),
            1,
            "a long-lived victim must still reach the cold tier"
        );
        assert!(dropped.is_empty(), "a spilled key is not a dropped key");
    }

    /// A victim with NO TTL is never affected by the guard.
    #[test]
    fn sync_spill_still_spills_a_victim_with_no_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        db.set_string(Bytes::from_static(b"forever"), Bytes::from_static(b"v"));
        let config = make_config(1, "allkeys-lru");

        let mut ctx = SpillContext {
            shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 0,
        };
        evict_to_budget(&mut db, &config, EvictionRun::sync_spill(Some(&mut ctx)))
            .expect("eviction succeeds");
        assert_eq!(manifest.files().len(), 1);
    }

    /// The async (AOF-backstopped) path must take the same decision: a
    /// nearly-expired victim never reaches the `SpillThread` channel.
    #[test]
    fn async_spill_drops_a_victim_that_is_about_to_expire() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let mut next_file_id = 1u64;

        let mut db = db_with_ttl_key(b"dying", SPILL_TTL_FLOOR_MS / 2);
        let mut config = make_config(1, "allkeys-lru");
        config.appendonly = "yes".to_string();

        let (tx, rx) = flume::bounded::<SpillRequest>(8);
        let mut dropped: Vec<Vec<u8>> = Vec::new();
        let mut sink = |k: &[u8]| dropped.push(k.to_vec());
        let result = evict_to_budget(
            &mut db,
            &config,
            EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, None).report(&mut sink),
        );

        assert!(result.is_ok());
        assert_eq!(db.len(), 0, "RAM is reclaimed");
        assert_eq!(
            rx.len(),
            0,
            "no SpillRequest may be queued for a key about to expire"
        );
        assert_eq!(dropped, vec![b"dying".to_vec()], "the drop is reported");
        assert!(
            db.spill_inflight_is_empty(),
            "and no in-flight record is left behind"
        );
    }

    /// The async path's non-guarded victims still queue.
    #[test]
    fn async_spill_still_queues_a_victim_with_a_long_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let mut next_file_id = 1u64;

        let mut db = db_with_ttl_key(b"living", SPILL_TTL_FLOOR_MS * 60);
        let mut config = make_config(1, "allkeys-lru");
        config.appendonly = "yes".to_string();

        let (tx, rx) = flume::bounded::<SpillRequest>(8);
        evict_to_budget(
            &mut db,
            &config,
            EvictionRun::async_spill(&tx, shard_dir, &mut next_file_id, 0, None),
        )
        .expect("eviction succeeds");

        assert_eq!(rx.len(), 1, "a long-lived victim must still be queued");
    }

    /// The exact boundary: `SPILL_TTL_FLOOR_MS` of remaining life spills,
    /// one millisecond less does not.
    #[test]
    fn spill_ttl_floor_boundary_is_exclusive() {
        let db = db_with_ttl_key(b"k", SPILL_TTL_FLOOR_MS);
        let now = db.now_ms();
        let at_floor = db.data().get(&b"k"[..]).expect("seeded").clone();
        assert!(
            !expiring_too_soon_to_spill(&at_floor, now),
            "exactly at the floor still spills"
        );

        let db = db_with_ttl_key(b"k", SPILL_TTL_FLOOR_MS - 1);
        let now = db.now_ms();
        let below = db.data().get(&b"k"[..]).expect("seeded").clone();
        assert!(
            expiring_too_soon_to_spill(&below, now),
            "one millisecond below the floor drops"
        );
    }

    /// A budget made entirely of about-to-expire victims must still report
    /// PROGRESS — `evict_batch_durable` returning 0 because its spill buffer
    /// stayed empty would make `evict_to_budget` answer a spurious OOM.
    #[test]
    fn a_batch_of_only_dropped_victims_still_counts_as_progress() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        for i in 0..32u32 {
            let mut entry = crate::storage::Entry::new_string(Bytes::from_static(b"a-value"));
            entry.set_expires_at_ms(db.now_ms() + 10);
            db.set(Bytes::from(format!("k{i}")), entry);
        }
        let config = make_config(1, "allkeys-lru");

        let mut ctx = SpillContext {
            shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 0,
        };
        let result = evict_to_budget(&mut db, &config, EvictionRun::sync_spill(Some(&mut ctx)));

        assert!(
            result.is_ok(),
            "an all-drops batch must not be mistaken for 'cannot evict'"
        );
        assert_eq!(db.len(), 0);
        assert_eq!(manifest.files().len(), 0, "zero spill IO");
    }

    #[test]
    fn test_evict_without_spill_unchanged() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        db.set_string(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"));

        let config = make_config(1, "allkeys-random");
        let result = evict_to_budget(&mut db, &config, EvictionRun::sync_spill(None));
        assert!(result.is_ok());
        assert_eq!(db.len(), 0);
    }

    // ── B-2: post-crash cold-spill file_id seeding ──────────────────────────
    // The spill file_id counter must resume ABOVE every recovered `heap-*.mpf`
    // so post-restart re-eviction never overwrites a cold file the rebuilt
    // cold_index still points at (which silently corrupts cold read-through).

    /// `None` dir (disk-offload off) → seed 1 (historical default, no change).
    #[test]
    fn test_spill_seed_none_dir_is_one() {
        assert_eq!(next_spill_file_id_seed(None), 1);
    }

    /// Fresh server (dir absent / empty) → seed 1 == legacy behaviour, so the
    /// live read-through hot path cannot regress from this change.
    #[test]
    fn test_spill_seed_fresh_server_is_one() {
        let tmp = tempfile::tempdir().unwrap();
        // data/ does not exist yet
        assert_eq!(next_spill_file_id_seed(Some(tmp.path())), 1);
        // data/ exists but empty
        std::fs::create_dir_all(tmp.path().join("data")).unwrap();
        assert_eq!(next_spill_file_id_seed(Some(tmp.path())), 1);
    }

    /// With recovered heap files, seed = max(file_id) + 1 so the next batch's
    /// filename is strictly greater than any existing one (filenames are the
    /// first request id of each batch; gaps are harmless).
    #[test]
    fn test_spill_seed_resumes_above_max_recovered() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        for id in [1u64, 257, 513] {
            std::fs::write(data.join(format!("heap-{id:06}.mpf")), b"x").unwrap();
        }
        // non-heap files and bad names must be ignored
        std::fs::write(data.join("manifest.bin"), b"x").unwrap();
        std::fs::write(data.join("heap-notanum.mpf"), b"x").unwrap();
        std::fs::write(data.join("base-000999.rdb"), b"x").unwrap();
        assert_eq!(next_spill_file_id_seed(Some(tmp.path())), 514);
    }

    /// The lock-free inline-write pre-gate must skip the runtime-config lock
    /// ONLY when the slow path (`evict_to_budget`, no-op iff
    /// `total <= budget`) would provably do nothing. Tests the pure decision
    /// core — the public wrapper only adds three Relaxed loads (the two
    /// process-global hints plus the cached footprint correction), which race
    /// with `CONFIG SET maxmemory` tests in the parallel test binary and are
    /// therefore not asserted on here. Every case below pins the neutral
    /// correction 1.0; ratios > 1.0 are covered by
    /// `test_inline_pre_gate_agrees_with_slow_path_under_footprint_correction`.
    #[test]
    fn test_inline_write_can_skip_eviction_gate() {
        const UNPUB: usize = usize::MAX;

        // Unpublished sentinel (either hint needed) → fail safe: slow path.
        assert!(!can_skip_eviction(UNPUB, UNPUB, 0, 0, 1.0));
        assert!(!can_skip_eviction(UNPUB, 1000, 0, 0, 1.0));
        assert!(!can_skip_eviction(1000, UNPUB, 0, 0, 1.0)); // per-shard needed, missing

        // maxmemory == 0 (unlimited): slow path early-returns → provable skip,
        // even if the per-shard hint is missing.
        assert!(can_skip_eviction(0, UNPUB, usize::MAX, 0, 1.0));

        // maxmemory 1000, per-shard 1000 (1 shard), no elastic budget:
        // skip iff est <= 1000 (slow-path loop condition is `total > budget`).
        assert!(can_skip_eviction(1000, 1000, 1000, 0, 1.0));
        assert!(!can_skip_eviction(1000, 1000, 1001, 0, 1.0));

        // Elastic budget overrides per-shard, capped at instance maxmemory —
        // mirror of `budget_override.min(config.maxmemory)` in the slow path.
        // (per-shard hint irrelevant when elastic > 0, even unpublished.)
        assert!(can_skip_eviction(1000, UNPUB, 700, 800, 1.0));
        assert!(!can_skip_eviction(1000, UNPUB, 900, 800, 1.0));
        assert!(can_skip_eviction(1000, UNPUB, 999, 1500, 1.0)); // cap: min(1500,1000)
        assert!(!can_skip_eviction(1000, UNPUB, 1400, 1500, 1.0));

        // Multi-shard: per-shard budget = ceil(maxmemory / num_shards), as
        // published from `maxmemory_per_shard()` (its rounding has its own
        // tests in config.rs). 1000 across 4 shards → 250.
        let rt = RuntimeConfig {
            maxmemory: 1000,
            num_shards: 4,
            ..Default::default()
        };
        let per_shard = rt.maxmemory_per_shard();
        assert!(can_skip_eviction(1000, per_shard, 250, 0, 1.0));
        assert!(!can_skip_eviction(1000, per_shard, 251, 0, 1.0));
    }

    /// The slow path's own budget rule, derived from `evict_to_budget`:
    /// `budget_override > 0 ? min(override, maxmemory) : maxmemory_per_shard`,
    /// scaled by the footprint correction, and the loop no-ops iff
    /// `total <= budget`. This is the ORACLE the lock-free pre-gate must agree
    /// with — it is what actually decides whether a write is capped.
    fn slow_path_would_noop(
        mm: usize,
        per_shard: usize,
        est: usize,
        elastic: usize,
        ratio: f64,
    ) -> bool {
        if mm == 0 {
            return true; // `evict_to_budget` early-returns Ok(())
        }
        est <= effective_budget(elastic, mm, per_shard, ratio)
    }

    /// moon#475 residual: the inline write path's pre-gate MUST agree with the
    /// slow path once the OS-footprint correction is active.
    ///
    /// The pre-gate's contract is "true iff `evict_to_budget` would provably
    /// no-op". A `true` that the slow path disagrees with is a write admitted
    /// above the operator's effective `maxmemory` — silently, with no error and
    /// no log line. The pre-gate is reachable ONLY from
    /// `server::conn::try_inline_dispatch`, so a plain `SET` got the
    /// uncorrected cap while `HSET`/`EVAL` (the other two dispatch paths, which
    /// call `evict_to_budget` unconditionally) got the corrected one: one
    /// server, two effective caps.
    ///
    /// Every ratio here is > 1.0 — the case the original ~20 assertions never
    /// covered, which is exactly why the divergence shipped.
    #[test]
    fn test_inline_pre_gate_agrees_with_slow_path_under_footprint_correction() {
        let rt = RuntimeConfig {
            maxmemory: 1_000_000,
            num_shards: 4,
            ..Default::default()
        };
        let mm = rt.maxmemory;
        let per_shard = rt.maxmemory_per_shard();

        // Named regression: maxmemory 1000, correction 2.0 -> effective 500.
        // 800 bytes is OVER the corrected cap, so the slow path would act and
        // the pre-gate must NOT claim a provable no-op.
        assert!(
            !can_skip_eviction(1000, 1000, 800, 0, 2.0),
            "moon#475: pre-gate skipped eviction at est=800 against a \
             correction-scaled budget of 500 (maxmemory 1000 / ratio 2.0)"
        );

        // Exhaustive agreement sweep across the live ratio range. The
        // correction is clamped to [1.0, 8.0] by `footprint::ratio_from`, and
        // 2.3 is the ratio actually measured on the instance that motivated
        // PR #478.
        for &ratio in &[1.0f64, 1.05, 1.5, 2.0, 2.3, 4.0, 8.0] {
            for &elastic in &[0usize, 1, per_shard, mm / 2, mm, mm * 2] {
                for &est in &[
                    0usize,
                    1,
                    per_shard / 2,
                    per_shard,
                    per_shard + 1,
                    mm / 2,
                    mm,
                    mm + 1,
                ] {
                    let gate = can_skip_eviction(mm, per_shard, est, elastic, ratio);
                    let oracle = slow_path_would_noop(mm, per_shard, est, elastic, ratio);
                    assert_eq!(
                        gate, oracle,
                        "pre-gate/slow-path divergence: ratio={ratio} elastic={elastic} \
                         est={est} per_shard={per_shard} mm={mm} -> gate={gate} oracle={oracle}"
                    );
                }
            }
        }

        // The fail-safe sentinels stay fail-safe regardless of the ratio: an
        // unpublished hint must never be rescued into a `true` by a correction.
        for &ratio in &[1.0f64, 2.3, 8.0] {
            assert!(!can_skip_eviction(usize::MAX, usize::MAX, 0, 0, ratio));
            assert!(!can_skip_eviction(1000, usize::MAX, 0, 0, ratio));
            // maxmemory == 0 is unlimited on BOTH paths, correction irrelevant.
            assert!(can_skip_eviction(0, usize::MAX, usize::MAX, 0, ratio));
        }
    }

    // ── W4: evict_to_budget (single entry point) behavior pins ────────────

    /// Plain sink: noeviction rejects with OOM; an evicting policy drops
    /// victims to budget and reports every plain-dropped key.
    #[test]
    fn evict_to_budget_plain_honors_policy_and_reports() {
        // noeviction: OOM, nothing removed.
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        let config = make_config(1, "noeviction");
        let result = evict_to_budget(&mut db, &config, EvictionRun::plain());
        assert!(result.is_err(), "noeviction over budget must OOM");
        assert_eq!(db.len(), 1, "noeviction must not remove keys");

        // allkeys-lru: drops to budget, reporting each plain-dropped key.
        let mut db = Database::new();
        for i in 0..8u8 {
            db.set_string(
                Bytes::copy_from_slice(format!("pk{i}").as_bytes()),
                Bytes::from_static(b"value_payload"),
            );
        }
        let config = make_config(1, "allkeys-lru");
        let mut dropped: Vec<Vec<u8>> = Vec::new();
        let mut sink = |k: &[u8]| dropped.push(k.to_vec());
        let result = evict_to_budget(&mut db, &config, EvictionRun::plain().report(&mut sink));
        assert!(result.is_ok(), "evicting policy must reclaim: {result:?}");
        assert_eq!(db.len(), 0, "budget 1 byte: everything must go");
        assert_eq!(dropped.len(), 8, "every plain-dropped victim reported");
    }

    /// Sync-spill sink: delegates to the durable batch writer — victims land
    /// in shared spill files and stay cold-readable (same pin as the W2
    /// `sync_spill_batches_victims_into_shared_files` test, but through the
    /// single entry point).
    #[test]
    fn evict_to_budget_sync_spill_delegates_to_batch() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = tmp.path().join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        for i in 0..40u8 {
            db.set_string(
                Bytes::copy_from_slice(format!("w4_key_{i:02}").as_bytes()),
                Bytes::from_static(b"batch_value_payload"),
            );
        }
        let config = make_config(1, "allkeys-lru");
        let mut ctx = SpillContext {
            shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 0,
        };
        let result = evict_to_budget(&mut db, &config, EvictionRun::sync_spill(Some(&mut ctx)));
        assert!(result.is_ok(), "sync spill must succeed: {result:?}");
        assert_eq!(db.len(), 0, "all victims evicted from RAM");
        let ci = db.cold_index.as_ref().unwrap();
        for i in 0..40u8 {
            let key = format!("w4_key_{i:02}");
            assert!(
                ci.lookup(key.as_bytes()).is_some(),
                "{key} must be cold-readable after spill"
            );
        }
        let spill_files = std::fs::read_dir(shard_dir.join("data"))
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "mpf")
            })
            .count();
        assert!(
            spill_files <= 4,
            "40 victims must batch into shared files, got {spill_files}"
        );
    }

    /// An elastic budget override wider than usage suppresses eviction; the
    /// override is capped at the instance maxmemory.
    #[test]
    fn evict_to_budget_budget_override_respected() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"bk"), Bytes::from_static(b"bv"));
        let used = db.estimated_memory();

        // Override comfortably above usage: nothing evicted.
        let config = make_config(used * 4, "allkeys-lru");
        let result = evict_to_budget(
            &mut db,
            &config,
            EvictionRun::plain().total(used).budget(used * 2),
        );
        assert!(result.is_ok());
        assert_eq!(db.len(), 1, "override above usage must not evict");

        // Override is capped at maxmemory: an override far above the
        // instance cap cannot widen the budget beyond maxmemory.
        let config = make_config(1, "noeviction");
        let result = evict_to_budget(
            &mut db,
            &config,
            EvictionRun::plain().total(used).budget(usize::MAX),
        );
        assert!(
            result.is_err(),
            "override must clamp to maxmemory (1 byte) and OOM under noeviction"
        );
    }

    // ── moon#600: the stall backstop, independent of any sampler ──────────
    //
    // The PR that added this made TWO claims: the sampler is now correct, and
    // the loop terminates REGARDLESS of the sampler. Reverting the sampler
    // proves the first (four tests below go red). Nothing proved the second:
    // with the backstop disabled and the sampler intact, the whole eviction
    // suite still passed, because no sampler can reach the backstop today.
    // That is precisely why it needs its own test — a defence for a bug that
    // does not exist yet is worthless if it rots unnoticed before that bug
    // arrives.

    /// A probe with all three fields distinct from `base`'s, so each field
    /// can be moved one at a time.
    fn probe_at(memory: usize, pending_spill: usize, hot_keys: usize) -> EvictionProgress {
        EvictionProgress {
            memory,
            pending_spill,
            hot_keys,
        }
    }

    /// Fires on the LIMIT-th consecutive no-op, and not one iteration sooner.
    /// An off-by-one either way is a real defect: too eager converts a slow
    /// reclaim into a spurious `OOM`, too lax leaves the shard spinning
    /// longer than the constant promises.
    #[test]
    fn eviction_stall_backstop_fires_exactly_on_the_limit() {
        let p = probe_at(100, 0, 10);
        let mut stalled = 0usize;
        for i in 1..EVICTION_STALL_LIMIT {
            assert!(
                !EvictionProgress::note_iteration(p, p, &mut stalled),
                "no-op #{i} must not trip the backstop before                  EVICTION_STALL_LIMIT ({EVICTION_STALL_LIMIT})"
            );
        }
        assert!(
            EvictionProgress::note_iteration(p, p, &mut stalled),
            "the {EVICTION_STALL_LIMIT}th consecutive no-op must answer OOM              rather than spin the shard thread"
        );
    }

    /// ANY of the three fields moving is progress. A bytes-only test would
    /// call async spill (moon#466) stalled — it moves a value from the hot
    /// plane to the pending plane and leaves `estimated_memory` flat — and
    /// convert a working reclaim into a rejection.
    #[test]
    fn eviction_stall_backstop_counts_each_field_as_progress() {
        let base = probe_at(100, 50, 10);
        for (label, moved) in [
            ("memory", probe_at(99, 50, 10)),
            ("pending_spill", probe_at(100, 51, 10)),
            ("hot_keys", probe_at(100, 50, 9)),
        ] {
            let mut stalled = EVICTION_STALL_LIMIT - 1;
            assert!(
                !EvictionProgress::note_iteration(base, moved, &mut stalled),
                "{label} moving is progress; the backstop must not fire"
            );
            assert_eq!(
                stalled, 0,
                "{label} moving must RESET the counter, not merely spare this                  iteration — otherwise slow reclaim accumulates into a false OOM"
            );
        }
    }

    /// The counter is CONSECUTIVE. A reclaim that frees nothing on most
    /// iterations but genuinely progresses now and then must never be
    /// rejected, however long it runs.
    #[test]
    fn eviction_stall_backstop_never_fires_on_intermittent_progress() {
        let rounds = EVICTION_STALL_LIMIT * 4;
        let mut stalled = 0usize;
        // One real victim per round, so the starting count must outlast them.
        let mut hot = rounds + 1;
        for round in 0..rounds {
            // LIMIT-1 no-ops, then one real victim, forever.
            for _ in 0..(EVICTION_STALL_LIMIT - 1) {
                let p = probe_at(100, 0, hot);
                assert!(
                    !EvictionProgress::note_iteration(p, p, &mut stalled),
                    "round {round}: intermittent progress must never OOM"
                );
            }
            let before = probe_at(100, 0, hot);
            hot -= 1;
            let after = probe_at(100, 0, hot);
            assert!(!EvictionProgress::note_iteration(
                before,
                after,
                &mut stalled
            ));
            assert_eq!(stalled, 0, "round {round}: real progress must reset");
        }
    }

    // ── moon#600: index-only volatile-ttl victims ─────────────────────────

    /// Build the "index-only victim" state: an `expiry_index` pair whose hot
    /// entry is absent from `data`. `volatile-ttl` is the only sampler that
    /// reads a maintained index instead of `data` itself, so it is the only
    /// one that can hand `evict_to_budget` a key it cannot remove.
    ///
    /// The ghost pair is given the SMALLEST possible deadline (`1`) so it is
    /// the index head — exactly what `peek_nearest_expiry` returns — and
    /// therefore the victim `select_victim` keeps re-picking.
    fn db_with_index_only_victim(live_keys: usize) -> Database {
        let mut db = Database::new();
        let far_ms = current_time_ms() + 3_600_000;
        for i in 0..live_keys {
            db.set(
                Bytes::from(format!("live_{i:03}")),
                Entry::new_string_with_expiry(
                    Bytes::from_static(b"a_reasonably_sized_value_payload"),
                    far_ms + i as u64,
                ),
            );
        }
        // The ghost: indexed, never present in `data`.
        db.expiry_index_insert(1, b"ghost");
        assert!(
            db.data().get(&b"ghost"[..]).is_none(),
            "ghost must exist ONLY in the expiry index"
        );
        db
    }

    /// moon#600 (sampler): the volatile-ttl sampler must not return a key
    /// that is not in the hot plane. A pair whose entry is gone is provably
    /// stale — the same predicate `expire_cycle` uses — so it is reaped and
    /// the walk continues to a real victim.
    #[test]
    fn volatile_ttl_sampler_reaps_index_only_pair() {
        let mut db = db_with_index_only_victim(4);
        let victim =
            find_victim_volatile_ttl(&mut db).expect("a live volatile key exists behind the ghost");
        assert_ne!(
            victim.as_bytes(),
            b"ghost",
            "sampler must never return a key absent from the hot plane"
        );
        assert!(
            db.data().get(victim.as_bytes()).is_some(),
            "the returned victim must be removable"
        );
        assert_eq!(
            db.expiry_index_len(),
            4,
            "the stale pair must be REAPED, not merely skipped, or every \
             later peek pays for it again"
        );
        assert!(
            db.debug_expiry_index_consistent(),
            "reaping must move the index toward consistency"
        );
    }

    /// moon#600 (sampler, degenerate): a database whose expiry index holds
    /// NOTHING but ghosts must answer `None` — not a phantom victim — and
    /// must leave the index empty rather than re-walking the same pairs.
    #[test]
    fn volatile_ttl_sampler_all_ghosts_yields_none() {
        let mut db = Database::new();
        for i in 0..8u64 {
            db.expiry_index_insert(i + 1, format!("ghost_{i}").as_bytes());
        }
        assert!(
            find_victim_volatile_ttl(&mut db).is_none(),
            "no hot volatile key exists: the sampler must give up, not \
             invent a victim"
        );
        assert_eq!(db.expiry_index_len(), 0, "every ghost reaped exactly once");
    }

    /// moon#600 (liveness, the defect): `evict_to_budget` under
    /// `volatile-ttl` must TERMINATE when the index head is a ghost.
    ///
    /// Before the fix this loop never exits: `evict_one_with_spill` returns
    /// `true` (progress claimed) while `db.remove` removed nothing, so
    /// `before == after`, `current_total` is unchanged, and the next
    /// iteration peeks the very same head pair. The watchdog turns that
    /// spin into a FAILURE instead of a hung test binary.
    #[test]
    fn evict_to_budget_volatile_ttl_index_only_victim_terminates() {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut db = db_with_index_only_victim(8);
            let config = make_config(1, "volatile-ttl");
            let mut dropped: Vec<Vec<u8>> = Vec::new();
            let mut sink = |k: &[u8]| dropped.push(k.to_vec());
            let result = evict_to_budget(&mut db, &config, EvictionRun::plain().report(&mut sink));
            let _ = tx.send((
                result.is_ok(),
                db.len(),
                db.expiry_index_len(),
                dropped.len(),
            ));
        });
        let (ok, len, index_len, reported) = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("evict_to_budget must terminate: an index-only victim spun the shard thread");
        assert!(
            ok,
            "8 live volatile keys are evictable: must not answer OOM"
        );
        assert_eq!(len, 0, "budget of 1 byte: every live key must go");
        assert_eq!(index_len, 0, "ghost reaped, live pairs unindexed by remove");
        assert_eq!(
            reported, 8,
            "only the 8 REAL removals are reported downstream"
        );
    }

    /// moon#600 (backstop, non-vacuity): the loop's stall probe must watch
    /// all THREE signals, not just bytes.
    ///
    /// A bytes-only probe would judge the async-spill path (moon#466)
    /// stalled — it moves a value from the hot plane to the pending plane,
    /// which `estimated_memory()` counts on both sides — and after
    /// `EVICTION_STALL_LIMIT` victims would convert a working reclaim into
    /// an `OOM`. This pins the composition so that regression cannot pass.
    #[test]
    fn eviction_progress_probe_watches_bytes_pending_and_hot_keys() {
        use crate::persistence::kv_page::ValueType;
        use crate::storage::db::PendingSpill;

        let mut db = Database::new();
        for i in 0..4u8 {
            db.set_string(
                Bytes::copy_from_slice(format!("pp{i}").as_bytes()),
                Bytes::from_static(b"probe_value_payload"),
            );
        }

        let base = EvictionProgress::probe(&db);
        assert_eq!(base.memory, db.estimated_memory());
        assert_eq!(base.pending_spill, db.pending_spill_bytes());
        assert_eq!(base.hot_keys, db.data().len());
        assert_eq!(
            EvictionProgress::probe(&db),
            base,
            "an untouched database must read as NO progress — that is the \
             only condition allowed to trip the stall counter"
        );

        // Signal 1+3: a hot-plane removal.
        db.remove(b"pp0");
        let removed = EvictionProgress::probe(&db);
        assert_ne!(removed, base, "a removal is progress");
        assert!(removed.hot_keys < base.hot_keys);

        // Signal 2: a spill accepted onto the pending plane. This is the one
        // a bytes-only probe gets wrong.
        db.spill_inflight_mark(
            Bytes::from_static(b"pp1"),
            PendingSpill {
                req_id: 1,
                value_type: ValueType::String,
                value_bytes: Bytes::from_static(b"probe_value_payload"),
                ttl_ms: None,
            },
        );
        let queued = EvictionProgress::probe(&db);
        assert_ne!(queued, removed, "queuing a spill is progress");
        assert!(
            queued.pending_spill > removed.pending_spill,
            "the pending plane must be part of the progress observation"
        );
    }

    /// moon#600 under `--disk-offload enable` (sync-spill sink): the durable
    /// batch path re-picks the identical index head every time, trips
    /// `stall >= NO_AOF_BATCH_STALL_LIMIT`, and returns 0 — which
    /// `evict_to_budget` reads as "no progress" and answers OOM even though
    /// eight live volatile keys were spillable. With the sampler fixed the
    /// ghost is reaped and those keys are TIERED, not rejected.
    #[test]
    fn evict_to_budget_volatile_ttl_index_only_victim_still_spills() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let manifest_path = dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = db_with_index_only_victim(8);
        db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        let config = make_config(1, "volatile-ttl");
        let mut ctx = SpillContext {
            shard_dir: dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 0,
        };
        let result = evict_to_budget(&mut db, &config, EvictionRun::sync_spill(Some(&mut ctx)));
        assert!(
            result.is_ok(),
            "live volatile keys are spillable: {result:?}"
        );
        assert_eq!(db.len(), 0, "every live victim left RAM");
        let ci = db.cold_index.as_ref().unwrap();
        for i in 0..8usize {
            let key = format!("live_{i:03}");
            assert!(
                ci.lookup(key.as_bytes()).is_some(),
                "{key} must stay cold-readable: a tiered key is NOT an eviction"
            );
        }
        assert!(
            ci.lookup(&b"ghost"[..]).is_none(),
            "the ghost was never in RAM and must not be conjured into the cold tier"
        );
        assert_eq!(db.expiry_index_len(), 0, "ghost reaped");
    }

    /// moon#600 + `noeviction`: an index-only victim must not change the
    /// `noeviction` contract. The policy gate runs before any victim is
    /// selected, so the answer is still an immediate OOM with nothing
    /// removed and — importantly — the stale pair left alone: `noeviction`
    /// promises not to mutate the keyspace, and reaping is reachable only
    /// through a sampler `noeviction` never runs.
    #[test]
    fn noeviction_with_index_only_victim_ooms_without_touching_anything() {
        let mut db = db_with_index_only_victim(4);
        let before = db.len();
        let config = make_config(1, "noeviction");
        let result = evict_to_budget(&mut db, &config, EvictionRun::plain());
        assert!(result.is_err(), "noeviction over budget must OOM");
        assert_eq!(db.len(), before, "noeviction must not remove keys");
        assert_eq!(
            db.expiry_index_len(),
            before + 1,
            "noeviction never selects a victim, so nothing — not even a stale \
             pair — is reaped"
        );
    }
}

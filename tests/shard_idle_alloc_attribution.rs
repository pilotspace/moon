//! Attribution harness for moon's **per-shard idle allocation** cost.
//!
//! # Why this file exists
//!
//! moon's idle RSS at `--shards 8` measured 15.26 MiB against Redis
//! `--io-threads 8`'s 12.08 MiB, while moon at `--shards 1` (11.68 MiB)
//! already BEATS Redis (11.96 MiB). The entire penalty is therefore the seven
//! extra shards: ~524 KB of resident memory per additional shard. Only the
//! `ChannelMesh` term had ever been *predicted* (never measured), and it
//! accounted for under a quarter of that.
//!
//! This harness replaces prediction with measurement. It installs a counting
//! global allocator (integration-test crates may do this; the `moon` library
//! declares none — only `src/main.rs` does) and takes live-bytes deltas across
//! the constructors that run once per shard at boot for the configuration the
//! RSS numbers were taken under:
//!
//! ```text
//! moon --shards N --appendonly no --disk-offload disable
//! ```
//!
//! Under that configuration `spill_thread`, `shard_manifest` and the AOF
//! writer are all gated OFF, so they are deliberately NOT part of the model.
//!
//! # What is host-independent here, and what is not
//!
//! Allocation **counts and requested sizes are structural**: they are the same
//! on macOS and Linux, because they come from the same `Vec::with_capacity` /
//! `HeapRb::new` calls. Everything this file asserts is one of those. It
//! asserts **no RSS number** — RSS depends on the allocator's retention policy
//! (jemalloc's `background_thread` does not exist on macOS at all) and must be
//! confirmed on Linux.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

// ── Counting allocator ────────────────────────────────────────────────────────

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE_BLOCKS: AtomicUsize = AtomicUsize::new(0);

struct Counting;

// SAFETY: every method forwards verbatim to `System`, which is a sound
// `GlobalAlloc`. The only added work is relaxed arithmetic on two counters,
// which neither produces, consumes, nor reinterprets any pointer, so the
// safety contract this impl must uphold is exactly `System`'s and is
// discharged by delegating to it unchanged.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` is forwarded unchanged to the system allocator,
        // whose contract is identical to the one our caller already upheld.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            LIVE_BLOCKS.fetch_add(1, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        LIVE_BLOCKS.fetch_sub(1, Ordering::Relaxed);
        // SAFETY: `ptr`/`layout` are forwarded unchanged; the caller already
        // guarantees the pair came from this allocator with this layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as `alloc` — unchanged forward to the system allocator.
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            LIVE_BLOCKS.fetch_add(1, Ordering::Relaxed);
        }
        p
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded unchanged; the caller upholds `System`'s contract.
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(new_size, Ordering::Relaxed);
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Live (bytes, blocks) still held after `f` returns its value.
///
/// The value is kept alive across the second sample and dropped afterwards, so
/// the delta is *retained* memory, not transient churn.
fn live_of<T>(f: impl FnOnce() -> T) -> (isize, isize) {
    let b0 = LIVE_BYTES.load(Ordering::Relaxed) as isize;
    let n0 = LIVE_BLOCKS.load(Ordering::Relaxed) as isize;
    let v = f();
    let b1 = LIVE_BYTES.load(Ordering::Relaxed) as isize;
    let n1 = LIVE_BLOCKS.load(Ordering::Relaxed) as isize;
    drop(v);
    (b1 - b0, n1 - n0)
}

// ── The model ─────────────────────────────────────────────────────────────────

/// Shard counts the marginal cost is fitted across.
const LOW: usize = 1;
const HIGH: usize = 8;

/// `--databases` default (SELECT 0-15).
const DB_COUNT: usize = 16;

/// Marginal bytes per additional shard for `f(n)`, fitted between `LOW` and
/// `HIGH`. This is the number that matters: shard 0 is free (moon at
/// `--shards 1` already beats Redis), every shard after it is the loss.
fn marginal_per_shard<T>(f: impl Fn(usize) -> T) -> f64 {
    let (lo, _) = live_of(|| f(LOW));
    let (hi, _) = live_of(|| f(HIGH));
    (hi - lo) as f64 / (HIGH - LOW) as f64
}

/// Everything the boot path allocates once per shard, for
/// `--appendonly no --disk-offload disable` with default cargo features.
///
/// Deliberately EXCLUDED, because they are gated off in this configuration and
/// including them would inflate the model against a measurement taken without
/// them:
///   * `create_admin_channels` — `#[cfg(feature = "console")]`, and `console`
///     is not in the default feature set.
///   * `create_aof_fold_channels` — only reached inside
///     `if let Some(aof_pool)`, and `aof_pool` is `None` without `--appendonly
///     yes` / `--save`.
///   * WAL writer, spill thread, page cache, cold index, checkpoint manager —
///     all behind `appendonly_enabled` / `disk_offload_enabled()`.
///
/// Kept in ONE closure so the fit sees the true marginal cost of a shard,
/// including the mesh term that is quadratic in `n`.
fn per_shard_boot_state(n: usize) -> Box<dyn std::any::Any> {
    use moon::shard::mesh;

    let mesh = mesh::ChannelMesh::new(n, mesh::CHANNEL_BUFFER_SIZE);
    let db_sets: Vec<Vec<moon::storage::Database>> = (0..n)
        .map(|_| {
            (0..DB_COUNT)
                .map(|_| moon::storage::Database::new())
                .collect()
        })
        .collect();
    let stores: Vec<_> = (0..n)
        .map(|_| {
            (
                moon::vector::store::VectorStore::new(),
                moon::text::store::TextStore::new(),
                moon::transaction::KvWriteIntents::new(),
            )
        })
        .collect();
    Box::new((mesh, db_sets, stores))
}

#[test]
fn per_shard_idle_allocation_is_attributed_and_budgeted() {
    use moon::shard::dispatch::ShardMessage;
    use moon::shard::mesh;

    let msg_size = std::mem::size_of::<ShardMessage>();

    // ── Term 1: the SPSC mesh ────────────────────────────────────────────────
    // The ONE term that had a published prediction: N(N-1) rings x 256 slots
    // x 64 B = 128 KB per extra shard at s8. Measure it; do not assume it.
    let mesh_marginal =
        marginal_per_shard(|n| mesh::ChannelMesh::new(n, mesh::CHANNEL_BUFFER_SIZE));

    // ── Term 2: the 16 databases every shard gets ────────────────────────────
    let db_marginal = marginal_per_shard(|n| {
        (0..n)
            .map(|_| {
                (0..DB_COUNT)
                    .map(|_| moon::storage::Database::new())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    });
    let (one_db_bytes, one_db_blocks) = live_of(moon::storage::Database::new);

    // ── Term 3: the per-shard engine stores ──────────────────────────────────
    let (vec_bytes, _) = live_of(moon::vector::store::VectorStore::new);
    let (text_bytes, _) = live_of(moon::text::store::TextStore::new);
    let (intents_bytes, _) = live_of(moon::transaction::KvWriteIntents::new);
    let stores_marginal = vec_bytes + text_bytes + intents_bytes;

    // ── Whole-model marginal ─────────────────────────────────────────────────
    let total_marginal = marginal_per_shard(per_shard_boot_state);
    let attributed = mesh_marginal + db_marginal + stores_marginal as f64;
    let residue = total_marginal - attributed;

    // Slab occupancy of the empty tables behind those Databases.
    let table: moon::storage::dashtable::DashTable<moon::storage::compact_key::CompactKey, u64> =
        moon::storage::dashtable::DashTable::new();
    let (live_segments, reserved_slots) = (table.segment_count(), table.reserved_segment_slots());

    println!("\n=== per-shard idle allocation attribution ===========================");
    println!("Config modelled: --shards N --appendonly no --disk-offload disable,");
    println!("default features (console OFF, so no admin channels; aof_pool None).");
    println!("STRUCTURAL (allocation sizes/counts) — host-independent.");
    println!("NOT an RSS measurement: reserved bytes are an UPPER BOUND on resident,");
    println!("because untouched pages of a fresh mapping are never resident. Every");
    println!("RSS claim derived from this table needs Linux confirmation.");
    println!("---------------------------------------------------------------------");
    println!("size_of::<ShardMessage>()            = {msg_size:>10} B");
    println!(
        "CHANNEL_BUFFER_SIZE                  = {:>10}",
        mesh::CHANNEL_BUFFER_SIZE
    );
    println!(
        "one SPSC ring                        = {:>10} B",
        msg_size * mesh::CHANNEL_BUFFER_SIZE
    );
    println!(
        "rings at s{HIGH}  (N(N-1))                = {:>10}",
        HIGH * (HIGH - 1)
    );
    println!("empty DashTable: live segments       = {live_segments:>10}");
    println!("empty DashTable: reserved slots      = {reserved_slots:>10}");
    println!("---------------------------------------------------------------------");
    println!("ChannelMesh::new    marginal/shard   = {mesh_marginal:>10.0} B");
    println!("{DB_COUNT} Databases       marginal/shard   = {db_marginal:>10.0} B");
    println!(
        "  one Database                       = {one_db_bytes:>10} B in {one_db_blocks} blocks"
    );
    println!("VectorStore + TextStore + Intents    = {stores_marginal:>10} B");
    println!("---------------------------------------------------------------------");
    println!("MODEL total         marginal/shard   = {total_marginal:>10.0} B");
    println!("attributed above                     = {attributed:>10.0} B");
    println!("unexplained residue                  = {residue:>10.0} B");
    println!("=====================================================================\n");

    // ── Confirmation by measurement, not arithmetic ──────────────────────────
    // The mesh really does allocate N(N-1) rings of CHANNEL_BUFFER_SIZE slots.
    // The ring ARRAY is the model; ringbuf adds a small per-ring header and the
    // mesh adds per-shard Vec/Notify/flume bookkeeping, so the measured figure
    // sits slightly above the array term and must never sit below it.
    let ring_array_total =
        (HIGH * (HIGH - 1)) as f64 * mesh::CHANNEL_BUFFER_SIZE as f64 * msg_size as f64;
    let measured_mesh_total = live_of(|| mesh::ChannelMesh::new(HIGH, mesh::CHANNEL_BUFFER_SIZE)).0
        as f64
        - live_of(|| mesh::ChannelMesh::new(1, mesh::CHANNEL_BUFFER_SIZE)).0 as f64;
    let ratio = measured_mesh_total / ring_array_total;
    assert!(
        (1.0..1.10).contains(&ratio),
        "mesh must allocate the N(N-1) x cap x size_of ring array and little else: \
         array term {ring_array_total:.0} B, measured {measured_mesh_total:.0} B (ratio {ratio:.3})"
    );
    assert_eq!(
        msg_size, 64,
        "the ring slot size the mesh model is built on"
    );

    // ── The regression gate ──────────────────────────────────────────────────
    // Everything above the first shard is the s8 idle loss against Redis:
    // moon at --shards 1 already wins. The 16 empty Databases used to reserve
    // 884,736 B/shard here — 84% of the whole per-shard total and four times
    // the mesh — for segments that never exist on an idle server.
    const BUDGET_PER_SHARD: f64 = 256.0 * 1024.0;
    assert!(
        total_marginal < BUDGET_PER_SHARD,
        "per-shard idle allocation is {total_marginal:.0} B, over the {BUDGET_PER_SHARD:.0} B \
         budget. Attribution printed above."
    );

    // The mesh must now be the LARGEST per-shard term. If a storage-side term
    // overtakes it again, that is the regression this file exists to catch.
    assert!(
        mesh_marginal > db_marginal,
        "the SPSC mesh ({mesh_marginal:.0} B/shard) should dominate the 16 empty \
         Databases ({db_marginal:.0} B/shard); a storage term this large on an EMPTY \
         server is over-provisioning"
    );
}

/// The mesh is `N(N-1)` rings, so its footprint is **quadratic in shard count**.
/// At the shard counts moon ships at this is affordable; it stops being
/// affordable quickly, and nothing else in the tree records that.
///
/// This test does not change the constant — the ring depth sits on the
/// cross-shard path, and `docs/internal/cross-shard-cost-model.md` requires a
/// Linux throughput A/B before that path is touched. It pins the cost model so
/// a future change to `CHANNEL_BUFFER_SIZE` or to `size_of::<ShardMessage>()`
/// cannot silently multiply it by `N(N-1)`.
#[test]
fn mesh_footprint_is_quadratic_and_pinned() {
    use moon::shard::mesh;

    let slot = std::mem::size_of::<moon::shard::dispatch::ShardMessage>();
    let ring = slot * mesh::CHANNEL_BUFFER_SIZE;
    let total = |n: usize| n * (n - 1) * ring;

    println!("\n=== SPSC mesh footprint vs shard count (N(N-1) rings) ===");
    for n in [1usize, 2, 4, 8, 16, 32, 64] {
        println!(
            "  s{n:<3} {:>5} rings  {:>12} B  ({:>7.2} MiB)  {:>9} B/extra-shard",
            n * (n - 1),
            total(n),
            total(n) as f64 / (1024.0 * 1024.0),
            if n > 1 { total(n) / (n - 1) } else { 0 }
        );
    }
    println!("=========================================================\n");

    // The published per-shard figure at s8, confirmed against the measured
    // allocation in the attribution test above.
    assert_eq!(ring, 16_384, "one SPSC ring");
    assert_eq!(total(8) / 7, 131_072, "128 KiB per extra shard at s8");

    // The ceiling this file exists to keep visible. `total(32)` is 15.5 MiB —
    // larger than moon's ENTIRE idle RSS at s8. Raising the depth or the
    // message size multiplies that by N(N-1), not by N.
    assert!(
        total(32) <= 16 * 1024 * 1024,
        "the mesh at s32 is {} B; it was 15.5 MiB when this bound was written, and \
         anything above 16 MiB means the ring depth or ShardMessage size grew. \
         Making the mesh linear needs depth ∝ 1/N — `clamp(2048 / N, 32, 256)` is \
         byte-identical for every N <= 8 and bounds s32 at 4.06 MiB — but that is a \
         cross-shard-path change and needs a Linux throughput A/B first.",
        total(32)
    );
}

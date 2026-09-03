//! Allocator-shaped size accounting (moon#788).
//!
//! `used_memory` is the ledger behind the global `--maxmemory` gate and the
//! per-db quotas. Every byte it reports must correspond to a byte the
//! allocator actually handed out — a "cost constant" that does not match a
//! real allocation is a guess wearing a number's clothes, and moon#788
//! measured what that costs: on Linux the container types under-reported by
//! up to 5.75x, so an operator who set `--maxmemory` as an OOM guard could
//! not make it bind.
//!
//! Two facts drive everything here:
//!
//! 1. **jemalloc never returns the requested size.** It rounds every request
//!    up to a *size class*. A 14-byte member costs 16 bytes; a 3168-byte
//!    `Vec<Node>` costs 3584. Billing `len` under-reports by the rounding.
//! 2. **Containers allocate their own tables.** A `HashMap` entry is not
//!    `key.len() + value.len()`; it is a slot in a power-of-two bucket array
//!    that also carries a control byte and is only ever 7/8 full.
//!
//! Everything in this module is pure integer arithmetic on values already in
//! registers (`len()`, `capacity()`, `size_of`). No allocation, no lock, no
//! heap walk — these helpers sit on the write path.

/// jemalloc's usable size for an allocation request of `n` bytes.
///
/// Models the 64-bit `lg_quantum = 4` class table: one 8-byte tiny class,
/// then four classes per doubling (`16, 32, 48, 64, 80, 96, 112, 128, 160,
/// 192, 224, 256, 320, ...`). The same progression continues into the large
/// (page-multiple) classes, so one formula covers the whole range.
///
/// This is `nallocx(n, 0)` without the FFI call and without needing the
/// allocator to be jemalloc at all: under the system allocator the figure
/// over-reports slightly (glibc rounds to 16 and adds a header), which is the
/// safe direction for a memory gate.
///
/// ```text
/// size_class(0)   == 0      // no allocation is made for a zero-byte request
/// size_class(1)   == 8
/// size_class(14)  == 16
/// size_class(129) == 160
/// size_class(3168)== 3584
/// ```
#[inline]
#[must_use]
pub const fn size_class(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    if n <= 8 {
        return 8;
    }
    if n <= 16 {
        return 16;
    }
    // `prev_pow2` of (n - 1): the base of the doubling group `n` falls in.
    // Four classes per group means the spacing is base/4, floored at the
    // 16-byte quantum.
    let prev_pow2 = 1usize << (usize::BITS - 1 - (n - 1).leading_zeros());
    let delta = if prev_pow2 >> 2 > 16 { prev_pow2 >> 2 } else { 16 };
    // Round `n` up to the next multiple of `delta`. `n <= usize::MAX - delta`
    // holds for every allocation that could have succeeded.
    n.div_ceil(delta) * delta
}

/// Bytes a `Vec<T>`-style buffer of `cap` elements really costs.
#[inline]
#[must_use]
pub const fn vec_bytes(cap: usize, elem: usize) -> usize {
    size_class(cap * elem)
}

/// Number of hashbrown buckets behind a reported `HashMap`/`HashSet`
/// `capacity()`.
///
/// hashbrown sizes its table as a power of two and grows when the table is
/// 7/8 full, so `capacity() == buckets / 8 * 7` for `buckets >= 8` and
/// `capacity() == buckets - 1` below that. This inverts that mapping exactly.
#[inline]
#[must_use]
pub const fn hash_buckets(capacity: usize) -> usize {
    if capacity == 0 {
        0
    } else if capacity < 8 {
        // 3 -> 4, 7 -> 8; both are the only reachable sub-8 capacities.
        capacity + 1
    } else {
        capacity / 7 * 8
    }
}

/// Bytes a hashbrown table with the given reported `capacity()` really costs,
/// for an entry type of `entry` bytes.
///
/// The allocation is one block: `buckets * entry` of data plus `buckets`
/// control bytes plus one trailing `Group` of control bytes (16 on every
/// target moon ships on).
#[inline]
#[must_use]
pub const fn hash_table_bytes(capacity: usize, entry: usize) -> usize {
    let buckets = hash_buckets(capacity);
    if buckets == 0 {
        return 0;
    }
    size_class(buckets * entry + buckets + 16)
}

/// Average allocated-bucket-per-live-item ratio over one doubling cycle of a
/// hashbrown table, expressed as a rational `num/den`.
///
/// A table with `i` items holds `next_pow2(ceil(i * 8 / 7))` buckets, so the
/// ratio sweeps from 16/7 (just after a grow) down to 8/7 (just before the
/// next one). Averaged over the cycle it is `ln 2 / 0.4375 = 1.584`.
///
/// Used only where an exact `capacity()` snapshot is not available at the
/// mutation site and the charge has to be a per-element constant — see the
/// `Hash` / `HashWithTtl` arms of `RedisValue::estimate_memory`. A single
/// linear constant cannot track a doubling allocator exactly: the true figure
/// oscillates between 1.14x and 2.29x of the slot within every cycle. This
/// picks the cycle average and rounds the resulting constant UP, so the
/// ledger is right on average and never far below.
pub const HASH_GROWTH_NUM: usize = 1584;
/// Denominator for [`HASH_GROWTH_NUM`].
pub const HASH_GROWTH_DEN: usize = 1000;

/// Average allocated-capacity-per-live-element ratio for a doubling `Vec` /
/// `VecDeque`, as a rational `num/den`.
///
/// Capacity sweeps from 2.0x (just after a grow) down to 1.0x; the average
/// over the cycle is `ln 2 / 0.5 = 1.386`.
pub const VEC_GROWTH_NUM: usize = 1386;
/// Denominator for [`VEC_GROWTH_NUM`].
pub const VEC_GROWTH_DEN: usize = 1000;

/// `slot` bytes scaled by the hashbrown growth-cycle average and rounded up
/// to the 8-byte quantum. `const` so the per-element constants below are
/// compile-time values, not runtime multiplications on the write path.
#[inline]
#[must_use]
pub const fn amortized_hash_slot(slot: usize) -> usize {
    // +1 for the control byte that accompanies every bucket.
    (((slot + 1) * HASH_GROWTH_NUM / HASH_GROWTH_DEN) + 7) / 8 * 8
}

/// `slot` bytes scaled by the `Vec` growth-cycle average, rounded up to 8.
#[inline]
#[must_use]
pub const fn amortized_vec_slot(slot: usize) -> usize {
    ((slot * VEC_GROWTH_NUM / VEC_GROWTH_DEN) + 7) / 8 * 8
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The instrument itself, checked against jemalloc's published 64-bit
    /// `lg_quantum = 4` class table. Every other assertion in the accounting
    /// suite is only as good as this one.
    #[test]
    fn size_class_matches_the_jemalloc_class_table() {
        // (request, usable) pairs straight off the class list.
        let cases: &[(usize, usize)] = &[
            (0, 0),
            (1, 8),
            (8, 8),
            (9, 16),
            (16, 16),
            (17, 32),
            (32, 32),
            (33, 48),
            (48, 48),
            (49, 64),
            (64, 64),
            (65, 80),
            (80, 80),
            (81, 96),
            (128, 128),
            (129, 160),
            (160, 160),
            (161, 192),
            (256, 256),
            (257, 320),
            (320, 320),
            (512, 512),
            (513, 640),
            (640, 640),
            (784, 896),
            (1024, 1024),
            (1025, 1280),
            (3168, 3584),
            (3584, 3584),
            (3585, 4096),
            (14336, 14336),
            (14337, 16384),
            (16385, 20480),
            (65536, 65536),
            (65537, 81920),
        ];
        for &(req, want) in cases {
            assert_eq!(
                size_class(req),
                want,
                "size_class({req}) should be {want} per the jemalloc class table"
            );
        }
    }

    /// A size class is never smaller than the request, and never more than
    /// 25% larger above the quantum-spaced range — the defining property of a
    /// 4-classes-per-doubling table.
    #[test]
    fn size_class_is_monotone_and_bounded() {
        let mut prev = 0;
        for n in 1..40_000usize {
            let c = size_class(n);
            assert!(c >= n, "size_class({n}) = {c} must not be below the request");
            assert!(c >= prev, "size_class must be monotone at {n}");
            if n > 64 {
                assert!(
                    c * 4 <= n * 5,
                    "size_class({n}) = {c} exceeds the 25% ceiling of a 4-per-doubling table"
                );
            }
            prev = c;
        }
    }

    /// `hash_buckets` must invert hashbrown's own capacity mapping.
    #[test]
    fn hash_buckets_inverts_hashbrown_capacity() {
        use std::collections::HashMap;
        for n in [0usize, 1, 3, 7, 8, 14, 28, 100, 1000, 10_000] {
            let mut m: HashMap<u64, u64> = HashMap::new();
            for i in 0..n as u64 {
                m.insert(i, i);
            }
            let buckets = hash_buckets(m.capacity());
            assert!(
                buckets >= m.len(),
                "n={n}: {buckets} buckets cannot hold {} items",
                m.len()
            );
            assert!(
                buckets.is_power_of_two() || buckets == 0,
                "n={n}: hashbrown tables are power-of-two sized, got {buckets}"
            );
            assert!(
                buckets * 7 >= m.len() * 8,
                "n={n}: {buckets} buckets is below hashbrown's 7/8 load factor for {} items",
                m.len()
            );
        }
    }
}

//! Micro-benchmark: `Group::match_h2` SIMD paths (SSE2 / NEON) vs scalar fallback.
//!
//! The `Segment::find` hot path in `DashTable` calls `match_h2` for every probe.
//! On aarch64 the scalar fallback was showing up as ~14% of CPU in the profiling
//! that motivated Track B of PR #71. This micro-bench measures the SIMD win on
//! the actual target CPU, decoupled from network benchmark infrastructure.
//!
//! We benchmark three workload shapes per path:
//!   - `miss`     : 16 FULL bytes, none match the needle
//!   - `hit_one`  : one match at a random position
//!   - `hit_many` : three matches (common for H2 collisions)
//!
//! The scalar implementation is inlined here to provide a direct A/B on the same
//! CPU, compiler, and optimiser settings.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use moon::storage::dashtable::simd::{BitMask, Group};

/// Scalar baseline — identical semantics to the SIMD `match_h2` but portable.
/// Inlined here (not re-exported from the library) to keep the comparison
/// honest across platforms.
#[inline]
fn match_h2_scalar(group: &Group, h2: u8) -> BitMask {
    let mut mask = 0u16;
    for i in 0..16 {
        if group.0[i] == h2 {
            mask |= 1 << i;
        }
    }
    BitMask(mask)
}

/// Scalar baseline for `match_empty_or_deleted` (bit 7 set).
#[inline]
fn match_empty_or_deleted_scalar(group: &Group) -> BitMask {
    let mut mask = 0u16;
    for i in 0..16 {
        if group.0[i] & 0x80 != 0 {
            mask |= 1 << i;
        }
    }
    BitMask(mask)
}

fn bench_match_h2(c: &mut Criterion) {
    // ── Workload fixtures ────────────────────────────────────────────────
    // `miss`: 16 FULL bytes, none equal the needle (0x2A).
    let miss = Group([
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f,
    ]);
    // `hit_one`: exactly one position matches 0x42 (at index 7).
    let hit_one = Group([
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x42, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f,
    ]);
    // `hit_many`: three positions match 0x42 (indices 0, 5, 15) — typical H2 collision.
    let hit_many = Group([
        0x42, 0x11, 0x12, 0x13, 0x14, 0x42, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x42,
    ]);

    // ── SIMD path (NEON on aarch64, SSE2 on x86_64) ──────────────────────
    let mut simd_group = c.benchmark_group("simd/match_h2");

    simd_group.bench_function("simd_miss", |b| {
        b.iter(|| {
            // SAFETY: SSE2 is baseline on x86_64; NEON is mandatory on AArch64.
            #[cfg(target_arch = "x86_64")]
            let r = unsafe { black_box(&miss).match_h2(black_box(0x42)) };
            #[cfg(not(target_arch = "x86_64"))]
            let r = black_box(&miss).match_h2(black_box(0x42));
            black_box(r)
        })
    });

    simd_group.bench_function("simd_hit_one", |b| {
        b.iter(|| {
            #[cfg(target_arch = "x86_64")]
            let r = unsafe { black_box(&hit_one).match_h2(black_box(0x42)) };
            #[cfg(not(target_arch = "x86_64"))]
            let r = black_box(&hit_one).match_h2(black_box(0x42));
            black_box(r)
        })
    });

    simd_group.bench_function("simd_hit_many", |b| {
        b.iter(|| {
            #[cfg(target_arch = "x86_64")]
            let r = unsafe { black_box(&hit_many).match_h2(black_box(0x42)) };
            #[cfg(not(target_arch = "x86_64"))]
            let r = black_box(&hit_many).match_h2(black_box(0x42));
            black_box(r)
        })
    });

    simd_group.finish();

    // ── Scalar baseline (same CPU, same compiler flags) ──────────────────
    let mut scalar_group = c.benchmark_group("simd/match_h2_scalar");

    scalar_group.bench_function("scalar_miss", |b| {
        b.iter(|| black_box(match_h2_scalar(black_box(&miss), black_box(0x42))))
    });

    scalar_group.bench_function("scalar_hit_one", |b| {
        b.iter(|| black_box(match_h2_scalar(black_box(&hit_one), black_box(0x42))))
    });

    scalar_group.bench_function("scalar_hit_many", |b| {
        b.iter(|| black_box(match_h2_scalar(black_box(&hit_many), black_box(0x42))))
    });

    scalar_group.finish();
}

fn bench_match_empty_or_deleted(c: &mut Criterion) {
    // Mixed group: 2 FULL (bit 7 clear), 1 DELETED (0x80), rest EMPTY (0xFF).
    let mixed = Group([
        0x10, 0x80, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff,
    ]);

    let mut g = c.benchmark_group("simd/match_empty_or_deleted");

    g.bench_function("simd", |b| {
        b.iter(|| {
            // SAFETY: SSE2 baseline on x86_64; NEON mandatory on AArch64.
            #[cfg(target_arch = "x86_64")]
            let r = unsafe { black_box(&mixed).match_empty_or_deleted() };
            #[cfg(not(target_arch = "x86_64"))]
            let r = black_box(&mixed).match_empty_or_deleted();
            black_box(r)
        })
    });

    g.bench_function("scalar", |b| {
        b.iter(|| black_box(match_empty_or_deleted_scalar(black_box(&mixed))))
    });

    g.finish();
}

criterion_group!(benches, bench_match_h2, bench_match_empty_or_deleted);
criterion_main!(benches);

//! moon#799: the cost of one HSET/HDEL/HGET against a *listpack*-encoded hash.
//!
//! The end-to-end `redis-benchmark` view of this defect is dominated by the
//! network and by whatever else is running on the box; the thing that actually
//! changed lives entirely inside `Listpack`, so measure it there. The
//! parameter is the field count, which is exactly what the issue's table
//! sweeps, and the shape is the one `hash_write::hset` executes:
//! locate the field, then overwrite its value.
//!
//! Read the curve, not the absolute numbers: a per-entry cost shows up as
//! time growing with the field count.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use moon::storage::listpack::Listpack;

const SIZES: &[usize] = &[8, 32, 64, 128];

fn build(pairs: usize) -> Listpack {
    let mut lp = Listpack::new();
    for i in 0..pairs {
        lp.push_back(format!("field:{i:08}").as_bytes());
        lp.push_back(format!("value-payload-{i:08}").as_bytes());
    }
    lp
}

/// HSET onto a field that already exists: the write path this issue names.
fn hset_existing(c: &mut Criterion) {
    let mut g = c.benchmark_group("listpack_hset_existing");
    for &n in SIZES {
        // Worst case for a linear scan and the honest one for a hash whose
        // fields are hit uniformly: the last field.
        let field = format!("field:{:08}", n - 1);
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut lp = build(n);
            b.iter(|| {
                let idx = lp.find_pair_index(black_box(field.as_bytes()));
                if let Some(idx) = idx {
                    lp.replace_at(idx * 2 + 1, black_box(b"value-payload-replaced"));
                }
                black_box(lp.len())
            });
        });
    }
    g.finish();
}

/// HGET of an existing field.
fn hget_existing(c: &mut Criterion) {
    let mut g = c.benchmark_group("listpack_hget_existing");
    for &n in SIZES {
        let field = format!("field:{:08}", n - 1);
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let lp = build(n);
            b.iter(|| {
                let idx = lp.find_pair_index(black_box(field.as_bytes()));
                black_box(idx.and_then(|i| lp.get_at(i * 2 + 1)))
            });
        });
    }
    g.finish();
}

/// HDEL of an existing field, immediately re-added so the fixture is stable.
fn hdel_existing(c: &mut Criterion) {
    let mut g = c.benchmark_group("listpack_hdel_existing");
    for &n in SIZES {
        let field = format!("field:{:08}", n - 1);
        let value = format!("value-payload-{:08}", n - 1);
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut lp = build(n);
            b.iter(|| {
                if let Some(i) = lp.find_pair_index(black_box(field.as_bytes())) {
                    lp.remove_at(i * 2 + 1);
                    lp.remove_at(i * 2);
                }
                lp.push_back(field.as_bytes());
                lp.push_back(value.as_bytes());
                black_box(lp.len())
            });
        });
    }
    g.finish();
}

/// SISMEMBER against a set-encoded listpack.
fn set_contains(c: &mut Criterion) {
    let mut g = c.benchmark_group("listpack_set_contains");
    for &n in SIZES {
        let member = format!("field:{:08}", n - 1);
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let lp = build(n);
            b.iter(|| black_box(lp.find(black_box(member.as_bytes()))));
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    hset_existing,
    hget_existing,
    hdel_existing,
    set_contains
);
criterion_main!(benches);

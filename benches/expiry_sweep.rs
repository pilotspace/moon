//! Cost of ONE active-expiry tick on the shard event loop (moon#543, #552).
//!
//! The sweep runs every 100ms on the latency-critical shard thread, so its
//! cost when it has NOTHING to do is what matters most. Three shapes:
//!
//! * `idle_ttl_heavy` — moon#552: a database full of TTL'd keys, none due.
//!   Pre-fix every tick entered the cycle to re-derive "nothing due".
//! * `one_due_key_among_field_ttl_hashes` — moon#543: the cycle DOES run
//!   (one whole key is due), and pre-fix sweep 2 then scanned the entire
//!   table for `HashWithTtl` keys and reaped every one of them.
//! * `due_field_ttl_backlog` — moon#543: the bounded case. Pre-fix ONE tick
//!   reaped all N hashes; the cost of a tick must now be flat in N.
//!
//! Run: `cargo bench --bench expiry_sweep`

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use moon::server::expiration::expire_cycle_direct;
use moon::storage::Database;
use moon::storage::compact_value::CompactValue;
use moon::storage::db::HashTtlCond;
use moon::storage::entry::{Entry, RedisValue, current_time_ms};
use std::collections::HashMap;

/// `n` string keys whose TTLs are an hour out — armed, but never due.
fn idle_volatile_db(n: u32) -> Database {
    let mut db = Database::new();
    let deadline = current_time_ms() + 3_600_000;
    for i in 0..n {
        db.set(
            &Bytes::from(format!("k{i}")),
            Entry::new_string_with_expiry(Bytes::from_static(b"value"), deadline),
        );
    }
    db
}

/// `n` hashes each carrying one field TTL `offset_ms` from now.
fn field_ttl_db(n: u32, offset_ms: i64) -> Database {
    let mut db = Database::new();
    let base = db.now_ms();
    // One never-due whole-key TTL, so `maybe_has_expiring_keys` is armed on
    // BOTH sides of an A/B: the pre-#543 code gates the entire cycle on that
    // latch, and a leg that early-returns would measure nothing at all.
    db.set(
        b"__arm__",
        Entry::new_string_with_expiry(Bytes::from_static(b"v"), base + 86_400_000),
    );
    if offset_ms >= 0 {
        let deadline = base + offset_ms as u64;
        for i in 0..n {
            let key = format!("h{i}");
            {
                let map = db.get_or_create_hash(key.as_bytes()).expect("hash");
                map.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
            }
            db.hash_set_field_ttl(key.as_bytes(), b"f", deadline, HashTtlCond::Always)
                .expect("field ttl stored");
        }
        return db;
    }
    // Already-due fields cannot go through `hash_set_field_ttl` (it deletes a
    // past-dated field inline instead of storing it), so build the value the
    // way RESTORE / replication does — which also exercises `Database::set`'s
    // index-on-arrival path.
    let deadline = base - offset_ms.unsigned_abs();
    for i in 0..n {
        let mut fields = HashMap::new();
        fields.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        let mut ttls = HashMap::new();
        ttls.insert(Bytes::from_static(b"f"), deadline);
        let mut entry = Entry::new_string(Bytes::new());
        entry.value = CompactValue::from_redis_value(RedisValue::HashWithTtl {
            fields,
            ttls,
            min_expiry_ms: deadline,
        });
        db.set(&Bytes::from(format!("h{i}")), entry);
    }
    db
}

/// moon#552 — the tick that should cost nothing.
fn bench_idle_ttl_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("expiry_tick/idle_ttl_heavy");
    for n in [1_000u32, 100_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut db = idle_volatile_db(n);
            b.iter(|| {
                expire_cycle_direct(black_box(&mut db), &mut |k| {
                    black_box(k);
                });
                black_box(db.len())
            });
        });
    }
    group.finish();
}

/// moon#543 — a cycle that MUST run, with a large field-TTL population that
/// has nothing due. Pre-fix this paid an O(N) table scan plus N reap calls.
fn bench_one_due_key_among_field_ttl_hashes(c: &mut Criterion) {
    let mut group = c.benchmark_group("expiry_tick/one_due_key_among_field_ttl_hashes");
    for n in [1_000u32, 20_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut db = field_ttl_db(n, 3_600_000);
            b.iter(|| {
                // Re-arm one whole-key victim so the cycle is entered every
                // iteration (the head-peek gate would otherwise skip it).
                db.set(
                    b"__due__",
                    Entry::new_string_with_expiry(
                        Bytes::from_static(b"v"),
                        current_time_ms().saturating_sub(1),
                    ),
                );
                expire_cycle_direct(black_box(&mut db), &mut |k| {
                    black_box(k);
                });
                black_box(db.len())
            });
        });
    }
    group.finish();
}

/// moon#543 — the bounded case: N hashes ALL due. One tick's cost must be
/// flat in N instead of linear in it.
fn bench_due_field_ttl_backlog(c: &mut Criterion) {
    let mut group = c.benchmark_group("expiry_tick/due_field_ttl_backlog");
    for n in [1_000u32, 20_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || field_ttl_db(n, -1),
                |db| {
                    expire_cycle_direct(black_box(db), &mut |k| {
                        black_box(k);
                    });
                    black_box(db.len())
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_idle_ttl_heavy,
    bench_one_due_key_among_field_ttl_hashes,
    bench_due_field_ttl_backlog
);
criterion_main!(benches);

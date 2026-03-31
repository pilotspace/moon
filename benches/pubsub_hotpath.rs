//! Micro-benchmark: PUBLISH hot path breakdown.
//!
//! Measures each stage of PUBLISH to identify bottlenecks:
//! 1. Registry lookup (HashMap get)
//! 2. Pre-serialization (serialize once)
//! 3. Channel send (try_send per subscriber, Bytes refcount clone)
//! 4. Full publish cycle

use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};

use moon::pubsub::PubSubRegistry;
use moon::pubsub::subscriber::Subscriber;
use moon::runtime::channel;

/// Bench: publish to N subscribers on a single channel
fn bench_publish_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_fanout");

    for n_subs in [1, 10, 50, 100] {
        group.bench_function(format!("{}_subs", n_subs), |b| {
            let channel = Bytes::from_static(b"bench:channel");
            let message = Bytes::from("x".repeat(32));

            b.iter(|| {
                let mut registry = PubSubRegistry::new();
                let mut rxs = Vec::with_capacity(n_subs);
                for i in 0..n_subs {
                    let (tx, rx) = channel::mpsc_bounded::<Bytes>(256);
                    let sub = Subscriber::new(tx, i as u64 + 1);
                    registry.subscribe(channel.clone(), sub);
                    rxs.push(rx);
                }
                let count = registry.publish(black_box(&channel), black_box(&message));
                black_box(count);
                for rx in &rxs {
                    while rx.try_recv().is_ok() {}
                }
            });
        });
    }
    group.finish();
}

/// Bench: channel try_send throughput with pre-serialized Bytes.
///
/// Creates channel inside `b.iter` to ensure each iteration measures a
/// successful send (not the full/failure path from channel saturation).
fn bench_try_send(c: &mut Criterion) {
    let mut group = c.benchmark_group("try_send_bytes");

    // Pre-serialized message (typical pub/sub message ~80 bytes)
    let data = Bytes::from(vec![b'x'; 80]);

    group.bench_function("single_send", |b| {
        b.iter(|| {
            let (tx, rx) = channel::mpsc_bounded::<Bytes>(16);
            black_box(tx.try_send(black_box(data.clone())).is_ok());
            black_box(rx.try_recv().ok());
        });
    });

    group.bench_function("10x_fanout_send", |b| {
        b.iter(|| {
            let mut txs = Vec::new();
            let mut rxs = Vec::new();
            for _ in 0..10 {
                let (tx, rx) = channel::mpsc_bounded::<Bytes>(16);
                txs.push(tx);
                rxs.push(rx);
            }
            for tx in &txs {
                black_box(tx.try_send(black_box(data.clone())).is_ok());
            }
            for rx in &rxs {
                black_box(rx.try_recv().ok());
            }
        });
    });
    group.finish();
}

/// Bench: RwLock contention simulation
fn bench_rwlock_contention(c: &mut Criterion) {
    use parking_lot::RwLock;
    use std::sync::Arc;

    let mut group = c.benchmark_group("rwlock_pubsub");
    let registry = Arc::new(RwLock::new(PubSubRegistry::new()));

    {
        let mut reg = registry.write();
        for i in 0..10 {
            let (tx, _rx) = channel::mpsc_bounded::<Bytes>(4096);
            let sub = Subscriber::new(tx, i + 1);
            reg.subscribe(Bytes::from_static(b"bench:ch"), sub);
        }
    }

    group.bench_function("write_lock_publish", |b| {
        let channel = Bytes::from_static(b"bench:ch");
        let message = Bytes::from("x".repeat(32));
        b.iter(|| {
            let count = registry
                .write()
                .publish(black_box(&channel), black_box(&message));
            black_box(count);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_publish_fanout,
    bench_try_send,
    bench_rwlock_contention,
);
criterion_main!(benches);

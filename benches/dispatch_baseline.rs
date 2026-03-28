//! Phase 44 dispatch baseline benchmarks for CROSS-03 regression detection.
//!
//! Run: `cargo bench --bench dispatch_baseline`
//! Save baseline: `cargo bench --bench dispatch_baseline -- --save-baseline phase44`
//! Compare later: `cargo bench --bench dispatch_baseline -- --baseline phase44`

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};

use moon::command::DispatchResult;
use moon::protocol::{self, Frame, ParseConfig};
use moon::storage::Database;
use moon::storage::entry::CompactEntry;

fn bench_dispatch_baseline(c: &mut Criterion) {
    // Setup: DB with 10K keys of 256 bytes
    let mut db = Database::new();
    for i in 0..10_000 {
        let key = format!("key:{:0>10}", i);
        let val = "x".repeat(256);
        let entry = CompactEntry::new_string(Bytes::from(val));
        db.set(Bytes::from(key), entry);
    }

    let lookup_key = Bytes::from("key:0000005000");
    let set_key = Bytes::from("key:bench_set_target");
    let set_val = Bytes::from("x".repeat(256));

    let config = ParseConfig::default();

    // Build GET wire format
    let get_frame = Frame::Array(
        vec![
            Frame::BulkString(Bytes::from_static(b"GET")),
            Frame::BulkString(lookup_key.clone()),
        ]
        .into(),
    );
    let mut get_wire = bytes::BytesMut::with_capacity(64);
    protocol::serialize(&get_frame, &mut get_wire);
    let get_wire_bytes = get_wire.freeze();

    // Build SET wire format
    let set_frame = Frame::Array(
        vec![
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(set_key.clone()),
            Frame::BulkString(set_val.clone()),
        ]
        .into(),
    );
    let mut set_wire = bytes::BytesMut::with_capacity(320);
    protocol::serialize(&set_frame, &mut set_wire);
    let set_wire_bytes = set_wire.freeze();

    // --- Benchmark 1: GET dispatch only ---
    c.bench_function("baseline_get_dispatch", |b| {
        let args = &[Frame::BulkString(lookup_key.clone())];
        b.iter(|| {
            let result = moon::command::dispatch(
                &mut db,
                black_box(b"GET"),
                black_box(args),
                &mut 0usize,
                16,
            );
            black_box(result);
        })
    });

    // --- Benchmark 2: SET dispatch only ---
    c.bench_function("baseline_set_dispatch", |b| {
        let args = &[
            Frame::BulkString(set_key.clone()),
            Frame::BulkString(set_val.clone()),
        ];
        b.iter(|| {
            let result = moon::command::dispatch(
                &mut db,
                black_box(b"SET"),
                black_box(args),
                &mut 0usize,
                16,
            );
            black_box(result);
        })
    });

    // --- Benchmark 3: GET full pipeline (parse + dispatch + serialize) ---
    c.bench_function("baseline_get_pipeline", |b| {
        let mut buf = bytes::BytesMut::with_capacity(300);
        b.iter(|| {
            let mut parse_buf = bytes::BytesMut::from(get_wire_bytes.as_ref());
            let frame = protocol::parse(&mut parse_buf, &config).unwrap().unwrap();
            let (cmd, args) = match &frame {
                Frame::Array(a) => {
                    let name = match &a[0] {
                        Frame::BulkString(s) => s.as_ref(),
                        _ => unreachable!(),
                    };
                    (name, &a[1..])
                }
                _ => unreachable!(),
            };
            let result = moon::command::dispatch(&mut db, cmd, args, &mut 0usize, 16);
            buf.clear();
            if let DispatchResult::Response(f) = result {
                protocol::serialize(&f, &mut buf);
            }
            black_box(&buf);
        })
    });

    // --- Benchmark 4: SET full pipeline (parse + dispatch + serialize) ---
    c.bench_function("baseline_set_pipeline", |b| {
        let mut buf = bytes::BytesMut::with_capacity(300);
        b.iter(|| {
            let mut parse_buf = bytes::BytesMut::from(set_wire_bytes.as_ref());
            let frame = protocol::parse(&mut parse_buf, &config).unwrap().unwrap();
            let (cmd, args) = match &frame {
                Frame::Array(a) => {
                    let name = match &a[0] {
                        Frame::BulkString(s) => s.as_ref(),
                        _ => unreachable!(),
                    };
                    (name, &a[1..])
                }
                _ => unreachable!(),
            };
            let result = moon::command::dispatch(&mut db, cmd, args, &mut 0usize, 16);
            buf.clear();
            if let DispatchResult::Response(f) = result {
                protocol::serialize(&f, &mut buf);
            }
            black_box(&buf);
        })
    });
}

criterion_group!(benches, bench_dispatch_baseline);
criterion_main!(benches);

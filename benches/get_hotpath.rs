//! Micro-benchmark: GET command hot path breakdown.
//!
//! Measures each stage of a GET command to identify exactly where time is spent
//! vs Redis's ~50ns per GET.

use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};

use moon::protocol::{self, Frame, ParseConfig};
use moon::storage::Database;
use moon::storage::entry::CompactEntry;

fn bench_get_hotpath(c: &mut Criterion) {
    // Setup: create DB with 10K keys of 256 bytes
    let mut db = Database::new();
    for i in 0..10_000 {
        let key = format!("key:{:0>10}", i);
        let val = "x".repeat(256);
        let entry = CompactEntry::new_string(Bytes::from(val));
        db.set(Bytes::from(key), entry);
    }

    let lookup_key = Bytes::from("key:0000005000");
    let missing_key = Bytes::from("key:missing_nope");

    // Build a GET command frame
    let get_frame = Frame::Array(
        vec![
            Frame::BulkString(Bytes::from_static(b"GET")),
            Frame::BulkString(lookup_key.clone()),
        ]
        .into(),
    );

    // Pre-serialize the GET command into wire format
    let mut wire = bytes::BytesMut::with_capacity(64);
    protocol::serialize(&get_frame, &mut wire);
    let wire_bytes = wire.freeze();

    let config = ParseConfig::default();

    // ─── Stage 1: RESP Parse ───
    c.bench_function("1_resp_parse_get_cmd", |b| {
        b.iter(|| {
            let mut buf = bytes::BytesMut::from(wire_bytes.as_ref());
            let frame = protocol::parse(&mut buf, &config).unwrap().unwrap();
            black_box(frame);
        })
    });

    // ─── Stage 2: extract_command ───
    c.bench_function("2_extract_command", |b| {
        b.iter(|| {
            let (cmd, args) = match &get_frame {
                Frame::Array(a) if !a.is_empty() => {
                    let name = match &a[0] {
                        Frame::BulkString(s) => s.as_ref(),
                        _ => unreachable!(),
                    };
                    (name, &a[1..])
                }
                _ => unreachable!(),
            };
            black_box((cmd, args));
        })
    });

    // ─── Stage 3: DashTable lookup (hit) ───
    c.bench_function("3_dashtable_get_hit", |b| {
        b.iter(|| {
            let entry = db.get(black_box(lookup_key.as_ref()));
            black_box(entry);
        })
    });

    // ─── Stage 3b: DashTable lookup (miss) ───
    c.bench_function("3b_dashtable_get_miss", |b| {
        b.iter(|| {
            let entry = db.get(black_box(missing_key.as_ref()));
            black_box(entry);
        })
    });

    // ─── Stage 4: as_bytes_owned (Arc bump) ───
    c.bench_function("4_as_bytes_owned", |b| {
        b.iter(|| {
            let entry = db.get(lookup_key.as_ref()).unwrap();
            let v = entry.value.as_bytes_owned();
            black_box(v);
        })
    });

    // ─── Stage 5: Frame::BulkString construction ───
    c.bench_function("5_frame_bulkstring", |b| {
        let val = Bytes::from("x".repeat(256));
        b.iter(|| {
            let frame = Frame::BulkString(black_box(val.clone()));
            black_box(frame);
        })
    });

    // ─── Stage 6: Response serialization ───
    c.bench_function("6_serialize_bulkstring_256b", |b| {
        let response = Frame::BulkString(Bytes::from("x".repeat(256)));
        let mut buf = bytes::BytesMut::with_capacity(300);
        b.iter(|| {
            buf.clear();
            protocol::serialize(black_box(&response), &mut buf);
            black_box(&buf);
        })
    });

    // ─── Stage 7: Full GET (dispatch → response) ───
    c.bench_function("7_full_get_dispatch", |b| {
        let args = &[Frame::BulkString(lookup_key.clone())];
        b.iter(|| {
            let frame = moon::command::string::get(&mut db, black_box(args));
            black_box(frame);
        })
    });

    // ─── Stage 8: Full dispatch() routing ───
    c.bench_function("8_dispatch_routing_get", |b| {
        let cmd = b"GET";
        let args: &[Frame] = &[Frame::BulkString(lookup_key.clone())];
        b.iter(|| {
            let result =
                moon::command::dispatch(&mut db, black_box(cmd), black_box(args), &mut 0usize, 16);
            black_box(result);
        })
    });

    // ─── Stage 9: Full pipeline cycle (parse + dispatch + serialize) ───
    c.bench_function("9_full_pipeline_get_256b", |b| {
        let mut buf = bytes::BytesMut::with_capacity(300);
        b.iter(|| {
            // Parse
            let mut parse_buf = bytes::BytesMut::from(wire_bytes.as_ref());
            let frame = protocol::parse(&mut parse_buf, &config).unwrap().unwrap();

            // Extract command
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

            // Dispatch
            let result = moon::command::dispatch(&mut db, cmd, args, &mut 0usize, 16);

            // Serialize response
            buf.clear();
            match result {
                moon::command::DispatchResult::Response(f) => {
                    protocol::serialize(&f, &mut buf);
                }
                _ => {}
            }
            black_box(&buf);
        })
    });

    // ─── Stage 10: is_write_command check (removed; function deleted) ───

    // ─── Stage 11: xxhash key routing ───
    c.bench_function("11_xxhash_key_route", |b| {
        b.iter(|| {
            let shard = moon::shard::dispatch::key_to_shard(black_box(lookup_key.as_ref()), 12);
            black_box(shard);
        })
    });
}

criterion_group!(benches, bench_get_hotpath);
criterion_main!(benches);

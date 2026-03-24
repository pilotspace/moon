use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use rust_redis::protocol::{parse, serialize, Frame, ParseConfig};

fn bench_parse_simple_string(c: &mut Criterion) {
    let config = ParseConfig::default();
    c.bench_function("parse_simple_string", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(&b"+OK\r\n"[..]);
            let frame = parse(black_box(&mut buf), &config).unwrap().unwrap();
            black_box(frame);
        })
    });
}

fn bench_parse_bulk_string(c: &mut Criterion) {
    let config = ParseConfig::default();
    c.bench_function("parse_bulk_string", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(&b"$5\r\nhello\r\n"[..]);
            let frame = parse(black_box(&mut buf), &config).unwrap().unwrap();
            black_box(frame);
        })
    });
}

fn bench_parse_integer(c: &mut Criterion) {
    let config = ParseConfig::default();
    c.bench_function("parse_integer", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(&b":12345\r\n"[..]);
            let frame = parse(black_box(&mut buf), &config).unwrap().unwrap();
            black_box(frame);
        })
    });
}

fn bench_parse_array(c: &mut Criterion) {
    let config = ParseConfig::default();
    let input = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    c.bench_function("parse_array_3elem", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(&input[..]);
            let frame = parse(black_box(&mut buf), &config).unwrap().unwrap();
            black_box(frame);
        })
    });
}

fn bench_parse_inline(c: &mut Criterion) {
    let config = ParseConfig::default();
    c.bench_function("parse_inline", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(&b"SET foo bar\r\n"[..]);
            let frame = parse(black_box(&mut buf), &config).unwrap().unwrap();
            black_box(frame);
        })
    });
}

fn bench_serialize_array(c: &mut Criterion) {
    let frame = Frame::Array(vec![
        Frame::BulkString(Bytes::from_static(b"SET")),
        Frame::BulkString(Bytes::from_static(b"foo")),
        Frame::BulkString(Bytes::from_static(b"bar")),
    ]);
    c.bench_function("serialize_array_3elem", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            serialize(black_box(&frame), &mut buf);
            black_box(buf);
        })
    });
}

fn bench_roundtrip(c: &mut Criterion) {
    let config = ParseConfig::default();
    let frame = Frame::Array(vec![
        Frame::BulkString(Bytes::from_static(b"SET")),
        Frame::BulkString(Bytes::from_static(b"mykey")),
        Frame::BulkString(Bytes::from_static(b"myvalue")),
    ]);
    c.bench_function("roundtrip_array_3elem", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            serialize(&frame, &mut buf);
            let parsed = parse(black_box(&mut buf), &config).unwrap().unwrap();
            black_box(parsed);
        })
    });
}

fn bench_parse_pipeline_32(c: &mut Criterion) {
    let config = ParseConfig::default();
    // Build a 32-command pipeline: *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n x32
    let single_cmd = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
    let mut pipeline_data = Vec::with_capacity(single_cmd.len() * 32);
    for _ in 0..32 {
        pipeline_data.extend_from_slice(single_cmd);
    }

    c.bench_function("parse_pipeline_32cmd", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(&pipeline_data[..]);
            let mut count = 0;
            while let Ok(Some(frame)) = parse(black_box(&mut buf), &config) {
                black_box(&frame);
                count += 1;
            }
            assert_eq!(count, 32);
        })
    });
}

fn bench_parse_get_single(c: &mut Criterion) {
    let config = ParseConfig::default();
    let input = b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";
    c.bench_function("parse_get_single", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(&input[..]);
            let frame = parse(black_box(&mut buf), &config).unwrap().unwrap();
            black_box(frame);
        })
    });
}

fn bench_parse_set_single(c: &mut Criterion) {
    let config = ParseConfig::default();
    let input = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
    c.bench_function("parse_set_single", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(&input[..]);
            let frame = parse(black_box(&mut buf), &config).unwrap().unwrap();
            black_box(frame);
        })
    });
}

criterion_group!(
    benches,
    bench_parse_simple_string,
    bench_parse_bulk_string,
    bench_parse_integer,
    bench_parse_array,
    bench_parse_inline,
    bench_serialize_array,
    bench_roundtrip,
    bench_parse_pipeline_32,
    bench_parse_get_single,
    bench_parse_set_single,
);
criterion_main!(benches);

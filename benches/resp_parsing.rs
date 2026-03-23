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

criterion_group!(
    benches,
    bench_parse_simple_string,
    bench_parse_bulk_string,
    bench_parse_integer,
    bench_parse_array,
    bench_parse_inline,
    bench_serialize_array,
    bench_roundtrip,
);
criterion_main!(benches);

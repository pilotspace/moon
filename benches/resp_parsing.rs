use bytes::{Bytes, BytesMut};
use criterion::{Criterion, criterion_group, criterion_main};
use moon::protocol::{Frame, ParseConfig, parse, serialize};
use std::hint::black_box;

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
    let frame = Frame::Array(
        vec![
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"foo")),
            Frame::BulkString(Bytes::from_static(b"bar")),
        ]
        .into(),
    );
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
    let frame = Frame::Array(
        vec![
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"mykey")),
            Frame::BulkString(Bytes::from_static(b"myvalue")),
        ]
        .into(),
    );
    c.bench_function("roundtrip_array_3elem", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            serialize(&frame, &mut buf);
            let parsed = parse(black_box(&mut buf), &config).unwrap().unwrap();
            black_box(parsed);
        })
    });
}

// ---------------------------------------------------------------------------
// Naive byte-by-byte RESP parser for benchmark comparison.
// Implements the same logic as the optimized parser but without memchr/atoi.
// This serves as a reference baseline to demonstrate SIMD/memchr speedup.
//
// NOTE: Naive parser skips Frame construction; actual pre-optimization parser
// was slower than this reference because it also allocated Bytes/Vec for each
// frame. Any speedup shown by the optimized parser over this naive one
// understates the real benefit.
// ---------------------------------------------------------------------------

fn naive_find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn naive_parse_integer(buf: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(buf).ok()?;
    s.parse::<i64>().ok()
}

/// Parse one RESP frame naively (byte-by-byte). Returns true if a frame was parsed.
fn naive_parse_one(buf: &[u8], pos: &mut usize) -> bool {
    if *pos >= buf.len() {
        return false;
    }
    match buf[*pos] {
        b'+' | b'-' => {
            // Simple string / Error: find CRLF
            if let Some(crlf) = naive_find_crlf(buf, *pos + 1) {
                *pos = crlf + 2;
                true
            } else {
                false
            }
        }
        b':' => {
            // Integer: find CRLF, parse integer
            if let Some(crlf) = naive_find_crlf(buf, *pos + 1) {
                let _ = naive_parse_integer(&buf[*pos + 1..crlf]);
                *pos = crlf + 2;
                true
            } else {
                false
            }
        }
        b'$' => {
            // Bulk string: parse length, skip data
            if let Some(crlf) = naive_find_crlf(buf, *pos + 1) {
                if let Some(len) = naive_parse_integer(&buf[*pos + 1..crlf]) {
                    if len < 0 {
                        *pos = crlf + 2;
                        return true;
                    }
                    let data_start = crlf + 2;
                    let data_end = data_start + len as usize;
                    if data_end + 2 <= buf.len() {
                        *pos = data_end + 2;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        }
        b'*' => {
            // Array: parse count, then parse each element
            if let Some(crlf) = naive_find_crlf(buf, *pos + 1) {
                if let Some(count) = naive_parse_integer(&buf[*pos + 1..crlf]) {
                    *pos = crlf + 2;
                    for _ in 0..count {
                        if !naive_parse_one(buf, pos) {
                            return false;
                        }
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

fn bench_parse_pipeline_32_naive(c: &mut Criterion) {
    let single_cmd = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
    let mut pipeline_data = Vec::with_capacity(single_cmd.len() * 32);
    for _ in 0..32 {
        pipeline_data.extend_from_slice(single_cmd);
    }

    c.bench_function("parse_pipeline_32cmd_naive", |b| {
        b.iter(|| {
            let buf = black_box(&pipeline_data[..]);
            let mut pos = 0;
            let mut count = 0;
            while naive_parse_one(buf, &mut pos) {
                count += 1;
            }
            assert_eq!(count, 32);
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
    bench_parse_pipeline_32_naive,
    bench_parse_get_single,
    bench_parse_set_single,
);
criterion_main!(benches);

//! moon#1164: a large multibulk arriving across many socket reads must decode
//! in time linear in its size.
//!
//! Before the fix, every read of an incomplete `*N` frame re-walked the whole
//! frame from byte 0 — twice (the flat scan, then `validate_frame`) — so one
//! `RPUSH` of 1M elements took 15.3 s against redis's 0.10 s and stalled its
//! shard for all of it. This drives the public `RespCodec::decode_frame` the
//! way the monoio read loop does (append 8 KiB, decode until "need more") and
//! compares an 8x larger upload against a small one: linear work gives a
//! ratio near 8, the old quadratic one near 64.
//!
//! A ratio of wall times on the same host, best of several runs, is robust to
//! a slow machine; it is not robust to a machine that is 3x noisier on one leg
//! than the other, hence the generous bound.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use moon::protocol::Frame;
use moon::server::codec::RespCodec;

fn rpush(n: usize) -> Vec<u8> {
    let mut out = format!("*{}\r\n$5\r\nRPUSH\r\n$4\r\nbigl\r\n", n + 2).into_bytes();
    for _ in 0..n {
        out.extend_from_slice(b"$1\r\nx\r\n");
    }
    out
}

/// Decode `input` delivered in `chunk`-byte reads; returns the elapsed time.
fn decode_chunked(input: &[u8], chunk: usize) -> Duration {
    let mut codec = RespCodec::default();
    let mut buf = BytesMut::with_capacity(chunk);
    let start = Instant::now();
    for piece in input.chunks(chunk) {
        buf.extend_from_slice(piece);
        match codec.decode_frame(&mut buf) {
            Ok(Some(Frame::Array(items))) => {
                let elapsed = start.elapsed();
                assert!(buf.is_empty(), "the whole frame must be consumed");
                assert_eq!(items.len(), input.iter().filter(|&&b| b == b'$').count());
                return elapsed;
            }
            Ok(Some(other)) => panic!("expected Array, got {other:?}"),
            Ok(None) => {}
            Err(e) => panic!("decode failed: {e}"),
        }
    }
    panic!("frame never completed");
}

fn best_of(runs: usize, input: &[u8]) -> Duration {
    (0..runs)
        .map(|_| decode_chunked(input, 8192))
        .min()
        .unwrap_or_default()
}

#[test]
fn chunked_multibulk_decode_time_is_linear_in_size() {
    let small = rpush(25_000);
    let large = rpush(200_000); // 8x the elements, 8x the bytes
    // Warm the allocator and caches once before measuring.
    let _ = decode_chunked(&small, 8192);

    let t_small = best_of(5, &small);
    let t_large = best_of(3, &large);
    let ratio = t_large.as_secs_f64() / t_small.as_secs_f64().max(1e-6);
    eprintln!(
        "decode 25K elements: {t_small:?}; 200K elements: {t_large:?}; ratio {ratio:.1} (linear ~8, quadratic ~64)"
    );
    assert!(
        ratio < 24.0,
        "8x the input took {ratio:.1}x the time ({t_small:?} -> {t_large:?}): \
         incremental multibulk decoding is not linear (moon#1164)"
    );
}

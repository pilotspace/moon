//! Record encoding for the AOF and the replication stream (moon#1187).
//!
//! Every write that reaches the durable log is serialized here, once, on the
//! shard thread that executed it. Two properties keep that cheap:
//!
//! - **Exactly one allocation per record.** The output buffer is sized from
//!   the record's exact RESP length ([`resp_len`]) before anything is written,
//!   so it never grows, and `BytesMut::freeze` of a buffer whose length equals
//!   its capacity hands the allocation to `Bytes` as-is (a buffer with spare
//!   capacity costs `Bytes` a second, shared-header allocation).
//! - **No intermediate frame for the expire rewrite.** A relative-expiry
//!   command (`SET … EX`, `SETEX`, `EXPIRE` …) is serialized in its absolute
//!   form straight from the borrowed original arguments
//!   ([`serialize_expire_rewrite`]) instead of first cloning them into a new
//!   `Frame` and serializing that.
//!
//! The bytes are identical to `protocol::serialize::serialize` of the
//! equivalent frame — pinned by `tests::encode_matches_head_for_corpus`.

use bytes::{BufMut, Bytes, BytesMut};

use crate::protocol::{Frame, serialize};
use crate::replication::expire_rewrite::{ExpireRewrite, RewriteArg};

/// Number of decimal digits of `n`.
#[inline]
fn dec_len(n: u64) -> usize {
    n.checked_ilog10().map_or(1, |l| l as usize + 1)
}

/// Length of the decimal rendering of `n`, sign included.
#[inline]
fn int_len(n: i64) -> usize {
    if n < 0 {
        1 + dec_len(n.unsigned_abs())
    } else {
        dec_len(n as u64)
    }
}

/// `$<len>\r\n<payload>\r\n`
#[inline]
fn bulk_len(payload: usize) -> usize {
    1 + dec_len(payload as u64) + 2 + payload + 2
}

/// `*<n>\r\n` / `%`-less array header.
#[inline]
fn array_header_len(n: usize) -> usize {
    1 + dec_len(n as u64) + 2
}

/// Byte count of `f`'s `Display` rendering, without allocating — the RESP2
/// serializer writes a finite `Double` with `{}`.
fn display_len(f: f64) -> usize {
    struct Count(usize);
    impl std::fmt::Write for Count {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            self.0 += s.len();
            Ok(())
        }
    }
    let mut c = Count(0);
    let _ = std::fmt::Write::write_fmt(&mut c, format_args!("{f}"));
    c.0
}

/// Exact number of bytes `protocol::serialize::serialize` writes for `frame`
/// (the RESP2 wire form every log record uses).
pub(crate) fn resp_len(frame: &Frame) -> usize {
    match frame {
        // `put_line` maps CR/LF to a space: the length is unchanged.
        Frame::SimpleString(s) | Frame::Error(s) => 1 + s.len() + 2,
        Frame::Integer(n) => 1 + int_len(*n) + 2,
        Frame::BulkString(data) => bulk_len(data.len()),
        Frame::Array(items) | Frame::Set(items) | Frame::Push(items) => {
            array_header_len(items.len()) + items.iter().map(resp_len).sum::<usize>()
        }
        Frame::Null | Frame::NullArray => 5,
        Frame::Map(entries) => {
            array_header_len(entries.len() * 2)
                + entries
                    .iter()
                    .map(|(k, v)| resp_len(k) + resp_len(v))
                    .sum::<usize>()
        }
        Frame::Double(f) => {
            if f.is_infinite() {
                bulk_len(if f.is_sign_positive() { 3 } else { 4 })
            } else if f.is_nan() {
                bulk_len(3)
            } else {
                bulk_len(display_len(*f))
            }
        }
        Frame::Boolean(_) => 4,
        Frame::VerbatimString { data, .. } => bulk_len(data.len()),
        Frame::BigNumber(n) => bulk_len(n.len()),
        Frame::PreSerialized(data) => data.len(),
    }
}

/// Serialize a Frame into RESP wire format bytes — one exactly-sized
/// allocation.
pub(crate) fn serialize_command(frame: &Frame) -> Bytes {
    let len = resp_len(frame);
    let mut buf = BytesMut::with_capacity(len);
    serialize::serialize(frame, &mut buf);
    debug_assert_eq!(buf.len(), len, "resp_len must be exact");
    buf.freeze()
}

/// Wire length of one rewritten argument.
#[inline]
fn rewrite_arg_len(arg: RewriteArg<'_>) -> usize {
    match arg {
        RewriteArg::Verbatim(f) => resp_len(f),
        RewriteArg::Literal(s) => bulk_len(s.len()),
        RewriteArg::Millis(n) => bulk_len(dec_len(n)),
    }
}

/// `$<len>\r\n<payload>\r\n` into `buf` (capacity already reserved).
#[inline]
fn put_bulk(buf: &mut BytesMut, payload: &[u8]) {
    let mut n = itoa::Buffer::new();
    buf.put_u8(b'$');
    buf.put_slice(n.format(payload.len()).as_bytes());
    buf.put_slice(b"\r\n");
    buf.put_slice(payload);
    buf.put_slice(b"\r\n");
}

/// Serialize the absolute-deadline form of a relative-expiry command
/// directly from the borrowed original arguments — one exactly-sized
/// allocation, no intermediate `Frame`. Byte-identical to serializing
/// [`ExpireRewrite::to_frame`].
pub(crate) fn serialize_expire_rewrite(plan: &ExpireRewrite<'_>) -> Bytes {
    let count = plan.arg_count();
    let mut len = array_header_len(count);
    plan.for_each_arg(|arg| len += rewrite_arg_len(arg));

    let mut buf = BytesMut::with_capacity(len);
    let mut n = itoa::Buffer::new();
    buf.put_u8(b'*');
    buf.put_slice(n.format(count).as_bytes());
    buf.put_slice(b"\r\n");
    plan.for_each_arg(|arg| match arg {
        RewriteArg::Verbatim(f) => serialize::serialize(f, &mut buf),
        RewriteArg::Literal(s) => put_bulk(&mut buf, s),
        RewriteArg::Millis(ms) => {
            let mut d = itoa::Buffer::new();
            put_bulk(&mut buf, d.format(ms).as_bytes());
        }
    });
    debug_assert_eq!(buf.len(), len, "rewrite length must be exact");
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::FrameVec;
    use crate::replication::expire_rewrite::plan_expire_rewrite;

    fn b(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn cmd(parts: &[&[u8]]) -> Frame {
        Frame::Array(FrameVec::from_vec(parts.iter().map(|p| b(p)).collect()))
    }

    /// HEAD `935c555`'s record encoder, verbatim: the oracle the
    /// allocation-free encoder must match byte for byte.
    mod head {
        use super::super::serialize;
        use crate::protocol::{Frame, FrameVec};
        use bytes::{Bytes, BytesMut};

        fn bulk(s: &[u8]) -> Frame {
            Frame::BulkString(Bytes::copy_from_slice(s))
        }
        fn bulk_u64(n: u64) -> Frame {
            let mut b = itoa::Buffer::new();
            bulk(b.format(n).as_bytes())
        }
        fn arg_bytes(frame: &Frame) -> Option<&[u8]> {
            match frame {
                Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.as_ref()),
                _ => None,
            }
        }
        fn parse_i64(bytes: &[u8]) -> Option<i64> {
            std::str::from_utf8(bytes).ok()?.trim().parse::<i64>().ok()
        }
        fn eq(hay: &[u8], n: &[u8]) -> bool {
            hay.len() == n.len() && hay.eq_ignore_ascii_case(n)
        }
        fn abs_s(now: u64, s: i64) -> Option<u64> {
            if s <= 0 {
                return None;
            }
            (s as u64)
                .checked_mul(1000)
                .and_then(|d| now.checked_add(d))
        }
        fn abs_ms(now: u64, ms: i64) -> Option<u64> {
            if ms <= 0 {
                return None;
            }
            now.checked_add(ms as u64)
        }
        fn pexpireat(key: &Frame, abs: u64) -> Frame {
            Frame::Array(FrameVec::from_vec(vec![
                bulk(b"PEXPIREAT"),
                key.clone(),
                bulk_u64(abs),
            ]))
        }
        fn set_pxat(key: &Frame, value: &Frame, abs: u64) -> Frame {
            Frame::Array(FrameVec::from_vec(vec![
                bulk(b"SET"),
                key.clone(),
                value.clone(),
                bulk(b"PXAT"),
                bulk_u64(abs),
            ]))
        }
        pub(super) fn rewrite(frame: &Frame, now: u64) -> Option<Frame> {
            let args = match frame {
                Frame::Array(a) => a,
                _ => return None,
            };
            let cmd = arg_bytes(args.first()?)?;
            if eq(cmd, b"EXPIRE") {
                if args.len() != 3 {
                    return None;
                }
                let abs = abs_s(now, parse_i64(arg_bytes(&args[2])?)?)?;
                Some(pexpireat(&args[1], abs))
            } else if eq(cmd, b"PEXPIRE") {
                if args.len() != 3 {
                    return None;
                }
                let abs = abs_ms(now, parse_i64(arg_bytes(&args[2])?)?)?;
                Some(pexpireat(&args[1], abs))
            } else if eq(cmd, b"SETEX") {
                if args.len() != 4 {
                    return None;
                }
                let abs = abs_s(now, parse_i64(arg_bytes(&args[2])?)?)?;
                Some(set_pxat(&args[1], &args[3], abs))
            } else if eq(cmd, b"PSETEX") {
                if args.len() != 4 {
                    return None;
                }
                let abs = abs_ms(now, parse_i64(arg_bytes(&args[2])?)?)?;
                Some(set_pxat(&args[1], &args[3], abs))
            } else if eq(cmd, b"GETEX") {
                if args.len() != 4 {
                    return None;
                }
                let opt = arg_bytes(&args[2])?;
                let val = parse_i64(arg_bytes(&args[3])?)?;
                let abs = if eq(opt, b"EX") {
                    abs_s(now, val)?
                } else if eq(opt, b"PX") {
                    abs_ms(now, val)?
                } else {
                    return None;
                };
                Some(pexpireat(&args[1], abs))
            } else if eq(cmd, b"SET") {
                if args.len() < 3 {
                    return None;
                }
                let mut out: Vec<Frame> = Vec::with_capacity(args.len());
                let mut i = 0;
                let mut rewrote = false;
                while i < args.len() {
                    let a = arg_bytes(&args[i]).unwrap_or(b"");
                    if i >= 3 && (eq(a, b"EX") || eq(a, b"PX")) {
                        let val = parse_i64(arg_bytes(args.get(i + 1)?)?)?;
                        let abs = if eq(a, b"EX") {
                            abs_s(now, val)?
                        } else {
                            abs_ms(now, val)?
                        };
                        out.push(bulk(b"PXAT"));
                        out.push(bulk_u64(abs));
                        i += 2;
                        rewrote = true;
                    } else if i >= 3 && (eq(a, b"EXAT") || eq(a, b"PXAT")) {
                        return None;
                    } else {
                        out.push(args[i].clone());
                        i += 1;
                    }
                }
                if rewrote {
                    Some(Frame::Array(FrameVec::from_vec(out)))
                } else {
                    None
                }
            } else {
                None
            }
        }
        pub(super) fn serialize_command(frame: &Frame) -> Bytes {
            let mut buf = BytesMut::with_capacity(64);
            serialize::serialize(frame, &mut buf);
            buf.freeze()
        }
        pub(super) fn serialize_for_log(frame: &Frame, now: u64) -> Bytes {
            match rewrite(frame, now) {
                Some(r) => serialize_command(&r),
                None => serialize_command(frame),
            }
        }
    }

    /// Commands a durable log sees, including every expiry form, malformed
    /// and overflowing TTLs, option values that spell option names, and
    /// non-bulk arguments.
    fn corpus() -> Vec<Frame> {
        let big = vec![b'x'; 70_000];
        let mut v: Vec<Frame> = [
            &[&b"SET"[..], b"k", b"v"][..],
            &[b"set", b"k", b"v"],
            &[b"SET", b"k", b"v", b"EX", b"100"],
            &[b"SET", b"k", b"v", b"ex", b"100"],
            &[b"SET", b"k", b"v", b"PX", b"250"],
            &[b"SET", b"k", b"v", b"NX", b"EX", b"100", b"GET"],
            &[b"SET", b"k", b"v", b"XX", b"PX", b"1", b"KEEPTTL"],
            &[b"SET", b"k", b"v", b"EXAT", b"99999999"],
            &[b"SET", b"k", b"v", b"PXAT", b"9999999999999"],
            &[b"SET", b"k", b"v", b"KEEPTTL"],
            &[b"SET", b"k", b"v", b"GET"],
            &[b"SET", b"k", b"EX", b"EX", b"100"],
            &[b"SET", b"k", b"v", b"EX", b"0"],
            &[b"SET", b"k", b"v", b"EX", b"-5"],
            &[b"SET", b"k", b"v", b"EX", b"abc"],
            &[b"SET", b"k", b"v", b"EX"],
            &[b"SET", b"k", b"v", b"EX", b"9223372036854775807"],
            &[b"SET", b"k", b"v", b"PX", b"9223372036854775807"],
            &[b"SET", b"k", b"v", b"EX", b"10", b"PX", b"20"],
            &[b"SET", b"k", b"v", b"EX", b"10", b"EXAT", b"20"],
            &[b"SET", b"k", b"v", b"EX", b" 10 "],
            &[b"SET", b"k", b"v", b"EX", b"+10"],
            &[b"SET", b"k"],
            &[b"SETEX", b"k", b"100", b"v"],
            &[b"setex", b"k", b"0", b"v"],
            &[b"SETEX", b"k", b"100"],
            &[b"PSETEX", b"k", b"500", b"v"],
            &[b"PSETEX", b"k", b"-1", b"v"],
            &[b"EXPIRE", b"k", b"100"],
            &[b"EXPIRE", b"k", b"0"],
            &[b"EXPIRE", b"k", b"100", b"NX"],
            &[b"EXPIRE", b"k", b"abc"],
            &[b"PEXPIRE", b"k", b"250"],
            &[b"PEXPIRE", b"k", b"-1"],
            &[b"EXPIREAT", b"k", b"99999999"],
            &[b"PEXPIREAT", b"k", b"9999999999999"],
            &[b"GETEX", b"k", b"EX", b"100"],
            &[b"GETEX", b"k", b"px", b"100"],
            &[b"GETEX", b"k", b"PXAT", b"9999999999999"],
            &[b"GETEX", b"k", b"PERSIST"],
            &[b"GETEX", b"k"],
            &[b"HSET", b"h", b"f", b"v"],
            &[b"HSET", b"h", b"EX", b"100"],
            &[b"INCR", b"counter"],
            &[b"DEL", b"a", b"b", b"c", b"d", b"e"],
            &[b"LPUSH", b"l", b"", b"a"],
            &[b"SELECT", b"3"],
        ]
        .iter()
        .map(|p| cmd(p))
        .collect();
        v.push(Frame::Array(FrameVec::from_vec(vec![
            b(b"SET"),
            b(b"big"),
            Frame::BulkString(Bytes::from(big)),
            b(b"EX"),
            b(b"7"),
        ])));
        // Non-bulk arguments and nested / RESP3 shapes.
        v.push(Frame::Array(FrameVec::from_vec(vec![
            b(b"SET"),
            b(b"k"),
            b(b"v"),
            Frame::Integer(-42),
            b(b"EX"),
            b(b"3"),
        ])));
        v.push(Frame::Array(FrameVec::from_vec(vec![
            b(b"SET"),
            b(b"k"),
            b(b"v"),
            b(b"EX"),
            Frame::Integer(3),
        ])));
        v.push(Frame::Array(FrameVec::from_vec(vec![
            Frame::SimpleString(Bytes::from_static(b"EXPIRE")),
            Frame::SimpleString(Bytes::from_static(b"k")),
            Frame::SimpleString(Bytes::from_static(b"9")),
        ])));
        v.push(Frame::Array(FrameVec::from_vec(vec![
            b(b"XADD"),
            Frame::Null,
            Frame::NullArray,
            Frame::Double(1.5),
            Frame::Double(f64::INFINITY),
            Frame::Double(f64::NEG_INFINITY),
            Frame::Double(f64::NAN),
            Frame::Double(5e-324),
            Frame::Boolean(true),
            Frame::BigNumber(Bytes::from_static(b"123456789012345678901234567890")),
            Frame::VerbatimString {
                encoding: *b"txt",
                data: Bytes::from_static(b"hello"),
            },
            Frame::Map(vec![(b(b"a"), Frame::Integer(i64::MIN))]),
            Frame::Error(Bytes::from_static(b"ERR\r\nx")),
            Frame::Set(FrameVec::from_vec(vec![b(b"m")])),
            Frame::PreSerialized(Bytes::from_static(b"$1\r\nz\r\n")),
        ])));
        v.push(Frame::Integer(0));
        v.push(b(b"bare"));
        v
    }

    /// The allocation-free encoder is byte-identical to HEAD's for every
    /// frame of the corpus, at several clocks (moon#1187).
    #[test]
    fn encode_matches_head_for_corpus() {
        for now in [0u64, 1, 1_000_000_000_000, u64::MAX - 5] {
            for f in corpus() {
                let expected = head::serialize_for_log(&f, now);
                let got = match plan_expire_rewrite(&f, now) {
                    Some(plan) => serialize_expire_rewrite(&plan),
                    None => serialize_command(&f),
                };
                assert_eq!(got, expected, "frame {f:?} at now={now}");
                // And the frame-building API agrees with both.
                let via_frame = match plan_expire_rewrite(&f, now) {
                    Some(plan) => head::serialize_command(&plan.to_frame()),
                    None => head::serialize_command(&f),
                };
                assert_eq!(via_frame, expected, "to_frame for {f:?}");
                // Rewrite decision is unchanged.
                assert_eq!(
                    crate::replication::expire_rewrite::rewrite_expire_for_propagation(&f, now),
                    head::rewrite(&f, now),
                    "decision for {f:?}"
                );
            }
        }
    }

    /// `resp_len` is exact for every frame shape (the pre-size never grows).
    #[test]
    fn resp_len_is_exact() {
        for f in corpus() {
            let mut buf = BytesMut::new();
            serialize::serialize(&f, &mut buf);
            assert_eq!(resp_len(&f), buf.len(), "frame {f:?}");
        }
        for n in [0i64, 9, 10, 99, 100, -1, -10, i64::MAX, i64::MIN] {
            let f = Frame::Integer(n);
            let mut buf = BytesMut::new();
            serialize::serialize(&f, &mut buf);
            assert_eq!(resp_len(&f), buf.len(), "integer {n}");
        }
    }
}

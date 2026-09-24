//! Master-side rewrite of relative-expiry commands to absolute deadlines for
//! deterministic propagation (#71a).
//!
//! A relative-TTL command (`EXPIRE k 100`, `SETEX k 100 v`, `SET k v EX 100`,
//! `GETEX k EX 100`, `PEXPIRE`/`PSETEX`/`PX` forms) means "expire 100 units
//! from *now*". If that verbatim command is streamed to a replica (or replayed
//! from the AOF), the countdown restarts when the replica **applies** it — so
//! replication/apply delay (and any master/replica clock skew) shifts the
//! key's expiry moment. Redis solves this by rewriting the propagated form to
//! an **absolute** deadline (`PEXPIREAT` / `SET ... PXAT`) computed once, on the
//! master, at execution time.
//!
//! This module is a **pure** transform: `(frame, now_ms) -> Option<Frame>`.
//! It returns `Some(rewritten)` for a relative-expiry command with a positive
//! TTL and `None` for everything else (propagate verbatim). Because the shard
//! timestamp is cached per event-loop tick — not per command — recomputing
//! `now_ms + relative` here yields the **exact** absolute deadline the command
//! handler already stored (`current_time_ms()` is identical within a tick), so
//! the replica's key expires at the same wall-clock instant as the master's.
//!
//! Non-positive TTLs (past-time deletes) and already-absolute forms
//! (`EXPIREAT`, `PEXPIREAT`, `EXAT`, `PXAT`, `PERSIST`) are left verbatim: they
//! are already deterministic across nodes.

use crate::protocol::{Frame, FrameVec};
use bytes::Bytes;

/// Extract the raw bytes of a `BulkString`/`SimpleString` arg.
#[inline]
fn arg_bytes(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.as_ref()),
        _ => None,
    }
}

/// Parse a base-10 `i64` from raw bytes (leading `+`/`-` allowed), matching the
/// command handlers' `parse_int` acceptance. Returns `None` on any garbage.
fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.trim().parse::<i64>().ok()
}

/// ASCII-case-insensitive equality against an already-uppercase needle.
#[inline]
fn eq_ignore_ascii(hay: &[u8], upper_needle: &[u8]) -> bool {
    hay.len() == upper_needle.len() && hay.eq_ignore_ascii_case(upper_needle)
}

/// Compute `now_ms + seconds * 1000` with overflow guards; `None` on overflow
/// or a non-positive input (caller propagates verbatim in that case).
#[inline]
fn abs_from_seconds(now_ms: u64, seconds: i64) -> Option<u64> {
    if seconds <= 0 {
        return None;
    }
    (seconds as u64)
        .checked_mul(1000)
        .and_then(|delta| now_ms.checked_add(delta))
}

/// Compute `now_ms + millis` with overflow guards; `None` on overflow or a
/// non-positive input.
#[inline]
fn abs_from_millis(now_ms: u64, millis: i64) -> Option<u64> {
    if millis <= 0 {
        return None;
    }
    now_ms.checked_add(millis as u64)
}

/// One argument of a rewritten command, borrowed or computed — never
/// allocated. A serializer writes these straight into its output buffer
/// ([`crate::persistence::aof::serialize_command_for_log`]); only
/// [`ExpireRewrite::to_frame`] materializes them as frames.
#[derive(Debug, Clone, Copy)]
pub enum RewriteArg<'a> {
    /// An argument of the original command, propagated as-is.
    Verbatim(&'a Frame),
    /// A command name or option keyword (`PEXPIREAT`, `SET`, `PXAT`).
    Literal(&'static [u8]),
    /// An absolute deadline in unix milliseconds, rendered in decimal.
    Millis(u64),
}

/// The absolute-deadline form of a relative-expiry command, decided WITHOUT
/// allocating (moon#1187).
///
/// Every write with at least three arguments reaches this rewrite on its way
/// to the AOF and the replication stream, and nearly all of them — a plain
/// `SET k v`, an `HSET`, an `SET … EXAT` — need no rewrite at all. The
/// decision therefore borrows the original arguments and allocates nothing;
/// the caller serializes the rewritten command directly into its record
/// buffer via [`Self::for_each_arg`].
#[derive(Debug, Clone, Copy)]
pub enum ExpireRewrite<'a> {
    /// `PEXPIREAT key <abs_ms>` — from `EXPIRE`, `PEXPIRE`, `GETEX … EX|PX`.
    PexpireAt { key: &'a Frame, abs_ms: u64 },
    /// `SET key value PXAT <abs_ms>` — from `SETEX` / `PSETEX`.
    SetPxat {
        key: &'a Frame,
        value: &'a Frame,
        abs_ms: u64,
    },
    /// `SET key value [options…]` with every relative `EX s` / `PX ms` option
    /// pair at index >= 3 replaced by `PXAT <abs_ms>`, every other argument
    /// propagated verbatim and in order. The pairs were validated when the
    /// rewrite was planned, so replaying them cannot fail.
    Set { args: &'a [Frame], now_ms: u64 },
}

impl<'a> ExpireRewrite<'a> {
    /// Number of arguments of the rewritten command (the RESP array length).
    #[inline]
    pub fn arg_count(&self) -> usize {
        match self {
            ExpireRewrite::PexpireAt { .. } => 3,
            ExpireRewrite::SetPxat { .. } => 5,
            // Each `EX s` / `PX ms` pair becomes a `PXAT abs` pair: the
            // argument count is unchanged.
            ExpireRewrite::Set { args, .. } => args.len(),
        }
    }

    /// Visit the rewritten command's arguments in order.
    pub fn for_each_arg(&self, mut f: impl FnMut(RewriteArg<'a>)) {
        match *self {
            ExpireRewrite::PexpireAt { key, abs_ms } => {
                f(RewriteArg::Literal(b"PEXPIREAT"));
                f(RewriteArg::Verbatim(key));
                f(RewriteArg::Millis(abs_ms));
            }
            ExpireRewrite::SetPxat { key, value, abs_ms } => {
                f(RewriteArg::Literal(b"SET"));
                f(RewriteArg::Verbatim(key));
                f(RewriteArg::Verbatim(value));
                f(RewriteArg::Literal(b"PXAT"));
                f(RewriteArg::Millis(abs_ms));
            }
            ExpireRewrite::Set { args, now_ms } => {
                let mut i = 0;
                while i < args.len() {
                    if i >= 3
                        && let Some(abs) = relative_option_at(args, i, now_ms)
                    {
                        f(RewriteArg::Literal(b"PXAT"));
                        f(RewriteArg::Millis(abs));
                        i += 2;
                    } else {
                        f(RewriteArg::Verbatim(&args[i]));
                        i += 1;
                    }
                }
            }
        }
    }

    /// Materialize the rewritten command as a frame. Allocates; the
    /// propagation hot path serializes through [`Self::for_each_arg`]
    /// instead.
    pub fn to_frame(&self) -> Frame {
        let mut out: Vec<Frame> = Vec::with_capacity(self.arg_count());
        self.for_each_arg(|arg| {
            out.push(match arg {
                RewriteArg::Verbatim(f) => f.clone(),
                RewriteArg::Literal(s) => Frame::BulkString(Bytes::from_static(s)),
                RewriteArg::Millis(n) => {
                    let mut b = itoa::Buffer::new();
                    Frame::BulkString(Bytes::copy_from_slice(b.format(n).as_bytes()))
                }
            })
        });
        Frame::Array(FrameVec::from_vec(out))
    }
}

/// When `args[i]` is a relative `EX`/`PX` option whose value parses and whose
/// deadline is representable, the absolute deadline it rewrites to.
///
/// [`plan_set`] has already proven every such pair valid for the args it
/// accepted, so during replay this returning `None` means "not an EX/PX
/// option", never "invalid".
#[inline]
fn relative_option_at(args: &[Frame], i: usize, now_ms: u64) -> Option<u64> {
    let a = arg_bytes(&args[i]).unwrap_or(b"");
    let seconds = if eq_ignore_ascii(a, b"EX") {
        true
    } else if eq_ignore_ascii(a, b"PX") {
        false
    } else {
        return None;
    };
    let val = parse_i64(arg_bytes(args.get(i + 1)?)?)?;
    if seconds {
        abs_from_seconds(now_ms, val)
    } else {
        abs_from_millis(now_ms, val)
    }
}

/// Decide the absolute-deadline propagation form of a relative-expiry
/// command without allocating.
///
/// Returns `Some(plan)` when `frame` is a relative-expiry command with a
/// positive TTL; `None` otherwise (the caller propagates `frame` verbatim).
/// `now_ms` MUST be the master's execution-time millisecond clock
/// (`current_time_ms()`), so the absolute deadline matches the value the
/// command handler stored.
pub fn plan_expire_rewrite(frame: &Frame, now_ms: u64) -> Option<ExpireRewrite<'_>> {
    let args = match frame {
        Frame::Array(a) => a.as_slice(),
        _ => return None,
    };
    let cmd = arg_bytes(args.first()?)?;
    // Dispatch on the name length first: almost every write is neither of
    // the six families, and a length mismatch rejects it in one compare.
    match cmd.len() {
        3 if eq_ignore_ascii(cmd, b"SET") => plan_set(args, now_ms),
        5 if eq_ignore_ascii(cmd, b"SETEX") => {
            // SETEX key seconds value  ->  SET key value PXAT <now + seconds*1000>
            if args.len() != 4 {
                return None;
            }
            let abs = abs_from_seconds(now_ms, parse_i64(arg_bytes(&args[2])?)?)?;
            Some(ExpireRewrite::SetPxat {
                key: &args[1],
                value: &args[3],
                abs_ms: abs,
            })
        }
        5 if eq_ignore_ascii(cmd, b"GETEX") => plan_getex(args, now_ms),
        6 if eq_ignore_ascii(cmd, b"EXPIRE") => {
            // EXPIRE key seconds  ->  PEXPIREAT key <now + seconds*1000>
            if args.len() != 3 {
                return None;
            }
            let abs = abs_from_seconds(now_ms, parse_i64(arg_bytes(&args[2])?)?)?;
            Some(ExpireRewrite::PexpireAt {
                key: &args[1],
                abs_ms: abs,
            })
        }
        6 if eq_ignore_ascii(cmd, b"PSETEX") => {
            // PSETEX key millis value  ->  SET key value PXAT <now + millis>
            if args.len() != 4 {
                return None;
            }
            let abs = abs_from_millis(now_ms, parse_i64(arg_bytes(&args[2])?)?)?;
            Some(ExpireRewrite::SetPxat {
                key: &args[1],
                value: &args[3],
                abs_ms: abs,
            })
        }
        7 if eq_ignore_ascii(cmd, b"PEXPIRE") => {
            // PEXPIRE key millis  ->  PEXPIREAT key <now + millis>
            if args.len() != 3 {
                return None;
            }
            let abs = abs_from_millis(now_ms, parse_i64(arg_bytes(&args[2])?)?)?;
            Some(ExpireRewrite::PexpireAt {
                key: &args[1],
                abs_ms: abs,
            })
        }
        _ => None,
    }
}

/// Rewrite a relative-expiry command to its absolute-deadline propagation form.
///
/// Returns `Some(rewritten_frame)` when `frame` is a relative-expiry command
/// with a positive TTL; `None` otherwise (the caller propagates `frame`
/// verbatim). `now_ms` MUST be the master's execution-time millisecond clock
/// (`current_time_ms()`), so the absolute deadline matches the value the
/// command handler stored.
///
/// Allocates the rewritten frame; the AOF / replication hot path uses
/// [`plan_expire_rewrite`] and serializes without building one.
pub fn rewrite_expire_for_propagation(frame: &Frame, now_ms: u64) -> Option<Frame> {
    plan_expire_rewrite(frame, now_ms).map(|plan| plan.to_frame())
}

/// GETEX rewrite: only the relative `EX`/`PX` forms need rewriting to
/// `PEXPIREAT`. `EXAT`/`PXAT` are already absolute, `PERSIST` and the bare
/// (read-only) form are deterministic — all propagate verbatim (`None`).
fn plan_getex(args: &[Frame], now_ms: u64) -> Option<ExpireRewrite<'_>> {
    // GETEX key [EX s | PX ms | EXAT ts | PXAT ms | PERSIST]
    if args.len() != 4 {
        return None;
    }
    let opt = arg_bytes(&args[2])?;
    let val = parse_i64(arg_bytes(&args[3])?)?;
    let abs = if eq_ignore_ascii(opt, b"EX") {
        abs_from_seconds(now_ms, val)?
    } else if eq_ignore_ascii(opt, b"PX") {
        abs_from_millis(now_ms, val)?
    } else {
        return None; // EXAT/PXAT already absolute; anything else: verbatim
    };
    Some(ExpireRewrite::PexpireAt {
        key: &args[1],
        abs_ms: abs,
    })
}

/// SET rewrite: replace a relative `EX <s>` / `PX <ms>` option pair with
/// `PXAT <abs_ms>`, preserving every other argument (`NX`/`XX`/`GET`/`KEEPTTL`)
/// in order. Already-absolute (`EXAT`/`PXAT`) or no-expiry SETs propagate
/// verbatim (`None`).
///
/// A read-only pre-scan (moon#1187): the old form cloned every argument into
/// a fresh `Vec` before it knew whether anything needed rewriting, and threw
/// the `Vec` away for the common no-expiry `SET`. The scan visits the options
/// exactly as that loop did — an `EX`/`PX` pair is consumed whole (its value
/// is never re-read as an option), an `EXAT`/`PXAT` or a malformed/overflowing
/// relative value aborts the rewrite — so it accepts exactly the commands the
/// old loop rewrote.
fn plan_set(args: &[Frame], now_ms: u64) -> Option<ExpireRewrite<'_>> {
    // SET key value [options...]
    if args.len() < 3 {
        return None;
    }
    let mut i = 3;
    let mut rewrote = false;
    while i < args.len() {
        let a = arg_bytes(&args[i]).unwrap_or(b"");
        if eq_ignore_ascii(a, b"EX") || eq_ignore_ascii(a, b"PX") {
            // Relative expiry option: needs a following numeric value whose
            // deadline is representable.
            relative_option_at(args, i, now_ms)?;
            i += 2;
            rewrote = true;
        } else if eq_ignore_ascii(a, b"EXAT") || eq_ignore_ascii(a, b"PXAT") {
            // Already absolute — nothing to make deterministic, propagate verbatim.
            return None;
        } else {
            i += 1;
        }
    }
    if rewrote {
        Some(ExpireRewrite::Set { args, now_ms })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_000_000_000_000; // fixed master clock

    fn bulk(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn cmd(parts: &[&[u8]]) -> Frame {
        Frame::Array(FrameVec::from_vec(
            parts.iter().map(|p| bulk(p)).collect::<Vec<_>>(),
        ))
    }

    /// Extract the args of a rewritten Array frame as owned byte vecs.
    fn parts_of(f: &Frame) -> Vec<Vec<u8>> {
        match f {
            Frame::Array(a) => a.iter().map(|x| arg_bytes(x).unwrap().to_vec()).collect(),
            _ => panic!("not an array"),
        }
    }

    #[test]
    fn expire_rewrites_to_pexpireat() {
        let f = rewrite_expire_for_propagation(&cmd(&[b"EXPIRE", b"k", b"100"]), NOW).unwrap();
        assert_eq!(
            parts_of(&f),
            vec![
                b"PEXPIREAT".to_vec(),
                b"k".to_vec(),
                b"1000000100000".to_vec()
            ]
        );
    }

    #[test]
    fn expire_case_insensitive() {
        let f = rewrite_expire_for_propagation(&cmd(&[b"expire", b"k", b"1"]), NOW).unwrap();
        assert_eq!(parts_of(&f)[0], b"PEXPIREAT".to_vec());
        assert_eq!(parts_of(&f)[2], b"1000000001000".to_vec());
    }

    #[test]
    fn pexpire_rewrites_to_pexpireat_ms() {
        let f = rewrite_expire_for_propagation(&cmd(&[b"PEXPIRE", b"k", b"250"]), NOW).unwrap();
        assert_eq!(parts_of(&f)[2], b"1000000000250".to_vec());
    }

    #[test]
    fn non_positive_ttl_left_verbatim() {
        // Past-time delete: both nodes delete identically, no rewrite.
        assert!(rewrite_expire_for_propagation(&cmd(&[b"EXPIRE", b"k", b"0"]), NOW).is_none());
        assert!(rewrite_expire_for_propagation(&cmd(&[b"EXPIRE", b"k", b"-5"]), NOW).is_none());
        assert!(rewrite_expire_for_propagation(&cmd(&[b"PEXPIRE", b"k", b"-1"]), NOW).is_none());
    }

    #[test]
    fn setex_rewrites_to_set_pxat() {
        let f = rewrite_expire_for_propagation(&cmd(&[b"SETEX", b"k", b"100", b"v"]), NOW).unwrap();
        assert_eq!(
            parts_of(&f),
            vec![
                b"SET".to_vec(),
                b"k".to_vec(),
                b"v".to_vec(),
                b"PXAT".to_vec(),
                b"1000000100000".to_vec(),
            ]
        );
    }

    #[test]
    fn psetex_rewrites_to_set_pxat_ms() {
        let f =
            rewrite_expire_for_propagation(&cmd(&[b"PSETEX", b"k", b"500", b"v"]), NOW).unwrap();
        assert_eq!(parts_of(&f)[3], b"PXAT".to_vec());
        assert_eq!(parts_of(&f)[4], b"1000000000500".to_vec());
    }

    #[test]
    fn getex_ex_rewrites_to_pexpireat() {
        let f =
            rewrite_expire_for_propagation(&cmd(&[b"GETEX", b"k", b"EX", b"100"]), NOW).unwrap();
        assert_eq!(parts_of(&f)[0], b"PEXPIREAT".to_vec());
        assert_eq!(parts_of(&f)[2], b"1000000100000".to_vec());
    }

    #[test]
    fn getex_persist_and_absolute_left_verbatim() {
        assert!(rewrite_expire_for_propagation(&cmd(&[b"GETEX", b"k", b"PERSIST"]), NOW).is_none());
        assert!(
            rewrite_expire_for_propagation(&cmd(&[b"GETEX", b"k", b"PXAT", b"9999999999999"]), NOW)
                .is_none()
        );
        // Bare GETEX (pure read) — nothing to rewrite.
        assert!(rewrite_expire_for_propagation(&cmd(&[b"GETEX", b"k"]), NOW).is_none());
    }

    #[test]
    fn set_with_ex_rewrites_pxat_preserving_options() {
        let f = rewrite_expire_for_propagation(
            &cmd(&[b"SET", b"k", b"v", b"NX", b"EX", b"100", b"GET"]),
            NOW,
        )
        .unwrap();
        assert_eq!(
            parts_of(&f),
            vec![
                b"SET".to_vec(),
                b"k".to_vec(),
                b"v".to_vec(),
                b"NX".to_vec(),
                b"PXAT".to_vec(),
                b"1000000100000".to_vec(),
                b"GET".to_vec(),
            ]
        );
    }

    #[test]
    fn set_with_px_rewrites_pxat_ms() {
        let f = rewrite_expire_for_propagation(&cmd(&[b"SET", b"k", b"v", b"PX", b"750"]), NOW)
            .unwrap();
        assert_eq!(parts_of(&f)[3], b"PXAT".to_vec());
        assert_eq!(parts_of(&f)[4], b"1000000000750".to_vec());
    }

    #[test]
    fn set_already_absolute_left_verbatim() {
        assert!(
            rewrite_expire_for_propagation(
                &cmd(&[b"SET", b"k", b"v", b"PXAT", b"9999999999999"]),
                NOW
            )
            .is_none()
        );
        assert!(
            rewrite_expire_for_propagation(&cmd(&[b"SET", b"k", b"v", b"EXAT", b"99999999"]), NOW)
                .is_none()
        );
    }

    #[test]
    fn set_without_expiry_left_verbatim() {
        assert!(rewrite_expire_for_propagation(&cmd(&[b"SET", b"k", b"v"]), NOW).is_none());
        assert!(
            rewrite_expire_for_propagation(&cmd(&[b"SET", b"k", b"v", b"KEEPTTL"]), NOW).is_none()
        );
        assert!(rewrite_expire_for_propagation(&cmd(&[b"SET", b"k", b"v", b"NX"]), NOW).is_none());
    }

    #[test]
    fn set_keeps_value_that_looks_like_option() {
        // The VALUE (arg index 2) is never scanned as an option, even if it
        // spells "EX" — only args at index >= 3 are options.
        let f = rewrite_expire_for_propagation(&cmd(&[b"SET", b"k", b"EX", b"EX", b"100"]), NOW)
            .unwrap();
        // value "EX" preserved at index 2; the option EX at index 3 rewritten.
        assert_eq!(parts_of(&f)[2], b"EX".to_vec());
        assert_eq!(parts_of(&f)[3], b"PXAT".to_vec());
        assert_eq!(parts_of(&f)[4], b"1000000100000".to_vec());
    }

    #[test]
    fn unrelated_commands_left_verbatim() {
        assert!(rewrite_expire_for_propagation(&cmd(&[b"GET", b"k"]), NOW).is_none());
        assert!(rewrite_expire_for_propagation(&cmd(&[b"DEL", b"k"]), NOW).is_none());
        // Already-absolute EXPIREAT/PEXPIREAT: deterministic, propagate verbatim.
        assert!(
            rewrite_expire_for_propagation(&cmd(&[b"PEXPIREAT", b"k", b"9999999999999"]), NOW)
                .is_none()
        );
        assert!(
            rewrite_expire_for_propagation(&cmd(&[b"EXPIREAT", b"k", b"99999999"]), NOW).is_none()
        );
    }

    #[test]
    fn overflow_ttl_falls_back_to_verbatim() {
        // seconds*1000 overflows u64 -> None (master already rejected as
        // out-of-range; verbatim is the safe fallback).
        assert!(
            rewrite_expire_for_propagation(&cmd(&[b"EXPIRE", b"k", b"9223372036854775807"]), NOW)
                .is_none()
        );
    }

    #[test]
    fn malformed_ttl_falls_back_to_verbatim() {
        assert!(rewrite_expire_for_propagation(&cmd(&[b"EXPIRE", b"k", b"abc"]), NOW).is_none());
        assert!(rewrite_expire_for_propagation(&cmd(&[b"EXPIRE", b"k"]), NOW).is_none());
    }
}

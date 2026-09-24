use bytes::{Bytes, BytesMut};

use crate::protocol::Frame;

/// Return ERR wrong number of arguments for a given command.
///
/// The name is normalised to the form Redis registers commands under, because
/// that is what Redis interpolates here (`commandCheckArity` formats with the
/// command table's `->fullname`) and clients string-match the result (#491):
///
///   * **lower case** — Redis normalises down regardless of what the client
///     sent, so `echo`, `ECHO` and `EcHo` all produce `'echo'`;
///   * **`parent|sub`** for a container command — `MEMORY USAGE` is
///     `'memory|usage'`, not `'memory usage'`. Callers pass the human form with
///     a space, so the space is translated here rather than at 700 call sites.
///
/// Measured against redis-server 8.0.5: `memory|usage`, `xgroup|create`,
/// `xinfo|consumers`, `object|encoding`, `client|setname`, `acl|getuser`.
/// Underscores are NOT separators and stay put (`sort_ro`, `bitfield_ro`).
///
/// This is only the ARITY message. Redis does *not* normalise the name in
/// `unknown command '<x>'`, which echoes back what the client actually sent —
/// see the guard test in `tests/wire_parity_naming_and_shards.rs`.
pub fn err_wrong_args(cmd: &str) -> Frame {
    const PREFIX: &str = "ERR wrong number of arguments for '";
    const SUFFIX: &str = "' command";
    let mut msg = String::with_capacity(PREFIX.len() + cmd.len() + SUFFIX.len());
    msg.push_str(PREFIX);
    for c in cmd.chars() {
        msg.push(if c == ' ' {
            '|'
        } else {
            c.to_ascii_lowercase()
        });
    }
    msg.push_str(SUFFIX);
    Frame::Error(Bytes::from(msg))
}

/// Redis's budget for the unknown-command argument echo below: `call()`
/// (`src/server.c`) stops concatenating `'<arg>' ` pieces once their combined
/// length reaches 128 bytes. Measured, not documented — an argument long
/// enough to reach the budget on its own is truncated mid-argument, and a
/// budget already spent by earlier arguments shrinks the next one's slice.
const UNKNOWN_COMMAND_ARGS_BUDGET: usize = 128;

/// Build `unknown command` the way Redis does — moon#1077.
///
/// Every one of the three dispatch paths that can decide a command is unknown
/// (`command::dispatch`, `command::dispatch_read`, and the `MULTI` queue-time
/// gate in `server::conn::shared::queue_time_rejection`) calls this, so the
/// three cannot drift the way they had: the live paths never listed the
/// arguments at all, and the queue-time gate listed them with `', '` between
/// entries where Redis has no comma, just `'a' 'b' `.
///
/// Measured against redis-server 8.6.1, raw socket, one case per row:
///
/// ```text
/// NOSUCHCMD            -> ERR unknown command 'NOSUCHCMD'
/// NOSUCHCMD a b        -> ERR unknown command 'NOSUCHCMD', with args beginning with: 'a' 'b'
/// nosuchcmd            -> ERR unknown command 'nosuchcmd'            (client's OWN casing —
/// NoSuchCmd a b        -> ERR unknown command 'NoSuchCmd', with args beginning with: 'a' 'b'   this is
///                          the OPPOSITE of the arity error, which lower-cases; see
///                          reference_redis_error_name_two_rules)
/// ```
///
/// Two behaviours a naive `format!` misses:
///
/// * **The suffix is absent, not empty, with zero arguments.** `NOSUCHCMD`
///   alone has no `, with args beginning with:` at all — that clause only
///   appears once there is at least one argument to list.
/// * **An argument is truncated at its first embedded NUL.** Redis formats
///   each piece with C's `%.*s`, which reads the argument as a NUL-terminated
///   string despite Moon's (and Redis's own) values being binary-safe. An
///   argument starting with `\0` therefore prints as `''`, not the byte.
///
/// `cmd` and every argument are raw, client-controlled bytes — built here with
/// `BytesMut`, never `String::from_utf8_lossy`, so a non-UTF8 byte (`\xff`)
/// reaches the wire unchanged instead of becoming a 3-byte replacement
/// character. CR and LF ARE substituted with a space: this reply is one RESP
/// error line, and passing either through verbatim lets a client-chosen
/// argument split it into extra, forged replies on a pipelined connection
/// (moon#1031's class of bug, general form still open; this is the narrow
/// fix for the one builder moon#1077 adds argument-echoing to).
pub fn err_unknown_command(cmd: &[u8], args: &[Frame]) -> Frame {
    let mut msg = BytesMut::with_capacity(24 + cmd.len());
    msg.extend_from_slice(b"ERR unknown command '");
    push_line_safe(&mut msg, cmd);
    msg.extend_from_slice(b"'");
    if !args.is_empty() {
        msg.extend_from_slice(b", with args beginning with: ");
        let mut used = 0usize;
        for a in args {
            if used >= UNKNOWN_COMMAND_ARGS_BUDGET {
                break;
            }
            let bytes: &[u8] = match a {
                Frame::BulkString(b) | Frame::SimpleString(b) => b.as_ref(),
                _ => &[],
            };
            let remaining = UNKNOWN_COMMAND_ARGS_BUDGET - used;
            let capped = &bytes[..bytes.len().min(remaining)];
            let cut = capped.iter().position(|&b| b == 0).unwrap_or(capped.len());
            let piece = &capped[..cut];
            msg.extend_from_slice(b"'");
            push_line_safe(&mut msg, piece);
            msg.extend_from_slice(b"' ");
            used += cut + 3; // 2 quotes + 1 trailing space, matching Redis's sdslen budget
        }
    }
    Frame::Error(msg.freeze())
}

/// Append `bytes` to `out`, mapping CR and LF to a space so the result can
/// never be more than one RESP line — see [`err_unknown_command`].
fn push_line_safe(out: &mut BytesMut, bytes: &[u8]) {
    out.reserve(bytes.len());
    for &b in bytes {
        out.extend_from_slice(&[if b == b'\r' || b == b'\n' { b' ' } else { b }]);
    }
}

/// Extract &Bytes from a BulkString or SimpleString frame.
pub fn extract_bytes(frame: &Frame) -> Option<&Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b),
        _ => None,
    }
}

/// True when every frame is argument-shaped — i.e. something a wire client
/// could actually have sent.
///
/// moon#823: a command that walks its argv inside a mutation loop and bails on
/// the first frame `extract_bytes` rejects has ALREADY written part of the
/// command by the time it returns the error. Propagation is gated on the reply
/// not being an error, so that partial write is applied on the master and
/// never reaches the AOF or a replica: silent loss across restart, permanent
/// replica divergence.
///
/// The boundary that let a non-bulk frame into an argv at all is
/// `scripting::types::lua_arg_to_frame`, and it now refuses — so on today's
/// code this check cannot fail. It is kept as a cheap, explicit guard in front
/// of each mutation window rather than an invariant a future caller has to
/// know about, because the failure mode is silent data loss and the check is
/// one pointer comparison per argument on a path that already walks them.
#[inline]
pub fn all_args_are_bytes(args: &[Frame]) -> bool {
    args.iter().all(|a| extract_bytes(a).is_some())
}

/// OK response.
pub fn ok() -> Frame {
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// Generic error response.
pub fn err(msg: &str) -> Frame {
    Frame::Error(Bytes::from(msg.to_string()))
}

/// The `count [WITHSCORES|WITHVALUES]` tail of `ZRANDMEMBER` / `HRANDFIELD`
/// (`tail` = the arguments after the key, at least one), parsed as redis
/// 7.0.15's `zrandmemberCommand` / `hrandfieldCommand` do — BEFORE the key is
/// looked up, so a bad count is an error on a missing or wrong-typed key too:
///
/// 1. `getRangeLongFromObjectOrReply(-LONG_MAX, LONG_MAX)`: a `string2ll`
///    integer (no `+`, no leading zero, no `-0`: [`canonical_i64`]), and not
///    `LONG_MIN`, which has its own message;
/// 2. anything after the count but exactly one `flag` is a syntax error;
/// 3. with the flag, `|count|` must fit `LONG_MAX / 2` (the reply is twice
///    as long).
///
/// Returns `(count, flag given)`.
///
/// [`canonical_i64`]: crate::storage::numeric::canonical_i64
pub fn parse_rand_count(tail: &[Frame], flag: &[u8]) -> Result<(i64, bool), Frame> {
    let count = match tail
        .first()
        .and_then(extract_bytes)
        .and_then(|b| crate::storage::numeric::canonical_i64(b))
    {
        Some(i64::MIN) => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807",
            )));
        }
        Some(c) => c,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            )));
        }
    };
    let with_flag = match tail.get(1..).unwrap_or_default() {
        [] => false,
        [f] if extract_bytes(f).is_some_and(|f| f.eq_ignore_ascii_case(flag)) => true,
        _ => return Err(Frame::Error(Bytes::from_static(b"ERR syntax error"))),
    };
    if with_flag && !(-(i64::MAX / 2)..=i64::MAX / 2).contains(&count) {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR value is out of range",
        )));
    }
    Ok((count, with_flag))
}

/// Whether an absolute expiry (unix millis) is representable without a
/// client-visible wrap.
///
/// Expiry is stored as `u64` millis, but `PTTL`/`PEXPIRETIME` cast it back to
/// `i64` — an expiry past `i64::MAX` surfaces as a NEGATIVE TTL on a key that is
/// very much alive. Redis rejects such out-of-range expiries outright
/// (`when > LLONG_MAX / 1000` → "invalid expire time"), so every expiry-setting
/// command must too. Returns `true` when `expires_at_ms` is safe to store.
#[inline]
pub fn expiry_ms_in_range(expires_at_ms: u64) -> bool {
    expires_at_ms <= i64::MAX as u64
}

/// Refuse an unknown container subcommand the way Redis does — moon#670.
///
/// One shape, every container:
///
/// ```text
/// ERR unknown subcommand '<as sent>'. Try <CONTAINER> HELP.
/// ```
///
/// Measured against `redis-server 8.6.1` on 2026-08-24 across all fifteen
/// containers it exposes: the string is byte-identical apart from the two
/// interpolations, and the subcommand is echoed **verbatim**, case included
/// (`CONFIG MiXeD` reports `'MiXeD'`). That is the opposite of the arity error,
/// which lower-cases the command name (moon#491) — a reviewer "correcting" one
/// to match the other breaks whichever they touch.
///
/// This exists as one function because the alternative was measured: Moon had
/// ten different spellings of this error, including `Unknown` with a capital U
/// (`COMMAND`), two that never named the offending subcommand at all
/// (`SLOWLOG`, and `XGROUP` which reported a literal `'UNKNOWN'`), and two that
/// reported an ARITY problem instead (`OBJECT`, `XINFO`) — which reads to a
/// client as "the subcommand exists, you called it wrong".
///
/// `container` is a static, uppercase, caller-supplied name; `sub` is UNTRUSTED
/// client input.
pub fn err_unknown_subcommand(container: &str, sub: &[u8]) -> Frame {
    let mut buf = Vec::with_capacity(32 + container.len() + sub.len());
    buf.extend_from_slice(b"ERR unknown subcommand '");
    // A subcommand arrives as a bulk string, so it may legally contain CR, LF
    // and NUL. `serialize_frame` writes an error's payload RAW and terminates
    // it with CRLF, so an un-substituted CRLF here would end the frame early
    // and let the client read the remainder as a second, attacker-chosen reply
    // — desyncing that connection for the rest of its life. Substitute rather
    // than trust the parser to have kept them out.
    buf.extend(
        sub.iter()
            .map(|&b| if b < 0x20 || b == 0x7f { b'?' } else { b }),
    );
    buf.extend_from_slice(b"'. Try ");
    buf.extend_from_slice(container.as_bytes());
    buf.extend_from_slice(b" HELP.");
    Frame::Error(Bytes::from(buf))
}

/// Build a `<CONTAINER> HELP` reply in Redis's shape — moon#698.
///
/// Redis's help replies are uniform across all 13 containers (measured against
/// `redis-server 8.6.1`, 2026-08-24): an array of **simple** strings that opens
/// with `<CONTAINER> <subcommand> [<arg> [value] [opt] ...]. Subcommands are:`
/// and closes with `HELP` / `    Print this help.`
///
/// The header and the footer are emitted HERE rather than repeated in each
/// container's table, so a container with a divergent shape is unrepresentable
/// rather than merely untested. Callers supply only the body lines; see
/// [`crate::command::help_text`].
///
/// Not a hot path — `HELP` is an introspection command — so building the header
/// into a `Vec<u8>` and sizing the reply with `with_capacity` is the whole cost.
pub fn help_reply(container: &str, body: &[&'static str]) -> Frame {
    let mut out: Vec<Frame> = Vec::with_capacity(body.len() + 3);

    let mut header = Vec::with_capacity(container.len() + 56);
    header.extend_from_slice(container.as_bytes());
    header.extend_from_slice(b" <subcommand> [<arg> [value] [opt] ...]. Subcommands are:");
    out.push(Frame::SimpleString(Bytes::from(header)));

    out.extend(
        body.iter()
            .map(|l| Frame::SimpleString(Bytes::from_static(l.as_bytes()))),
    );

    out.push(Frame::SimpleString(Bytes::from_static(b"HELP")));
    out.push(Frame::SimpleString(Bytes::from_static(
        b"    Print this help.",
    )));
    Frame::Array(out.into())
}

/// moon#1077 — `err_unknown_command`'s exact grammar, measured on redis-server
/// 8.6.1 raw sockets (`/tmp/oracle-1060-1076-1077/probe.py` in the PR).
#[cfg(test)]
mod err_unknown_command_1077_tests {
    use super::*;

    fn bulk(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn text(frame: &Frame) -> Vec<u8> {
        match frame {
            Frame::Error(e) => e.to_vec(),
            other => panic!("expected an error reply, got {other:?}"),
        }
    }

    #[test]
    fn zero_args_has_no_suffix_at_all() {
        assert_eq!(
            text(&err_unknown_command(b"NOSUCHCMD", &[])),
            b"ERR unknown command 'NOSUCHCMD'"
        );
    }

    #[test]
    fn args_are_listed_space_separated_with_no_commas() {
        assert_eq!(
            text(&err_unknown_command(
                b"NOSUCHCMD",
                &[bulk(b"a"), bulk(b"b")]
            )),
            b"ERR unknown command 'NOSUCHCMD', with args beginning with: 'a' 'b' "
        );
    }

    /// The client's OWN casing is echoed — the opposite of the arity error,
    /// which normalises to the registered name (moon#491).
    #[test]
    fn command_name_keeps_the_clients_casing() {
        assert_eq!(
            text(&err_unknown_command(b"NoSuchCmd", &[bulk(b"a")])),
            b"ERR unknown command 'NoSuchCmd', with args beginning with: 'a' "
        );
    }

    /// C's `%.*s` reads the value as NUL-terminated even though it is
    /// binary-safe: an argument starting with `\0` prints as `''`.
    #[test]
    fn an_argument_truncates_at_its_first_embedded_nul() {
        assert_eq!(
            text(&err_unknown_command(
                b"NOSUCHCMD",
                &[bulk(&[0, 1, 2, 0xff, 0xfe])]
            )),
            b"ERR unknown command 'NOSUCHCMD', with args beginning with: '' "
        );
    }

    /// Non-UTF8 bytes reach the wire unchanged — never a lossy replacement
    /// character.
    #[test]
    fn binary_bytes_are_not_utf8_lossy_replaced() {
        assert_eq!(
            text(&err_unknown_command(
                b"NOSUCHCMD",
                &[bulk(&[0xff, 0xfe, 0xfd])]
            )),
            b"ERR unknown command 'NOSUCHCMD', with args beginning with: '\xff\xfe\xfd' "
        );
    }

    /// CR and LF are mapped to a space so this reply can never split into a
    /// second, client-forged RESP line (moon#1031's class of bug) — measured
    /// against redis-server 8.6.1, which does the same.
    #[test]
    fn cr_and_lf_become_spaces_in_both_the_name_and_the_arguments() {
        assert_eq!(
            text(&err_unknown_command(b"foo\r\nbar", &[bulk(b"x\r\ny")])),
            b"ERR unknown command 'foo  bar', with args beginning with: 'x  y' "
        );
    }

    /// The combined argument budget is 128 bytes: a single long argument is
    /// truncated to fit, and a budget already spent shrinks the next one.
    #[test]
    fn the_combined_argument_budget_is_128_bytes() {
        let one_two_eight = vec![b'x'; 128];
        assert_eq!(
            text(&err_unknown_command(
                b"NOSUCHCMD",
                &[bulk(&vec![b'x'; 300])]
            )),
            [
                b"ERR unknown command 'NOSUCHCMD', with args beginning with: '".as_slice(),
                one_two_eight.as_slice(),
                b"' ".as_slice(),
            ]
            .concat()
        );

        let hundred_y = [b'y'; 100];
        let twenty_five_z = vec![b'z'; 25];
        assert_eq!(
            text(&err_unknown_command(
                b"NOSUCHCMD",
                &[bulk(&hundred_y), bulk(&[b'z'; 100])]
            )),
            [
                b"ERR unknown command 'NOSUCHCMD', with args beginning with: '".as_slice(),
                hundred_y.as_slice(),
                b"' '".as_slice(),
                twenty_five_z.as_slice(),
                b"' ".as_slice(),
            ]
            .concat()
        );
    }

    /// A non-bulk-string argument (never sent by a real RESP client, but
    /// defensively handled) contributes an empty piece rather than panicking.
    #[test]
    fn a_non_bulk_argument_contributes_an_empty_piece() {
        assert_eq!(
            text(&err_unknown_command(b"NOSUCHCMD", &[Frame::Integer(5)])),
            b"ERR unknown command 'NOSUCHCMD', with args beginning with: '' "
        );
    }
}

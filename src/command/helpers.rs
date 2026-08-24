use bytes::Bytes;

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

/// Extract &Bytes from a BulkString or SimpleString frame.
pub fn extract_bytes(frame: &Frame) -> Option<&Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b),
        _ => None,
    }
}

/// OK response.
pub fn ok() -> Frame {
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// Generic error response.
pub fn err(msg: &str) -> Frame {
    Frame::Error(Bytes::from(msg.to_string()))
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

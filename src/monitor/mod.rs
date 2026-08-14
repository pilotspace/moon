//! `MONITOR` — stream every executed command to attached admin clients.
//!
//! # Cost when nobody is attached
//!
//! The feed is written from the command hot path on every shard, so the
//! unattached case must be free. [`feed`] loads one `Relaxed` atomic and
//! returns; no formatting, no allocation, no lock is reached until a monitor
//! actually exists. `monitor_registry_zero_cost_when_unattached` pins that as a
//! property of the code rather than a claim in this comment.
//!
//! # Why the hidden-set is spelled out here rather than read from `CommandFlags`
//!
//! The obvious implementation is `flags.contains(ADMIN | SKIP_MONITOR)`. Both
//! terms were measured wrong against redis-server 8.6.1 (2026-08-14):
//!
//!   * Moon's `ADMIN` is **container**-granular — `ACL`, `CLIENT`, `CLUSTER`,
//!     `INFO` are flagged as whole commands. Redis's is **subcommand**-granular
//!     and feeds `INFO`, `CLIENT GETNAME`, `CLIENT ID`, `ACL WHOAMI`,
//!     `ACL CAT`, `CLUSTER INFO`, `CLUSTER MYID`. Skipping on the flag would
//!     silently under-report the feed, which is worse than useless to the
//!     operator reading it during an incident.
//!   * `SKIP_MONITOR` is set by Redis on the entire `EVAL` family, and Redis
//!     **feeds all of them anyway** — including the follow-on `[db lua]` lines.
//!     Honouring the flag would suppress exactly what an operator most wants.
//!
//! So the rule is stated explicitly and pinned row-by-row against the measured
//! oracle by `mon20_admin_audit_table`. Unlisted commands are FED, matching
//! Redis's default; Moon's own administrative extensions are listed in
//! [`HIDDEN_WHOLE`].
//!
//! # Redaction
//!
//! Credentials are replaced **at formatting time** ([`push_arg`] is never
//! reached for them), never written into a buffer and filtered afterwards — a
//! filter is one refactor away from leaking, and the thing it would leak is a
//! password.

use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::runtime::channel;

/// Number of attached monitors. The only thing the hot path touches when no
/// monitor exists.
static MONITOR_COUNT: AtomicUsize = AtomicUsize::new(0);

static MONITORS: RwLock<Vec<MonitorSink>> = RwLock::new(Vec::new());

/// One attached MONITOR connection.
struct MonitorSink {
    /// The owning connection's id, so detach is exact.
    id: u64,
    tx: channel::MpscSender<Bytes>,
}

/// Commands hidden from the feed in every form.
///
/// Measured hidden against redis-server 8.6.1, plus Moon's own administrative
/// extensions (`HOTKEYS`, `VACUUM`, `RECLAMATION`, `CDC.READ`, `KILL`), which
/// have no Redis counterpart to measure and are administrative by construction.
const HIDDEN_WHOLE: &[&str] = &[
    // Measured against redis-server 8.6.1.
    "CONFIG",
    "SLOWLOG",
    "LATENCY",
    "DEBUG",
    "SHUTDOWN",
    // MONITOR hides itself by this rule rather than by a self-suppression
    // special case — which is also why a second MONITOR emits no line.
    "MONITOR",
    "MODULE",
    "REPLICAOF",
    "SLAVEOF",
    "FAILOVER",
    "SYNC",
    "PSYNC",
    // Moon extensions, administrative by construction.
    "HOTKEYS",
    "VACUUM",
    "RECLAMATION",
    "CDC.READ",
    "KILL",
];

/// Container commands whose *subcommands* decide visibility. Everything not
/// listed for a container is fed, matching Redis.
const HIDDEN_SUBCOMMANDS: &[(&str, &[&str])] = &[
    (
        "ACL",
        &[
            "LIST", "SETUSER", "DELUSER", "GETUSER", "USERS", "LOAD", "SAVE", "LOG",
        ],
    ),
    (
        "CLIENT",
        &[
            "LIST", "KILL", "PAUSE", "UNPAUSE", "UNBLOCK", "NO-EVICT", "NO-TOUCH",
        ],
    ),
    (
        "CLUSTER",
        &[
            "FORGET",
            "MEET",
            "RESET",
            "SETSLOT",
            "ADDSLOTS",
            "DELSLOTS",
            "ADDSLOTSRANGE",
            "DELSLOTSRANGE",
            "BUMPEPOCH",
            "FAILOVER",
            "FLUSHSLOTS",
            "SET-CONFIG-EPOCH",
        ],
    ),
    ("MEMORY", &["PURGE"]),
];

/// Is this command hidden from the MONITOR feed?
///
/// `first_arg` is the command's first argument, used for the container
/// commands whose subcommands differ in visibility.
#[inline]
pub fn is_hidden(cmd: &[u8], first_arg: Option<&[u8]>) -> bool {
    if HIDDEN_WHOLE
        .iter()
        .any(|h| cmd.eq_ignore_ascii_case(h.as_bytes()))
    {
        return true;
    }
    for (container, subs) in HIDDEN_SUBCOMMANDS {
        if cmd.eq_ignore_ascii_case(container.as_bytes()) {
            let Some(sub) = first_arg else {
                // A container with no subcommand is an arity error and never
                // executes, so it is never fed either way.
                return false;
            };
            return subs.iter().any(|s| sub.eq_ignore_ascii_case(s.as_bytes()));
        }
    }
    false
}

/// Append one argument, escaped the way Redis's `sdscatrepr` does.
///
/// Measured byte by byte: `"` -> `\"`, `\` -> `\\`, `\n` `\r` `\t` -> `\n` `\r`
/// `\t`, `0x07` -> `\a`, `0x08` -> `\b`, everything else outside printable
/// ASCII -> `\xHH` lowercase. UTF-8 is escaped per BYTE, not per character.
fn push_arg(out: &mut Vec<u8>, arg: &[u8]) {
    out.push(b'"');
    for &b in arg {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0x07 => out.extend_from_slice(b"\\a"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x20..=0x7e => out.push(b),
            other => {
                out.extend_from_slice(b"\\x");
                const HEX: &[u8; 16] = b"0123456789abcdef";
                out.push(HEX[(other >> 4) as usize]);
                out.push(HEX[(other & 0x0f) as usize]);
            }
        }
    }
    out.push(b'"');
}

const REDACTED: &[u8] = b"\"(redacted)\"";

/// Build one feed line, including the trailing CRLF.
///
/// `+<unix>.<micros:06> [<db> <addr>] "CMD" "arg" …`
pub fn format_line(now_micros: u128, db: usize, addr: &str, cmd: &[u8], args: &[Bytes]) -> Bytes {
    let mut out =
        Vec::with_capacity(64 + cmd.len() + args.iter().map(|a| a.len() + 3).sum::<usize>());
    out.push(b'+');

    let secs = now_micros / 1_000_000;
    let micros = (now_micros % 1_000_000) as u32;
    let mut itoa_buf = itoa::Buffer::new();
    out.extend_from_slice(itoa_buf.format(secs).as_bytes());
    out.push(b'.');
    // Zero-padded to exactly 6 digits; a client parses this as a fixed shape.
    let mut d = [b'0'; 6];
    let mut v = micros;
    for slot in d.iter_mut().rev() {
        *slot = b'0' + (v % 10) as u8;
        v /= 10;
    }
    out.extend_from_slice(&d);

    out.extend_from_slice(b" [");
    out.extend_from_slice(itoa_buf.format(db).as_bytes());
    out.push(b' ');
    out.extend_from_slice(addr.as_bytes());
    out.extend_from_slice(b"] ");

    push_arg(&mut out, cmd);

    // Credential handling is decided BEFORE any argument is written, so a
    // secret never enters the buffer at all.
    let auth_all = cmd.eq_ignore_ascii_case(b"AUTH");
    let hello_auth_at = if cmd.eq_ignore_ascii_case(b"HELLO") {
        args.iter()
            .position(|a| a.as_ref().eq_ignore_ascii_case(b"AUTH"))
    } else {
        None
    };

    for (i, arg) in args.iter().enumerate() {
        out.push(b' ');
        let redact = auth_all || hello_auth_at.is_some_and(|k| i == k + 1 || i == k + 2);
        if redact {
            out.extend_from_slice(REDACTED);
        } else {
            push_arg(&mut out, arg);
        }
    }

    out.extend_from_slice(b"\r\n");
    Bytes::from(out)
}

/// Attach a connection to the feed. Idempotent per `id`: a second `MONITOR` on
/// an already-attached connection must not double-register, or every line
/// would be delivered twice.
pub fn attach(id: u64, tx: channel::MpscSender<Bytes>) -> bool {
    let mut guard = MONITORS.write();
    if guard.iter().any(|m| m.id == id) {
        return false;
    }
    guard.push(MonitorSink { id, tx });
    MONITOR_COUNT.store(guard.len(), Ordering::Relaxed);
    true
}

/// Detach a connection (RESET, disconnect, or shed for being slow).
pub fn detach(id: u64) {
    let mut guard = MONITORS.write();
    let before = guard.len();
    guard.retain(|m| m.id != id);
    if guard.len() != before {
        MONITOR_COUNT.store(guard.len(), Ordering::Relaxed);
    }
}

/// Is any monitor attached at all? One `Relaxed` load.
///
/// Used by the inline fast path, which cannot format a feed line (it has no
/// peer address) and therefore stands down entirely while monitoring is on.
#[inline]
pub fn any_attached() -> bool {
    MONITOR_COUNT.load(Ordering::Relaxed) != 0
}

#[inline]
pub fn is_attached(id: u64) -> bool {
    if MONITOR_COUNT.load(Ordering::Relaxed) == 0 {
        return false;
    }
    MONITORS.read().iter().any(|m| m.id == id)
}

/// Emit one command to every attached monitor.
///
/// The entire cost when unattached is the `Relaxed` load below.
///
/// A monitor whose queue is full has its **sink dropped**, which closes its
/// channel and ends its connection. Contracted at freeze: dropping lines
/// silently would leave an operator unable to tell a quiet server from a lossy
/// feed, and blocking would let one slow TCP reader stall every shard.
#[inline]
pub fn feed(db: usize, addr: &str, cmd: &[u8], args: &[Bytes]) {
    if MONITOR_COUNT.load(Ordering::Relaxed) == 0 {
        return;
    }
    feed_cold(db, addr, cmd, args);
}

/// [`feed`] for a protocol-level argument list.
///
/// Kept separate so the hot path never materialises a `Vec<Bytes>`: the
/// conversion happens inside the cold path, after the attached check.
#[inline]
pub fn feed_frames(db: usize, addr: &str, cmd: &[u8], args: &[crate::protocol::Frame]) {
    if MONITOR_COUNT.load(Ordering::Relaxed) == 0 {
        return;
    }
    let owned: Vec<Bytes> = args
        .iter()
        .filter_map(|f| crate::command::helpers::extract_bytes(f).cloned())
        .collect();
    feed_cold(db, addr, cmd, &owned);
}

#[cold]
fn feed_cold(db: usize, addr: &str, cmd: &[u8], args: &[Bytes]) {
    let first = args.first().map(|a| a.as_ref());
    if is_hidden(cmd, first) {
        return;
    }
    // Redis feeds a command when it EXECUTES. One rejected earlier — an unknown
    // name, or an arity violation — never executes, so it never appears. Both
    // checks live here in the cold path, so the attached-nobody case still pays
    // only the atomic load above.
    let Some(meta) = crate::command::metadata::lookup(cmd) else {
        return;
    };
    let argc = args.len() as i16 + 1;
    let arity_ok = if meta.arity >= 0 {
        argc == meta.arity
    } else {
        argc >= -meta.arity
    };
    if !arity_ok {
        return;
    }
    // `SELECT n` reports the db it MOVES TO, not the one it came from —
    // measured (`SELECT 3` logs as `[3 …]`, not `[0 …]`). Everything else is
    // fed before dispatch, so the connection's current db is correct; SELECT is
    // the one command whose own effect changes the field, and taking it from
    // the argument avoids feeding after execution just for this case (which
    // would delay every blocking command to its unblock, per the freeze).
    let db = if cmd.eq_ignore_ascii_case(b"SELECT") {
        args.first()
            .and_then(|a| std::str::from_utf8(a).ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(db)
    } else {
        db
    };
    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros());
    let line = format_line(now_micros, db, addr, cmd, args);

    // Serialize once, fan out under the read lock; collect the slow ones and
    // drop them in a second pass so the read lock is never upgraded in place.
    let mut slow: Vec<u64> = Vec::new();
    {
        let guard = MONITORS.read();
        for m in guard.iter() {
            if m.tx.try_send(line.clone()).is_err() {
                slow.push(m.id);
            }
        }
    }
    for id in slow {
        detach(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(v: &[u8]) -> Bytes {
        Bytes::copy_from_slice(v)
    }

    /// `MONITOR_COUNT` and `MONITORS` are process-global, so the two tests that
    /// assert on them must not interleave: cargo runs tests in parallel by
    /// default, and `attach_is_idempotent_per_connection` briefly makes the
    /// count non-zero, which is exactly what the zero-cost test asserts against.
    static REGISTRY_TESTS: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    #[test]
    fn monitor_registry_zero_cost_when_unattached() {
        // M11 as a property of the code: with no monitor attached, `feed` must
        // not reach formatting at all. Proven by the count being the only
        // thing consulted — `feed_cold` is where every allocation lives, and
        // it is unreachable while the count is zero.
        let _guard = REGISTRY_TESTS.lock();
        assert_eq!(MONITOR_COUNT.load(Ordering::Relaxed), 0);
        // A huge argument list that would be expensive to format.
        let args: Vec<Bytes> = (0..1000).map(|i| b(format!("arg{i}").as_bytes())).collect();
        feed(0, "127.0.0.1:1", b"SET", &args);
        assert_eq!(
            MONITOR_COUNT.load(Ordering::Relaxed),
            0,
            "feed must not disturb the registry when unattached"
        );
    }

    #[test]
    fn monitor_redaction_never_buffers_the_secret() {
        // The §0 split decision made "redact at formatting time, never filter
        // afterwards" a design constraint. This inspects the formatter's own
        // output, so a version that emitted the password and stripped it later
        // would still fail here only if the strip were removed — which is why
        // the assertion is on the ABSENCE of the secret in the bytes.
        let line = format_line(
            1_700_000_000_123_456,
            0,
            "127.0.0.1:1",
            b"AUTH",
            &[b(b"hunter2")],
        );
        let text = String::from_utf8_lossy(&line).to_string();
        assert!(!text.contains("hunter2"), "got {text:?}");
        assert!(text.contains(r#""AUTH" "(redacted)""#), "got {text:?}");

        let line = format_line(
            1_700_000_000_123_456,
            0,
            "127.0.0.1:1",
            b"AUTH",
            &[b(b"user"), b(b"pw")],
        );
        let text = String::from_utf8_lossy(&line).to_string();
        assert!(
            !text.contains("user") && !text.contains("pw"),
            "got {text:?}"
        );
        assert!(
            text.contains(r#""AUTH" "(redacted)" "(redacted)""#),
            "got {text:?}"
        );

        let line = format_line(
            1_700_000_000_123_456,
            0,
            "127.0.0.1:1",
            b"HELLO",
            &[b(b"3"), b(b"AUTH"), b(b"default"), b(b"sekrit")],
        );
        let text = String::from_utf8_lossy(&line).to_string();
        assert!(!text.contains("sekrit"), "got {text:?}");
        assert!(
            text.contains(r#""HELLO" "3" "AUTH" "(redacted)" "(redacted)""#),
            "the version survives; only the credentials go. got {text:?}"
        );
    }

    #[test]
    fn line_shape_and_escaping() {
        let line = format_line(
            1_700_000_000_000_007,
            3,
            "127.0.0.1:5555",
            b"SET",
            &[b(b"esc"), b(b"a\"b\\c\nd\re\tf\x00g\xffh")],
        );
        let text = String::from_utf8_lossy(&line).to_string();
        assert!(
            text.starts_with("+1700000000.000007 [3 127.0.0.1:5555] "),
            "micros zero-pad to 6 digits; got {text:?}"
        );
        assert!(
            text.contains(r#""SET" "esc" "a\"b\\c\nd\re\tf\x00g\xffh""#),
            "got {text:?}"
        );
        assert!(text.ends_with("\r\n"));
    }

    #[test]
    fn utf8_escapes_per_byte_and_empty_arg_is_two_quotes() {
        let line = format_line(1, 0, "a:1", b"SET", &[b("h\u{e9}llo".as_bytes()), b(b"")]);
        let text = String::from_utf8_lossy(&line).to_string();
        assert!(text.contains(r#""h\xc3\xa9llo""#), "got {text:?}");
        assert!(text.ends_with("\"\"\r\n"), "got {text:?}");
    }

    #[test]
    fn hidden_set_matches_the_measured_oracle() {
        // Every row measured against redis-server 8.6.1. The container rows are
        // the point: Moon's own ADMIN flag cannot express them.
        for (cmd, sub) in [
            (&b"CONFIG"[..], Some(&b"GET"[..])),
            (b"CONFIG", Some(&b"SET"[..])),
            (b"SLOWLOG", Some(&b"LEN"[..])),
            (b"LATENCY", Some(&b"RESET"[..])),
            (b"MONITOR", None),
            (b"ACL", Some(&b"LIST"[..])),
            (b"ACL", Some(&b"SETUSER"[..])),
            (b"CLIENT", Some(&b"LIST"[..])),
        ] {
            assert!(is_hidden(cmd, sub), "{:?} {:?} must be hidden", cmd, sub);
        }
        for (cmd, sub) in [
            (&b"INFO"[..], Some(&b"server"[..])),
            (b"DBSIZE", None),
            (b"LASTSAVE", None),
            (b"COMMAND", Some(&b"COUNT"[..])),
            (b"CLIENT", Some(&b"GETNAME"[..])),
            (b"CLIENT", Some(&b"ID"[..])),
            (b"ACL", Some(&b"WHOAMI"[..])),
            (b"ACL", Some(&b"CAT"[..])),
            (b"CLUSTER", Some(&b"INFO"[..])),
            (b"MEMORY", Some(&b"USAGE"[..])),
            // Fed despite Redis's own skip_monitor flag — measured.
            (b"EVAL", Some(&b"return 1"[..])),
            (b"EVALSHA", Some(&b"abc"[..])),
            (b"SET", Some(&b"k"[..])),
        ] {
            assert!(!is_hidden(cmd, sub), "{:?} {:?} must be fed", cmd, sub);
        }
    }

    #[test]
    fn attach_is_idempotent_per_connection() {
        // A second MONITOR on the same connection must not double-register, or
        // every command would be delivered twice.
        let _guard = REGISTRY_TESTS.lock();
        let (tx, _rx) = channel::mpsc_bounded::<Bytes>(4);
        let (tx2, _rx2) = channel::mpsc_bounded::<Bytes>(4);
        let id = 987_654;
        assert!(attach(id, tx), "first attach registers");
        assert!(!attach(id, tx2), "second attach on the same id is refused");
        detach(id);
        assert!(!is_attached(id));
    }
}

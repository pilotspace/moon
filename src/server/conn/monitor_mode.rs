//! The rules that apply to a connection once it is attached as a `MONITOR`.
//!
//! Stated ONCE and consulted by every handler. The pub/sub subscriber-mode
//! allow-list was the counter-example: it lived in three handlers with two
//! different texts and two different behaviours, and none of them matched
//! Redis. A rule restated in N places drifts to N behaviours.

use bytes::Bytes;

use crate::command::metadata::CommandFlags;
use crate::protocol::Frame;

/// Redis's verbatim refusal. A monitor is flagged as a replica internally,
/// which is why the message talks about replicas rather than monitors — the
/// text is measured, not composed.
const ERR_KEYSPACE: &[u8] = b"ERR Replica can't interact with the keyspace";

/// Keyspace commands that name NO key (`first_key == 0`), so `first_key` alone
/// cannot find them. Measured refused against redis-server 8.6.1 (2026-08-14,
/// one fresh connection per probe).
const REFUSED_BY_NAME: &[&str] = &[
    "DBSIZE",
    "KEYS",
    "SCAN",
    "RANDOMKEY",
    "EVAL",
    "EVALSHA",
    "EVAL_RO",
    "EVALSHA_RO",
    "PUBLISH",
    "SPUBLISH",
];

/// Container commands whose *subcommands* differ, exactly as the MONITOR
/// hidden-set does: `MEMORY USAGE` is refused, `MEMORY DOCTOR` is served.
const REFUSED_SUBCOMMANDS: &[(&str, &[&str])] = &[("MEMORY", &["USAGE"])];

/// `SELECT` carries Moon's `WRITE` flag (it mutates connection state) but is
/// served on a monitor connection, and its feed line is how the `[db …]` field
/// is observed changing. The one exception to the `WRITE` term below.
const SERVED_DESPITE_WRITE: &[&str] = &["SELECT"];

/// Should this command be refused on an attached monitor connection?
///
/// The rule is: it names a key (`first_key != 0`), OR it carries `WRITE`, OR it
/// is one of the measured zero-key keyspace commands above.
///
/// Two rules were tried and are measurably wrong; both are recorded because the
/// tempting fix is to go back to one of them:
///
///   * `first_key != 0` alone SERVES `DBSIZE`, `KEYS`, `SCAN`, `RANDOMKEY`,
///     `FLUSHALL`, `FLUSHDB`, `SWAPDB`, `EVAL` and `PUBLISH`, all of which Redis
///     refuses.
///   * `WRITE | READONLY` — the flags Redis itself uses — REFUSES `PING`,
///     `ECHO`, `TIME`, `INFO`, `COMMAND`, `LASTSAVE` and `WAIT`, because Moon
///     flags all of them `READONLY` and Redis flags none of them so. This is the
///     same trap as `CommandFlags::ADMIN` in the MONITOR hidden-set: Moon's
///     flags are not Redis's flags, and reusing them silently changes behaviour.
///
/// So the rule is stated explicitly and pinned row by row by
/// `mon23_refusal_rule_is_write_or_readonly_not_first_key`.
///
/// An unknown command returns `None` — dispatch refuses it later with the
/// unknown-command error, which is what Redis does too.
pub fn refuse_if_keyspace(cmd: &[u8], first_arg: Option<&[u8]>) -> Option<Frame> {
    let meta = crate::command::metadata::lookup(cmd)?;

    let by_sub = REFUSED_SUBCOMMANDS.iter().find_map(|(container, subs)| {
        if !cmd.eq_ignore_ascii_case(container.as_bytes()) {
            return None;
        }
        let sub = first_arg?;
        Some(subs.iter().any(|s| sub.eq_ignore_ascii_case(s.as_bytes())))
    });
    if let Some(refused) = by_sub {
        return refused.then(|| Frame::Error(Bytes::from_static(ERR_KEYSPACE)));
    }

    let named = REFUSED_BY_NAME
        .iter()
        .any(|n| cmd.eq_ignore_ascii_case(n.as_bytes()));
    let writes = meta.flags.contains(CommandFlags::WRITE)
        && !SERVED_DESPITE_WRITE
            .iter()
            .any(|n| cmd.eq_ignore_ascii_case(n.as_bytes()));

    if meta.first_key != 0 || writes || named {
        return Some(Frame::Error(Bytes::from_static(ERR_KEYSPACE)));
    }
    None
}

/// `MONITOR` — attach this connection to the feed. The rule lives here, once,
/// and both handlers call it; the previous version had the monoio copy in
/// `handler_monoio::dispatch` and a hand-written second copy in
/// `handler_sharded`, which had already drifted in structure. Two copies of an
/// attach rule diverge the same way the subscriber-mode allow-list did.
///
/// Returns the reply to send, or `None` when the connection is ALREADY attached:
/// Redis answers a second `MONITOR` with nothing at all. Measured — silence,
/// not an error.
///
/// The ACL check is not repeated here: this is reached only below the ACL gate,
/// and `MONITOR` carries the admin category in `COMMAND_META`, so a non-admin
/// user is refused by the general gate with the same `NOPERM` text.
pub fn handle_monitor(
    arg_count: usize,
    client_id: u64,
    already_attached: &mut bool,
    rx_slot: &mut Option<crate::runtime::channel::MpscReceiver<Bytes>>,
) -> Option<Frame> {
    if arg_count != 0 {
        return Some(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'monitor' command",
        )));
    }
    if *already_attached {
        return None;
    }
    // Bounded, and deliberately not large. A monitor that cannot keep up has
    // this channel fill; the feed then drops the SINK, which closes the channel
    // and ends the connection. Contracted at freeze: silently skipping lines
    // would leave an operator unable to tell a quiet server from a lossy feed,
    // and blocking would let one slow reader stall every shard.
    let (tx, rx) = crate::runtime::channel::mpsc_bounded::<Bytes>(MONITOR_QUEUE_DEPTH);
    if crate::monitor::attach(client_id, tx) {
        *already_attached = true;
        *rx_slot = Some(rx);
        return Some(Frame::SimpleString(Bytes::from_static(b"OK")));
    }
    // The registry already holds a sink for this id while THIS connection
    // believes it is unattached — a stale registration (a reused client id, or
    // a teardown path that did not run). Marking the connection attached and
    // dropping the receiver, as an earlier version did, produced the worst
    // possible state: keyspace commands refused, no reply, no feed line, and no
    // way to notice. Evict the stale sink and take the registration.
    crate::monitor::detach(client_id);
    let (tx, rx) = crate::runtime::channel::mpsc_bounded::<Bytes>(MONITOR_QUEUE_DEPTH);
    if crate::monitor::attach(client_id, tx) {
        *already_attached = true;
        *rx_slot = Some(rx);
        return Some(Frame::SimpleString(Bytes::from_static(b"OK")));
    }
    // Unreachable in practice: nothing else can register this id concurrently,
    // because a client id belongs to exactly one connection task. Fail LOUDLY
    // rather than leaving the connection in the half-attached state above.
    *already_attached = false;
    *rx_slot = None;
    Some(Frame::Error(Bytes::from_static(
        b"ERR MONITOR could not attach: the feed registry is holding a stale registration for this connection",
    )))
}

/// Per-monitor queue depth. See `handle_monitor` for why it is bounded and what
/// happens when it fills.
const MONITOR_QUEUE_DEPTH: usize = 4096;

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(cmd: &[u8]) -> bool {
        refuse_if_keyspace(cmd, None).is_some()
    }

    #[test]
    fn keyspace_commands_are_refused() {
        for c in [&b"SET"[..], b"GET", b"DEL", b"INCR", b"HSET", b"LPUSH"] {
            assert!(
                refused(c),
                "{:?} addresses a key",
                String::from_utf8_lossy(c)
            );
        }
    }

    #[test]
    fn zero_first_key_commands_are_still_refused() {
        // The regression guard for the original `first_key != 0` rule: every
        // one of these carries `first_key == 0` and every one is refused by
        // redis-server 8.6.1 (measured 2026-08-14, one connection per probe).
        for c in [
            &b"DBSIZE"[..],
            b"KEYS",
            b"SCAN",
            b"RANDOMKEY",
            b"FLUSHALL",
            b"FLUSHDB",
            b"SWAPDB",
            b"EVAL",
            b"EVALSHA",
            b"PUBLISH",
            b"SPUBLISH",
            // `EVAL_RO` / `EVALSHA_RO` are deliberately absent: they are not in
            // Moon's registry at all, so `lookup` returns None and dispatch
            // refuses them as unknown commands. They stay listed in
            // `REFUSED_BY_NAME` so the rule is already right if they land.
        ] {
            assert!(
                refused(c),
                "{:?} has first_key == 0 and IS refused — this is the exact \
                 row the first implementation got wrong",
                String::from_utf8_lossy(c)
            );
        }
    }

    #[test]
    fn connection_and_server_commands_are_served() {
        // Measured on a real monitor connection: each of these works. Note
        // `DBSIZE` is deliberately ABSENT — an earlier version of this test
        // asserted it was served, which was a wrong belief encoded as a test
        // and is what let the wrong rule ship green.
        for c in [
            &b"PING"[..],
            b"INFO",
            b"CLIENT",
            b"ACL",
            b"SUBSCRIBE",
            b"RESET",
            b"QUIT",
            b"COMMAND",
            b"LASTSAVE",
            b"TIME",
            b"ECHO",
            b"SELECT",
            b"BGSAVE",
        ] {
            assert!(
                !refused(c),
                "{:?} is served on a monitor connection",
                String::from_utf8_lossy(c)
            );
        }
    }

    #[test]
    fn the_refusal_text_is_verbatim_redis() {
        let f = refuse_if_keyspace(b"SET", None).expect("SET is refused");
        match f {
            Frame::Error(e) => assert_eq!(
                &e[..],
                b"ERR Replica can't interact with the keyspace",
                "byte-compared: a near-miss here is a client-visible divergence"
            ),
            other => panic!("expected an error frame, got {other:?}"),
        }
    }
}

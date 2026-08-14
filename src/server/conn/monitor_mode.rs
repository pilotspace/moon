//! The rules that apply to a connection once it is attached as a `MONITOR`.
//!
//! Stated ONCE and consulted by every handler. The pub/sub subscriber-mode
//! allow-list was the counter-example: it lived in three handlers with two
//! different texts and two different behaviours, and none of them matched
//! Redis. A rule restated in N places drifts to N behaviours.

use bytes::Bytes;

use crate::protocol::Frame;

/// Redis's verbatim refusal. A monitor is flagged as a replica internally,
/// which is why the message talks about replicas rather than monitors — the
/// text is measured, not composed.
const ERR_KEYSPACE: &[u8] = b"ERR Replica can't interact with the keyspace";

/// Should this command be refused on an attached monitor connection?
///
/// The rule is "does it address a key", read straight off the registry's
/// `first_key`: `SET`, `GET` and `DEL` are refused; `PING`, `INFO`, `DBSIZE`,
/// `CLIENT`, `SUBSCRIBE` and `RESET` are served. Measured against
/// redis-server 8.6.1 for each of those.
pub fn refuse_if_keyspace(cmd: &[u8]) -> Option<Frame> {
    let meta = crate::command::metadata::lookup(cmd)?;
    if meta.first_key != 0 {
        return Some(Frame::Error(Bytes::from_static(ERR_KEYSPACE)));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(cmd: &[u8]) -> bool {
        refuse_if_keyspace(cmd).is_some()
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
    fn connection_and_server_commands_are_served() {
        // Measured on a real monitor connection: each of these works.
        for c in [
            &b"PING"[..],
            b"INFO",
            b"DBSIZE",
            b"CLIENT",
            b"SUBSCRIBE",
            b"RESET",
            b"QUIT",
            b"COMMAND",
        ] {
            assert!(
                !refused(c),
                "{:?} does not address a key and must be served",
                String::from_utf8_lossy(c)
            );
        }
    }

    #[test]
    fn the_refusal_text_is_verbatim_redis() {
        let f = refuse_if_keyspace(b"SET").expect("SET is refused");
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

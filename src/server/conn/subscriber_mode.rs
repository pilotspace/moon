//! The subscriber-mode rules, stated ONCE.
//!
//! Moon used to state this allow-list in three places — `handler_monoio`,
//! `handler_sharded` and `handler_single` — with **two different texts** and
//! two different behaviours: only the sharded handler accepted `RESET`, and
//! `handler_single` advertised `HELLO` as allowed in an error message while
//! refusing it. Three copies of a rule is three chances to drift, and they had
//! already drifted before anyone looked.
//!
//! Two rules live here, both measured against redis-server 8.6.1:
//!
//! * **RESP2 restricts, RESP3 does not.** Under RESP2 a subscribed connection
//!   may only run the verbs below. Under RESP3 there is no restriction at all —
//!   letting one connection both subscribe and issue commands is a reason RESP3
//!   exists, and Redis answers `GET`/`SET` normally on a subscribed RESP3
//!   connection.
//! * **`PING`'s shape follows the PROTOCOL, not the mode.** A subscribed RESP2
//!   connection gets `*2 pong ""`; a subscribed RESP3 connection gets `+PONG`.

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;

/// Verbs a subscribed RESP2 connection may still run.
///
/// `HELLO` is deliberately absent: measured, Redis refuses it, so a RESP2
/// subscriber cannot upgrade mid-subscription. `RESET` is deliberately present
/// — it is the sanctioned way out of subscriber mode.
pub fn allowed_in_subscriber_mode(cmd: &[u8]) -> bool {
    const ALLOWED: [&[u8]; 9] = [
        b"SUBSCRIBE",
        b"UNSUBSCRIBE",
        b"PSUBSCRIBE",
        b"PUNSUBSCRIBE",
        b"SSUBSCRIBE",
        b"SUNSUBSCRIBE",
        b"PING",
        b"QUIT",
        b"RESET",
    ];
    ALLOWED.iter().any(|a| cmd.eq_ignore_ascii_case(a))
}

/// Redis 8.6.1's verbatim refusal for a command not on the allow-list.
///
/// Byte-for-byte, including the `(P|S)` alternation and the trailing `RESET` —
/// a driver that string-matches on this text sees a different error if any of
/// it drifts, which is why it is built in one place from one format string.
pub fn subscriber_mode_error(cmd: &[u8]) -> Frame {
    let name = String::from_utf8_lossy(cmd).to_lowercase();
    Frame::Error(Bytes::from(format!(
        "ERR Can't execute '{name}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / \
         RESET are allowed in this context"
    )))
}

/// Should this command be refused right now?
///
/// `resp3` connections are never refused: the restriction is a RESP2 rule.
#[inline]
pub fn refuse_in_subscriber_mode(cmd: &[u8], resp3: bool) -> Option<Frame> {
    if resp3 || allowed_in_subscriber_mode(cmd) {
        None
    } else {
        Some(subscriber_mode_error(cmd))
    }
}

/// The reply to `PING` from a subscribed connection.
///
/// RESP2 keeps the two-element array form; RESP3 answers the ordinary
/// `+PONG`. Moon gave the array form under both, which mistypes the reply for
/// every RESP3 client.
#[inline]
pub fn subscriber_ping_reply(resp3: bool, message: Option<&Bytes>) -> Frame {
    match (resp3, message) {
        (true, None) => Frame::SimpleString(Bytes::from_static(b"PONG")),
        (true, Some(m)) => Frame::BulkString(m.clone()),
        (false, m) => Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"pong")),
            Frame::BulkString(m.cloned().unwrap_or_default()),
        ]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_allow_list_is_redis_s_and_is_case_insensitive() {
        for ok in [
            "SUBSCRIBE",
            "unsubscribe",
            "PSubscribe",
            "PUNSUBSCRIBE",
            "ssubscribe",
            "SUNSUBSCRIBE",
            "ping",
            "QUIT",
            "reset",
        ] {
            assert!(
                allowed_in_subscriber_mode(ok.as_bytes()),
                "{ok} is on Redis's subscriber-mode allow-list"
            );
        }
    }

    #[test]
    fn hello_is_not_allowed_so_a_resp2_subscriber_cannot_upgrade() {
        // Measured against redis-server 8.6.1. handler_single.rs used to name
        // HELLO as allowed in its error text while refusing it.
        assert!(!allowed_in_subscriber_mode(b"HELLO"));
    }

    #[test]
    fn ordinary_commands_are_refused_under_resp2_and_allowed_under_resp3() {
        assert!(refuse_in_subscriber_mode(b"GET", false).is_some());
        assert!(
            refuse_in_subscriber_mode(b"GET", true).is_none(),
            "RESP3 lifts the restriction entirely"
        );
    }

    #[test]
    fn the_refusal_text_is_verbatim_and_names_the_command_lowercased() {
        let Frame::Error(e) = subscriber_mode_error(b"GET") else {
            panic!("must be an Error frame");
        };
        assert_eq!(
            e,
            Bytes::from_static(
                b"ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
            )
        );
    }

    #[test]
    fn ping_shape_follows_the_protocol_not_the_mode() {
        assert_eq!(
            subscriber_ping_reply(false, None),
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"pong")),
                Frame::BulkString(Bytes::new()),
            ])
        );
        assert_eq!(
            subscriber_ping_reply(true, None),
            Frame::SimpleString(Bytes::from_static(b"PONG"))
        );
    }
}

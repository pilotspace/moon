//! The client-side-caching subcommands of `CLIENT`: `TRACKING`, `CACHING`,
//! `TRACKINGINFO` and `GETREDIR`.
//!
//! One implementation shared by all three connection handlers (monoio,
//! sharded tokio, single tokio). Each used to carry its own copy of the
//! `TRACKING` arm, and none had the other three at all — moon#1049 was
//! `CACHING` answering "unknown subcommand" on every path.
//!
//! Every reply and error string below was measured against redis-server
//! 8.6.1 over a raw socket.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::runtime::channel;
use crate::tracking::{TrackingState, TrackingTable};

/// Whether `sub` is one of the subcommands [`handle`] owns.
#[inline]
pub fn is_tracking_subcommand(sub: &[u8]) -> bool {
    sub.eq_ignore_ascii_case(b"TRACKING")
        || sub.eq_ignore_ascii_case(b"CACHING")
        || sub.eq_ignore_ascii_case(b"TRACKINGINFO")
        || sub.eq_ignore_ascii_case(b"GETREDIR")
}

/// Run `CLIENT <sub> ...`. `args` starts AT the subcommand (`args[0]` is
/// `TRACKING`, `CACHING`, ...). Returns `None` for any other subcommand.
///
/// `rx` is the connection's invalidation receiver: created when tracking is
/// first enabled, dropped when it is disabled.
pub fn handle(
    args: &[Frame],
    client_id: u64,
    state: &mut TrackingState,
    rx: &mut Option<channel::MpscReceiver<Frame>>,
    table: &parking_lot::Mutex<TrackingTable>,
) -> Option<Frame> {
    let sub = match args.first()? {
        Frame::BulkString(s) | Frame::SimpleString(s) => s.clone(),
        _ => return None,
    };
    if sub.eq_ignore_ascii_case(b"TRACKING") {
        Some(tracking(args, client_id, state, rx, table))
    } else if sub.eq_ignore_ascii_case(b"CACHING") {
        Some(caching(args, state))
    } else if sub.eq_ignore_ascii_case(b"TRACKINGINFO") {
        Some(tracking_info(args, client_id, state, table))
    } else if sub.eq_ignore_ascii_case(b"GETREDIR") {
        Some(get_redir(args, state))
    } else {
        None
    }
}

fn err(msg: &'static [u8]) -> Frame {
    Frame::Error(Bytes::from_static(msg))
}

fn ok() -> Frame {
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// `CLIENT TRACKING ON|OFF [REDIRECT id] [PREFIX p ...] [BCAST] [OPTIN]
/// [OPTOUT] [NOLOOP]` — redis `clientCommand` + `enableTracking`.
fn tracking(
    args: &[Frame],
    client_id: u64,
    state: &mut TrackingState,
    rx: &mut Option<channel::MpscReceiver<Frame>>,
    table: &parking_lot::Mutex<TrackingTable>,
) -> Frame {
    let cfg = match crate::command::client::parse_tracking_args(args, |id| {
        table.lock().client_exists(id)
    }) {
        Ok(cfg) => cfg,
        Err(e) => return e,
    };

    if !cfg.enable {
        disable(client_id, state, rx, table);
        return ok();
    }

    // The mode checks that depend on what is ALREADY enabled, in redis's
    // order.
    if state.enabled && state.bcast != cfg.bcast {
        return err(
            b"ERR You can't switch BCAST mode on/off before disabling tracking for this client, and then re-enabling it with a different mode.",
        );
    }
    if cfg.bcast && (cfg.optin || cfg.optout) {
        return err(b"ERR OPTIN and OPTOUT are not compatible with BCAST");
    }
    if cfg.optin && cfg.optout {
        return err(b"ERR You can't specify both OPTIN mode and OPTOUT mode");
    }
    if (cfg.optin && state.optout) || (cfg.optout && state.optin) {
        return err(
            b"ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this client, and then re-enabling it with a different mode.",
        );
    }
    let mut prefixes = cfg.prefixes;
    if cfg.bcast {
        if let Some(e) = prefix_collision(&prefixes, &state.prefixes) {
            return e;
        }
        // `BCAST` with no `PREFIX` means "invalidate me for EVERY key". The
        // table matches with `key.starts_with(prefix)`, for which the empty
        // prefix is exactly "all keys"; registering nothing (the old
        // behaviour) meant such a client never heard of any write.
        if prefixes.is_empty() {
            prefixes.push(Bytes::new());
        }
    }

    // enableTracking: re-enabling REPLACES the redirect (an ON without
    // REDIRECT resets it to none) and the NOLOOP/OPTIN/OPTOUT flags, ADDS
    // BCAST prefixes, clears a broken redirect, and leaves the CACHING flag
    // alone.
    let mut t = table.lock();
    if rx.is_none() {
        let (tx, new_rx) = channel::mpsc_bounded::<Frame>(256);
        state.invalidation_tx = Some(tx.clone());
        *rx = Some(new_rx);
        t.register_client(client_id, tx);
    }
    t.set_redirect(client_id, cfg.redirect);
    if cfg.bcast {
        t.set_bcast_noloop(client_id, cfg.noloop);
        for prefix in &prefixes {
            t.register_prefix(client_id, prefix.clone(), cfg.noloop);
        }
    }
    drop(t);

    state.enabled = true;
    state.bcast = cfg.bcast;
    state.optin = cfg.optin;
    state.optout = cfg.optout;
    state.noloop = cfg.noloop;
    state.redirect = cfg.redirect;
    for prefix in prefixes {
        if let Err(at) = state.prefixes.binary_search(&prefix) {
            state.prefixes.insert(at, prefix);
        }
    }
    ok()
}

/// Redis `checkPrefixCollisionsOrReply`: no BCAST prefix may be a prefix of
/// another — neither of one the client already has nor of another one in the
/// same request (equal counts as overlapping). Only the prefixes actually
/// given are checked; a bare `BCAST` is exempt.
///
/// One deliberate divergence: when the self-overlap is found at a position
/// other than the first, redis 8.6.1 writes the error AND then enables
/// tracking and writes `+OK` — two replies to one command, which desyncs any
/// client. Moon refuses with the error alone.
fn prefix_collision(given: &[Bytes], existing: &[Bytes]) -> Option<Frame> {
    let overlaps = |a: &[u8], b: &[u8]| {
        let n = a.len().min(b.len());
        a[..n] == b[..n]
    };
    let msg = |p: &[u8], other: &[u8], what: &str| {
        Frame::Error(Bytes::from(format!(
            "ERR Prefix '{}' overlaps with {what} '{}'. Prefixes for a single client must not overlap.",
            String::from_utf8_lossy(p),
            String::from_utf8_lossy(other)
        )))
    };
    for (i, p) in given.iter().enumerate() {
        if let Some(e) = existing.iter().find(|e| overlaps(e, p)) {
            return Some(msg(p, e, "an existing prefix"));
        }
        if let Some(q) = given[i + 1..].iter().find(|q| overlaps(p, q)) {
            return Some(msg(p, q, "another provided prefix"));
        }
    }
    None
}

/// `CLIENT TRACKING OFF`.
pub fn disable(
    client_id: u64,
    state: &mut TrackingState,
    rx: &mut Option<channel::MpscReceiver<Frame>>,
    table: &parking_lot::Mutex<TrackingTable>,
) {
    *state = TrackingState::default();
    table.lock().untrack_all(client_id);
    *rx = None;
}

/// `CLIENT CACHING YES|NO` — redis arity 3, then the tracking-mode checks.
fn caching(args: &[Frame], state: &mut TrackingState) -> Frame {
    if args.len() != 2 {
        return err(b"ERR wrong number of arguments for 'client|caching' command");
    }
    if !state.enabled {
        return err(
            b"ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
        );
    }
    let opt: &[u8] = match &args[1] {
        Frame::BulkString(s) | Frame::SimpleString(s) => s,
        _ => b"",
    };
    if opt.eq_ignore_ascii_case(b"YES") {
        if !state.optin {
            return err(
                b"ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.",
            );
        }
    } else if opt.eq_ignore_ascii_case(b"NO") {
        if !state.optout {
            return err(
                b"ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.",
            );
        }
    } else {
        return err(b"ERR syntax error");
    }
    // Consumed by the NEXT command (or transaction): see
    // `TrackingState::before_command`.
    state.caching = true;
    ok()
}

/// Whether the queued command `cmd args` is a `CLIENT CACHING` that
/// [`caching`] would accept for `state` — i.e. one that arms the flag for
/// the transaction commands after it.
pub fn caching_would_arm(state: &TrackingState, cmd: &[u8], args: &[Frame]) -> bool {
    if !state.enabled || !cmd.eq_ignore_ascii_case(b"CLIENT") || args.len() != 2 {
        return false;
    }
    let word = |f: &Frame, w: &[u8]| matches!(f, Frame::BulkString(s) | Frame::SimpleString(s) if s.eq_ignore_ascii_case(w));
    word(&args[0], b"CACHING")
        && ((word(&args[1], b"YES") && state.optin) || (word(&args[1], b"NO") && state.optout))
}

/// The `redirect` field redis reports: the target id, 0 for none, -1 when
/// tracking is off.
fn redirect_field(state: &TrackingState) -> i64 {
    if !state.enabled {
        return -1;
    }
    state
        .redirect
        .map_or(0, |id| i64::try_from(id).unwrap_or(i64::MAX))
}

/// `CLIENT TRACKINGINFO` — a 3-entry map; `flags` is a set.
fn tracking_info(
    args: &[Frame],
    client_id: u64,
    state: &TrackingState,
    table: &parking_lot::Mutex<TrackingTable>,
) -> Frame {
    if args.len() != 1 {
        return err(b"ERR wrong number of arguments for 'client|trackinginfo' command");
    }
    let flag = |s: &'static [u8]| Frame::BulkString(Bytes::from_static(s));
    let mut flags: Vec<Frame> = Vec::with_capacity(4);
    if state.enabled {
        flags.push(flag(b"on"));
        if state.bcast {
            flags.push(flag(b"bcast"));
        }
        if state.optin {
            flags.push(flag(b"optin"));
            if state.caching {
                flags.push(flag(b"caching-yes"));
            }
        }
        if state.optout {
            flags.push(flag(b"optout"));
            if state.caching {
                flags.push(flag(b"caching-no"));
            }
        }
        if state.noloop {
            flags.push(flag(b"noloop"));
        }
        if state.redirect.is_some() && table.lock().is_redirect_broken(client_id) {
            flags.push(flag(b"broken_redirect"));
        }
    } else {
        flags.push(flag(b"off"));
    }
    let prefixes: Vec<Frame> = if state.enabled {
        state
            .prefixes
            .iter()
            .map(|p| Frame::BulkString(p.clone()))
            .collect()
    } else {
        Vec::new()
    };
    Frame::Map(vec![
        (flag(b"flags"), Frame::Set(flags.into())),
        (flag(b"redirect"), Frame::Integer(redirect_field(state))),
        (flag(b"prefixes"), Frame::Array(prefixes.into())),
    ])
}

/// `CLIENT GETREDIR`.
fn get_redir(args: &[Frame], state: &TrackingState) -> Frame {
    if args.len() != 1 {
        return err(b"ERR wrong number of arguments for 'client|getredir' command");
    }
    Frame::Integer(redirect_field(state))
}

/// A connection's registration of its pub/sub channel as a REDIRECT inbox
/// (moon#1048). Dropping it unregisters, so every way a connection handler
/// can end — a normal close, an early return on a write error, a task park,
/// a migration to another shard — releases the entry; none of them carries
/// the pub/sub channel along.
pub struct InboxGuard {
    client_id: u64,
    table: std::sync::Arc<parking_lot::Mutex<TrackingTable>>,
}

impl std::fmt::Debug for InboxGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboxGuard")
            .field("client_id", &self.client_id)
            .finish()
    }
}

impl Drop for InboxGuard {
    fn drop(&mut self) {
        self.table.lock().unregister_inbox(self.client_id);
    }
}

/// Register a connection's freshly created pub/sub channel as a REDIRECT
/// inbox. Called once per connection, where the channel is created — before
/// its first subscription, which is what makes a connection a target that
/// can actually receive. Costs one table lock per subscribing connection,
/// never per command.
#[must_use = "dropping the guard unregisters the inbox"]
pub fn register_inbox(
    client_id: u64,
    tx: &channel::MpscSender<Bytes>,
    resp3: bool,
    table: &std::sync::Arc<parking_lot::Mutex<TrackingTable>>,
) -> InboxGuard {
    table.lock().register_inbox(
        client_id,
        crate::tracking::PubSubInbox {
            tx: tx.clone(),
            resp3,
        },
    );
    InboxGuard {
        client_id,
        table: std::sync::Arc::clone(table),
    }
}

/// Whether a tracking message may be written to a connection speaking
/// `protocol`. A push is RESP3-only: redis never writes one to a RESP2
/// connection (it has nowhere to put it), and moon used to write the RESP3
/// bytes into the RESP2 reply stream.
#[inline]
pub fn push_deliverable(protocol: u8) -> bool {
    protocol >= 3
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    fn args(parts: &[&str]) -> Vec<Frame> {
        parts.iter().map(|p| bs(p)).collect()
    }

    fn err_text(f: &Frame) -> String {
        match f {
            Frame::Error(e) => String::from_utf8_lossy(e).into_owned(),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    struct Conn {
        id: u64,
        state: TrackingState,
        rx: Option<channel::MpscReceiver<Frame>>,
        table: std::sync::Arc<parking_lot::Mutex<TrackingTable>>,
    }

    impl Conn {
        fn new(id: u64) -> Self {
            Self {
                id,
                state: TrackingState::default(),
                rx: None,
                // Ids >= 5000 exist; everything else does not.
                table: std::sync::Arc::new(parking_lot::Mutex::new(
                    TrackingTable::new().with_liveness(|id| id >= 5000),
                )),
            }
        }

        fn run(&mut self, parts: &[&str]) -> Frame {
            let a = args(parts);
            handle(&a, self.id, &mut self.state, &mut self.rx, &self.table)
                .expect("a tracking subcommand")
        }
    }

    impl Drop for Conn {
        fn drop(&mut self) {
            // Keeps the process-wide ACTIVE_TRACKERS count balanced.
            self.table.lock().untrack_all(self.id);
        }
    }

    #[test]
    fn other_subcommands_are_not_owned() {
        let mut c = Conn::new(4001);
        let a = args(&["ID"]);
        assert!(handle(&a, c.id, &mut c.state, &mut c.rx, &c.table).is_none());
        assert!(!is_tracking_subcommand(b"LIST"));
        assert!(is_tracking_subcommand(b"caching"));
        assert!(is_tracking_subcommand(b"TrackingInfo"));
        assert!(is_tracking_subcommand(b"getredir"));
    }

    #[test]
    fn caching_error_order_matches_redis() {
        let mut c = Conn::new(4002);
        // Arity is checked before the tracking state.
        assert_eq!(
            err_text(&c.run(&["CACHING"])),
            "ERR wrong number of arguments for 'client|caching' command"
        );
        // Not tracking: this error even for a bogus argument.
        assert!(err_text(&c.run(&["CACHING", "maybe"])).contains("can be called only when"));
        assert_eq!(c.run(&["TRACKING", "on", "OPTIN"]), ok());
        assert_eq!(c.run(&["CACHING", "yes"]), ok());
        assert!(c.state.caching);
        assert_eq!(err_text(&c.run(&["CACHING", "maybe"])), "ERR syntax error");
    }

    #[test]
    fn mode_switch_errors_depend_on_the_current_mode() {
        let mut c = Conn::new(4003);
        assert_eq!(c.run(&["TRACKING", "on", "OPTIN"]), ok());
        assert!(err_text(&c.run(&["TRACKING", "on", "OPTOUT"])).contains("switch OPTIN/OPTOUT"));
        assert!(err_text(&c.run(&["TRACKING", "on", "BCAST"])).contains("switch BCAST"));
        // The same mode again is fine, and keeps the CACHING flag.
        assert_eq!(c.run(&["CACHING", "yes"]), ok());
        assert_eq!(c.run(&["TRACKING", "on", "OPTIN"]), ok());
        assert!(c.state.caching);
        // OFF, then any mode.
        assert_eq!(c.run(&["TRACKING", "off"]), ok());
        assert!(!c.state.caching);
        assert_eq!(c.run(&["TRACKING", "on", "OPTOUT"]), ok());
    }

    #[test]
    fn redirect_must_name_a_live_client_and_reenable_replaces_it() {
        let mut c = Conn::new(4004);
        let missing = "ERR The client ID you want redirect to does not exist";
        assert_eq!(
            err_text(&c.run(&["TRACKING", "on", "REDIRECT", "12"])),
            missing
        );
        assert_eq!(
            err_text(&c.run(&["TRACKING", "on", "REDIRECT", "0"])),
            missing
        );
        assert_eq!(
            err_text(&c.run(&["TRACKING", "on", "REDIRECT", "-3"])),
            missing
        );
        // Refused: tracking stayed off.
        assert!(!c.state.enabled);
        assert_eq!(
            err_text(&c.run(&["TRACKING", "on", "REDIRECT", "x"])),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(c.run(&["TRACKING", "on", "REDIRECT", "5001"]), ok());
        assert_eq!(c.run(&["GETREDIR"]), Frame::Integer(5001));
        assert_eq!(c.run(&["TRACKING", "on"]), ok());
        assert_eq!(c.run(&["GETREDIR"]), Frame::Integer(0));
        assert_eq!(c.run(&["TRACKING", "off"]), ok());
        assert_eq!(c.run(&["GETREDIR"]), Frame::Integer(-1));
    }

    #[test]
    fn trackinginfo_reports_flags_in_redis_order() {
        let mut c = Conn::new(4005);
        assert_eq!(c.run(&["TRACKING", "on", "OPTOUT", "NOLOOP"]), ok());
        assert_eq!(c.run(&["CACHING", "no"]), ok());
        let Frame::Map(entries) = c.run(&["TRACKINGINFO"]) else {
            panic!("TRACKINGINFO is a map");
        };
        let Frame::Set(flags) = &entries[0].1 else {
            panic!("flags is a set");
        };
        let names: Vec<Frame> = flags.iter().cloned().collect();
        assert_eq!(names, args(&["on", "optout", "caching-no", "noloop"]));
    }

    /// Measured on redis-server 8.6.1: prefixes accumulate across re-enables
    /// (reported sorted), and none may be a prefix of another.
    #[test]
    fn bcast_prefixes_accumulate_sorted_and_never_overlap() {
        let mut c = Conn::new(4006);
        assert_eq!(
            c.run(&["TRACKING", "on", "BCAST", "PREFIX", "zz", "PREFIX", "aa"]),
            ok()
        );
        assert_eq!(c.run(&["TRACKING", "on", "BCAST", "PREFIX", "mm"]), ok());
        assert_eq!(
            c.state.prefixes,
            vec![
                Bytes::from_static(b"aa"),
                Bytes::from_static(b"mm"),
                Bytes::from_static(b"zz"),
            ]
        );
        let overlap = |p: &str, e: &str, what: &str| {
            format!(
                "ERR Prefix '{p}' overlaps with {what} '{e}'. Prefixes for a single client must not overlap."
            )
        };
        assert_eq!(
            err_text(&c.run(&["TRACKING", "on", "BCAST", "PREFIX", "aa"])),
            overlap("aa", "aa", "an existing prefix")
        );
        assert_eq!(
            err_text(&c.run(&["TRACKING", "on", "BCAST", "PREFIX", "aab"])),
            overlap("aab", "aa", "an existing prefix")
        );
        assert_eq!(
            err_text(&c.run(&["TRACKING", "on", "BCAST", "PREFIX", "m"])),
            overlap("m", "mm", "an existing prefix")
        );
        // A bare BCAST is exempt and adds the all-keys prefix.
        assert_eq!(c.run(&["TRACKING", "on", "BCAST"]), ok());
        assert_eq!(c.state.prefixes.first(), Some(&Bytes::new()));

        let mut c = Conn::new(4008);
        assert_eq!(
            err_text(&c.run(&["TRACKING", "on", "BCAST", "PREFIX", "x", "PREFIX", "xy"])),
            overlap("x", "xy", "another provided prefix")
        );
        assert!(!c.state.enabled, "refused: tracking stays off");
        // Redis answers this one with the error AND `+OK`; moon refuses.
        assert_eq!(
            err_text(&c.run(&[
                "TRACKING", "on", "BCAST", "PREFIX", "a", "PREFIX", "b", "PREFIX", "bc"
            ])),
            overlap("b", "bc", "another provided prefix")
        );
        assert_eq!(c.run(&["TRACKING", "on", "BCAST"]), ok());
        assert_eq!(
            err_text(&c.run(&["TRACKING", "on", "BCAST", "PREFIX", "x"])),
            overlap("x", "", "an existing prefix")
        );
    }

    #[test]
    fn caching_would_arm_only_for_an_accepted_caching() {
        let mut s = TrackingState {
            enabled: true,
            optin: true,
            ..TrackingState::default()
        };
        assert!(caching_would_arm(&s, b"client", &args(&["caching", "YES"])));
        assert!(!caching_would_arm(&s, b"CLIENT", &args(&["CACHING", "no"])));
        assert!(!caching_would_arm(&s, b"CLIENT", &args(&["CACHING"])));
        assert!(!caching_would_arm(&s, b"CLIENT", &args(&["ID", "yes"])));
        s.enabled = false;
        assert!(!caching_would_arm(
            &s,
            b"CLIENT",
            &args(&["CACHING", "yes"])
        ));
    }

    /// TRACKING OFF is about this connection's own tracking; its pub/sub
    /// inbox stays reachable for others until the guard drops.
    #[test]
    fn the_inbox_lives_exactly_as_long_as_its_guard() {
        let mut c = Conn::new(4007);
        let (tx, _rx) = channel::mpsc_unbounded::<Bytes>();
        let guard = register_inbox(c.id, &tx, false, &c.table);
        assert_eq!(c.run(&["TRACKING", "on"]), ok());
        assert_eq!(c.run(&["TRACKING", "off"]), ok());
        assert!(c.table.lock().client_exists(c.id));
        drop(guard);
        assert!(
            !c.table.lock().client_exists(c.id),
            "a dropped guard must unregister, or a dead id reads as alive"
        );
    }

    #[test]
    fn only_resp3_connections_take_a_push() {
        assert!(!push_deliverable(2));
        assert!(push_deliverable(3));
    }
}

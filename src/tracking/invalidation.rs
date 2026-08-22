use crate::framevec;
use crate::protocol::Frame;
use bytes::Bytes;
use smallvec::SmallVec;

/// Key set of one write command, captured for deferred (cross-shard)
/// invalidation.
pub type TrackedWriteKeys = SmallVec<[Bytes; 4]>;

/// Construct an invalidation Push frame for the given keys.
/// Wire format: >2\r\n$10\r\ninvalidate\r\n*N\r\n$len\r\nkey\r\n...
pub fn invalidation_push(keys: &[Bytes]) -> Frame {
    let key_frames: Vec<Frame> = keys.iter().map(|k| Frame::BulkString(k.clone())).collect();
    Frame::Push(framevec![
        Frame::BulkString(Bytes::from_static(b"invalidate")),
        Frame::Array(key_frames.into()),
    ])
}

/// Construct the cache-flush invalidation Push (FLUSHALL/FLUSHDB): the RESP3
/// convention is `invalidate` with a Null payload — "drop everything you
/// have cached".
pub fn flush_invalidation_push() -> Frame {
    Frame::Push(framevec![
        Frame::BulkString(Bytes::from_static(b"invalidate")),
        Frame::Null,
    ])
}

/// Post-write invalidation hook, shared by every connection-handler stack.
///
/// Fires for ANY successful write command regardless of the writer's own
/// tracking state (a non-tracking writer must still invalidate other
/// clients' caches — the old per-stack hooks got this backwards). Gated by
/// [`crate::tracking::tracking_active`], so with no tracking clients the
/// write path pays one relaxed atomic load.
///
/// Invalidates every key the command may MODIFY ([`written_keys`]), pushing
/// one `invalidate` frame per key to each tracker. Senders are cross-thread
/// (flume) — this works from any shard thread against the process-global
/// table.
pub fn invalidate_after_write(
    table: &parking_lot::Mutex<crate::tracking::TrackingTable>,
    cmd: &[u8],
    cmd_args: &[Frame],
    writer_client_id: u64,
) {
    if !crate::tracking::tracking_active() {
        return;
    }
    if !crate::command::metadata::is_write(cmd) {
        return;
    }
    let keys = written_keys(cmd, cmd_args);
    invalidate_keys(table, &keys, writer_client_id);
}

/// Keys a blocking pop ACTUALLY modified, derived from its REPLY.
///
/// Blocking commands cannot reuse [`written_keys`] the way every other write
/// does, because the arguments name CANDIDATES and only one of them is served.
/// Both divergences below were measured against redis-server 8.6.1 (moon#644):
///
/// ```text
/// BLPOP k1 k2 0     k1 empty, k2 populated -> redis invalidates k2 ONLY.
///                   k1 was never touched and a client's cached copy of it is
///                   still correct; `written_keys` names both.
/// BLPOP k1 0.1      times out              -> redis invalidates NOTHING.
///                   `written_keys` names k1 whether or not anything popped.
/// ```
///
/// Over-invalidating is not free here: moon#584 is the issue filed because
/// moon pushed invalidations for keys a command only READ, and the fix for
/// that would be undone by a blocking path that invalidates every candidate.
/// So the served key comes from the reply.
///
/// Reply shapes, all pre-RESP3-conversion:
///
/// ```text
/// BLPOP/BRPOP              [key, value]              -> key
/// BLMPOP/BZMPOP            [key, [entries...]]       -> key
/// BZPOPMIN/BZPOPMAX        [key, member, score]      -> key
/// BLMOVE/BRPOPLPUSH        the moved element         -> BOTH args (src, dst)
/// any of them, timed out   Null / NullArray          -> nothing
/// ```
///
/// `BLMOVE` is the one that cannot read its key off the reply: the reply is
/// the element, not a key name. It has exactly two keys and modifies BOTH
/// whenever it serves, so a non-null reply invalidates both arguments —
/// which is what redis does (measured: `BLMOVE src dst` pushes for src and
/// for dst).
pub fn blocking_served_keys(cmd: &[u8], cmd_args: &[Frame], reply: &Frame) -> Vec<Bytes> {
    // A timeout answers Null (RESP2 `*-1` / `$-1`); nothing was modified.
    if matches!(reply, Frame::Null | Frame::NullArray) {
        return Vec::new();
    }
    // BLMOVE / BRPOPLPUSH: two fixed keys, both modified, reply is the element.
    if cmd.eq_ignore_ascii_case(b"BLMOVE") || cmd.eq_ignore_ascii_case(b"BRPOPLPUSH") {
        let mut keys = Vec::with_capacity(2);
        for arg in cmd_args.iter().take(2) {
            if let Frame::BulkString(k) = arg {
                keys.push(k.clone());
            }
        }
        return keys;
    }
    // Everything else answers an array whose FIRST element is the served key.
    match reply {
        Frame::Array(items) => match items.first() {
            Some(Frame::BulkString(k)) => vec![k.clone()],
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Invalidate for a blocking command that has just served a client.
///
/// The single seam for the blocking write path. moon#644: this path did not
/// exist, so every blocking pop modified the keyspace while `invalidate_after_write`
/// — hand-copied at twelve OTHER call sites — was never reached, and a
/// tracking client's cache of a `BLPOP`ped key stayed stale forever.
pub fn invalidate_after_blocking_serve(
    table: &parking_lot::Mutex<crate::tracking::TrackingTable>,
    cmd: &[u8],
    cmd_args: &[Frame],
    reply: &Frame,
    writer_client_id: u64,
) {
    if !crate::tracking::tracking_active() {
        return;
    }
    let keys = blocking_served_keys(cmd, cmd_args, reply);
    invalidate_keys(table, &keys, writer_client_id);
}

/// Invalidate a pre-extracted key list (used by the cross-shard write leg,
/// where the command frame has been moved into the dispatch message by the
/// time the reply arrives — keys are captured at enqueue time under the
/// same `tracking_active()` gate).
pub fn invalidate_keys(
    table: &parking_lot::Mutex<crate::tracking::TrackingTable>,
    keys: &[Bytes],
    writer_client_id: u64,
) {
    if keys.is_empty() {
        return;
    }
    let mut table = table.lock();
    for key in keys {
        let senders = table.invalidate_key(key, writer_client_id);
        if !senders.is_empty() {
            let push = invalidation_push(std::slice::from_ref(key));
            for tx in senders {
                let _ = tx.try_send(push.clone());
            }
        }
    }
}

/// FLUSHALL/FLUSHDB invalidation: push the RESP3 flush invalidation
/// (`invalidate` + Null) to every registered tracking client and clear the
/// per-key table. Called once at the ORIGINATING connection (the table is
/// process-global, so the D-2 cross-shard flush broadcast needs no
/// per-shard invalidation legs).
pub fn invalidate_flush(table: &parking_lot::Mutex<crate::tracking::TrackingTable>) {
    if !crate::tracking::tracking_active() {
        return;
    }
    let senders = table.lock().invalidate_all();
    if senders.is_empty() {
        return;
    }
    let push = flush_invalidation_push();
    for tx in senders {
        let _ = tx.try_send(push.clone());
    }
}

/// Post-read tracking hook: register every key of a successful read command
/// for a tracking client (normal mode). Multi-key reads (`MGET a b`) must
/// track every key, not just the first.
pub fn track_read_keys(
    table: &parking_lot::Mutex<crate::tracking::TrackingTable>,
    cmd: &[u8],
    cmd_args: &[Frame],
    client_id: u64,
    noloop: bool,
) {
    if !crate::command::metadata::is_read(cmd) {
        return;
    }
    let keys = command_keys(cmd, cmd_args);
    if keys.is_empty() {
        return;
    }
    let mut table = table.lock();
    for key in &keys {
        if let Some((evicted_key, senders)) = table.track_key(client_id, key, noloop) {
            // Cap eviction (G1): tell the evicted key's trackers to drop
            // their cached copy — silently forgetting the tracking entry
            // would leave client-side caches permanently stale.
            let push = invalidation_push(std::slice::from_ref(&evicted_key));
            for tx in senders {
                let _ = tx.try_send(push.clone());
            }
        }
    }
}

/// Extract the key arguments of `cmd`. `cmd_args` EXCLUDES the command name.
///
/// Delegates to the shared walker in [`crate::acl::keyspec`], which knows both
/// the registry key specs (`first_key`/`last_key`/`step`) and the layouts a
/// fixed spec cannot express — `numkeys`-counted vectors, the `STREAMS` token,
/// positional `STORE` clauses, subcommand-shaped positions.
///
/// moon#582: this used to walk the registry spec itself and bail on
/// `first_key <= 0`. That is true of every **movablekeys** command, where it
/// means "the keys are not at a FIXED position", NOT "there are no keys" — so
/// `LMPOP`, `ZMPOP`, `SINTERCARD`, `ZDIFF`, `XREAD` and friends silently
/// produced an empty list, and a tracking client's cache of those keys went
/// stale forever with no signal. `SORT src STORE dst` was worse: `first_key=1`
/// named the SOURCE, so it invalidated a key it had not written and missed the
/// one it had.
///
/// An argv we cannot enumerate yields NO keys rather than a guess: invalidating
/// the wrong key would evict a still-valid cache entry, and (on the read side)
/// registering the wrong key would be a silent miss anyway.
pub fn command_keys(cmd: &[u8], cmd_args: &[Frame]) -> SmallVec<[Bytes; 4]> {
    collect_keys(cmd, cmd_args, None)
}

/// The keys `cmd` may MODIFY — the subset of [`command_keys`] that an
/// invalidation push is allowed to name.
///
/// moon#584: `invalidate_after_write` used every key the command NAMED, but
/// redis invalidates only what it MODIFIES (`signalModifiedKey`). A tracking
/// client caching `a` was told it changed by `ZUNIONSTORE d 2 a b`, which only
/// read `a`; `SORT src STORE dst` invalidated `src`; a plain `SORT src` (a
/// write-flagged command that writes nothing without `STORE`) invalidated
/// `src` too. Each spurious push costs the client a dropped cache entry and a
/// refetch.
///
/// The role comes from the shared walker's [`KeyRole`], so the answer cannot
/// drift from the key positions themselves. It is deliberately over-inclusive
/// where the argv genuinely cannot say: `LMPOP 2 a b LEFT` pops from whichever
/// key is non-empty at execution time, and `EVAL` hands its script keys it may
/// or may not write, so all of those stay `Write`. Closing that last gap needs
/// an invalidation hook at the storage-mutation point, not a better key spec.
///
/// [`KeyRole`]: crate::acl::keyspec::KeyRole
pub fn written_keys(cmd: &[u8], cmd_args: &[Frame]) -> SmallVec<[Bytes; 4]> {
    collect_keys(cmd, cmd_args, Some(crate::acl::keyspec::KeyRole::Write))
}

/// Shared body: collect the walker's key positions, optionally filtered to one
/// role.
fn collect_keys(
    cmd: &[u8],
    cmd_args: &[Frame],
    only: Option<crate::acl::keyspec::KeyRole>,
) -> SmallVec<[Bytes; 4]> {
    use crate::acl::keyspec::{KeyPositions, command_key_positions};

    let mut keys: SmallVec<[Bytes; 4]> = SmallVec::new();
    let idx = match command_key_positions(cmd, cmd_args) {
        // `AtPlusComputed` is `SORT ... BY w_*`: the pattern reads keys nobody
        // can name, but the ones we CAN name are still written/read and must
        // still be invalidated — redis reports them either way. (ACL takes the
        // opposite view of the same argv and denies it; that asymmetry is the
        // reason the walker reports facts and each caller applies policy.)
        KeyPositions::At(idx) | KeyPositions::AtPlusComputed(idx) => idx,
        KeyPositions::None | KeyPositions::Unknown => return keys,
    };
    keys.reserve(idx.len());
    for k in idx {
        if only.is_some_and(|want| k.role != want) {
            continue;
        }
        // Cheap: `extract_bytes` clones the `Bytes` handle (refcount), it does
        // not copy the key. A non-string in a key position cannot be a key.
        if let Some(b) = cmd_args
            .get(k.idx)
            .and_then(crate::server::conn::util::extract_bytes)
        {
            keys.push(b);
        }
    }
    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &'static str) -> Frame {
        Frame::BulkString(Bytes::from_static(s.as_bytes()))
    }

    #[test]
    fn test_command_keys_single_key_write() {
        let args = [bulk("k1"), bulk("v1")];
        let keys = command_keys(b"SET", &args);
        assert_eq!(keys.as_slice(), &[Bytes::from_static(b"k1")]);
    }

    #[test]
    fn test_command_keys_del_all_args() {
        let args = [bulk("a"), bulk("b"), bulk("c")];
        let keys = command_keys(b"DEL", &args);
        assert_eq!(keys.len(), 3, "DEL keys = every argument");
    }

    #[test]
    fn test_command_keys_mset_every_second() {
        let args = [bulk("k1"), bulk("v1"), bulk("k2"), bulk("v2")];
        let keys = command_keys(b"MSET", &args);
        assert_eq!(
            keys.as_slice(),
            &[Bytes::from_static(b"k1"), Bytes::from_static(b"k2")],
            "MSET keys = every second argument, values excluded"
        );
    }

    #[test]
    fn test_command_keys_keyless_and_unknown() {
        assert!(command_keys(b"FLUSHALL", &[]).is_empty());
        assert!(command_keys(b"NOSUCHCMD", &[bulk("x")]).is_empty());
    }

    fn keys_of(cmd: &[u8], parts: &[&str]) -> Vec<String> {
        let args: Vec<Frame> = parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
            .collect();
        command_keys(cmd, &args)
            .iter()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .collect()
    }

    /// moon#582. Every command below carries `first_key: 0` in `COMMAND_META`
    /// — which mirrors redis and means "the keys are not at a FIXED argument
    /// position", NOT "there are no keys". Reading it as the latter made this
    /// extractor return an empty list, so `invalidate_after_write` pushed
    /// nothing and `track_read_keys` registered nothing: a tracking client's
    /// cache of these keys went stale permanently, with no signal.
    #[test]
    fn blocking_served_keys_reads_the_reply_not_the_arguments() {
        use crate::protocol::Frame;
        let bs = |b: &[u8]| Frame::BulkString(Bytes::copy_from_slice(b));

        // BLPOP k1 k2 -> served k2. The arguments name BOTH; only the reply
        // says which one actually changed. Invalidating k1 here is the
        // moon#584 over-invalidation bug, re-introduced on a new path.
        let args = [bs(b"k1"), bs(b"k2"), bs(b"0")];
        let reply = Frame::Array(vec![bs(b"k2"), bs(b"v")].into());
        assert_eq!(
            blocking_served_keys(b"BLPOP", &args, &reply),
            vec![Bytes::from_static(b"k2")]
        );

        // A timeout modified nothing, on either null form.
        assert!(blocking_served_keys(b"BLPOP", &args, &Frame::NullArray).is_empty());
        assert!(blocking_served_keys(b"BLPOP", &args, &Frame::Null).is_empty());

        // BLMPOP answers [key, [entries]] — same first-element rule.
        let mp_args = [bs(b"0"), bs(b"2"), bs(b"a"), bs(b"b"), bs(b"LEFT")];
        let mp_reply = Frame::Array(vec![bs(b"b"), Frame::Array(vec![bs(b"x")].into())].into());
        assert_eq!(
            blocking_served_keys(b"BLMPOP", &mp_args, &mp_reply),
            vec![Bytes::from_static(b"b")]
        );

        // BZPOPMIN answers [key, member, score].
        let z_reply = Frame::Array(vec![bs(b"z1"), bs(b"m"), bs(b"1")].into());
        assert_eq!(
            blocking_served_keys(b"BZPOPMIN", &[bs(b"z1"), bs(b"0")], &z_reply),
            vec![Bytes::from_static(b"z1")]
        );

        // BLMOVE is the exception: its reply is the ELEMENT, not a key name,
        // and it modifies both ends. Both arguments, and only on a serve.
        let mv_args = [bs(b"src"), bs(b"dst"), bs(b"LEFT"), bs(b"RIGHT"), bs(b"0")];
        assert_eq!(
            blocking_served_keys(b"BLMOVE", &mv_args, &bs(b"elem")),
            vec![Bytes::from_static(b"src"), Bytes::from_static(b"dst")]
        );
        assert!(blocking_served_keys(b"BLMOVE", &mv_args, &Frame::Null).is_empty());
        assert_eq!(
            blocking_served_keys(b"BRPOPLPUSH", &[bs(b"s"), bs(b"d"), bs(b"0")], &bs(b"e")),
            vec![Bytes::from_static(b"s"), Bytes::from_static(b"d")]
        );
    }

    #[test]
    fn movablekeys_commands_extract_their_keys() {
        // numkeys-counted vectors
        assert_eq!(keys_of(b"LMPOP", &["2", "a", "b", "LEFT"]), ["a", "b"]);
        assert_eq!(keys_of(b"ZMPOP", &["1", "z", "MIN"]), ["z"]);
        assert_eq!(
            keys_of(b"BLMPOP", &["0", "2", "a", "b", "LEFT"]),
            ["a", "b"]
        );
        assert_eq!(keys_of(b"BZMPOP", &["0", "1", "z", "MIN"]), ["z"]);
        assert_eq!(keys_of(b"SINTERCARD", &["2", "a", "b"]), ["a", "b"]);
        assert_eq!(keys_of(b"ZINTERCARD", &["2", "a", "b"]), ["a", "b"]);
        assert_eq!(keys_of(b"ZDIFF", &["2", "a", "b"]), ["a", "b"]);
        assert_eq!(keys_of(b"ZINTER", &["2", "a", "b"]), ["a", "b"]);
        assert_eq!(keys_of(b"ZUNION", &["2", "a", "b"]), ["a", "b"]);
        // scripting: the declared keys are real accesses
        assert_eq!(keys_of(b"EVAL", &["s", "1", "k"]), ["k"]);
        assert_eq!(keys_of(b"EVALSHA", &["sha", "1", "k"]), ["k"]);
        assert_eq!(keys_of(b"FCALL", &["f", "2", "a", "b"]), ["a", "b"]);
        // keys after the STREAMS token
        assert_eq!(
            keys_of(b"XREAD", &["COUNT", "1", "STREAMS", "a", "b", "0", "0"]),
            ["a", "b"]
        );
        assert_eq!(
            keys_of(b"XREADGROUP", &["GROUP", "g", "c", "STREAMS", "s", ">"]),
            ["s"]
        );
    }

    /// The third failure shape: `SORT src STORE dst` WRITES `dst`, but
    /// `first_key = 1` names `src`. Moon invalidated the key it did not write
    /// and missed the one it did. A `BY`/`GET` wildcard does not change which
    /// keys are NAMED — redis reports and invalidates them either way — even
    /// though the ACL extractor deliberately calls that argv indeterminate so
    /// a `~pattern` user cannot reach runtime-computed key names through it.
    #[test]
    fn store_clauses_invalidate_the_destination_they_write() {
        assert_eq!(
            keys_of(b"SORT", &["src", "ALPHA", "STORE", "dst"]),
            ["src", "dst"]
        );
        assert_eq!(
            keys_of(b"SORT", &["src", "BY", "w_*", "STORE", "dst"]),
            ["src", "dst"]
        );
        assert_eq!(keys_of(b"SORT", &["src", "GET", "d_*"]), ["src"]);
        assert_eq!(
            keys_of(b"GEORADIUS", &["src", "1", "2", "3", "m", "STORE", "dst"]),
            ["src", "dst"]
        );
        assert_eq!(
            keys_of(
                b"GEORADIUSBYMEMBER",
                &["src", "m", "3", "m", "STOREDIST", "dst"]
            ),
            ["src", "dst"]
        );
    }

    /// Subcommand-shaped key positions, and the keyless subcommands of the
    /// same containers, which must stay empty.
    #[test]
    fn subcommand_shaped_keys_and_their_keyless_siblings() {
        assert_eq!(keys_of(b"OBJECT", &["ENCODING", "k"]), ["k"]);
        assert_eq!(keys_of(b"XINFO", &["STREAM", "k"]), ["k"]);
        assert_eq!(keys_of(b"MEMORY", &["USAGE", "k"]), ["k"]);
        assert_eq!(keys_of(b"XGROUP", &["CREATE", "k", "g", "$"]), ["k"]);
        assert!(keys_of(b"OBJECT", &["HELP"]).is_empty());
        assert!(keys_of(b"MEMORY", &["STATS"]).is_empty());
    }

    /// A malformed argv must not invalidate a wrong key. `numkeys` larger than
    /// the argv, or unparseable, means we cannot say which keys were touched —
    /// invalidating a guess would be worse than invalidating nothing.
    #[test]
    fn unenumerable_argv_yields_no_keys_rather_than_a_guess() {
        assert!(keys_of(b"LMPOP", &["notanumber", "a", "LEFT"]).is_empty());
        assert!(keys_of(b"LMPOP", &["4", "a", "b", "LEFT"]).is_empty());
        assert!(keys_of(b"LMPOP", &[]).is_empty());
        // usize-overflowing numkeys must not panic (this runs on a write path)
        let huge = usize::MAX.to_string();
        assert!(keys_of(b"LMPOP", &[&huge, "a", "LEFT"]).is_empty());
        assert!(keys_of(b"EVAL", &["s", &huge, "a"]).is_empty());
    }

    fn written_of(cmd: &[u8], parts: &[&str]) -> Vec<String> {
        let args: Vec<Frame> = parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
            .collect();
        written_keys(cmd, &args)
            .iter()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .collect()
    }

    /// moon#584. `invalidate_after_write` pushed every key the command NAMED;
    /// redis pushes only what it MODIFIES. A tracking client caching `a` was
    /// told `a` changed by `ZUNIONSTORE d 2 a b`, which only read it.
    ///
    /// Each case asserts the FULL key list on both sides: `command_keys` must
    /// keep naming every key (routing and read-tracking depend on it) while
    /// `written_keys` names only the destination. Asserting one without the
    /// other would pass for a fix that simply dropped the sources everywhere.
    #[test]
    fn issue_584_store_sources_are_read_not_written() {
        // The two rows measured in the issue.
        assert_eq!(
            keys_of(b"ZUNIONSTORE", &["d", "2", "a", "b"]),
            ["d", "a", "b"]
        );
        assert_eq!(written_of(b"ZUNIONSTORE", &["d", "2", "a", "b"]), ["d"]);
        assert_eq!(
            keys_of(b"SORT", &["src", "ALPHA", "STORE", "dst"]),
            ["src", "dst"]
        );
        assert_eq!(
            written_of(b"SORT", &["src", "ALPHA", "STORE", "dst"]),
            ["dst"]
        );

        // The rest of the same shape.
        assert_eq!(written_of(b"ZINTERSTORE", &["d", "2", "a", "b"]), ["d"]);
        assert_eq!(written_of(b"ZDIFFSTORE", &["d", "2", "a", "b"]), ["d"]);
        assert_eq!(
            written_of(b"GEORADIUS", &["src", "1", "2", "3", "m", "STORE", "dst"]),
            ["dst"]
        );
        assert_eq!(
            written_of(
                b"GEORADIUSBYMEMBER",
                &["src", "m", "3", "m", "STOREDIST", "dst"]
            ),
            ["dst"]
        );
        // Fixed-spec destination-first commands have exactly the same defect.
        assert_eq!(written_of(b"SINTERSTORE", &["d", "a", "b"]), ["d"]);
        assert_eq!(written_of(b"SUNIONSTORE", &["d", "a", "b"]), ["d"]);
        assert_eq!(written_of(b"SDIFFSTORE", &["d", "a", "b"]), ["d"]);
        assert_eq!(written_of(b"PFMERGE", &["d", "a", "b"]), ["d"]);
        assert_eq!(written_of(b"BITOP", &["AND", "d", "a", "b"]), ["d"]);
        assert_eq!(written_of(b"ZRANGESTORE", &["d", "s", "0", "-1"]), ["d"]);
        assert_eq!(written_of(b"GEOSEARCHSTORE", &["d", "s", "x"]), ["d"]);
        // ... and `COPY`, whose destination is LAST, not first.
        assert_eq!(keys_of(b"COPY", &["src", "dst"]), ["src", "dst"]);
        assert_eq!(written_of(b"COPY", &["src", "dst"]), ["dst"]);
    }

    /// `SORT` is a WRITE-flagged command that writes NOTHING without a `STORE`
    /// clause, so a plain `SORT src` must push no invalidation at all — redis
    /// does not. This is the case a "drop the first key" fix would get wrong.
    #[test]
    fn issue_584_sort_without_store_invalidates_nothing() {
        assert_eq!(keys_of(b"SORT", &["src", "ALPHA"]), ["src"]);
        assert!(written_of(b"SORT", &["src", "ALPHA"]).is_empty());
        assert!(written_of(b"SORT", &["src", "BY", "w_*"]).is_empty());
        assert!(
            written_of(b"GEORADIUS", &["src", "1", "2", "3", "m"]).is_empty(),
            "GEORADIUS without STORE writes nothing"
        );
        // A `BY` pattern does not change which keys are NAMED, and the named
        // source is still only read.
        assert_eq!(
            written_of(b"SORT", &["src", "BY", "w_*", "STORE", "dst"]),
            ["dst"]
        );
    }

    /// The direction that must NOT change: a command that really does write
    /// every key it names keeps invalidating every one of them. Under-
    /// invalidating is moon#582 — a client cache stale forever — so this is
    /// the guard that the role split did not overshoot.
    #[test]
    fn commands_that_write_every_key_still_invalidate_every_key() {
        assert_eq!(written_of(b"MSET", &["a", "1", "b", "2"]), ["a", "b"]);
        assert_eq!(written_of(b"DEL", &["a", "b", "c"]), ["a", "b", "c"]);
        assert_eq!(written_of(b"SET", &["k", "v"]), ["k"]);
        assert_eq!(written_of(b"RENAME", &["a", "b"]), ["a", "b"]);
        assert_eq!(written_of(b"RENAMENX", &["a", "b"]), ["a", "b"]);
        assert_eq!(written_of(b"SMOVE", &["s", "d", "m"]), ["s", "d"]);
        assert_eq!(
            written_of(b"LMOVE", &["s", "d", "LEFT", "RIGHT"]),
            ["s", "d"]
        );
        assert_eq!(written_of(b"RPOPLPUSH", &["s", "d"]), ["s", "d"]);
        assert_eq!(written_of(b"XGROUP", &["CREATE", "k", "g", "$"]), ["k"]);
        assert_eq!(
            written_of(b"XREADGROUP", &["GROUP", "g", "c", "STREAMS", "s", ">"]),
            ["s"],
            "XREADGROUP advances the group's last-delivered id"
        );
        // The MPOP family: which key is popped depends on which is non-empty
        // at execution time, so a static spec MUST keep all of them writable.
        // Over-invalidating costs a refetch; under-invalidating goes stale.
        assert_eq!(written_of(b"LMPOP", &["2", "a", "b", "LEFT"]), ["a", "b"]);
        assert_eq!(written_of(b"ZMPOP", &["2", "a", "b", "MIN"]), ["a", "b"]);
        assert_eq!(
            written_of(b"BLMPOP", &["0", "2", "a", "b", "LEFT"]),
            ["a", "b"]
        );
    }

    /// Read-only commands never reach `invalidate_after_write` (it gates on
    /// `is_write` first), but their keys must still be READ-role so that a
    /// future caller cannot mistake them for modifications.
    #[test]
    fn read_only_commands_write_nothing() {
        assert_eq!(keys_of(b"SINTERCARD", &["2", "a", "b"]), ["a", "b"]);
        assert!(written_of(b"SINTERCARD", &["2", "a", "b"]).is_empty());
        assert!(written_of(b"ZDIFF", &["2", "a", "b"]).is_empty());
        assert!(written_of(b"ZUNION", &["2", "a", "b"]).is_empty());
        assert!(written_of(b"ZINTER", &["2", "a", "b"]).is_empty());
        assert!(written_of(b"ZINTERCARD", &["2", "a", "b"]).is_empty());
        assert!(written_of(b"MGET", &["a", "b"]).is_empty());
        assert!(written_of(b"SORT_RO", &["k"]).is_empty());
        assert!(written_of(b"FCALL_RO", &["f", "1", "k"]).is_empty());
        assert!(
            written_of(b"XREAD", &["COUNT", "1", "STREAMS", "a", "0"]).is_empty(),
            "plain XREAD does not modify the stream"
        );
        assert!(written_of(b"OBJECT", &["ENCODING", "k"]).is_empty());
        assert!(written_of(b"MEMORY", &["USAGE", "k"]).is_empty());
    }

    /// `written_keys` is a SUBSET of `command_keys` by construction; pinning
    /// it means a future role change can never invent a key the argv does not
    /// name (which would invalidate a key nobody asked about).
    #[test]
    fn written_keys_never_names_a_key_the_command_does_not() {
        for (cmd, argv) in [
            (b"ZUNIONSTORE".as_ref(), &["d", "2", "a", "b"][..]),
            (b"SORT".as_ref(), &["s", "STORE", "d"][..]),
            (b"COPY".as_ref(), &["s", "d"][..]),
            (b"BITOP".as_ref(), &["AND", "d", "a"][..]),
            (b"LMPOP".as_ref(), &["2", "a", "b", "LEFT"][..]),
            (b"DEL".as_ref(), &["a", "b"][..]),
        ] {
            let all = keys_of(cmd, argv);
            for k in written_of(cmd, argv) {
                assert!(
                    all.contains(&k),
                    "{} invalidated {k:?}, which it does not name",
                    String::from_utf8_lossy(cmd)
                );
            }
        }
    }

    #[test]
    fn test_flush_invalidation_push_shape() {
        assert_eq!(
            flush_invalidation_push(),
            Frame::Push(framevec![
                Frame::BulkString(Bytes::from_static(b"invalidate")),
                Frame::Null,
            ])
        );
    }

    #[test]
    fn test_invalidation_push_single_key() {
        let key = Bytes::from_static(b"foo");
        let frame = invalidation_push(std::slice::from_ref(&key));
        assert_eq!(
            frame,
            Frame::Push(framevec![
                Frame::BulkString(Bytes::from_static(b"invalidate")),
                Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"foo"))]),
            ])
        );
    }

    #[test]
    fn test_invalidation_push_multiple_keys() {
        let keys = vec![Bytes::from_static(b"foo"), Bytes::from_static(b"bar")];
        let frame = invalidation_push(&keys);
        assert_eq!(
            frame,
            Frame::Push(framevec![
                Frame::BulkString(Bytes::from_static(b"invalidate")),
                Frame::Array(framevec![
                    Frame::BulkString(Bytes::from_static(b"foo")),
                    Frame::BulkString(Bytes::from_static(b"bar")),
                ]),
            ])
        );
    }
}

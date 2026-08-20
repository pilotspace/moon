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
/// Invalidates EVERY key of the command per its registry key spec
/// (`DEL a b`, `MSET k1 v1 k2 v2`), pushing one `invalidate` frame per key
/// to each tracker. Senders are cross-thread (flume) — this works from any
/// shard thread against the process-global table.
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
    let keys = command_keys(cmd, cmd_args);
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
    for i in idx {
        // Cheap: `extract_bytes` clones the `Bytes` handle (refcount), it does
        // not copy the key. A non-string in a key position cannot be a key.
        if let Some(b) = cmd_args
            .get(i)
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

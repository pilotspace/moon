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
        table.track_key(client_id, key, noloop);
    }
}

/// Extract the key arguments of `cmd` per its registry key spec
/// (`first_key`/`last_key`/`step`, Redis argv semantics where index 1 is the
/// first argument after the command name; `cmd_args` here EXCLUDES the
/// command name, so spec index N maps to `cmd_args[N-1]`).
///
/// Unknown commands and keyless commands return an empty vec. Used by the
/// CLIENT TRACKING invalidation hook so multi-key writes (`DEL a b`,
/// `MSET k1 v1 k2 v2`) invalidate every written key, not just the first.
pub fn command_keys(cmd: &[u8], cmd_args: &[Frame]) -> SmallVec<[Bytes; 4]> {
    let mut keys: SmallVec<[Bytes; 4]> = SmallVec::new();
    let Some(meta) = crate::command::metadata::lookup(cmd) else {
        return keys;
    };
    if meta.first_key <= 0 || cmd_args.is_empty() {
        return keys;
    }
    let first = (meta.first_key - 1) as usize;
    // last_key < 0 counts from the end (-1 = last argument).
    let last = if meta.last_key < 0 {
        match cmd_args.len().checked_sub((-meta.last_key) as usize) {
            Some(idx) => idx,
            None => return keys,
        }
    } else {
        ((meta.last_key - 1) as usize).min(cmd_args.len() - 1)
    };
    let step = meta.step.max(1) as usize;
    let mut i = first;
    while i <= last {
        if let Some(b) = crate::server::conn::util::extract_bytes(&cmd_args[i]) {
            keys.push(b);
        }
        i += step;
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

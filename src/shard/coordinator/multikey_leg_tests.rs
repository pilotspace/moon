//! The coordinator's multi-key write legs, driven on a real current-thread
//! runtime against a fake second shard.
//!
//! Shard 0 is this test thread (its slice is installed with
//! `reset_test_shard`); shard 1 is a thread that pops the SPSC ring and
//! answers every `MultiExecute` sub-command, recording what it was sent. That
//! is enough to drive the coordinator's local leg AND its remote legs without
//! a shard event loop.
//!
//! - moon#1228: an armed BGSAVE epoch must capture the pre-image of every key
//!   an `MSET` local leg overwrites (the all-local fast path and the local
//!   slice of a spanning `MSET`).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use ringbuf::traits::{Consumer, Split};
use ringbuf::{HeapProd, HeapRb};

use crate::persistence::snapshot_cow;
use crate::protocol::Frame;
use crate::runtime::channel;
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::Database;
use crate::storage::entry::CachedClock;

/// Runs `fut` on a current-thread runtime of the compiled-in flavour, timers
/// on (the remote legs' reply await races a timer).
fn block_on_with_timer<F: std::future::Future>(fut: F) -> F::Output {
    #[cfg(feature = "runtime-monoio")]
    {
        monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(fut)
    }
    #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-monoio")))]
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("tokio runtime")
            .block_on(fut)
    }
}

fn bulk(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s))
}

/// `n` distinct key names owned by `shard` of `num_shards`.
fn keys_on(shard: usize, num_shards: usize, n: usize, prefix: &str) -> Vec<Bytes> {
    (0..)
        .map(|i| Bytes::from(format!("{prefix}:{i}")))
        .filter(|k| key_to_shard(k, num_shards) == shard)
        .take(n)
        .collect()
}

/// The sub-commands one `MultiExecute` message carried.
type SeenMessage = Vec<Frame>;

/// What the fake shard answers for one sub-command.
fn fake_reply(command: &Frame) -> Frame {
    let Frame::Array(parts) = command else {
        return Frame::Error(Bytes::from_static(b"ERR fake shard: not an array"));
    };
    let name = match parts.first() {
        Some(Frame::BulkString(n)) => n.to_ascii_uppercase(),
        _ => return Frame::Error(Bytes::from_static(b"ERR fake shard: no name")),
    };
    match name.as_slice() {
        b"SET" | b"MSET" => Frame::SimpleString(Bytes::from_static(b"OK")),
        // Every named key "existed": the count is the number of key args.
        b"DEL" | b"UNLINK" | b"EXISTS" | b"TOUCH" => Frame::Integer(parts.len() as i64 - 1),
        _ => Frame::Error(Bytes::from_static(b"ERR fake shard: unexpected command")),
    }
}

/// Shard 0 local (slice installed on this thread), shard 1 faked.
struct TwoShards {
    shard_databases: Arc<ShardDatabases>,
    dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    notifiers: Vec<Arc<channel::Notify>>,
    seen: Arc<parking_lot::Mutex<Vec<SeenMessage>>>,
    stop: Arc<AtomicBool>,
    remote: Option<std::thread::JoinHandle<()>>,
}

impl TwoShards {
    /// `local` is shard 0's db 0 contents.
    fn new(local: &[(&Bytes, &[u8])]) -> Self {
        let mut db0 = Database::new();
        for (k, v) in local {
            db0.set_string(k, Bytes::copy_from_slice(v));
        }
        let (shard_databases, mut inits) =
            ShardDatabases::new(vec![vec![db0], vec![Database::new()]]);
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));
        // Shard 0's only producer targets shard 1 (`target_index(0, 1) == 0`).
        let (prod, mut cons) = HeapRb::<ShardMessage>::new(64).split();
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let remote = {
            let seen = seen.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let Some(msg) = cons.try_pop() else {
                        std::thread::sleep(std::time::Duration::from_micros(50));
                        continue;
                    };
                    if let ShardMessage::MultiExecute {
                        commands, reply_tx, ..
                    } = msg
                    {
                        let frames: Vec<Frame> = commands.into_iter().map(|(_, f)| f).collect();
                        let replies = frames.iter().map(fake_reply).collect();
                        seen.lock().push(frames);
                        let _ = reply_tx.send(replies);
                    }
                }
            })
        };
        Self {
            shard_databases,
            dispatch_tx: Rc::new(RefCell::new(vec![prod])),
            notifiers: vec![
                Arc::new(channel::Notify::new()),
                Arc::new(channel::Notify::new()),
            ],
            seen,
            stop,
            remote: Some(remote),
        }
    }

    fn mset(&self, args: &[Frame]) -> Frame {
        let clock = CachedClock::new();
        let mut barrier = false;
        block_on_with_timer(super::coordinate_mset(
            args,
            0,
            2,
            0,
            &self.shard_databases,
            &self.dispatch_tx,
            &self.notifiers,
            &clock,
            None,
            &None,
            &mut barrier,
            &(),
        ))
    }
}

impl Drop for TwoShards {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.remote.take() {
            let _ = h.join();
        }
    }
}

/// Every key the fake shard was sent (`k` of `<CMD> k v k v …` and
/// `<CMD> k k …` alike), sorted.
fn sent_keys(two: &TwoShards) -> Vec<Bytes> {
    let mut keys = Vec::new();
    for message in two.seen.lock().iter() {
        for command in message {
            let Frame::Array(parts) = command else {
                continue;
            };
            let name = match parts.first() {
                Some(Frame::BulkString(n)) => n.to_ascii_uppercase(),
                _ => continue,
            };
            let step = if matches!(name.as_slice(), b"SET" | b"MSET") {
                2
            } else {
                1
            };
            for arg in parts[1..].iter().step_by(step) {
                if let Frame::BulkString(k) = arg {
                    keys.push(k.clone());
                }
            }
        }
    }
    keys.sort();
    keys
}

fn sorted(keys: &[Bytes]) -> Vec<Bytes> {
    let mut v = keys.to_vec();
    v.sort();
    v
}

/// `(key, captured value)` for every captured entry, and the keys captured
/// as absent — then disarm.
fn take_captures() -> (Vec<(Bytes, Bytes)>, Vec<Bytes>) {
    let entries = snapshot_cow::pending_for_test()
        .into_iter()
        .map(|(db, k, e)| {
            assert_eq!(db, 0);
            let v = e
                .value
                .as_bytes()
                .map(Bytes::copy_from_slice)
                .unwrap_or_default();
            (k, v)
        })
        .collect();
    let absent = snapshot_cow::pending_tombstones_for_test()
        .into_iter()
        .map(|(_, k)| k)
        .collect();
    snapshot_cow::disarm();
    (entries, absent)
}

/// moon#1228: the all-local fast path (`--shards 1`, or every key on the
/// connection's shard) wrote through `string::mset` and captured nothing.
#[test]
fn mset_all_local_fast_path_captures_every_pre_image() {
    let mut db0 = Database::new();
    db0.set_string(b"a", Bytes::from_static(b"old-a"));
    db0.set_string(b"b", Bytes::from_static(b"old-b"));
    let (shard_databases, mut inits) = ShardDatabases::new(vec![vec![db0]]);
    crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(inits.remove(0)));
    let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> = Rc::new(RefCell::new(Vec::new()));
    let clock = CachedClock::new();
    let mut barrier = false;
    let args = [
        bulk(b"a"),
        bulk(b"new"),
        bulk(b"b"),
        bulk(b"new"),
        bulk(b"c"),
        bulk(b"new"),
    ];

    snapshot_cow::disarm();
    snapshot_cow::arm();
    let reply = block_on_with_timer(super::coordinate_mset(
        &args,
        0,
        1,
        0,
        &shard_databases,
        &dispatch_tx,
        &[],
        &clock,
        None,
        &None,
        &mut barrier,
        &(),
    ));
    let (mut entries, absent) = take_captures();
    entries.sort();

    assert_eq!(reply, Frame::SimpleString(Bytes::from_static(b"OK")));
    assert_eq!(
        entries,
        vec![
            (Bytes::from_static(b"a"), Bytes::from_static(b"old-a")),
            (Bytes::from_static(b"b"), Bytes::from_static(b"old-b")),
        ],
        "an armed epoch must hold the EPOCH-START value of every key MSET overwrote"
    );
    assert_eq!(
        absent,
        vec![Bytes::from_static(b"c")],
        "a created key is captured absent"
    );
}

/// moon#1228: the local slice of a spanning `MSET` wrote through a bare
/// `set_string` loop and captured nothing; the remote slices are captured on
/// their owners (`cow_intercept`), so only this leg leaked post-images.
#[test]
fn spanning_mset_local_slice_captures_every_pre_image() {
    let local = keys_on(0, 2, 3, "cap");
    let remote = keys_on(1, 2, 3, "cap");
    let two = TwoShards::new(&[(&local[0], b"old-0"), (&local[1], b"old-1")]);
    let mut args = Vec::new();
    for k in local.iter().chain(remote.iter()) {
        args.push(Frame::BulkString(k.clone()));
        args.push(bulk(b"new"));
    }

    snapshot_cow::disarm();
    snapshot_cow::arm();
    let reply = two.mset(&args);
    let (mut entries, absent) = take_captures();
    entries.sort();

    assert_eq!(reply, Frame::SimpleString(Bytes::from_static(b"OK")));
    let mut want = vec![
        (local[0].clone(), Bytes::from_static(b"old-0")),
        (local[1].clone(), Bytes::from_static(b"old-1")),
    ];
    want.sort();
    assert_eq!(
        entries, want,
        "the local slice of a spanning MSET must capture its epoch-start values"
    );
    assert_eq!(
        absent,
        vec![local[2].clone()],
        "a created local key is captured absent"
    );
    // The local slice really was written (and only the local slice).
    crate::shard::slice::with_shard_db(0, |db| {
        for k in &local {
            assert_eq!(
                db.get(k)
                    .and_then(|e| e.value.as_bytes().map(<[u8]>::to_vec)),
                Some(b"new".to_vec())
            );
        }
        for k in &remote {
            assert!(db.get(k).is_none(), "a remote key must not land on shard 0");
        }
    });
    assert_eq!(
        sent_keys(&two),
        sorted(&remote),
        "the remote leg carries the remote keys"
    );
}

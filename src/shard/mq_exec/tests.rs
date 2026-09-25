//! Unit tests of `shard::mq_exec` (moved out of `mq_exec.rs` to keep it
//! under the 1,500-line file limit).

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use super::*;
use crate::shard::shared_databases::ShardStoreMemory;
use crate::shard::slice::{ShardSlice, ShardSliceInit, init_shard};
use crate::storage::Database;
use crate::text::store::TextStore;
use crate::transaction::{DeferredHnswInserts, KvWriteIntents};
use crate::vector::store::VectorStore;

fn make_test_slice(db_count: usize) -> ShardSlice {
    let databases =
        crate::shard::db_plane::build_sets(vec![(0..db_count).map(|_| Database::new()).collect()])
            .first()
            .map(std::sync::Arc::clone)
            .expect("build_sets yields one set per shard");
    ShardSlice::new(ShardSliceInit {
        shard_id: 0,
        databases,
        vector_store: VectorStore::new(),
        text_store: TextStore::new(),
        #[cfg(feature = "graph")]
        graph_store: crate::graph::store::GraphStore::new(),
        kv_write_intents: KvWriteIntents::new(),
        deferred_hnsw_inserts: DeferredHnswInserts::new(),
        temporal_registry: None,
        temporal_kv_index: None,
        durable_queue_registry: None,
        trigger_registry: None,
        wal_append_tx: None,
        estimated_memory: Arc::new(AtomicUsize::new(0)),
        store_memory: Arc::new(ShardStoreMemory {
            vector: AtomicUsize::new(0),
            text: AtomicUsize::new(0),
            graph: AtomicUsize::new(0),
            lua: AtomicUsize::new(0),
            lua_vm: AtomicUsize::new(0),
            pagecache: AtomicUsize::new(0),
        }),
    })
}

// ── dead-letter ceiling ───────────────────────────────────────────────────

#[test]
fn dead_letter_ceiling_pins_the_shipped_comparison() {
    // MAXDELIVERY 0 disables dead-lettering entirely — the documented
    // escape hatch for callers hit by moon#663.
    assert!(!should_dead_letter(1, 0));
    assert!(!should_dead_letter(9_999, 0));

    // MAXDELIVERY >= 2 delivers a first attempt to the consumer.
    assert!(!should_dead_letter(1, 2));
    assert!(!should_dead_letter(1, 3));

    // KNOWN DEFECT (moon#663), pinned deliberately so the fix has to come
    // here: `delivery_count` counts the delivery in progress, so `>=`
    // makes MAXDELIVERY 1 dead-letter the FIRST delivery and such a queue
    // never delivers anything. Correcting this to `>` requires a
    // redelivery path to exist first, or the branch becomes unreachable.
    assert!(should_dead_letter(1, 1));
}

// ── effective_key derivation ──────────────────────────────────────────────

#[test]
fn effective_key_without_prefix() {
    let prefix = Bytes::new();
    let raw = Bytes::from_static(b"myqueue");
    let eff = effective_key(&prefix, &raw);
    assert_eq!(eff.as_ref(), b"myqueue");
}

#[test]
fn effective_key_with_prefix() {
    // key_prefix = "{" + 32 hex chars + "}:"
    let ws_hex = "0102030405060708090a0b0c0d0e0f10";
    let prefix_str = format!("{{{ws_hex}}}:");
    let prefix = Bytes::from(prefix_str.clone());
    let raw = Bytes::from_static(b"tasks");
    let eff = effective_key(&prefix, &raw);
    let expected = format!("{prefix_str}tasks");
    assert_eq!(eff.as_ref(), expected.as_bytes());
}

// ── derive_trig_key ───────────────────────────────────────────────────────

#[test]
fn trig_key_without_prefix() {
    let prefix = Bytes::new();
    let raw = Bytes::from_static(b"alerts");
    let trig = derive_trig_key(&prefix, &raw);
    assert_eq!(trig.as_ref(), b"alerts");
}

#[test]
fn trig_key_with_prefix() {
    // prefix = "{0102...10}:" (35 bytes)
    let ws_hex = "0102030405060708090a0b0c0d0e0f10";
    let prefix = Bytes::from(format!("{{{ws_hex}}}:"));
    let raw = Bytes::from_static(b"alerts");
    let trig = derive_trig_key(&prefix, &raw);
    // expected: ws_hex + ":" + "alerts"
    let expected = format!("{ws_hex}:alerts");
    assert_eq!(trig.as_ref(), expected.as_bytes());
}

// ── DLQLEN on empty queue ─────────────────────────────────────────────────

#[test]
fn dlqlen_empty_queue_returns_zero() {
    // Use a fresh OS thread so init_shard doesn't conflict with the test thread.
    let result = std::thread::spawn(|| {
        init_shard(make_test_slice(1));

        // Build a fake MQ DLQLEN command frame.
        let cmd = Arc::new(Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"MQ")),
                Frame::BulkString(Bytes::from_static(b"DLQLEN")),
                Frame::BulkString(Bytes::from_static(b"nosuchqueue")),
            ]
            .into(),
        ));
        execute_mq_on_owner(0, Bytes::new(), cmd, &mut |_, _| Ok(()))
    })
    .join()
    .expect("test thread panicked");

    assert_eq!(result, Frame::Integer(0));
}

// ── MQ.CREATE + DLQLEN round-trip ─────────────────────────────────────────

#[test]
fn create_then_dlqlen_zero() {
    let result = std::thread::spawn(|| {
        init_shard(make_test_slice(1));

        // CREATE
        let create_cmd = Arc::new(Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"MQ")),
                Frame::BulkString(Bytes::from_static(b"CREATE")),
                Frame::BulkString(Bytes::from_static(b"testq")),
            ]
            .into(),
        ));
        let create_result = execute_mq_on_owner(0, Bytes::new(), create_cmd, &mut |_, _| Ok(()));
        assert_eq!(
            create_result,
            Frame::SimpleString(Bytes::from_static(b"OK")),
            "CREATE must return +OK"
        );

        // DLQLEN on newly created queue — DLQ stream doesn't exist yet.
        let dlqlen_cmd = Arc::new(Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"MQ")),
                Frame::BulkString(Bytes::from_static(b"DLQLEN")),
                Frame::BulkString(Bytes::from_static(b"testq")),
            ]
            .into(),
        ));
        execute_mq_on_owner(0, Bytes::new(), dlqlen_cmd, &mut |_, _| Ok(()))
    })
    .join()
    .expect("test thread panicked");

    assert_eq!(result, Frame::Integer(0));
}

// ── moon#1228: snapshot pre-image capture ─────────────────────────────────

fn mq(parts: &[&str]) -> Frame {
    let mut argv = vec![Frame::BulkString(Bytes::from_static(b"MQ"))];
    argv.extend(
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes()))),
    );
    execute_mq_on_owner(
        0,
        Bytes::new(),
        Arc::new(Frame::Array(argv.into())),
        &mut |_, _| Ok(()),
    )
}

/// The id of the one entry an `MQ POP ... COUNT 1` reply carries.
fn popped_id(reply: &Frame) -> String {
    let Frame::Array(entries) = reply else {
        panic!("POP reply {reply:?}");
    };
    let Some(Frame::Array(entry)) = entries.first() else {
        panic!("POP delivered nothing: {reply:?}");
    };
    match entry.first() {
        Some(Frame::BulkString(id)) => String::from_utf8_lossy(id).into_owned(),
        other => panic!("POP entry id {other:?}"),
    }
}

/// MQ.CREATE / PUSH / POP / ACK, a TXN MQ.PUBLISH materialization and a
/// replicated MQ record all write their queue outside `command::dispatch`.
/// Under an armed BGSAVE epoch each must capture the queue's epoch-start
/// state first; before moon#1228 none did, so a queue whose range the save
/// had not written yet reached the file with its post-epoch length, PEL
/// and group cursor, and a queue created mid-save was in the file at all.
#[test]
fn mq_writes_mid_epoch_keep_the_snapshot_point_in_time() {
    use crate::persistence::snapshot::{SnapshotState, shard_snapshot_load};
    use crate::persistence::snapshot_cow;
    use crate::shard::slice::with_shard_db;

    std::thread::spawn(|| {
        init_shard(make_test_slice(1));
        const QUEUES: usize = 64;
        let q = |i: usize| format!("q{i:03}");
        with_shard_db(0, |db| {
            for i in 0..3000u32 {
                db.set_string(format!("fill:{i}").as_bytes(), Bytes::from_static(b"x"));
            }
        });
        for i in 0..QUEUES {
            assert_eq!(
                mq(&["CREATE", &q(i)]),
                Frame::SimpleString(Bytes::from_static(b"OK"))
            );
            for _ in 0..3 {
                assert!(matches!(
                    mq(&["PUSH", &q(i), "f", "v"]),
                    Frame::BulkString(_)
                ));
            }
        }

        // BGSAVE starts; one segment of db 0 is written.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("s.rrdshard");
        let mut state = with_shard_db(0, |db| {
            SnapshotState::new(0, 1, std::slice::from_ref(&*db), path.clone())
        });
        snapshot_cow::disarm();
        snapshot_cow::arm_with_layout(state.segment_counts().to_vec());
        assert!(!with_shard_db(0, |db| state.advance_one_segment_db(db)));
        snapshot_cow::note_progress(state.current_db_index(), state.cursor());

        // Mid-epoch MQ writes, on queues on both sides of the cursor.
        for i in 0..QUEUES {
            assert!(matches!(
                mq(&["PUSH", &q(i), "f", "v2"]),
                Frame::BulkString(_)
            ));
            let id = popped_id(&mq(&["POP", &q(i), "COUNT", "1"]));
            if i % 2 == 0 {
                assert_eq!(mq(&["ACK", &q(i), &id]), Frame::Integer(1));
            }
        }
        for i in QUEUES..QUEUES + 8 {
            mq(&["CREATE", &q(i)]);
        }
        let intents: Vec<crate::transaction::MqIntent> = (0..QUEUES)
            .step_by(3)
            .map(|i| crate::transaction::MqIntent {
                queue_key: Bytes::from(q(i)),
                fields: vec![(Bytes::from_static(b"t"), Bytes::from_static(b"x"))],
            })
            .collect();
        let pushed = with_shard_db(0, |db| materialize_mq_intents(db, 0, &intents)).len();
        assert_eq!(pushed, intents.len(), "setup: TXN intents applied");
        crate::shard::slice::with_shard(|s| {
            for i in (1..QUEUES).step_by(3) {
                crate::shard::shared_databases::apply_mq_push(
                    s,
                    0,
                    q(i).as_bytes(),
                    StreamId { ms: 1, seq: 0 },
                    vec![(Bytes::from_static(b"r"), Bytes::from_static(b"y"))],
                );
            }
        });

        // The epoch finishes.
        snapshot_cow::drain_pending_for_test(&mut state);
        while !with_shard_db(0, |db| state.advance_one_segment_db(db)) {
            snapshot_cow::note_progress(state.current_db_index(), state.cursor());
        }
        state.finalize().expect("finalize");
        snapshot_cow::disarm();

        let mut loaded = vec![Database::new()];
        shard_snapshot_load(&mut loaded, &path).expect("load");
        let mut wrong = Vec::new();
        for i in 0..QUEUES {
            let s = loaded[0]
                .get_stream(q(i).as_bytes())
                .ok()
                .flatten()
                .expect("queue in the file");
            let g = &s.groups[b"__mq_consumers".as_ref()];
            if s.length != 3 || !g.pel.is_empty() || g.last_delivered_id != StreamId::ZERO {
                wrong.push(format!(
                    "{}: length {} pel {} cursor {:?}",
                    q(i),
                    s.length,
                    g.pel.len(),
                    g.last_delivered_id
                ));
            }
        }
        let created: Vec<String> = (QUEUES..QUEUES + 8)
            .map(q)
            .filter(|k| loaded[0].get_stream(k.as_bytes()).ok().flatten().is_some())
            .collect();
        (wrong, created)
    })
    .join()
    .map(|(wrong, created)| {
        assert!(
            wrong.is_empty(),
            "{} queues reached the file at their post-epoch state: {:?}",
            wrong.len(),
            &wrong[..wrong.len().min(5)]
        );
        assert!(
            created.is_empty(),
            "queues created mid-epoch are in the file: {created:?}"
        );
    })
    .expect("test thread panicked");
}

/// Review 5 nit: POP over-claims `COUNT + MAXDELIVERY` entries. With
/// COUNT near `usize::MAX` that sum overflowed: a debug build panicked
/// the shard, a release build wrapped to a claim of MAXDELIVERY - 1
/// entries and delivered fewer than were there.
#[test]
fn pop_with_a_count_near_usize_max_delivers_everything() {
    std::thread::spawn(|| {
        init_shard(make_test_slice(1));
        assert_eq!(
            mq(&["CREATE", "q"]),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );
        for i in 0..3 {
            let v = format!("m{i}");
            assert!(matches!(mq(&["PUSH", "q", "f", &v]), Frame::BulkString(_)));
        }
        match mq(&["POP", "q", "COUNT", &usize::MAX.to_string()]) {
            Frame::Array(entries) => entries.len(),
            other => panic!("POP: {other:?}"),
        }
    })
    .join()
    .map(|delivered| assert_eq!(delivered, 3, "every queued entry"))
    .expect("the POP panicked the shard thread");
}

/// Review 5 nit: a TXN MQ.PUBLISH into a queue whose `Stream::add`
/// refuses the next id (moon#1249: the last possible ID is taken) adds
/// nothing, so it must log no MqPush either. It used to encode and
/// return the payload before trying the add.
#[test]
fn a_refused_txn_push_logs_no_mq_push() {
    use crate::shard::slice::with_shard_db;
    std::thread::spawn(|| {
        init_shard(make_test_slice(1));
        assert_eq!(
            mq(&["CREATE", "q"]),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );
        with_shard_db(0, |db| {
            db.get_stream_mut(b"q").unwrap().unwrap().last_id = StreamId::MAX;
        });
        let intents = [crate::transaction::MqIntent {
            queue_key: Bytes::from_static(b"q"),
            fields: vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
        }];
        let payloads = with_shard_db(0, |db| materialize_mq_intents(db, 0, &intents));
        let length = with_shard_db(0, |db| db.get_stream_mut(b"q").unwrap().unwrap().length);
        (payloads.len(), length)
    })
    .join()
    .map(|(payloads, length)| {
        assert_eq!(length, 0, "setup: the push was refused");
        assert_eq!(
            payloads, 0,
            "a refused push logged {payloads} MqPush record(s)"
        );
    })
    .expect("test thread panicked");
}

/// Review 5 nit: a dead letter whose DLQ refuses the next id (its last
/// possible ID is taken) was acked from the queue's PEL AND logged as
/// routed, although the DLQ never received it — the message was lost,
/// and the MqPop record said it was dead-lettered. Now it is neither
/// routed nor logged nor acked: it stays pending in the queue's PEL, and
/// the MqPop record (which lists it as claimed, with no routing) replays
/// to the same state.
#[test]
fn a_dead_letter_the_dlq_refuses_stays_pending_and_is_not_logged_as_routed() {
    use crate::persistence::wal_v3::record::WalRecordType;
    use crate::shard::slice::{with_shard, with_shard_db};
    std::thread::spawn(|| {
        init_shard(make_test_slice(1));
        let (tx, rx) = crate::runtime::channel::mpsc_bounded(64);
        with_shard(|s| s.wal_append_tx = Some(tx));
        // MAXDELIVERY 1 dead-letters the first delivery (moon#663).
        assert_eq!(
            mq(&["CREATE", "q", "MAXDELIVERY", "1"]),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );
        let Frame::BulkString(id) = mq(&["PUSH", "q", "f", "v"]) else {
            panic!("PUSH");
        };
        let id = StreamId::parse(&id, 0).expect("pushed id");
        with_shard_db(0, |db| {
            db.get_or_create_stream(b"q::mq:dlq").unwrap().last_id = StreamId::MAX;
        });
        while rx.try_recv().is_ok() {}
        let _ = mq(&["POP", "q"]);
        let mut routed = None;
        while let Ok((kind, payload)) = rx.try_recv() {
            if kind == WalRecordType::MqPop {
                let (_, _, _, claimed, dlq, _, _) =
                    crate::mq::wal::decode_mq_pop(&payload).expect("MqPop payload");
                routed = Some((claimed.len(), dlq.len()));
            }
        }
        let (pending, dlq_len) = with_shard_db(0, |db| {
            let pending = db.get_stream_mut(b"q").unwrap().unwrap().groups
                [b"__mq_consumers".as_ref()]
            .pel
            .contains_key(&id);
            let dlq_len = db.get_stream_mut(b"q::mq:dlq").unwrap().unwrap().length;
            (pending, dlq_len)
        });
        (routed, pending, dlq_len)
    })
    .join()
    .map(|(routed, pending, dlq_len)| {
        assert_eq!(dlq_len, 0, "setup: the DLQ refused the dead letter");
        assert_eq!(
            routed,
            Some((1, 0)),
            "(claimed, routed) in the MqPop record: a routing logged for a dead letter \
             the DLQ never received"
        );
        assert!(pending, "the unrouted dead letter was acked away: lost");
    })
    .expect("test thread panicked");
}

// ── moon#1250: MQ writes are charged to used_memory and gated ─────────────

/// Before moon#1250 no MQ subcommand drained the stream's unbilled delta
/// (`Stream::take_unbilled`), so 20,000 pushes of 100 B grew
/// `used_memory` by a few hundred bytes; and MQ ran no write gate, so
/// `maxmemory` could never refuse one.
#[test]
fn mq_writes_are_charged_to_used_memory_and_run_the_write_gate() {
    use crate::shard::slice::with_shard_db;
    std::thread::spawn(|| {
        init_shard(make_test_slice(1));
        assert_eq!(
            mq(&["CREATE", "q"]),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );
        let used = || with_shard_db(0, |db| db.estimated_memory());
        let before = used();
        let value = "v".repeat(100);
        let mut ids = Vec::new();
        for _ in 0..200 {
            match mq(&["PUSH", "q", "f", &value]) {
                Frame::BulkString(id) => ids.push(String::from_utf8_lossy(&id).into_owned()),
                other => panic!("PUSH: {other:?}"),
            }
        }
        let pushed = used() - before;
        assert!(
            pushed >= 200 * 100,
            "200 pushes of 100 B must be charged: used_memory +{pushed}"
        );
        // A claim adds PEL entries; the ACK credits them back.
        let _ = mq(&["POP", "q", "COUNT", "50"]);
        let claimed = used();
        let mut ack = vec!["ACK", "q"];
        ack.extend(ids.iter().take(50).map(String::as_str));
        assert_eq!(mq(&ack), Frame::Integer(50));
        assert!(used() < claimed, "an ACK must credit its PEL entries back");

        // The gate refuses a growing write, and nothing is pushed then.
        let _gate = crate::storage::eviction::force_write_gate(true);
        let oom = Frame::Error(Bytes::from_static(b"OOM test refusal"));
        let refused = execute_mq_on_owner(
            0,
            Bytes::new(),
            Arc::new(Frame::Array(
                vec![
                    Frame::BulkString(Bytes::from_static(b"MQ")),
                    Frame::BulkString(Bytes::from_static(b"PUSH")),
                    Frame::BulkString(Bytes::from_static(b"q")),
                    Frame::BulkString(Bytes::from_static(b"f")),
                    Frame::BulkString(Bytes::from_static(b"v")),
                ]
                .into(),
            )),
            &mut |_, _| Err(oom.clone()),
        );
        assert_eq!(refused, oom);
        let len = with_shard_db(0, |db| db.get_stream(b"q").ok().flatten().map(|s| s.length));
        assert_eq!(len, Some(200), "a refused PUSH adds nothing");
    })
    .join()
    .expect("test thread panicked");
}

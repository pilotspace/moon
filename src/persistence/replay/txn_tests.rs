//! moon#1300: `MOON.TXN` blocks on replay.

use bytes::Bytes;

use crate::persistence::replay::pseudo::{
    Pseudo, TXN_RECORD_MAX_LEN, TxnMarker, TxnRecord, classify,
};
use crate::persistence::replay::{CommandReplayEngine, DispatchReplayEngine, ReplayRoute};
use crate::protocol::Frame;
use crate::storage::Database;

fn bulk(b: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(b))
}

/// Replay one record given as words.
fn rec(
    engine: &DispatchReplayEngine,
    dbs: &mut [Database],
    sel: &mut usize,
    words: &[&[u8]],
) -> ReplayRoute {
    let args: Vec<Frame> = words[1..].iter().map(|w| bulk(w)).collect();
    engine.replay_command(dbs, words[0], &args, sel)
}

fn get(dbs: &mut [Database], db: usize, key: &[u8]) -> Option<Vec<u8>> {
    dbs[db]
        .get(key)
        .and_then(|e| e.value.as_bytes().map(<[u8]>::to_vec))
}

fn dbs() -> Vec<Database> {
    (0..4).map(|_| Database::new()).collect()
}

const BEGIN: &[u8] = b"BEGIN";
const PAUSE: &[u8] = b"PAUSE";
const END: &[u8] = b"END";

#[test]
fn txn_records_are_plain_resp_arrays_that_classify_back() {
    for marker in [
        TxnMarker::Begin(1),
        TxnMarker::Pause(42),
        TxnMarker::End(u64::MAX),
        TxnMarker::Reset,
    ] {
        let rec = TxnRecord::new(marker);
        let mut buf = bytes::BytesMut::from(rec.as_bytes());
        let frame =
            crate::protocol::parse::parse(&mut buf, &crate::protocol::ParseConfig::default())
                .expect("parse")
                .expect("one whole frame");
        assert!(buf.is_empty());
        let Frame::Array(arr) = frame else {
            panic!("not an array")
        };
        let Frame::BulkString(name) = &arr[0] else {
            panic!("name")
        };
        assert_eq!(classify(name, &arr[1..]), Some(Pseudo::Txn(marker)));
        assert_eq!(Pseudo::Txn(marker).route(), ReplayRoute::Marker);
    }
    assert_eq!(
        TxnRecord::new(TxnMarker::Pause(u64::MAX)).as_bytes().len(),
        TXN_RECORD_MAX_LEN
    );
}

#[test]
fn malformed_txn_records_are_skipped_markers() {
    let cases: &[&[&[u8]]] = &[
        &[],
        &[b"BEGIN"],
        &[b"BEGIN", b"0"],
        &[b"BEGIN", b"x"],
        &[b"BEGIN", b"1", b"2"],
        &[b"COMMIT", b"1"],
        &[b"RESET", b"1"],
    ];
    for args in cases {
        let args: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        assert_eq!(
            classify(b"MOON.TXN", &args),
            Some(Pseudo::MalformedTxn),
            "{args:?}"
        );
    }
    // Case-insensitive like every other record name.
    assert_eq!(
        classify(b"moon.txn", &[bulk(b"end"), bulk(b"7")]),
        Some(Pseudo::Txn(TxnMarker::End(7)))
    );
}

/// A committed block replays fully, interleaved with another client's
/// records; a malformed marker in between changes nothing.
#[test]
fn an_ended_block_replays_fully() {
    let engine = DispatchReplayEngine::new();
    let mut d = dbs();
    let mut sel = 0;
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"orig"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", BEGIN, b"5"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"txn"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", PAUSE, b"5"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"other", b"x"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", b"BOGUS", b"5"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", BEGIN, b"5"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"new", b"txn"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", END, b"5"]);
    assert_eq!(engine.finish_log(&mut d), 0);
    assert_eq!(get(&mut d, 0, b"k"), Some(b"txn".to_vec()));
    assert_eq!(get(&mut d, 0, b"new"), Some(b"txn".to_vec()));
    assert_eq!(get(&mut d, 0, b"other"), Some(b"x".to_vec()));
}

/// An unterminated block is rolled back at the end of the file: every key
/// it wrote returns to its pre-transaction state (a created key goes, a
/// deleted one comes back, across databases), and another client's records
/// inside the block stand.
#[test]
fn an_unterminated_block_is_rolled_back_at_the_end_of_the_file() {
    let engine = DispatchReplayEngine::new();
    let mut d = dbs();
    let mut sel = 0;
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"orig"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"gone", b"here"]);
    rec(&engine, &mut d, &mut sel, &[b"SELECT", b"2"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k2", b"two"]);
    rec(&engine, &mut d, &mut sel, &[b"SELECT", b"0"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", BEGIN, b"9"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"unc"]);
    rec(&engine, &mut d, &mut sel, &[b"INCR", b"ctr"]);
    rec(&engine, &mut d, &mut sel, &[b"DEL", b"gone"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", PAUSE, b"9"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"other", b"acked"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TS", b"1790000000000"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", BEGIN, b"9"]);
    rec(&engine, &mut d, &mut sel, &[b"SELECT", b"2"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k2", b"unc"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"unc-again"]);
    assert_eq!(
        get(&mut d, 0, b"k"),
        Some(b"unc".to_vec()),
        "applied live first"
    );
    assert_eq!(engine.finish_log(&mut d), 1);
    assert_eq!(get(&mut d, 0, b"k"), Some(b"orig".to_vec()));
    assert_eq!(get(&mut d, 0, b"ctr"), None);
    assert_eq!(get(&mut d, 0, b"gone"), Some(b"here".to_vec()));
    assert_eq!(get(&mut d, 2, b"k2"), Some(b"two".to_vec()));
    assert_eq!(get(&mut d, 2, b"k"), None, "a key created in db 2 goes");
    assert_eq!(get(&mut d, 0, b"other"), Some(b"acked".to_vec()));
    // The engine is idle again: the next file starts clean.
    assert_eq!(engine.finish_log(&mut d), 0);
}

/// A record outside the block that writes a key the block captured proves
/// the transaction had released it (its END was lost): a later rollback
/// must not overwrite that acknowledged write.
#[test]
fn a_write_outside_the_block_releases_its_key_from_the_rollback() {
    let engine = DispatchReplayEngine::new();
    let mut d = dbs();
    let mut sel = 0;
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"orig"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"j", b"orig"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", BEGIN, b"3"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"txn"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"j", b"txn"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", PAUSE, b"3"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"later"]);
    // A second transaction writing `j` (after 3 lost it) also releases it.
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", BEGIN, b"4"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"j", b"four"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", END, b"4"]);
    assert_eq!(engine.finish_log(&mut d), 1);
    assert_eq!(get(&mut d, 0, b"k"), Some(b"later".to_vec()));
    assert_eq!(get(&mut d, 0, b"j"), Some(b"four".to_vec()));
}

/// `MOON.TXN RESET` (a writer reopening a file a crash left inside a block)
/// rolls the dead transaction back WHERE it is, so the next session's
/// records apply on top of the rolled-back state — including a read of a
/// key the dead transaction had written.
#[test]
fn reset_rolls_back_before_the_next_sessions_records() {
    let engine = DispatchReplayEngine::new();
    let mut d = dbs();
    let mut sel = 0;
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"orig"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", BEGIN, b"1"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"unc"]);
    // crash; the next session reopens the file
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", b"RESET"]);
    rec(&engine, &mut d, &mut sel, &[b"APPEND", b"k", b"+next"]);
    // the new session's own transaction 1 (ids restart) commits
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", BEGIN, b"1"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"n", b"committed"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", END, b"1"]);
    assert_eq!(engine.finish_log(&mut d), 0);
    assert_eq!(get(&mut d, 0, b"k"), Some(b"orig+next".to_vec()));
    assert_eq!(get(&mut d, 0, b"n"), Some(b"committed".to_vec()));
}

/// A clean-close marker ends every open block: the process stopped.
#[test]
fn a_clean_close_marker_rolls_open_blocks_back() {
    let engine = DispatchReplayEngine::new();
    let mut d = dbs();
    let mut sel = 0;
    let _pin = crate::persistence::replay::clock::pin_replay_clock_ms(1_790_000_000_000);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"orig"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", BEGIN, b"2"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"unc"]);
    rec(
        &engine,
        &mut d,
        &mut sel,
        &[b"MOON.TS", b"1790000000001", b"CLOSE"],
    );
    assert_eq!(get(&mut d, 0, b"k"), Some(b"orig".to_vec()));
    rec(&engine, &mut d, &mut sel, &[b"MOON.TS", b"1790000000002"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"after"]);
    assert_eq!(engine.finish_log(&mut d), 0);
    assert_eq!(get(&mut d, 0, b"k"), Some(b"after".to_vec()));
}

/// The abort's compensation for a hash with field deadlines is `RESTORE …
/// REPLACE ABSTTL` then one `HPEXPIREAT` per deadline, inside the block. A
/// crash between them must not bring the hash back without its deadline:
/// the block is unterminated, so the rollback restores the pre-transaction
/// hash — field TTLs included — whatever prefix of the compensation landed.
#[test]
fn a_crash_between_restore_and_hpexpireat_keeps_every_field_deadline() {
    use crate::storage::compact_value::RedisValueRef;
    let deadline = crate::storage::entry::current_time_ms() + 600_000;
    let dl = deadline.to_string();
    for landed in 0..=2usize {
        let engine = DispatchReplayEngine::new();
        let mut d = dbs();
        let mut sel = 0;
        rec(
            &engine,
            &mut d,
            &mut sel,
            &[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"],
        );
        rec(
            &engine,
            &mut d,
            &mut sel,
            &[b"HPEXPIREAT", b"h", dl.as_bytes(), b"FIELDS", b"1", b"f1"],
        );
        // The pre-image the live abort would restore, as its records.
        let pre = d[0].get(b"h").cloned().expect("hash");
        let mut comp = Vec::new();
        crate::transaction::kv_compensation::push_restore_records(
            0,
            &Bytes::from_static(b"h"),
            &pre,
            &mut comp,
        );
        assert_eq!(comp.len(), 2, "RESTORE + one HPEXPIREAT");
        rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", BEGIN, b"8"]);
        rec(
            &engine,
            &mut d,
            &mut sel,
            &[b"HSET", b"h", b"f1", b"X", b"f3", b"Y"],
        );
        for (_, record) in comp.iter().take(landed) {
            let n = crate::persistence::replay::replay_resp_payload(
                &engine,
                &mut d,
                record,
                &mut sel,
                |_| {},
            );
            assert_eq!(n, 1);
        }
        // crash: no END
        assert_eq!(engine.finish_log(&mut d), 1, "landed {landed}");
        let h = d[0].get(b"h").cloned().expect("hash back");
        match h.value.as_redis_value() {
            RedisValueRef::HashWithTtl { fields, ttls, .. } => {
                assert_eq!(fields.len(), 2, "landed {landed}");
                assert_eq!(
                    ttls.get(&Bytes::from_static(b"f1")).copied(),
                    Some(deadline),
                    "landed {landed}"
                );
                assert!(!fields.contains_key(&Bytes::from_static(b"f3")));
            }
            _ => panic!("landed {landed}: the hash lost its field deadlines"),
        }
    }
}

/// Clock records inside a block are observations (decision Q6): applied
/// where they sit, never part of the rollback.
#[test]
fn a_clock_record_inside_a_rolled_back_block_still_moves_the_clock() {
    let engine = DispatchReplayEngine::new();
    let mut d = dbs();
    let mut sel = 0;
    let _pin = crate::persistence::replay::clock::pin_replay_clock_ms(1);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", BEGIN, b"6"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TS", b"1790000000777"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"k", b"v"]);
    assert_eq!(engine.finish_log(&mut d), 1);
    assert_eq!(
        crate::persistence::replay::clock::pinned_replay_clock_ms(),
        Some(1_790_000_000_777)
    );
    assert_eq!(get(&mut d, 0, b"k"), None);
}

/// Whole-database writes and two-database writes outside a block release
/// what they clear or overwrite.
#[test]
fn whole_and_two_database_writes_release_their_keys() {
    let engine = DispatchReplayEngine::new();
    let mut d = dbs();
    let mut sel = 0;
    rec(&engine, &mut d, &mut sel, &[b"SET", b"a", b"orig"]);
    rec(&engine, &mut d, &mut sel, &[b"SELECT", b"1"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"b", b"orig"]);
    rec(&engine, &mut d, &mut sel, &[b"SELECT", b"0"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", BEGIN, b"2"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"a", b"txn"]);
    rec(&engine, &mut d, &mut sel, &[b"SELECT", b"1"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"b", b"txn"]);
    rec(&engine, &mut d, &mut sel, &[b"MOON.TXN", PAUSE, b"2"]);
    rec(&engine, &mut d, &mut sel, &[b"FLUSHDB"]);
    rec(&engine, &mut d, &mut sel, &[b"SELECT", b"2"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"c", b"moved"]);
    rec(&engine, &mut d, &mut sel, &[b"MOVE", b"c", b"0"]);
    assert_eq!(engine.finish_log(&mut d), 1);
    assert_eq!(get(&mut d, 1, b"b"), None, "the FLUSHDB stands");
    assert_eq!(
        get(&mut d, 0, b"a"),
        Some(b"orig".to_vec()),
        "db 0 was not flushed"
    );
    assert_eq!(get(&mut d, 0, b"c"), Some(b"moved".to_vec()));
}

/// Feed every RESP frame of `buf` (a replicated unit) to the engine.
fn feed(engine: &DispatchReplayEngine, d: &mut [Database], sel: &mut usize, buf: &[u8]) {
    let mut buf = bytes::BytesMut::from(buf);
    let cfg = crate::protocol::ParseConfig::default();
    while let Some(frame) = crate::protocol::parse::parse(&mut buf, &cfg).expect("parse") {
        let Frame::Array(arr) = frame else {
            panic!("not an array")
        };
        let Frame::BulkString(name) = &arr[0] else {
            panic!("name")
        };
        engine.replay_command(d, name, &arr[1..], sel);
    }
    assert!(buf.is_empty());
}

fn set_record(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = b"*3\r\n$3\r\nSET\r\n".to_vec();
    for w in [key, value] {
        out.extend_from_slice(format!("${}\r\n", w.len()).as_bytes());
        out.extend_from_slice(w);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// R2b W1: a multi-shard master merges its shards' records into one
/// replication stream, and each shard's transaction manager issues the same
/// ids (1, 2, 3, …). As written by the master (`txn_log::repl_record`,
/// `aof::txn_end_record`), shard 0's END of its transaction 1 must not
/// close shard 1's transaction 1: that block is still rolled back.
#[test]
fn two_shards_same_manager_id_stay_two_blocks() {
    use crate::persistence::aof::txn_end_record;
    use crate::server::conn::txn_log::repl_record;
    let engine = DispatchReplayEngine::new();
    let mut d = dbs();
    let mut sel = 0;
    rec(&engine, &mut d, &mut sel, &[b"SET", b"a", b"origA"]);
    rec(&engine, &mut d, &mut sel, &[b"SET", b"b", b"origB"]);
    feed(
        &engine,
        &mut d,
        &mut sel,
        &repl_record(0, 1, &set_record(b"a", b"txnA")),
    );
    feed(
        &engine,
        &mut d,
        &mut sel,
        &repl_record(1, 1, &set_record(b"b", b"txnB")),
    );
    feed(&engine, &mut d, &mut sel, &txn_end_record(0, 1));
    assert_eq!(get(&mut d, 0, b"b"), Some(b"txnB".to_vec()));
    assert_eq!(
        engine.finish_log(&mut d),
        1,
        "shard 1's block is still open"
    );
    assert_eq!(get(&mut d, 0, b"a"), Some(b"txnA".to_vec()), "committed");
    assert_eq!(get(&mut d, 0, b"b"), Some(b"origB".to_vec()), "rolled back");
}

/// A record of another plane (`GRAPH.*`, `FT.*`, `MQ.*`, …) names a graph,
/// index or queue, not a KV key: outside a block it must not release a
/// transaction's capture of a KV key that happens to share the name (a
/// replica applies those planes through the same hook).
#[test]
fn a_non_kv_record_never_releases_a_kv_capture() {
    use crate::persistence::replay::txn::TxnReplay;
    let mut d = dbs();
    d[0].set(
        b"users",
        crate::storage::entry::Entry::new_string(Bytes::from_static(b"orig")),
    );
    let mut t = TxnReplay::default();
    t.on_marker(&mut d, TxnMarker::Begin(7));
    t.before_data(&mut d, 0, b"SET", &[bulk(b"users"), bulk(b"txn")]);
    d[0].set(
        b"users",
        crate::storage::entry::Entry::new_string(Bytes::from_static(b"txn")),
    );
    t.on_marker(&mut d, TxnMarker::Pause(7));
    for cmd in [
        &b"GRAPH.QUERY"[..],
        b"FT.CREATE",
        b"MQ.PUSH",
        b"TEMPORAL.SET",
        b"WS.CREATE",
    ] {
        t.before_data(&mut d, 0, cmd, &[bulk(b"users"), bulk(b"x")]);
    }
    assert_eq!(t.finish(&mut d), 1);
    assert_eq!(
        get(&mut d, 0, b"users"),
        Some(b"orig".to_vec()),
        "the cut transaction's write is rolled back despite the GRAPH/FT/MQ records"
    );
}

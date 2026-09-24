//! moon#1216 — a DashTable split during a snapshot epoch must neither lose
//! nor duplicate an epoch-start key, nor let a post-epoch value or key into
//! the file.
//!
//! The snapshot is point-in-time only if, for every key, the file holds
//! EXACTLY its epoch-start state: present with its epoch-start value if it
//! existed, absent if it did not. A split moves about half of a segment's
//! keys into a brand-new segment while the epoch is still serializing; these
//! tests interleave that structural change with segment advances, pre-image
//! captures and drains in every order the event loop can produce.

use std::collections::BTreeMap;

use bytes::Bytes;

use super::epoch_harness::{Epoch, Rng, diverge, read_records, run, string_keyspace};
use super::*;
use crate::protocol::{Frame, FrameVec};

fn preload(db: &mut Database, prefix: &str, n: u32) {
    for i in 0..n {
        db.set_string(
            format!("{prefix}:{i:06}").as_bytes(),
            Bytes::from(format!("{i}")),
        );
    }
}

/// The moon#1216 repro, verbatim: 2,000 pre-epoch keys, ONE segment
/// serialized, then 20,000 inserts split every other segment. HEAD lost
/// 1,744 of the 2,000 keys (segments 62 -> 529): the split-off halves sit at
/// segment indices past the epoch-start count, which the serializer never
/// visits.
#[test]
fn issue_1216_repro_split_of_pending_segment_keeps_pre_epoch_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("p.rrdshard");
    let mut dbs = vec![Database::new()];
    for i in 0..2000u32 {
        dbs[0].set_string(format!("pre:{i:06}").as_bytes(), Bytes::from_static(b"v"));
    }
    let mut state = SnapshotState::new(0, 1, &dbs, path.clone());
    let count0 = dbs[0].data().segment_count();
    state.advance_one_segment(&dbs); // serialize ONE segment
    for i in 0..20000u32 {
        // now split everything else
        dbs[0].set_string(format!("new:{i:06}").as_bytes(), Bytes::from_static(b"n"));
    }
    let count1 = dbs[0].data().segment_count();
    assert!(
        count1 > 4 * count0,
        "fixture must split: {count0} -> {count1}"
    );
    while !state.advance_one_segment(&dbs) {}
    state.finalize().unwrap();
    let mut loaded = vec![Database::new()];
    shard_snapshot_load(&mut loaded, &path).unwrap();
    let missing = (0..2000u32)
        .filter(|i| loaded[0].get(format!("pre:{i:06}").as_bytes()).is_none())
        .count();
    assert_eq!(
        missing, 0,
        "segments {count0} -> {count1}: pre-epoch keys missing from the snapshot"
    );
}

/// A split of a segment that is ALREADY in the file moves keys whose
/// epoch-start bytes are written; they must not be written a second time
/// (a naive "serialize every segment that exists" fix does exactly that).
/// Half the segments are written first, then every segment splits.
#[test]
fn split_of_already_serialized_segments_writes_no_key_twice() {
    let mut dbs = vec![Database::new()];
    preload(&mut dbs[0], "pre", 4000);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    let half = dbs[0].data().segment_count() / 2;
    for _ in 0..half {
        assert!(!epoch.tick_one(&dbs));
    }
    // Structural churn only (raw inserts, no capture): every segment,
    // written or pending, splits at least once.
    for i in 0..40_000u32 {
        dbs[0].set_string(format!("new:{i:06}").as_bytes(), Bytes::from_static(b"n"));
    }
    let records = epoch.finish(&dbs);
    let pre: Vec<_> = records
        .iter()
        .filter(|(_, k, _)| k.starts_with(b"pre:"))
        .cloned()
        .collect();
    let d = diverge(&expected, &pre);
    assert_eq!(d.missing, 0, "{d:?}");
    assert_eq!(d.duplicated, 0, "{d:?}");
    assert_eq!(d.wrong_value, 0, "{d:?}");
}

/// A pre-image captured while its key sat in a pending segment must still
/// be the ONE record for that key after a split moves the key into a new
/// segment — captured and drained before the split.
#[test]
fn drained_pre_image_follows_its_key_into_a_split_off_segment() {
    let mut dbs = vec![Database::new()];
    preload(&mut dbs[0], "pre", 3000);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));
    // Overwrite every epoch-start key (capture -> queue).
    for i in 0..3000u32 {
        run(&mut dbs, 0, &[b"INCR", format!("pre:{i:06}").as_bytes()]);
    }
    // Next tick drains every pre-image into the state (and writes one more
    // segment).
    assert!(!epoch.tick_one(&dbs));
    // Now split every remaining segment: moved keys take their pre-images'
    // segment with them.
    for i in 0..30_000u32 {
        dbs[0].set_string(format!("new:{i:06}").as_bytes(), Bytes::from_static(b"n"));
    }
    let records = epoch.finish(&dbs);
    let pre: Vec<_> = records
        .iter()
        .filter(|(_, k, _)| k.starts_with(b"pre:"))
        .cloned()
        .collect();
    assert_eq!(diverge(&expected, &pre), Default::default());
}

/// The same, but the split lands BETWEEN the capture and its drain: the
/// drain must still file the pre-image under a pending position.
#[test]
fn queued_pre_image_survives_a_split_before_its_drain() {
    let mut dbs = vec![Database::new()];
    preload(&mut dbs[0], "pre", 3000);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));
    for i in 0..3000u32 {
        run(&mut dbs, 0, &[b"INCR", format!("pre:{i:06}").as_bytes()]);
    }
    // No tick: the pre-images are still queued when the splits happen.
    for i in 0..30_000u32 {
        dbs[0].set_string(format!("new:{i:06}").as_bytes(), Bytes::from_static(b"n"));
    }
    let records = epoch.finish(&dbs);
    let pre: Vec<_> = records
        .iter()
        .filter(|(_, k, _)| k.starts_with(b"pre:"))
        .cloned()
        .collect();
    assert_eq!(diverge(&expected, &pre), Default::default());
}

/// A key CREATED during the epoch did not exist at its start, so the file
/// must not contain it — otherwise the WAL replays its creation on top
/// (`INCR new` → snapshot holds 1, replay makes it 2).
#[test]
fn keys_created_during_the_epoch_are_absent_from_the_snapshot() {
    let mut dbs = vec![Database::new()];
    preload(&mut dbs[0], "pre", 2000);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));
    for i in 0..2000u32 {
        run(&mut dbs, 0, &[b"INCR", format!("new:{i:06}").as_bytes()]);
    }
    let records = epoch.finish(&dbs);
    assert_eq!(diverge(&expected, &records), Default::default());
}

/// One randomized interleaving: structural change (insert bursts that split
/// pending AND written segments, directory doublings), overwrites, deletes,
/// re-creates and INCRs — some through the routed arm's `cow_intercept` +
/// dispatch, most through dispatch alone — against segment advances and
/// drains, across three databases. The file must equal the epoch-start
/// keyspace exactly.
/// A key: mostly an epoch-start one, sometimes a post-epoch one.
fn pick(rng: &mut Rng, names: &[Vec<u8>], fresh: u64, db: usize) -> Vec<u8> {
    if !names.is_empty() && rng.below(4) != 0 {
        names[rng.below(names.len() as u64) as usize].clone()
    } else {
        format!("n{db}:{:06}", rng.below(fresh + 1)).into_bytes()
    }
}

/// What one randomized epoch did, so the test can prove it exercised the
/// structural change it claims to (a green run over a fixture that never
/// split would be vacuous).
#[derive(Default)]
struct Coverage {
    /// Splits while at least one segment was already in the file.
    splits_after_first_write: u64,
    /// Pre-images drained into the state (keys written while pending).
    captured_writes: usize,
}

fn run_randomized_epoch(seed: u64, cov: &mut Coverage) -> super::epoch_harness::Divergence {
    let mut rng = Rng(seed);
    let mut dbs = vec![Database::new(), Database::new(), Database::new()];
    preload(&mut dbs[0], "a", 2500);
    preload(&mut dbs[1], "b", 700);
    // db 2 starts empty: everything written there is post-epoch.
    let expected = string_keyspace(&dbs);
    let names: Vec<Vec<Vec<u8>>> = vec![
        (0..2500)
            .map(|i| format!("a:{i:06}").into_bytes())
            .collect(),
        (0..700).map(|i| format!("b:{i:06}").into_bytes()).collect(),
        Vec::new(),
    ];
    let mut fresh = 0u64;
    let mut epoch = Epoch::begin(&dbs);
    let mut done = false;
    for _ in 0..6000 {
        let db = rng.below(3) as usize;
        let op = rng.below(100);
        let key = pick(&mut rng, &names[db], fresh, db);
        let parts: Vec<Vec<u8>> = match op {
            0..=24 => vec![b"INCR".to_vec(), key],
            25..=39 => vec![b"SET".to_vec(), key, b"x".to_vec()],
            40..=54 => vec![b"DEL".to_vec(), key],
            55..=60 => vec![b"APPEND".to_vec(), key, b"+".to_vec()],
            // moon#1217: multi-key writes, the second key usually elsewhere
            // in hash space (and often in another segment's range).
            61..=64 => {
                let other = pick(&mut rng, &names[db], fresh, db);
                match rng.below(3) {
                    0 => vec![b"MSET".to_vec(), key, b"m".to_vec(), other, b"m".to_vec()],
                    1 => vec![b"DEL".to_vec(), key, other],
                    _ => vec![b"RENAME".to_vec(), key, other],
                }
            }
            65..=74 => {
                // An insert burst: splits whatever segments these land in.
                let n = 1 + rng.below(400);
                let splits_before = dbs[db].data().split_count();
                for _ in 0..n {
                    fresh += 1;
                    let k = format!("n{db}:{fresh:06}");
                    run(&mut dbs, db, &[b"SET", k.as_bytes(), b"n"]);
                }
                if epoch
                    .state
                    .as_ref()
                    .is_some_and(|s| s.segments_written() > 0)
                {
                    cov.splits_after_first_write += dbs[db].data().split_count() - splits_before;
                }
                continue;
            }
            _ => {
                if !done {
                    for _ in 0..=rng.below(3) {
                        if let Some(state) = epoch.state.as_ref() {
                            cov.captured_writes =
                                cov.captured_writes.max(state.pending_pre_images());
                        }
                        // The production tick (a budget of segments) or a
                        // single segment, so the walk stops everywhere.
                        done = if rng.below(3) == 0 {
                            epoch.tick(&dbs)
                        } else {
                            epoch.tick_one(&dbs)
                        };
                    }
                }
                continue;
            }
        };
        let argv: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
        if rng.below(4) == 0 {
            // The routed SPSC arm: `cow_intercept` first, then dispatch.
            let frame = Frame::Array(FrameVec::from_vec(
                argv.iter()
                    .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
                    .collect(),
            ));
            crate::shard::spsc_handler::cow_intercept(&mut epoch.state, &dbs[db], db, &frame);
        }
        run(&mut dbs, db, &argv);
    }
    let records = epoch.finish(&dbs);
    diverge(&expected, &records)
}

#[test]
fn randomized_splits_during_an_epoch_keep_the_file_point_in_time() {
    let mut failures = BTreeMap::new();
    let mut vacuous = Vec::new();
    for seed in 0..12u64 {
        let mut cov = Coverage::default();
        let d = run_randomized_epoch(0x1216_0000 + seed, &mut cov);
        if d != Default::default() {
            failures.insert(seed, d);
        }
        if cov.splits_after_first_write == 0 || cov.captured_writes == 0 {
            vacuous.push((seed, cov.splits_after_first_write, cov.captured_writes));
        }
    }
    assert!(failures.is_empty(), "seeds diverged: {failures:#?}");
    assert!(
        vacuous.is_empty(),
        "seeds that never split a segment after the first write, or never had \
         a pending pre-image (seed, splits, pre-images): {vacuous:?}"
    );
}

/// Sanity for the harness itself: with no writes at all, the file is the
/// keyspace, record for record — so a green run above is not vacuous.
#[test]
fn harness_reads_back_an_untouched_epoch_exactly() {
    let mut dbs = vec![Database::new(), Database::new()];
    preload(&mut dbs[0], "a", 1500);
    preload(&mut dbs[1], "b", 10);
    let expected = string_keyspace(&dbs);
    let records = Epoch::begin(&dbs).finish(&dbs);
    assert_eq!(records.len(), 1510);
    assert_eq!(diverge(&expected, &records), Default::default());
    // And the harness's reader agrees with the real loader.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.rrdshard");
    shard_snapshot_save(0, 1, &dbs, &path).unwrap();
    assert_eq!(read_records(&path).len(), 1510);
}

/// One production tick serializes a BOUNDED batch: at most
/// `TICK_SEGMENT_BUDGET` segments, and it stops once `TICK_ENTRY_BUDGET`
/// entries are written (so at most one segment's worth past it).
#[test]
fn a_tick_writes_a_bounded_batch() {
    let mut dbs = vec![Database::new()];
    preload(&mut dbs[0], "a", 40_000);
    let segs = dbs[0].data().segment_count();
    let mut epoch = Epoch::begin(&dbs);
    let mut ticks = 0usize;
    loop {
        let (s0, e0) = {
            let st = epoch.state.as_ref().unwrap();
            (st.segments_written(), st.entries_written())
        };
        let done = epoch.tick(&dbs);
        ticks += 1;
        let st = epoch.state.as_ref().unwrap();
        let (ds, de) = (st.segments_written() - s0, st.entries_written() - e0);
        assert!(
            ds as u32 <= TICK_SEGMENT_BUDGET,
            "{ds} segments in one tick"
        );
        assert!(
            de <= u64::from(TICK_ENTRY_BUDGET) + 64,
            "{de} entries in one tick"
        );
        if done {
            break;
        }
    }
    assert!(
        ticks * 8 < segs,
        "{ticks} ticks for {segs} segments: the budget must batch segments"
    );
    let records = epoch.finish(&dbs);
    assert_eq!(records.len(), 40_000);
}

/// moon#1227 review F3: a tick is bounded in BYTES too. With 64 KiB values
/// the entry budget alone let 1,024 of them through in one tick — 65 MiB, a
/// 212 ms tick in a debug build — and a tick could overshoot the stream's
/// in-flight cap by as much. A tick now stops once it has produced
/// `TICK_BYTE_BUDGET`, or as soon as the writer is backlogged. A segment is
/// the walk's unit of progress (the cursor moves past a whole block), so a
/// tick ends at most ONE segment past the budget: every slot of it holding a
/// 64 KiB value. The walk still makes progress every tick the writer can
/// take more, and finishes.
#[test]
fn a_tick_is_bounded_in_bytes_when_values_are_large() {
    const VALUE: usize = 64 * 1024;
    const KEYS: u64 = 1_100;
    let mut dbs = vec![Database::new()];
    let value = Bytes::from(vec![b'v'; VALUE]);
    for i in 0..KEYS {
        dbs[0].set_string(format!("big:{i:06}").as_bytes(), value.clone());
    }
    // A record: type tag, key and value lengths, TTL, the key — far below
    // 256 bytes on top of the value.
    let one_segment = (crate::storage::dashtable::segment::TOTAL_SLOTS * (VALUE + 256)) as u64;
    let bound = TICK_BYTE_BUDGET + one_segment;
    let mut epoch = Epoch::begin(&dbs);
    // Streamed, as every event-loop snapshot is: the file goes to the writer
    // thread instead of piling up in memory, and the backlog check is live.
    let state = epoch.state.as_mut().expect("epoch");
    state.start_streaming().expect("writer thread");
    let cap = (crate::persistence::snapshot_stream::SNAPSHOT_STREAM_MAX_IN_FLIGHT as u64) + bound;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut max_tick = 0u64;
    loop {
        let before = epoch.state.as_ref().map_or(0, |s| s.bytes_serialized());
        let done = epoch.tick(&dbs);
        let st = epoch.state.as_ref().expect("epoch");
        let tick_bytes = st.bytes_serialized() - before;
        max_tick = max_tick.max(tick_bytes);
        assert!(
            tick_bytes <= bound,
            "{tick_bytes} bytes in one tick; the bound is {bound} \
             (budget {TICK_BYTE_BUDGET} + one full segment)"
        );
        assert!(
            (st.stream_in_flight() as u64) <= cap,
            "{} bytes in flight after a tick; the cap is {cap}",
            st.stream_in_flight()
        );
        if done {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the walk never finished"
        );
    }
    let st = epoch.state.as_ref().expect("epoch");
    assert_eq!(
        st.entries_written(),
        KEYS,
        "every entry written exactly once"
    );
    assert!(max_tick > 0);
    eprintln!("largest tick: {max_tick} bytes (bound {bound})");
    // Dropping the epoch abandons the stream: its writer is joined and its
    // temp file removed (moon#1227 review F1).
    crate::persistence::snapshot_cow::disarm();
}

/// A sustained insert flood keeps SPLITTING the pending segments. The walk
/// must still converge: the production tick outpaces the table's growth.
/// One segment per tick does not — that pacing never finished under the
/// moon#1216 evidence load (11.7 GB, 60 s stall) once the walk had to visit
/// the split-off halves it used to skip.
#[test]
fn the_walk_converges_under_a_sustained_insert_flood() {
    let run = |budgeted: bool| -> Option<usize> {
        let mut dbs = vec![Database::new()];
        preload(&mut dbs[0], "a", 20_000);
        let mut epoch = Epoch::begin(&dbs);
        let mut fresh = 0u32;
        for tick in 0..1_000usize {
            // ~300 inserts per 1 ms tick (300K/s): ~7 new segments a tick.
            for _ in 0..300 {
                fresh += 1;
                let k = format!("n:{fresh:08}");
                run(&mut dbs, 0, &[b"SET", k.as_bytes(), b"n"]);
            }
            let done = if budgeted {
                epoch.tick(&dbs)
            } else {
                epoch.tick_one(&dbs)
            };
            if done {
                let records = epoch.finish(&dbs);
                assert_eq!(records.len(), 20_000, "exactly the epoch-start keys");
                return Some(tick + 1);
            }
        }
        crate::persistence::snapshot_cow::disarm();
        None
    };
    let budgeted = run(true);
    assert!(
        budgeted.is_some_and(|t| t < 200),
        "the production tick must converge, took {budgeted:?} ticks"
    );
    assert_eq!(
        run(false),
        None,
        "control: one segment per tick falls behind the splits (if this ever \
         converges, the flood is too small to mean anything)"
    );
}

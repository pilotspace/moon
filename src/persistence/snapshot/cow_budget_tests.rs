//! moon#1228 — epoch liveness under a write flood, and the visibility of the
//! memory an epoch holds for its own sake.
//!
//! The per-tick budget of `advance_budgeted_db` used to be constant. An
//! insert flood into the ranges the walk has not written yet splits them
//! faster than a constant number of segments per tick can visit, so the
//! epoch never converged — and every such insert left a tombstone pre-image
//! behind, invisible to `used_memory` and to INFO.

use bytes::Bytes;

use super::epoch_harness::{Epoch, run};
use super::*;
use crate::persistence::snapshot_cow;

fn preload(db: &mut Database, n: u32) {
    for i in 0..n {
        db.set_string(format!("pre:{i:07}").as_bytes(), Bytes::from_static(b"p"));
    }
}

/// Outcome of one flood run: ticks to converge (`None` = gave up) and the
/// most pre-images the epoch held at once.
struct Flood {
    ticks: Option<usize>,
    peak_pre_images: usize,
}

/// Keys in the database when the epoch starts.
const START_KEYS: u32 = 50_000;

/// `per_tick` fresh keys inserted through `command::dispatch` before every
/// production tick, for at most `max_ticks` ticks.
fn flood(per_tick: u32, max_ticks: usize, fixed_budget: bool) -> Flood {
    let mut dbs = vec![Database::new()];
    preload(&mut dbs[0], START_KEYS);
    let mut epoch = Epoch::begin(&dbs);
    if fixed_budget {
        epoch.state.as_mut().expect("epoch").pin_fixed_budget();
    }
    let mut fresh = 0u32;
    let mut peak = 0usize;
    for tick in 0..max_ticks {
        for _ in 0..per_tick {
            fresh += 1;
            let k = format!("n:{fresh:09}");
            run(&mut dbs, 0, &[b"SET", k.as_bytes(), b"n"]);
        }
        let done = epoch.tick(&dbs);
        if let Some(st) = epoch.state.as_ref() {
            peak = peak.max(st.pending_pre_images());
        }
        if done {
            let records = epoch.finish(&dbs);
            assert_eq!(
                records.len(),
                START_KEYS as usize,
                "exactly the epoch-start keys"
            );
            return Flood {
                ticks: Some(tick + 1),
                peak_pre_images: peak,
            };
        }
    }
    snapshot_cow::disarm();
    Flood {
        ticks: None,
        peak_pre_images: peak,
    }
}

/// An insert flood into the ranges the walk has not written yet. Every such
/// insert splits pending segments the walk must still visit and leaves a
/// tombstone behind until its range is written. With a CONSTANT budget the
/// walk's pace in hash space shrinks as the table grows (a fixed number of
/// ever-smaller segments per tick), so the epoch drags on and the
/// tombstones pile up for as long as the flood lasts; the budget scaled by
/// the pre-image backlog keeps up. Deterministic: same keys, same hashes,
/// same walk.
///
/// 5,000 inserts per 1 ms tick is 5M/s — a pipelined `SET` load on one shard
/// of a fast host. The constant-budget arm is the pre-moon#1228 walk.
#[test]
fn the_walk_keeps_up_with_an_insert_flood_the_constant_budget_cannot() {
    const PER_TICK: u32 = 5_000;
    const MAX_TICKS: usize = 400;
    let scaled = flood(PER_TICK, MAX_TICKS, false);
    let fixed = flood(PER_TICK, MAX_TICKS, true);
    eprintln!(
        "scaled budget: {:?} ticks, peak {} pre-images; constant budget: {:?} ticks, peak {}",
        scaled.ticks, scaled.peak_pre_images, fixed.ticks, fixed.peak_pre_images
    );
    let scaled_ticks = scaled.ticks.expect("the scaled budget must converge");
    let fixed_ticks = fixed.ticks.unwrap_or(MAX_TICKS);
    assert!(
        scaled_ticks * 4 <= fixed_ticks,
        "the scaled budget must converge at least 4x sooner than the constant one: \
         {scaled_ticks} vs {fixed_ticks} ticks"
    );
    assert!(
        scaled.peak_pre_images * 4 <= fixed.peak_pre_images,
        "and hold at most a quarter of its tombstones: {} vs {}",
        scaled.peak_pre_images,
        fixed.peak_pre_images
    );
}

/// The scale is 1 with no backlog (the old per-tick cost) and capped.
#[test]
fn the_budget_scale_tracks_the_backlog_and_is_capped() {
    assert_eq!(tick_budget_scale(0), 1);
    assert_eq!(tick_budget_scale(TICK_ENTRY_BUDGET as usize - 1), 1);
    assert_eq!(tick_budget_scale(TICK_ENTRY_BUDGET as usize), 2);
    assert_eq!(tick_budget_scale(usize::MAX), MAX_TICK_BUDGET_SCALE);
}

/// INFO `current_cow_size`: what an armed epoch holds for its own sake —
/// pre-images and tombstones, a table a FLUSHDB handed over — is published at
/// every drain, and returns to 0 when the save ends.
#[test]
fn the_cow_bytes_an_epoch_holds_are_published_and_released() {
    let mut dbs: Vec<Database> = (0..2).map(|_| Database::new()).collect();
    preload(&mut dbs[0], 2_000);
    for i in 0..500u32 {
        dbs[1].set_string(format!("b:{i}").as_bytes(), Bytes::from(vec![b'x'; 200]));
    }
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));
    assert_eq!(
        snapshot_cow::published_cow_size_for_test(),
        0,
        "nothing held yet"
    );

    // Writes into db 1 (not written yet): 100 overwrites and 100 inserts.
    for i in 0..100u32 {
        run(&mut dbs, 1, &[b"SET", format!("b:{i}").as_bytes(), b"new"]);
        run(
            &mut dbs,
            1,
            &[b"SET", format!("fresh:{i}").as_bytes(), b"new"],
        );
    }
    assert!(!epoch.tick_one(&dbs));
    let with_pre_images = snapshot_cow::published_cow_size_for_test();
    let state = epoch.state.as_ref().expect("epoch");
    assert_eq!(state.pending_pre_images(), 200);
    assert!(
        with_pre_images >= 100 * 200,
        "100 captured 200-byte values and 100 tombstones must show: {with_pre_images}"
    );

    // FLUSHDB of db 1 hands its table to the epoch: that shows too.
    let held = dbs[1].estimated_memory() as u64;
    run(&mut dbs, 1, &[b"FLUSHDB"]);
    assert!(!epoch.tick_one(&dbs));
    let with_table = snapshot_cow::published_cow_size_for_test();
    assert!(
        with_table >= with_pre_images + held / 2,
        "the frozen table ({held} bytes) must show: {with_pre_images} -> {with_table}"
    );

    let _ = epoch.finish(&dbs);
    assert_eq!(
        snapshot_cow::published_cow_size_for_test(),
        0,
        "a finished save holds nothing"
    );
}

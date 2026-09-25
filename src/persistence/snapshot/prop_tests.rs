//! moon#1228 review 6 (the reviewer's property test, adopted): the published
//! file is EXACTLY the epoch-start image, whatever a random workload does to
//! the keyspace while the save runs — and the FLUSHDB freeze stays within the
//! epoch-start bills of the databases not yet written.
//!
//! Each seed: `N_DBS` databases seeded with mixed types (strings, strings with
//! a far TTL, small and >64-field hashes, lists, sets with a far TTL, zsets),
//! some databases empty. The epoch-start image is dumped canonically (type,
//! absolute expiry, sorted content). The epoch is armed and a random mix of
//! SET / SET PXAT / DEL / UNLINK (lazy-free big hashes) / INCR / HSET / RPUSH
//! / SADD / ZADD / PEXPIREAT (future, near, PAST = delete) / PERSIST / RENAME
//! / MOVE / COPY DB / SWAPDB / FLUSHDB [ASYNC] / grow-then-FLUSHDB runs,
//! interleaved with held ticks (drain only), one-segment ticks and budgeted
//! ticks — FLUSHDB is biased onto the database in progress (the cursor case).
//! After every drain, each frozen table whose (budgeted, review 6) trim is
//! done holds at most its database's epoch-start bill, and together they
//! stay within the epoch-start bills of the unwritten databases. At the end
//! the published file must be EXACTLY the epoch-start image (no missing /
//! extra / duplicated key, no wrong value, type or expiry), and file +
//! replayed tail the live keyspace.
//!
//! 16 seeds by default (~6 s in a debug build; 64 take ~25 s);
//! `MOON_TEST_SNAPSHOT_PROP_SEEDS=<n>` and
//! `MOON_TEST_SNAPSHOT_PROP_SEED_START=<s>` run others (the reviewer ran
//! 2,000). Mutation evidence (NOTES): removing the pre-image restore from the
//! trim makes seed 1 red; applying the table events before the captures in
//! the drain makes the bound check red.

use std::collections::BTreeMap;

use bytes::Bytes;

use super::epoch_harness::{Epoch, Record, Rng, run};
use super::*;
use crate::persistence::snapshot_cow;
use crate::protocol::Frame;
use crate::storage::db::entry_overhead;

const N_DBS: usize = 4;
/// A TTL that never runs out during a test.
const FAR_MS: u64 = 1_000_000_000;

type Tail = Vec<(usize, Vec<Vec<u8>>)>;
type Image = BTreeMap<(usize, Vec<u8>), String>;

fn bulk(f: &Frame) -> Vec<u8> {
    match f {
        Frame::BulkString(b) | Frame::SimpleString(b) => b.to_vec(),
        Frame::Integer(i) => i.to_string().into_bytes(),
        Frame::Null => b"<nil>".to_vec(),
        other => format!("{other:?}").into_bytes(),
    }
}

fn items(f: &Frame) -> Vec<Vec<u8>> {
    match f {
        Frame::Array(v) => v.iter().map(bulk).collect(),
        other => vec![bulk(other)],
    }
}

/// Canonical image of `dbs`: `(db, key) -> "type|exp=<abs ms>|<sorted content>"`.
fn dump(dbs: &mut [Database], now: u64) -> Image {
    let mut out = Image::new();
    for i in 0..dbs.len() {
        let keys: Vec<(Vec<u8>, u64)> = dbs[i]
            .data()
            .iter()
            .filter(|(_, e)| !(e.has_expiry() && e.is_expired_at(now)))
            .map(|(k, e)| {
                let exp = if e.has_expiry() { e.expires_at_ms() } else { 0 };
                (k.as_bytes().to_vec(), exp)
            })
            .collect();
        for (k, exp) in keys {
            let ty = bulk(&run(dbs, i, &[b"TYPE", &k]));
            let body: Vec<Vec<u8>> = match ty.as_slice() {
                b"string" => vec![bulk(&run(dbs, i, &[b"GET", &k]))],
                b"hash" => {
                    let flat = items(&run(dbs, i, &[b"HGETALL", &k]));
                    let mut pairs: Vec<Vec<u8>> = flat
                        .chunks(2)
                        .map(|c| [c[0].as_slice(), b"=", c.get(1).map_or(&[][..], |v| v)].concat())
                        .collect();
                    pairs.sort();
                    pairs
                }
                b"list" => items(&run(dbs, i, &[b"LRANGE", &k, b"0", b"-1"])),
                b"set" => {
                    let mut m = items(&run(dbs, i, &[b"SMEMBERS", &k]));
                    m.sort();
                    m
                }
                b"zset" => items(&run(dbs, i, &[b"ZRANGE", &k, b"0", b"-1", b"WITHSCORES"])),
                _ => vec![b"?".to_vec()],
            };
            let body: Vec<String> = body
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect();
            out.insert(
                (i, k),
                format!("{}|exp={exp}|{body:?}", String::from_utf8_lossy(&ty)),
            );
        }
    }
    out
}

fn two(dbs: &mut [Database], a: usize, b: usize) -> (&mut Database, &mut Database) {
    assert_ne!(a, b);
    if a < b {
        let (l, r) = dbs.split_at_mut(b);
        (&mut l[a], &mut r[0])
    } else {
        let (l, r) = dbs.split_at_mut(a);
        (&mut r[0], &mut l[b])
    }
}

fn num(b: &[u8]) -> usize {
    std::str::from_utf8(b).unwrap().parse().unwrap()
}

/// Apply one command as the live paths do (SWAPDB exchanges slots and tells
/// the epoch, MOVE / COPY … DB n run their two-database cores).
fn apply(dbs: &mut [Database], db: usize, parts: &[&[u8]]) -> Frame {
    let cmd = parts[0];
    if cmd.eq_ignore_ascii_case(b"SWAPDB") {
        let (a, b) = (num(parts[1]), num(parts[2]));
        snapshot_cow::note_swapdb(a, b);
        dbs.swap(a, b);
        return Frame::SimpleString(Bytes::from_static(b"OK"));
    }
    if cmd.eq_ignore_ascii_case(b"MOVE") {
        let dst = num(parts[2]);
        if dst == db {
            return Frame::Integer(0);
        }
        let (s, d) = two(dbs, db, dst);
        return crate::command::keyspace::move_cmd::move_core(s, db, d, dst, parts[1]);
    }
    if cmd.eq_ignore_ascii_case(b"COPY") {
        let dst = num(parts[4]);
        if dst == db {
            return Frame::Integer(0);
        }
        let (s, d) = two(dbs, db, dst);
        return crate::command::keyspace::move_cmd::copy_core(
            s, db, d, dst, parts[1], parts[2], true,
        );
    }
    run(dbs, db, parts)
}

fn live(dbs: &mut [Database], tail: &mut Tail, db: usize, parts: &[&[u8]]) {
    let _ = apply(dbs, db, parts);
    tail.push((db, parts.iter().map(|p| p.to_vec()).collect()));
}

fn seed_dbs(rng: &mut Rng, now: u64) -> (Vec<Database>, Vec<u64>) {
    let mut dbs: Vec<Database> = (0..N_DBS).map(|_| Database::new()).collect();
    let mut seeded = vec![0u64; N_DBS];
    let far = (now + FAR_MS).to_string();
    for d in 0..N_DBS {
        let n = [0u64, 60, 400, 1500, 3000][rng.below(5) as usize];
        seeded[d] = n;
        for i in 0..n {
            let k = format!("s{d}:{i:05}");
            let k = k.as_bytes();
            let v = format!("v{d}.{i}");
            let v = v.as_bytes();
            match i % 7 {
                0 | 1 => {
                    run(&mut dbs, d, &[b"SET", k, v]);
                }
                2 => {
                    run(&mut dbs, d, &[b"SET", k, v, b"PXAT", far.as_bytes()]);
                }
                3 => {
                    if i % 70 == 3 {
                        // > LAZY_FREE_THRESHOLD fields: an UNLINK lazy-frees it.
                        for f in 0..100 {
                            let f = format!("f{f}");
                            run(&mut dbs, d, &[b"HSET", k, f.as_bytes(), v]);
                        }
                    } else {
                        run(&mut dbs, d, &[b"HSET", k, b"f1", v, b"f2", b"x"]);
                    }
                }
                4 => {
                    run(&mut dbs, d, &[b"RPUSH", k, b"a", v, b"c"]);
                }
                5 => {
                    run(&mut dbs, d, &[b"SADD", k, b"m1", v]);
                    run(&mut dbs, d, &[b"PEXPIREAT", k, far.as_bytes()]);
                }
                _ => {
                    run(&mut dbs, d, &[b"ZADD", k, b"1", b"a", b"2", v]);
                }
            }
        }
    }
    (dbs, seeded)
}

#[derive(Default, Debug)]
struct Coverage {
    seeds: u64,
    ops: u64,
    flushes_frozen: u64,
    flush_of_db_in_progress_mid_walk: u64,
    flush_after_swap: u64,
    grown_flushes_over_2x: u64,
    grown_flush_of_db_in_progress_mid_walk: u64,
    rebuilds_seen: u64,
    epoch_start_keys_deleted: u64,
    moves: u64,
    swaps: u64,
    max_bill_error: i64,
    bill_checks: u64,
    /// Frozen tables seen after a drain with their trim still running
    /// (budgeted across drains since review 6; the bound holds once done).
    trims_in_progress: u64,
    /// Seeds run with a tiny trim budget (1-40 row operations per drain).
    tiny_budget_seeds: u64,
    /// Ticks at which the walk's database was being rebuilt (it waits).
    walk_waits_for_rebuild: u64,
    max_frozen_over_unwritten_ratio_pct: u64,
}

/// The slot that holds epoch database `logical`'s table now, if any.
fn slot_of(logical: usize) -> Option<usize> {
    snapshot_cow::logical_of_slot_for_test()
        .iter()
        .position(|l| *l == Some(logical))
}

fn check_after_drain(seed: u64, state: &SnapshotState, start_bills: &[u64], cov: &mut Coverage) {
    let cur = state.current_db_index();
    let unwritten: u64 = start_bills.iter().skip(cur).sum();
    let mut frozen_bill = 0u64;
    let mut frozen_actual = 0u64;
    for (db, source) in state.sources.iter().enumerate() {
        if let Source::Frozen(frozen) = source {
            assert!(
                db >= cur,
                "seed {seed}: db {db} frozen behind the walk ({cur})"
            );
            if !frozen.trimmed() {
                // The byte bound holds once the (budgeted) trim is done.
                cov.trims_in_progress += 1;
                continue;
            }
            let bill = &frozen.bill;
            let actual: u64 = frozen
                .table
                .iter()
                .map(|(k, e)| entry_overhead(k.as_bytes(), e) as u64)
                .sum();
            let err = *bill as i64 - actual as i64;
            if err.abs() > cov.max_bill_error.abs() {
                cov.max_bill_error = err;
            }
            cov.bill_checks += 1;
            assert!(
                actual <= start_bills[db],
                "seed {seed}: db {db}'s frozen rows are {actual} B; its epoch-start bill {} B",
                start_bills[db]
            );
            frozen_bill += *bill;
            frozen_actual += actual;
        }
    }
    assert!(
        frozen_actual <= unwritten,
        "seed {seed}: frozen rows {frozen_actual} B > epoch-start bills of unwritten dbs {unwritten} B"
    );
    if unwritten > 0 {
        cov.max_frozen_over_unwritten_ratio_pct = cov
            .max_frozen_over_unwritten_ratio_pct
            .max(frozen_bill * 100 / unwritten);
    }
}

fn one_seed(seed: u64, cov: &mut Coverage) {
    let mut rng = Rng(seed);
    // Review 6 (S1): half the seeds spread every trim over many drains,
    // interleaved with the walk writing the same database.
    let tiny = (seed % 2 == 0).then(|| 1 + rng.below(40) as usize);
    crate::persistence::snapshot::frozen::set_trim_budget_for_test(tiny);
    if tiny.is_some() {
        cov.tiny_budget_seeds += 1;
    }
    let now = current_time_ms();
    let (mut dbs, seeded) = seed_dbs(&mut rng, now);
    let expected = dump(&mut dbs, now);
    let start_bills: Vec<u64> = dbs.iter().map(|d| d.ledger_bytes() as u64).collect();
    let start_segments: Vec<usize> = dbs.iter().map(|d| d.data().segment_count()).collect();
    let epoch_start_keys: std::collections::BTreeSet<(usize, Vec<u8>)> =
        expected.keys().cloned().collect();
    let mut epoch = Epoch::begin(&dbs);
    let mut tail = Tail::new();
    let mut ticks = 0u32;
    let mut swapped_since_arm = [false; N_DBS];
    let heavy = rng.below(2) == 0;
    loop {
        for _ in 0..1 + rng.below(12) {
            cov.ops += 1;
            let mut db = rng.below(N_DBS as u64) as usize;
            let pick = rng.below(10);
            let owner = snapshot_cow::logical_of_slot_for_test()
                .get(db)
                .copied()
                .flatten();
            let k = if pick < 6
                && let Some(l) = owner
                && seeded[l] > 0
            {
                format!("s{l}:{:05}", rng.below(seeded[l]))
            } else if pick < 8 {
                format!("s{}:{:05}", rng.below(N_DBS as u64), rng.below(3100))
            } else {
                format!("n:{}", rng.below(400))
            };
            let kb = k.as_bytes();
            let at = |off: i64| ((now as i64) + off).to_string();
            let (cur, cursor) = {
                let s = epoch.state.as_ref().expect("epoch");
                (s.current_db_index(), s.cursor())
            };
            let r = rng.below(if heavy { 40 } else { 240 });
            let r = if r >= 40 { r % 27 } else { r };
            match r {
                0..=5 => {
                    let v = format!("w{}", rng.below(1000));
                    live(&mut dbs, &mut tail, db, &[b"SET", kb, v.as_bytes()]);
                }
                6 => {
                    let t = at(FAR_MS as i64 / 2);
                    live(
                        &mut dbs,
                        &mut tail,
                        db,
                        &[b"SET", kb, b"px", b"PXAT", t.as_bytes()],
                    );
                }
                7..=9 => {
                    if epoch_start_keys.contains(&(db, kb.to_vec())) {
                        cov.epoch_start_keys_deleted += 1;
                    }
                    live(&mut dbs, &mut tail, db, &[b"DEL", kb]);
                }
                10 => live(&mut dbs, &mut tail, db, &[b"UNLINK", kb]),
                11 => live(&mut dbs, &mut tail, db, &[b"INCR", kb]),
                12 => live(&mut dbs, &mut tail, db, &[b"HSET", kb, b"f1", b"h"]),
                13 => live(&mut dbs, &mut tail, db, &[b"RPUSH", kb, b"z"]),
                14 => live(&mut dbs, &mut tail, db, &[b"SADD", kb, b"m9"]),
                15 => live(&mut dbs, &mut tail, db, &[b"ZADD", kb, b"5", b"q"]),
                16 => {
                    let t = at(rng.below(20) as i64 + 1);
                    live(&mut dbs, &mut tail, db, &[b"PEXPIREAT", kb, t.as_bytes()]);
                }
                17 => {
                    let t = at(-1000);
                    live(&mut dbs, &mut tail, db, &[b"PEXPIREAT", kb, t.as_bytes()]);
                }
                18 => live(&mut dbs, &mut tail, db, &[b"PERSIST", kb]),
                19 => {
                    let k2 = format!("r:{}", rng.below(200));
                    live(&mut dbs, &mut tail, db, &[b"RENAME", kb, k2.as_bytes()]);
                }
                20..=22 => {
                    cov.moves += 1;
                    let dst = rng.below(N_DBS as u64).to_string();
                    live(&mut dbs, &mut tail, db, &[b"MOVE", kb, dst.as_bytes()]);
                }
                23 => {
                    let dst = rng.below(N_DBS as u64).to_string();
                    live(
                        &mut dbs,
                        &mut tail,
                        db,
                        &[b"COPY", kb, kb, b"DB", dst.as_bytes()],
                    );
                }
                24..=26 => {
                    cov.swaps += 1;
                    let a = rng.below(N_DBS as u64) as usize;
                    let b = rng.below(N_DBS as u64) as usize;
                    if a != b {
                        swapped_since_arm[a] = true;
                        swapped_since_arm[b] = true;
                    }
                    let (a, b) = (a.to_string(), b.to_string());
                    live(
                        &mut dbs,
                        &mut tail,
                        db,
                        &[b"SWAPDB", a.as_bytes(), b.as_bytes()],
                    );
                }
                27..=33 => {
                    // FLUSHDB; half of them aimed at the database in progress.
                    if rng.below(4) != 0
                        && let Some(slot) = slot_of(cur)
                    {
                        db = slot;
                    }
                    let logical = snapshot_cow::logical_of_slot_for_test()[db];
                    if let Some(l) = logical
                        && l >= cur
                    {
                        cov.flushes_frozen += 1;
                        if l == cur && cursor > 0 {
                            cov.flush_of_db_in_progress_mid_walk += 1;
                        }
                        if swapped_since_arm[db] {
                            cov.flush_after_swap += 1;
                        }
                    }
                    if rng.below(3) == 0 {
                        live(&mut dbs, &mut tail, db, &[b"FLUSHDB", b"ASYNC"]);
                    } else {
                        live(&mut dbs, &mut tail, db, &[b"FLUSHDB"]);
                    }
                }
                _ => {
                    // Grow a database well past its epoch-start size, then
                    // (usually) flush it: the trim must drop every post-epoch
                    // row and rebuild the table.
                    if rng.below(4) != 0
                        && let Some(slot) = slot_of(cur)
                    {
                        db = slot;
                    }
                    let g = [200u64, 3000][rng.below(2) as usize];
                    let tag = rng.next();
                    for j in 0..g {
                        let gk = format!("g{tag:x}:{j}");
                        live(&mut dbs, &mut tail, db, &[b"SET", gk.as_bytes(), b"gv"]);
                    }
                    if rng.below(4) != 0 {
                        let logical = snapshot_cow::logical_of_slot_for_test()[db];
                        if let Some(l) = logical
                            && l >= cur
                        {
                            cov.flushes_frozen += 1;
                            if dbs[db].data().segment_count() > 2 * start_segments[l].max(1) {
                                cov.grown_flushes_over_2x += 1;
                            }
                            if l == cur && cursor > 0 {
                                cov.flush_of_db_in_progress_mid_walk += 1;
                                cov.grown_flush_of_db_in_progress_mid_walk += 1;
                            }
                        }
                        let segs_at_flush = dbs[db].data().segment_count();
                        live(&mut dbs, &mut tail, db, &[b"FLUSHDB"]);
                        epoch.drain();
                        if let Some(l) = logical
                            && let Some(frozen) = epoch
                                .state
                                .as_ref()
                                .and_then(|s| s.frozen_segments_for_test(l))
                            && frozen < segs_at_flush
                        {
                            cov.rebuilds_seen += 1;
                        }
                        check_after_drain(
                            seed,
                            epoch.state.as_ref().expect("epoch"),
                            &start_bills,
                            cov,
                        );
                    }
                }
            }
        }
        if epoch
            .state
            .as_ref()
            .is_some_and(SnapshotState::current_table_rebuilding)
        {
            cov.walk_waits_for_rebuild += 1;
        }
        let done = match rng.below(10) {
            0..=4 => {
                epoch.drain();
                false
            }
            5..=7 => epoch.tick_one(&dbs),
            _ => epoch.tick(&dbs),
        };
        if let Some(state) = epoch.state.as_ref() {
            assert!(
                state.aborted().is_none(),
                "seed {seed}: the save aborted: {:?}",
                state.aborted()
            );
            check_after_drain(seed, state, &start_bills, cov);
        }
        if done {
            break;
        }
        ticks += 1;
        assert!(ticks < 200_000, "seed {seed}: the epoch never converged");
    }
    let records: Vec<Record> = epoch
        .try_finish(&dbs)
        .unwrap_or_else(|e| panic!("seed {seed}: the save failed: {e}"));
    // No key twice.
    let mut counts: BTreeMap<(usize, Vec<u8>), usize> = BTreeMap::new();
    for (db, key, _) in &records {
        *counts.entry((*db, key.to_vec())).or_default() += 1;
    }
    let dups: Vec<_> = counts
        .iter()
        .filter(|(_, n)| **n > 1)
        .map(|(k, _)| k)
        .collect();
    assert!(dups.is_empty(), "seed {seed}: keys written twice: {dups:?}");
    // The file, loaded alone, is the epoch-start image.
    let mut loaded: Vec<Database> = (0..N_DBS).map(|_| Database::new()).collect();
    for (db, key, entry) in records {
        loaded[db].set(&key, entry);
    }
    let end = current_time_ms();
    let got = dump(&mut loaded, now);
    if got != expected {
        let missing: Vec<_> = expected
            .keys()
            .filter(|k| !got.contains_key(*k))
            .take(5)
            .collect();
        let extra: Vec<_> = got
            .keys()
            .filter(|k| !expected.contains_key(*k))
            .take(5)
            .collect();
        let wrong: Vec<_> = expected
            .iter()
            .filter(|(k, v)| got.get(*k).is_some_and(|g| g != *v))
            .take(5)
            .map(|(k, v)| (k.clone(), v.clone(), got[k].clone()))
            .collect();
        panic!(
            "seed {seed}: the file is not the epoch-start image: {} missing {missing:?}, {} extra \
             {extra:?}, wrong {wrong:?}",
            expected.keys().filter(|k| !got.contains_key(*k)).count(),
            got.keys().filter(|k| !expected.contains_key(*k)).count(),
        );
    }
    // File + tail = live.
    snapshot_cow::disarm();
    for (db, parts) in &tail {
        let argv: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
        let _ = apply(&mut loaded, *db, &argv);
    }
    // A key given a NEAR expiry is not replay-deterministic (a write that
    // found it alive live may find it expired on replay — a harness artifact,
    // redis's AOF load has the same property), so those keys are left out of
    // the tail check only. The file check above is exact for every key.
    let near: std::collections::BTreeSet<Vec<u8>> = tail
        .iter()
        .filter(|(_, p)| {
            p[0] == b"PEXPIREAT" && p[2][0] != b'-' && num(&p[2]) < (now + 1000) as usize
        })
        .map(|(_, p)| p[1].clone())
        .collect();
    let keep = |m: Image| -> Image {
        m.into_iter()
            .filter(|((_, k), _)| !near.contains(k))
            .collect()
    };
    let (a, b) = (keep(dump(&mut loaded, end)), keep(dump(&mut dbs, end)));
    if a != b {
        let diff: Vec<_> = a
            .keys()
            .chain(b.keys())
            .filter(|k| a.get(*k) != b.get(*k))
            .take(6)
            .map(|k| {
                (
                    k.0,
                    String::from_utf8_lossy(&k.1).into_owned(),
                    a.get(k).cloned(),
                    b.get(k).cloned(),
                )
            })
            .collect();
        let near_expiry_keys: Vec<String> = tail
            .iter()
            .filter(|(_, p)| {
                p[0] == b"PEXPIREAT"
                    && p[2][0] != b'-'
                    && num(&p[2]) < (now + 1000) as usize
                    && num(&p[2]) > now as usize
            })
            .map(|(_, p)| String::from_utf8_lossy(&p[1]).into_owned())
            .collect();
        panic!(
            "seed {seed}: file + tail is not the live keyspace (recovered, live): {diff:?}; near-expiry keys in the tail: {near_expiry_keys:?}"
        );
    }
    cov.seeds += 1;
}

#[test]
fn the_file_is_exactly_the_epoch_start_image_over_random_workloads() {
    let n: u64 = std::env::var("MOON_TEST_SNAPSHOT_PROP_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    let start: u64 = std::env::var("MOON_TEST_SNAPSHOT_PROP_SEED_START")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let mut cov = Coverage::default();
    for seed in start..start + n {
        one_seed(seed, &mut cov);
    }
    crate::persistence::snapshot::frozen::set_trim_budget_for_test(None);
    eprintln!("snapshot property coverage: {cov:#?}");
    assert!(
        n < 8 || (cov.trims_in_progress > 0 && cov.rebuilds_seen > 0),
        "the seeds never spread a trim over several drains: {cov:#?}"
    );
}

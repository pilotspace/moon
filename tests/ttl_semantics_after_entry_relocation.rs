//! TTL semantics must be BYTE-IDENTICAL after the whole-key deadline moved out
//! of `CompactEntry` and into the `Database`'s `expires` map.
//!
//! The move exists to reclaim 8 bytes from every SLOT of every dashtable
//! segment (60 per segment, occupied or not) for a field that was zero on
//! every key of a no-TTL workload. Nothing about what a client observes may
//! change with it, so this file drives the real command handlers — the same
//! functions dispatch calls — and pins:
//!
//!   * every command that reads or writes a whole-key deadline;
//!   * MILLISECOND fidelity, the property W3 introduced (a key must live at
//!     least the requested TTL, and `PTTL` must read back exactly what was
//!     written);
//!   * the deadline surviving a value edit (APPEND/SETRANGE/INCR/SETBIT),
//!     a rename, a move, a copy, and a persistence round-trip;
//!   * `PERSIST` / `SET` without `KEEPTTL` actually CLEARING it.
//!
//! Every assertion here passes identically on the parent commit — that is the
//! point. What would have failed there is `size_of::<CompactEntry>()`.

use bytes::Bytes;
use moon::command::{key as key_cmd, string as string_cmd};
use moon::protocol::Frame;
use moon::storage::Database;
use moon::storage::entry::{Entry, current_time_ms};

/// One value-mutating command, boxed so a table can hold several shapes.
type Edit = Box<dyn Fn(&mut Database)>;

fn bulk(b: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(b))
}

fn args(list: &[&[u8]]) -> Vec<Frame> {
    list.iter().map(|b| bulk(b)).collect()
}

fn int(f: &Frame) -> i64 {
    match f {
        Frame::Integer(i) => *i,
        other => panic!("expected an integer reply, got {other:?}"),
    }
}

fn db_with(key: &[u8], val: &[u8]) -> Database {
    let mut db = Database::new();
    db.set(key, Entry::new_string(Bytes::copy_from_slice(val)));
    db
}

// ── the reading commands ────────────────────────────────────────────────────

#[test]
fn ttl_pttl_expiretime_pexpiretime_answer_for_all_three_key_states() {
    let mut db = db_with(b"k", b"v");

    // No TTL: -1 from all four.
    assert_eq!(int(&key_cmd::ttl(&mut db, &args(&[b"k"]))), -1);
    assert_eq!(int(&key_cmd::pttl(&mut db, &args(&[b"k"]))), -1);
    assert_eq!(int(&key_cmd::expiretime(&mut db, &args(&[b"k"]))), -1);
    assert_eq!(int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"]))), -1);

    // Missing key: -2 from all four.
    assert_eq!(int(&key_cmd::ttl(&mut db, &args(&[b"nope"]))), -2);
    assert_eq!(int(&key_cmd::pttl(&mut db, &args(&[b"nope"]))), -2);
    assert_eq!(int(&key_cmd::expiretime(&mut db, &args(&[b"nope"]))), -2);
    assert_eq!(int(&key_cmd::pexpiretime(&mut db, &args(&[b"nope"]))), -2);

    // With a TTL: the exact absolute deadline, in both units.
    let deadline = current_time_ms() + 100_000;
    let set = key_cmd::pexpireat(&mut db, &args(&[b"k", deadline.to_string().as_bytes()]));
    assert_eq!(int(&set), 1);
    assert_eq!(
        int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"]))),
        deadline as i64,
        "PEXPIRETIME must report the exact millisecond deadline"
    );
    assert_eq!(
        int(&key_cmd::expiretime(&mut db, &args(&[b"k"]))),
        (deadline / 1000) as i64
    );
    let pttl = int(&key_cmd::pttl(&mut db, &args(&[b"k"])));
    assert!((99_000..=100_000).contains(&pttl), "PTTL was {pttl}");
    assert_eq!(int(&key_cmd::ttl(&mut db, &args(&[b"k"]))), 100);
}

// ── millisecond fidelity ────────────────────────────────────────────────────

#[test]
fn pexpire_keeps_sub_second_precision_and_the_key_lives_the_full_ttl() {
    let mut db = db_with(b"k", b"v");
    // 1500ms: a seconds-truncating store would expire it at 1000ms — up to
    // 999ms EARLY, violating "a key lives at least the requested TTL".
    assert_eq!(int(&key_cmd::pexpire(&mut db, &args(&[b"k", b"1500"]))), 1);
    let pttl = int(&key_cmd::pttl(&mut db, &args(&[b"k"])));
    assert!(
        (1_400..=1_500).contains(&pttl),
        "PTTL must keep the sub-second remainder, got {pttl}"
    );

    // And an exact absolute deadline round-trips to the millisecond.
    let odd = current_time_ms() + 10_000_500 % 1000 + 60_000 + 777;
    assert_eq!(
        int(&key_cmd::pexpireat(
            &mut db,
            &args(&[b"k", odd.to_string().as_bytes()])
        )),
        1
    );
    assert_eq!(
        int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"]))),
        odd as i64
    );
}

#[test]
fn expire_and_expireat_agree_with_their_millisecond_siblings() {
    let mut db = db_with(b"k", b"v");
    assert_eq!(int(&key_cmd::expire(&mut db, &args(&[b"k", b"100"]))), 1);
    let by_secs = int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"])));

    let mut db2 = db_with(b"k", b"v");
    assert_eq!(
        int(&key_cmd::pexpire(&mut db2, &args(&[b"k", b"100000"]))),
        1
    );
    let by_ms = int(&key_cmd::pexpiretime(&mut db2, &args(&[b"k"])));
    assert!(
        (by_secs - by_ms).abs() < 1_000,
        "EXPIRE 100 and PEXPIRE 100000 must land on the same deadline: {by_secs} vs {by_ms}"
    );

    let at = current_time_ms() / 1000 + 500;
    let mut db3 = db_with(b"k", b"v");
    assert_eq!(
        int(&key_cmd::expireat(
            &mut db3,
            &args(&[b"k", at.to_string().as_bytes()])
        )),
        1
    );
    assert_eq!(
        int(&key_cmd::pexpiretime(&mut db3, &args(&[b"k"]))),
        (at * 1000) as i64
    );
}

// ── clearing a deadline ─────────────────────────────────────────────────────

#[test]
fn persist_clears_the_deadline_and_reports_whether_there_was_one() {
    let mut db = db_with(b"k", b"v");
    assert_eq!(
        int(&key_cmd::persist(&mut db, &args(&[b"k"]))),
        0,
        "PERSIST on a key with no TTL answers 0"
    );
    assert_eq!(int(&key_cmd::expire(&mut db, &args(&[b"k", b"100"]))), 1);
    assert_eq!(int(&key_cmd::persist(&mut db, &args(&[b"k"]))), 1);
    assert_eq!(int(&key_cmd::ttl(&mut db, &args(&[b"k"]))), -1);
    assert_eq!(db.expires_map_len(), 0, "PERSIST must free the map row");
    assert!(db.debug_expires_consistent());
    assert_eq!(int(&key_cmd::persist(&mut db, &args(&[b"nope"]))), 0);
}

#[test]
fn a_plain_set_clears_the_ttl_and_keepttl_preserves_it() {
    let mut db = db_with(b"k", b"v");
    assert_eq!(int(&key_cmd::expire(&mut db, &args(&[b"k", b"100"]))), 1);
    let before = int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"])));

    string_cmd::set(&mut db, &args(&[b"k", b"v2", b"KEEPTTL"]));
    assert_eq!(
        int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"]))),
        before,
        "KEEPTTL must keep the EXACT deadline, not an approximation"
    );

    string_cmd::set(&mut db, &args(&[b"k", b"v3"]));
    assert_eq!(
        int(&key_cmd::ttl(&mut db, &args(&[b"k"]))),
        -1,
        "a plain SET drops the TTL"
    );
    assert_eq!(db.expires_map_len(), 0);
    assert!(db.debug_expires_consistent());
}

#[test]
fn set_with_ex_px_exat_pxat_all_arm_the_same_deadline() {
    for (opt, val) in [
        (&b"EX"[..], "100".to_string()),
        (b"PX", "100000".to_string()),
    ] {
        let mut db = Database::new();
        string_cmd::set(&mut db, &args(&[b"k", b"v", opt, val.as_bytes()]));
        let pttl = int(&key_cmd::pttl(&mut db, &args(&[b"k"])));
        assert!(
            (99_000..=100_000).contains(&pttl),
            "{opt:?} gave PTTL {pttl}"
        );
        assert!(db.debug_expires_consistent());
    }

    let at_ms = current_time_ms() + 100_000;
    let mut db = Database::new();
    string_cmd::set(
        &mut db,
        &args(&[b"k", b"v", b"PXAT", at_ms.to_string().as_bytes()]),
    );
    assert_eq!(
        int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"]))),
        at_ms as i64
    );

    let at_s = at_ms / 1000;
    let mut db = Database::new();
    string_cmd::set(
        &mut db,
        &args(&[b"k", b"v", b"EXAT", at_s.to_string().as_bytes()]),
    );
    assert_eq!(
        int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"]))),
        (at_s * 1000) as i64
    );
}

#[test]
fn getex_arms_and_clears_exactly_like_expire_and_persist() {
    let mut db = db_with(b"k", b"v");
    string_cmd::getex(&mut db, &args(&[b"k", b"PX", b"100000"]));
    let pttl = int(&key_cmd::pttl(&mut db, &args(&[b"k"])));
    assert!((99_000..=100_000).contains(&pttl), "GETEX PX gave {pttl}");

    string_cmd::getex(&mut db, &args(&[b"k", b"PERSIST"]));
    assert_eq!(int(&key_cmd::ttl(&mut db, &args(&[b"k"]))), -1);
    assert_eq!(db.expires_map_len(), 0);
    assert!(db.debug_expires_consistent());

    // A bare GETEX must NOT touch the deadline.
    string_cmd::getex(&mut db, &args(&[b"k", b"EX", b"100"]));
    let before = int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"])));
    string_cmd::getex(&mut db, &args(&[b"k"]));
    assert_eq!(int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"]))), before);
}

// ── a value edit must not lose the deadline ─────────────────────────────────

#[test]
fn in_place_value_edits_keep_the_exact_deadline() {
    // Every one of these rebuilds the entry from scratch, so each is its own
    // chance to drop the TTL now that it no longer rides inside the entry.
    let cases: Vec<(&str, Edit)> = vec![
        (
            "APPEND",
            Box::new(|db: &mut Database| {
                string_cmd::append(db, &args(&[b"k", b"more"]));
            }),
        ),
        (
            "SETRANGE",
            Box::new(|db: &mut Database| {
                string_cmd::setrange(db, &args(&[b"k", b"1", b"X"]));
            }),
        ),
        (
            "SETBIT",
            Box::new(|db: &mut Database| {
                string_cmd::setbit(db, &args(&[b"k", b"3", b"1"]));
            }),
        ),
    ];
    for (name, edit) in cases {
        let mut db = db_with(b"k", b"12");
        assert_eq!(int(&key_cmd::expire(&mut db, &args(&[b"k", b"100"]))), 1);
        let before = int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"])));
        edit(&mut db);
        assert_eq!(
            int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"]))),
            before,
            "{name} must keep the deadline it found"
        );
        assert!(db.debug_expires_consistent(), "{name}");
    }

    // INCR/INCRBYFLOAT need a numeric value.
    for (name, edit) in [
        (
            "INCR",
            Box::new(|db: &mut Database| {
                string_cmd::incr(db, &args(&[b"k"]));
            }) as Edit,
        ),
        (
            "INCRBYFLOAT",
            Box::new(|db: &mut Database| {
                string_cmd::incrbyfloat(db, &args(&[b"k", b"1.5"]));
            }),
        ),
    ] {
        let mut db = db_with(b"k", b"12");
        assert_eq!(int(&key_cmd::expire(&mut db, &args(&[b"k", b"100"]))), 1);
        let before = int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"])));
        edit(&mut db);
        assert_eq!(
            int(&key_cmd::pexpiretime(&mut db, &args(&[b"k"]))),
            before,
            "{name} must keep the deadline it found"
        );
        assert!(db.debug_expires_consistent(), "{name}");
    }
}

// ── a key that MOVES carries its deadline ───────────────────────────────────

#[test]
fn rename_and_renamenx_carry_the_exact_deadline() {
    for (name, run) in [
        (
            "RENAME",
            Box::new(|db: &mut Database| {
                key_cmd::rename(db, &args(&[b"src", b"dst"]));
            }) as Edit,
        ),
        (
            "RENAMENX",
            Box::new(|db: &mut Database| {
                key_cmd::renamenx(db, &args(&[b"src", b"dst"]));
            }),
        ),
    ] {
        let mut db = db_with(b"src", b"v");
        assert_eq!(int(&key_cmd::expire(&mut db, &args(&[b"src", b"100"]))), 1);
        let before = int(&key_cmd::pexpiretime(&mut db, &args(&[b"src"])));
        run(&mut db);
        assert_eq!(
            int(&key_cmd::pexpiretime(&mut db, &args(&[b"dst"]))),
            before,
            "{name} must move the deadline with the value"
        );
        assert_eq!(int(&key_cmd::ttl(&mut db, &args(&[b"src"]))), -2);
        assert_eq!(db.expires_map_len(), 1, "{name} leaked or lost a map row");
        assert!(db.debug_expires_consistent(), "{name}");
    }
}

#[test]
fn move_and_copy_across_databases_carry_the_exact_deadline() {
    use moon::command::keyspace::move_cmd::{copy_core, move_core};

    // MOVE: the deadline leaves src with the value.
    let mut src = db_with(b"k", b"v");
    let mut dst = Database::new();
    assert_eq!(int(&key_cmd::expire(&mut src, &args(&[b"k", b"100"]))), 1);
    let before = int(&key_cmd::pexpiretime(&mut src, &args(&[b"k"])));
    assert_eq!(int(&move_core(&mut src, &mut dst, b"k")), 1);
    assert_eq!(int(&key_cmd::pexpiretime(&mut dst, &args(&[b"k"]))), before);
    assert_eq!(src.expires_map_len(), 0, "MOVE left the deadline behind");
    assert!(src.debug_expires_consistent() && dst.debug_expires_consistent());

    // MOVE that COLLIDES must put the deadline back, not eat it.
    let mut src = db_with(b"k", b"v");
    let mut dst = db_with(b"k", b"other");
    assert_eq!(int(&key_cmd::expire(&mut src, &args(&[b"k", b"100"]))), 1);
    let before = int(&key_cmd::pexpiretime(&mut src, &args(&[b"k"])));
    assert_eq!(int(&move_core(&mut src, &mut dst, b"k")), 0);
    assert_eq!(
        int(&key_cmd::pexpiretime(&mut src, &args(&[b"k"]))),
        before,
        "a rolled-back MOVE must restore the deadline"
    );
    assert!(src.debug_expires_consistent());

    // COPY: Redis copies the TTL too; the source keeps its own.
    let mut src = db_with(b"k", b"v");
    let mut dst = Database::new();
    assert_eq!(int(&key_cmd::expire(&mut src, &args(&[b"k", b"100"]))), 1);
    let before = int(&key_cmd::pexpiretime(&mut src, &args(&[b"k"])));
    assert_eq!(int(&copy_core(&mut src, &mut dst, b"k", b"k2", false)), 1);
    assert_eq!(
        int(&key_cmd::pexpiretime(&mut dst, &args(&[b"k2"]))),
        before
    );
    assert_eq!(int(&key_cmd::pexpiretime(&mut src, &args(&[b"k"]))), before);
    assert!(src.debug_expires_consistent() && dst.debug_expires_consistent());
}

// ── persistence round-trips ─────────────────────────────────────────────────

#[test]
fn moon_rdb_round_trip_preserves_the_exact_millisecond_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dump.rdb");

    let deadline = current_time_ms() + 3_600_000;
    let mut dbs = vec![Database::new()];
    dbs[0].set_with_expiry(
        b"volatile",
        Entry::new_string(Bytes::from_static(b"v")),
        deadline,
    );
    dbs[0].set_string(b"permanent", Bytes::from_static(b"v"));
    // A key already past its deadline must not come back at all.
    dbs[0].set_with_expiry(
        b"gone",
        Entry::new_string(Bytes::from_static(b"v")),
        current_time_ms() - 1,
    );

    moon::persistence::rdb::save(&dbs, &path).unwrap();

    let mut loaded = vec![Database::new()];
    moon::persistence::rdb::load(&mut loaded, &path).unwrap();

    assert_eq!(
        loaded[0].expires_at_ms(b"volatile"),
        deadline,
        "RDB round-trip must preserve the exact millisecond deadline"
    );
    assert_eq!(loaded[0].expires_at_ms(b"permanent"), 0);
    assert!(!loaded[0].get(b"permanent").unwrap().has_expiry());
    assert!(loaded[0].get(b"gone").is_none());
    assert_eq!(loaded[0].expires_map_len(), 1);
    assert_eq!(
        loaded[0].expiry_index_len(),
        1,
        "the deadline index must be rebuilt by the load path"
    );
    assert!(loaded[0].debug_expires_consistent());
}

#[test]
fn redis_format_rdb_round_trip_preserves_the_deadline() {
    let deadline = current_time_ms() + 3_600_000;
    let mut dbs = vec![Database::new()];
    dbs[0].set_with_expiry(b"ek", Entry::new_string(Bytes::from_static(b"v")), deadline);
    dbs[0].set_string(b"pk", Bytes::from_static(b"v"));

    let mut buf = Vec::new();
    moon::persistence::redis_rdb::write_rdb(&dbs, &mut buf);

    let mut loaded = vec![Database::new()];
    moon::persistence::redis_rdb::load_rdb(&mut loaded, &buf).unwrap();

    assert_eq!(loaded[0].expires_at_ms(b"ek"), deadline);
    assert_eq!(loaded[0].expires_at_ms(b"pk"), 0);
    assert!(loaded[0].debug_expires_consistent());
}

#[test]
fn a_keyspace_with_no_ttls_reloads_with_an_empty_expires_map() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dump.rdb");
    let mut dbs = vec![Database::new()];
    for i in 0..500u32 {
        dbs[0].set_string(&Bytes::from(format!("k{i}")), Bytes::from_static(b"v"));
    }
    moon::persistence::rdb::save(&dbs, &path).unwrap();

    let mut loaded = vec![Database::new()];
    moon::persistence::rdb::load(&mut loaded, &path).unwrap();
    assert_eq!(loaded[0].len(), 500);
    assert_eq!(
        loaded[0].expires_map_len(),
        0,
        "keys without a TTL must cost nothing in the expires map"
    );
    assert!(loaded[0].debug_expires_consistent());
}

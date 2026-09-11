mod set_read;
mod set_write;

use bytes::Bytes;
use std::collections::HashSet;

use crate::protocol::Frame;
use crate::storage::Database;

use super::helpers::extract_bytes;

// ---------------------------------------------------------------------------
// Shared helpers (used by both set_read and set_write)
// ---------------------------------------------------------------------------

/// Helper: parse an integer from a frame.
pub(crate) fn parse_int(frame: &Frame) -> Option<i64> {
    let b = extract_bytes(frame)?;
    std::str::from_utf8(b).ok()?.parse().ok()
}

/// Helper: simple glob match (reused pattern from key.rs).
pub(crate) fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut pi = 0;
    let mut si = 0;
    let mut star_pi = usize::MAX;
    let mut star_si = usize::MAX;

    while si < string.len() {
        if pi < pattern.len() && pattern[pi] == b'\\' {
            pi += 1;
            if pi < pattern.len() && pattern[pi] == string[si] {
                pi += 1;
                si += 1;
                continue;
            }
        } else if pi < pattern.len() && pattern[pi] == b'?' {
            pi += 1;
            si += 1;
            continue;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_si = si;
            pi += 1;
            continue;
        } else if pi < pattern.len() && pattern[pi] == string[si] {
            pi += 1;
            si += 1;
            continue;
        }

        if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_si += 1;
            si = star_si;
            continue;
        }

        return false;
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

/// Collect sets from database as cloned HashSets to avoid borrow conflicts.
/// Returns Err(WRONGTYPE) if any key is the wrong type.
pub(crate) fn collect_sets(
    db: &mut Database,
    keys: &[&Bytes],
) -> Result<Vec<Option<HashSet<Bytes>>>, Frame> {
    let mut sets = Vec::with_capacity(keys.len());
    for key in keys {
        match db.get_set(key) {
            Ok(Some(set)) => sets.push(Some(set.iter().cloned().collect())),
            Ok(None) => sets.push(None),
            Err(e) => return Err(e),
        }
    }
    Ok(sets)
}

// ---------------------------------------------------------------------------
// Re-exports: read operations
// ---------------------------------------------------------------------------
pub use set_read::scard;
pub use set_read::scard_readonly;
pub use set_read::sdiff;
pub use set_read::sdiff_readonly;
pub use set_read::sinter;
pub use set_read::sinter_readonly;
pub use set_read::sismember;
pub use set_read::sismember_readonly;
pub use set_read::smembers;
pub use set_read::smembers_readonly;
pub use set_read::smismember;
pub use set_read::smismember_readonly;
pub use set_read::srandmember;
pub use set_read::srandmember_readonly;
pub use set_read::sscan;
pub use set_read::sscan_readonly;
pub use set_read::sunion;
pub use set_read::sunion_readonly;

// ---------------------------------------------------------------------------
// Re-exports: write operations
// ---------------------------------------------------------------------------
pub use set_read::sintercard;
pub use set_read::sintercard_readonly;
pub use set_write::sadd;
pub use set_write::sdiffstore;
pub use set_write::sinterstore;
pub use set_write::smove;
pub use set_write::spop;
pub use set_write::srem;
pub use set_write::sunionstore;

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framevec;
    use crate::storage::Database;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn setup_set(db: &mut Database, key: &[u8], members: &[&[u8]]) {
        for m in members {
            sadd(db, &[bs(key), bs(m)]);
        }
    }

    // --- SADD / SREM tests ---

    #[test]
    fn test_sadd_basic() {
        let mut db = Database::new();
        let result = sadd(&mut db, &[bs(b"myset"), bs(b"a"), bs(b"b"), bs(b"c")]);
        assert_eq!(result, Frame::Integer(3));

        // Adding duplicates
        let result = sadd(&mut db, &[bs(b"myset"), bs(b"a"), bs(b"d")]);
        assert_eq!(result, Frame::Integer(1)); // only "d" is new
    }

    #[test]
    fn test_srandmember_negative_count_exact_redis_contract() {
        // Redis contract: SRANDMEMBER key -N returns EXACTLY N elements
        // (duplicates allowed) even when N exceeds the cardinality — a
        // relative cap that silently truncates is a compliance break.
        let mut db = Database::new();
        setup_set(&mut db, b"s", &[b"a", b"b", b"c"]);
        match srandmember(&mut db, &[bs(b"s"), bs(b"-1000")]) {
            Frame::Array(items) => {
                assert_eq!(items.len(), 1000, "exactly |count| elements required")
            }
            other => panic!("expected array, got {other:?}"),
        }
        // The read-only twin shares the same contract.
        match srandmember_readonly(&db, &[bs(b"s"), bs(b"-1000")], 0) {
            Frame::Array(items) => assert_eq!(items.len(), 1000),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_srandmember_huge_negative_count_errors_no_abort() {
        // Batch A DoS guard: a COUNT beyond RAND_DUP_COUNT_MAX must be refused
        // loudly (never drive an unbounded Vec::with_capacity -> allocator
        // abort, and never return a silently short reply).
        let mut db = Database::new();
        setup_set(&mut db, b"s", &[b"a", b"b", b"c"]);
        let huge = format!("-{}", crate::command::RAND_DUP_COUNT_MAX + 1);
        match srandmember(&mut db, &[bs(b"s"), bs(huge.as_bytes())]) {
            Frame::Error(e) => assert_eq!(&e[..], crate::command::ERR_RAND_COUNT_RANGE),
            other => panic!("expected error, got {other:?}"),
        }
        // i64::MIN: unsigned_abs() of the most extreme input stays guarded.
        match srandmember_readonly(&db, &[bs(b"s"), bs(i64::MIN.to_string().as_bytes())], 0) {
            Frame::Error(e) => assert_eq!(&e[..], crate::command::ERR_RAND_COUNT_RANGE),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn test_srem_basic() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = srem(&mut db, &[bs(b"myset"), bs(b"a"), bs(b"d")]);
        assert_eq!(result, Frame::Integer(1)); // only "a" removed

        // Remove remaining to trigger auto-delete
        let result = srem(&mut db, &[bs(b"myset"), bs(b"b"), bs(b"c")]);
        assert_eq!(result, Frame::Integer(2));
        assert!(!db.exists(b"myset")); // key should be deleted
    }

    // --- SMEMBERS / SCARD tests ---

    #[test]
    fn test_smembers_and_scard() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = scard(&mut db, &[bs(b"myset")]);
        assert_eq!(result, Frame::Integer(3));

        let result = smembers(&mut db, &[bs(b"myset")]);
        match result {
            Frame::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("expected array"),
        }

        // Missing key
        let result = scard(&mut db, &[bs(b"missing")]);
        assert_eq!(result, Frame::Integer(0));

        let result = smembers(&mut db, &[bs(b"missing")]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    // --- SISMEMBER / SMISMEMBER tests ---

    #[test]
    fn test_sismember() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b"]);

        assert_eq!(
            sismember(&mut db, &[bs(b"myset"), bs(b"a")]),
            Frame::Integer(1)
        );
        assert_eq!(
            sismember(&mut db, &[bs(b"myset"), bs(b"c")]),
            Frame::Integer(0)
        );
        assert_eq!(
            sismember(&mut db, &[bs(b"missing"), bs(b"a")]),
            Frame::Integer(0)
        );
    }

    #[test]
    fn test_smismember() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b"]);

        let result = smismember(&mut db, &[bs(b"myset"), bs(b"a"), bs(b"c"), bs(b"b")]);
        assert_eq!(
            result,
            Frame::Array(framevec![
                Frame::Integer(1),
                Frame::Integer(0),
                Frame::Integer(1)
            ])
        );
    }

    // --- SINTER tests ---

    #[test]
    fn test_sinter() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b", b"c"]);
        setup_set(&mut db, b"s2", &[b"b", b"c", b"d"]);

        let result = sinter(&mut db, &[bs(b"s1"), bs(b"s2")]);
        match result {
            Frame::Array(arr) => {
                let members: HashSet<Bytes> = arr
                    .into_iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b,
                        _ => panic!("expected bulkstring"),
                    })
                    .collect();
                assert_eq!(members.len(), 2);
                assert!(members.contains(&Bytes::from_static(b"b")));
                assert!(members.contains(&Bytes::from_static(b"c")));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sinter_missing_key() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b"]);

        let result = sinter(&mut db, &[bs(b"s1"), bs(b"missing")]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    // --- SUNION tests ---

    #[test]
    fn test_sunion() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b"]);
        setup_set(&mut db, b"s2", &[b"b", b"c"]);

        let result = sunion(&mut db, &[bs(b"s1"), bs(b"s2")]);
        match result {
            Frame::Array(arr) => {
                let members: HashSet<Bytes> = arr
                    .into_iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b,
                        _ => panic!("expected bulkstring"),
                    })
                    .collect();
                assert_eq!(members.len(), 3);
                assert!(members.contains(&Bytes::from_static(b"a")));
                assert!(members.contains(&Bytes::from_static(b"b")));
                assert!(members.contains(&Bytes::from_static(b"c")));
            }
            _ => panic!("expected array"),
        }
    }

    // --- SDIFF tests ---

    #[test]
    fn test_sdiff() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b", b"c"]);
        setup_set(&mut db, b"s2", &[b"b", b"c", b"d"]);

        let result = sdiff(&mut db, &[bs(b"s1"), bs(b"s2")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"a")));
            }
            _ => panic!("expected array"),
        }
    }

    // --- SINTERSTORE / SUNIONSTORE / SDIFFSTORE tests ---

    #[test]
    fn test_sinterstore() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b", b"c"]);
        setup_set(&mut db, b"s2", &[b"b", b"c", b"d"]);

        let result = sinterstore(&mut db, &[bs(b"dest"), bs(b"s1"), bs(b"s2")]);
        assert_eq!(result, Frame::Integer(2));

        let result = scard(&mut db, &[bs(b"dest")]);
        assert_eq!(result, Frame::Integer(2));
    }

    #[test]
    fn test_sunionstore() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b"]);
        setup_set(&mut db, b"s2", &[b"b", b"c"]);

        let result = sunionstore(&mut db, &[bs(b"dest"), bs(b"s1"), bs(b"s2")]);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_sdiffstore() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b", b"c"]);
        setup_set(&mut db, b"s2", &[b"b", b"c"]);

        let result = sdiffstore(&mut db, &[bs(b"dest"), bs(b"s1"), bs(b"s2")]);
        assert_eq!(result, Frame::Integer(1));
    }

    // --- SRANDMEMBER tests ---

    #[test]
    fn test_srandmember_single() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = srandmember(&mut db, &[bs(b"myset")]);
        match result {
            Frame::BulkString(_) => {} // any member is fine
            _ => panic!("expected bulkstring"),
        }

        // Missing key
        let result = srandmember(&mut db, &[bs(b"missing")]);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_srandmember_positive_count() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        // Request more than set size: should return all distinct
        let result = srandmember(&mut db, &[bs(b"myset"), bs(b"10")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3); // min(10, 3) = 3
                // Check uniqueness by extracting bytes into a HashSet
                let set: HashSet<Bytes> = arr
                    .iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b.clone(),
                        _ => panic!("expected bulkstring"),
                    })
                    .collect();
                assert_eq!(set.len(), 3);
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_srandmember_negative_count() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a"]);

        // Negative count: may have duplicates, always returns abs(count) elements
        let result = srandmember(&mut db, &[bs(b"myset"), bs(b"-5")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 5); // always returns 5 elements
            }
            _ => panic!("expected array"),
        }
    }

    // --- SPOP tests ---

    #[test]
    fn test_spop_single() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = spop(&mut db, &[bs(b"myset")]);
        match result {
            Frame::BulkString(_) => {}
            _ => panic!("expected bulkstring"),
        }

        assert_eq!(scard(&mut db, &[bs(b"myset")]), Frame::Integer(2));
    }

    #[test]
    fn test_spop_count() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = spop(&mut db, &[bs(b"myset"), bs(b"2")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("expected array"),
        }

        assert_eq!(scard(&mut db, &[bs(b"myset")]), Frame::Integer(1));
    }

    #[test]
    fn test_spop_all_auto_delete() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b"]);

        let _ = spop(&mut db, &[bs(b"myset"), bs(b"10")]);
        assert!(!db.exists(b"myset")); // key should be removed
    }

    #[test]
    fn test_spop_missing_key() {
        let mut db = Database::new();
        let result = spop(&mut db, &[bs(b"missing")]);
        assert_eq!(result, Frame::Null);

        let result = spop(&mut db, &[bs(b"missing"), bs(b"3")]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    // ── Guards for index-based random selection ───────────────────────────
    // SPOP/SRANDMEMBER now address members by index instead of materializing
    // the set. Two failure modes come with that, and neither is caught by the
    // existing tests above.

    /// `swap_remove` moves the LAST member into the freed slot. If SPOP
    /// resolved an index after a removal instead of before, it would return a
    /// member it had already returned, or one it never drew. With a set of 50
    /// and a count of 25, a shifted index is overwhelmingly likely.
    #[test]
    fn spop_count_returns_distinct_members_despite_swap_remove() {
        for _ in 0..20 {
            let mut db = Database::new();
            let mut args = vec![bs(b"s")];
            for i in 0..50u32 {
                args.push(Frame::BulkString(Bytes::from(format!("m{i}"))));
            }
            sadd(&mut db, &args);

            let out = spop(&mut db, &[bs(b"s"), bs(b"25")]);
            let Frame::Array(items) = out else {
                panic!("SPOP with count must return an array");
            };
            assert_eq!(items.len(), 25, "SPOP returned the wrong count");
            let mut seen: Vec<Vec<u8>> = items
                .iter()
                .map(|f| match f {
                    Frame::BulkString(b) => b.to_vec(),
                    other => panic!("expected bulk string, got {other:?}"),
                })
                .collect();
            seen.sort();
            seen.dedup();
            assert_eq!(seen.len(), 25, "SPOP returned duplicate members");
            // and the set must have shrunk by exactly that many
            assert_eq!(scard(&mut db, &[bs(b"s")]), Frame::Integer(25));
        }
    }

    /// A fixed index would satisfy every distinctness assertion above while
    /// being badly wrong: SPOP would always pop the same slot. Draw many times
    /// and require more than one distinct answer.
    #[test]
    fn spop_single_is_not_deterministic() {
        let mut distinct = std::collections::HashSet::new();
        for _ in 0..40 {
            let mut db = Database::new();
            let mut args = vec![bs(b"s")];
            for i in 0..20u32 {
                args.push(Frame::BulkString(Bytes::from(format!("m{i}"))));
            }
            sadd(&mut db, &args);
            if let Frame::BulkString(b) = spop(&mut db, &[bs(b"s")]) {
                distinct.insert(b.to_vec());
            }
        }
        assert!(
            distinct.len() > 1,
            "SPOP returned the same member every time across 40 fresh sets —              the index is not actually random"
        );
    }

    /// Same argument for SRANDMEMBER, which shares the primitive.
    #[test]
    fn srandmember_single_is_not_deterministic() {
        let mut db = Database::new();
        let mut args = vec![bs(b"s")];
        for i in 0..20u32 {
            args.push(Frame::BulkString(Bytes::from(format!("m{i}"))));
        }
        sadd(&mut db, &args);

        let mut distinct = std::collections::HashSet::new();
        for _ in 0..40 {
            if let Frame::BulkString(b) = srandmember(&mut db, &[bs(b"s")]) {
                distinct.insert(b.to_vec());
            }
        }
        assert!(
            distinct.len() > 1,
            "SRANDMEMBER returned the same member 40 times — index is not random"
        );
        // and it must not have mutated the set
        assert_eq!(scard(&mut db, &[bs(b"s")]), Frame::Integer(20));
    }

    /// Popping exactly the whole set must yield every member once and delete
    /// the key -- the boundary where `index::sample(len, len)` is a full
    /// permutation and every `swap_remove` shifts something.
    #[test]
    fn spop_entire_set_yields_every_member_once() {
        let mut db = Database::new();
        let mut args = vec![bs(b"s")];
        for i in 0..32u32 {
            args.push(Frame::BulkString(Bytes::from(format!("m{i}"))));
        }
        sadd(&mut db, &args);

        let Frame::Array(items) = spop(&mut db, &[bs(b"s"), bs(b"32")]) else {
            panic!("expected array");
        };
        let mut seen: Vec<Vec<u8>> = items
            .iter()
            .map(|f| match f {
                Frame::BulkString(b) => b.to_vec(),
                other => panic!("expected bulk string, got {other:?}"),
            })
            .collect();
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), 32, "not every member came back exactly once");
        assert_eq!(scard(&mut db, &[bs(b"s")]), Frame::Integer(0));
    }

    // --- SSCAN tests ---

    #[test]
    fn test_sscan_basic() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = sscan(&mut db, &[bs(b"myset"), bs(b"0")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                // Should return cursor "0" (all scanned) and array of members
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"0")));
                match &arr[1] {
                    Frame::Array(members) => assert_eq!(members.len(), 3),
                    _ => panic!("expected inner array"),
                }
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sscan_with_match() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"apple", b"banana", b"apricot"]);

        let result = sscan(&mut db, &[bs(b"myset"), bs(b"0"), bs(b"MATCH"), bs(b"ap*")]);
        match result {
            Frame::Array(arr) => match &arr[1] {
                Frame::Array(members) => {
                    assert_eq!(members.len(), 2);
                }
                _ => panic!("expected inner array"),
            },
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sscan_missing_key() {
        let mut db = Database::new();
        let result = sscan(&mut db, &[bs(b"missing"), bs(b"0")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"0")));
                assert_eq!(arr[1], Frame::Array(framevec![]));
            }
            _ => panic!("expected array"),
        }
    }

    // --- WRONGTYPE tests ---

    #[test]
    fn test_wrongtype_on_string_key() {
        let mut db = Database::new();
        db.set_string(b"mystr", Bytes::from_static(b"hello"));

        let result = sadd(&mut db, &[bs(b"mystr"), bs(b"a")]);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"WRONGTYPE")),
            _ => panic!("expected WRONGTYPE error"),
        }
    }

    // --- Wrong args tests ---

    #[test]
    fn test_wrong_args() {
        let mut db = Database::new();
        match sadd(&mut db, &[bs(b"key")]) {
            Frame::Error(e) => assert!(e.starts_with(b"ERR wrong number")),
            _ => panic!("expected error"),
        }
        match srem(&mut db, &[bs(b"key")]) {
            Frame::Error(e) => assert!(e.starts_with(b"ERR wrong number")),
            _ => panic!("expected error"),
        }
    }

    // -----------------------------------------------------------------
    // WS6 — container-growth memory accounting (src/storage/db.rs).
    // -----------------------------------------------------------------

    #[test]
    fn test_estimated_memory_rises_with_sadd_growth() {
        let mut db = Database::new();
        // Non-integer members force the standard HashSet path (not intset).
        sadd(&mut db, &[bs(b"s"), bs(b"member-000")]);
        let one = db.estimated_memory();
        for i in 1..50 {
            let m = format!("member-{i:03}");
            sadd(&mut db, &[bs(b"s"), bs(m.as_bytes())]);
        }
        let many = db.estimated_memory();
        assert!(
            many > one,
            "estimated_memory must rise as members are added: one={one} many={many}"
        );
        assert!(
            many - one > 49 * 10,
            "growth must be at least proportional to the added member payload"
        );
    }

    #[test]
    fn test_estimated_memory_falls_with_srem() {
        // The claim: `SREM` credits the ledger, and a fully drained set
        // returns it to zero.
        //
        // It has to be asserted PER ENCODING, because the two answer the
        // "did memory fall?" question differently and both answers are
        // correct. Until moon#897 this test could not see that: the first
        // `SREM` flattened the listpack to a `hashtable` (the assertion below
        // read `"hashtable"`), so every row measured the same path.
        //
        //   * `hashtable` — a member is an owned `Bytes` in the `IndexSet`;
        //     removing it frees that allocation, so `estimated_memory` FALLS
        //     by `set_member_cost` per member. (The index TABLE does not
        //     shrink — `swap_remove` keeps its capacity — which is why the
        //     table is snapshotted rather than credited per member.)
        //   * `listpack` — every member lives inside ONE `Vec<u8>`, billed by
        //     the jemalloc size class of its CAPACITY (moon#788/#810).
        //     `Vec::drain` lowers the length and keeps the capacity, so the
        //     allocator still holds those bytes and the honest ledger does
        //     NOT fall. It must not RISE either, and it must go to zero when
        //     the key itself is removed.

        // -- hashtable: past `set-max-listpack-entries`, so removals credit --
        let n = crate::storage::db::EncodingLimits::moon_defaults().set_entries + 1;
        let mut db = Database::new();
        for i in 0..n {
            let m = format!("member-{i:03}");
            sadd(&mut db, &[bs(b"s"), bs(m.as_bytes())]);
        }
        assert_eq!(encoding_of(&mut db, b"s"), "hashtable");
        let grown = db.estimated_memory();
        for i in 0..n - 1 {
            let m = format!("member-{i:03}");
            srem(&mut db, &[bs(b"s"), bs(m.as_bytes())]);
        }
        let drained = db.estimated_memory();
        assert!(
            drained < grown,
            "estimated_memory must fall as members are removed from a hashtable: \
             grown={grown} drained={drained}"
        );
        srem(
            &mut db,
            &[bs(b"s"), bs(format!("member-{:03}", n - 1).as_bytes())],
        );
        assert_eq!(
            db.estimated_memory(),
            0,
            "estimated_memory must return to zero once the hashtable set is fully drained"
        );

        // -- listpack: capacity is retained, so the ledger holds flat -------
        let mut db = Database::new();
        for i in 0..50 {
            let m = format!("member-{i:03}");
            sadd(&mut db, &[bs(b"s"), bs(m.as_bytes())]);
        }
        srem(&mut db, &[bs(b"s"), bs(b"member-000")]);
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "moon#897: SREM must not flatten a 50-member listpack set"
        );
        let grown = db.estimated_memory();
        for i in 1..49 {
            let m = format!("member-{i:03}");
            srem(&mut db, &[bs(b"s"), bs(m.as_bytes())]);
        }
        let drained = db.estimated_memory();
        assert!(
            drained <= grown,
            "estimated_memory must never RISE as members are removed: \
             grown={grown} drained={drained}"
        );
        assert_eq!(encoding_of(&mut db, b"s"), "listpack");

        srem(&mut db, &[bs(b"s"), bs(b"member-049")]);
        assert_eq!(
            db.estimated_memory(),
            0,
            "estimated_memory must return to zero once the listpack set is fully drained"
        );
    }

    #[test]
    fn test_estimated_memory_spop_credits_removed_member() {
        let mut db = Database::new();
        for i in 0..10 {
            let m = format!("member-{i:03}");
            sadd(&mut db, &[bs(b"s"), bs(m.as_bytes())]);
        }
        // moon#787 / moon#832: the first SPOP promotes the listpack to the
        // IndexSet form and is charged for it; snapshot after that swing.
        spop(&mut db, &[bs(b"s")]);
        assert_eq!(encoding_of(&mut db, b"s"), "hashtable");
        let grown = db.estimated_memory();
        spop(&mut db, &[bs(b"s")]);
        let after = db.estimated_memory();
        assert!(
            after < grown,
            "SPOP must credit the removed member: grown={grown} after={after}"
        );
    }

    // ── moon#832: a read on the MUTABLE path must not flatten the encoding ──
    //
    // Red on b04e8990: every one of these handlers reached the set through
    // `Database::get_set` -> `get_promoted` -> `SetKind::upgrade`, an
    // unconditional one-way conversion. `SADD s 1 2 3` then `SCARD s` on the
    // mutable path left the key a `hashtable` forever. Measured cost of that
    // on the unmodified binary: 1000 eight-member integer sets went
    // 333,055 -> 1,149,055 bytes of `used_memory` (3.45x) after one SCARD each.
    //
    // These call the handlers DIRECTLY, which is what `command::dispatch`,
    // the MULTI/EXEC executor, the Lua bridge and `try_inline_dispatch` all
    // ultimately do -- so one assertion covers every mutable entry point.
    //
    // These share the `encoding_of` probe defined with the #787 listpack
    // tests below: it asks `OBJECT ENCODING`, which reads `entry.value`
    // through `Database::get` and so cannot itself flatten the encoding.

    #[allow(clippy::type_complexity)]
    const READ_HANDLERS_832: &[(&str, fn(&mut Database))] = &[
        ("SMEMBERS", |db| {
            smembers(db, &[bs(b"s")]);
        }),
        ("SCARD", |db| {
            scard(db, &[bs(b"s")]);
        }),
        ("SISMEMBER", |db| {
            sismember(db, &[bs(b"s"), bs(b"1")]);
        }),
        ("SMISMEMBER", |db| {
            smismember(db, &[bs(b"s"), bs(b"1"), bs(b"2")]);
        }),
        ("SRANDMEMBER", |db| {
            srandmember(db, &[bs(b"s")]);
        }),
        ("SSCAN", |db| {
            sscan(db, &[bs(b"s"), bs(b"0")]);
        }),
        ("SINTER", |db| {
            sinter(db, &[bs(b"s")]);
        }),
        ("SUNION", |db| {
            sunion(db, &[bs(b"s")]);
        }),
        ("SDIFF", |db| {
            sdiff(db, &[bs(b"s")]);
        }),
        ("SINTERCARD", |db| {
            sintercard(db, &[bs(b"1"), bs(b"s")]);
        }),
    ];

    #[test]
    fn read_on_mutable_path_keeps_intset_encoding() {
        for (name, call) in READ_HANDLERS_832 {
            let mut db = Database::new();
            setup_set(&mut db, b"s", &[b"1", b"2", b"3"]);
            assert_eq!(
                encoding_of(&mut db, b"s"),
                "intset",
                "{name}: fixture must start compact, or the test proves nothing"
            );
            call(&mut db);
            assert_eq!(
                encoding_of(&mut db, b"s"),
                "intset",
                "{name} flattened the set: a read on the mutable dispatch path rewrote the encoding (moon#832)"
            );
        }
    }

    /// The read must still ANSWER correctly from the compact form — a fix that
    /// preserved the encoding by not reading the set would pass the test above.
    #[test]
    fn read_on_mutable_path_still_answers_from_the_compact_form() {
        let mut db = Database::new();
        setup_set(&mut db, b"s", &[b"1", b"2", b"3"]);
        assert_eq!(scard(&mut db, &[bs(b"s")]), Frame::Integer(3));
        assert_eq!(sismember(&mut db, &[bs(b"s"), bs(b"2")]), Frame::Integer(1));
        assert_eq!(sismember(&mut db, &[bs(b"s"), bs(b"9")]), Frame::Integer(0));
        let Frame::Array(members) = smembers(&mut db, &[bs(b"s")]) else {
            panic!("SMEMBERS must answer an array");
        };
        assert_eq!(members.len(), 3);
        assert_eq!(encoding_of(&mut db, b"s"), "intset");
    }

    /// A WRITE still changes the encoding — the moon#832 fix must not have
    /// disabled the write-side transitions. A non-integer member has no
    /// intset form, so the intset must leave that encoding: to a listpack
    /// when the result still fits (the `intset -> listpack` edge, moon#899 —
    /// this assertion used to demand `hashtable`, codifying the divergence
    /// from redis exactly as `test_object_encoding_sorted_set` once codified
    /// moon#787), and to a hashtable when it cannot, which is the half that
    /// proves `K::upgrade` is still live.
    #[test]
    fn write_on_mutable_path_still_upgrades() {
        let mut db = Database::new();
        setup_set(&mut db, b"s", &[b"1", b"2", b"3"]);
        assert_eq!(encoding_of(&mut db, b"s"), "intset");
        sadd(&mut db, &[bs(b"s"), bs(b"not-an-int")]);
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "a non-integer member has no intset form; a small set moves to a listpack"
        );
        let big = vec![b'x'; crate::storage::db::EncodingLimits::moon_defaults().set_value + 1];
        sadd(&mut db, &[bs(b"s"), bs(&big)]);
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "hashtable",
            "an oversized member has no compact form at all; the write must upgrade"
        );
        assert_eq!(scard(&mut db, &[bs(b"s")]), Frame::Integer(5));
    }

    // ── #787: SADD must reach the listpack encoding ──────────────────────
    //
    // Redis keeps a string set in a listpack until it exceeds
    // set-max-listpack-entries (128) or set-max-listpack-value (64); moon's
    // `SetListpack` variant is wired end to end EXCEPT that nothing ever
    // created one, because `get_or_create_set` calls `SetKind::upgrade`
    // unconditionally. Verified against a redis 8.6.1 oracle: `SADD s a b c d e`
    // reports `listpack` there and `hashtable` here.
    //
    // Probe choice (moon#832): `OBJECT ENCODING` reads `entry.value` through
    // `Database::get`, which does not route through `get_promoted`, so asking
    // about the encoding cannot itself flatten it. Every READ below goes
    // through the `_readonly` twins for the same reason — the mutable
    // `scard`/`sismember`/`smembers` take `get_set` = `get_promoted`, which
    // upgrades a listpack to an `IndexSet` on the FIRST call and would leave
    // the rest of the test measuring a hashtable. `now_ms = 0` is safe: none
    // of these keys carries a TTL.

    /// What `OBJECT ENCODING <key>` actually replies — asserted through the
    /// real command handler, not a private field, so the test checks the
    /// user-visible answer that diverges from Redis.
    fn encoding_of(db: &mut Database, key: &[u8]) -> String {
        match crate::command::key::object(db, &[bs(b"ENCODING"), bs(key)]) {
            Frame::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
            // A key that is not there has no encoding; say so rather than
            // panicking, so a test that loses its fixture fails on the
            // assertion it wrote instead of inside the probe.
            Frame::Null => "<missing>".to_string(),
            other => panic!("OBJECT ENCODING did not reply a bulk string: {other:?}"),
        }
    }

    /// `SMEMBERS` through the shared-read twin, sorted so the assertion does
    /// not depend on listpack (insertion) vs `IndexSet` (swap-remove) order.
    fn ro_members_sorted(db: &Database, key: &[u8]) -> Vec<Vec<u8>> {
        match smembers_readonly(db, &[bs(key)], 0) {
            Frame::Array(items) => {
                let mut v: Vec<Vec<u8>> = items
                    .iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b.to_vec(),
                        other => panic!("expected bulk, got {other:?}"),
                    })
                    .collect();
                v.sort();
                v
            }
            other => panic!("SMEMBERS did not reply an array: {other:?}"),
        }
    }

    #[test]
    fn sadd_small_string_set_stays_listpack() {
        let mut db = Database::new();
        sadd(
            &mut db,
            &[bs(b"s"), bs(b"a"), bs(b"b"), bs(b"c"), bs(b"d"), bs(b"e")],
        );
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "a 5-member string set is far below set-max-listpack-entries (128); \
             Redis reports `listpack` here"
        );
    }

    #[test]
    fn sadd_promotes_past_the_entry_threshold() {
        let mut db = Database::new();
        for i in 0..crate::storage::db::EncodingLimits::moon_defaults().set_entries {
            let m = format!("m{i:04}");
            sadd(&mut db, &[bs(b"s"), bs(m.as_bytes())]);
        }
        // Exactly at the threshold it is still a listpack (Redis: 128 is
        // inclusive) — this is the half of the boundary the pre-fix binary
        // gets wrong, so the test cannot pass vacuously.
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "exactly set-max-listpack-entries members must still be a listpack"
        );
        sadd(&mut db, &[bs(b"s"), bs(b"one-more")]);
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "hashtable",
            "past set-max-listpack-entries the set must promote to a hashtable"
        );
        // The promotion must not lose or duplicate a member.
        assert_eq!(
            scard_readonly(&db, &[bs(b"s")], 0),
            Frame::Integer(
                crate::storage::db::EncodingLimits::moon_defaults().set_entries as i64 + 1
            )
        );
        assert_eq!(
            sismember_readonly(&db, &[bs(b"s"), bs(b"m0000")], 0),
            Frame::Integer(1)
        );
        assert_eq!(
            sismember_readonly(&db, &[bs(b"s"), bs(b"one-more")], 0),
            Frame::Integer(1)
        );
    }

    #[test]
    fn sadd_promotes_on_an_oversized_member() {
        let mut db = Database::new();
        let at_limit = vec![b'y'; crate::storage::db::EncodingLimits::moon_defaults().set_value];
        sadd(&mut db, &[bs(b"s"), bs(b"small"), bs(&at_limit)]);
        // Exactly set-max-listpack-value bytes is still a listpack — the
        // pre-fix binary answers `hashtable` here, so this half cannot pass
        // vacuously.
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "a member of exactly set-max-listpack-value bytes fits a listpack"
        );
        let big = vec![b'x'; crate::storage::db::EncodingLimits::moon_defaults().set_value + 1];
        sadd(&mut db, &[bs(b"s"), bs(&big)]);
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "hashtable",
            "a member longer than set-max-listpack-value must promote"
        );
        assert_eq!(scard_readonly(&db, &[bs(b"s")], 0), Frame::Integer(3));
        assert_eq!(
            sismember_readonly(&db, &[bs(b"s"), bs(&big)], 0),
            Frame::Integer(1)
        );
    }

    #[test]
    fn listpack_set_answers_reads_identically() {
        let mut db = Database::new();
        // A mixed bag: `7` is stored as a listpack INTEGER entry and must
        // still answer to its decimal spelling; `007` is a string.
        assert_eq!(
            sadd(
                &mut db,
                &[bs(b"s"), bs(b"a"), bs(b"b"), bs(b"c"), bs(b"7"), bs(b"007")]
            ),
            Frame::Integer(5)
        );
        assert_eq!(encoding_of(&mut db, b"s"), "listpack");
        // The VALUE, not just the encoding: every member present, none
        // invented, through the twin that classifies the listpack form.
        assert_eq!(
            ro_members_sorted(&db, b"s"),
            vec![
                b"007".to_vec(),
                b"7".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec()
            ]
        );
        assert_eq!(scard_readonly(&db, &[bs(b"s")], 0), Frame::Integer(5));
        assert_eq!(
            sismember_readonly(&db, &[bs(b"s"), bs(b"b")], 0),
            Frame::Integer(1)
        );
        assert_eq!(
            sismember_readonly(&db, &[bs(b"s"), bs(b"7")], 0),
            Frame::Integer(1)
        );
        assert_eq!(
            sismember_readonly(&db, &[bs(b"s"), bs(b"z")], 0),
            Frame::Integer(0)
        );
        // A duplicate insert must be rejected WHILE in listpack form — both
        // the string and the integer-encoded member — and must not promote.
        assert_eq!(sadd(&mut db, &[bs(b"s"), bs(b"a")]), Frame::Integer(0));
        assert_eq!(sadd(&mut db, &[bs(b"s"), bs(b"7")]), Frame::Integer(0));
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "a duplicate SADD must neither grow nor promote the set"
        );
        assert_eq!(scard_readonly(&db, &[bs(b"s")], 0), Frame::Integer(5));
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "reads through the shared-read twins must not change the encoding"
        );
    }

    // ── moon#899: the intset -> listpack edge ────────────────────────────
    //
    // Redis's set state machine has three forward edges — intset -> listpack,
    // listpack -> hashtable, intset -> hashtable — and moon had no
    // intset -> listpack. Measured against redis 8.6.1: `SADD s 1 2 3` then
    // `SADD s abc` is `listpack` there and was `hashtable` here. A fixture at
    // 200 ints misses it entirely (200 exceeds the listpack threshold, so
    // hashtable is right on both), which is why every test below sits BELOW
    // the threshold or straddles it.

    /// The edge itself, far below the threshold. Red on f7c83769.
    #[test]
    fn sadd_string_into_small_intset_lands_in_listpack() {
        let mut db = Database::new();
        sadd(&mut db, &[bs(b"s"), bs(b"1"), bs(b"2"), bs(b"3")]);
        assert_eq!(encoding_of(&mut db, b"s"), "intset");
        assert_eq!(sadd(&mut db, &[bs(b"s"), bs(b"abc")]), Frame::Integer(1));
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "a string joining a 3-member intset must land in a listpack (redis 7.2+)"
        );
        assert_eq!(scard_readonly(&db, &[bs(b"s")], 0), Frame::Integer(4));
        assert_eq!(
            ro_members_sorted(&db, b"s"),
            vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec(), b"abc".to_vec()]
        );
        assert_eq!(
            sismember_readonly(&db, &[bs(b"s"), bs(b"2")], 0),
            Frame::Integer(1),
            "an integer member must still answer after the conversion"
        );
        // Integers added afterwards stay in the listpack; the intset path
        // answers `Ok(None)` for a `SetListpack` and falls through.
        assert_eq!(sadd(&mut db, &[bs(b"s"), bs(b"99")]), Frame::Integer(1));
        assert_eq!(encoding_of(&mut db, b"s"), "listpack");
        assert_eq!(
            sismember_readonly(&db, &[bs(b"s"), bs(b"99")], 0),
            Frame::Integer(1)
        );
        // And a duplicate of a converted integer is still a duplicate.
        assert_eq!(sadd(&mut db, &[bs(b"s"), bs(b"3")]), Frame::Integer(0));
        assert_eq!(scard_readonly(&db, &[bs(b"s")], 0), Frame::Integer(5));
    }

    /// Redis's rule is `intsetLen < set-max-listpack-entries`: 127 ints plus
    /// a string (128 members) is a listpack, 128 ints plus a string (129) is
    /// a hashtable. Both halves, so the boundary cannot be off by one in
    /// either direction and pass.
    #[test]
    fn intset_to_listpack_edge_straddles_the_entry_threshold() {
        let limit = crate::storage::db::EncodingLimits::moon_defaults().set_entries;
        for (ints, want) in [(limit - 1, "listpack"), (limit, "hashtable")] {
            let mut db = Database::new();
            for i in 0..ints {
                let m = i.to_string();
                sadd(&mut db, &[bs(b"s"), bs(m.as_bytes())]);
            }
            assert_eq!(encoding_of(&mut db, b"s"), "intset", "fixture: {ints} ints");
            assert_eq!(sadd(&mut db, &[bs(b"s"), bs(b"abc")]), Frame::Integer(1));
            assert_eq!(
                encoding_of(&mut db, b"s"),
                want,
                "{ints} ints + one string: {} members",
                ints + 1
            );
            assert_eq!(
                scard_readonly(&db, &[bs(b"s")], 0),
                Frame::Integer(ints as i64 + 1),
                "no member may be lost across the edge ({ints} ints)"
            );
            assert_eq!(
                sismember_readonly(&db, &[bs(b"s"), bs(b"abc")], 0),
                Frame::Integer(1)
            );
            assert_eq!(
                sismember_readonly(&db, &[bs(b"s"), bs(b"0")], 0),
                Frame::Integer(1)
            );
        }
    }

    /// The value threshold applies to the incoming string too: a 65-byte
    /// member cannot live in a listpack, so the intset goes to a hashtable.
    #[test]
    fn oversized_string_into_small_intset_goes_to_hashtable() {
        let mut db = Database::new();
        sadd(&mut db, &[bs(b"s"), bs(b"1"), bs(b"2"), bs(b"3")]);
        let big = vec![b'x'; crate::storage::db::EncodingLimits::moon_defaults().set_value + 1];
        assert_eq!(sadd(&mut db, &[bs(b"s"), bs(&big)]), Frame::Integer(1));
        assert_eq!(encoding_of(&mut db, b"s"), "hashtable");
        assert_eq!(scard_readonly(&db, &[bs(b"s")], 0), Frame::Integer(4));
    }

    /// moon#795 across the edge. The conversion renders every `i64` in the
    /// intset into the listpack; the bytes that come back must be the bytes
    /// that went in, for the extreme integers as well as the small ones, and
    /// the non-canonical spellings added in the same command must be stored
    /// as the distinct strings they are — `+5` is not `5`.
    #[test]
    fn intset_to_listpack_preserves_every_member_byte_for_byte() {
        let mut db = Database::new();
        let ints: [&[u8]; 5] = [
            b"5",
            b"12345",
            b"-7",
            b"-9223372036854775808",
            b"9223372036854775807",
        ];
        let mut args = vec![bs(b"s")];
        args.extend(ints.iter().map(|m| bs(m)));
        assert_eq!(sadd(&mut db, &args), Frame::Integer(5));
        assert_eq!(encoding_of(&mut db, b"s"), "intset");
        let strings: [&[u8]; 4] = [b"+5", b"000000012345", b"-0", b"abc"];
        let mut args = vec![bs(b"s")];
        args.extend(strings.iter().map(|m| bs(m)));
        assert_eq!(sadd(&mut db, &args), Frame::Integer(4));
        assert_eq!(encoding_of(&mut db, b"s"), "listpack");

        let mut want: Vec<Vec<u8>> = ints
            .iter()
            .chain(strings.iter())
            .map(|m| m.to_vec())
            .collect();
        want.sort();
        assert_eq!(ro_members_sorted(&db, b"s"), want, "byte-exact membership");
        assert_eq!(scard_readonly(&db, &[bs(b"s")], 0), Frame::Integer(9));
        for (probe, want) in [
            (&b"+5"[..], 1),
            (b"5", 1),
            (b"000000012345", 1),
            (b"12345", 1),
            (b"-0", 1),
            (b"0", 0),
            (b"-9223372036854775808", 1),
            (b"7", 0),
        ] {
            assert_eq!(
                sismember_readonly(&db, &[bs(b"s"), bs(probe)], 0),
                Frame::Integer(want),
                "SISMEMBER {:?}",
                String::from_utf8_lossy(probe)
            );
        }
    }

    /// A listpack stores an integer-shaped member in its INTEGER encoding, so
    /// the member's identity survives only if the encode step refuses
    /// non-canonical spellings. `000000012345` re-rendered from an `i64` is
    /// `12345` — a different member. This pins the whole mixed batch
    /// byte-exact: the class recurred once already (moon#802, commit
    /// 038819f3, "stop rewriting numeric strings with leading zeros or '+'"),
    /// and the listpack set path is a NEW caller of that guard.
    #[test]
    fn sadd_listpack_preserves_non_canonical_integer_spellings() {
        let mut db = Database::new();
        // One canonical integer (really stored as a listpack integer), three
        // non-canonical spellings that must stay strings, and a plain string.
        assert_eq!(
            sadd(
                &mut db,
                &[
                    bs(b"s"),
                    bs(b"000000012345"),
                    bs(b"abcdefgh"),
                    bs(b"+5"),
                    bs(b"-0"),
                    bs(b"12345"),
                ]
            ),
            Frame::Integer(5),
            "all five spellings are distinct members"
        );
        assert_eq!(encoding_of(&mut db, b"s"), "listpack");
        assert_eq!(
            ro_members_sorted(&db, b"s"),
            vec![
                b"+5".to_vec(),
                b"-0".to_vec(),
                b"000000012345".to_vec(),
                b"12345".to_vec(),
                b"abcdefgh".to_vec(),
            ],
            "SMEMBERS must return every member byte-for-byte as written"
        );
        // Identity, not just the byte dump: the padded spelling and the
        // canonical one are DIFFERENT members and neither answers for the
        // other.
        for m in [&b"000000012345"[..], b"12345", b"+5", b"-0", b"abcdefgh"] {
            assert_eq!(
                sismember_readonly(&db, &[bs(b"s"), bs(m)], 0),
                Frame::Integer(1),
                "{} must be a member",
                String::from_utf8_lossy(m)
            );
        }
        for m in [&b"5"[..], b"0", b"0000012345"] {
            assert_eq!(
                sismember_readonly(&db, &[bs(b"s"), bs(m)], 0),
                Frame::Integer(0),
                "{} was never added; a re-rendered integer must not answer for it",
                String::from_utf8_lossy(m)
            );
        }
        // A duplicate of the non-canonical spelling is still a duplicate.
        assert_eq!(
            sadd(&mut db, &[bs(b"s"), bs(b"000000012345")]),
            Frame::Integer(0)
        );
        assert_eq!(scard_readonly(&db, &[bs(b"s")], 0), Frame::Integer(5));
    }

    #[test]
    fn sadd_listpack_rejects_a_wrong_type_key() {
        let mut db = Database::new();
        db.set(
            b"str",
            crate::storage::entry::Entry::new_string(Bytes::from_static(b"v")),
        );
        match sadd(&mut db, &[bs(b"str"), bs(b"a")]) {
            Frame::Error(e) => assert!(
                e.starts_with(b"WRONGTYPE"),
                "expected WRONGTYPE, got {:?}",
                String::from_utf8_lossy(&e)
            ),
            other => panic!("expected WRONGTYPE error, got {other:?}"),
        }
    }

    // ── moon#897: SREM must not flatten the container it removes from ────
    //
    // Measured on f7c83769 against a redis 8.6.1 oracle, same host, one
    // shard: a three-member set built one `SADD` at a time reported
    // `listpack` (or `intset`), then ONE `SREM` of one member reported
    // `hashtable` where redis still reported `listpack`/`intset`. Nothing
    // demotes (moon#832), so that promotion is permanent for the key's
    // lifetime — the memory difference the compact encodings exist for is
    // lost on the first update of a session set.
    //
    // The mechanism was `get_or_create_set`, whose `SetKind::upgrade`
    // materialises the full `IndexSet` unconditionally, exactly as
    // `get_promoted` did on the READ path before moon#853.
    //
    // These guards are proven able to FAIL by MUTATION, not by removal:
    // pointing `srem`'s router at the eager arm (making `set_route` answer
    // `SetRoute::Full`) turns every `assert_eq!(…, "listpack"/"intset")`
    // below red while the reply assertions stay green — which is the whole
    // point, since the replies were never wrong.

    #[test]
    fn srem_keeps_a_small_listpack_set_a_listpack() {
        let mut db = Database::new();
        setup_set(&mut db, b"s", &[&b"alpha"[..], b"beta", b"gamma"]);
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "fixture precondition"
        );

        assert_eq!(srem(&mut db, &[bs(b"s"), bs(b"beta")]), Frame::Integer(1));
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "moon#897: SREM flattened a 3-member listpack set; redis 8.6.1 keeps it a listpack"
        );
        assert_eq!(
            ro_members_sorted(&db, b"s"),
            vec![b"alpha".to_vec(), b"gamma".to_vec()]
        );
        // A member that was not there costs nothing and changes nothing.
        assert_eq!(srem(&mut db, &[bs(b"s"), bs(b"absent")]), Frame::Integer(0));
        assert_eq!(encoding_of(&mut db, b"s"), "listpack");
    }

    #[test]
    fn srem_keeps_a_small_intset_an_intset() {
        // The intset source is a DIFFERENT code path from the listpack one
        // (`get_or_create_intset` vs `get_or_create_set_listpack`), and moon
        // got BOTH wrong. Redis answers `intset` here.
        let mut db = Database::new();
        setup_set(&mut db, b"s", &[b"1", b"2", b"3"]);
        assert_eq!(encoding_of(&mut db, b"s"), "intset", "fixture precondition");

        assert_eq!(srem(&mut db, &[bs(b"s"), bs(b"2")]), Frame::Integer(1));
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "intset",
            "moon#897: SREM flattened a 3-member intset; redis 8.6.1 keeps it an intset"
        );
        assert_eq!(
            ro_members_sorted(&db, b"s"),
            vec![b"1".to_vec(), b"3".to_vec()]
        );

        // A non-canonical spelling is not a member of an intset — the same
        // verdict redis's `string2ll` gate reaches — and it must not be an
        // excuse to leave the compact form either.
        assert_eq!(srem(&mut db, &[bs(b"s"), bs(b"+1")]), Frame::Integer(0));
        assert_eq!(srem(&mut db, &[bs(b"s"), bs(b"abc")]), Frame::Integer(0));
        assert_eq!(encoding_of(&mut db, b"s"), "intset");
        assert_eq!(scard(&mut db, &[bs(b"s")]), Frame::Integer(2));
    }

    #[test]
    fn srem_still_promotes_a_container_that_legitimately_exceeds_the_threshold() {
        // The fix must not disable the encoding policy it preserves. A set
        // built past `set-max-listpack-entries` is a hashtable BEFORE the
        // SREM and must still be one after it — and one past
        // `set-max-listpack-value` likewise. Both sides of both boundaries,
        // because a guard that only checks the compact side cannot tell
        // "preserved" from "never promotes".
        let limits = crate::storage::db::EncodingLimits::moon_defaults();
        let n = limits.set_entries;
        for (count, want) in [(n, "listpack"), (n + 1, "hashtable")] {
            let mut db = Database::new();
            let owned: Vec<Vec<u8>> = (0..count)
                .map(|i| format!("m{i:05}").into_bytes())
                .collect();
            for m in &owned {
                sadd(&mut db, &[bs(b"s"), bs(m)]);
            }
            assert_eq!(
                encoding_of(&mut db, b"s"),
                want,
                "fixture at {count} members"
            );
            assert_eq!(srem(&mut db, &[bs(b"s"), bs(&owned[0])]), Frame::Integer(1));
            assert_eq!(
                encoding_of(&mut db, b"s"),
                want,
                "SREM changed the encoding at {count} members"
            );
            assert_eq!(
                scard(&mut db, &[bs(b"s")]),
                Frame::Integer(count as i64 - 1)
            );
        }

        // The intset ceiling, both sides.
        for (count, want) in [
            (limits.set_intset, "intset"),
            (limits.set_intset + 1, "hashtable"),
        ] {
            let mut db = Database::new();
            for i in 0..count {
                sadd(&mut db, &[bs(b"s"), bs(i.to_string().as_bytes())]);
            }
            assert_eq!(
                encoding_of(&mut db, b"s"),
                want,
                "intset fixture at {count}"
            );
            assert_eq!(srem(&mut db, &[bs(b"s"), bs(b"0")]), Frame::Integer(1));
            assert_eq!(
                encoding_of(&mut db, b"s"),
                want,
                "SREM changed the encoding at {count} integer members"
            );
        }

        // An oversized member has no listpack form at all: the set is a
        // hashtable from the SADD that carried it, and SREM of an unrelated
        // member leaves it one.
        let mut db = Database::new();
        let big = vec![b'x'; limits.set_value + 1];
        setup_set(&mut db, b"s", &[b"small"]);
        sadd(&mut db, &[bs(b"s"), bs(&big)]);
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "hashtable",
            "fixture precondition"
        );
        assert_eq!(srem(&mut db, &[bs(b"s"), bs(b"small")]), Frame::Integer(1));
        assert_eq!(encoding_of(&mut db, b"s"), "hashtable");
    }

    #[test]
    fn srem_holds_byte_transparency_for_numeric_looking_members() {
        // moon#795/#903: the compact encodings are NOT byte-transparent for
        // numeric-looking strings unless every value entering one went
        // through `numeric::canonical_i64`. `+5`, `000000012345` and `-0`
        // must come back out of a listpack with their bytes intact after a
        // SREM of an unrelated member — this regression class was found and
        // fixed twice in one day, so it gets its own guard.
        let mut db = Database::new();
        setup_set(
            &mut db,
            b"s",
            &[&b"+5"[..], b"000000012345", b"-0", b"victim"],
        );
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "fixture precondition"
        );

        assert_eq!(srem(&mut db, &[bs(b"s"), bs(b"victim")]), Frame::Integer(1));
        assert_eq!(encoding_of(&mut db, b"s"), "listpack");
        assert_eq!(
            ro_members_sorted(&db, b"s"),
            vec![b"+5".to_vec(), b"-0".to_vec(), b"000000012345".to_vec()],
            "a member's bytes changed across SREM"
        );
        // And the canonical spellings are still NOT members, so nothing was
        // silently re-parsed on the way through.
        for canonical in [&b"5"[..], &b"12345"[..], &b"0"[..]] {
            assert_eq!(
                sismember_readonly(&db, &[bs(b"s"), bs(canonical)], 0),
                Frame::Integer(0),
                "{} answered as a member",
                String::from_utf8_lossy(canonical)
            );
        }
    }

    #[test]
    fn srem_deletes_the_key_when_the_last_member_goes_from_either_compact_form() {
        // Delete-when-empty is the semantics half of the fix: the in-place
        // arms own the cleanup that `get_or_create_set` + `get_set` used to
        // do, and `get_set` is `get_promoted` — so the emptiness PROBE was
        // itself a flattener (moon#832).
        for members in [&[&b"only"[..]][..], &[&b"7"[..]][..]] {
            let mut db = Database::new();
            setup_set(&mut db, b"s", members);
            assert_eq!(
                srem(&mut db, &[bs(b"s"), bs(members[0])]),
                Frame::Integer(1)
            );
            assert_eq!(
                encoding_of(&mut db, b"s"),
                "<missing>",
                "the key survived an SREM that emptied it"
            );
        }
    }

    #[test]
    fn srem_semantics_are_unchanged_by_the_routing() {
        // The routing must be invisible to every non-encoding answer: the
        // integer reply, the missing-key answer, WRONGTYPE, and arity.
        let mut db = Database::new();
        assert_eq!(srem(&mut db, &[bs(b"nokey"), bs(b"m")]), Frame::Integer(0));
        assert_eq!(
            encoding_of(&mut db, b"nokey"),
            "<missing>",
            "SREM left a key behind for a set that never existed"
        );

        setup_set(&mut db, b"s", &[b"a", b"b", b"c", b"d", b"e"]);
        assert_eq!(
            srem(
                &mut db,
                &[bs(b"s"), bs(b"a"), bs(b"c"), bs(b"zzz"), bs(b"e")]
            ),
            Frame::Integer(3),
            "multi-member SREM must count only the members that were present"
        );
        assert_eq!(encoding_of(&mut db, b"s"), "listpack");

        // Duplicate members in one call count once, as in redis.
        let mut db = Database::new();
        setup_set(&mut db, b"s", &[b"a", b"b"]);
        assert_eq!(
            srem(&mut db, &[bs(b"s"), bs(b"a"), bs(b"a")]),
            Frame::Integer(1)
        );

        // WRONGTYPE, decided by the `&self` router before any mutation.
        let mut db = Database::new();
        crate::command::string::set(&mut db, &[bs(b"str"), bs(b"v")]);
        match srem(&mut db, &[bs(b"str"), bs(b"m")]) {
            Frame::Error(e) => assert!(
                e.starts_with(b"WRONGTYPE"),
                "expected WRONGTYPE, got {:?}",
                String::from_utf8_lossy(&e)
            ),
            other => panic!("expected WRONGTYPE error, got {other:?}"),
        }

        // Arity.
        let mut db = Database::new();
        match srem(&mut db, &[bs(b"s")]) {
            Frame::Error(e) => assert!(e.starts_with(b"ERR wrong number of arguments")),
            other => panic!("expected arity error, got {other:?}"),
        }
    }

    /// The post-removal upgrade check is NOT decoration.
    ///
    /// A removal only shrinks, so a listpack `SADD` produced can never be over
    /// the policy when `SREM` runs — which would make
    /// `!limits.listpack_fits(..)` unreachable, and an unreachable guard is a
    /// guard nobody can prove. `Database::set_encoding_limits` makes it
    /// reachable, and its own doc comment says why the case is real: "Applies
    /// to writes from this point on; an existing container is not re-encoded."
    /// So a container built under a loose policy and then written under a
    /// tightened one is exactly the state this check exists to converge.
    ///
    /// Mutating the check to `let should_upgrade = false;` turns this red and
    /// leaves every other SREM test green.
    #[test]
    fn srem_promotes_a_listpack_that_a_tightened_policy_no_longer_fits() {
        let mut db = Database::new();
        let loose = crate::storage::db::EncodingLimits::moon_defaults();
        for i in 0..20u32 {
            sadd(&mut db, &[bs(b"s"), bs(format!("m{i:03}").as_bytes())]);
        }
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "fixture precondition"
        );

        // Tighten the policy under the live container. It is NOT re-encoded
        // by the setter — that is the documented contract.
        db.set_encoding_limits(crate::storage::db::EncodingLimits {
            set_entries: 4,
            ..loose
        });
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "listpack",
            "the setter must not re-encode an existing container"
        );

        // The next write converges it, because the check consults the
        // authority rather than assuming a shrink is always safe.
        assert_eq!(srem(&mut db, &[bs(b"s"), bs(b"m000")]), Frame::Integer(1));
        assert_eq!(
            encoding_of(&mut db, b"s"),
            "hashtable",
            "SREM left a 19-member listpack under a 4-member policy"
        );
        assert_eq!(scard(&mut db, &[bs(b"s")]), Frame::Integer(19));

        // And once the container genuinely fits again, nothing re-promotes:
        // the check is a threshold test, not an unconditional upgrade.
        let mut db = Database::new();
        db.set_encoding_limits(crate::storage::db::EncodingLimits {
            set_entries: 4,
            ..loose
        });
        setup_set(&mut db, b"t", &[b"a", b"b", b"c"]);
        assert_eq!(encoding_of(&mut db, b"t"), "listpack");
        assert_eq!(srem(&mut db, &[bs(b"t"), bs(b"a")]), Frame::Integer(1));
        assert_eq!(encoding_of(&mut db, b"t"), "listpack");
    }
}

#[cfg(test)]
mod sadd_listpack_batch_tests {
    use super::*;
    use crate::storage::Database;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    /// moon#865, the set arm. Before the batch guard this branch pushed every
    /// member into the listpack and only then compared the entry count with
    /// set-max-listpack-entries, so a single large SADD wrapped the header's u16
    /// counter: `SADD` replied 70000 and `SCARD` replied 4464 (= 70000 -
    /// 65536), with the write already acknowledged.
    #[test]
    fn sadd_one_call_past_u16_keeps_every_member() {
        let mut db = Database::new();
        const N: usize = 70_000;
        let owned: Vec<Vec<u8>> = (0..N).map(|i| format!("m{i:07}").into_bytes()).collect();
        let mut args: Vec<Frame> = Vec::with_capacity(N + 1);
        args.push(bs(b"big"));
        args.extend(owned.iter().map(|m| bs(m)));

        assert_eq!(
            sadd(&mut db, &args),
            Frame::Integer(N as i64),
            "SADD under-reported the members it accepted"
        );
        assert_eq!(
            scard(&mut db, &[bs(b"big")]),
            Frame::Integer(N as i64),
            "SCARD lost members to the u16 wrap"
        );
        assert_eq!(
            sismember(&mut db, &[bs(b"big"), bs(&owned[N - 1])]),
            Frame::Integer(1),
            "last member unreachable"
        );
    }

    /// The guard must not disable the encoding it protects: a batch that
    /// belongs in a listpack must still land in one, and repeated small calls
    /// must still accumulate correctly past the ceiling.
    #[test]
    fn small_batches_still_reach_the_listpack_and_accumulate() {
        for n in [1usize, 8, 128, 129] {
            let mut db = Database::new();
            let owned: Vec<Vec<u8>> = (0..n).map(|i| format!("m{i:05}").into_bytes()).collect();
            let mut args: Vec<Frame> = Vec::with_capacity(n + 1);
            args.push(bs(b"s"));
            args.extend(owned.iter().map(|m| bs(m)));
            assert_eq!(sadd(&mut db, &args), Frame::Integer(n as i64), "SADD n={n}");
            assert_eq!(
                scard(&mut db, &[bs(b"s")]),
                Frame::Integer(n as i64),
                "SCARD n={n}"
            );
        }

        let mut db = Database::new();
        for c in 0..700 {
            let owned: Vec<Vec<u8>> = (0..100)
                .map(|i| format!("m{:07}", c * 100 + i).into_bytes())
                .collect();
            let mut args: Vec<Frame> = Vec::with_capacity(101);
            args.push(bs(b"acc"));
            args.extend(owned.iter().map(|m| bs(m)));
            sadd(&mut db, &args);
        }
        assert_eq!(scard(&mut db, &[bs(b"acc")]), Frame::Integer(70_000));
    }
}

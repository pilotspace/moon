#![no_main]
//! Values put into a listpack must come back byte-identical.
//!
//! moon#795: `try_encode_as_integer` used a bare `parse::<i64>()`, so any
//! numeric string that is not the canonical rendering of its value --
//! `000000012345`, `+5`, `-0` -- was stored as the parsed integer and read
//! back re-rendered. `SADD s 000000012345` then `SMEMBERS` returned `12345`.
//! Silent data loss for zero-padded IDs, account numbers, zip codes.
//!
//! The invariant is round-trip identity, which holds for every input and needs
//! no oracle: whatever bytes go in must come out.
//!
//! moon#799 extended this to the PAIR operations. `locate_pair` walks
//! field/value pairs by byte offset instead of by ordinal, and
//! `replace_pair_value` / `remove_pair` / `take_pair_value` / `pair_value`
//! splice and drain at those offsets. Their oracle is the find-then-index
//! pair they replaced, which is still present: for every input, the one-scan
//! operation must produce byte-identical results to the two-scan one.

use libfuzzer_sys::fuzz_target;
use moon::storage::listpack::Listpack;

fuzz_target!(|data: &[u8]| {
    // Split the input into elements on a rare byte so the fuzzer can build
    // multi-element listpacks and exercise indexing, not just a single entry.
    let elements: Vec<&[u8]> = data.split(|b| *b == 0xFF).take(64).collect();

    let mut lp = Listpack::new();
    let mut pushed: Vec<&[u8]> = Vec::with_capacity(elements.len());
    for e in elements {
        // Listpack is a *compact* encoding with size limits; skip anything the
        // caller would not have routed here in the first place.
        if e.len() > 64 {
            continue;
        }
        lp.push_back(e);
        pushed.push(e);
    }

    assert_eq!(lp.len(), pushed.len(), "listpack length diverged from input count");

    for (i, want) in pushed.iter().enumerate() {
        let got = lp.get_at(i).expect("entry must be present").to_bytes();
        assert_eq!(
            got.as_ref(),
            *want,
            "listpack rewrote element {i}: in {want:?} out {:?}",
            got.as_ref()
        );

        // Lookup must agree with storage: an element that was stored must be
        // findable by its own exact bytes.
        assert!(
            lp.find(want).is_some(),
            "stored element {i} ({want:?}) not findable by its own bytes"
        );
    }

    // --- moon#799: the one-scan pair ops against their two-scan oracle ---
    //
    // Read the same listpack as field/value pairs. Only complete pairs count,
    // and a FIELD is an even-indexed entry, which is exactly the distinction
    // `locate_pair` has to get right.
    for (pair_idx, chunk) in pushed.chunks_exact(2).enumerate() {
        let field = chunk[0];
        // Duplicate fields are legal in this fixture; every operation must
        // agree on the FIRST match, so compare against the oracle rather than
        // against `pair_idx`.
        let Some(oracle_idx) = lp.find_pair_index(field) else {
            panic!("pair {pair_idx}: stored field {field:?} not found by find_pair_index");
        };

        // pair_value == find_pair_index + get_at
        let want = lp
            .get_at(oracle_idx * 2 + 1)
            .expect("value entry must be present")
            .as_bytes();
        assert_eq!(
            lp.pair_value(field).expect("pair_value must find it").to_vec(),
            want,
            "pair {pair_idx}: pair_value disagreed with find_pair_index + get_at"
        );

        // replace_pair_value == find_pair_index + replace_at
        let mut got = lp.clone();
        assert!(got.replace_pair_value(field, b"REPLACEMENT"));
        let mut oracle = lp.clone();
        oracle.replace_at(oracle_idx * 2 + 1, b"REPLACEMENT");
        assert_eq!(got.len(), oracle.len(), "pair {pair_idx}: replace changed the count");
        assert_eq!(
            got.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
            oracle.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
            "pair {pair_idx}: replace_pair_value diverged from replace_at"
        );

        // remove_pair == find_pair_index + remove_at x2
        let mut got = lp.clone();
        assert!(got.remove_pair(field));
        let mut oracle = lp.clone();
        oracle.remove_at(oracle_idx * 2 + 1);
        oracle.remove_at(oracle_idx * 2);
        assert_eq!(got.len(), oracle.len(), "pair {pair_idx}: remove changed the count");
        assert_eq!(
            got.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
            oracle.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
            "pair {pair_idx}: remove_pair diverged from remove_at x2"
        );

        // take_pair_value returns the value AND leaves what remove_pair leaves
        let mut got = lp.clone();
        let taken = got.take_pair_value(field).expect("take_pair_value must find it");
        assert_eq!(taken.as_ref(), want.as_slice(), "pair {pair_idx}: wrong value taken");
        assert_eq!(
            got.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
            oracle.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
            "pair {pair_idx}: take_pair_value left a different listpack"
        );
    }
});

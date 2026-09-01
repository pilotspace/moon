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
});

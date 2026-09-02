#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::storage::listpack::Listpack;

/// Differential fuzz: the owning decoder vs the borrowing decoder.
///
/// `decode_entry_at` (yielding an owned `ListpackEntry`) and
/// `decode_entry_ref_at` (yielding a borrowed `ListpackRef`) are two
/// hand-written walks over the same byte format. The borrowing one exists so
/// lookups do not allocate per element, and the hot paths -- HSET, HMSET,
/// HGET, ZSCORE, the hash-TTL probes -- read through it. If the two ever
/// disagree, those paths silently answer a different question than the
/// allocating scan they replaced.
///
/// Invariant, needing no oracle: for any listpack this module can BUILD, the
/// two decoders must yield the same number of entries, the same bytes for
/// each, and the same answers from the lookup helpers.
fuzz_target!(|data: &[u8]| {
    // Split the input into elements on 0xFF (the listpack terminator byte, so
    // it can never appear inside a well-formed element payload here).
    let mut lp = Listpack::new();
    let mut elements: Vec<Vec<u8>> = Vec::new();
    for chunk in data.split(|&b| b == 0xFF) {
        // Keep elements inside the 12-bit string encoding the builder emits;
        // longer ones are exercised by the unit tests, not worth the fuzz time.
        if chunk.len() > 4096 {
            continue;
        }
        lp.push_back(chunk);
        elements.push(chunk.to_vec());
    }
    if elements.is_empty() {
        return;
    }

    // 1. Entry-for-entry agreement between the two decoders.
    let owned: Vec<Vec<u8>> = lp.iter().map(|e| e.as_bytes()).collect();
    let borrowed: Vec<Vec<u8>> = lp.iter_refs().map(|e| e.to_vec()).collect();
    assert_eq!(
        owned.len(),
        borrowed.len(),
        "decoder disagreement: entry COUNT differs ({} owned vs {} borrowed)",
        owned.len(),
        borrowed.len()
    );
    assert_eq!(
        owned, borrowed,
        "decoder disagreement: entry BYTES differ between iter() and iter_refs()"
    );

    // 2. `contains_element` must agree with the allocating scan it replaced.
    for e in &elements {
        let old = lp.iter().any(|x| x.as_bytes() == *e);
        let new = lp.contains_element(e);
        assert_eq!(
            old, new,
            "contains_element disagreed with the owning scan for {e:?}"
        );
    }

    // 3. `find_pair_index` must agree with the allocating pair scan it
    //    replaced, including on the odd-length case where the final field has
    //    no value and therefore forms no pair.
    for e in &elements {
        let old = lp
            .iter_pairs()
            .position(|(f, _v)| f.as_bytes() == *e);
        let new = lp.find_pair_index(e);
        assert_eq!(
            old, new,
            "find_pair_index disagreed with the owning pair scan for {e:?}"
        );
    }
});

//! moon#799: the listpack index walk moved onto the BORROWED decoder
//! (`seek_to`). These check that it still lands on the same entry as the
//! owned walk did, across every encoding the format has, and that it does not
//! truncate a container far larger than the promotion threshold.
//!
//! They live out of tree only because `src/storage/listpack.rs` is at
//! CLAUDE.md's 1500-line ceiling and these need nothing private. The
//! in-module `seek_to_agrees_with_the_owned_walk_on_every_encoding` covers
//! the part that does.

use moon::storage::listpack::{Listpack, ListpackEntry, ListpackRef};

/// One value per encoding arm the decoder distinguishes, plus both sides of
/// every width boundary. A seek that gets any single arm's width wrong lands
/// in the middle of the next entry, so ordering them together is the point.
fn all_encoding_widths() -> Vec<Vec<u8>> {
    vec![
        b"0".to_vec(),                    // 7-bit uint
        b"127".to_vec(),                  // 7-bit uint, top
        b"-1".to_vec(),                   // 13-bit int
        b"4095".to_vec(),                 // 13-bit int, top
        b"32767".to_vec(),                // 16-bit int
        b"8388607".to_vec(),              // 24-bit int
        b"2147483647".to_vec(),           // 32-bit int
        b"9223372036854775807".to_vec(),  // 64-bit int
        b"-9223372036854775808".to_vec(), // 64-bit int, bottom
        b"".to_vec(),                     // 6-bit string, empty
        vec![b'x'; 63],                   // 6-bit string, top
        vec![b'y'; 64],                   // 12-bit string
        vec![b'z'; 4095],                 // 12-bit string, top
        vec![b'w'; 4096],                 // 32-bit string
    ]
}

/// `replace_at` / `remove_at` no longer decode what they step over. They must
/// still land on the same entry, across every encoding width and at every
/// index -- including when the replacement changes the entry's encoded WIDTH,
/// which shifts everything after it.
#[test]
fn replace_and_remove_reach_the_same_entry_on_every_encoding() {
    let inputs = all_encoding_widths();

    for target in 0..inputs.len() {
        let mut lp = Listpack::new();
        for v in &inputs {
            lp.push_back(v);
        }
        // A replacement whose encoding width differs from most of the
        // originals, so a mis-seek corrupts the tail visibly.
        lp.replace_at(target, b"REPLACED-with-a-longer-string");
        assert_eq!(lp.len(), inputs.len(), "replace_at changed the count");
        for (i, want) in inputs.iter().enumerate() {
            let got = lp.get_at(i).expect("entry present").as_bytes();
            if i == target {
                assert_eq!(got, b"REPLACED-with-a-longer-string");
            } else {
                assert_eq!(&got, want, "replace_at({target}) corrupted entry {i}");
            }
        }

        let mut lp = Listpack::new();
        for v in &inputs {
            lp.push_back(v);
        }
        assert!(lp.remove_at(target), "remove_at({target}) found nothing");
        assert_eq!(lp.len(), inputs.len() - 1);
        let want: Vec<&Vec<u8>> = inputs
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != target)
            .map(|(_, v)| v)
            .collect();
        for (i, w) in want.iter().enumerate() {
            assert_eq!(
                &lp.get_at(i).expect("entry present").as_bytes(),
                *w,
                "remove_at({target}) corrupted entry {i}"
            );
        }
    }
}

/// moon#866 class: a listpack far larger than the 128-entry promotion
/// threshold must survive a seek-driven mutation with every entry intact.
/// The seek rewrite touches entry-boundary arithmetic, which is exactly what
/// lost 65,536 elements last time.
#[test]
fn large_listpack_survives_seek_driven_mutation() {
    const N: usize = 4096; // 32x the promotion threshold
    let mut lp = Listpack::new();
    for i in 0..N {
        lp.push_back(format!("field:{i:08}").as_bytes());
        lp.push_back(format!("value-{i:08}").as_bytes());
    }
    assert_eq!(lp.len(), N * 2);

    // Mutate at the head, in the middle and at the tail.
    for &pair in &[0usize, N / 2, N - 1] {
        lp.replace_at(pair * 2 + 1, b"REPLACED");
    }
    assert_eq!(lp.len(), N * 2, "replace_at changed the element count");

    // Verify with ONE linear pass, not `get_at` per index: `get_at` is O(n),
    // so a per-index check would be O(n^2) on 8,192 entries.
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = lp
        .iter_pair_refs()
        .map(|(f, v)| (f.to_vec(), v.to_vec()))
        .collect();
    assert_eq!(pairs.len(), N, "pair count changed");
    for (i, (f, v)) in pairs.iter().enumerate() {
        assert_eq!(*f, format!("field:{i:08}").into_bytes(), "field {i} lost");
        let want = if i == 0 || i == N / 2 || i == N - 1 {
            b"REPLACED".to_vec()
        } else {
            format!("value-{i:08}").into_bytes()
        };
        assert_eq!(*v, want, "value {i} lost");
    }

    // Removing the last pair must not disturb the 4,095 before it.
    assert!(lp.remove_at((N - 1) * 2 + 1));
    assert!(lp.remove_at((N - 1) * 2));
    assert_eq!(lp.len(), (N - 1) * 2);
    assert_eq!(
        lp.get_at((N - 2) * 2).expect("field present").as_bytes(),
        format!("field:{:08}", N - 2).into_bytes()
    );
    assert_eq!(lp.get_at((N - 1) * 2), None, "tail pair still reachable");
}

/// The borrowed view must agree with the owning decoder on every entry,
/// for both the integer and the string encodings. If these ever diverge,
/// the zero-alloc scan would silently answer a different question than the
/// allocating one it replaces.
#[test]
fn refs_agree_with_owned_entries() {
    let mut lp = Listpack::new();
    // Cover every encoding width the decoder distinguishes.
    let inputs: Vec<Vec<u8>> = vec![
        b"0".to_vec(),
        b"127".to_vec(),
        b"-1".to_vec(),
        b"4095".to_vec(),
        b"-4096".to_vec(),
        b"32767".to_vec(),
        b"-32768".to_vec(),
        b"8388607".to_vec(),
        b"2147483647".to_vec(),
        b"9223372036854775807".to_vec(),
        b"-9223372036854775808".to_vec(),
        b"".to_vec(),
        b"short".to_vec(),
        // NOTE: non-canonical integer spellings (`0123`, `+5`) are
        // deliberately absent. On this branch the ENCODER still folds them
        // to `123` / `5`, losing the original bytes -- that is bug #795,
        // fixed separately in `try_encode_as_integer`. Asserting the
        // correct round trip here would fail for a defect this change does
        // not own. `find_pair_index_handles_integer_encoded_fields` still
        // covers the LOOKUP side, which is what this change is responsible
        // for: a non-canonical query must not match a canonical entry.
        vec![b'x'; 63],   // 6-bit string boundary
        vec![b'y'; 64],   // just past it
        vec![b'z'; 4095], // 12-bit string boundary
    ];
    for v in &inputs {
        lp.push_back(v);
    }

    let owned: Vec<ListpackEntry> = lp.iter().collect();
    let borrowed: Vec<ListpackRef<'_>> = lp.iter_refs().collect();
    assert_eq!(owned.len(), inputs.len(), "iter() lost entries");
    assert_eq!(borrowed.len(), inputs.len(), "iter_refs() lost entries");

    for (i, (o, b)) in owned.iter().zip(borrowed.iter()).enumerate() {
        assert_eq!(
            o.as_bytes(),
            b.to_vec(),
            "entry {i} decoded differently by iter_refs()"
        );
        // The comparison helper is the thing the hot paths actually call.
        assert!(
            b.eq_bytes(&inputs[i]),
            "entry {i} ({:?}) failed eq_bytes against its own input",
            inputs[i]
        );
        assert!(
            !b.eq_bytes(b"\xffdefinitely-not-this"),
            "entry {i} matched a value it does not hold"
        );
    }
}

/// The one-scan pair operations (moon#799 commit 2) must be exactly
/// equivalent to the find-then-index pair they replace, INCLUDING when the
/// field or value takes an integer encoding, when the value's replacement
/// changes width, and at the head, middle and tail of the listpack.
#[test]
fn one_scan_pair_ops_match_find_then_index() {
    // Fields and values spanning integer and string encodings, plus a value
    // that collides with a later FIELD name so a value-position match would
    // be caught.
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (b"alpha".to_vec(), b"gamma".to_vec()),
        (b"7".to_vec(), b"-9223372036854775808".to_vec()),
        (b"gamma".to_vec(), b"".to_vec()),
        (b"".to_vec(), b"empty-field-value".to_vec()),
        (vec![b'k'; 200], vec![b'v'; 5000]),
        (b"omega".to_vec(), b"127".to_vec()),
    ];
    let build = || {
        let mut lp = Listpack::new();
        for (f, v) in &pairs {
            lp.push_back(f);
            lp.push_back(v);
        }
        lp
    };

    for (i, (field, value)) in pairs.iter().enumerate() {
        // --- pair_value == find_pair_index + get_at ---
        let lp = build();
        let oracle = lp
            .find_pair_index(field)
            .and_then(|idx| lp.get_at(idx * 2 + 1))
            .map(|e| e.as_bytes());
        assert_eq!(
            lp.pair_value(field).map(|v| v.to_vec()),
            oracle,
            "pair_value disagreed at pair {i}"
        );
        assert_eq!(oracle.as_deref(), Some(value.as_slice()));

        // --- replace_pair_value == find_pair_index + replace_at ---
        let mut got = build();
        assert!(got.replace_pair_value(field, b"NEW-VALUE-of-a-different-width"));
        let mut want = build();
        let idx = want.find_pair_index(field).expect("field present");
        want.replace_at(idx * 2 + 1, b"NEW-VALUE-of-a-different-width");
        assert_eq!(got.len(), want.len(), "element count diverged at pair {i}");
        assert_eq!(
            got.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
            want.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
            "replace_pair_value diverged at pair {i}"
        );

        // --- remove_pair == find_pair_index + remove_at x2 ---
        let mut got = build();
        assert!(got.remove_pair(field));
        let mut want = build();
        let idx = want.find_pair_index(field).expect("field present");
        want.remove_at(idx * 2 + 1);
        want.remove_at(idx * 2);
        assert_eq!(got.len(), want.len(), "element count diverged at pair {i}");
        assert_eq!(
            got.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
            want.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
            "remove_pair diverged at pair {i}"
        );

        // --- take_pair_value returns the value AND removes the pair ---
        let mut got = build();
        let taken = got.take_pair_value(field).expect("field present");
        assert_eq!(taken.as_ref(), value.as_slice(), "wrong value at pair {i}");
        assert_eq!(
            got.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
            want.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
            "take_pair_value left a different listpack at pair {i}"
        );
    }

    // --- absent field: every op is a no-op reporting failure ---
    let mut lp = build();
    let before: Vec<Vec<u8>> = lp.iter().map(|e| e.as_bytes()).collect();
    assert_eq!(lp.pair_value(b"nope").map(|v| v.to_vec()), None);
    assert!(!lp.replace_pair_value(b"nope", b"x"));
    assert!(!lp.remove_pair(b"nope"));
    assert_eq!(lp.take_pair_value(b"nope"), None);
    assert_eq!(
        lp.iter().map(|e| e.as_bytes()).collect::<Vec<_>>(),
        before,
        "a miss must not modify the listpack"
    );

    // --- a VALUE that equals a FIELD name must never match on the value slot ---
    let lp = build();
    assert_eq!(
        lp.pair_value(b"gamma").map(|v| v.to_vec()),
        Some(Vec::new())
    );
}

/// A field/value listpack far above the promotion threshold must survive the
/// one-scan pair ops with every other pair intact (the moon#866 class again,
/// now for the byte-span path rather than the index path).
#[test]
fn large_listpack_survives_one_scan_pair_ops() {
    const N: usize = 2048; // 16x the promotion threshold
    let build = || {
        let mut lp = Listpack::new();
        for i in 0..N {
            lp.push_back(format!("field:{i:08}").as_bytes());
            lp.push_back(format!("value-{i:08}").as_bytes());
        }
        lp
    };

    let mut lp = build();
    for &i in &[0usize, N / 2, N - 1] {
        assert!(
            lp.replace_pair_value(format!("field:{i:08}").as_bytes(), b"REPLACED"),
            "field {i} not found"
        );
    }
    assert_eq!(lp.len(), N * 2, "replace_pair_value changed the count");
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = lp
        .iter_pair_refs()
        .map(|(f, v)| (f.to_vec(), v.to_vec()))
        .collect();
    assert_eq!(pairs.len(), N);
    for (i, (f, v)) in pairs.iter().enumerate() {
        assert_eq!(*f, format!("field:{i:08}").into_bytes(), "field {i} lost");
        let want = if i == 0 || i == N / 2 || i == N - 1 {
            b"REPLACED".to_vec()
        } else {
            format!("value-{i:08}").into_bytes()
        };
        assert_eq!(*v, want, "value {i} lost");
    }

    // Removing three pairs must leave exactly N-3, with nothing else touched.
    let mut lp = build();
    for &i in &[0usize, N / 2, N - 1] {
        assert!(lp.remove_pair(format!("field:{i:08}").as_bytes()));
    }
    assert_eq!(lp.len(), (N - 3) * 2, "element count wrong after removals");
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = lp
        .iter_pair_refs()
        .map(|(f, v)| (f.to_vec(), v.to_vec()))
        .collect();
    let want: Vec<usize> = (0..N)
        .filter(|i| *i != 0 && *i != N / 2 && *i != N - 1)
        .collect();
    assert_eq!(pairs.len(), want.len());
    for (got, &i) in pairs.iter().zip(want.iter()) {
        assert_eq!(
            got.0,
            format!("field:{i:08}").into_bytes(),
            "field {i} lost"
        );
        assert_eq!(
            got.1,
            format!("value-{i:08}").into_bytes(),
            "value {i} lost"
        );
    }
}

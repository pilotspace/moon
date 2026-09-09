//! moon#799: the listpack index walk moved onto the BORROWED decoder
//! (`seek_to`). These check that it still lands on the same entry as the
//! owned walk did, across every encoding the format has, and that it does not
//! truncate a container far larger than the promotion threshold.
//!
//! They live out of tree only because `src/storage/listpack.rs` is at
//! CLAUDE.md's 1500-line ceiling and these need nothing private. The
//! in-module `seek_to_agrees_with_the_owned_walk_on_every_encoding` covers
//! the part that does.

use moon::storage::listpack::Listpack;

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

#![no_main]
use libfuzzer_sys::fuzz_target;

use bytes::Bytes;
use moon::text::postings_persist::{
    DOC_ID_SLACK, PersistedTextIndex, decode, encode_index, trailer_checksum,
};
use moon::text::store::TextIndex;
use moon::text::types::{BM25Config, NumericFieldDef, TagFieldDef, TextFieldDef};

/// Fuzz the `.tpost` text-postings decoder (`docs/internal/text-postings-persistence.md`)
/// AND the install of whatever it accepts.
///
/// Exercises header/version/length framing, the trailer checksum, every
/// per-section count (docs, terms, postings, positions, TAG/NUMERIC
/// entries) and the structural invariants the store relies on. Any panic,
/// OOB access or unbounded allocation is a bug: a torn, bit-rotted or
/// hostile file must always fail closed with `Err`, never panic, and
/// never hand back a partially populated index.
///
/// Most random inputs die at the checksum, so the target also re-stamps
/// the trailer over the input to reach the structural checks — that is the
/// half a corrupt-but-checksummed file (a buggy writer) would hit.
///
/// Every file `decode` accepts is then installed into an empty index whose
/// schema is built to match it (moon#1221 review: install sized the dense
/// per-document columns by the largest doc id, so a 107-byte file billed
/// 72 MB). Install must stay O(file size), and the installed index must
/// re-encode to a file `decode` accepts and hand a new document the
/// smallest free doc id.
fuzz_target!(|data: &[u8]| {
    if let Ok(p) = decode(data) {
        install(p);
    }
    if data.len() >= 32 {
        let mut restamped = data.to_vec();
        let n = restamped.len();
        // Make the header claim the real payload length so the length
        // check passes and the checksum is recomputed over the body.
        let payload_len = (n - 24 - 8) as u64;
        restamped[16..24].copy_from_slice(&payload_len.to_le_bytes());
        let sum = trailer_checksum(&restamped[..n - 8]);
        restamped[n - 8..].copy_from_slice(&sum.to_le_bytes());
        if let Ok(p) = decode(&restamped) {
            install(p);
        }
    }
});

/// Install `p` into an empty index with a schema that matches it by construction (the TEXT
/// field count and the TAG/NUMERIC field names it mentions; the schema hash re-stamped), so the
/// install path — not only the decoder — sees every accepted input.
fn install(mut p: PersistedTextIndex) {
    // Rows cost 4 B per TEXT field per doc id; `decode` alone covers wider files.
    if p.fields.len() > 64 {
        return;
    }
    let text: Vec<TextFieldDef> = (0..p.fields.len())
        .map(|i| TextFieldDef::new(Bytes::from(format!("text{i}"))))
        .collect();
    let mut tags: Vec<Bytes> = p
        .tag_docs
        .iter()
        .flat_map(|(_, entries)| entries.iter().map(|(field, _)| field.clone()))
        .collect();
    tags.sort();
    tags.dedup();
    let mut nums: Vec<Bytes> = p
        .numeric_docs
        .iter()
        .flat_map(|(_, entries)| entries.iter().map(|(field, _)| field.clone()))
        .collect();
    nums.sort();
    nums.dedup();
    let mut idx = TextIndex::new_with_schema(
        p.name.clone(),
        Vec::new(),
        text,
        tags.into_iter().map(TagFieldDef::new).collect(),
        nums.into_iter().map(NumericFieldDef::new).collect(),
        BM25Config::default(),
    );
    idx.db_index = p.db_index;
    p.schema_hash = idx.schema_hash();
    let docs = p.docs.len();
    if idx.install_recovered(p).is_err() {
        return;
    }

    // O(file size): at most 2 slots per document plus the fixed allowance (×2 Vec growth).
    let max_slots = 2 * docs + DOC_ID_SLACK as usize;
    assert!(idx.next_doc_id() as usize <= max_slots);
    assert!(
        idx.doc_id_to_key.footprint()
            <= 2 * max_slots.max(4) * std::mem::size_of::<Option<Bytes>>()
    );
    assert_eq!(
        idx.free_doc_ids().len() + idx.doc_id_to_key.len() as u64,
        u64::from(idx.next_doc_id())
    );
    let _ = idx.resident_bytes();

    // Query every field, then the round trip the periodic flush performs.
    let probe = vec!["a".to_owned()];
    for f in 0..idx.text_fields.len() {
        let _ = idx.search_field(f, &probe, None, None, 10);
    }
    assert!(
        decode(&encode_index(&idx)).is_ok(),
        "an installed index must re-encode to a file decode accepts"
    );

    // A new document takes the smallest free id; removing it restores the free set. (A fixed
    // key hash: removal copes with a hash other than xxh64(key).)
    const KEY_HASH: u64 = 0x1221_f00d_1221_f00d;
    if idx.key_hash_to_doc_id.contains_key(&KEY_HASH) {
        return;
    }
    let free_before = idx.free_doc_ids().clone();
    let next_before = idx.next_doc_id();
    let want = free_before.min().unwrap_or(next_before);
    idx.index_document(KEY_HASH, b"\xfffuzz-new-doc", &[]);
    assert_eq!(idx.key_hash_to_doc_id.get(&KEY_HASH), Some(&want));
    idx.remove_doc_by_doc_id(want);
    assert_eq!(idx.free_doc_ids(), &free_before);
    assert_eq!(idx.next_doc_id(), next_before);
    assert!(!idx.key_hash_to_doc_id.contains_key(&KEY_HASH));
}

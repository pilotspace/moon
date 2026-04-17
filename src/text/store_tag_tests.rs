//! Plan 152-06 TAG storage unit tests.
//!
//! Kept as a sibling file (loaded via `#[path]` from `store.rs`) so the
//! runtime code + pre-existing BM25 tests in `store.rs` stay under the
//! 1500-LOC cap mandated by CLAUDE.md.

use bytes::Bytes;

use super::*;
use crate::text::types::{BM25Config, TagFieldDef, TextFieldDef};

fn tag_args(pairs: &[(&[u8], &[u8])]) -> Vec<crate::protocol::Frame> {
    let mut v = Vec::with_capacity(pairs.len() * 2);
    for (k, val) in pairs {
        v.push(crate::protocol::Frame::BulkString(Bytes::copy_from_slice(
            k,
        )));
        v.push(crate::protocol::Frame::BulkString(Bytes::copy_from_slice(
            val,
        )));
    }
    v
}

fn tag_only_index(tag_names: &[&[u8]]) -> TextIndex {
    let tag_fields: Vec<TagFieldDef> = tag_names
        .iter()
        .map(|n| TagFieldDef::new(Bytes::copy_from_slice(n)))
        .collect();
    TextIndex::new_with_schema(
        Bytes::from_static(b"tag_idx"),
        Vec::new(),
        Vec::new(),
        tag_fields,
        Vec::new(),
        BM25Config::default(),
    )
}

#[test]
fn tag_field_def_defaults() {
    let def = TagFieldDef::new(Bytes::from_static(b"status"));
    assert_eq!(def.field_name.as_ref(), b"status");
    assert_eq!(def.separator, b',');
    assert!(!def.case_sensitive);
    assert!(!def.sortable);
    assert!(!def.noindex);
}

#[test]
fn new_with_schema_seeds_tag_indexes() {
    let idx = tag_only_index(&[b"status", b"priority"]);
    assert_eq!(idx.tag_fields.len(), 2);
    assert!(idx.tag_indexes.contains_key(&Bytes::from_static(b"status")));
    assert!(
        idx.tag_indexes
            .contains_key(&Bytes::from_static(b"priority"))
    );
    assert!(idx.text_fields.is_empty());
    assert!(idx.field_analyzers.is_empty());
}

#[test]
fn new_preserves_legacy_signature_and_tag_fields_empty() {
    let idx = TextIndex::new(
        Bytes::from_static(b"legacy"),
        Vec::new(),
        vec![TextFieldDef::new(Bytes::from_static(b"body"))],
        BM25Config::default(),
    );
    assert!(idx.tag_fields.is_empty());
    assert!(idx.tag_indexes.is_empty());
    assert_eq!(idx.text_fields.len(), 1);
}

#[test]
fn tag_index_document_multi_field() {
    let mut idx = tag_only_index(&[b"status", b"priority"]);
    let args = tag_args(&[(b"status", b"open"), (b"priority", b"high")]);
    idx.tag_index_document(0xdead_beef, b"doc:1", &args);
    assert_eq!(
        idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"open"))
            .len(),
        1
    );
    assert_eq!(
        idx.search_tag(
            &Bytes::from_static(b"priority"),
            &Bytes::from_static(b"high")
        )
        .len(),
        1
    );
}

#[test]
fn tag_index_document_multi_value_split_default_comma() {
    let mut idx = tag_only_index(&[b"tags"]);
    let args = tag_args(&[(b"tags", b"a,b,c")]);
    idx.tag_index_document(1, b"doc:x", &args);
    for v in [b"a", b"b", b"c"] {
        let r = idx.search_tag(&Bytes::from_static(b"tags"), &Bytes::copy_from_slice(v));
        assert_eq!(r.len(), 1, "tag {:?} missing", v);
    }
}

#[test]
fn tag_index_document_custom_separator() {
    let mut tag_def = TagFieldDef::new(Bytes::from_static(b"tags"));
    tag_def.separator = b';';
    let mut idx = TextIndex::new_with_schema(
        Bytes::from_static(b"sep"),
        Vec::new(),
        Vec::new(),
        vec![tag_def],
        Vec::new(),
        BM25Config::default(),
    );
    let args = tag_args(&[(b"tags", b"a;b;c")]);
    idx.tag_index_document(2, b"doc:y", &args);
    for v in [b"a", b"b", b"c"] {
        assert_eq!(
            idx.search_tag(&Bytes::from_static(b"tags"), &Bytes::copy_from_slice(v))
                .len(),
            1
        );
    }
}

#[test]
fn tag_default_case_insensitive_and_fast_path() {
    let mut idx = tag_only_index(&[b"status"]);
    let args = tag_args(&[(b"status", b"Open")]);
    idx.tag_index_document(10, b"d:1", &args);
    let r = idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"open"));
    assert_eq!(r.len(), 1);
    let r_up = idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"OPEN"));
    assert_eq!(r_up.len(), 1);
}

#[test]
fn tag_case_sensitive_preserves_case() {
    let mut tag_def = TagFieldDef::new(Bytes::from_static(b"code"));
    tag_def.case_sensitive = true;
    let mut idx = TextIndex::new_with_schema(
        Bytes::from_static(b"cs"),
        Vec::new(),
        Vec::new(),
        vec![tag_def],
        Vec::new(),
        BM25Config::default(),
    );
    let args = tag_args(&[(b"code", b"Open")]);
    idx.tag_index_document(20, b"d:2", &args);
    assert_eq!(
        idx.search_tag(&Bytes::from_static(b"code"), &Bytes::from_static(b"Open"))
            .len(),
        1
    );
    assert_eq!(
        idx.search_tag(&Bytes::from_static(b"code"), &Bytes::from_static(b"open"))
            .len(),
        0
    );
}

#[test]
fn tag_per_field_upsert_preserves_untouched() {
    // Blocker 4: partial HSET must not clobber prior entries for absent fields.
    let mut idx = tag_only_index(&[b"status", b"priority"]);
    let args1 = tag_args(&[(b"status", b"open"), (b"priority", b"high")]);
    idx.tag_index_document(100, b"doc:X", &args1);
    let args2 = tag_args(&[(b"priority", b"low")]);
    idx.tag_index_document(100, b"doc:X", &args2);
    let status_open = idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"open"));
    assert_eq!(status_open.len(), 1, "status=open must be preserved");
    let prio_high = idx.search_tag(
        &Bytes::from_static(b"priority"),
        &Bytes::from_static(b"high"),
    );
    assert_eq!(prio_high.len(), 0, "prior priority=high must be revoked");
    let prio_low = idx.search_tag(
        &Bytes::from_static(b"priority"),
        &Bytes::from_static(b"low"),
    );
    assert_eq!(prio_low.len(), 1, "new priority=low must be indexed");
}

#[test]
fn tag_full_upsert_revokes_old() {
    let mut idx = tag_only_index(&[b"status", b"priority"]);
    let args1 = tag_args(&[(b"status", b"open"), (b"priority", b"high")]);
    idx.tag_index_document(200, b"doc:Y", &args1);
    let args2 = tag_args(&[(b"status", b"closed"), (b"priority", b"low")]);
    idx.tag_index_document(200, b"doc:Y", &args2);
    assert_eq!(
        idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"open"))
            .len(),
        0
    );
    assert_eq!(
        idx.search_tag(
            &Bytes::from_static(b"status"),
            &Bytes::from_static(b"closed")
        )
        .len(),
        1
    );
}

#[test]
fn search_tag_case_insensitive_field_resolution() {
    // Blocker 2: @Status:{open} on index `status` must match.
    let mut idx = tag_only_index(&[b"status"]);
    let args = tag_args(&[(b"status", b"open")]);
    idx.tag_index_document(300, b"doc:Z", &args);
    let r = idx.search_tag(&Bytes::from_static(b"Status"), &Bytes::from_static(b"open"));
    assert_eq!(
        r.len(),
        1,
        "mixed-case field lookup must resolve canonical `status`"
    );
}

#[test]
fn search_tag_unknown_field_returns_empty() {
    let idx = tag_only_index(&[b"status"]);
    let r = idx.search_tag(
        &Bytes::from_static(b"missing"),
        &Bytes::from_static(b"open"),
    );
    assert!(r.is_empty());
}

#[test]
fn tag_ensure_doc_id_shared_allocator() {
    // Blocker 7: tag_index_document and index_document share ensure_doc_id.
    let mut idx = TextIndex::new_with_schema(
        Bytes::from_static(b"shared"),
        Vec::new(),
        vec![TextFieldDef::new(Bytes::from_static(b"body"))],
        vec![TagFieldDef::new(Bytes::from_static(b"status"))],
        Vec::new(),
        BM25Config::default(),
    );
    let tag_args_v = tag_args(&[(b"status", b"open")]);
    idx.tag_index_document(777, b"doc:shared", &tag_args_v);
    let tag_ids = idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"open"));
    assert_eq!(tag_ids, vec![0]);
    assert_eq!(idx.key_hash_to_doc_id.get(&777), Some(&0));
    assert_eq!(
        idx.doc_id_to_key.get(&0).map(|b| b.as_ref()),
        Some(&b"doc:shared"[..])
    );
    let txt_args = tag_args(&[(b"body", b"hello world")]);
    idx.index_document(777, b"doc:shared", &txt_args);
    assert_eq!(idx.key_hash_to_doc_id.len(), 1);
    assert_eq!(idx.key_hash_to_doc_id.get(&777), Some(&0));
    idx.tag_index_document(777, b"doc:shared", &tag_args_v);
    assert_eq!(idx.key_hash_to_doc_id.len(), 1);
}

#[test]
fn tag_empty_schema_noop() {
    let mut idx = TextIndex::new(
        Bytes::from_static(b"noop"),
        Vec::new(),
        vec![TextFieldDef::new(Bytes::from_static(b"body"))],
        BM25Config::default(),
    );
    let args = tag_args(&[(b"status", b"open")]);
    idx.tag_index_document(888, b"doc:empty", &args);
    assert!(idx.tag_indexes.is_empty());
    assert!(idx.doc_tag_entries.is_empty());
    assert!(idx.doc_id_to_key.is_empty());
}

#[test]
fn pre_parse_field_filter_tag_basic() {
    use crate::command::vector_search::ft_text_search::{FieldFilter, pre_parse_field_filter};
    let r = pre_parse_field_filter(b"@status:{open}").expect("ok");
    let clause = r.expect("some");
    assert!(clause.terms.is_empty());
    assert_eq!(clause.field_name, None);
    match clause.filter {
        Some(FieldFilter::Tag { field, value }) => {
            assert_eq!(field.as_ref(), b"status");
            assert_eq!(value.as_ref(), b"open");
        }
        _ => panic!("expected Tag filter"),
    }
}

#[test]
fn pre_parse_field_filter_falls_through_for_bm25() {
    use crate::command::vector_search::ft_text_search::pre_parse_field_filter;
    assert!(
        pre_parse_field_filter(b"@title:(machine learning)")
            .unwrap()
            .is_none()
    );
    assert!(
        pre_parse_field_filter(b"machine learning")
            .unwrap()
            .is_none()
    );
}

#[test]
fn pre_parse_field_filter_errors() {
    use crate::command::vector_search::ft_text_search::pre_parse_field_filter;
    assert!(pre_parse_field_filter(b"@status:{open").is_err());
    let err = pre_parse_field_filter(b"@status:{open|closed}").unwrap_err();
    assert!(err.contains("multi-tag OR"));
    assert!(pre_parse_field_filter(b"@:{x}").is_err());
    let long = {
        let mut q = b"@x:{".to_vec();
        q.extend(std::iter::repeat_n(b'A', 4097));
        q.push(b'}');
        q
    };
    assert!(pre_parse_field_filter(&long).is_err());
}

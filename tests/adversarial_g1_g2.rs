//! Adversarial TDD verification for v0.1.10 G-1 (BM25 AS_OF) + G-2 (RESP3 Push).
//!
//! Goal: hunt defects in the landed fixes by probing edge cases beyond the
//! happy-path unit tests. Each test names the exact defect class it targets.
#![cfg(feature = "runtime-tokio")]

use bytes::{Bytes, BytesMut};
use moon::protocol::{Frame, ParseConfig, parse};
use moon::pubsub::subscriber::Subscriber;
use moon::pubsub::{PubSubRegistry, next_subscriber_id};
use moon::runtime::channel;
#[cfg(feature = "text-index")]
use moon::text::store::TextIndex;
#[cfg(feature = "text-index")]
use moon::text::types::{BM25Config, TextFieldDef};

fn parse_resp(data: &[u8]) -> Frame {
    let mut buf = BytesMut::from(data);
    parse(&mut buf, &ParseConfig::default())
        .expect("valid RESP")
        .expect("complete frame")
}

// ───────────────────────────────────────────────────────────────────────
// G-1 adversarial cases
// ───────────────────────────────────────────────────────────────────────

/// Defect class: top-k recall loss. If `top_k` visible docs rank behind many
/// post-snapshot docs, the 2× oversample must still surface them. Seed 1
/// visible + 20 invisible docs, request top_k=5, expect the single visible
/// doc returned.
#[cfg(feature = "text-index")]
#[test]
fn g1_as_of_top_k_oversample_rescues_low_ranked_visible_doc() {
    use moon::protocol::Frame;
    let field = TextFieldDef::new(Bytes::from_static(b"body"));
    let mut idx = TextIndex::new(
        Bytes::from_static(b"adv"),
        Vec::new(),
        vec![field],
        BM25Config::default(),
    );
    // Doc 0: the visible doc — sparser term → lower BM25 score than noisy docs
    let args0 = vec![
        Frame::BulkString(Bytes::from_static(b"body")),
        Frame::BulkString(Bytes::from_static(b"alpha")),
    ];
    idx.index_document_with_lsn(0, b"doc:visible", &args0, 10);
    // Docs 1..20: term appears many times → higher BM25, all post-snapshot
    for i in 1u64..21 {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"body")),
            Frame::BulkString(Bytes::from_static(b"alpha alpha alpha alpha alpha")),
        ];
        idx.index_document_with_lsn(i, format!("doc:noisy:{i}").as_bytes(), &args, 100);
    }

    // AS_OF = 50 → visible docs are those with insert_lsn <= 50 (doc 0 only).
    let terms = vec!["alpha".to_string()];
    let results = idx.search_field_as_of(0, &terms, None, None, 5, 50);
    let keys: Vec<&[u8]> = results.iter().map(|r| r.key.as_ref()).collect();
    assert!(
        keys.contains(&(b"doc:visible".as_ref())),
        "2x oversample must rescue the visible doc even when it ranks last; got {:?}",
        keys
    );
    assert!(
        !keys.iter().any(|k| std::str::from_utf8(k).unwrap().starts_with("doc:noisy:")),
        "post-snapshot docs must be filtered; got {:?}",
        keys
    );
}

/// Defect class: **UPSERT overwrite loses pre-upsert visibility**. A doc
/// indexed at lsn=10, then upserted at lsn=50: at AS_OF=30 the ORIGINAL
/// version existed, but our current design overwrites insert_lsn → 50,
/// making the doc invisible at AS_OF=30.
///
/// This test DOCUMENTS the current behavior (upsert-wins) and will fail if
/// someone later adds version history without updating the lock-in.
///
/// This is a known design limitation per `TextIndex.doc_id_to_insert_lsn`
/// docs — v0.2 text-index-mvcc work would add multi-version storage.
#[cfg(feature = "text-index")]
#[test]
fn g1_upsert_overwrites_insert_lsn_documented_limitation() {
    use moon::protocol::Frame;
    let field = TextFieldDef::new(Bytes::from_static(b"body"));
    let mut idx = TextIndex::new(
        Bytes::from_static(b"adv"),
        Vec::new(),
        vec![field],
        BM25Config::default(),
    );
    let args_v1 = vec![
        Frame::BulkString(Bytes::from_static(b"body")),
        Frame::BulkString(Bytes::from_static(b"alpha v1")),
    ];
    let args_v2 = vec![
        Frame::BulkString(Bytes::from_static(b"body")),
        Frame::BulkString(Bytes::from_static(b"alpha v2")),
    ];
    let doc_id = idx.index_document_with_lsn(0, b"doc:x", &args_v1, 10);
    assert_eq!(idx.doc_id_to_insert_lsn.get(&doc_id).copied(), Some(10));
    idx.index_document_with_lsn(0, b"doc:x", &args_v2, 50);
    assert_eq!(
        idx.doc_id_to_insert_lsn.get(&doc_id).copied(),
        Some(50),
        "upsert replaces insert_lsn — multi-version history deferred to v0.2"
    );
    // AS_OF=30: the doc is NOT visible (our design collapses history).
    // v0.2 text-index-mvcc work item will add doc_id → Vec<(lsn, tombstone)>.
    assert!(
        !idx.is_doc_visible_at(doc_id, 30),
        "Current design: upsert invalidates older snapshots. Flag as broken if v0.2 MVCC lands."
    );
    assert!(
        idx.is_doc_visible_at(doc_id, 50),
        "upserted version visible at AS_OF=50"
    );
}

/// Defect class: empty result path correctness. If ALL results filter out,
/// we must return an empty vec (not a partial vec, not an error).
#[cfg(feature = "text-index")]
#[test]
fn g1_as_of_filters_everything_returns_empty() {
    use moon::protocol::Frame;
    let field = TextFieldDef::new(Bytes::from_static(b"body"));
    let mut idx = TextIndex::new(
        Bytes::from_static(b"adv"),
        Vec::new(),
        vec![field],
        BM25Config::default(),
    );
    for i in 0u64..5 {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"body")),
            Frame::BulkString(Bytes::from_static(b"alpha")),
        ];
        idx.index_document_with_lsn(i, format!("doc:{i}").as_bytes(), &args, 100);
    }
    let terms = vec!["alpha".to_string()];
    let results = idx.search_field_as_of(0, &terms, None, None, 10, 50);
    assert!(
        results.is_empty(),
        "all filtered → empty vec; got len={}",
        results.len()
    );
}

/// Defect class: as_of_lsn=u64::MAX boundary — every doc must be visible.
#[cfg(feature = "text-index")]
#[test]
fn g1_as_of_max_u64_sees_all() {
    use moon::protocol::Frame;
    let field = TextFieldDef::new(Bytes::from_static(b"body"));
    let mut idx = TextIndex::new(
        Bytes::from_static(b"adv"),
        Vec::new(),
        vec![field],
        BM25Config::default(),
    );
    for i in 0u64..5 {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"body")),
            Frame::BulkString(Bytes::from_static(b"alpha")),
        ];
        idx.index_document_with_lsn(i, format!("doc:{i}").as_bytes(), &args, i * 10 + 1);
    }
    let terms = vec!["alpha".to_string()];
    let results = idx.search_field_as_of(0, &terms, None, None, 10, u64::MAX);
    assert_eq!(results.len(), 5, "as_of_lsn = u64::MAX must see all docs");
}

/// Defect class: pre-MVCC doc (lsn=0) + post-MVCC doc in same index.
/// AS_OF query must see BOTH (pre-MVCC is grandfathered-in per design).
#[cfg(feature = "text-index")]
#[test]
fn g1_mixed_pre_mvcc_and_mvcc_docs_both_visible() {
    use moon::protocol::Frame;
    let field = TextFieldDef::new(Bytes::from_static(b"body"));
    let mut idx = TextIndex::new(
        Bytes::from_static(b"adv"),
        Vec::new(),
        vec![field],
        BM25Config::default(),
    );
    // Pre-MVCC: use index_document (no LSN)
    let args = vec![
        Frame::BulkString(Bytes::from_static(b"body")),
        Frame::BulkString(Bytes::from_static(b"alpha")),
    ];
    idx.index_document(0, b"doc:pre_mvcc", &args);
    // MVCC-tagged at lsn=20
    idx.index_document_with_lsn(1, b"doc:mvcc20", &args, 20);
    // MVCC-tagged at lsn=100 (post-snapshot)
    idx.index_document_with_lsn(2, b"doc:mvcc100", &args, 100);

    let terms = vec!["alpha".to_string()];
    let results = idx.search_field_as_of(0, &terms, None, None, 10, 50);
    let keys: std::collections::HashSet<&[u8]> = results.iter().map(|r| r.key.as_ref()).collect();
    assert!(keys.contains(b"doc:pre_mvcc".as_ref()), "pre-MVCC grandfathered");
    assert!(keys.contains(b"doc:mvcc20".as_ref()), "pre-snapshot MVCC visible");
    assert!(
        !keys.contains(b"doc:mvcc100".as_ref()),
        "post-snapshot MVCC filtered"
    );
}

// ───────────────────────────────────────────────────────────────────────
// G-2 adversarial cases
// ───────────────────────────────────────────────────────────────────────

/// Defect class: PUBLISH to channel with only RESP3 subscribers must never
/// touch the RESP2 serialization path (behavioral: RESP3 subscriber sees
/// `>` prefix; absence of RESP2 sub means resp2_bytes lazy init never fires).
#[tokio::test]
async fn g2_pure_resp3_population_delivers_push_only() {
    let mut registry = PubSubRegistry::new();
    let (tx, rx) = channel::mpsc_bounded::<Bytes>(8);
    registry.subscribe(
        Bytes::from_static(b"evt"),
        Subscriber::with_protocol(tx, next_subscriber_id(), true),
    );
    let count = registry.publish(&Bytes::from_static(b"evt"), &Bytes::from_static(b"x"));
    assert_eq!(count, 1);
    let raw = rx.try_recv().expect("got msg");
    assert!(raw.starts_with(b">"), "RESP3-only delivers Push; got {raw:?}");
}

/// Defect class: PUBLISH to channel with only RESP2 subs must produce Array
/// only. Regression guard to ensure G-2 didn't accidentally default to Push.
#[tokio::test]
async fn g2_pure_resp2_population_delivers_array_only() {
    let mut registry = PubSubRegistry::new();
    let (tx, rx) = channel::mpsc_bounded::<Bytes>(8);
    // Use bare `new` (defaults to RESP2) to confirm backwards compat
    registry.subscribe(
        Bytes::from_static(b"evt"),
        Subscriber::new(tx, next_subscriber_id()),
    );
    registry.publish(&Bytes::from_static(b"evt"), &Bytes::from_static(b"x"));
    let raw = rx.try_recv().expect("got msg");
    assert!(raw.starts_with(b"*"), "RESP2-only delivers Array; got {raw:?}");
}

/// Defect class: pattern `news.*` matches channel `news.tech` AND delivers
/// pmessage Push to RESP3 sub. Also: subscriber NOT registered on exact
/// channel `news.tech` must not receive twice.
#[tokio::test]
async fn g2_pattern_match_only_pmessage_no_duplicate() {
    let mut registry = PubSubRegistry::new();
    let (tx, rx) = channel::mpsc_bounded::<Bytes>(8);
    registry.psubscribe(
        Bytes::from_static(b"news.*"),
        Subscriber::with_protocol(tx, next_subscriber_id(), true),
    );
    let count = registry.publish(
        &Bytes::from_static(b"news.tech"),
        &Bytes::from_static(b"body"),
    );
    assert_eq!(count, 1, "pattern match delivers exactly once");
    let raw = rx.try_recv().expect("got pmessage");
    match parse_resp(&raw) {
        Frame::Push(items) => {
            assert_eq!(items.len(), 4, "pmessage has 4 elements");
            if let Frame::BulkString(head) = &items[0] {
                assert_eq!(head.as_ref(), b"pmessage");
            }
        }
        other => panic!("expected pmessage Push; got {other:?}"),
    }
    assert!(rx.try_recv().is_err(), "no duplicate delivery");
}

/// Defect class: slow-subscriber eviction path must still work when the
/// subscriber is RESP3. If publish removes a slow RESP3 sub, subsequent
/// PUBLISH must NOT panic and count must decrement.
#[tokio::test]
async fn g2_resp3_slow_subscriber_evicted_cleanly() {
    let mut registry = PubSubRegistry::new();
    // Channel capacity=1 → second PUBLISH on same subscriber fills + drops
    let (tx, _rx_ignored_never_drained) = channel::mpsc_bounded::<Bytes>(1);
    registry.subscribe(
        Bytes::from_static(b"evt"),
        Subscriber::with_protocol(tx, next_subscriber_id(), true),
    );
    // First publish succeeds (buffer 0→1).
    let c1 = registry.publish(&Bytes::from_static(b"evt"), &Bytes::from_static(b"m1"));
    assert_eq!(c1, 1);
    // Second publish: buffer is full → subscriber evicted.
    let c2 = registry.publish(&Bytes::from_static(b"evt"), &Bytes::from_static(b"m2"));
    assert_eq!(c2, 0, "slow sub evicted — count=0");
    // Third publish: no subs → count=0, no panic.
    let c3 = registry.publish(&Bytes::from_static(b"evt"), &Bytes::from_static(b"m3"));
    assert_eq!(c3, 0);
}

/// Defect class: lazy init with mixed population must produce both frames
/// exactly once. Drain 2 subs and verify parsing contracts of both frames.
#[tokio::test]
async fn g2_lazy_serialize_mixed_population_both_frames_valid() {
    let mut registry = PubSubRegistry::new();
    let (tx2, rx2) = channel::mpsc_bounded::<Bytes>(8);
    let (tx3, rx3) = channel::mpsc_bounded::<Bytes>(8);
    registry.subscribe(
        Bytes::from_static(b"evt"),
        Subscriber::with_protocol(tx2, next_subscriber_id(), false),
    );
    registry.subscribe(
        Bytes::from_static(b"evt"),
        Subscriber::with_protocol(tx3, next_subscriber_id(), true),
    );
    registry.publish(&Bytes::from_static(b"evt"), &Bytes::from_static(b"payload"));

    // Validate RESP2 frame is a valid Array with 3 BulkString elements
    let raw2 = rx2.try_recv().expect("resp2 msg");
    match parse_resp(&raw2) {
        Frame::Array(items) => {
            assert_eq!(items.len(), 3, "Array has 3 elements");
            if let Frame::BulkString(h) = &items[0] {
                assert_eq!(h.as_ref(), b"message");
            } else {
                panic!("item[0] not BulkString");
            }
        }
        other => panic!("expected Array; got {other:?}"),
    }
    // Validate RESP3 frame is a valid Push with 3 BulkString elements
    let raw3 = rx3.try_recv().expect("resp3 msg");
    match parse_resp(&raw3) {
        Frame::Push(items) => {
            assert_eq!(items.len(), 3);
            if let Frame::BulkString(h) = &items[0] {
                assert_eq!(h.as_ref(), b"message");
            }
        }
        other => panic!("expected Push; got {other:?}"),
    }
}

//! RED tests for moon#691 — FT query errors were bare snake_case tokens no client can classify.
//!
//! A RESP error's first word is its *code*, and clients branch on it: redis-py surfaces it via
//! `ResponseError`, redis-rs matches `ErrorKind` off the leading token, and every client has a
//! rule for `ERR`, `WRONGTYPE`, `NOSCRIPT`, `MOVED`, `LOADING`. None has a rule for
//! `numeric_filter_invalid`, so the whole message was swallowed as the code and nothing
//! downstream could tell a client-side query mistake from a server fault.
//!
//! The five tokens are a frozen §3 contract (`fts-query-combinators`), so they are KEPT and
//! given a prefix and a readable detail — `ERR <token>: <what went wrong>` — rather than
//! renamed out from under whatever still greps for them.
#![cfg(feature = "text-index")]

use bytes::Bytes;
use moon::command::vector_search::run_text_query;
use moon::protocol::Frame;
use moon::text::query::QueryError;
use moon::text::store::{TextIndex, TextStore};
use moon::text::types::{BM25Config, NumericFieldDef, TagFieldDef, TextFieldDef};

fn all_errors() -> Vec<QueryError> {
    vec![
        QueryError::Syntax,
        QueryError::EmptyQuery,
        QueryError::UnknownField(Bytes::from_static(b"nope")),
        QueryError::NumericInvalid,
        QueryError::TagInvalid,
    ]
}

/// Every FT query error is a well-formed RESP error: `ERR` first, so a client can classify it.
#[test]
fn every_query_error_leads_with_the_err_code() {
    for e in all_errors() {
        let wire = e.wire_error();
        assert!(
            wire.starts_with(b"ERR "),
            "{:?} -> {:?}: a RESP error's first word is its code, and clients only \
             recognise ERR / WRONGTYPE / NOSCRIPT / MOVED / LOADING",
            e,
            String::from_utf8_lossy(&wire)
        );
    }
}

/// The five frozen tokens survive the prefix — anything still grepping for them keeps working.
#[test]
fn the_frozen_tokens_are_kept_not_renamed() {
    for e in all_errors() {
        let wire = e.wire_error();
        let token = e.code().as_bytes();
        assert!(
            wire.windows(token.len()).any(|w| w == token),
            "{:?} -> {:?} must still carry its frozen code `{}`",
            e,
            String::from_utf8_lossy(&wire),
            e.code()
        );
    }
}

/// ...and each one now says what actually went wrong, not just that something did.
#[test]
fn every_query_error_carries_a_readable_detail() {
    for e in all_errors() {
        let wire = e.wire_error();
        let minimum = 4 + e.code().len() + 2; // "ERR " + token + ": "
        assert!(
            wire.len() > minimum + 8,
            "{:?} -> {:?} is a bare token with no explanation",
            e,
            String::from_utf8_lossy(&wire)
        );
    }
}

/// The unknown-field error names the field the user actually typed — otherwise a query with
/// several `@field:` clauses gives no clue which one is wrong.
#[test]
fn the_unknown_field_error_names_the_offending_field() {
    let wire = QueryError::UnknownField(Bytes::from_static(b"authr")).wire_error();
    assert!(
        wire.windows(5).any(|w| w == b"authr"),
        "{:?} must name the field it could not resolve",
        String::from_utf8_lossy(&wire)
    );
}

/// A field name is user input echoed into a RESP error, which is written to the wire raw.
/// It must not be able to carry control bytes into the reply.
#[test]
fn an_echoed_field_name_cannot_inject_control_bytes() {
    let hostile = Bytes::from_static(b"a\r\n-INJECTED\r\n\x00\x07b\x7f");
    let wire = QueryError::UnknownField(hostile).wire_error();
    assert!(
        !wire.iter().any(|&b| b < 0x20 || b == 0x7f),
        "no control byte may reach the wire: {:?}",
        String::from_utf8_lossy(&wire)
    );
    assert!(
        !wire.windows(2).any(|w| w == b"\r\n"),
        "a CRLF in an error would terminate the frame early and desync the connection"
    );
}

// ── the wire boundary ───────────────────────────────────────────────────────

fn store_with_index() -> TextStore {
    let idx = TextIndex::new_with_schema(
        Bytes::from_static(b"idx"),
        Vec::new(),
        vec![TextFieldDef::new(Bytes::from_static(b"body"))],
        vec![TagFieldDef::new(Bytes::from_static(b"tag"))],
        vec![NumericFieldDef::new(Bytes::from_static(b"price"))],
        BM25Config::default(),
    );
    let mut ts = TextStore::new();
    ts.create_index(Bytes::from_static(b"idx"), idx)
        .expect("create_index ok");
    ts
}

fn err_of(ts: &TextStore, q: &str) -> Vec<u8> {
    match run_text_query(ts, b"idx", q.as_bytes(), 10, 0, usize::MAX, 0) {
        Frame::Error(b) => b.to_vec(),
        other => panic!("expected Frame::Error for {q:?}, got {other:?}"),
    }
}

/// What actually reaches the client, for each way a query can be wrong.
#[test]
fn the_wire_errors_are_classifiable_end_to_end() {
    let ts = store_with_index();
    for (query, token) in [
        ("alpha | (beta", "syntax_error"),
        ("@nope:foo", "unknown_field"),
        ("@price:[100 10]", "numeric_filter_invalid"),
        ("@tag:{}", "tag_filter_invalid"),
    ] {
        let e = err_of(&ts, query);
        assert!(
            e.starts_with(b"ERR "),
            "{query:?} -> {:?} must lead with ERR",
            String::from_utf8_lossy(&e)
        );
        assert!(
            e.windows(token.len()).any(|w| w == token.as_bytes()),
            "{query:?} -> {:?} must still carry `{token}`",
            String::from_utf8_lossy(&e)
        );
    }
}

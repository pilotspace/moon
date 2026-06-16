//! RED tests for `fts-query-combinators` task 2a — the FT.SEARCH query PARSER.
//!
//! Contract FROZEN @ v1 (`.add/tasks/fts-query-combinators/TASK.md` §3): a recursive-descent
//! parser turns an FT.SEARCH query string into a `QueryNode` AST over the RediSearch subset
//! (terms+modifiers · implicit-AND · OR `|` · grouping `()` · `@field:` clauses · TAG `{a|b}` ·
//! NUMERIC `[min max]`), with DIALECT-2 precedence (AND > modifiers > OR) and 5 named error codes.
//!
//! These are PURE parser tests — `parse_query(bytes, schema) -> Result<QueryNode, QueryError>`,
//! no index/server. The AST holds RAW (un-analyzed) tokens; analysis happens in 2b (eval). The
//! end-to-end matched-SET behaviour (M1/M2/M3 result sets, M5 no-regression, M7 server-stays-up)
//! is `fts-query-eval-dispatch`'s suite, not here.
//!
//! TDD — RED before build: `moon::text::query` does not exist yet, so the suite fails to compile;
//! once 2a lands `parse_query`/`QueryNode`/`QueryError`/`QuerySchema`, these go green unchanged.
#![cfg(feature = "text-index")]

use bytes::Bytes;
use moon::text::query::{QueryError, QueryNode, QuerySchema, parse_query};
use moon::text::store::TermModifier;

/// Schema: text fields body(0), title(1); tag field "tag"; numeric field "price".
fn schema() -> QuerySchema {
    QuerySchema::from_names(&["body", "title"], &["tag"], &["price"])
}

// ── small constructors so expected trees read cleanly ──────────────────────
fn ex(field: Option<usize>, tok: &str) -> QueryNode {
    QueryNode::Term {
        field,
        token: Bytes::copy_from_slice(tok.as_bytes()),
        modifier: TermModifier::Exact,
    }
}
fn tag(field: &str, values: &[&str]) -> QueryNode {
    QueryNode::Tag {
        field: Bytes::copy_from_slice(field.as_bytes()),
        values: values
            .iter()
            .map(|v| Bytes::copy_from_slice(v.as_bytes()))
            .collect(),
    }
}
fn p(q: &str) -> QueryNode {
    parse_query(q.as_bytes(), &schema()).expect("parse ok")
}

// ─────────────────────────── M1 — OR is a union node ───────────────────────
#[test]
fn test_or_parses_as_or_node() {
    assert_eq!(
        p("alpha | beta"),
        QueryNode::Or(vec![ex(None, "alpha"), ex(None, "beta")])
    );
}

/// The headline regression: `|` must NEVER be silently dropped (the old bug turned it into AND).
#[test]
fn test_pipe_never_discarded() {
    match p("alpha | beta") {
        QueryNode::Or(children) => assert_eq!(children.len(), 2, "OR must keep both branches"),
        other => panic!("`|` must parse to an Or node, got {other:?}"),
    }
}

// ──────────────── M2 — multi-clause: distinct typed clauses, AND-joined ─────
#[test]
fn test_multiclause_parses_distinct_clauses() {
    // @body:foo @tag:{bar} -> And[ Term{body,foo}, Tag(tag,[bar]) ]
    // The tag clause is a Tag NODE, never word-tokens "tag","bar" scoped to body (the old bug).
    assert_eq!(
        p("@body:foo @tag:{bar}"),
        QueryNode::And(vec![ex(Some(0), "foo"), tag("tag", &["bar"])]),
    );
}

#[test]
fn test_text_numeric_clause() {
    // @body:phone @price:[10 20] -> And[ Term{body,phone}, Numeric(price,10,20,false,false) ]
    assert_eq!(
        p("@body:phone @price:[10 20]"),
        QueryNode::And(vec![
            ex(Some(0), "phone"),
            QueryNode::Numeric {
                field: Bytes::from_static(b"price"),
                min: 10.0,
                max: 20.0,
                min_excl: false,
                max_excl: false,
            },
        ]),
    );
}

#[test]
fn test_multi_tag_values() {
    assert_eq!(p("@tag:{a|b}"), tag("tag", &["a", "b"]));
}

// ─────────────────────────── M3 — grouping ─────────────────────────────────
#[test]
fn test_grouping_scopes_union() {
    // car (red | blue) -> And[ Term(car), Or[Term(red),Term(blue)] ]
    assert_eq!(
        p("car (red | blue)"),
        QueryNode::And(vec![
            ex(None, "car"),
            QueryNode::Or(vec![ex(None, "red"), ex(None, "blue")]),
        ]),
    );
}

#[test]
fn test_field_scoped_group() {
    // @body:(a | b) -> Or[ Term{body,a}, Term{body,b} ]   (field pushed into the group)
    assert_eq!(
        p("@body:(a | b)"),
        QueryNode::Or(vec![ex(Some(0), "a"), ex(Some(0), "b")]),
    );
}

// ─────────────── M4 — precedence: AND binds tighter than OR (DIALECT 2) ─────
#[test]
fn test_precedence_and_binds_tighter() {
    // a b | c d  ==  (a AND b) OR (c AND d)
    assert_eq!(
        p("a b | c d"),
        QueryNode::Or(vec![
            QueryNode::And(vec![ex(None, "a"), ex(None, "b")]),
            QueryNode::And(vec![ex(None, "c"), ex(None, "d")]),
        ]),
    );
}

// ─────────────── M5 (parse level) — modifiers + numeric capability preserved ─
#[test]
fn test_modifiers_preserved() {
    // %alpa% -> fuzzy distance 1 on raw "alpa"; al* -> prefix on raw "al".
    match p("%alpa%") {
        QueryNode::Term {
            token, modifier, ..
        } => {
            assert_eq!(token, Bytes::from_static(b"alpa"));
            assert_eq!(modifier, TermModifier::Fuzzy(1));
        }
        other => panic!("expected fuzzy Term, got {other:?}"),
    }
    match p("al*") {
        QueryNode::Term {
            token, modifier, ..
        } => {
            assert_eq!(token, Bytes::from_static(b"al"));
            assert_eq!(modifier, TermModifier::Prefix);
        }
        other => panic!("expected prefix Term, got {other:?}"),
    }
}

#[test]
fn test_numeric_exclusive_and_inf() {
    // @price:[(10 +inf] -> exclusive min 10, inclusive max +inf  (must NOT regress existing capability)
    match p("@price:[(10 +inf]") {
        QueryNode::Numeric {
            field,
            min,
            max,
            min_excl,
            max_excl,
        } => {
            assert_eq!(field, Bytes::from_static(b"price"));
            assert_eq!(min, 10.0);
            assert!(max.is_infinite() && max.is_sign_positive());
            assert!(min_excl, "(10 -> exclusive min");
            assert!(!max_excl);
        }
        other => panic!("expected Numeric, got {other:?}"),
    }
}

#[test]
fn test_single_field_term_unchanged() {
    // @body:alpha -> Term{body, alpha}  (the already-correct single-clause path)
    assert_eq!(p("@body:alpha"), ex(Some(0), "alpha"));
}

// ─────────────── Rejects — each a named QueryError, never a panic ───────────
#[test]
fn test_unbalanced_paren_is_syntax() {
    assert_eq!(
        parse_query(b"alpha | (beta", &schema()),
        Err(QueryError::Syntax)
    );
}

#[test]
fn test_unbalanced_brace_bracket() {
    assert_eq!(
        parse_query(b"@tag:{bar", &schema()),
        Err(QueryError::Syntax)
    );
    assert_eq!(
        parse_query(b"@price:[10 20", &schema()),
        Err(QueryError::Syntax)
    );
}

#[test]
fn test_empty_query() {
    assert_eq!(parse_query(b"", &schema()), Err(QueryError::EmptyQuery));
    assert_eq!(parse_query(b"   ", &schema()), Err(QueryError::EmptyQuery));
    assert_eq!(parse_query(b"()", &schema()), Err(QueryError::EmptyQuery));
}

#[test]
fn test_unknown_field() {
    assert_eq!(
        parse_query(b"@nope:foo", &schema()),
        Err(QueryError::UnknownField(Bytes::from_static(b"nope"))),
    );
}

#[test]
fn test_numeric_invalid() {
    assert_eq!(
        parse_query(b"@price:[20 10]", &schema()),
        Err(QueryError::NumericInvalid)
    ); // min>max
    assert_eq!(
        parse_query(b"@price:[x y]", &schema()),
        Err(QueryError::NumericInvalid)
    ); // non-numeric
}

#[test]
fn test_tag_invalid() {
    assert_eq!(
        parse_query(b"@tag:{}", &schema()),
        Err(QueryError::TagInvalid)
    );
    assert_eq!(
        parse_query(b"@tag:{ }", &schema()),
        Err(QueryError::TagInvalid)
    );
}

/// A term matching nothing is NOT a parse error — emptiness is an EVAL outcome.
#[test]
fn test_absent_term_is_not_error() {
    assert_eq!(p("zzz"), ex(None, "zzz"));
    assert_eq!(
        p("alpha zzz"),
        QueryNode::And(vec![ex(None, "alpha"), ex(None, "zzz")]),
    );
}

/// M7 — a table of malformed inputs must each return Err, never panic / hang.
#[test]
fn test_parser_never_panics() {
    let malformed: &[&[u8]] = &[
        b"|",
        b"| |",
        b"(",
        b")",
        b"(((",
        b"@",
        b"@:",
        b"@:foo",
        b"@tag:",
        b"@tag:{",
        b"[",
        b"]",
        b"@price:[",
        b"@price:]",
        b"{}",
        b"@@@",
        b":::",
        b"a | | b",
        b"((a)",
        b"@tag:{a|}",
        b"@price:[1]",
        b"@price:[1 2 3]",
        b"\xff\xfe",
        b"a (b | c",
    ];
    for q in malformed {
        // The only contract is: returns a Result, never panics. (Err or Ok both fine here.)
        let _ = parse_query(q, &schema());
    }
}

/// The frozen §3 wire error codes — QueryError::code() must map 1:1 (the dispatch layer in 2b
/// emits these as Frame::Error). Locking them here keeps 2b from drifting off the contract.
#[test]
fn test_error_codes_match_contract() {
    assert_eq!(QueryError::Syntax.code(), "syntax_error");
    assert_eq!(QueryError::EmptyQuery.code(), "empty_query");
    assert_eq!(
        QueryError::UnknownField(Bytes::new()).code(),
        "unknown_field"
    );
    assert_eq!(QueryError::NumericInvalid.code(), "numeric_filter_invalid");
    assert_eq!(QueryError::TagInvalid.code(), "tag_filter_invalid");
}

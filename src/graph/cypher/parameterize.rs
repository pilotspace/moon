//! Literal → auto-parameter rewrite for plan-cache normalization.
//!
//! `MATCH (a:N {id: 3})` and `MATCH (a:N {id: 4})` compile to the same
//! physical plan modulo the literal, but hash to different plan-cache keys,
//! so a point-query workload with varying literals never hits the cache.
//! [`parameterize`] rewrites value literals into synthetic parameters
//! (`$__p0`, `$__p1`, …) at the token level and extracts their values, so
//! the cache keys on the normalized text and the executor resolves the
//! values per run — exactly like a user-supplied `--params` query (which
//! `PhysicalOp::IndexScan` already index-hits on).
//!
//! Structural literals are NOT rewritten, because they are baked into the
//! compiled plan rather than evaluated at runtime:
//! - variable-length hop bounds (`[*1..3]`, `*2`, shortestPath bounds) —
//!   parsed by `parse_var_length` into `Expand`/`ShortestPath` `max_hops`;
//! - `LIMIT n` / `SKIP n` — evaluated as `Expr` today, but kept literal
//!   conservatively so the plan text pins the bound;
//! - literals with a leading `-`: logos' maximal munch lexes `x-1` as
//!   `Ident(x) Integer(-1)`, so rewriting the integer would also swallow a
//!   binary minus.
//!
//! The rewrite is fail-open: any surprise (lex error, non-UTF-8 string,
//! unparsable number, pre-existing `$__p` name) returns `None` and the
//! caller falls back to the raw text with raw-hash caching — never wrong
//! results, only a missed cache share.

use logos::Logos;

use super::executor::Value;
use super::lexer::Token;

/// A literal-normalized query plus the extracted literal values.
pub struct ParameterizedQuery {
    /// The query text with value literals replaced by `$__pN` parameters.
    pub normalized: Vec<u8>,
    /// Extracted literal values, keyed by generated parameter name WITHOUT
    /// the `$` sigil (matching `Expr::Parameter`'s stored name). Merged into
    /// the user params map before execution.
    pub auto_params: Vec<(String, Value)>,
}

/// Reserved prefix for auto-generated parameter names.
const AUTO_PREFIX: &[u8] = b"$__p";

/// Rewrite value literals into auto-parameters. Returns `None` when nothing
/// was rewritten (no literals, or only structural ones) or when the input is
/// unsuitable for rewriting — callers then use the raw text unchanged.
pub fn parameterize(input: &[u8]) -> Option<ParameterizedQuery> {
    // A user query already naming $__p* could collide with generated names.
    if input.windows(AUTO_PREFIX.len()).any(|w| w == AUTO_PREFIX) {
        return None;
    }

    // Tokenize with spans. Comments stay in the stream (copied verbatim,
    // skipped for prev/next significance checks). Any lex error bails: the
    // parser skips unrecognized bytes, but a rewrite around them is not
    // worth reasoning about.
    let mut lexer = Token::lexer(input);
    let mut tokens: Vec<(Token, core::ops::Range<usize>)> = Vec::new();
    while let Some(result) = lexer.next() {
        match result {
            Ok(t) => tokens.push((t, lexer.span())),
            Err(()) => return None,
        }
    }

    let significant = |t: &Token| !matches!(t, Token::LineComment | Token::BlockComment);

    let mut normalized = Vec::with_capacity(input.len());
    let mut auto_params: Vec<(String, Value)> = Vec::new();
    let mut last_end = 0usize;

    for i in 0..tokens.len() {
        let (tok, span) = &tokens[i];
        let Some(value) = literal_value(tok) else {
            continue;
        };
        // Structural positions: hop bounds and LIMIT/SKIP stay literal.
        let prev = tokens[..i]
            .iter()
            .rev()
            .map(|(t, _)| t)
            .find(|t| significant(t));
        let next = tokens[i + 1..]
            .iter()
            .map(|(t, _)| t)
            .find(|t| significant(t));
        if matches!(
            prev,
            Some(Token::Star | Token::DotDot | Token::Limit | Token::Skip)
        ) || matches!(next, Some(Token::DotDot))
        {
            continue;
        }

        normalized.extend_from_slice(&input[last_end..span.start]);
        let name = format!("__p{}", auto_params.len());
        normalized.push(b'$');
        normalized.extend_from_slice(name.as_bytes());
        auto_params.push((name, value));
        last_end = span.end;
    }

    if auto_params.is_empty() {
        return None;
    }
    normalized.extend_from_slice(&input[last_end..]);
    Some(ParameterizedQuery {
        normalized,
        auto_params,
    })
}

/// Decode a rewritable literal token into its runtime [`Value`].
///
/// Returns `None` for non-literal tokens AND for literals we refuse to
/// rewrite (leading `-`, unparsable numbers, non-UTF-8 strings) — refusing
/// just leaves that literal in place, which is always correct.
fn literal_value(tok: &Token) -> Option<Value> {
    match tok {
        Token::Integer(s) => {
            if s.first() == Some(&b'-') {
                return None;
            }
            let text = core::str::from_utf8(s).ok()?;
            text.parse::<i64>().ok().map(Value::Int)
        }
        Token::Float(s) => {
            if s.first() == Some(&b'-') {
                return None;
            }
            let text = core::str::from_utf8(s).ok()?;
            text.parse::<f64>().ok().map(Value::Float)
        }
        Token::StringLit(s) => {
            // Strip the surrounding single quotes.
            let inner = &s[1..s.len() - 1];
            core::str::from_utf8(inner)
                .ok()
                .map(|t| Value::String(bytes::Bytes::copy_from_slice(t.as_bytes())))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm_str(pq: &ParameterizedQuery) -> &str {
        core::str::from_utf8(&pq.normalized).expect("normalized text is utf8")
    }

    #[test]
    fn test_int_and_string_literals_rewritten() {
        let pq =
            parameterize(b"MATCH (a:N {id: 3, name: 'x'}) RETURN a.id").expect("literals present");
        assert_eq!(
            norm_str(&pq),
            "MATCH (a:N {id: $__p0, name: $__p1}) RETURN a.id"
        );
        assert_eq!(pq.auto_params.len(), 2);
        assert_eq!(pq.auto_params[0].0, "__p0");
        assert!(matches!(pq.auto_params[0].1, Value::Int(3)));
        assert!(matches!(&pq.auto_params[1].1, Value::String(s) if s == "x"));
    }

    #[test]
    fn test_float_literal_rewritten() {
        let pq = parameterize(b"MATCH (a {w: 1.5}) RETURN a").expect("literal present");
        assert_eq!(norm_str(&pq), "MATCH (a {w: $__p0}) RETURN a");
        assert!(matches!(pq.auto_params[0].1, Value::Float(f) if (f - 1.5).abs() < 1e-9));
    }

    #[test]
    fn test_literal_variants_normalize_identically() {
        let a = parameterize(b"MATCH (a:N {id: 3}) RETURN a").expect("rewrites");
        let b = parameterize(b"MATCH (a:N {id: 44}) RETURN a").expect("rewrites");
        assert_eq!(a.normalized, b.normalized);
    }

    #[test]
    fn test_var_length_hops_not_rewritten() {
        assert!(parameterize(b"MATCH (a)-[*1..3]->(b) RETURN b").is_none());
        assert!(parameterize(b"MATCH (a)-[*2]->(b) RETURN b").is_none());
        assert!(parameterize(b"MATCH (a)-[*..5]->(b) RETURN b").is_none());
        // Mixed: the inline prop rewrites, the hop bounds stay literal.
        let pq = parameterize(b"MATCH (a:N {id: 3})-[*1..3]->(b) RETURN b").expect("id rewrites");
        assert_eq!(
            norm_str(&pq),
            "MATCH (a:N {id: $__p0})-[*1..3]->(b) RETURN b"
        );
        assert_eq!(pq.auto_params.len(), 1);
    }

    #[test]
    fn test_limit_and_skip_not_rewritten() {
        assert!(parameterize(b"MATCH (a) RETURN a SKIP 2 LIMIT 5").is_none());
        let pq =
            parameterize(b"MATCH (a:N {id: 7}) RETURN a LIMIT 5").expect("inline prop rewrites");
        assert_eq!(norm_str(&pq), "MATCH (a:N {id: $__p0}) RETURN a LIMIT 5");
    }

    #[test]
    fn test_negative_literal_not_rewritten() {
        // logos lexes `x-1` as Ident(x) Integer(-1); rewriting would swallow
        // the binary minus. Leading-minus literals always stay in place.
        assert!(parameterize(b"MATCH (a) WHERE a.x = -5 RETURN a").is_none());
    }

    #[test]
    fn test_no_literals_returns_none() {
        assert!(parameterize(b"MATCH (a:Person) RETURN a").is_none());
        assert!(parameterize(b"MATCH (a {id: $id}) RETURN a").is_none());
    }

    #[test]
    fn test_existing_auto_prefix_bails() {
        assert!(parameterize(b"MATCH (a {id: $__p0, x: 3}) RETURN a").is_none());
    }

    #[test]
    fn test_where_clause_literal_rewritten() {
        let pq = parameterize(b"MATCH (a) WHERE a.id = 9 RETURN a").expect("rewrites");
        assert_eq!(norm_str(&pq), "MATCH (a) WHERE a.id = $__p0 RETURN a");
        assert!(matches!(pq.auto_params[0].1, Value::Int(9)));
    }

    #[test]
    fn test_normalized_text_reparses() {
        // The rewrite must produce grammatically valid Cypher.
        let inputs: &[&[u8]] = &[
            b"MATCH (a:N {id: 3, name: 'x'}) RETURN a.id",
            b"MATCH (a) WHERE a.id = 9 AND a.w > 1.5 RETURN a LIMIT 3",
            b"CREATE (:Person {id: 1, name: 'alice'})",
            b"MATCH (a:N {id: 3})-[*1..3]->(b) RETURN b",
        ];
        for input in inputs {
            let pq = parameterize(input).expect("rewrites");
            super::super::parse_cypher(&pq.normalized).unwrap_or_else(|e| {
                panic!(
                    "normalized text must reparse: {e:?} — {:?}",
                    core::str::from_utf8(&pq.normalized)
                )
            });
        }
    }
}

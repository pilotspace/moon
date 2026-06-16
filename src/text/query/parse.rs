//! Recursive-descent parser for the FT.SEARCH query subset (task fts-query-combinators 2a).
//!
//! Grammar (frozen §3 @ v1, RediSearch DIALECT-2 precedence — AND > modifiers > OR):
//! ```text
//!   query      = union                         (empty -> EmptyQuery)
//!   union      = intersect ( '|' intersect )*   (OR — lowest precedence)
//!   intersect  = factor+                        (implicit AND by juxtaposition)
//!   factor     = group | field_clause | term
//!   group      = '(' union ')'
//!   field_clause = '@' name ':' ( tag | numeric | group | term+ )   (field pushed onto leaf Terms)
//!   tag        = '{' value ( '|' value )* '}'
//!   numeric    = '[' bound bound ']'            (bound may be '(' -prefixed exclusive; ±inf ok)
//!   term       = WORD ( '*' prefix | wrapped in %..% fuzzy )?
//! ```
//! Pure: bytes + schema -> AST. Tokens are RAW; analysis happens in the evaluator (2b). The parser
//! NEVER panics (M7): every malformed shape resolves to a [`QueryError`]. Numeric-bound parsing is
//! re-implemented locally (≈ the command-layer `parse_numeric_bound`) so `text/` keeps NO upward
//! dependency on `command/` — same exclusive/±inf semantics, asserted by `test_numeric_exclusive_and_inf`.

use super::ast::{QueryError, QueryNode};
use crate::text::store::TermModifier;
use bytes::Bytes;

/// Max parenthesis-nesting depth — guards the recursive descent against stack exhaustion on
/// pathological input like `((((((…))))))`. Exceeding it is a `Syntax` error, never a crash.
const MAX_DEPTH: usize = 64;

/// Resolved kind of an `@field` reference.
#[derive(Clone, Copy)]
enum FieldRef {
    /// A TEXT field at this index (into the index's text fields) — needed by `search_field`.
    Text(usize),
    Tag,
    Numeric,
}

/// Field-name → kind resolver for the parser. Built from an index (`from_index`) in production or
/// from plain names (`from_names`) in tests. Resolution is ASCII-case-insensitive, matching the
/// rest of the text engine (`@Status` resolves `status`).
pub struct QuerySchema {
    text: Vec<Bytes>,
    tag: Vec<Bytes>,
    numeric: Vec<Bytes>,
}

impl QuerySchema {
    /// Build from field-name string slices (test/general construction).
    pub fn from_names(text: &[&str], tag: &[&str], numeric: &[&str]) -> Self {
        let conv = |xs: &[&str]| {
            xs.iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect()
        };
        Self {
            text: conv(text),
            tag: conv(tag),
            numeric: conv(numeric),
        }
    }

    /// Build from a live `TextIndex`'s declared field defs.
    pub fn from_index(idx: &crate::text::store::TextIndex) -> Self {
        Self {
            text: idx
                .text_fields
                .iter()
                .map(|f| f.field_name.clone())
                .collect(),
            tag: idx
                .tag_fields
                .iter()
                .map(|f| f.field_name.clone())
                .collect(),
            numeric: idx
                .numeric_fields
                .iter()
                .map(|f| f.field_name.clone())
                .collect(),
        }
    }

    fn resolve(&self, name: &[u8]) -> Option<FieldRef> {
        if let Some(i) = self
            .text
            .iter()
            .position(|f| f.as_ref().eq_ignore_ascii_case(name))
        {
            return Some(FieldRef::Text(i));
        }
        if self
            .tag
            .iter()
            .any(|f| f.as_ref().eq_ignore_ascii_case(name))
        {
            return Some(FieldRef::Tag);
        }
        if self
            .numeric
            .iter()
            .any(|f| f.as_ref().eq_ignore_ascii_case(name))
        {
            return Some(FieldRef::Numeric);
        }
        None
    }
}

/// Parse an FT.SEARCH query string into a [`QueryNode`] AST.
///
/// Returns `Err(QueryError::EmptyQuery)` for blank input, and the matching named error for any
/// malformed structure. Never panics.
pub fn parse_query(input: &[u8], schema: &QuerySchema) -> Result<QueryNode, QueryError> {
    let mut p = Parser {
        input,
        pos: 0,
        schema,
    };
    p.skip_ws();
    if p.at_end() {
        return Err(QueryError::EmptyQuery);
    }
    let node = p.parse_union(0)?;
    p.skip_ws();
    if !p.at_end() {
        // Trailing junk (e.g. a stray `)`) — unbalanced structure.
        return Err(QueryError::Syntax);
    }
    Ok(node)
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
    schema: &'a QuerySchema,
}

#[inline]
fn is_term_byte(b: u8) -> bool {
    !b.is_ascii_whitespace() && !matches!(b, b'|' | b'(' | b')' | b'{' | b'}' | b'[' | b']' | b'@')
}

impl Parser<'_> {
    #[inline]
    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    #[inline]
    fn at_end(&self) -> bool {
        self.pos >= self.input.len()
    }

    #[inline]
    fn skip_ws(&mut self) {
        while let Some(b) = self.peek() {
            if b.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    /// union = intersect ( '|' intersect )*
    fn parse_union(&mut self, depth: usize) -> Result<QueryNode, QueryError> {
        let mut branches = vec![self.parse_intersect(depth)?];
        loop {
            self.skip_ws();
            if self.peek() == Some(b'|') {
                self.pos += 1;
                branches.push(self.parse_intersect(depth)?);
            } else {
                break;
            }
        }
        Ok(if branches.len() == 1 {
            branches.pop().unwrap_or(QueryNode::Empty)
        } else {
            QueryNode::Or(branches)
        })
    }

    /// intersect = factor+  (at least one factor required)
    fn parse_intersect(&mut self, depth: usize) -> Result<QueryNode, QueryError> {
        let mut factors = Vec::new();
        loop {
            self.skip_ws();
            match self.parse_factor(depth)? {
                Some(node) => factors.push(node),
                None => break,
            }
        }
        match factors.len() {
            0 => Err(QueryError::Syntax), // empty branch (e.g. "a |", "| b")
            1 => Ok(factors.pop().unwrap_or(QueryNode::Empty)),
            _ => Ok(QueryNode::And(factors)),
        }
    }

    /// factor = group | field_clause | term ; returns None at a boundary ('|', ')', end).
    fn parse_factor(&mut self, depth: usize) -> Result<Option<QueryNode>, QueryError> {
        self.skip_ws();
        match self.peek() {
            None => Ok(None),
            Some(b'|') | Some(b')') => Ok(None),
            Some(b'(') => {
                let node = self.parse_group(depth)?;
                Ok(Some(node))
            }
            Some(b'@') => Ok(Some(self.parse_field_clause(depth)?)),
            // A tag/numeric delimiter or stray closer with no field in front is malformed.
            Some(b'{') | Some(b'}') | Some(b'[') | Some(b']') => Err(QueryError::Syntax),
            Some(_) => Ok(Some(self.parse_term(None)?)),
        }
    }

    /// group = '(' union ')'  — an empty group "()" is EmptyQuery.
    fn parse_group(&mut self, depth: usize) -> Result<QueryNode, QueryError> {
        if depth + 1 >= MAX_DEPTH {
            return Err(QueryError::Syntax);
        }
        self.pos += 1; // consume '('
        self.skip_ws();
        if self.peek() == Some(b')') {
            self.pos += 1;
            return Err(QueryError::EmptyQuery);
        }
        let node = self.parse_union(depth + 1)?;
        self.skip_ws();
        if self.peek() != Some(b')') {
            return Err(QueryError::Syntax);
        }
        self.pos += 1; // consume ')'
        Ok(node)
    }

    /// field_clause = '@' name ':' ( tag | numeric | group | term+ )
    fn parse_field_clause(&mut self, depth: usize) -> Result<QueryNode, QueryError> {
        self.pos += 1; // consume '@'
        let start = self.pos;
        while let Some(b) = self.peek() {
            if is_term_byte(b) && b != b':' {
                self.pos += 1;
            } else {
                break;
            }
        }
        let name = &self.input[start..self.pos];
        if name.is_empty() || self.peek() != Some(b':') {
            return Err(QueryError::Syntax);
        }
        self.pos += 1; // consume ':'
        let field = match self.schema.resolve(name) {
            Some(f) => f,
            None => return Err(QueryError::UnknownField(Bytes::copy_from_slice(name))),
        };
        self.skip_ws();
        match (self.peek(), field) {
            (Some(b'{'), FieldRef::Tag) => self.parse_tag(name),
            (Some(b'['), FieldRef::Numeric) => self.parse_numeric(name),
            (Some(b'('), FieldRef::Text(idx)) => {
                let mut node = self.parse_group(depth)?;
                push_field(&mut node, idx);
                Ok(node)
            }
            (Some(b), FieldRef::Text(idx)) if is_term_byte(b) => {
                // term+ : consecutive bare terms all scoped to this text field.
                let mut terms = vec![self.parse_term(Some(idx))?];
                loop {
                    self.skip_ws();
                    match self.peek() {
                        Some(b) if is_term_byte(b) => terms.push(self.parse_term(Some(idx))?),
                        _ => break,
                    }
                }
                Ok(if terms.len() == 1 {
                    terms.pop().unwrap_or(QueryNode::Empty)
                } else {
                    QueryNode::And(terms)
                })
            }
            // wrong delimiter for the field's kind, or nothing after ':'.
            _ => Err(QueryError::Syntax),
        }
    }

    /// tag = '{' value ( '|' value )* '}'   — empty / blank values -> TagInvalid.
    fn parse_tag(&mut self, field: &[u8]) -> Result<QueryNode, QueryError> {
        self.pos += 1; // consume '{'
        let start = self.pos;
        while let Some(b) = self.peek() {
            if b == b'}' {
                break;
            }
            self.pos += 1;
        }
        if self.peek() != Some(b'}') {
            return Err(QueryError::Syntax); // unbalanced
        }
        let body = &self.input[start..self.pos];
        self.pos += 1; // consume '}'
        let mut values = Vec::new();
        for raw in body.split(|&b| b == b'|') {
            let v = trim(raw);
            if v.is_empty() {
                return Err(QueryError::TagInvalid);
            }
            values.push(Bytes::copy_from_slice(v));
        }
        if values.is_empty() {
            return Err(QueryError::TagInvalid);
        }
        Ok(QueryNode::Tag {
            field: Bytes::copy_from_slice(field),
            values,
        })
    }

    /// numeric = '[' bound bound ']'  — exactly two bounds; non-numeric / min>max -> NumericInvalid.
    fn parse_numeric(&mut self, field: &[u8]) -> Result<QueryNode, QueryError> {
        self.pos += 1; // consume '['
        let start = self.pos;
        while let Some(b) = self.peek() {
            if b == b']' {
                break;
            }
            self.pos += 1;
        }
        if self.peek() != Some(b']') {
            return Err(QueryError::Syntax); // unbalanced
        }
        let body = &self.input[start..self.pos];
        self.pos += 1; // consume ']'
        let parts: Vec<&[u8]> = body
            .split(|b| b.is_ascii_whitespace())
            .filter(|s| !s.is_empty())
            .collect();
        if parts.len() != 2 {
            return Err(QueryError::NumericInvalid);
        }
        let (min, min_excl) = parse_bound(parts[0]).ok_or(QueryError::NumericInvalid)?;
        let (max, max_excl) = parse_bound(parts[1]).ok_or(QueryError::NumericInvalid)?;
        if min > max {
            return Err(QueryError::NumericInvalid);
        }
        Ok(QueryNode::Numeric {
            field: Bytes::copy_from_slice(field),
            min,
            max,
            min_excl,
            max_excl,
        })
    }

    /// term = WORD with an optional prefix '*' or %..% fuzzy wrapper. `field` scopes it.
    fn parse_term(&mut self, field: Option<usize>) -> Result<QueryNode, QueryError> {
        self.skip_ws();
        let start = self.pos;
        while let Some(b) = self.peek() {
            if is_term_byte(b) {
                self.pos += 1;
            } else {
                break;
            }
        }
        let word = &self.input[start..self.pos];
        if word.is_empty() {
            return Err(QueryError::Syntax);
        }
        let (token, modifier) = classify_term(word)?;
        Ok(QueryNode::Term {
            field,
            token: Bytes::copy_from_slice(token),
            modifier,
        })
    }
}

/// Push `idx` onto every still-unscoped `Term` inside `node` (field-scoped group `@f:(…)`).
fn push_field(node: &mut QueryNode, idx: usize) {
    match node {
        QueryNode::Term { field, .. } => {
            if field.is_none() {
                *field = Some(idx);
            }
        }
        QueryNode::And(children) | QueryNode::Or(children) => {
            for c in children {
                push_field(c, idx);
            }
        }
        QueryNode::Tag { .. } | QueryNode::Numeric { .. } | QueryNode::Empty => {}
    }
}

/// Split a raw WORD into (raw token, modifier). Fuzzy `%t%`/`%%t%%`/`%%%t%%%` (distance = % count),
/// prefix `t*`, else exact. Empty/malformed -> Syntax.
fn classify_term(word: &[u8]) -> Result<(&[u8], TermModifier), QueryError> {
    // Fuzzy: symmetric leading/trailing '%' (1..=3).
    if word.first() == Some(&b'%') {
        let lead = word.iter().take_while(|&&b| b == b'%').count();
        let trail = word.iter().rev().take_while(|&&b| b == b'%').count();
        if lead == trail && (1..=3).contains(&lead) && word.len() > 2 * lead {
            let inner = &word[lead..word.len() - lead];
            if inner.iter().all(|&b| b != b'%') {
                return Ok((inner, TermModifier::Fuzzy(lead as u8)));
            }
        }
        return Err(QueryError::Syntax);
    }
    // Prefix: trailing '*'.
    if word.last() == Some(&b'*') {
        let inner = &word[..word.len() - 1];
        if inner.is_empty() || inner.contains(&b'*') {
            return Err(QueryError::Syntax);
        }
        return Ok((inner, TermModifier::Prefix));
    }
    Ok((word, TermModifier::Exact))
}

/// Parse a numeric bound: optional leading `(` (exclusive), then a finite number or ±inf.
/// Returns `None` on a non-numeric / NaN bound. Mirrors the command-layer `parse_numeric_bound`
/// semantics without depending on it (layering: `text/` must not import from `command/`).
fn parse_bound(s: &[u8]) -> Option<(f64, bool)> {
    let (excl, rest) = if s.first() == Some(&b'(') {
        (true, &s[1..])
    } else {
        (false, s)
    };
    let t = std::str::from_utf8(rest).ok()?.trim();
    let lower = t.to_ascii_lowercase();
    let v = match lower.as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => f64::INFINITY,
        "-inf" | "-infinity" => f64::NEG_INFINITY,
        _ => t.parse::<f64>().ok()?,
    };
    if v.is_nan() {
        return None;
    }
    Some((v, excl))
}

#[inline]
fn trim(s: &[u8]) -> &[u8] {
    let mut a = 0;
    let mut b = s.len();
    while a < b && s[a].is_ascii_whitespace() {
        a += 1;
    }
    while b > a && s[b - 1].is_ascii_whitespace() {
        b -= 1;
    }
    &s[a..b]
}

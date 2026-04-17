//! FT.SEARCH parser helpers.
//!
//! Split from the monolithic `ft_search.rs` in Phase 152 (Plan 02.5).
//! This is a pure refactor: no behavior change, no signature change, no
//! visibility change. All parser helpers (query string, clauses, filter
//! grammar) live here; dispatch + execution live in the sibling
//! `dispatch.rs` submodule.
//!
//! Module-level imports use `crate::` paths per CLAUDE.md Module Structure rule.

use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::command::vector_search::{extract_bulk, matches_keyword, parse_u32};
use crate::protocol::Frame;
use crate::vector::filter::FilterExpr;

// -- KNN query string parsing --

/// Parse "*=>[KNN <k> @<field> $<param>]" query string.
/// Returns (k, field_name, param_name) on success.
///
/// `field_name` is `Some(name)` if the query contains `@field_name` (without the `@` prefix),
/// or `None` if the token after k starts with `$` directly (backward compat for queries
/// where @field is omitted or unrecognized).
pub(crate) fn parse_knn_query(query: &[u8]) -> Option<(usize, Option<Bytes>, Bytes)> {
    let s = std::str::from_utf8(query).ok()?;
    let knn_start = s.find("KNN ")?;
    let after_knn = &s[knn_start + 4..];

    // Parse k (first number after KNN)
    let k_end = after_knn.find(' ')?;
    let k: usize = after_knn[..k_end].trim().parse().ok()?;

    // Parse @field_name (optional — may be $param directly)
    let after_k = after_knn[k_end + 1..].trim_start();
    let (field_name, param_start) = if after_k.starts_with('@') {
        let field_end = after_k.find(' ').unwrap_or(after_k.len());
        let name = &after_k[1..field_end]; // strip @
        let remaining = if field_end < after_k.len() {
            after_k[field_end + 1..].trim_start()
        } else {
            ""
        };
        (Some(Bytes::from(name.to_owned())), remaining)
    } else {
        (None, after_k)
    };

    // Parse $param_name
    let param_str = param_start.trim().trim_end_matches(']');
    if !param_str.starts_with('$') {
        return None;
    }
    let param_name = &param_str[1..];
    Some((k, field_name, Bytes::from(param_name.to_owned())))
}

/// Extract a named parameter blob from PARAMS section.
/// Format: ... PARAMS <count> <name1> <blob1> <name2> <blob2> ...
pub(crate) fn extract_param_blob(args: &[Frame], param_name: &[u8]) -> Option<Bytes> {
    // Find PARAMS keyword starting after index_name and query
    let mut i = 2;
    while i < args.len() {
        if matches_keyword(&args[i], b"PARAMS") {
            i += 1;
            if i >= args.len() {
                return None;
            }
            let count = parse_u32(&args[i])? as usize;
            i += 1;
            // Iterate through name/value pairs
            for _ in 0..count / 2 {
                if i + 1 >= args.len() {
                    return None;
                }
                let name = extract_bulk(&args[i])?;
                i += 1;
                let value = extract_bulk(&args[i])?;
                i += 1;
                if name.eq_ignore_ascii_case(param_name) {
                    return Some(value);
                }
            }
            return None;
        }
        i += 1;
    }
    None
}

// -- LIMIT parsing --

/// Parse LIMIT clause from FT.SEARCH args.
/// Scans args (starting after index_name and query) for "LIMIT" keyword,
/// then reads the next two args as offset and count (both usize).
///
/// Default: (0, usize::MAX) -- return all results (backward compatible).
/// LIMIT 0 0 is valid: returns total count only (no document entries).
pub(crate) fn parse_limit_clause(args: &[Frame]) -> (usize, usize) {
    let mut i = 2;
    while i < args.len() {
        if matches_keyword(&args[i], b"LIMIT") {
            i += 1;
            let offset = if i < args.len() {
                parse_usize(&args[i]).unwrap_or(0)
            } else {
                return (0, usize::MAX);
            };
            i += 1;
            let count = if i < args.len() {
                parse_usize(&args[i]).unwrap_or(usize::MAX)
            } else {
                return (offset, usize::MAX);
            };
            return (offset, count);
        }
        i += 1;
    }
    (0, usize::MAX)
}

/// Parse a frame as usize. Similar to parse_u32 but returns usize for pagination.
pub(crate) fn parse_usize(frame: &Frame) -> Option<usize> {
    match frame {
        Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        Frame::Integer(n) => usize::try_from(*n).ok(),
        _ => None,
    }
}

// -- Filter parsing --

/// Parse FILTER clause from FT.SEARCH args.
/// Looks for "FILTER" keyword after the query string, parses the filter expression.
///
/// Supported syntax:
///   @field:{value}              -- tag equality
///   @field:[min max]            -- numeric range
///   @field:{value} @field2:[a b] -- implicit AND of multiple conditions
pub(crate) fn parse_filter_clause(args: &[Frame]) -> Option<FilterExpr> {
    // Find FILTER keyword in args (after index_name and query)
    let mut i = 2;
    while i < args.len() {
        if matches_keyword(&args[i], b"FILTER") {
            i += 1;
            if i >= args.len() {
                return None;
            }
            let filter_str = extract_bulk(&args[i])?;
            return parse_filter_string(&filter_str);
        }
        i += 1;
    }
    None
}

/// Parse inline filter prefix from query string (RediSearch-compatible).
///
/// Extracts filter expressions from before the `=>` arrow in queries like:
///   `@category:{science}=>[KNN 3 @vec $q]`
///   `@ts:[1000 2000]=>[KNN 5 @vec $q]`
///   `@active:{true} @topic:{ml}=>[KNN 3 @vec $q]`
///
/// Returns `None` if query starts with `*=>` (no filter prefix).
pub(super) fn parse_inline_filter(query: &[u8]) -> Option<FilterExpr> {
    let s = std::str::from_utf8(query).ok()?;
    let arrow_pos = s.find("=>")?;
    let prefix = s[..arrow_pos].trim();
    if prefix.is_empty() || prefix == "*" {
        return None;
    }
    parse_filter_string(prefix.as_bytes())
}

/// Parse SESSION clause from FT.SEARCH args.
/// Looks for "SESSION" keyword after the query string, returns the session key.
///
/// Syntax: FT.SEARCH idx query ... SESSION sess:conv1
pub fn parse_session_clause(args: &[Frame]) -> Option<Bytes> {
    let mut i = 2;
    while i < args.len() {
        if matches_keyword(&args[i], b"SESSION") {
            i += 1;
            if i >= args.len() {
                return None;
            }
            return extract_bulk(&args[i]);
        }
        i += 1;
    }
    None
}

/// Parse filter string like "@field:{value}" or "@field:[min max]" or "@field:[lon lat radius_km]"
/// Multiple conditions are implicitly ANDed.
/// Datetime filtering: epoch seconds use NumRange -- no special handling needed (FNDN-02).
fn parse_filter_string(s: &[u8]) -> Option<FilterExpr> {
    let s = std::str::from_utf8(s).ok()?;
    let mut exprs: Vec<FilterExpr> = Vec::new();
    let mut pos = 0;
    while pos < s.len() {
        // Skip whitespace
        while pos < s.len() && s.as_bytes()[pos] == b' ' {
            pos += 1;
        }
        if pos >= s.len() {
            break;
        }
        if s.as_bytes()[pos] != b'@' {
            return None;
        }
        pos += 1; // skip @

        // Read field name until : or { or [
        let field_start = pos;
        while pos < s.len() && !matches!(s.as_bytes()[pos], b':' | b'{' | b'[') {
            pos += 1;
        }
        let field = Bytes::from(s[field_start..pos].to_owned());
        if pos >= s.len() {
            return None;
        }

        // Determine type
        if s.as_bytes()[pos] == b':' {
            pos += 1; // skip :
        }

        if pos < s.len() && s.as_bytes()[pos] == b'{' {
            // Tag: @field:{value}
            pos += 1;
            let val_start = pos;
            while pos < s.len() && s.as_bytes()[pos] != b'}' {
                pos += 1;
            }
            let val_str = &s[val_start..pos];
            if pos < s.len() {
                pos += 1; // skip }
            }
            // Boolean detection: "true"/"false" (case-insensitive) -> BoolEq
            if val_str.eq_ignore_ascii_case("true") {
                exprs.push(FilterExpr::BoolEq { field, value: true });
            } else if val_str.eq_ignore_ascii_case("false") {
                exprs.push(FilterExpr::BoolEq {
                    field,
                    value: false,
                });
            } else if val_str.contains(' ') {
                // Multi-word content -> full-text match (AND semantics)
                let terms: Vec<Bytes> = val_str
                    .split_whitespace()
                    .map(|t| Bytes::from(t.to_owned()))
                    .collect();
                exprs.push(FilterExpr::TextMatch { field, terms });
            } else {
                let value = Bytes::from(val_str.to_owned());
                exprs.push(FilterExpr::TagEq { field, value });
            }
        } else if pos < s.len() && s.as_bytes()[pos] == b'[' {
            // Numeric range: @field:[min max]
            pos += 1;
            let range_start = pos;
            while pos < s.len() && s.as_bytes()[pos] != b']' {
                pos += 1;
            }
            let range_str = &s[range_start..pos];
            if pos < s.len() {
                pos += 1; // skip ]
            }
            let parts: Vec<&str> = range_str.split_whitespace().collect();
            if parts.len() == 3 {
                // Geo radius: @field:[lon lat radius_km]
                let lon: f64 = parts[0].parse().ok()?;
                let lat: f64 = parts[1].parse().ok()?;
                let radius_km: f64 = parts[2].parse().ok()?;
                exprs.push(FilterExpr::GeoRadius {
                    field,
                    lon,
                    lat,
                    radius_km,
                });
            } else if parts.len() == 2 {
                let min: f64 = parts[0].parse().ok()?;
                let max: f64 = parts[1].parse().ok()?;
                if (min - max).abs() < f64::EPSILON {
                    exprs.push(FilterExpr::NumEq {
                        field,
                        value: OrderedFloat(min),
                    });
                } else {
                    exprs.push(FilterExpr::NumRange {
                        field,
                        min: OrderedFloat(min),
                        max: OrderedFloat(max),
                    });
                }
            } else {
                return None;
            }
        } else {
            return None;
        }
    }
    // Combine with AND
    if exprs.is_empty() {
        return None;
    }
    let mut result = exprs.remove(0);
    for expr in exprs {
        result = FilterExpr::And(Box::new(result), Box::new(expr));
    }
    Some(result)
}

// -- SPARSE clause parsing --

/// Parse SPARSE clause from FT.SEARCH args.
///
/// Syntax: `... SPARSE @field_name $param_name ...`
///
/// Returns `(field_name, param_name)` where field_name has the `@` stripped
/// and param_name has the `$` stripped.
pub(crate) fn parse_sparse_clause(args: &[Frame]) -> Option<(Bytes, Bytes)> {
    let mut i = 2;
    while i < args.len() {
        if matches_keyword(&args[i], b"SPARSE") {
            i += 1;
            if i >= args.len() {
                return None;
            }
            // Read @field_name
            let field_raw = extract_bulk(&args[i])?;
            let field_str = std::str::from_utf8(&field_raw).ok()?;
            let field_name = if let Some(stripped) = field_str.strip_prefix('@') {
                Bytes::from(stripped.to_owned())
            } else {
                field_raw
            };
            i += 1;
            if i >= args.len() {
                return None;
            }
            // Read $param_name
            let param_raw = extract_bulk(&args[i])?;
            let param_str = std::str::from_utf8(&param_raw).ok()?;
            let param_name = if let Some(stripped) = param_str.strip_prefix('$') {
                Bytes::from(stripped.to_owned())
            } else {
                param_raw
            };
            return Some((field_name, param_name));
        }
        i += 1;
    }
    None
}

/// Parse optional RANGE threshold from args.
/// Returns `Some(f32)` if RANGE keyword found with a valid float value.
/// Used by FT.SEARCH to post-filter results by distance/similarity threshold.
pub(crate) fn parse_range_clause(args: &[Frame]) -> Option<f32> {
    for i in 0..args.len().saturating_sub(1) {
        if matches_keyword(&args[i], b"RANGE") {
            if let Some(val_bytes) = extract_bulk(&args[i + 1]) {
                return std::str::from_utf8(&val_bytes).ok()?.parse::<f32>().ok();
            }
        }
    }
    None
}

/// Parse optional `EXPAND GRAPH depth:N` clause from FT.SEARCH args.
///
/// Returns `None` if no EXPAND clause, `Some(depth)` if present.
/// Accepts both `EXPAND GRAPH depth:N` and `EXPAND GRAPH N` syntax.
#[cfg(feature = "graph")]
pub(crate) fn parse_expand_clause(args: &[Frame]) -> Option<u32> {
    for i in 0..args.len() {
        if matches_keyword(&args[i], b"EXPAND") {
            if i + 1 < args.len() && matches_keyword(&args[i + 1], b"GRAPH") {
                if i + 2 < args.len() {
                    // Parse "depth:N" syntax
                    if let Some(depth_arg) = extract_bulk(&args[i + 2]) {
                        if let Some(stripped) = depth_arg.strip_prefix(b"depth:") {
                            if let Ok(s) = std::str::from_utf8(stripped) {
                                return s.parse::<u32>().ok();
                            }
                        }
                        // Also accept bare integer
                        return parse_u32(&args[i + 2]);
                    }
                }
            }
        }
    }
    None
}

/// Parse AS_OF <timestamp_ms> clause from FT.SEARCH args.
///
/// Returns the wall-clock timestamp (i64 Unix millis) if present.
/// Used with TemporalRegistry.lsn_at() to resolve to a snapshot LSN.
pub(crate) fn parse_as_of_clause(args: &[Frame]) -> Option<i64> {
    for i in 0..args.len().saturating_sub(1) {
        if matches_keyword(&args[i], b"AS_OF") {
            if let Some(val_bytes) = extract_bulk(&args[i + 1]) {
                return std::str::from_utf8(&val_bytes)
                    .ok()?
                    .trim()
                    .parse::<i64>()
                    .ok();
            }
        }
    }
    None
}

//! FT.SEARCH query parsing — the recursive-descent parser + AST (task fts-query-combinators 2a).
//!
//! `parse_query(bytes, schema)` turns a query string into a [`QueryNode`] tree over the frozen
//! RediSearch subset (terms+modifiers · implicit-AND · OR `|` · grouping · `@field:` clauses ·
//! TAG `{a|b}` · NUMERIC `[min max]`), with DIALECT-2 precedence and five named error codes.
//! The companion evaluator (`fts-query-eval-dispatch`, 2b) folds a `QueryNode` to a matched
//! doc-id `RoaringBitmap`; `fts-search-count-semantics` then counts that set's cardinality.
//!
//! The parser is a pure function (no index/analyzer): tokens are RAW and analyzed at eval time.

mod ast;
mod eval;
mod parse;

pub use ast::{QueryError, QueryNode};
pub use eval::{
    collect_df_field_terms, collect_highlight_terms, eval_query, eval_query_counted, eval_set,
};
pub use parse::{QuerySchema, parse_query};

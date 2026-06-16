#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::text::query::{parse_query, QuerySchema};

// Fuzz the FT.SEARCH query parser (task fts-query-combinators 2a).
//
// `parse_query` runs on untrusted client query strings and MUST never panic / hang / blow the
// stack — every malformed shape has to resolve to a `QueryError` (contract M7). The schema mixes
// the three field kinds so tag / numeric / text clause paths are all exercised by the input.
fuzz_target!(|data: &[u8]| {
    let schema = QuerySchema::from_names(&["body", "title"], &["tag", "color"], &["price"]);
    let _ = parse_query(data, &schema);
});

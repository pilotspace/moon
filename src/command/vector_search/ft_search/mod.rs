//! FT.SEARCH command — directory module (split from monolithic `ft_search.rs` in Phase 152).
//!
//! Split rationale: the original 1512-LOC single file sat at CLAUDE.md's 1500-line cap.
//! Phase 152's HYBRID detection branch (Plan 04 Task 3) would push it over. Splitting
//! preserves all public API paths via re-exports — every caller continues to resolve
//! `crate::command::vector_search::ft_search::X` unchanged.
//!
//! Layout:
//! - `parse.rs` — parser helpers (KNN query string, FILTER, LIMIT, SPARSE, RANGE,
//!   SESSION, EXPAND, parse_filter_string, etc.)
//! - `execute.rs` — local execution (`search_local`, `search_local_filtered`,
//!   `search_local_raw`, `SearchRawResult`, `apply_range_filter`,
//!   `parse_sparse_query_blob`).
//! - `response.rs` — Frame builders (`build_search_response`, `build_hybrid_response`,
//!   `build_combined_response`), cross-shard `merge_search_results`,
//!   `parse_ft_search_args`, `extract_score_from_fields`,
//!   `extract_seeds_from_response`.
//! - `dispatch.rs` — top-level `ft_search()` entry and `ft_search_with_graph()`
//!   orchestration (routing between hybrid / sparse / dense paths, session +
//!   range handling).
//!
//! The 4-file layout (vs. the plan's originally-proposed 3 files) is a deviation
//! documented in the plan SUMMARY: a single `dispatch.rs` containing entry +
//! execute + response helpers would have exceeded the plan's explicit `< 800 LOC
//! per file` acceptance criterion (dispatch would have been ~1145 LOC). Splitting
//! execute.rs and response.rs out keeps every resulting file under that threshold
//! while preserving the plan's parse/dispatch logical boundary — dispatch.rs
//! still owns routing, parse.rs still owns parsing, and the two new files are
//! cohesive implementation details (execution mechanics + Frame building) that
//! dispatch.rs delegates to.

pub mod dispatch;
pub mod execute;
pub mod parse;
pub mod response;

// Re-export to preserve the public API path `crate::command::vector_search::ft_search::X`.
// The glob re-exports pull in all `pub` and `pub(crate)` items at their original
// visibility — external callers see no change.
pub use dispatch::*;
pub use execute::*;
pub use parse::*;
pub use response::*;

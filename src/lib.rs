// Crate-wide clippy suppressions — HYGIENE-03 audited 2026-04-10.
// Reduced from 58 to 32 entries. 18 zero-violation lints removed.
// Correctness and performance lints remain enabled.
#![allow(unsafe_op_in_unsafe_fn)] // 156 unsafe blocks use SAFETY comments; migrating to unsafe{} inside unsafe fn is deferred
#![allow(
    clippy::too_many_arguments,        // 11 sites: conn_accept, persistence_tick, sorted_set
    clippy::collapsible_if,            // 205 sites: pervasive if-let chains in command dispatch
    clippy::type_complexity,           // 18 sites: generic tower of Fn+Future bounds in server/shard
    clippy::redundant_closure,         // 26 sites: closures needed for lifetime capture in async contexts
    clippy::explicit_auto_deref,       // 6 sites: explicit deref improves readability in pattern matches
    clippy::if_same_then_else,         // 3 sites: intentional identical branches for future divergence
    clippy::unnecessary_cast,          // 5 sites: cross-platform u32/usize casts for clarity
    clippy::manual_range_contains,     // 9 sites: chained comparisons read better in protocol parsing
    clippy::manual_div_ceil,           // 28 sites: SIMD/vector alignment math — explicit form clearer
    clippy::single_match,              // 3 sites: single-arm match + wildcard for future expansion
    clippy::map_entry,                 // 4 sites: entry API doesn't fit conditional-insert patterns
    clippy::multiple_bound_locations,  // 6 sites: bounds split across impl+fn for readability
    clippy::iter_kv_map,               // 2 sites: explicit key/value destructuring in hash iteration
    clippy::derivable_impls,           // 12 sites: explicit Default impls document non-obvious defaults
    clippy::should_implement_trait,    // 2 sites: from_str methods that don't match FromStr contract
    clippy::needless_range_loop,       // 4 sites: index variable needed for parallel array access
    clippy::len_without_is_empty,      // 1 site: len() on types where is_empty() is meaningless
    clippy::new_without_default,       // 12 sites: new() takes args or has side effects
    clippy::unnecessary_map_or,        // 10 sites: style preference in option chains
    clippy::manual_flatten,            // 2 sites: explicit nested iteration for clarity
    clippy::explicit_counter_loop,     // 4 sites: counter used for non-sequential indexing
    clippy::op_ref,                    // 4 sites: explicit &ref in operator overloads for clarity
    clippy::for_kv_map,               // 5 sites: destructured iteration in config/metadata paths
    clippy::ptr_arg,                   // 2 sites: &Vec/&String in FFI boundaries
    clippy::manual_ok_err,             // 3 sites: explicit match arms for error context
    clippy::empty_line_after_doc_comments, // 2 sites: spacing preference for section breaks
    clippy::only_used_in_recursion,    // 2 sites: parameter threads through recursive tree walks
    clippy::let_and_return,            // 2 sites: named binding before return for debuggability
    clippy::filter_next,               // 1 site: filter().next() reads better than find() in context
    clippy::unwrap_or_default,         // 1 site: explicit unwrap_or(Vec::new()) for type inference
    clippy::mem_replace_with_default,  // 3 sites: tokio handler guard patterns
    clippy::nonminimal_bool            // 1 site: tokio handler complex bool expression
)]

pub mod acl;
pub mod admin;
pub mod auth_ratelimit;
pub mod blocking;
pub mod cluster;
pub mod command;
pub mod config;
pub mod error;
pub mod io;
pub mod persistence;
pub mod protocol;
pub mod pubsub;
pub mod replication;
pub mod runtime;
pub mod scripting;
pub mod server;
pub mod shard;
pub mod storage;
#[cfg(any(feature = "runtime-tokio", feature = "runtime-monoio"))]
pub mod tls;
pub mod tracking;
pub mod vector;
#[cfg(feature = "graph")]
pub mod graph;

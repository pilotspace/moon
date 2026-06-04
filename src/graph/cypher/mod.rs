//! Cypher parser for the Moon graph engine.
//!
//! Hand-rolled recursive descent parser on a `logos`-based lexer.
//! Supports: MATCH, WHERE, RETURN, ORDER BY, LIMIT, SKIP, CREATE,
//! DELETE, SET, MERGE, WITH, UNWIND, CALL ... YIELD.
//!
//! Parameterized queries (`$param`) prevent Cypher injection.
//! Nesting depth is limited (`DEFAULT_MAX_NESTING_DEPTH`) to prevent a
//! stack-overflow DoS — deep nesting returns `NestingDepthExceeded`, never aborts.

pub mod ast;
pub mod executor;
pub mod lexer;
pub mod parser;
pub mod planner;

pub use ast::{Clause, CypherQuery, Expr};
pub use executor::{ExecResult, OpProfile, ProfileResult, Value};
pub use parser::{CypherError, DEFAULT_MAX_NESTING_DEPTH, Parser};
pub use planner::{CostEstimate, PhysicalPlan, PlanCache, Strategy};

/// Parse a Cypher query from a byte slice.
///
/// Uses `DEFAULT_MAX_NESTING_DEPTH` so pathologically nested input returns
/// `NestingDepthExceeded` instead of overflowing the (2 MiB worker-thread) stack.
pub fn parse_cypher(input: &[u8]) -> Result<CypherQuery, CypherError> {
    let mut parser = Parser::new(input, DEFAULT_MAX_NESTING_DEPTH);
    parser.parse()
}

/// Check if a Cypher query is read-only (no write clauses).
pub fn is_read_only(query: &CypherQuery) -> bool {
    query.is_read_only()
}

//! Cypher parser for the Moon graph engine.
//!
//! Hand-rolled recursive descent parser on a `logos`-based lexer.
//! Supports: MATCH, WHERE, RETURN, ORDER BY, LIMIT, SKIP, CREATE,
//! DELETE, SET, MERGE, WITH, UNWIND, CALL ... YIELD.
//!
//! Parameterized queries (`$param`) prevent Cypher injection.
//! Nesting depth is limited (default 64) to prevent stack overflow DoS.

pub mod ast;
pub mod executor;
pub mod lexer;
pub mod parser;
pub mod planner;

pub use ast::{Clause, CypherQuery, Expr};
pub use executor::{ExecResult, OpProfile, ProfileResult, Value};
pub use parser::{CypherError, Parser};
pub use planner::{CostEstimate, PhysicalPlan, PlanCache, Strategy};

/// Parse a Cypher query from a byte slice.
///
/// Uses the default nesting depth limit of 64.
pub fn parse_cypher(input: &[u8]) -> Result<CypherQuery, CypherError> {
    let mut parser = Parser::new(input, 64);
    parser.parse()
}

/// Check if a Cypher query is read-only (no write clauses).
pub fn is_read_only(query: &CypherQuery) -> bool {
    query.is_read_only()
}

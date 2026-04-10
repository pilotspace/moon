//! Basic Cypher query planner with plan caching.
//!
//! Compiles a `CypherQuery` AST into a `PhysicalPlan` — a sequence of
//! operators that the executor can evaluate. Plans are cached by xxhash
//! of the original Cypher string.

use std::collections::HashMap;
use std::sync::Arc;

use crate::graph::cypher::ast::*;

/// A compiled physical plan: a sequence of operators.
#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub operators: Vec<PhysicalOp>,
}

/// Individual physical operators in the execution pipeline.
#[derive(Debug, Clone)]
pub enum PhysicalOp {
    /// Scan nodes by label.
    NodeScan {
        variable: String,
        label: Option<String>,
    },
    /// Expand along edges from a source variable.
    Expand {
        source: String,
        target: String,
        edge_types: Vec<String>,
        direction: EdgeDirection,
        min_hops: u32,
        max_hops: u32,
    },
    /// Filter rows by a predicate expression.
    Filter {
        expr: Expr,
    },
    /// Project specific columns.
    Project {
        items: Vec<ReturnItem>,
        distinct: bool,
    },
    /// Sort by expressions.
    Sort {
        items: Vec<(Expr, bool)>,
    },
    /// Limit output rows.
    Limit {
        count: Expr,
    },
    /// Skip output rows.
    Skip {
        count: Expr,
    },
    /// Create nodes/edges.
    CreatePattern {
        patterns: Vec<Pattern>,
    },
    /// Delete nodes/edges.
    DeleteEntities {
        exprs: Vec<Expr>,
        detach: bool,
    },
    /// Set properties/labels.
    SetProperties {
        items: Vec<SetItem>,
    },
    /// Procedure call.
    ProcedureCall {
        procedure: String,
        args: Vec<Expr>,
        yields: Vec<YieldItem>,
    },
    /// Unwind a list into rows.
    Unwind {
        expr: Expr,
        alias: String,
    },
}

/// Error during plan compilation.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanError {
    /// Unsupported clause or expression.
    Unsupported(String),
}

impl core::fmt::Display for PlanError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Unsupported(msg) => write!(f, "unsupported: {msg}"),
        }
    }
}

/// Plan cache: maps xxhash of Cypher string to compiled plan.
pub struct PlanCache {
    cache: HashMap<u64, Arc<PhysicalPlan>>,
    max_entries: usize,
}

impl PlanCache {
    /// Create a new plan cache with the given maximum size.
    pub fn new(max_entries: usize) -> Self {
        Self {
            cache: HashMap::new(),
            max_entries,
        }
    }

    /// Look up a cached plan by query hash.
    pub fn get(&self, hash: u64) -> Option<Arc<PhysicalPlan>> {
        self.cache.get(&hash).cloned()
    }

    /// Insert a plan into the cache. Evicts oldest if at capacity.
    pub fn insert(&mut self, hash: u64, plan: Arc<PhysicalPlan>) {
        if self.cache.len() >= self.max_entries {
            // Simple eviction: remove first key (not ideal but sufficient for now).
            if let Some(&first_key) = self.cache.keys().next() {
                self.cache.remove(&first_key);
            }
        }
        self.cache.insert(hash, plan);
    }

    /// Number of cached plans.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Clear all cached plans.
    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

/// Compile a Cypher AST into a physical plan.
///
/// This is a basic compiler that translates clauses into operators.
/// A full cost-based optimizer is deferred to Phase 119.
pub fn compile(query: &CypherQuery) -> Result<PhysicalPlan, PlanError> {
    let mut ops = Vec::new();

    for clause in &query.clauses {
        match clause {
            Clause::Match(m) => {
                compile_match(m, &mut ops);
            }
            Clause::Where(w) => {
                ops.push(PhysicalOp::Filter {
                    expr: w.expr.clone(),
                });
            }
            Clause::Return(r) => {
                ops.push(PhysicalOp::Project {
                    items: r.items.clone(),
                    distinct: r.distinct,
                });
            }
            Clause::Create(c) => {
                ops.push(PhysicalOp::CreatePattern {
                    patterns: c.patterns.clone(),
                });
            }
            Clause::Delete(d) => {
                ops.push(PhysicalOp::DeleteEntities {
                    exprs: d.exprs.clone(),
                    detach: d.detach,
                });
            }
            Clause::Set(s) => {
                ops.push(PhysicalOp::SetProperties {
                    items: s.items.clone(),
                });
            }
            Clause::Merge(_) => {
                // MERGE is complex: pattern match + conditional create.
                // For now, emit a basic pattern.
                return Err(PlanError::Unsupported("MERGE not yet compiled".to_string()));
            }
            Clause::With(w) => {
                ops.push(PhysicalOp::Project {
                    items: w.items.clone(),
                    distinct: w.distinct,
                });
            }
            Clause::Unwind(u) => {
                ops.push(PhysicalOp::Unwind {
                    expr: u.expr.clone(),
                    alias: u.alias.clone(),
                });
            }
            Clause::Call(c) => {
                ops.push(PhysicalOp::ProcedureCall {
                    procedure: c.procedure.clone(),
                    args: c.args.clone(),
                    yields: c.yields.clone(),
                });
            }
            Clause::OrderBy(o) => {
                ops.push(PhysicalOp::Sort {
                    items: o.items.clone(),
                });
            }
            Clause::Limit(l) => {
                ops.push(PhysicalOp::Limit {
                    count: l.count.clone(),
                });
            }
            Clause::Skip(s) => {
                ops.push(PhysicalOp::Skip {
                    count: s.count.clone(),
                });
            }
        }
    }

    Ok(PhysicalPlan { operators: ops })
}

/// Compile a MATCH clause into scan + expand operators.
fn compile_match(m: &MatchClause, ops: &mut Vec<PhysicalOp>) {
    for pattern in &m.patterns {
        if pattern.nodes.is_empty() {
            continue;
        }

        // First node becomes a scan.
        let first = &pattern.nodes[0];
        ops.push(PhysicalOp::NodeScan {
            variable: first
                .variable
                .clone()
                .unwrap_or_else(|| "_anon".to_string()),
            label: first.labels.first().cloned(),
        });

        // Subsequent node+edge pairs become expands.
        for (i, edge) in pattern.edges.iter().enumerate() {
            let target = &pattern.nodes[i + 1];
            let (min_hops, max_hops) = edge.var_length.unwrap_or((1, 1));
            ops.push(PhysicalOp::Expand {
                source: first
                    .variable
                    .clone()
                    .unwrap_or_else(|| "_anon".to_string()),
                target: target
                    .variable
                    .clone()
                    .unwrap_or_else(|| format!("_anon_{}", i + 1)),
                edge_types: edge.edge_types.clone(),
                direction: edge.direction,
                min_hops,
                max_hops,
            });
        }
    }
}

/// Hash a Cypher query string for plan cache lookup.
pub fn hash_query(input: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(input, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::cypher::parse_cypher;

    #[test]
    fn test_compile_simple_match() {
        let query = parse_cypher(b"MATCH (n:Person) RETURN n").expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        assert!(!plan.operators.is_empty());
        assert!(matches!(plan.operators[0], PhysicalOp::NodeScan { .. }));
        assert!(matches!(
            plan.operators.last(),
            Some(PhysicalOp::Project { .. })
        ));
    }

    #[test]
    fn test_compile_match_with_expand() {
        let query =
            parse_cypher(b"MATCH (a:Person)-[:KNOWS]->(b) RETURN b").expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        assert!(plan
            .operators
            .iter()
            .any(|op| matches!(op, PhysicalOp::Expand { .. })));
    }

    #[test]
    fn test_compile_with_filter() {
        let query =
            parse_cypher(b"MATCH (n) WHERE n.age > 30 RETURN n").expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        assert!(plan
            .operators
            .iter()
            .any(|op| matches!(op, PhysicalOp::Filter { .. })));
    }

    #[test]
    fn test_plan_cache() {
        let mut cache = PlanCache::new(2);
        assert!(cache.is_empty());

        let plan = Arc::new(PhysicalPlan {
            operators: vec![],
        });
        cache.insert(42, plan.clone());
        assert_eq!(cache.len(), 1);
        assert!(cache.get(42).is_some());
        assert!(cache.get(99).is_none());

        // Fill and evict
        cache.insert(43, plan.clone());
        cache.insert(44, plan.clone());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_hash_query_deterministic() {
        let h1 = hash_query(b"MATCH (n) RETURN n");
        let h2 = hash_query(b"MATCH (n) RETURN n");
        assert_eq!(h1, h2);

        let h3 = hash_query(b"MATCH (m) RETURN m");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_compile_call_procedure() {
        let query = parse_cypher(
            b"CALL oxid.vector.search('Person', 'emb', 10, $v) YIELD node RETURN node",
        )
        .expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        assert!(plan
            .operators
            .iter()
            .any(|op| matches!(op, PhysicalOp::ProcedureCall { .. })));
    }
}

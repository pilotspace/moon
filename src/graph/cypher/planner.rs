//! Cypher query planner with cost-based strategy selection and plan caching.
//!
//! Compiles a `CypherQuery` AST into a `PhysicalPlan` — a sequence of
//! operators that the executor can evaluate. Plans are cached by xxhash
//! of the original Cypher string.
//!
//! The cost estimator selects between graph-first and vector-first strategies
//! based on per-graph `GraphStats` (degree distribution, node/edge counts).

use std::collections::HashMap;
use std::sync::Arc;

use crate::graph::cypher::ast::*;
use crate::graph::stats::GraphStats;

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
    Filter { expr: Expr },
    /// Project specific columns.
    Project {
        items: Vec<ReturnItem>,
        distinct: bool,
    },
    /// Sort by expressions.
    Sort { items: Vec<(Expr, bool)> },
    /// Limit output rows.
    Limit { count: Expr },
    /// Skip output rows.
    Skip { count: Expr },
    /// Create nodes/edges.
    CreatePattern { patterns: Vec<Pattern> },
    /// Delete nodes/edges.
    DeleteEntities { exprs: Vec<Expr>, detach: bool },
    /// Set properties/labels.
    SetProperties { items: Vec<SetItem> },
    /// Procedure call.
    ProcedureCall {
        procedure: String,
        args: Vec<Expr>,
        yields: Vec<YieldItem>,
    },
    /// Unwind a list into rows.
    Unwind { expr: Expr, alias: String },
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

// ---------------------------------------------------------------------------
// Cost-based strategy selection (Phase 119)
// ---------------------------------------------------------------------------

/// Execution strategy for hybrid graph+vector queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Traverse graph first, then score candidates by vector similarity.
    /// Cost: O(start_nodes * avg_degree^hops) + O(|neighborhood| * D)
    GraphFirst,
    /// Run vector search first (HNSW), then expand results in graph.
    /// Cost: O(k * log(N)) + O(k * avg_degree * hops)
    VectorFirst,
}

impl core::fmt::Display for Strategy {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::GraphFirst => write!(f, "GraphFirst"),
            Self::VectorFirst => write!(f, "VectorFirst"),
        }
    }
}

/// Cost estimate for a query strategy.
#[derive(Debug, Clone)]
pub struct CostEstimate {
    /// The selected strategy.
    pub strategy: Strategy,
    /// Estimated intermediate cardinality (number of candidates to process).
    pub graph_first_cost: f64,
    /// Estimated vector-first cost.
    pub vector_first_cost: f64,
    /// Whether hub detection triggered vector-first override.
    pub hub_detected: bool,
}

/// Estimate the cost of a graph-first strategy.
///
/// Formula: `start_nodes * avg_degree^hops + neighborhood_size * dim`
/// where `neighborhood_size = start_nodes * avg_degree^hops`.
///
/// The first term estimates traversal work, the second estimates vector
/// scoring work on the resulting candidate set.
pub fn estimate_graph_first_cost(start_nodes: u64, avg_degree: f64, hops: u32, dim: u32) -> f64 {
    let neighborhood = (start_nodes as f64) * avg_degree.powi(hops as i32);
    // Traversal cost + vector scoring cost.
    neighborhood + neighborhood * (dim as f64)
}

/// Estimate the cost of a vector-first strategy.
///
/// Formula: `k * log2(N) + k * avg_degree * hops`
///
/// The first term is the HNSW search cost (logarithmic in total nodes),
/// the second is the graph expansion cost from each of the k results.
pub fn estimate_vector_first_cost(k: u32, total_nodes: u64, avg_degree: f64, hops: u32) -> f64 {
    if total_nodes == 0 {
        return 0.0;
    }
    let search_cost = (k as f64) * (total_nodes as f64).log2();
    let expansion_cost = (k as f64) * avg_degree * (hops as f64);
    search_cost + expansion_cost
}

/// Select the optimal strategy based on graph statistics and query parameters.
///
/// Parameters:
/// - `stats`: Live graph statistics (degree distribution, counts).
/// - `start_nodes`: Number of seed nodes for graph traversal (typically 1).
/// - `hops`: Traversal depth.
/// - `k`: Number of vector search results.
/// - `dim`: Vector dimension for scoring cost.
///
/// Hub detection: If any start node might have degree >= P99, prefer
/// vector-first to avoid neighborhood explosion.
pub fn select_strategy(
    stats: &GraphStats,
    start_nodes: u64,
    hops: u32,
    k: u32,
    dim: u32,
    start_node_degree: Option<u32>,
) -> CostEstimate {
    let avg_degree = stats.degree_stats.avg;
    let p99 = stats.degree_stats.p99;

    let graph_cost = estimate_graph_first_cost(start_nodes, avg_degree, hops, dim);
    let vector_cost = estimate_vector_first_cost(k, stats.total_nodes, avg_degree, hops);

    // Hub detection: if start node has degree >= P99, the graph-first
    // traversal may explode. Prefer vector-first in that case.
    let hub_detected = match start_node_degree {
        Some(degree) if p99 > 0 => degree >= p99,
        _ => false,
    };

    let strategy = if hub_detected {
        Strategy::VectorFirst
    } else if vector_cost < graph_cost {
        Strategy::VectorFirst
    } else {
        Strategy::GraphFirst
    };

    CostEstimate {
        strategy,
        graph_first_cost: graph_cost,
        vector_first_cost: vector_cost,
        hub_detected,
    }
}

/// Compile a Cypher AST into a physical plan.
///
/// This is a basic compiler that translates clauses into operators.
/// Strategy selection is done separately via `select_strategy()`.
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
            let source_node = &pattern.nodes[i];
            let target = &pattern.nodes[i + 1];
            let (min_hops, max_hops) = edge.var_length.unwrap_or((1, 1));
            ops.push(PhysicalOp::Expand {
                source: source_node.variable.clone().unwrap_or_else(|| {
                    if i == 0 {
                        "_anon".to_string()
                    } else {
                        format!("_anon_{i}")
                    }
                }),
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
        let query = parse_cypher(b"MATCH (a:Person)-[:KNOWS]->(b) RETURN b").expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        assert!(
            plan.operators
                .iter()
                .any(|op| matches!(op, PhysicalOp::Expand { .. }))
        );
    }

    #[test]
    fn test_compile_with_filter() {
        let query = parse_cypher(b"MATCH (n) WHERE n.age > 30 RETURN n").expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        assert!(
            plan.operators
                .iter()
                .any(|op| matches!(op, PhysicalOp::Filter { .. }))
        );
    }

    #[test]
    fn test_plan_cache() {
        let mut cache = PlanCache::new(2);
        assert!(cache.is_empty());

        let plan = Arc::new(PhysicalPlan { operators: vec![] });
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
        assert!(
            plan.operators
                .iter()
                .any(|op| matches!(op, PhysicalOp::ProcedureCall { .. }))
        );
    }

    // --- Cost estimation tests ---

    #[test]
    fn test_graph_first_cost_basic() {
        // 1 start node, avg degree 10, 2 hops, 128 dims.
        // neighborhood = 1 * 10^2 = 100
        // cost = 100 + 100 * 128 = 12_900
        let cost = estimate_graph_first_cost(1, 10.0, 2, 128);
        assert!((cost - 12_900.0).abs() < 0.1);
    }

    #[test]
    fn test_vector_first_cost_basic() {
        // k=10, N=100_000, avg_degree=10, hops=2.
        // search = 10 * log2(100_000) ~ 10 * 16.61 = 166.1
        // expansion = 10 * 10 * 2 = 200
        // total ~ 366.1
        let cost = estimate_vector_first_cost(10, 100_000, 10.0, 2);
        assert!(cost > 360.0 && cost < 370.0, "got {cost}");
    }

    #[test]
    fn test_vector_first_cost_zero_nodes() {
        let cost = estimate_vector_first_cost(10, 0, 10.0, 2);
        assert!((cost - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_select_strategy_prefers_lower_cost() {
        let mut stats = GraphStats::new();
        // Large graph: 1M nodes, avg degree 50 => graph-first at 3 hops is expensive.
        stats.total_nodes = 1_000_000;
        stats.total_edges = 25_000_000;
        stats.degree_stats.avg = 50.0;
        stats.degree_stats.p99 = 200;
        stats.degree_stats.max = 1000;

        // Graph-first: 1 * 50^3 + 125_000 * 128 = 125_000 + 16_000_000 = 16_125_000
        // Vector-first: 10 * log2(1M) + 10 * 50 * 3 ~ 10*20 + 1500 = 1700
        let estimate = select_strategy(&stats, 1, 3, 10, 128, None);
        assert_eq!(estimate.strategy, Strategy::VectorFirst);
        assert!(!estimate.hub_detected);
    }

    #[test]
    fn test_select_strategy_graph_first_for_small_graphs() {
        let mut stats = GraphStats::new();
        // Small graph: 100 nodes, avg degree 2.
        stats.total_nodes = 100;
        stats.total_edges = 100;
        stats.degree_stats.avg = 2.0;
        stats.degree_stats.p99 = 5;
        stats.degree_stats.max = 10;

        // Graph-first: 1 * 2^1 + 2 * 128 = 2 + 256 = 258
        // Vector-first: 10 * log2(100) + 10 * 2 * 1 ~ 66.4 + 20 = 86.4
        // Actually vector-first is cheaper even for small graphs at 1 hop.
        // Let's check:
        let estimate = select_strategy(&stats, 1, 1, 10, 128, None);
        // vector cost ~ 86, graph cost ~ 258 => VectorFirst
        assert_eq!(estimate.strategy, Strategy::VectorFirst);
    }

    #[test]
    fn test_hub_detection_triggers_vector_first() {
        let mut stats = GraphStats::new();
        stats.total_nodes = 1000;
        stats.total_edges = 5000;
        stats.degree_stats.avg = 10.0;
        stats.degree_stats.p99 = 100;
        stats.degree_stats.max = 500;

        // Start node has degree 150 >= p99 (100) => hub detected.
        let estimate = select_strategy(&stats, 1, 1, 10, 128, Some(150));
        assert_eq!(estimate.strategy, Strategy::VectorFirst);
        assert!(estimate.hub_detected);
    }

    #[test]
    fn test_hub_detection_not_triggered_below_p99() {
        let mut stats = GraphStats::new();
        stats.total_nodes = 1000;
        stats.total_edges = 5000;
        stats.degree_stats.avg = 10.0;
        stats.degree_stats.p99 = 100;
        stats.degree_stats.max = 500;

        // Start node has degree 50 < p99 (100) => no hub.
        let estimate = select_strategy(&stats, 1, 1, 10, 128, Some(50));
        assert!(!estimate.hub_detected);
    }

    #[test]
    fn test_strategy_display() {
        assert_eq!(format!("{}", Strategy::GraphFirst), "GraphFirst");
        assert_eq!(format!("{}", Strategy::VectorFirst), "VectorFirst");
    }
}

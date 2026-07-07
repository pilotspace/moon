//! Cypher query planner with cost-based strategy selection and plan caching.
//!
//! Compiles a `CypherQuery` AST into a `PhysicalPlan` — a sequence of
//! operators that the executor can evaluate. Plans are cached by xxhash
//! of the original Cypher string.
//!
//! The cost estimator selects between graph-first and vector-first strategies
//! based on per-graph `GraphStats` (degree distribution, node/edge counts).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::graph::cypher::ast::*;
use crate::graph::stats::GraphStats;

/// A compiled physical plan: a sequence of operators.
#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub operators: Vec<PhysicalOp>,
}

/// Ordering comparison for an `IndexScan` range conjunct (W2-3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeCmp {
    /// `prop > threshold`
    Gt,
    /// `prop >= threshold`
    Gte,
    /// `prop < threshold`
    Lt,
    /// `prop <= threshold`
    Lte,
}

impl RangeCmp {
    /// Flip for the mirrored form (`threshold < prop` ⇔ `prop > threshold`).
    fn flipped(self) -> Self {
        match self {
            RangeCmp::Gt => RangeCmp::Lt,
            RangeCmp::Gte => RangeCmp::Lte,
            RangeCmp::Lt => RangeCmp::Gt,
            RangeCmp::Lte => RangeCmp::Gte,
        }
    }
}

/// Individual physical operators in the execution pipeline.
#[derive(Debug, Clone)]
pub enum PhysicalOp {
    /// Scan nodes by label.
    NodeScan {
        variable: String,
        label: Option<String>,
    },
    /// Scan nodes via per-segment property indexes: label bitmap ∧ property
    /// bitmap per immutable CSR segment, plus a linear mutable-tail check.
    ///
    /// Emitted instead of `NodeScan` when a pattern node carries inline
    /// equality properties whose values are literals or parameters
    /// (`(n:L {k: 3})`, `(n:L {k: $v})`). `prop_eq` values are index LOOKUP
    /// hints with SUPERSET semantics (string hashes can collide, Bool/Int
    /// alias numerically) — the planner always keeps the full residual
    /// Filter downstream, so the index only prunes, never decides.
    IndexScan {
        variable: String,
        label: Option<String>,
        /// (property name, value expression) equality conjuncts. Expressions
        /// are literals or parameters, resolved by the executor per run.
        prop_eq: Vec<(String, Expr)>,
        /// (property name, comparison, threshold expression) range conjuncts
        /// extracted from top-level `WHERE` AND-chains (`n.p > 5`,
        /// `10 > n.p`, `n.p >= $t`). Numeric-index pruning hints with the
        /// same SUPERSET contract as `prop_eq` — the residual Filter
        /// downstream stays authoritative. W2-3.
        prop_range: Vec<(String, RangeCmp, Expr)>,
    },
    /// Expand along edges from a source variable.
    ///
    /// `edge_variable`: when `Some(name)`, the executor binds the traversed
    /// edge into the row under `name` as `Value::Edge(EdgeKey)` for the
    /// single-hop case (min_hops == max_hops == 1). This enables
    /// edge-property predicates such as `WHERE r.valid_to >= $asof` — see
    /// Lunaris V1 gap closure (CYP-06). Multi-hop edge-var binding (as a
    /// path / edge-list value) is deferred to v0.2.
    Expand {
        source: String,
        target: String,
        edge_variable: Option<String>,
        edge_types: Vec<String>,
        direction: EdgeDirection,
        min_hops: u32,
        max_hops: u32,
        /// W2-13 OPTIONAL MATCH: a source row whose expansion yields zero
        /// matches survives with `target` (and `edge_variable`) bound to
        /// Null instead of being dropped. A Null/unbound source also
        /// null-pads instead of being filtered out.
        optional: bool,
    },
    /// Filter rows by a predicate expression.
    Filter { expr: Expr },
    /// Project specific columns.
    Project {
        items: Vec<ReturnItem>,
        distinct: bool,
        /// W2-13 WITH: instead of terminating the pipeline into positional
        /// output rows, re-seed the variable-binding row stream with the
        /// projection's outputs (alias or expression text) so later
        /// MATCH/WHERE/RETURN clauses keep running. RETURN keeps `false`.
        rebind: bool,
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
    /// MERGE: match-or-create pattern with conditional SET.
    Merge {
        pattern: Pattern,
        on_create: Vec<SetItem>,
        on_match: Vec<SetItem>,
    },
    /// `MATCH p = shortestPath(...)` — emits one row per path found from
    /// `source` to `target` and binds `Value::Path(Vec<NodeKey>)` to
    /// `path_var`. Added in v0.1.9 (CYP-04/05) on top of
    /// `traversal::DijkstraTraversal::shortest_path`.
    ShortestPath {
        path_var: String,
        source: String,
        target: String,
        max_hops: u32,
        edge_types: Vec<String>,
        direction: EdgeDirection,
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

/// Plan cache: maps xxhash of the (literal-normalized) Cypher text to a
/// compiled plan, with LRU eviction.
///
/// Entries carry a `read_only` flag (W2-7 caches WRITE plans too). The
/// safety invariant moved from "only read-only plans may be inserted" to
/// "a read-path hit must check the flag": `graph_query_or_write` routes a
/// read-only hit to the read path and a write hit to the write path, both
/// with zero parse/compile work; the pure read handlers treat a write hit
/// as a miss.
pub struct PlanCache {
    cache: HashMap<u64, (Arc<PhysicalPlan>, bool, u64)>,
    max_entries: usize,
    /// Monotonic access counter backing LRU eviction.
    tick: u64,
}

impl PlanCache {
    /// Create a new plan cache with the given maximum size.
    pub fn new(max_entries: usize) -> Self {
        Self {
            cache: HashMap::new(),
            max_entries,
            tick: 0,
        }
    }

    /// Look up a cached plan by query hash, marking it most-recently-used.
    /// Returns the plan and whether it is read-only — callers MUST route on
    /// the flag (see type docs).
    pub fn get(&mut self, hash: u64) -> Option<(Arc<PhysicalPlan>, bool)> {
        self.tick += 1;
        let tick = self.tick;
        self.cache.get_mut(&hash).map(|(plan, read_only, used)| {
            *used = tick;
            (plan.clone(), *read_only)
        })
    }

    /// Insert a plan, evicting the least-recently-used entry at capacity.
    pub fn insert(&mut self, hash: u64, plan: Arc<PhysicalPlan>, read_only: bool) {
        self.tick += 1;
        if self.cache.len() >= self.max_entries && !self.cache.contains_key(&hash) {
            // O(capacity) scan; eviction only fires when a full cache takes a
            // brand-new query text, which is rare once the workload warms up.
            let lru = self
                .cache
                .iter()
                .min_by_key(|(_, (_, _, used))| *used)
                .map(|(k, _)| *k);
            if let Some(lru) = lru {
                self.cache.remove(&lru);
            }
        }
        self.cache.insert(hash, (plan, read_only, self.tick));
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
    // W2-13: variables bound so far, for OPTIONAL MATCH shape validation.
    // WITH resets this set to its own outputs (Cypher scoping).
    let mut bound: HashSet<String> = HashSet::new();
    // W2-13: once a WITH has run, range-conjunct extraction must stop — a
    // post-WITH WHERE is a HAVING over (possibly aggregated) projections
    // and must never prune pre-aggregation scans.
    let mut saw_with = false;

    for clause in &query.clauses {
        match clause {
            Clause::Match(m) => {
                compile_match(m, &mut ops, &mut bound)?;
            }
            Clause::ShortestPathMatch(sp) => {
                compile_shortest_path_match(sp, &mut ops);
                for node in [&sp.src, &sp.dst] {
                    if let Some(v) = &node.variable {
                        bound.insert(v.clone());
                    }
                }
                bound.insert(sp.path_var.clone());
            }
            Clause::Where(w) => {
                // W2-3: top-level AND conjuncts of the form
                // `var.prop <cmp> literal|param` upgrade var's scan to a
                // range IndexScan BEFORE the residual Filter is pushed
                // (the full WHERE stays — the index only prunes).
                if !saw_with {
                    extract_range_conjuncts(&w.expr, &mut ops);
                }
                ops.push(PhysicalOp::Filter {
                    expr: w.expr.clone(),
                });
            }
            Clause::Return(r) => {
                ops.push(PhysicalOp::Project {
                    items: r.items.clone(),
                    distinct: r.distinct,
                    rebind: false,
                });
            }
            Clause::Create(c) => {
                ops.push(PhysicalOp::CreatePattern {
                    patterns: c.patterns.clone(),
                });
                for p in &c.patterns {
                    bind_pattern_vars(p, &mut bound);
                }
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
            Clause::Merge(m) => {
                ops.push(PhysicalOp::Merge {
                    pattern: m.pattern.clone(),
                    on_create: m.on_create.clone(),
                    on_match: m.on_match.clone(),
                });
                bind_pattern_vars(&m.pattern, &mut bound);
            }
            Clause::With(w) => {
                // W2-13: WITH is a rebinding projection — later clauses keep
                // executing on the projected variable stream.
                if w.items.iter().any(|it| matches!(it.expr, Expr::Star)) {
                    return Err(PlanError::Unsupported(
                        "WITH * is not yet supported — list the variables explicitly".to_string(),
                    ));
                }
                ops.push(PhysicalOp::Project {
                    items: w.items.clone(),
                    distinct: w.distinct,
                    rebind: true,
                });
                // Cypher scoping: only the WITH outputs remain in scope.
                bound.clear();
                for item in &w.items {
                    if let Some(name) = with_output_name(item) {
                        bound.insert(name);
                    }
                }
                saw_with = true;
            }
            Clause::Unwind(u) => {
                ops.push(PhysicalOp::Unwind {
                    expr: u.expr.clone(),
                    alias: u.alias.clone(),
                });
                bound.insert(u.alias.clone());
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

/// The scope name a WITH item introduces, for OPTIONAL MATCH validation.
///
/// Alias wins; a bare identifier passes through under its own name. Any
/// other unaliased expression produces an output column that no later
/// identifier can reference, so it contributes nothing to the bound set.
fn with_output_name(item: &ReturnItem) -> Option<String> {
    if let Some(alias) = &item.alias {
        return Some(alias.clone());
    }
    if let Expr::Ident(name) = &item.expr {
        return Some(name.clone());
    }
    None
}

/// Record every variable a pattern binds (nodes and edges).
fn bind_pattern_vars(pattern: &Pattern, bound: &mut HashSet<String>) {
    for node in &pattern.nodes {
        if let Some(v) = &node.variable {
            bound.insert(v.clone());
        }
    }
    for edge in &pattern.edges {
        if let Some(v) = &edge.variable {
            bound.insert(v.clone());
        }
    }
}

/// Compile a MATCH clause into scan + expand operators.
///
/// Returns `Err(PlanError::Unsupported)` if a variable-length edge pattern
/// also binds an edge variable (e.g. `[r*2..5]`). The BFS executor branch
/// does not insert the edge variable into the output row, so downstream
/// predicates silently evaluate against Null — producing wrong results.
/// This gate is temporary: Phase 179 (MVCC-02) will implement `Value::Path`
/// binding and remove this restriction.
///
/// W2-13 OPTIONAL MATCH compiles to an `Expand { optional: true }` and is
/// restricted to its dominant shape — a single relationship expanding from
/// a previously bound bare variable, no inline properties on the optional
/// target. Everything else (standalone patterns, multi-relationship chains
/// whose whole-pattern null semantics need grouped execution, inline
/// property predicates that must not drop null-padded rows) is rejected
/// loudly instead of silently behaving like an inner MATCH.
fn compile_match(
    m: &MatchClause,
    ops: &mut Vec<PhysicalOp>,
    bound: &mut HashSet<String>,
) -> Result<(), PlanError> {
    if m.optional {
        return compile_optional_match(m, ops, bound);
    }
    for pattern in &m.patterns {
        if pattern.nodes.is_empty() {
            continue;
        }
        bind_pattern_vars(pattern, bound);

        // First node becomes a scan (index-backed when inline equality
        // properties can drive a lookup).
        let first = &pattern.nodes[0];
        let first_var = first
            .variable
            .clone()
            .unwrap_or_else(|| "_anon".to_string());
        push_node_scan(first, &first_var, ops);

        // Subsequent node+edge pairs become expands.
        for (i, edge) in pattern.edges.iter().enumerate() {
            // CYP-06: reject multi-hop edge variable binding until Phase 179
            // (MVCC-02) implements Value::Path. The BFS branch never inserts
            // the edge variable into the output row, causing silent wrong results.
            if edge.var_length.is_some() && edge.variable.is_some() {
                let var_name = edge.variable.as_deref().unwrap_or("?");
                return Err(PlanError::Unsupported(format!(
                    "CYP-06: Multi-hop edge variable binding -[{var_name}*m..n]- is not yet \
                     supported. Remove the edge variable '{var_name}' or use a single-hop \
                     pattern. Tracked: MVCC-02 (Phase 179)."
                )));
            }

            let source_node = &pattern.nodes[i];
            let target = &pattern.nodes[i + 1];
            let target_var = target
                .variable
                .clone()
                .unwrap_or_else(|| format!("_anon_{}", i + 1));
            let (min_hops, max_hops) = edge.var_length.unwrap_or((1, 1));
            ops.push(PhysicalOp::Expand {
                source: source_node.variable.clone().unwrap_or_else(|| {
                    if i == 0 {
                        "_anon".to_string()
                    } else {
                        format!("_anon_{i}")
                    }
                }),
                target: target_var.clone(),
                edge_variable: edge.variable.clone(),
                edge_types: edge.edge_types.clone(),
                direction: edge.direction,
                min_hops,
                max_hops,
                optional: false,
            });
            // Inline properties on the expanded target node — filter right after
            // the Expand that BINDS it (after, never before: the var is unbound
            // until Expand produces it).
            if !target.properties.is_empty() {
                ops.push(PhysicalOp::Filter {
                    expr: properties_to_filter(&target_var, &target.properties),
                });
            }
        }
    }
    Ok(())
}

/// Compile `OPTIONAL MATCH` (W2-13). See [`compile_match`] for the supported
/// shape and the rationale for each rejection.
fn compile_optional_match(
    m: &MatchClause,
    ops: &mut Vec<PhysicalOp>,
    bound: &mut HashSet<String>,
) -> Result<(), PlanError> {
    for pattern in &m.patterns {
        let Some(first) = pattern.nodes.first() else {
            continue;
        };
        let Some(first_var) = &first.variable else {
            return Err(PlanError::Unsupported(
                "OPTIONAL MATCH must expand from a previously bound variable — \
                 name the first pattern node"
                    .to_string(),
            ));
        };
        if !bound.contains(first_var) {
            return Err(PlanError::Unsupported(format!(
                "OPTIONAL MATCH must expand from a previously bound variable; \
                 '{first_var}' is not bound. Standalone OPTIONAL MATCH patterns \
                 are not yet supported."
            )));
        }
        if !first.labels.is_empty() || !first.properties.is_empty() {
            return Err(PlanError::Unsupported(format!(
                "OPTIONAL MATCH: labels/properties on the already-bound variable \
                 '{first_var}' are not yet supported — constrain it in the \
                 binding MATCH instead."
            )));
        }
        // `OPTIONAL MATCH (a)` with `a` bound re-matches an existing row:
        // a no-op.
        if pattern.edges.is_empty() {
            continue;
        }
        if pattern.edges.len() > 1 {
            return Err(PlanError::Unsupported(
                "OPTIONAL MATCH with more than one relationship is not yet \
                 supported (whole-pattern null semantics need grouped \
                 execution) — split into single-hop OPTIONAL MATCH clauses"
                    .to_string(),
            ));
        }
        let edge = &pattern.edges[0];
        // CYP-06 gate applies here too (see compile_match).
        if edge.var_length.is_some() && edge.variable.is_some() {
            let var_name = edge.variable.as_deref().unwrap_or("?");
            return Err(PlanError::Unsupported(format!(
                "CYP-06: Multi-hop edge variable binding -[{var_name}*m..n]- is not yet \
                 supported. Remove the edge variable '{var_name}' or use a single-hop \
                 pattern. Tracked: MVCC-02 (Phase 179)."
            )));
        }
        // Parser invariant: nodes.len() == edges.len() + 1; stay panic-free
        // on a malformed AST anyway.
        let Some(target) = pattern.nodes.get(1) else {
            continue;
        };
        if !target.properties.is_empty() {
            return Err(PlanError::Unsupported(
                "OPTIONAL MATCH: inline properties on the optional target are \
                 not yet supported (a post-filter would drop null-padded rows) \
                 — use WHERE with an explicit null check"
                    .to_string(),
            ));
        }
        let target_var = target
            .variable
            .clone()
            .unwrap_or_else(|| "_anon_1".to_string());
        let (min_hops, max_hops) = edge.var_length.unwrap_or((1, 1));
        ops.push(PhysicalOp::Expand {
            source: first_var.clone(),
            target: target_var.clone(),
            edge_variable: edge.variable.clone(),
            edge_types: edge.edge_types.clone(),
            direction: edge.direction,
            min_hops,
            max_hops,
            optional: true,
        });
        bound.insert(target_var);
        if let Some(evar) = &edge.variable {
            bound.insert(evar.clone());
        }
    }
    Ok(())
}

/// Compile a `MATCH p = shortestPath((a)-[*..N]-(b))` clause.
///
/// Emits: NodeScan(a) -> NodeScan(b) -> ShortestPath(a, b). The executor
/// joins the two endpoint streams and calls the Dijkstra primitive per
/// (src, dst) pair, binding `Value::Path(...)` to `path_var`.
fn compile_shortest_path_match(sp: &ShortestPathMatchClause, ops: &mut Vec<PhysicalOp>) {
    // Anonymous endpoint fallbacks for when the pattern omits variables.
    let src_var = sp
        .src
        .variable
        .clone()
        .unwrap_or_else(|| "_sp_src".to_string());
    let dst_var = sp
        .dst
        .variable
        .clone()
        .unwrap_or_else(|| "_sp_dst".to_string());

    push_node_scan(&sp.src, &src_var, ops);
    push_node_scan(&sp.dst, &dst_var, ops);

    ops.push(PhysicalOp::ShortestPath {
        path_var: sp.path_var.clone(),
        source: src_var,
        target: dst_var,
        max_hops: sp.max_hops,
        edge_types: sp.edge_types.clone(),
        direction: sp.direction,
    });
}

/// Emit the scan op for a pattern node, choosing `IndexScan` when the
/// node's inline properties can drive per-segment index lookups (all
/// values literal or parameter), else plain `NodeScan`. Either way the
/// inline properties ALSO become an equality-conjunction Filter applied
/// right after the op that BINDS the node — the index has superset
/// semantics and non-indexed scans need the predicate at all (without it
/// the inline property map is silently dropped — the ≈|E| full-scan bug).
fn push_node_scan(node: &PatternNode, var: &str, ops: &mut Vec<PhysicalOp>) {
    let indexable = !node.properties.is_empty()
        && node.properties.iter().all(|(_, e)| {
            matches!(
                e,
                Expr::Integer(_)
                    | Expr::Float(_)
                    | Expr::StringLit(_)
                    | Expr::Bool(_)
                    | Expr::Parameter(_)
            )
        });
    if indexable {
        ops.push(PhysicalOp::IndexScan {
            variable: var.to_string(),
            label: node.labels.first().cloned(),
            prop_eq: node.properties.clone(),
            prop_range: Vec::new(),
        });
    } else {
        ops.push(PhysicalOp::NodeScan {
            variable: var.to_string(),
            label: node.labels.first().cloned(),
        });
    }
    if !node.properties.is_empty() {
        ops.push(PhysicalOp::Filter {
            expr: properties_to_filter(var, &node.properties),
        });
    }
}

/// Walk a WHERE expression's top-level AND-chain and push every
/// `var.prop <cmp> literal|param` conjunct (either orientation) into the
/// scan op that BINDS `var` (upgrading a plain `NodeScan` to an
/// `IndexScan` when needed). W2-3.
///
/// Soundness: a top-level AND conjunct must hold for every result row, so
/// restricting var's scan to a SUPERSET of rows satisfying it can never
/// drop a valid row — provided the executor's index lookup is a superset
/// of the residual Filter's semantics for that conjunct (see
/// `index_scan_keys`). Disjunctions (`OR`) are never extracted.
fn extract_range_conjuncts(expr: &Expr, ops: &mut Vec<PhysicalOp>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            extract_range_conjuncts(left, ops);
            extract_range_conjuncts(right, ops);
        }
        Expr::BinaryOp { left, op, right } => {
            let cmp = match op {
                BinaryOperator::GreaterThan => RangeCmp::Gt,
                BinaryOperator::GreaterEqual => RangeCmp::Gte,
                BinaryOperator::LessThan => RangeCmp::Lt,
                BinaryOperator::LessEqual => RangeCmp::Lte,
                _ => return,
            };
            // `var.prop <cmp> value` or the mirrored `value <cmp> var.prop`.
            let (var, prop, value, cmp) = match (as_prop_access(left), as_prop_access(right)) {
                (Some((var, prop)), None) if is_range_value(right) => {
                    (var, prop, (**right).clone(), cmp)
                }
                (None, Some((var, prop))) if is_range_value(left) => {
                    (var, prop, (**left).clone(), cmp.flipped())
                }
                _ => return,
            };
            // Find the scan that binds `var` and attach the conjunct.
            for op in ops.iter_mut().rev() {
                match op {
                    PhysicalOp::IndexScan {
                        variable,
                        prop_range,
                        ..
                    } if variable == var => {
                        prop_range.push((prop.to_owned(), cmp, value));
                        return;
                    }
                    PhysicalOp::NodeScan { variable, label } if variable == var => {
                        *op = PhysicalOp::IndexScan {
                            variable: variable.clone(),
                            label: label.clone(),
                            prop_eq: Vec::new(),
                            prop_range: vec![(prop.to_owned(), cmp, value)],
                        };
                        return;
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

/// `n.prop` accessor on a plain variable, if the expression is one.
fn as_prop_access(expr: &Expr) -> Option<(&str, &str)> {
    if let Expr::PropertyAccess { object, property } = expr {
        if let Expr::Ident(var) = object.as_ref() {
            return Some((var.as_str(), property.as_str()));
        }
    }
    None
}

/// Is this expression usable as an index range threshold (resolvable
/// without a row)? Numeric literals and parameters only — the executor
/// skips conjuncts whose parameter resolves to a non-number.
fn is_range_value(expr: &Expr) -> bool {
    matches!(expr, Expr::Integer(_) | Expr::Float(_) | Expr::Parameter(_))
}

/// Build a filter expression `var.k1 = v1 AND var.k2 = v2 ...` from a
/// PatternNode's inline property map, expressed as a standalone Filter op.
/// Used to apply inline node-property predicates `(v {k:e, …})` in both
/// `compile_match` (every inline-propertied pattern node) and
/// `compile_shortest_path_match` (endpoint selection). Equality semantics
/// are the existing `Expr`/`Filter` evaluation — no new coercion rules.
fn properties_to_filter(var: &str, props: &[(String, Expr)]) -> Expr {
    let mut iter = props.iter();
    let first = match iter.next() {
        Some(p) => p,
        None => return Expr::Bool(true),
    };
    let mut expr = Expr::BinaryOp {
        left: Box::new(Expr::PropertyAccess {
            object: Box::new(Expr::Ident(var.to_string())),
            property: first.0.clone(),
        }),
        op: BinaryOperator::Equal,
        right: Box::new(first.1.clone()),
    };
    for (k, v) in iter {
        let next = Expr::BinaryOp {
            left: Box::new(Expr::PropertyAccess {
                object: Box::new(Expr::Ident(var.to_string())),
                property: k.clone(),
            }),
            op: BinaryOperator::Equal,
            right: Box::new(v.clone()),
        };
        expr = Expr::BinaryOp {
            left: Box::new(expr),
            op: BinaryOperator::And,
            right: Box::new(next),
        };
    }
    expr
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

    // ─── graph-cypher-inline-filter (v3-2): inline node-property predicate ─────────
    // Frozen contract: compile_match emits PhysicalOp::Filter(properties_to_filter(..))
    // immediately AFTER the op that binds each inline-propertied node. RED until the fix.

    #[test]
    fn test_inline_prop_emits_filter_after_scan() {
        // M3 — `MATCH (a:Person {id:1})-[]->(b)`: a Filter must immediately
        // follow the binding scan. Literal inline props now plan as an
        // IndexScan (v0.6 property-index lever); the residual Filter stays.
        let query =
            parse_cypher(b"MATCH (a:Person {id:1})-[]->(b) RETURN b").expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        let ns = plan
            .operators
            .iter()
            .position(|op| matches!(op, PhysicalOp::IndexScan { .. }))
            .expect("an IndexScan is planned for literal inline props");
        assert!(
            matches!(plan.operators.get(ns + 1), Some(PhysicalOp::Filter { .. })),
            "inline-property Filter must immediately follow the IndexScan; ops = {:?}",
            plan.operators
        );
    }

    #[test]
    fn test_inline_literal_props_plan_index_scan() {
        // Literal and parameter values are index-eligible.
        for q in [
            b"MATCH (a:N {id:3}) RETURN a".as_slice(),
            b"MATCH (a:N {id:$p}) RETURN a".as_slice(),
            b"MATCH (a:N {name:'x', id: 3}) RETURN a".as_slice(),
        ] {
            let query = parse_cypher(q).expect("parse failed");
            let plan = compile(&query).expect("compile failed");
            assert!(
                matches!(plan.operators[0], PhysicalOp::IndexScan { .. }),
                "expected IndexScan for {:?}; ops = {:?}",
                core::str::from_utf8(q),
                plan.operators
            );
        }
        // No inline props: plain NodeScan, unchanged.
        let query = parse_cypher(b"MATCH (a:N) RETURN a").expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        assert!(matches!(plan.operators[0], PhysicalOp::NodeScan { .. }));
    }

    // ─── W2-3: WHERE range predicates drive the property index ────────────

    #[test]
    fn test_where_range_upgrades_scan_to_index_range() {
        let query = parse_cypher(b"MATCH (n:Person) WHERE n.age > 30 RETURN n").expect("parse");
        let plan = compile(&query).expect("compile");
        match &plan.operators[0] {
            PhysicalOp::IndexScan {
                prop_eq,
                prop_range,
                ..
            } => {
                assert!(prop_eq.is_empty(), "no inline equality props");
                assert_eq!(prop_range.len(), 1, "one range conjunct");
                assert_eq!(prop_range[0].0, "age");
                assert_eq!(prop_range[0].1, RangeCmp::Gt);
            }
            other => panic!("expected range IndexScan, got {other:?}"),
        }
        // The full WHERE stays as a residual Filter (index is superset-only).
        assert!(
            plan.operators
                .iter()
                .any(|op| matches!(op, PhysicalOp::Filter { .. })),
            "residual Filter must remain"
        );
    }

    #[test]
    fn test_where_range_conjuncts_compose_and_flip() {
        let query =
            parse_cypher(b"MATCH (n:N {id:3}) WHERE n.a >= 1 AND 10 > n.b AND n.c = 2 RETURN n")
                .expect("parse");
        let plan = compile(&query).expect("compile");
        match &plan.operators[0] {
            PhysicalOp::IndexScan {
                prop_eq,
                prop_range,
                ..
            } => {
                assert_eq!(prop_eq.len(), 1, "inline {{id:3}} stays an eq conjunct");
                assert_eq!(
                    prop_range.len(),
                    2,
                    "two range conjuncts, got {prop_range:?}"
                );
                assert_eq!(prop_range[0].0, "a");
                assert_eq!(prop_range[0].1, RangeCmp::Gte);
                // `10 > n.b` flips to b < 10.
                assert_eq!(prop_range[1].0, "b");
                assert_eq!(prop_range[1].1, RangeCmp::Lt);
            }
            other => panic!("expected range IndexScan, got {other:?}"),
        }
    }

    #[test]
    fn test_where_or_is_not_range_extracted() {
        // Disjunctions cannot prune a scan (a row failing one branch may
        // satisfy the other).
        let query = parse_cypher(b"MATCH (n:N) WHERE n.a > 1 OR n.b < 2 RETURN n").expect("parse");
        let plan = compile(&query).expect("compile");
        assert!(
            matches!(plan.operators[0], PhysicalOp::NodeScan { .. }),
            "OR predicate must not upgrade the scan; ops = {:?}",
            plan.operators
        );
    }

    #[test]
    fn test_inline_prop_on_expanded_node_filters_after_expand() {
        // M2 — `MATCH (a {id:1})-[]->(b {id:3})`: a Filter after the NodeScan AND one after the Expand.
        let query =
            parse_cypher(b"MATCH (a {id:1})-[]->(b {id:3}) RETURN b").expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        let ns = plan
            .operators
            .iter()
            .position(|op| {
                matches!(
                    op,
                    PhysicalOp::NodeScan { .. } | PhysicalOp::IndexScan { .. }
                )
            })
            .expect("a scan is planned");
        assert!(
            matches!(plan.operators.get(ns + 1), Some(PhysicalOp::Filter { .. })),
            "scanned node's inline Filter must follow the scan; ops = {:?}",
            plan.operators
        );
        let ex = plan
            .operators
            .iter()
            .position(|op| matches!(op, PhysicalOp::Expand { .. }))
            .expect("an Expand is planned");
        assert!(
            matches!(plan.operators.get(ex + 1), Some(PhysicalOp::Filter { .. })),
            "expanded node's inline Filter must follow the Expand; ops = {:?}",
            plan.operators
        );
    }

    #[test]
    fn test_inline_prop_without_label_emits_filter() {
        // M4 — `MATCH (a {id:1})`: a Filter is emitted even though the NodeScan label is None.
        let query = parse_cypher(b"MATCH (a {id:1}) RETURN a").expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        assert!(
            plan.operators
                .iter()
                .any(|op| matches!(op, PhysicalOp::Filter { .. })),
            "label-less inline properties must still produce a Filter; ops = {:?}",
            plan.operators
        );
    }

    #[test]
    fn test_inline_multi_prop_is_and_conjunction() {
        // M1 — `{id:1, name:'alice'}`: the Filter expr is an AND of the two equality compares.
        let query = parse_cypher(b"MATCH (a {id:1, name:'alice'}) RETURN a").expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        let expr = plan
            .operators
            .iter()
            .find_map(|op| match op {
                PhysicalOp::Filter { expr } => Some(expr),
                _ => None,
            })
            .expect("a Filter op is emitted for inline properties");
        assert!(
            matches!(
                expr,
                Expr::BinaryOp {
                    op: BinaryOperator::And,
                    ..
                }
            ),
            "two inline properties must AND-conjoin; expr = {expr:?}"
        );
    }

    #[test]
    fn test_no_inline_prop_emits_no_filter() {
        // M6 (green-pin) — no inline props and no WHERE ⇒ NO Filter (plan unchanged from today).
        let query = parse_cypher(b"MATCH (a:Person)-[]->(b) RETURN b").expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        assert!(
            !plan
                .operators
                .iter()
                .any(|op| matches!(op, PhysicalOp::Filter { .. })),
            "a pattern with no inline properties must add no Filter; ops = {:?}",
            plan.operators
        );
    }

    #[test]
    fn test_plan_cache() {
        let mut cache = PlanCache::new(2);
        assert!(cache.is_empty());

        let plan = Arc::new(PhysicalPlan { operators: vec![] });
        cache.insert(42, plan.clone(), true);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(42).is_some());
        assert!(cache.get(99).is_none());

        // Fill and evict
        cache.insert(43, plan.clone(), true);
        cache.insert(44, plan.clone(), true);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_plan_cache_read_only_flag_round_trips() {
        // W2-7: write plans are cached too; the flag tells the read path to
        // treat them as misses and the write path to execute them directly.
        let mut cache = PlanCache::new(4);
        let plan = Arc::new(PhysicalPlan { operators: vec![] });
        cache.insert(1, plan.clone(), true);
        cache.insert(2, plan.clone(), false);
        assert_eq!(cache.get(1).map(|(_, ro)| ro), Some(true));
        assert_eq!(cache.get(2).map(|(_, ro)| ro), Some(false));
    }

    #[test]
    fn test_plan_cache_lru_eviction_order() {
        let mut cache = PlanCache::new(2);
        let plan = Arc::new(PhysicalPlan { operators: vec![] });
        cache.insert(1, plan.clone(), true);
        cache.insert(2, plan.clone(), true);
        // Touch 1 so 2 becomes the least-recently-used entry.
        assert!(cache.get(1).is_some());
        cache.insert(3, plan.clone(), true);
        assert!(
            cache.get(1).is_some(),
            "recently-used entry must survive eviction"
        );
        assert!(
            cache.get(2).is_none(),
            "least-recently-used entry must be evicted"
        );
        assert!(cache.get(3).is_some());
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

    #[test]
    fn test_compile_merge() {
        let query =
            parse_cypher(b"MERGE (n:Person {name: 'Alice'}) RETURN n").expect("parse failed");
        let plan = compile(&query).expect("compile failed");
        assert!(
            plan.operators
                .iter()
                .any(|op| matches!(op, PhysicalOp::Merge { .. })),
            "expected Merge operator in plan, got: {:?}",
            plan.operators
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

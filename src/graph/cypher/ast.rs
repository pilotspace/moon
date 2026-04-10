//! Cypher AST types.
//!
//! Represents the parsed structure of a Cypher query. All string data is
//! owned (`String`) since the AST outlives the input slice.

/// A complete Cypher query: a sequence of clauses.
#[derive(Debug, Clone, PartialEq)]
pub struct CypherQuery {
    pub clauses: Vec<Clause>,
}

/// Top-level clause in a Cypher query.
#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    Match(MatchClause),
    Where(WhereClause),
    Return(ReturnClause),
    Create(CreateClause),
    Delete(DeleteClause),
    Set(SetClause),
    Merge(MergeClause),
    With(WithClause),
    Unwind(UnwindClause),
    Call(CallClause),
    OrderBy(OrderByClause),
    Limit(LimitClause),
    Skip(SkipClause),
}

// ---------------------------------------------------------------------------
// MATCH
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct MatchClause {
    pub patterns: Vec<Pattern>,
    pub optional: bool,
}

/// A graph pattern: alternating nodes and edges.
/// Example: `(a:Person)-[:KNOWS]->(b:Person)`
/// Stored as: nodes = [a, b], edges = [KNOWS]
/// nodes.len() == edges.len() + 1 always.
#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    pub nodes: Vec<PatternNode>,
    pub edges: Vec<PatternEdge>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatternNode {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatternEdge {
    pub variable: Option<String>,
    pub edge_types: Vec<String>,
    pub direction: EdgeDirection,
    /// Variable-length path: `*min..max`. None means fixed single hop.
    pub var_length: Option<(u32, u32)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    /// `-->` (left to right)
    Right,
    /// `<--` (right to left)
    Left,
    /// `--` (undirected)
    Both,
}

// ---------------------------------------------------------------------------
// WHERE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub expr: Expr,
}

// ---------------------------------------------------------------------------
// RETURN
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
    pub distinct: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

// ---------------------------------------------------------------------------
// CREATE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct CreateClause {
    pub patterns: Vec<Pattern>,
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteClause {
    pub exprs: Vec<Expr>,
    pub detach: bool,
}

// ---------------------------------------------------------------------------
// SET
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetItem {
    /// `n.prop = expr`
    Property {
        variable: String,
        property: String,
        value: Expr,
    },
    /// `n:Label`
    Label { variable: String, label: String },
}

// ---------------------------------------------------------------------------
// MERGE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct MergeClause {
    pub pattern: Pattern,
    pub on_create: Vec<SetItem>,
    pub on_match: Vec<SetItem>,
}

// ---------------------------------------------------------------------------
// WITH
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub items: Vec<ReturnItem>,
    pub distinct: bool,
}

// ---------------------------------------------------------------------------
// UNWIND
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct UnwindClause {
    pub expr: Expr,
    pub alias: String,
}

// ---------------------------------------------------------------------------
// CALL (procedure calls)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct CallClause {
    /// Dotted procedure name: e.g. `oxid.vector.search`
    pub procedure: String,
    pub args: Vec<Expr>,
    pub yields: Vec<YieldItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct YieldItem {
    pub name: String,
    pub alias: Option<String>,
}

// ---------------------------------------------------------------------------
// ORDER BY, LIMIT, SKIP
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByClause {
    /// (expression, ascending)
    pub items: Vec<(Expr, bool)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LimitClause {
    pub count: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SkipClause {
    pub count: Expr,
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Integer literal.
    Integer(i64),
    /// Float literal.
    Float(f64),
    /// String literal (content without quotes).
    StringLit(String),
    /// Boolean literal.
    Bool(bool),
    /// NULL literal.
    Null,
    /// Variable or identifier reference.
    Ident(String),
    /// Parameter reference: `$name`.
    Parameter(String),
    /// Property access: `n.name`
    PropertyAccess {
        object: Box<Expr>,
        property: String,
    },
    /// Binary operation.
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary NOT.
    Not(Box<Expr>),
    /// Unary minus.
    Negate(Box<Expr>),
    /// Function call: `collect(x)`, `count(x)`, etc.
    FunctionCall {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    /// List literal: `[1, 2, 3]`
    List(Vec<Expr>),
    /// Map literal: `{name: 'Alice', age: 30}`
    MapLit(Vec<(String, Expr)>),
    /// Star `*` (for RETURN *)
    Star,
    /// `expr IS NULL` / `expr IS NOT NULL`
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// `expr IN list_expr`
    InList {
        expr: Box<Expr>,
        list: Box<Expr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessEqual,
    GreaterEqual,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    RegexMatch,
}

impl CypherQuery {
    /// Check if this query is read-only (no CREATE, DELETE, SET, MERGE).
    pub fn is_read_only(&self) -> bool {
        self.clauses.iter().all(|c| {
            !matches!(
                c,
                Clause::Create(_) | Clause::Delete(_) | Clause::Set(_) | Clause::Merge(_)
            )
        })
    }
}

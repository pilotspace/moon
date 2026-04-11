//! Hand-rolled recursive descent Cypher parser.
//!
//! Operates on tokens from the `logos`-based lexer. Each clause type has its
//! own parsing method. Nesting depth is tracked to prevent stack overflow
//! from pathologically nested expressions (default limit: 64).
//!
//! Parameters (`$name`) are stored as `Expr::Parameter` -- never interpolated
//! into the query string, preventing Cypher injection (CVE-2024-8309).

use crate::graph::cypher::ast::*;
use crate::graph::cypher::lexer::{Lexer, SpannedToken, Token};

/// Errors produced during Cypher parsing.
#[derive(Debug, Clone, PartialEq)]
pub enum CypherError {
    /// Unexpected token at given byte offset.
    UnexpectedToken {
        offset: usize,
        expected: &'static str,
        found: String,
    },
    /// Unexpected end of input.
    UnexpectedEof { expected: &'static str },
    /// Nesting depth exceeded the configured limit.
    NestingDepthExceeded { limit: u32 },
    /// Invalid numeric literal.
    InvalidNumber { offset: usize, value: String },
    /// Empty query.
    EmptyQuery,
}

impl core::fmt::Display for CypherError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnexpectedToken {
                offset,
                expected,
                found,
            } => write!(
                f,
                "unexpected token at {offset}: expected {expected}, found {found}"
            ),
            Self::UnexpectedEof { expected } => {
                write!(f, "unexpected end of input: expected {expected}")
            }
            Self::NestingDepthExceeded { limit } => {
                write!(f, "nesting depth exceeded limit of {limit}")
            }
            Self::InvalidNumber { offset, value } => {
                write!(f, "invalid number at {offset}: '{value}'")
            }
            Self::EmptyQuery => write!(f, "empty query"),
        }
    }
}

/// Recursive descent Cypher parser.
pub struct Parser<'a> {
    lexer: Lexer<'a>,
    max_depth: u32,
    current_depth: u32,
}

impl<'a> Parser<'a> {
    /// Create a new parser with the given nesting depth limit.
    pub fn new(input: &'a [u8], max_depth: u32) -> Self {
        Self {
            lexer: Lexer::new(input),
            max_depth,
            current_depth: 0,
        }
    }

    /// Parse a complete Cypher query.
    pub fn parse(&mut self) -> Result<CypherQuery, CypherError> {
        let mut clauses = Vec::new();

        while self.lexer.peek().is_some() {
            let clause = self.parse_clause()?;
            clauses.push(clause);
        }

        if clauses.is_empty() {
            return Err(CypherError::EmptyQuery);
        }

        Ok(CypherQuery { clauses })
    }

    // -----------------------------------------------------------------------
    // Clause dispatch
    // -----------------------------------------------------------------------

    fn parse_clause(&mut self) -> Result<Clause, CypherError> {
        let tok = self.peek_token_ref()?;
        match &tok.token {
            Token::Optional => {
                self.advance();
                self.expect_token_match(|t| matches!(t, Token::Match), "MATCH after OPTIONAL")?;
                let patterns = self.parse_pattern_list()?;
                Ok(Clause::Match(MatchClause {
                    patterns,
                    optional: true,
                }))
            }
            Token::Match => {
                self.advance();
                let patterns = self.parse_pattern_list()?;
                Ok(Clause::Match(MatchClause {
                    patterns,
                    optional: false,
                }))
            }
            Token::Where => {
                self.advance();
                let expr = self.parse_expression()?;
                Ok(Clause::Where(WhereClause { expr }))
            }
            Token::Return => {
                self.advance();
                self.parse_return_clause()
            }
            Token::Create => {
                self.advance();
                let patterns = self.parse_pattern_list()?;
                Ok(Clause::Create(CreateClause { patterns }))
            }
            Token::Delete => {
                self.advance();
                self.parse_delete_clause(false)
            }
            Token::Detach => {
                self.advance();
                self.expect_token_match(|t| matches!(t, Token::Delete), "DELETE after DETACH")?;
                self.parse_delete_clause(true)
            }
            Token::Set => {
                self.advance();
                self.parse_set_clause()
            }
            Token::Merge => {
                self.advance();
                self.parse_merge_clause()
            }
            Token::With => {
                self.advance();
                self.parse_with_clause()
            }
            Token::Unwind => {
                self.advance();
                self.parse_unwind_clause()
            }
            Token::Call => {
                self.advance();
                self.parse_call_clause()
            }
            Token::Order => {
                self.advance();
                self.expect_token_match(|t| matches!(t, Token::By), "BY after ORDER")?;
                self.parse_order_by_clause()
            }
            Token::Limit => {
                self.advance();
                let count = self.parse_expression()?;
                Ok(Clause::Limit(LimitClause { count }))
            }
            Token::Skip => {
                self.advance();
                let count = self.parse_expression()?;
                Ok(Clause::Skip(SkipClause { count }))
            }
            _ => {
                let tok = self.advance();
                Err(CypherError::UnexpectedToken {
                    offset: tok.span.start,
                    expected: "clause keyword (MATCH, WHERE, RETURN, CREATE, etc.)",
                    found: format!("{:?}", tok.token),
                })
            }
        }
    }

    // -----------------------------------------------------------------------
    // Pattern parsing
    // -----------------------------------------------------------------------

    fn parse_pattern_list(&mut self) -> Result<Vec<Pattern>, CypherError> {
        let mut patterns = vec![self.parse_pattern()?];
        while self.peek_is(|t| matches!(t, Token::Comma)) {
            self.advance();
            patterns.push(self.parse_pattern()?);
        }
        Ok(patterns)
    }

    fn parse_pattern(&mut self) -> Result<Pattern, CypherError> {
        self.check_depth()?;
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        // First node
        nodes.push(self.parse_pattern_node()?);

        // Alternating edge-node pairs
        while self.peek_is_edge_start() {
            edges.push(self.parse_pattern_edge()?);
            nodes.push(self.parse_pattern_node()?);
        }

        self.current_depth = self.current_depth.saturating_sub(1);
        Ok(Pattern { nodes, edges })
    }

    fn parse_pattern_node(&mut self) -> Result<PatternNode, CypherError> {
        self.expect_token_match(|t| matches!(t, Token::LParen), "(")?;

        let mut variable = None;
        let mut labels = Vec::new();
        let mut properties = Vec::new();

        // Optional variable name
        if self.peek_is(|t| matches!(t, Token::Ident(_))) {
            variable = Some(self.parse_ident()?);
        }

        // Labels: `:Label1:Label2`
        while self.peek_is(|t| matches!(t, Token::Colon)) {
            self.advance();
            labels.push(self.parse_ident()?);
        }

        // Properties: `{key: value, ...}`
        if self.peek_is(|t| matches!(t, Token::LBrace)) {
            properties = self.parse_property_map()?;
        }

        self.expect_token_match(|t| matches!(t, Token::RParen), ")")?;

        Ok(PatternNode {
            variable,
            labels,
            properties,
        })
    }

    fn peek_is_edge_start(&mut self) -> bool {
        self.peek_is(|t| matches!(t, Token::Minus | Token::ArrowLeft))
    }

    fn parse_pattern_edge(&mut self) -> Result<PatternEdge, CypherError> {
        let mut direction = EdgeDirection::Both;
        let left_arrow = self.peek_is(|t| matches!(t, Token::ArrowLeft));

        if left_arrow {
            // <-
            self.advance();
            direction = EdgeDirection::Left;
        } else {
            // -
            self.expect_token_match(|t| matches!(t, Token::Minus), "-")?;
        }

        let mut variable = None;
        let mut edge_types = Vec::new();
        let mut var_length = None;

        // Optional edge details in brackets
        if self.peek_is(|t| matches!(t, Token::LBracket)) {
            self.advance();

            // Optional variable
            if self.peek_is(|t| matches!(t, Token::Ident(_))) {
                variable = Some(self.parse_ident()?);
            }

            // Edge types: `:TYPE1|TYPE2`
            if self.peek_is(|t| matches!(t, Token::Colon)) {
                self.advance();
                edge_types.push(self.parse_ident()?);
                while self.peek_is(|t| matches!(t, Token::Pipe)) {
                    self.advance();
                    edge_types.push(self.parse_ident()?);
                }
            }

            // Variable length: `*`, `*1..3`, `*..3`, `*1..`
            if self.peek_is(|t| matches!(t, Token::Star)) {
                self.advance();
                var_length = Some(self.parse_var_length()?);
            }

            self.expect_token_match(|t| matches!(t, Token::RBracket), "]")?;
        }

        // Right side: `->` or `-`
        if self.peek_is(|t| matches!(t, Token::ArrowRight)) {
            self.advance();
            if direction == EdgeDirection::Left {
                // `<-[]->`  is treated as both/undirected
                direction = EdgeDirection::Both;
            } else {
                direction = EdgeDirection::Right;
            }
        } else if self.peek_is(|t| matches!(t, Token::Minus)) {
            self.advance();
            // `-[]-` stays as Both (unless already left)
        }

        Ok(PatternEdge {
            variable,
            edge_types,
            direction,
            var_length,
        })
    }

    fn parse_var_length(&mut self) -> Result<(u32, u32), CypherError> {
        let mut min = 1u32;
        let mut max = u32::MAX;

        if self.peek_is(|t| matches!(t, Token::Integer(_))) {
            min = self.parse_u32_lit()?;
            if self.peek_is(|t| matches!(t, Token::DotDot)) {
                self.advance();
                if self.peek_is(|t| matches!(t, Token::Integer(_))) {
                    max = self.parse_u32_lit()?;
                }
            } else {
                max = min; // `*3` means exactly 3 hops
            }
        } else if self.peek_is(|t| matches!(t, Token::DotDot)) {
            self.advance();
            min = 1;
            if self.peek_is(|t| matches!(t, Token::Integer(_))) {
                max = self.parse_u32_lit()?;
            }
        }
        // Just `*` means `*1..MAX`

        Ok((min, max))
    }

    fn parse_property_map(&mut self) -> Result<Vec<(String, Expr)>, CypherError> {
        self.expect_token_match(|t| matches!(t, Token::LBrace), "{")?;
        let mut props = Vec::new();

        if !self.peek_is(|t| matches!(t, Token::RBrace)) {
            let key = self.parse_ident()?;
            self.expect_token_match(|t| matches!(t, Token::Colon), ":")?;
            let value = self.parse_expression()?;
            props.push((key, value));

            while self.peek_is(|t| matches!(t, Token::Comma)) {
                self.advance();
                let key = self.parse_ident()?;
                self.expect_token_match(|t| matches!(t, Token::Colon), ":")?;
                let value = self.parse_expression()?;
                props.push((key, value));
            }
        }

        self.expect_token_match(|t| matches!(t, Token::RBrace), "}")?;
        Ok(props)
    }

    // -----------------------------------------------------------------------
    // RETURN clause
    // -----------------------------------------------------------------------

    fn parse_return_clause(&mut self) -> Result<Clause, CypherError> {
        let distinct = self.peek_is(|t| matches!(t, Token::Distinct));
        if distinct {
            self.advance();
        }

        let items = self.parse_return_items()?;
        Ok(Clause::Return(ReturnClause { items, distinct }))
    }

    fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>, CypherError> {
        let mut items = vec![self.parse_return_item()?];
        while self.peek_is(|t| matches!(t, Token::Comma)) {
            self.advance();
            items.push(self.parse_return_item()?);
        }
        Ok(items)
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, CypherError> {
        // Special case: RETURN *
        if self.peek_is(|t| matches!(t, Token::Star)) {
            self.advance();
            return Ok(ReturnItem {
                expr: Expr::Star,
                alias: None,
            });
        }

        let expr = self.parse_expression()?;
        let alias = if self.peek_is(|t| matches!(t, Token::As)) {
            self.advance();
            Some(self.parse_ident()?)
        } else {
            None
        };
        Ok(ReturnItem { expr, alias })
    }

    // -----------------------------------------------------------------------
    // DELETE clause
    // -----------------------------------------------------------------------

    fn parse_delete_clause(&mut self, detach: bool) -> Result<Clause, CypherError> {
        let mut exprs = vec![self.parse_expression()?];
        while self.peek_is(|t| matches!(t, Token::Comma)) {
            self.advance();
            exprs.push(self.parse_expression()?);
        }
        Ok(Clause::Delete(DeleteClause { exprs, detach }))
    }

    // -----------------------------------------------------------------------
    // SET clause
    // -----------------------------------------------------------------------

    fn parse_set_clause(&mut self) -> Result<Clause, CypherError> {
        let mut items = vec![self.parse_set_item()?];
        while self.peek_is(|t| matches!(t, Token::Comma)) {
            self.advance();
            items.push(self.parse_set_item()?);
        }
        Ok(Clause::Set(SetClause { items }))
    }

    fn parse_set_item(&mut self) -> Result<SetItem, CypherError> {
        let variable = self.parse_ident()?;

        if self.peek_is(|t| matches!(t, Token::Colon)) {
            // SET n:Label
            self.advance();
            let label = self.parse_ident()?;
            Ok(SetItem::Label { variable, label })
        } else {
            // SET n.prop = expr
            self.expect_token_match(|t| matches!(t, Token::Dot), ".")?;
            let property = self.parse_ident()?;
            self.expect_token_match(|t| matches!(t, Token::Equal), "=")?;
            let value = self.parse_expression()?;
            Ok(SetItem::Property {
                variable,
                property,
                value,
            })
        }
    }

    // -----------------------------------------------------------------------
    // MERGE clause
    // -----------------------------------------------------------------------

    fn parse_merge_clause(&mut self) -> Result<Clause, CypherError> {
        let pattern = self.parse_pattern()?;
        let mut on_create = Vec::new();
        let mut on_match = Vec::new();

        loop {
            if self.peek_is(|t| matches!(t, Token::On)) {
                self.advance();
                if self.peek_is(|t| matches!(t, Token::Create)) {
                    self.advance();
                    self.expect_token_match(|t| matches!(t, Token::Set), "SET")?;
                    on_create.push(self.parse_set_item()?);
                    while self.peek_is(|t| matches!(t, Token::Comma)) {
                        self.advance();
                        on_create.push(self.parse_set_item()?);
                    }
                } else if self.peek_is(|t| matches!(t, Token::Match)) {
                    self.advance();
                    self.expect_token_match(|t| matches!(t, Token::Set), "SET")?;
                    on_match.push(self.parse_set_item()?);
                    while self.peek_is(|t| matches!(t, Token::Comma)) {
                        self.advance();
                        on_match.push(self.parse_set_item()?);
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(Clause::Merge(MergeClause {
            pattern,
            on_create,
            on_match,
        }))
    }

    // -----------------------------------------------------------------------
    // WITH clause
    // -----------------------------------------------------------------------

    fn parse_with_clause(&mut self) -> Result<Clause, CypherError> {
        let distinct = self.peek_is(|t| matches!(t, Token::Distinct));
        if distinct {
            self.advance();
        }
        let items = self.parse_return_items()?;
        Ok(Clause::With(WithClause { items, distinct }))
    }

    // -----------------------------------------------------------------------
    // UNWIND clause
    // -----------------------------------------------------------------------

    fn parse_unwind_clause(&mut self) -> Result<Clause, CypherError> {
        let expr = self.parse_expression()?;
        self.expect_token_match(|t| matches!(t, Token::As), "AS")?;
        let alias = self.parse_ident()?;
        Ok(Clause::Unwind(UnwindClause { expr, alias }))
    }

    // -----------------------------------------------------------------------
    // CALL clause
    // -----------------------------------------------------------------------

    fn parse_call_clause(&mut self) -> Result<Clause, CypherError> {
        // Parse dotted procedure name: `oxid.vector.search`
        let mut name = self.parse_ident()?;
        while self.peek_is(|t| matches!(t, Token::Dot)) {
            self.advance();
            name.push('.');
            name.push_str(&self.parse_ident()?);
        }

        // Arguments in parentheses
        self.expect_token_match(|t| matches!(t, Token::LParen), "(")?;
        let mut args = Vec::new();
        if !self.peek_is(|t| matches!(t, Token::RParen)) {
            args.push(self.parse_expression()?);
            while self.peek_is(|t| matches!(t, Token::Comma)) {
                self.advance();
                args.push(self.parse_expression()?);
            }
        }
        self.expect_token_match(|t| matches!(t, Token::RParen), ")")?;

        // Optional YIELD
        let mut yields = Vec::new();
        if self.peek_is(|t| matches!(t, Token::Yield)) {
            self.advance();
            yields.push(self.parse_yield_item()?);
            while self.peek_is(|t| matches!(t, Token::Comma)) {
                self.advance();
                yields.push(self.parse_yield_item()?);
            }
        }

        Ok(Clause::Call(CallClause {
            procedure: name,
            args,
            yields,
        }))
    }

    fn parse_yield_item(&mut self) -> Result<YieldItem, CypherError> {
        let name = self.parse_ident()?;
        let alias = if self.peek_is(|t| matches!(t, Token::As)) {
            self.advance();
            Some(self.parse_ident()?)
        } else {
            None
        };
        Ok(YieldItem { name, alias })
    }

    // -----------------------------------------------------------------------
    // ORDER BY clause
    // -----------------------------------------------------------------------

    fn parse_order_by_clause(&mut self) -> Result<Clause, CypherError> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expression()?;
            let ascending = if self.peek_is(|t| matches!(t, Token::Desc)) {
                self.advance();
                false
            } else {
                if self.peek_is(|t| matches!(t, Token::Asc)) {
                    self.advance();
                }
                true
            };
            items.push((expr, ascending));
            if !self.peek_is(|t| matches!(t, Token::Comma)) {
                break;
            }
            self.advance();
        }
        Ok(Clause::OrderBy(OrderByClause { items }))
    }

    // -----------------------------------------------------------------------
    // Expression parsing (precedence climbing)
    // -----------------------------------------------------------------------

    fn parse_expression(&mut self) -> Result<Expr, CypherError> {
        self.check_depth()?;
        let result = self.parse_or_expr();
        self.current_depth = self.current_depth.saturating_sub(1);
        result
    }

    fn parse_or_expr(&mut self) -> Result<Expr, CypherError> {
        let mut left = self.parse_and_expr()?;
        while self.peek_is(|t| matches!(t, Token::Or)) {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr, CypherError> {
        let mut left = self.parse_not_expr()?;
        while self.peek_is(|t| matches!(t, Token::And)) {
            self.advance();
            let right = self.parse_not_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr, CypherError> {
        if self.peek_is(|t| matches!(t, Token::Not)) {
            self.advance();
            let expr = self.parse_not_expr()?;
            return Ok(Expr::Not(Box::new(expr)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr, CypherError> {
        let mut left = self.parse_addition()?;

        // IS NULL / IS NOT NULL
        if self.peek_is(|t| matches!(t, Token::Is)) {
            self.advance();
            let negated = self.peek_is(|t| matches!(t, Token::Not));
            if negated {
                self.advance();
            }
            self.expect_token_match(|t| matches!(t, Token::Null), "NULL")?;
            return Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            });
        }

        // IN
        if self.peek_is(|t| matches!(t, Token::In)) {
            self.advance();
            let list = self.parse_addition()?;
            return Ok(Expr::InList {
                expr: Box::new(left),
                list: Box::new(list),
            });
        }

        // Comparison operators
        loop {
            let op = if self.peek_is(|t| matches!(t, Token::Equal)) {
                Some(BinaryOperator::Equal)
            } else if self.peek_is(|t| matches!(t, Token::NotEqual)) {
                Some(BinaryOperator::NotEqual)
            } else if self.peek_is(|t| matches!(t, Token::LessThan)) {
                Some(BinaryOperator::LessThan)
            } else if self.peek_is(|t| matches!(t, Token::GreaterThan)) {
                Some(BinaryOperator::GreaterThan)
            } else if self.peek_is(|t| matches!(t, Token::LessEqual)) {
                Some(BinaryOperator::LessEqual)
            } else if self.peek_is(|t| matches!(t, Token::GreaterEqual)) {
                Some(BinaryOperator::GreaterEqual)
            } else if self.peek_is(|t| matches!(t, Token::RegexMatch)) {
                Some(BinaryOperator::RegexMatch)
            } else {
                None
            };

            if let Some(op) = op {
                self.advance();
                let right = self.parse_addition()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_addition(&mut self) -> Result<Expr, CypherError> {
        let mut left = self.parse_multiplication()?;
        loop {
            let op = if self.peek_is(|t| matches!(t, Token::Plus)) {
                Some(BinaryOperator::Add)
            } else if self.peek_is(|t| matches!(t, Token::Minus)) {
                Some(BinaryOperator::Sub)
            } else {
                None
            };

            if let Some(op) = op {
                self.advance();
                let right = self.parse_multiplication()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_multiplication(&mut self) -> Result<Expr, CypherError> {
        let mut left = self.parse_unary()?;
        loop {
            let op = if self.peek_is(|t| matches!(t, Token::Star)) {
                Some(BinaryOperator::Mul)
            } else if self.peek_is(|t| matches!(t, Token::Slash)) {
                Some(BinaryOperator::Div)
            } else if self.peek_is(|t| matches!(t, Token::Percent)) {
                Some(BinaryOperator::Mod)
            } else {
                None
            };

            if let Some(op) = op {
                self.advance();
                let right = self.parse_unary()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, CypherError> {
        if self.peek_is(|t| matches!(t, Token::Minus)) {
            self.advance();
            let expr = self.parse_primary()?;
            return Ok(Expr::Negate(Box::new(expr)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, CypherError> {
        let tok = self.peek_token_ref()?;

        match &tok.token {
            Token::Integer(_) => {
                let t = self.advance();
                if let Token::Integer(s) = t.token {
                    let val = self.parse_i64_bytes(s, t.span.start)?;
                    let mut expr = Expr::Integer(val);
                    expr = self.parse_postfix(expr)?;
                    Ok(expr)
                } else {
                    // Should not happen
                    Ok(Expr::Null)
                }
            }
            Token::Float(_) => {
                let t = self.advance();
                if let Token::Float(s) = t.token {
                    let val = self.parse_f64_bytes(s, t.span.start)?;
                    let mut expr = Expr::Float(val);
                    expr = self.parse_postfix(expr)?;
                    Ok(expr)
                } else {
                    Ok(Expr::Null)
                }
            }
            Token::StringLit(_) => {
                let t = self.advance();
                if let Token::StringLit(s) = t.token {
                    // Strip quotes
                    let inner = &s[1..s.len() - 1];
                    let val = String::from_utf8_lossy(inner).into_owned();
                    Ok(Expr::StringLit(val))
                } else {
                    Ok(Expr::Null)
                }
            }
            Token::True => {
                self.advance();
                Ok(Expr::Bool(true))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Bool(false))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Null)
            }
            Token::Parameter(_) => {
                let t = self.advance();
                if let Token::Parameter(s) = t.token {
                    // Strip the `$` prefix
                    let name = String::from_utf8_lossy(&s[1..]).into_owned();
                    Ok(Expr::Parameter(name))
                } else {
                    Ok(Expr::Null)
                }
            }
            Token::LParen => {
                // Parenthesized expression
                self.advance();
                let expr = self.parse_expression()?;
                self.expect_token_match(|t| matches!(t, Token::RParen), ")")?;
                let expr = self.parse_postfix(expr)?;
                Ok(expr)
            }
            Token::LBracket => {
                // List literal
                self.advance();
                let mut items = Vec::new();
                if !self.peek_is(|t| matches!(t, Token::RBracket)) {
                    items.push(self.parse_expression()?);
                    while self.peek_is(|t| matches!(t, Token::Comma)) {
                        self.advance();
                        items.push(self.parse_expression()?);
                    }
                }
                self.expect_token_match(|t| matches!(t, Token::RBracket), "]")?;
                Ok(Expr::List(items))
            }
            Token::LBrace => {
                // Map literal
                let props = self.parse_property_map()?;
                Ok(Expr::MapLit(props))
            }
            Token::Ident(_) => {
                let t = self.advance();
                let name = if let Token::Ident(s) = t.token {
                    String::from_utf8_lossy(s).into_owned()
                } else {
                    String::new()
                };

                // Check if function call: ident(...)
                if self.peek_is(|t| matches!(t, Token::LParen)) {
                    self.advance();
                    let distinct = self.peek_is(|t| matches!(t, Token::Distinct));
                    if distinct {
                        self.advance();
                    }
                    let mut args = Vec::new();
                    if !self.peek_is(|t| matches!(t, Token::RParen)) {
                        // Check for star arg: count(*)
                        if self.peek_is(|t| matches!(t, Token::Star)) {
                            self.advance();
                            args.push(Expr::Star);
                        } else {
                            args.push(self.parse_expression()?);
                            while self.peek_is(|t| matches!(t, Token::Comma)) {
                                self.advance();
                                args.push(self.parse_expression()?);
                            }
                        }
                    }
                    self.expect_token_match(|t| matches!(t, Token::RParen), ")")?;
                    let mut expr = Expr::FunctionCall {
                        name,
                        args,
                        distinct,
                    };
                    expr = self.parse_postfix(expr)?;
                    Ok(expr)
                } else {
                    let mut expr = Expr::Ident(name);
                    expr = self.parse_postfix(expr)?;
                    Ok(expr)
                }
            }
            // Keywords that could be used as identifiers in some contexts
            // (e.g., `node` is a common variable name but not a keyword in our lexer)
            Token::Star => {
                self.advance();
                Ok(Expr::Star)
            }
            _ => {
                let t = self.advance();
                Err(CypherError::UnexpectedToken {
                    offset: t.span.start,
                    expected: "expression",
                    found: format!("{:?}", t.token),
                })
            }
        }
    }

    /// Parse postfix operators: property access `.prop`
    fn parse_postfix(&mut self, mut expr: Expr) -> Result<Expr, CypherError> {
        while self.peek_is(|t| matches!(t, Token::Dot)) {
            self.advance();
            let prop = self.parse_ident()?;
            expr = Expr::PropertyAccess {
                object: Box::new(expr),
                property: prop,
            };
        }
        Ok(expr)
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn check_depth(&mut self) -> Result<(), CypherError> {
        self.current_depth += 1;
        if self.current_depth > self.max_depth {
            return Err(CypherError::NestingDepthExceeded {
                limit: self.max_depth,
            });
        }
        Ok(())
    }

    fn peek_is<F>(&mut self, pred: F) -> bool
    where
        F: FnOnce(&Token<'_>) -> bool,
    {
        self.lexer.peek().map_or(false, |t| pred(&t.token))
    }

    fn peek_token_ref(&mut self) -> Result<SpannedToken<'a>, CypherError> {
        self.lexer
            .peek()
            .cloned()
            .ok_or(CypherError::UnexpectedEof { expected: "token" })
    }

    fn advance(&mut self) -> SpannedToken<'a> {
        // Caller must have checked peek() first
        self.lexer.next_token().unwrap_or(SpannedToken {
            token: Token::Null,
            span: 0..0,
        })
    }

    fn expect_token_match<F>(
        &mut self,
        pred: F,
        expected: &'static str,
    ) -> Result<SpannedToken<'a>, CypherError>
    where
        F: FnOnce(&Token<'_>) -> bool,
    {
        let tok = self
            .lexer
            .next_token()
            .ok_or(CypherError::UnexpectedEof { expected })?;
        if pred(&tok.token) {
            Ok(tok)
        } else {
            Err(CypherError::UnexpectedToken {
                offset: tok.span.start,
                expected,
                found: format!("{:?}", tok.token),
            })
        }
    }

    fn parse_ident(&mut self) -> Result<String, CypherError> {
        let tok = self.lexer.next_token().ok_or(CypherError::UnexpectedEof {
            expected: "identifier",
        })?;
        match tok.token {
            Token::Ident(s) => Ok(String::from_utf8_lossy(s).into_owned()),
            Token::QuotedIdent(s) => {
                let inner = &s[1..s.len() - 1];
                Ok(String::from_utf8_lossy(inner).into_owned())
            }
            _ => Err(CypherError::UnexpectedToken {
                offset: tok.span.start,
                expected: "identifier",
                found: format!("{:?}", tok.token),
            }),
        }
    }

    fn parse_u32_lit(&mut self) -> Result<u32, CypherError> {
        let tok = self.lexer.next_token().ok_or(CypherError::UnexpectedEof {
            expected: "integer",
        })?;
        if let Token::Integer(s) = tok.token {
            let text = core::str::from_utf8(s).unwrap_or("?");
            text.parse::<u32>().map_err(|_| CypherError::InvalidNumber {
                offset: tok.span.start,
                value: text.to_string(),
            })
        } else {
            Err(CypherError::UnexpectedToken {
                offset: tok.span.start,
                expected: "integer",
                found: format!("{:?}", tok.token),
            })
        }
    }

    fn parse_i64_bytes(&self, s: &[u8], offset: usize) -> Result<i64, CypherError> {
        let text = core::str::from_utf8(s).unwrap_or("?");
        text.parse::<i64>().map_err(|_| CypherError::InvalidNumber {
            offset,
            value: text.to_string(),
        })
    }

    fn parse_f64_bytes(&self, s: &[u8], offset: usize) -> Result<f64, CypherError> {
        let text = core::str::from_utf8(s).unwrap_or("?");
        text.parse::<f64>().map_err(|_| CypherError::InvalidNumber {
            offset,
            value: text.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> Result<CypherQuery, CypherError> {
        let mut parser = Parser::new(input.as_bytes(), 64);
        parser.parse()
    }

    // -----------------------------------------------------------------------
    // Basic MATCH
    // -----------------------------------------------------------------------

    #[test]
    fn test_simple_match_return() {
        let q = parse("MATCH (n) RETURN n").expect("parse failed");
        assert_eq!(q.clauses.len(), 2);
        assert!(matches!(q.clauses[0], Clause::Match(_)));
        assert!(matches!(q.clauses[1], Clause::Return(_)));
    }

    #[test]
    fn test_match_with_label() {
        let q = parse("MATCH (n:Person) RETURN n").expect("parse failed");
        if let Clause::Match(m) = &q.clauses[0] {
            assert_eq!(m.patterns[0].nodes[0].labels, vec!["Person"]);
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_match_with_properties() {
        let q = parse("MATCH (n:Person {name: 'Alice'}) RETURN n").expect("parse failed");
        if let Clause::Match(m) = &q.clauses[0] {
            let node = &m.patterns[0].nodes[0];
            assert_eq!(node.labels, vec!["Person"]);
            assert_eq!(node.properties.len(), 1);
            assert_eq!(node.properties[0].0, "name");
            assert_eq!(node.properties[0].1, Expr::StringLit("Alice".to_string()));
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_match_with_edge() {
        let q = parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").expect("parse failed");
        if let Clause::Match(m) = &q.clauses[0] {
            let pattern = &m.patterns[0];
            assert_eq!(pattern.nodes.len(), 2);
            assert_eq!(pattern.edges.len(), 1);
            assert_eq!(pattern.edges[0].edge_types, vec!["KNOWS"]);
            assert_eq!(pattern.edges[0].direction, EdgeDirection::Right);
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_match_left_arrow() {
        let q = parse("MATCH (a)<-[:FOLLOWS]-(b) RETURN a").expect("parse failed");
        if let Clause::Match(m) = &q.clauses[0] {
            assert_eq!(m.patterns[0].edges[0].direction, EdgeDirection::Left);
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_match_undirected() {
        let q = parse("MATCH (a)-[:KNOWS]-(b) RETURN a").expect("parse failed");
        if let Clause::Match(m) = &q.clauses[0] {
            assert_eq!(m.patterns[0].edges[0].direction, EdgeDirection::Both);
        } else {
            panic!("expected Match clause");
        }
    }

    // -----------------------------------------------------------------------
    // Variable-length paths
    // -----------------------------------------------------------------------

    #[test]
    fn test_variable_length_path() {
        let q = parse("MATCH (n)-[:KNOWS*1..3]->(m) RETURN m").expect("parse failed");
        if let Clause::Match(m) = &q.clauses[0] {
            assert_eq!(m.patterns[0].edges[0].var_length, Some((1, 3)));
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_variable_length_unbounded() {
        let q = parse("MATCH (n)-[*]->(m) RETURN m").expect("parse failed");
        if let Clause::Match(m) = &q.clauses[0] {
            assert_eq!(m.patterns[0].edges[0].var_length, Some((1, u32::MAX)));
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_variable_length_exact() {
        let q = parse("MATCH (n)-[*3]->(m) RETURN m").expect("parse failed");
        if let Clause::Match(m) = &q.clauses[0] {
            assert_eq!(m.patterns[0].edges[0].var_length, Some((3, 3)));
        } else {
            panic!("expected Match clause");
        }
    }

    // -----------------------------------------------------------------------
    // WHERE
    // -----------------------------------------------------------------------

    #[test]
    fn test_where_comparison() {
        let q = parse("MATCH (n) WHERE n.age > 30 RETURN n").expect("parse failed");
        assert!(matches!(q.clauses[1], Clause::Where(_)));
    }

    #[test]
    fn test_where_and_or() {
        let q = parse("MATCH (n) WHERE n.age > 30 AND n.name = 'Alice' RETURN n")
            .expect("parse failed");
        if let Clause::Where(w) = &q.clauses[1] {
            assert!(matches!(
                w.expr,
                Expr::BinaryOp {
                    op: BinaryOperator::And,
                    ..
                }
            ));
        } else {
            panic!("expected Where clause");
        }
    }

    #[test]
    fn test_where_is_null() {
        let q = parse("MATCH (n) WHERE n.email IS NULL RETURN n").expect("parse failed");
        if let Clause::Where(w) = &q.clauses[1] {
            assert!(matches!(w.expr, Expr::IsNull { negated: false, .. }));
        } else {
            panic!("expected Where clause");
        }
    }

    #[test]
    fn test_where_is_not_null() {
        let q = parse("MATCH (n) WHERE n.email IS NOT NULL RETURN n").expect("parse failed");
        if let Clause::Where(w) = &q.clauses[1] {
            assert!(matches!(w.expr, Expr::IsNull { negated: true, .. }));
        } else {
            panic!("expected Where clause");
        }
    }

    // -----------------------------------------------------------------------
    // Parameters (injection prevention)
    // -----------------------------------------------------------------------

    #[test]
    fn test_parameter_not_interpolated() {
        let q = parse("MATCH (n) WHERE n.name = $name RETURN n").expect("parse failed");
        if let Clause::Where(w) = &q.clauses[1] {
            if let Expr::BinaryOp { right, .. } = &w.expr {
                assert_eq!(**right, Expr::Parameter("name".to_string()));
            } else {
                panic!("expected BinaryOp");
            }
        } else {
            panic!("expected Where clause");
        }
    }

    #[test]
    fn test_injection_attempt_treated_as_literal() {
        // Attempt to inject: $name = "' RETURN n UNION MATCH (x) RETURN x --"
        // The parameter is treated as a placeholder, not interpolated
        let q = parse("MATCH (n) WHERE n.name = $name RETURN n").expect("parse failed");
        // Verify $name is an Expr::Parameter, not expanded
        if let Clause::Where(w) = &q.clauses[1] {
            if let Expr::BinaryOp { right, .. } = &w.expr {
                assert!(matches!(**right, Expr::Parameter(_)));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Nesting depth limit
    // -----------------------------------------------------------------------

    #[test]
    fn test_nesting_depth_exceeded() {
        // Create deeply nested expression: (((((...))))) > 64 levels
        let mut query = String::from("MATCH (n) WHERE ");
        for _ in 0..70 {
            query.push('(');
        }
        query.push('1');
        for _ in 0..70 {
            query.push(')');
        }
        query.push_str(" = 1 RETURN n");

        let result = parse(&query);
        assert!(
            matches!(result, Err(CypherError::NestingDepthExceeded { limit: 64 })),
            "expected NestingDepthExceeded, got {:?}",
            result
        );
    }

    #[test]
    fn test_nesting_within_limit() {
        // 10 levels of nesting should be fine
        let mut query = String::from("MATCH (n) WHERE ");
        for _ in 0..10 {
            query.push('(');
        }
        query.push('1');
        for _ in 0..10 {
            query.push(')');
        }
        query.push_str(" = 1 RETURN n");

        let result = parse(&query);
        assert!(result.is_ok(), "expected ok, got {:?}", result);
    }

    // -----------------------------------------------------------------------
    // CALL procedure
    // -----------------------------------------------------------------------

    #[test]
    fn test_call_vector_search() {
        let q = parse(
            "CALL oxid.vector.search('Person', 'embedding', 10, $query_vec) YIELD node, score RETURN node.name, score",
        )
        .expect("parse failed");

        if let Clause::Call(c) = &q.clauses[0] {
            assert_eq!(c.procedure, "oxid.vector.search");
            assert_eq!(c.args.len(), 4);
            assert_eq!(c.yields.len(), 2);
            assert_eq!(c.yields[0].name, "node");
            assert_eq!(c.yields[1].name, "score");
        } else {
            panic!("expected Call clause");
        }
    }

    #[test]
    fn test_call_vector_walk() {
        let q = parse(
            "CALL oxid.graph.vectorWalk($start_node, 'embedding', $query_vec, 5, 0.7) YIELD path, totalScore RETURN path",
        )
        .expect("parse failed");

        if let Clause::Call(c) = &q.clauses[0] {
            assert_eq!(c.procedure, "oxid.graph.vectorWalk");
            assert_eq!(c.args.len(), 5);
        } else {
            panic!("expected Call clause");
        }
    }

    // -----------------------------------------------------------------------
    // CREATE, DELETE, SET, MERGE
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_node() {
        let q = parse("CREATE (n:Person {name: 'Alice', age: 30})").expect("parse failed");
        if let Clause::Create(c) = &q.clauses[0] {
            assert_eq!(c.patterns.len(), 1);
            let node = &c.patterns[0].nodes[0];
            assert_eq!(node.labels, vec!["Person"]);
            assert_eq!(node.properties.len(), 2);
        } else {
            panic!("expected Create clause");
        }
    }

    #[test]
    fn test_delete_detach() {
        let q = parse("MATCH (n) DETACH DELETE n").expect("parse failed");
        if let Clause::Delete(d) = &q.clauses[1] {
            assert!(d.detach);
        } else {
            panic!("expected Delete clause");
        }
    }

    #[test]
    fn test_set_property() {
        let q = parse("MATCH (n) SET n.name = 'Bob' RETURN n").expect("parse failed");
        if let Clause::Set(s) = &q.clauses[1] {
            assert_eq!(s.items.len(), 1);
            if let SetItem::Property {
                variable, property, ..
            } = &s.items[0]
            {
                assert_eq!(variable, "n");
                assert_eq!(property, "name");
            } else {
                panic!("expected Property set item");
            }
        } else {
            panic!("expected Set clause");
        }
    }

    #[test]
    fn test_merge_on_create() {
        let q = parse("MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = 1")
            .expect("parse failed");
        if let Clause::Merge(m) = &q.clauses[0] {
            assert_eq!(m.on_create.len(), 1);
            assert!(m.on_match.is_empty());
        } else {
            panic!("expected Merge clause");
        }
    }

    // -----------------------------------------------------------------------
    // WITH, UNWIND
    // -----------------------------------------------------------------------

    #[test]
    fn test_with_clause() {
        let q = parse("MATCH (n) WITH n.name AS name RETURN name").expect("parse failed");
        if let Clause::With(w) = &q.clauses[1] {
            assert_eq!(w.items.len(), 1);
            assert_eq!(w.items[0].alias.as_deref(), Some("name"));
        } else {
            panic!("expected With clause");
        }
    }

    #[test]
    fn test_unwind_clause() {
        let q = parse("UNWIND [1, 2, 3] AS x RETURN x").expect("parse failed");
        if let Clause::Unwind(u) = &q.clauses[0] {
            assert_eq!(u.alias, "x");
            assert!(matches!(u.expr, Expr::List(_)));
        } else {
            panic!("expected Unwind clause");
        }
    }

    // -----------------------------------------------------------------------
    // ORDER BY, LIMIT, SKIP
    // -----------------------------------------------------------------------

    #[test]
    fn test_order_by_limit() {
        let q = parse("MATCH (n) RETURN n ORDER BY n.age DESC LIMIT 10").expect("parse failed");
        assert!(matches!(q.clauses[2], Clause::OrderBy(_)));
        assert!(matches!(q.clauses[3], Clause::Limit(_)));

        if let Clause::OrderBy(o) = &q.clauses[2] {
            assert_eq!(o.items.len(), 1);
            assert!(!o.items[0].1); // DESC
        }
    }

    #[test]
    fn test_skip() {
        let q = parse("MATCH (n) RETURN n SKIP 5 LIMIT 10").expect("parse failed");
        assert!(matches!(q.clauses[2], Clause::Skip(_)));
        assert!(matches!(q.clauses[3], Clause::Limit(_)));
    }

    // -----------------------------------------------------------------------
    // OPTIONAL MATCH
    // -----------------------------------------------------------------------

    #[test]
    fn test_optional_match() {
        let q =
            parse("MATCH (n) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n, m").expect("parse failed");
        if let Clause::Match(m) = &q.clauses[1] {
            assert!(m.optional);
        } else {
            panic!("expected Match clause");
        }
    }

    // -----------------------------------------------------------------------
    // Read-only check
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_read_only() {
        let q = parse("MATCH (n) RETURN n").expect("parse failed");
        assert!(q.is_read_only());

        let q = parse("CREATE (n:Person)").expect("parse failed");
        assert!(!q.is_read_only());

        let q = parse("MATCH (n) DELETE n").expect("parse failed");
        assert!(!q.is_read_only());
    }

    // -----------------------------------------------------------------------
    // Function calls
    // -----------------------------------------------------------------------

    #[test]
    fn test_function_call() {
        let q = parse("MATCH (n) RETURN count(n)").expect("parse failed");
        if let Clause::Return(r) = &q.clauses[1] {
            if let Expr::FunctionCall { name, args, .. } = &r.items[0].expr {
                assert_eq!(name, "count");
                assert_eq!(args.len(), 1);
            } else {
                panic!("expected FunctionCall");
            }
        }
    }

    #[test]
    fn test_collect_distinct() {
        let q = parse("MATCH (n) RETURN collect(DISTINCT n.name)").expect("parse failed");
        if let Clause::Return(r) = &q.clauses[1] {
            if let Expr::FunctionCall { name, distinct, .. } = &r.items[0].expr {
                assert_eq!(name, "collect");
                assert!(*distinct);
            } else {
                panic!("expected FunctionCall");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Complex full query
    // -----------------------------------------------------------------------

    #[test]
    fn test_full_complex_query() {
        let q = parse(
            "MATCH (n:Person)-[:KNOWS*1..3]->(m) WHERE n.name = $name RETURN m ORDER BY m.age LIMIT 10",
        )
        .expect("parse failed");

        assert_eq!(q.clauses.len(), 5);
        assert!(matches!(q.clauses[0], Clause::Match(_)));
        assert!(matches!(q.clauses[1], Clause::Where(_)));
        assert!(matches!(q.clauses[2], Clause::Return(_)));
        assert!(matches!(q.clauses[3], Clause::OrderBy(_)));
        assert!(matches!(q.clauses[4], Clause::Limit(_)));

        // Match clause details
        if let Clause::Match(m) = &q.clauses[0] {
            let pattern = &m.patterns[0];
            assert_eq!(pattern.nodes.len(), 2);
            assert_eq!(pattern.edges.len(), 1);
            assert_eq!(pattern.nodes[0].labels, vec!["Person"]);
            assert_eq!(pattern.edges[0].edge_types, vec!["KNOWS"]);
            assert_eq!(pattern.edges[0].var_length, Some((1, 3)));
            assert_eq!(pattern.edges[0].direction, EdgeDirection::Right);
        }
    }

    #[test]
    fn test_hybrid_query_pattern() {
        // Pattern 1: Graph-filtered vector search
        let q = parse(
            "MATCH (y:Person {name: 'Alice'})-[:KNOWS*1..2]-(friend) \
             WITH collect(friend) AS neighborhood \
             CALL oxid.vector.search('Person', 'embedding', 10, $query_vec, neighborhood) \
             YIELD node, score \
             RETURN node.name, score ORDER BY score DESC",
        )
        .expect("parse failed");

        // Should have: MATCH, WITH, CALL, RETURN, ORDER BY
        assert_eq!(q.clauses.len(), 5);
        assert!(matches!(q.clauses[0], Clause::Match(_)));
        assert!(matches!(q.clauses[1], Clause::With(_)));
        assert!(matches!(q.clauses[2], Clause::Call(_)));
        assert!(matches!(q.clauses[3], Clause::Return(_)));
        assert!(matches!(q.clauses[4], Clause::OrderBy(_)));
    }

    // -----------------------------------------------------------------------
    // Empty / edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_empty_query() {
        let result = parse("");
        assert!(matches!(result, Err(CypherError::EmptyQuery)));
    }

    #[test]
    fn test_return_star() {
        let q = parse("MATCH (n) RETURN *").expect("parse failed");
        if let Clause::Return(r) = &q.clauses[1] {
            assert!(matches!(r.items[0].expr, Expr::Star));
        }
    }

    #[test]
    fn test_return_distinct() {
        let q = parse("MATCH (n) RETURN DISTINCT n.name").expect("parse failed");
        if let Clause::Return(r) = &q.clauses[1] {
            assert!(r.distinct);
        }
    }

    #[test]
    fn test_in_expression() {
        let q = parse("MATCH (n) WHERE n.age IN [20, 30, 40] RETURN n").expect("parse failed");
        if let Clause::Where(w) = &q.clauses[1] {
            assert!(matches!(w.expr, Expr::InList { .. }));
        }
    }

    #[test]
    fn test_multiple_patterns() {
        let q = parse("MATCH (a)-[:R]->(b), (c)-[:S]->(d) RETURN a, c").expect("parse failed");
        if let Clause::Match(m) = &q.clauses[0] {
            assert_eq!(m.patterns.len(), 2);
        }
    }

    #[test]
    fn test_multiple_labels() {
        let q = parse("MATCH (n:Person:Employee) RETURN n").expect("parse failed");
        if let Clause::Match(m) = &q.clauses[0] {
            assert_eq!(m.patterns[0].nodes[0].labels, vec!["Person", "Employee"]);
        }
    }

    #[test]
    fn test_edge_with_variable() {
        let q = parse("MATCH (a)-[r:KNOWS]->(b) RETURN r").expect("parse failed");
        if let Clause::Match(m) = &q.clauses[0] {
            assert_eq!(m.patterns[0].edges[0].variable.as_deref(), Some("r"));
        }
    }

    #[test]
    fn test_yield_with_alias() {
        let q = parse(
            "CALL oxid.vector.search('Doc', 'emb', 5, $v) YIELD node AS doc, score RETURN doc",
        )
        .expect("parse failed");
        if let Clause::Call(c) = &q.clauses[0] {
            assert_eq!(c.yields[0].alias.as_deref(), Some("doc"));
            assert!(c.yields[1].alias.is_none());
        }
    }
}

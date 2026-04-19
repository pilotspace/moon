//! Expression parsing for the Cypher parser.
//!
//! Precedence climbing: OR > AND > NOT > comparison > addition > multiplication > unary > primary.

use super::*;

impl<'a> Parser<'a> {
    // -----------------------------------------------------------------------
    // Expression parsing (precedence climbing)
    // -----------------------------------------------------------------------

    pub(super) fn parse_expression(&mut self) -> Result<Expr, CypherError> {
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
                    // v0.1.9 CYP-05: `shortestPath((a)-[*..N]-(b))` as a
                    // RETURN/expression form. The argument is a graph
                    // pattern, NOT an expression — intercept before the
                    // generic FunctionCall path.
                    if name.eq_ignore_ascii_case("shortestPath") {
                        self.advance(); // consume LParen
                        let src = self.parse_pattern_node()?;
                        let edge = self.parse_pattern_edge()?;
                        let dst = self.parse_pattern_node()?;
                        self.expect_token_match(|t| matches!(t, Token::RParen), ")")?;
                        let max_hops = match edge.var_length {
                            Some((_, max)) => max,
                            None => {
                                return Err(CypherError::UnexpectedToken {
                                    offset: t.span.start,
                                    expected: "variable-length edge in shortestPath(..)",
                                    found: "fixed-length edge".to_string(),
                                });
                            }
                        };
                        return Ok(Expr::ShortestPathCall {
                            src,
                            dst,
                            max_hops,
                            edge_types: edge.edge_types,
                            direction: edge.direction,
                        });
                    }

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
    pub(super) fn parse_postfix(&mut self, mut expr: Expr) -> Result<Expr, CypherError> {
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
}

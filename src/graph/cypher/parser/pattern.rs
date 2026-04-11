//! Pattern parsing for the Cypher parser.
//!
//! Handles node patterns `(n:Label {props})`, edge patterns `-[:TYPE]->`,
//! variable-length paths `*1..3`, and property maps `{key: value}`.

use super::*;

impl<'a> Parser<'a> {
    // -----------------------------------------------------------------------
    // Pattern parsing
    // -----------------------------------------------------------------------

    pub(super) fn parse_pattern_list(&mut self) -> Result<Vec<Pattern>, CypherError> {
        let mut patterns = vec![self.parse_pattern()?];
        while self.peek_is(|t| matches!(t, Token::Comma)) {
            self.advance();
            patterns.push(self.parse_pattern()?);
        }
        Ok(patterns)
    }

    pub(super) fn parse_pattern(&mut self) -> Result<Pattern, CypherError> {
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

    pub(super) fn parse_property_map(&mut self) -> Result<Vec<(String, Expr)>, CypherError> {
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
}

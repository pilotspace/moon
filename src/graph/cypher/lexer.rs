//! Logos-based Cypher lexer operating on `&[u8]` slices.
//!
//! Tokens borrow from the input slice -- zero heap allocation during lexing.
//! All keyword matching is case-insensitive via logos callbacks.

use logos::Logos;

/// Token type produced by the Cypher lexer.
///
/// Each variant either carries no data (keywords, punctuation) or borrows
/// a `&[u8]` slice from the input (identifiers, literals, parameters).
#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(source = [u8])]
#[logos(skip br"[ \t\r\n]+")]
pub enum Token<'a> {
    // --- Keywords (case-insensitive via callback) ---
    #[token(b"MATCH", ignore(ascii_case))]
    Match,
    #[token(b"OPTIONAL", ignore(ascii_case))]
    Optional,
    #[token(b"WHERE", ignore(ascii_case))]
    Where,
    #[token(b"RETURN", ignore(ascii_case))]
    Return,
    #[token(b"CREATE", ignore(ascii_case))]
    Create,
    #[token(b"DELETE", ignore(ascii_case))]
    Delete,
    #[token(b"DETACH", ignore(ascii_case))]
    Detach,
    #[token(b"SET", ignore(ascii_case))]
    Set,
    #[token(b"MERGE", ignore(ascii_case))]
    Merge,
    #[token(b"WITH", ignore(ascii_case))]
    With,
    #[token(b"UNWIND", ignore(ascii_case))]
    Unwind,
    #[token(b"ORDER", ignore(ascii_case))]
    Order,
    #[token(b"BY", ignore(ascii_case))]
    By,
    #[token(b"LIMIT", ignore(ascii_case))]
    Limit,
    #[token(b"SKIP", ignore(ascii_case))]
    Skip,
    #[token(b"AS", ignore(ascii_case))]
    As,
    #[token(b"AND", ignore(ascii_case))]
    And,
    #[token(b"OR", ignore(ascii_case))]
    Or,
    #[token(b"NOT", ignore(ascii_case))]
    Not,
    #[token(b"TRUE", ignore(ascii_case))]
    True,
    #[token(b"FALSE", ignore(ascii_case))]
    False,
    #[token(b"NULL", ignore(ascii_case))]
    Null,
    #[token(b"IN", ignore(ascii_case))]
    In,
    #[token(b"IS", ignore(ascii_case))]
    Is,
    #[token(b"CALL", ignore(ascii_case))]
    Call,
    #[token(b"YIELD", ignore(ascii_case))]
    Yield,
    #[token(b"DISTINCT", ignore(ascii_case))]
    Distinct,
    #[token(b"ASC", ignore(ascii_case))]
    Asc,
    #[token(b"DESC", ignore(ascii_case))]
    Desc,
    #[token(b"ON", ignore(ascii_case))]
    On,

    // --- Identifiers ---
    // Must come after keywords so logos prefers keyword matches.
    #[regex(br"[a-zA-Z_][a-zA-Z0-9_]*")]
    Ident(&'a [u8]),

    // --- Literals ---
    #[regex(br"-?[0-9]+", priority = 2)]
    Integer(&'a [u8]),

    #[regex(br"-?[0-9]+\.[0-9]+")]
    Float(&'a [u8]),

    // Single-quoted string: content between quotes (no escapes for now).
    #[regex(br"'[^']*'")]
    StringLit(&'a [u8]),

    // Double-quoted string (for identifiers with spaces).
    #[regex(br#""[^"]*""#)]
    QuotedIdent(&'a [u8]),

    // --- Parameters ---
    #[regex(br"\$[a-zA-Z_][a-zA-Z0-9_]*")]
    Parameter(&'a [u8]),

    // --- Operators ---
    #[token(b"<>")]
    NotEqual,
    #[token(b"<=")]
    LessEqual,
    #[token(b">=")]
    GreaterEqual,
    #[token(b"=~")]
    RegexMatch,
    #[token(b"=")]
    Equal,
    #[token(b"<")]
    LessThan,
    #[token(b">")]
    GreaterThan,
    #[token(b"+")]
    Plus,
    #[token(b"-")]
    Minus,
    #[token(b"*")]
    Star,
    #[token(b"/")]
    Slash,
    #[token(b"%")]
    Percent,

    // --- Punctuation ---
    #[token(b"(")]
    LParen,
    #[token(b")")]
    RParen,
    #[token(b"[")]
    LBracket,
    #[token(b"]")]
    RBracket,
    #[token(b"{")]
    LBrace,
    #[token(b"}")]
    RBrace,
    #[token(b":")]
    Colon,
    #[token(b"..")]
    DotDot,
    #[token(b".")]
    Dot,
    #[token(b",")]
    Comma,
    #[token(b"|")]
    Pipe,

    // --- Arrows ---
    #[token(b"->")]
    ArrowRight,
    #[token(b"<-")]
    ArrowLeft,

    // --- Comments (skipped) ---
    #[regex(br"//[^\n]*")]
    LineComment,

    // --- Block comments ---
    #[regex(br"/\*([^*]|\*[^/])*\*/")]
    BlockComment,
}

/// A positioned token: the token plus its byte offset in the source.
#[derive(Debug, Clone)]
pub struct SpannedToken<'a> {
    pub token: Token<'a>,
    pub span: core::ops::Range<usize>,
}

/// Cypher lexer wrapping logos.
///
/// Provides `next_token()` and `peek()` with zero allocation.
pub struct Lexer<'a> {
    inner: logos::Lexer<'a, Token<'a>>,
    peeked: Option<Option<SpannedToken<'a>>>,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer over the given byte slice.
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            inner: Token::lexer(input),
            peeked: None,
        }
    }

    /// Advance and return the next token, or `None` at EOF.
    /// Skips comments automatically.
    pub fn next_token(&mut self) -> Option<SpannedToken<'a>> {
        if let Some(peeked) = self.peeked.take() {
            return peeked;
        }
        self.advance()
    }

    /// Peek at the next token without consuming it.
    pub fn peek(&mut self) -> Option<&SpannedToken<'a>> {
        if self.peeked.is_none() {
            let next = self.advance();
            self.peeked = Some(next);
        }
        self.peeked.as_ref().and_then(|opt| opt.as_ref())
    }

    /// Internal: advance the logos lexer, skipping comments and errors.
    fn advance(&mut self) -> Option<SpannedToken<'a>> {
        loop {
            let result = self.inner.next()?;
            let span = self.inner.span();
            match result {
                Ok(Token::LineComment) | Ok(Token::BlockComment) => continue,
                Ok(token) => {
                    return Some(SpannedToken { token, span });
                }
                Err(()) => {
                    // Skip unrecognized bytes and continue.
                    // The parser will handle unexpected tokens.
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keywords_case_insensitive() {
        let input = b"MATCH match Match";
        let mut lexer = Lexer::new(input);
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::Match));
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::Match));
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::Match));
        assert!(lexer.next_token().is_none());
    }

    #[test]
    fn test_identifiers() {
        let input = b"name age_2 _private";
        let mut lexer = Lexer::new(input);
        assert_eq!(
            lexer.next_token().map(|t| t.token),
            Some(Token::Ident(b"name"))
        );
        assert_eq!(
            lexer.next_token().map(|t| t.token),
            Some(Token::Ident(b"age_2"))
        );
        assert_eq!(
            lexer.next_token().map(|t| t.token),
            Some(Token::Ident(b"_private"))
        );
    }

    #[test]
    fn test_parameters() {
        let input = b"$name $query_vec";
        let mut lexer = Lexer::new(input);
        assert_eq!(
            lexer.next_token().map(|t| t.token),
            Some(Token::Parameter(b"$name"))
        );
        assert_eq!(
            lexer.next_token().map(|t| t.token),
            Some(Token::Parameter(b"$query_vec"))
        );
    }

    #[test]
    fn test_numbers() {
        let input = b"42 3.14 -7";
        let mut lexer = Lexer::new(input);
        assert_eq!(
            lexer.next_token().map(|t| t.token),
            Some(Token::Integer(b"42"))
        );
        assert_eq!(
            lexer.next_token().map(|t| t.token),
            Some(Token::Float(b"3.14"))
        );
        assert_eq!(
            lexer.next_token().map(|t| t.token),
            Some(Token::Integer(b"-7"))
        );
    }

    #[test]
    fn test_string_literals() {
        let input = b"'hello world'";
        let mut lexer = Lexer::new(input);
        assert_eq!(
            lexer.next_token().map(|t| t.token),
            Some(Token::StringLit(b"'hello world'"))
        );
    }

    #[test]
    fn test_operators_and_punctuation() {
        let input = b"( ) [ ] { } : , . .. | = <> < > <= >= + - * / ->";
        let mut lexer = Lexer::new(input);
        let expected = vec![
            Token::LParen,
            Token::RParen,
            Token::LBracket,
            Token::RBracket,
            Token::LBrace,
            Token::RBrace,
            Token::Colon,
            Token::Comma,
            Token::Dot,
            Token::DotDot,
            Token::Pipe,
            Token::Equal,
            Token::NotEqual,
            Token::LessThan,
            Token::GreaterThan,
            Token::LessEqual,
            Token::GreaterEqual,
            Token::Plus,
            Token::Minus,
            Token::Star,
            Token::Slash,
            Token::ArrowRight,
        ];
        for exp in expected {
            let tok = lexer.next_token().map(|t| t.token);
            assert_eq!(tok, Some(exp));
        }
    }

    #[test]
    fn test_peek_does_not_consume() {
        let input = b"MATCH RETURN";
        let mut lexer = Lexer::new(input);
        assert_eq!(lexer.peek().map(|t| &t.token), Some(&Token::Match));
        assert_eq!(lexer.peek().map(|t| &t.token), Some(&Token::Match));
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::Match));
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::Return));
    }

    #[test]
    fn test_arrow_tokens() {
        let input = b"()-[]->()<-[]-()";
        let mut lexer = Lexer::new(input);
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::LParen));
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::RParen));
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::Minus));
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::LBracket));
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::RBracket));
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::ArrowRight));
    }

    #[test]
    fn test_comments_skipped() {
        let input = b"MATCH // line comment\nRETURN /* block */";
        let mut lexer = Lexer::new(input);
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::Match));
        assert_eq!(lexer.next_token().map(|t| t.token), Some(Token::Return));
        assert!(lexer.next_token().is_none());
    }

    #[test]
    fn test_full_cypher_pattern() {
        let input = b"MATCH (n:Person)-[:KNOWS*1..3]->(m) WHERE n.name = $name RETURN m";
        let mut lexer = Lexer::new(input);
        let mut tokens = Vec::new();
        while let Some(t) = lexer.next_token() {
            tokens.push(t.token);
        }
        // Verify key tokens are present
        assert_eq!(tokens[0], Token::Match);
        assert_eq!(tokens[1], Token::LParen);
        assert!(matches!(tokens[2], Token::Ident(b"n")));
    }
}

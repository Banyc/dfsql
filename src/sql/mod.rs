use thiserror::Error;

use self::lexer::Token;

pub mod expr;
pub mod lexer;
pub mod stat;

pub(crate) type Tokens<'a> = &'a [Token];

#[derive(Debug, Error, Clone)]
pub enum ParseError {
    #[error("{0}")]
    Lexer(String),
    #[error("{0}")]
    Parser(String),
}

#[derive(Debug, Clone)]
pub(crate) struct TokenParseError {
    pub(crate) offset: usize,
    pub(crate) detail: String,
}

impl TokenParseError {
    pub(crate) fn new(offset: usize, detail: String) -> Self {
        Self { offset, detail }
    }

    pub(crate) fn byte_offset(&self, spans: &[lexer::TokenSpan]) -> usize {
        spans
            .get(self.offset)
            .map(|span| span.range.start)
            .or_else(|| spans.last().map(|span| span.range.end))
            .unwrap_or(0)
    }

    pub(crate) fn render_source(
        &self,
        source: &str,
        _tokens: &[Token],
        spans: &[lexer::TokenSpan],
        _max_offset: usize,
    ) -> String {
        let byte_off = self.byte_offset(spans);
        render_source_error(source, byte_off, &self.detail)
    }
}

fn render_source_error(source: &str, byte_offset: usize, detail: &str) -> String {
    let line_start = source[..byte_offset]
        .rfind('\n')
        .map(|i| i + 1)
        .unwrap_or(0);
    let line_end = source[byte_offset..]
        .find('\n')
        .map(|i| byte_offset + i)
        .unwrap_or(source.len());
    let line_num = source[..byte_offset].matches('\n').count() + 1;
    let col = byte_offset - line_start + 1;
    let offending_line = &source[line_start..line_end];
    let caret = " ".repeat(col - 1) + "^";

    format!("at line {line_num}, column {col}:\n  {offending_line}\n  {caret}\n{detail}")
}

pub fn parse(source: &str) -> Result<S, ParseError> {
    let tokens = lexer::lex(source).map_err(ParseError::Lexer)?;
    match stat::parse_detailed(&tokens) {
        Ok(statements) => Ok(statements),
        Err(error) => {
            let spans = lexer::token_spans(source, &tokens).map_err(ParseError::Lexer)?;
            let top_level = error.render_source(source, &tokens, &spans, source.len());
            let nested = nested_expression_error(source, &tokens, &spans);
            Err(ParseError::Parser(
                nested.map(|(_, message)| message).unwrap_or(top_level),
            ))
        }
    }
}

pub(crate) fn nested_expression_error(
    source: &str,
    tokens: &[Token],
    spans: &[lexer::TokenSpan],
) -> Option<(usize, String)> {
    let mut earliest: Option<(usize, String)> = None;
    for (token, span) in tokens.iter().zip(spans) {
        let inner = match token {
            Token::Parens(inner) | Token::Brackets(inner) => inner,
            _ => continue,
        };
        if matches!(token, Token::Parens(_)) {
            let mut remaining = inner.as_slice();
            let direct_error = match expr::expr(&mut remaining) {
                Ok(_) if remaining.is_empty() => None,
                Ok(_) => Some(format!("unexpected token {:?}", remaining.first())),
                Err(detail) => Some(detail),
            };
            if let Some(detail) = direct_error {
                let token_offset = inner.len() - remaining.len();
                let byte_offset = span
                    .nested
                    .get(token_offset)
                    .map(|nested| nested.range.start)
                    .unwrap_or_else(|| span.range.end.saturating_sub(1));
                earliest = Some((
                    byte_offset,
                    render_source_error(source, byte_offset, &detail),
                ));
            }
        }
        if let Some((off, msg)) = nested_expression_error(source, inner, &span.nested) {
            match &earliest {
                Some((best_off, _)) if off < *best_off => earliest = Some((off, msg)),
                None => earliest = Some((off, msg)),
                _ => {}
            }
        }
    }
    earliest
}

#[derive(Debug, Clone, PartialEq)]
pub struct S {
    pub statements: Vec<stat::Stat>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

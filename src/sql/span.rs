use std::ops::Range;

use super::lexer::{Token, skip_ws, token};

#[derive(Debug)]
pub(crate) struct TokenSpan {
    pub(crate) range: Range<usize>,
    pub(crate) nested: Vec<TokenSpan>,
}

pub(crate) fn token_spans(source: &str, tokens: &[Token]) -> Result<Vec<TokenSpan>, String> {
    let mut input = source;
    let mut offset = 0;
    let spans = spans_for_tokens(&mut input, &mut offset, tokens)?;
    skip_ws_at(&mut input, &mut offset);
    if input.is_empty() {
        Ok(spans)
    } else {
        Err(format!("unexpected input {:?}", input))
    }
}

fn spans_for_tokens(
    input: &mut &str,
    offset: &mut usize,
    tokens: &[Token],
) -> Result<Vec<TokenSpan>, String> {
    let mut spans = Vec::new();
    for expected in tokens {
        skip_ws_at(input, offset);
        let start = *offset;
        let nested = match expected {
            Token::Parens(inner) => {
                consume_char(input, offset, '(')?;
                let nested = spans_for_tokens(input, offset, inner)?;
                skip_ws_at(input, offset);
                consume_char(input, offset, ')')?;
                nested
            }
            Token::Brackets(inner) => {
                consume_char(input, offset, '[')?;
                let nested = spans_for_tokens(input, offset, inner)?;
                skip_ws_at(input, offset);
                consume_char(input, offset, ']')?;
                nested
            }
            _ => {
                let before = input.len();
                let actual = token(input).map_err(|e| format!("{:?}", e))?;
                if actual != *expected {
                    return Err(format!("expected token {:?}, got {:?}", expected, actual));
                }
                *offset += before - input.len();
                Vec::new()
            }
        };
        spans.push(TokenSpan {
            range: start..*offset,
            nested,
        });
    }
    Ok(spans)
}

fn skip_ws_at(input: &mut &str, offset: &mut usize) {
    let before = input.len();
    skip_ws(input);
    *offset += before - input.len();
}

fn consume_char(input: &mut &str, offset: &mut usize, expected: char) -> Result<(), String> {
    match input.chars().next() {
        Some(actual) if actual == expected => {
            *input = &input[actual.len_utf8()..];
            *offset += actual.len_utf8();
            Ok(())
        }
        actual => Err(format!("expected {:?}, got {:?}", expected, actual)),
    }
}

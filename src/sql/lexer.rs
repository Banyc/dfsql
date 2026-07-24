use winnow::prelude::*;
use winnow::stream::AsChar;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Stat(StatKeyword),
    Conditional(Conditional),
    Type(Type),
    ExprKeyword(ExprKeyword),
    StringKeyword(StringKeyword),
    Parens(Vec<Token>),
    Brackets(Vec<Token>),
    Variable(String),
    Literal(Literal),
    Symbol(Symbol),
}

pub fn lex(source: &str) -> Result<Vec<Token>, String> {
    let mut input = source;
    let tokens = lexer.parse_next(&mut input).map_err(|e| format!("{e:?}"))?;
    skip_ws(&mut input);
    if input.is_empty() {
        Ok(tokens)
    } else {
        Err(format!("unexpected input {input:?}"))
    }
}

pub fn lexer(input: &mut &str) -> ModalResult<Vec<Token>> {
    let mut tokens = Vec::new();
    loop {
        skip_ws(input);
        if input.is_empty() || matches!(input.chars().next(), Some(')' | ']')) {
            break Ok(tokens);
        }
        let start = input.len();
        let token = token(input)?;
        tokens.push(token);
        if input.len() == start {
            return Err(winnow::error::ErrMode::Backtrack(
                winnow::error::ContextError::new(),
            ));
        }
    }
}

fn skip_ws(input: &mut &str) {
    while let Some(c) = input.chars().next() {
        if c.is_whitespace() {
            *input = &input[c.len_utf8()..];
        } else {
            break;
        }
    }
}

fn token(input: &mut &str) -> ModalResult<Token> {
    let c = input
        .chars()
        .next()
        .ok_or_else(|| winnow::error::ErrMode::Backtrack(winnow::error::ContextError::new()))?;
    match c {
        '(' => {
            *input = &input[1..];
            let inner = lexer.parse_next(input)?;
            skip_ws(input);
            expect_char(input, ')')?;
            Ok(Token::Parens(inner))
        }
        ')' => Err(winnow::error::ErrMode::Backtrack(
            winnow::error::ContextError::new(),
        )),
        '[' => {
            *input = &input[1..];
            let inner = lexer.parse_next(input)?;
            skip_ws(input);
            expect_char(input, ']')?;
            Ok(Token::Brackets(inner))
        }
        ']' => Err(winnow::error::ErrMode::Backtrack(
            winnow::error::ContextError::new(),
        )),
        '"' => parse_string(input).map(|s| Token::Literal(Literal::String(s))),
        '-' => {
            *input = &input[1..];
            Ok(Token::Symbol(Symbol::Sub))
        }
        '0'..='9' => number_literal(input).map(Token::Literal),
        '+' | '*' | '/' | '=' | '!' | '<' | '>' | '&' | '|' | ',' | '%' => {
            symbol_char(c);
            *input = &input[1..];
            Ok(Token::Symbol(symbol_char(c)))
        }
        'a'..='z' | 'A'..='Z' | '_' => {
            let ident = parse_ident(input);
            Ok(keyword_or_var(ident))
        }
        _ => Err(winnow::error::ErrMode::Backtrack(
            winnow::error::ContextError::new(),
        )),
    }
}

fn symbol_char(c: char) -> Symbol {
    match c {
        '+' => Symbol::Add,
        '*' => Symbol::Mul,
        '/' => Symbol::Div,
        '=' => Symbol::Eq,
        '!' => Symbol::Bang,
        '<' => Symbol::LeftAngle,
        '>' => Symbol::RightAngle,
        '&' => Symbol::Ampersand,
        '|' => Symbol::Pipe,
        ',' => Symbol::Comma,
        '%' => Symbol::Percent,
        _ => unreachable!(),
    }
}

fn expect_char(input: &mut &str, expected: char) -> ModalResult<()> {
    match input.chars().next() {
        Some(c) if c == expected => {
            *input = &input[expected.len_utf8()..];
            Ok(())
        }
        _ => Err(winnow::error::ErrMode::Backtrack(
            winnow::error::ContextError::new(),
        )),
    }
}

fn parse_ident<'a>(input: &mut &'a str) -> &'a str {
    let start = *input;
    let mut len = 0;
    for c in input.chars() {
        if c.is_alphanum() || c == '_' {
            len += c.len_utf8();
        } else {
            break;
        }
    }
    *input = &start[len..];
    &start[..len]
}

fn keyword_or_var(ident: &str) -> Token {
    let lower = ident.to_ascii_lowercase();
    if let Some(kw) = stat_keyword(&lower) {
        return Token::Stat(kw);
    }
    if let Some(kw) = expr_keyword(ident, &lower) {
        return kw;
    }
    if let Some(kw) = type_keyword(&lower) {
        return Token::Type(kw);
    }
    if let Some(kw) = conditional_keyword(&lower) {
        return Token::Conditional(kw);
    }
    if let Some(kw) = string_keyword(&lower) {
        return Token::StringKeyword(kw);
    }
    Token::Variable(ident.to_string())
}

fn stat_keyword(s: &str) -> Option<StatKeyword> {
    Some(match s {
        "select" => StatKeyword::Select,
        "group" => StatKeyword::GroupBy,
        "agg" => StatKeyword::Agg,
        "filter" => StatKeyword::Filter,
        "limit" => StatKeyword::Limit,
        "reverse" => StatKeyword::Reverse,
        "sort" => StatKeyword::Sort,
        "join" => StatKeyword::Join,
        "on" => StatKeyword::On,
        "left" => StatKeyword::Left,
        "right" => StatKeyword::Right,
        "inner" => StatKeyword::Inner,
        "full" => StatKeyword::Full,
        "use" => StatKeyword::Use,
        "clone" => StatKeyword::Clone,
        _ => return None,
    })
}

fn expr_keyword(_original: &str, lower: &str) -> Option<Token> {
    let kw = match lower {
        "sum" => ExprKeyword::Sum,
        "sqrt" => ExprKeyword::Sqrt,
        "count" => ExprKeyword::Count,
        "len" => ExprKeyword::Len,
        "first" => ExprKeyword::First,
        "last" => ExprKeyword::Last,
        "col_sort" => ExprKeyword::Sort,
        "asc" => ExprKeyword::Asc,
        "desc" => ExprKeyword::Desc,
        "col_reverse" => ExprKeyword::Reverse,
        "mean" => ExprKeyword::Mean,
        "median" => ExprKeyword::Median,
        "max" => ExprKeyword::Max,
        "min" => ExprKeyword::Min,
        "var" => ExprKeyword::Var,
        "std" => ExprKeyword::Std,
        "abs" => ExprKeyword::Abs,
        "unique" => ExprKeyword::Unique,
        "by" => ExprKeyword::By,
        "is" => ExprKeyword::Is,
        "alias" => ExprKeyword::Alias,
        "col" => ExprKeyword::Col,
        "exclude" => ExprKeyword::Exclude,
        "cast" => ExprKeyword::Cast,
        "nan" => ExprKeyword::Nan,
        "all" => ExprKeyword::All,
        "any" => ExprKeyword::Any,
        "pow" => ExprKeyword::Pow,
        "log" => ExprKeyword::Log,
        _ => return None,
    };
    Some(Token::ExprKeyword(kw))
}

fn type_keyword(s: &str) -> Option<Type> {
    Some(match s {
        "str" => Type::Str,
        "uint" => Type::UInt,
        "int" => Type::Int,
        "float" => Type::Float,
        _ => return None,
    })
}

fn conditional_keyword(s: &str) -> Option<Conditional> {
    Some(match s {
        "if" => Conditional::If,
        "then" => Conditional::Then,
        "else" => Conditional::Else,
        _ => return None,
    })
}

fn string_keyword(s: &str) -> Option<StringKeyword> {
    Some(match s {
        "contains" => StringKeyword::Contains,
        "extract" => StringKeyword::Extract,
        "split" => StringKeyword::Split,
        _ => return None,
    })
}

fn number_literal(input: &mut &str) -> ModalResult<Literal> {
    let start = *input;
    let mut seen_dot = false;
    let mut len = 0;
    for c in input.chars() {
        if c.is_dec_digit() {
            len += c.len_utf8();
        } else if c == '.' && !seen_dot {
            seen_dot = true;
            len += c.len_utf8();
        } else {
            break;
        }
    }
    if len == 0 {
        return Err(winnow::error::ErrMode::Backtrack(
            winnow::error::ContextError::new(),
        ));
    }
    let s = &start[..len];
    *input = &start[len..];
    if seen_dot {
        Ok(Literal::Float(s.to_string()))
    } else {
        Ok(Literal::Int(s.to_string()))
    }
}

fn parse_string(input: &mut &str) -> ModalResult<String> {
    let _ = expect_char(input, '"');
    let mut result = String::new();
    loop {
        match input.chars().next() {
            None => {
                return Err(winnow::error::ErrMode::Backtrack(
                    winnow::error::ContextError::new(),
                ));
            }
            Some('"') => {
                *input = &input[1..];
                return Ok(result);
            }
            Some('\\') => {
                *input = &input[1..];
                match input.chars().next() {
                    None => {
                        return Err(winnow::error::ErrMode::Backtrack(
                            winnow::error::ContextError::new(),
                        ));
                    }
                    Some(c) => {
                        let esc = match c {
                            '"' => '"',
                            '\\' => '\\',
                            '/' => '/',
                            'b' => '\x08',
                            'f' => '\x0C',
                            'n' => '\n',
                            'r' => '\r',
                            't' => '\t',
                            'u' => {
                                *input = &input[1..];
                                let hex: String = input.chars().take(4).collect();
                                if hex.len() < 4 {
                                    return Err(winnow::error::ErrMode::Backtrack(
                                        winnow::error::ContextError::new(),
                                    ));
                                }
                                *input = &input[hex.len()..];
                                let code = u32::from_str_radix(&hex, 16).map_err(|_| {
                                    winnow::error::ErrMode::Backtrack(
                                        winnow::error::ContextError::new(),
                                    )
                                })?;
                                char::from_u32(code).ok_or_else(|| {
                                    winnow::error::ErrMode::Backtrack(
                                        winnow::error::ContextError::new(),
                                    )
                                })?
                            }
                            _ => {
                                return Err(winnow::error::ErrMode::Backtrack(
                                    winnow::error::ContextError::new(),
                                ));
                            }
                        };
                        result.push(esc);
                        *input = &input[c.len_utf8()..];
                    }
                }
            }
            Some(c) => {
                result.push(c);
                *input = &input[c.len_utf8()..];
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum StatKeyword {
    Select,
    GroupBy,
    Agg,
    Filter,
    Limit,
    Reverse,
    Sort,
    Join,
    On,
    Left,
    Right,
    Inner,
    Full,
    Use,
    Clone,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Symbol {
    Bang,
    LeftAngle,
    RightAngle,
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Ampersand,
    Pipe,
    Comma,
    Percent,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExprKeyword {
    Sum,
    Sqrt,
    Count,
    Len,
    First,
    Last,
    Sort,
    Asc,
    Desc,
    Reverse,
    Mean,
    Median,
    Max,
    Min,
    Var,
    Std,
    Abs,
    Unique,
    By,
    Is,
    Alias,
    Col,
    Exclude,
    Cast,
    Nan,
    All,
    Any,
    Pow,
    Log,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Int(String),
    Float(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
    Str,
    UInt,
    Int,
    Float,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Conditional {
    If,
    Then,
    Else,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StringKeyword {
    Contains,
    Extract,
    All,
    Split,
}

use std::ops::Range;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lexer_empty() {
        let tokens = lex("").unwrap();
        assert!(tokens.is_empty());
    }

    #[test]
    fn rejects_unbalanced_delimiters() {
        assert_eq!(
            lex("(a)").unwrap(),
            [Token::Parens(vec![Token::Variable("a".into())])]
        );
        assert!(lex("a)").is_err());
        assert!(lex("(a").is_err());
        assert!(lex("[a").is_err());
    }

    #[test]
    fn test_lexer() {
        let src =
            r#"select group agg sum count filter alias col [ ] ( ) < > hi + - * / = -42 0.1 "hi""#;
        let tokens = lex(src).unwrap();
        assert_eq!(
            tokens,
            [
                Token::Stat(StatKeyword::Select),
                Token::Stat(StatKeyword::GroupBy),
                Token::Stat(StatKeyword::Agg),
                Token::ExprKeyword(ExprKeyword::Sum),
                Token::ExprKeyword(ExprKeyword::Count),
                Token::Stat(StatKeyword::Filter),
                Token::ExprKeyword(ExprKeyword::Alias),
                Token::ExprKeyword(ExprKeyword::Col),
                Token::Brackets(vec![]),
                Token::Parens(vec![]),
                Token::Symbol(Symbol::LeftAngle),
                Token::Symbol(Symbol::RightAngle),
                Token::Variable(String::from("hi")),
                Token::Symbol(Symbol::Add),
                Token::Symbol(Symbol::Sub),
                Token::Symbol(Symbol::Mul),
                Token::Symbol(Symbol::Div),
                Token::Symbol(Symbol::Eq),
                Token::Symbol(Symbol::Sub),
                Token::Literal(Literal::Int(String::from("42"))),
                Token::Literal(Literal::Float(String::from("0.1"))),
                Token::Literal(Literal::String(String::from("hi"))),
            ]
        );
    }

    #[test]
    fn test_string() {
        let s = lex(r#""\\""#).unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0], Token::Literal(Literal::String(String::from(r#"\"#))));

        let src = r#" "\"" "#;
        let tokens = lex(src).unwrap();
        assert_eq!(tokens.len(), 1);
        let Token::Literal(Literal::String(s)) = &tokens[0] else {
            panic!();
        };
        assert_eq!(s, r#"""#);

        let src = r#" "\\." "#;
        let tokens = lex(src).unwrap();
        assert_eq!(tokens.len(), 1);
        let Token::Literal(Literal::String(s)) = &tokens[0] else {
            panic!();
        };
        assert_eq!(s, r#"\."#);
    }
}

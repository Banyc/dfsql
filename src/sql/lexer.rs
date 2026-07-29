use winnow::prelude::*;
use winnow::stream::AsChar;

fn backtrack() -> winnow::error::ErrMode<winnow::error::ContextError> {
    winnow::error::ErrMode::Backtrack(winnow::error::ContextError::new())
}

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
            return Err(backtrack());
        }
    }
}

pub(super) fn skip_ws(input: &mut &str) {
    while let Some(c) = input.chars().next() {
        if c.is_whitespace() {
            *input = &input[c.len_utf8()..];
        } else {
            break;
        }
    }
}

pub(super) fn token(input: &mut &str) -> ModalResult<Token> {
    let c = input.chars().next().ok_or_else(backtrack)?;
    match c {
        '(' => {
            *input = &input[1..];
            let inner = lexer.parse_next(input)?;
            skip_ws(input);
            expect_char(input, ')')?;
            Ok(Token::Parens(inner))
        }
        ')' => Err(backtrack()),
        '[' => {
            *input = &input[1..];
            let inner = lexer.parse_next(input)?;
            skip_ws(input);
            expect_char(input, ']')?;
            Ok(Token::Brackets(inner))
        }
        ']' => Err(backtrack()),
        '"' => parse_string(input).map(|s| Token::Literal(Literal::String(s))),
        '-' => {
            *input = &input[1..];
            Ok(Token::Symbol(Symbol::Sub))
        }
        '0'..='9' => number_literal(input).map(Token::Literal),
        '+' | '*' | '/' | '=' | '!' | '<' | '>' | '&' | '|' | ',' | '%' => {
            *input = &input[1..];
            Ok(Token::Symbol(symbol_char(c)))
        }
        'a'..='z' | 'A'..='Z' | '_' => {
            let ident = parse_ident(input);
            Ok(keyword_or_var(ident))
        }
        _ => Err(backtrack()),
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
        _ => Err(backtrack()),
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
    if let Some((_, literal)) = LITERAL_KEYWORDS
        .iter()
        .find(|(keyword, _)| *keyword == lower)
    {
        return Token::Literal(literal.clone());
    }
    if let Some(kw) = expr_keyword(&lower) {
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

pub(crate) const STAT_KEYWORDS: &[(&str, StatKeyword)] = &[
    ("select", StatKeyword::Select),
    ("group", StatKeyword::GroupBy),
    ("agg", StatKeyword::Agg),
    ("filter", StatKeyword::Filter),
    ("limit", StatKeyword::Limit),
    ("reverse", StatKeyword::Reverse),
    ("sort", StatKeyword::Sort),
    ("join", StatKeyword::Join),
    ("on", StatKeyword::On),
    ("left", StatKeyword::Left),
    ("right", StatKeyword::Right),
    ("inner", StatKeyword::Inner),
    ("full", StatKeyword::Full),
    ("use", StatKeyword::Use),
    ("clone", StatKeyword::Clone),
];

pub(crate) const EXPR_KEYWORDS: &[(&str, ExprKeyword)] = &[
    ("sum", ExprKeyword::Sum),
    ("sqrt", ExprKeyword::Sqrt),
    ("count", ExprKeyword::Count),
    ("len", ExprKeyword::Len),
    ("first", ExprKeyword::First),
    ("last", ExprKeyword::Last),
    ("col_sort", ExprKeyword::Sort),
    ("asc", ExprKeyword::Asc),
    ("desc", ExprKeyword::Desc),
    ("col_reverse", ExprKeyword::Reverse),
    ("mean", ExprKeyword::Mean),
    ("median", ExprKeyword::Median),
    ("max", ExprKeyword::Max),
    ("min", ExprKeyword::Min),
    ("var", ExprKeyword::Var),
    ("std", ExprKeyword::Std),
    ("abs", ExprKeyword::Abs),
    ("unique", ExprKeyword::Unique),
    ("by", ExprKeyword::By),
    ("is", ExprKeyword::Is),
    ("alias", ExprKeyword::Alias),
    ("col", ExprKeyword::Col),
    ("exclude", ExprKeyword::Exclude),
    ("cast", ExprKeyword::Cast),
    ("nan", ExprKeyword::Nan),
    ("all", ExprKeyword::All),
    ("any", ExprKeyword::Any),
    ("pow", ExprKeyword::Pow),
    ("log", ExprKeyword::Log),
];

pub(crate) const TYPE_KEYWORDS: &[(&str, Type)] = &[
    ("str", Type::Str),
    ("uint", Type::UInt),
    ("int", Type::Int),
    ("float", Type::Float),
];

pub(crate) const CONDITIONAL_KEYWORDS: &[(&str, Conditional)] = &[
    ("if", Conditional::If),
    ("then", Conditional::Then),
    ("else", Conditional::Else),
];

pub(crate) const STRING_KEYWORDS: &[(&str, StringKeyword)] = &[
    ("contains", StringKeyword::Contains),
    ("extract", StringKeyword::Extract),
    ("all", StringKeyword::All),
    ("split", StringKeyword::Split),
];

pub(crate) const LITERAL_KEYWORDS: &[(&str, Literal)] = &[
    ("true", Literal::Bool(true)),
    ("false", Literal::Bool(false)),
    ("null", Literal::Null),
];

fn stat_keyword(s: &str) -> Option<StatKeyword> {
    STAT_KEYWORDS
        .iter()
        .find_map(|(keyword, value)| (*keyword == s).then_some(*value))
}

fn expr_keyword(s: &str) -> Option<Token> {
    EXPR_KEYWORDS
        .iter()
        .find_map(|(keyword, value)| (*keyword == s).then_some(Token::ExprKeyword(*value)))
}

fn type_keyword(s: &str) -> Option<Type> {
    TYPE_KEYWORDS
        .iter()
        .find_map(|(keyword, value)| (*keyword == s).then_some(*value))
}

fn conditional_keyword(s: &str) -> Option<Conditional> {
    CONDITIONAL_KEYWORDS
        .iter()
        .find_map(|(keyword, value)| (*keyword == s).then_some(*value))
}

fn string_keyword(s: &str) -> Option<StringKeyword> {
    STRING_KEYWORDS
        .iter()
        .find_map(|(keyword, value)| (*keyword == s).then_some(*value))
}

fn number_literal(input: &mut &str) -> ModalResult<Literal> {
    let start = *input;
    let mut seen_dot = false;
    let mut len = 0;
    for c in input.chars() {
        if c.is_ascii_digit() {
            len += c.len_utf8();
        } else if c == '.' && !seen_dot {
            seen_dot = true;
            len += c.len_utf8();
        } else {
            break;
        }
    }
    if len == 0 {
        return Err(backtrack());
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
                return Err(backtrack());
            }
            Some('"') => {
                *input = &input[1..];
                return Ok(result);
            }
            Some('\\') => {
                *input = &input[1..];
                match input.chars().next() {
                    None => {
                        return Err(backtrack());
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
                                if hex.chars().count() < 4 {
                                    return Err(backtrack());
                                }
                                *input = &input[hex.len()..];
                                let code =
                                    u32::from_str_radix(&hex, 16).map_err(|_| backtrack())?;
                                let code = char::from_u32(code).ok_or_else(backtrack)?;
                                result.push(code);
                                continue;
                            }
                            _ => {
                                result.push('\\');
                                result.push(c);
                                *input = &input[c.len_utf8()..];
                                continue;
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

#[derive(Debug, Clone, Copy, PartialEq)]
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

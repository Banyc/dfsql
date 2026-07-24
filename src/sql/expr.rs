use crate::sql::lexer::{
    Conditional, ExprKeyword, Literal, StatKeyword, StringKeyword, Symbol, Token,
};

use crate::sql::SortOrder;

type Tokens<'a> = crate::sql::Tokens<'a>;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Col(String),
    Exclude(ExcludeExpr),
    Literal(Literal),
    Binary(Box<BinaryExpr>),
    Unary(Box<UnaryExpr>),
    Alias(Box<AliasExpr>),
    Conditional(Box<ConditionalExpr>),
    Cast(Box<CastExpr>),
    Log(Box<LogExpr>),
    Str(Box<StrExpr>),
    Standalone(Box<StandaloneExpr>),
    SortBy(Box<SortByExpr>),
    Sort(Box<SortExpr>),
}

pub fn expr(input: &mut Tokens<'_>) -> Result<Expr, String> {
    logic_expr(input)
}

// ---- Precedence parsers ----

fn logic_expr(input: &mut Tokens<'_>) -> Result<Expr, String> {
    let mut left = cmp_expr(input)?;
    loop {
        let op = match input.first() {
            Some(Token::Symbol(Symbol::Ampersand)) => {
                *input = &input[1..];
                BinaryOperator::And
            }
            Some(Token::Symbol(Symbol::Pipe)) => {
                *input = &input[1..];
                BinaryOperator::Or
            }
            _ => break,
        };
        let right = cmp_expr(input)?;
        left = Expr::Binary(Box::new(BinaryExpr {
            operator: op,
            left,
            right,
        }));
    }
    Ok(left)
}

fn cmp_expr(input: &mut Tokens<'_>) -> Result<Expr, String> {
    let mut left = sum_expr(input)?;
    loop {
        let op = match input.first() {
            Some(Token::Symbol(Symbol::Bang)) => {
                *input = &input[1..];
                match input.first() {
                    Some(Token::Symbol(Symbol::Eq)) => {
                        *input = &input[1..];
                        BinaryOperator::NotEq
                    }
                    _ => return Err("expected '=' after '!'".to_string()),
                }
            }
            Some(Token::Symbol(Symbol::LeftAngle)) => {
                *input = &input[1..];
                match input.first() {
                    Some(Token::Symbol(Symbol::Eq)) => {
                        *input = &input[1..];
                        BinaryOperator::LtEq
                    }
                    _ => BinaryOperator::Lt,
                }
            }
            Some(Token::Symbol(Symbol::RightAngle)) => {
                *input = &input[1..];
                match input.first() {
                    Some(Token::Symbol(Symbol::Eq)) => {
                        *input = &input[1..];
                        BinaryOperator::GtEq
                    }
                    _ => BinaryOperator::Gt,
                }
            }
            Some(Token::Symbol(Symbol::Eq)) => {
                *input = &input[1..];
                BinaryOperator::Eq
            }
            _ => break,
        };
        let right = sum_expr(input)?;
        left = Expr::Binary(Box::new(BinaryExpr {
            operator: op,
            left,
            right,
        }));
    }
    Ok(left)
}

fn sum_expr(input: &mut Tokens<'_>) -> Result<Expr, String> {
    let mut left = term_expr(input)?;
    loop {
        let op = match input.first() {
            Some(Token::Symbol(Symbol::Add)) => {
                *input = &input[1..];
                BinaryOperator::Add
            }
            Some(Token::Symbol(Symbol::Sub)) => {
                *input = &input[1..];
                BinaryOperator::Sub
            }
            _ => break,
        };
        let right = term_expr(input)?;
        left = Expr::Binary(Box::new(BinaryExpr {
            operator: op,
            left,
            right,
        }));
    }
    Ok(left)
}

fn term_expr(input: &mut Tokens<'_>) -> Result<Expr, String> {
    let mut left = power_expr(input)?;
    loop {
        let op = match input.first() {
            Some(Token::Symbol(Symbol::Mul)) => {
                *input = &input[1..];
                BinaryOperator::Mul
            }
            Some(Token::Symbol(Symbol::Div)) => {
                *input = &input[1..];
                BinaryOperator::Div
            }
            Some(Token::Symbol(Symbol::Percent)) => {
                *input = &input[1..];
                BinaryOperator::Modulo
            }
            _ => break,
        };
        let right = power_expr(input)?;
        left = Expr::Binary(Box::new(BinaryExpr {
            operator: op,
            left,
            right,
        }));
    }
    Ok(left)
}

fn power_expr(input: &mut Tokens<'_>) -> Result<Expr, String> {
    let mut left = atom(input)?;
    while let Some(Token::ExprKeyword(ExprKeyword::Pow)) = input.first() {
        *input = &input[1..];
        let right = atom(input)?;
        left = Expr::Binary(Box::new(BinaryExpr {
            operator: BinaryOperator::Pow,
            left,
            right,
        }));
    }
    Ok(left)
}

fn atom(input: &mut Tokens<'_>) -> Result<Expr, String> {
    if input.is_empty() {
        return Err("unexpected end of input".to_string());
    }
    match input.first() {
        Some(Token::ExprKeyword(ExprKeyword::Col)) => col_expr(input).map(Expr::Col),
        Some(Token::ExprKeyword(ExprKeyword::Exclude)) => exclude_expr(input).map(Expr::Exclude),
        Some(Token::Literal(_)) => {
            let t = input[0].clone();
            *input = &input[1..];
            Ok(Expr::Literal(match t {
                Token::Literal(lit) => lit,
                _ => unreachable!(),
            }))
        }
        Some(Token::ExprKeyword(ExprKeyword::Alias)) => {
            alias_expr(input).map(Box::new).map(Expr::Alias)
        }
        Some(Token::ExprKeyword(ExprKeyword::Len)) => {
            *input = &input[1..];
            Ok(Expr::Standalone(Box::new(StandaloneExpr {
                operator: StandaloneOperator::Len,
            })))
        }
        Some(Token::ExprKeyword(ExprKeyword::Cast)) => {
            cast_expr(input).map(Box::new).map(Expr::Cast)
        }
        Some(Token::ExprKeyword(ExprKeyword::Log)) => log_expr(input).map(Box::new).map(Expr::Log),
        Some(Token::ExprKeyword(ExprKeyword::Sort)) => {
            sort_expr(input).map(Box::new).map(Expr::Sort)
        }
        Some(Token::ExprKeyword(_))
        | Some(Token::Symbol(Symbol::Sub))
        | Some(Token::Symbol(Symbol::Bang)) => unary_expr(input).map(Box::new).map(Expr::Unary),
        Some(Token::Stat(StatKeyword::Sort)) => sort_by_expr(input).map(Box::new).map(Expr::SortBy),
        Some(Token::Conditional(Conditional::If)) => {
            conditional_expr(input).map(Box::new).map(Expr::Conditional)
        }
        Some(Token::StringKeyword(_)) => str_expr(input).map(Box::new).map(Expr::Str),
        Some(Token::Parens(_)) => parens_expr(input),
        Some(_) => variable_token(input).map(Expr::Col),
        None => Err("unexpected end of input".to_string()),
    }
}

// ---- Helpers ----

fn expect_token(input: &mut &[Token], expected: &Token) -> Result<(), String> {
    let t = input
        .first()
        .ok_or_else(|| format!("expected {expected:?}, got end of input"))?;
    if *t == *expected {
        *input = &input[1..];
        Ok(())
    } else {
        Err(format!("expected {expected:?}, got {t:?}"))
    }
}

fn variable_token(input: &mut Tokens<'_>) -> Result<String, String> {
    let t = input.first().ok_or("expected variable token")?;
    match t {
        Token::Variable(s) => {
            let s = s.clone();
            *input = &input[1..];
            Ok(s)
        }
        _ => Err(format!("expected variable, got {t:?}")),
    }
}

fn string_token(input: &mut Tokens<'_>) -> Result<String, String> {
    let t = input.first().ok_or("expected string literal")?;
    match t {
        Token::Literal(Literal::String(s)) => {
            let s = s.clone();
            *input = &input[1..];
            Ok(s)
        }
        _ => Err(format!("expected string literal, got {t:?}")),
    }
}

fn col_name(input: &mut Tokens<'_>) -> Result<String, String> {
    match input.first() {
        Some(Token::Symbol(Symbol::Mul)) => {
            *input = &input[1..];
            Ok(String::from("*"))
        }
        _ => alt2(string_token, variable_token)(input),
    }
}

pub fn lax_col_name(input: &mut Tokens<'_>) -> Result<String, String> {
    match input.first() {
        Some(Token::ExprKeyword(ExprKeyword::Col)) => col_expr(input),
        _ => variable_token(input),
    }
}

fn alt2<A, B, O>(a: A, b: B) -> impl FnOnce(&mut Tokens<'_>) -> Result<O, String>
where
    A: FnOnce(&mut Tokens<'_>) -> Result<O, String>,
    B: FnOnce(&mut Tokens<'_>) -> Result<O, String>,
{
    move |input| {
        let saved = *input;
        a(input).or_else(|_| {
            *input = saved;
            b(input)
        })
    }
}

// ---- Col ----

fn col_expr(input: &mut Tokens<'_>) -> Result<String, String> {
    expect_token(input, &Token::ExprKeyword(ExprKeyword::Col))?;
    col_name(input)
}

// ---- Exclude ----

#[derive(Debug, Clone, PartialEq)]
pub struct ExcludeExpr {
    pub columns: Vec<String>,
}

fn exclude_expr(input: &mut Tokens<'_>) -> Result<ExcludeExpr, String> {
    expect_token(input, &Token::ExprKeyword(ExprKeyword::Exclude))?;
    let mut columns = Vec::new();
    while !input.is_empty() && can_start_column_name(input.first()) {
        columns.push(lax_col_name(input)?);
    }
    if columns.is_empty() {
        return Err("expected at least one column name after exclude".to_string());
    }
    Ok(ExcludeExpr { columns })
}

fn can_start_column_name(t: Option<&Token>) -> bool {
    matches!(
        t,
        Some(Token::ExprKeyword(ExprKeyword::Col))
            | Some(Token::Variable(_))
            | Some(Token::Literal(Literal::String(_)))
    )
}

// ---- Parens ----

fn parens_expr(input: &mut Tokens<'_>) -> Result<Expr, String> {
    match input.first() {
        Some(Token::Parens(inner)) => {
            let mut inner_slice = inner.as_slice();
            let parsed = expr(&mut inner_slice)?;
            if let Some(token) = inner_slice.first() {
                return Err(format!(
                    "unexpected token {:?} in parenthesized expression",
                    token
                ));
            }
            *input = &input[1..];
            Ok(parsed)
        }
        _ => Err("expected parenthesized expression".to_string()),
    }
}

// ---- Binary ----

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryExpr {
    pub operator: BinaryOperator,
    pub left: Expr,
    pub right: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOperator {
    Add,
    Sub,
    Mul,
    Div,
    Modulo,
    Eq,
    NotEq,
    LtEq,
    Lt,
    GtEq,
    Gt,
    And,
    Or,
    Pow,
}

// ---- SortBy ----

#[derive(Debug, Clone, PartialEq)]
pub struct SortByExpr {
    pub pairs: Vec<(SortOrder, Expr)>,
    pub expr: Expr,
}

fn sort_by_expr(input: &mut Tokens<'_>) -> Result<SortByExpr, String> {
    let saved = *input;
    expect_token(input, &Token::Stat(StatKeyword::Sort))?;
    let value = expr(input)?;
    expect_token(input, &Token::ExprKeyword(ExprKeyword::By))?;
    let mut pairs = Vec::new();
    while !input.is_empty()
        && (can_start_expression(input.first())
            || matches!(
                input.first(),
                Some(Token::ExprKeyword(ExprKeyword::Asc | ExprKeyword::Desc))
            ))
    {
        let order = sort_order(input)?;
        let e = expr(input)?;
        pairs.push((order, e));
    }
    if pairs.is_empty() {
        *input = saved;
        return Err("sort_by requires at least one key".to_string());
    }
    Ok(SortByExpr { pairs, expr: value })
}

// ---- Sort ----

#[derive(Debug, Clone, PartialEq)]
pub struct SortExpr {
    pub expr: Expr,
    pub order: SortOrder,
}

fn sort_expr(input: &mut Tokens<'_>) -> Result<SortExpr, String> {
    expect_token(input, &Token::ExprKeyword(ExprKeyword::Sort))?;
    let order = sort_order(input)?;
    let e = expr(input)?;
    Ok(SortExpr { expr: e, order })
}

fn sort_order(input: &mut Tokens<'_>) -> Result<SortOrder, String> {
    Ok(match input.first() {
        Some(Token::ExprKeyword(ExprKeyword::Asc)) => {
            *input = &input[1..];
            SortOrder::Asc
        }
        Some(Token::ExprKeyword(ExprKeyword::Desc)) => {
            *input = &input[1..];
            SortOrder::Desc
        }
        _ => SortOrder::Asc,
    })
}

// ---- Unary ----

#[derive(Debug, Clone, PartialEq)]
pub struct UnaryExpr {
    pub operator: UnaryOperator,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Sum,
    Sqrt,
    Count,
    First,
    Last,
    Reverse,
    Mean,
    Median,
    Max,
    Min,
    Var,
    Std,
    Abs,
    Unique,
    Not,
    Neg,
    IsNull,
    IsNan,
    All,
    Any,
}

fn unary_expr(input: &mut Tokens<'_>) -> Result<UnaryExpr, String> {
    let op = match input.first() {
        Some(Token::ExprKeyword(ExprKeyword::Sum)) => {
            *input = &input[1..];
            UnaryOperator::Sum
        }
        Some(Token::ExprKeyword(ExprKeyword::Sqrt)) => {
            *input = &input[1..];
            UnaryOperator::Sqrt
        }
        Some(Token::ExprKeyword(ExprKeyword::Count)) => {
            *input = &input[1..];
            UnaryOperator::Count
        }
        Some(Token::ExprKeyword(ExprKeyword::First)) => {
            *input = &input[1..];
            UnaryOperator::First
        }
        Some(Token::ExprKeyword(ExprKeyword::Last)) => {
            *input = &input[1..];
            UnaryOperator::Last
        }
        Some(Token::ExprKeyword(ExprKeyword::Reverse)) => {
            *input = &input[1..];
            UnaryOperator::Reverse
        }
        Some(Token::ExprKeyword(ExprKeyword::Mean)) => {
            *input = &input[1..];
            UnaryOperator::Mean
        }
        Some(Token::ExprKeyword(ExprKeyword::Median)) => {
            *input = &input[1..];
            UnaryOperator::Median
        }
        Some(Token::ExprKeyword(ExprKeyword::Max)) => {
            *input = &input[1..];
            UnaryOperator::Max
        }
        Some(Token::ExprKeyword(ExprKeyword::Min)) => {
            *input = &input[1..];
            UnaryOperator::Min
        }
        Some(Token::ExprKeyword(ExprKeyword::Var)) => {
            *input = &input[1..];
            UnaryOperator::Var
        }
        Some(Token::ExprKeyword(ExprKeyword::Std)) => {
            *input = &input[1..];
            UnaryOperator::Std
        }
        Some(Token::ExprKeyword(ExprKeyword::Abs)) => {
            *input = &input[1..];
            UnaryOperator::Abs
        }
        Some(Token::ExprKeyword(ExprKeyword::Unique)) => {
            *input = &input[1..];
            UnaryOperator::Unique
        }
        Some(Token::ExprKeyword(ExprKeyword::All)) => {
            *input = &input[1..];
            UnaryOperator::All
        }
        Some(Token::ExprKeyword(ExprKeyword::Any)) => {
            *input = &input[1..];
            UnaryOperator::Any
        }
        Some(Token::Symbol(Symbol::Sub)) => {
            *input = &input[1..];
            UnaryOperator::Neg
        }
        Some(Token::Symbol(Symbol::Bang)) => {
            *input = &input[1..];
            UnaryOperator::Not
        }
        Some(Token::ExprKeyword(ExprKeyword::Is)) => {
            *input = &input[1..];
            match input.first() {
                Some(Token::Literal(Literal::Null)) => {
                    *input = &input[1..];
                    UnaryOperator::IsNull
                }
                Some(Token::ExprKeyword(ExprKeyword::Nan)) => {
                    *input = &input[1..];
                    UnaryOperator::IsNan
                }
                _ => return Err("expected 'null' or 'nan' after 'is'".to_string()),
            }
        }
        _ => return Err("expected unary operator".to_string()),
    };
    let e = expr(input)?;
    Ok(UnaryExpr {
        operator: op,
        expr: e,
    })
}

// ---- Standalone ----

#[derive(Debug, Clone, PartialEq)]
pub struct StandaloneExpr {
    pub operator: StandaloneOperator,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StandaloneOperator {
    Len,
}

// ---- Alias ----

#[derive(Debug, Clone, PartialEq)]
pub struct AliasExpr {
    pub name: String,
    pub expr: Expr,
}

fn alias_expr(input: &mut Tokens<'_>) -> Result<AliasExpr, String> {
    expect_token(input, &Token::ExprKeyword(ExprKeyword::Alias))?;
    let name = alt2(string_token, lax_col_name)(input)?;
    let e = expr(input)?;
    Ok(AliasExpr { name, expr: e })
}

// ---- Conditional ----

#[derive(Debug, Clone, PartialEq)]
pub struct ConditionalExpr {
    pub first_case: ConditionalCase,
    pub other_cases: Vec<ConditionalCase>,
    pub otherwise: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConditionalCase {
    pub when: Expr,
    pub then: Expr,
}

fn conditional_expr(input: &mut Tokens<'_>) -> Result<ConditionalExpr, String> {
    let first_case = conditional_case(input)?;
    let mut other_cases = Vec::new();
    loop {
        match input.first() {
            Some(Token::Conditional(Conditional::If)) => {
                other_cases.push(conditional_case(input)?);
            }
            Some(Token::Conditional(Conditional::Else)) => {
                *input = &input[1..];
                let otherwise = expr(input)?;
                return Ok(ConditionalExpr {
                    first_case,
                    other_cases,
                    otherwise,
                });
            }
            _ => {
                return Err("expected 'if' or 'else' in conditional".to_string());
            }
        }
    }
}

fn conditional_case(input: &mut Tokens<'_>) -> Result<ConditionalCase, String> {
    expect_token(input, &Token::Conditional(Conditional::If))?;
    let when = expr(input)?;
    expect_token(input, &Token::Conditional(Conditional::Then))?;
    let then = expr(input)?;
    Ok(ConditionalCase { when, then })
}

// ---- Cast ----

#[derive(Debug, Clone, PartialEq)]
pub struct CastExpr {
    pub expr: Expr,
    pub ty: crate::sql::lexer::Type,
}

fn cast_expr(input: &mut Tokens<'_>) -> Result<CastExpr, String> {
    expect_token(input, &Token::ExprKeyword(ExprKeyword::Cast))?;
    let t = input.first().ok_or("expected type keyword after cast")?;
    let ty = match t {
        Token::Type(ty) => *ty,
        _ => return Err(format!("expected type, got {t:?}")),
    };
    *input = &input[1..];
    let e = expr(input)?;
    Ok(CastExpr { expr: e, ty })
}

// ---- Log ----

#[derive(Debug, Clone, PartialEq)]
pub struct LogExpr {
    pub expr: Expr,
    pub base: f64,
}

fn log_expr(input: &mut Tokens<'_>) -> Result<LogExpr, String> {
    expect_token(input, &Token::ExprKeyword(ExprKeyword::Log))?;
    let t = input.first().ok_or("expected numeric base after log")?;
    let base = match t {
        Token::Literal(Literal::Float(s) | Literal::Int(s)) => s
            .parse::<f64>()
            .map_err(|_| format!("invalid numeric literal: {s}"))?,
        _ => return Err(format!("expected numeric base, got {t:?}")),
    };
    *input = &input[1..];
    let e = expr(input)?;
    Ok(LogExpr { expr: e, base })
}

// ---- Str ----

#[derive(Debug, Clone, PartialEq)]
pub enum StrExpr {
    Contains(Contains),
    Extract(Extract),
    ExtractAll(ExtractAll),
    Split(Split),
}

fn str_expr(input: &mut Tokens<'_>) -> Result<StrExpr, String> {
    match input.first() {
        Some(Token::StringKeyword(StringKeyword::Contains)) => {
            contains(input).map(StrExpr::Contains)
        }
        Some(Token::StringKeyword(StringKeyword::Extract)) => {
            // Could be Extract or ExtractAll
            let saved = *input;
            *input = &input[1..];
            match input.first() {
                Some(Token::StringKeyword(StringKeyword::All)) => {
                    *input = &input[1..];
                    let pattern = expr(input)?;
                    let s = expr(input)?;
                    Ok(StrExpr::ExtractAll(ExtractAll { str: s, pattern }))
                }
                _ => {
                    *input = saved;
                    extract(input).map(StrExpr::Extract)
                }
            }
        }
        Some(Token::StringKeyword(StringKeyword::Split)) => split(input).map(StrExpr::Split),
        _ => Err("expected string function".to_string()),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Contains {
    pub str: Expr,
    pub pattern: Expr,
}

fn contains(input: &mut Tokens<'_>) -> Result<Contains, String> {
    expect_token(input, &Token::StringKeyword(StringKeyword::Contains))?;
    let pattern = expr(input)?;
    let s = expr(input)?;
    Ok(Contains { str: s, pattern })
}

#[derive(Debug, Clone, PartialEq)]
pub struct Extract {
    pub str: Expr,
    pub pattern: Expr,
    pub group: usize,
}

fn extract(input: &mut Tokens<'_>) -> Result<Extract, String> {
    expect_token(input, &Token::StringKeyword(StringKeyword::Extract))?;
    let pattern = expr(input)?;
    let t = input.first().ok_or("expected group number")?;
    let group = match t {
        Token::Literal(Literal::Int(s)) => s
            .parse::<usize>()
            .map_err(|_| format!("invalid group number: {s}"))?,
        _ => return Err(format!("expected group number, got {t:?}")),
    };
    *input = &input[1..];
    let s = expr(input)?;
    Ok(Extract {
        str: s,
        pattern,
        group,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExtractAll {
    pub str: Expr,
    pub pattern: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Split {
    pub str: Expr,
    pub pattern: Expr,
}

fn split(input: &mut Tokens<'_>) -> Result<Split, String> {
    expect_token(input, &Token::StringKeyword(StringKeyword::Split))?;
    let pattern = expr(input)?;
    let s = expr(input)?;
    Ok(Split { str: s, pattern })
}

// ---- can_start_expression ----

pub(crate) fn can_start_expression(token: Option<&Token>) -> bool {
    match token {
        None => false,
        Some(
            Token::Literal(_)
            | Token::Variable(_)
            | Token::ExprKeyword(_)
            | Token::Conditional(_)
            | Token::StringKeyword(_)
            | Token::Parens(_)
            | Token::Brackets(_)
            | Token::Symbol(Symbol::Sub)
            | Token::Symbol(Symbol::Bang)
            | Token::Symbol(Symbol::Mul),
        ) => true,
        Some(Token::Stat(StatKeyword::Sort)) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::lexer::lex;

    use super::*;

    fn parse_expr(src: &str) -> Expr {
        let tokens = lex(src).unwrap();
        expr(&mut tokens.as_slice()).unwrap()
    }

    #[test]
    fn test_cmp_expr() {
        let e = parse_expr(r#"42 >= -sum (col "foo")"#);
        assert_eq!(
            e,
            Expr::Binary(Box::new(BinaryExpr {
                operator: BinaryOperator::GtEq,
                left: Expr::Literal(Literal::Int(String::from("42"))),
                right: Expr::Unary(Box::new(UnaryExpr {
                    operator: UnaryOperator::Neg,
                    expr: Expr::Unary(Box::new(UnaryExpr {
                        operator: UnaryOperator::Sum,
                        expr: Expr::Col(String::from("foo")),
                    })),
                })),
            }))
        );
    }

    #[test]
    fn test_sum_expr() {
        let e = parse_expr(r#"(42 + -sum (col "foo"))"#);
        assert_eq!(
            e,
            Expr::Binary(Box::new(BinaryExpr {
                operator: BinaryOperator::Add,
                left: Expr::Literal(Literal::Int(String::from("42"))),
                right: Expr::Unary(Box::new(UnaryExpr {
                    operator: UnaryOperator::Neg,
                    expr: Expr::Unary(Box::new(UnaryExpr {
                        operator: UnaryOperator::Sum,
                        expr: Expr::Col(String::from("foo")),
                    })),
                })),
            }))
        );
    }

    #[test]
    fn test_term_expr() {
        let e = parse_expr(r#"(42 + 1 * -sum (col "foo"))"#);
        assert_eq!(
            e,
            Expr::Binary(Box::new(BinaryExpr {
                operator: BinaryOperator::Add,
                left: Expr::Literal(Literal::Int(String::from("42"))),
                right: Expr::Binary(Box::new(BinaryExpr {
                    operator: BinaryOperator::Mul,
                    left: Expr::Literal(Literal::Int(String::from("1"))),
                    right: Expr::Unary(Box::new(UnaryExpr {
                        operator: UnaryOperator::Neg,
                        expr: Expr::Unary(Box::new(UnaryExpr {
                            operator: UnaryOperator::Sum,
                            expr: Expr::Col(String::from("foo")),
                        })),
                    })),
                })),
            }))
        );
    }

    #[test]
    fn test_unary_expr() {
        let e = parse_expr(r#"(-sum (col "foo"))"#);
        assert_eq!(
            e,
            Expr::Unary(Box::new(UnaryExpr {
                operator: UnaryOperator::Neg,
                expr: Expr::Unary(Box::new(UnaryExpr {
                    operator: UnaryOperator::Sum,
                    expr: Expr::Col(String::from("foo")),
                })),
            }))
        );
    }

    #[test]
    fn test_agg_expr() {
        let e = parse_expr(r#"(sum (col "foo"))"#);
        assert_eq!(
            e,
            Expr::Unary(Box::new(UnaryExpr {
                operator: UnaryOperator::Sum,
                expr: Expr::Col(String::from("foo")),
            })),
        );
    }

    #[test]
    fn test_alias_expr() {
        let e = parse_expr(r#"(alias "foo" 42)"#);
        assert_eq!(
            e,
            Expr::Alias(Box::new(AliasExpr {
                name: String::from("foo"),
                expr: Expr::Literal(Literal::Int(String::from("42"))),
            }))
        );
    }

    #[test]
    fn test_nested_conditional_expr() {
        let src = r#"(if (if 1.1 then 1.2 if 1.3 then 1.4 else 1.5) then (if 2.1 then 2.2 if 2.3 then 2.4 else 2.5) if (if 3.1 then 3.2 if 3.3 then 3.4 else 3.5) then (if 4.1 then 4.2 if 4.3 then 4.4 else 4.5) else (if 5.1 then 5.2 if 5.3 then 5.4 else 5.5))"#;
        let e = parse_expr(src);
        assert_eq!(
            e,
            Expr::Conditional(Box::new(ConditionalExpr {
                first_case: ConditionalCase {
                    when: Expr::Conditional(Box::new(ConditionalExpr {
                        first_case: ConditionalCase {
                            when: Expr::Literal(Literal::Float(String::from("1.1"))),
                            then: Expr::Literal(Literal::Float(String::from("1.2")))
                        },
                        other_cases: vec![ConditionalCase {
                            when: Expr::Literal(Literal::Float(String::from("1.3"))),
                            then: Expr::Literal(Literal::Float(String::from("1.4")))
                        }],
                        otherwise: Expr::Literal(Literal::Float(String::from("1.5"))),
                    })),
                    then: Expr::Conditional(Box::new(ConditionalExpr {
                        first_case: ConditionalCase {
                            when: Expr::Literal(Literal::Float(String::from("2.1"))),
                            then: Expr::Literal(Literal::Float(String::from("2.2")))
                        },
                        other_cases: vec![ConditionalCase {
                            when: Expr::Literal(Literal::Float(String::from("2.3"))),
                            then: Expr::Literal(Literal::Float(String::from("2.4")))
                        }],
                        otherwise: Expr::Literal(Literal::Float(String::from("2.5"))),
                    })),
                },
                other_cases: vec![ConditionalCase {
                    when: Expr::Conditional(Box::new(ConditionalExpr {
                        first_case: ConditionalCase {
                            when: Expr::Literal(Literal::Float(String::from("3.1"))),
                            then: Expr::Literal(Literal::Float(String::from("3.2")))
                        },
                        other_cases: vec![ConditionalCase {
                            when: Expr::Literal(Literal::Float(String::from("3.3"))),
                            then: Expr::Literal(Literal::Float(String::from("3.4")))
                        }],
                        otherwise: Expr::Literal(Literal::Float(String::from("3.5"))),
                    })),
                    then: Expr::Conditional(Box::new(ConditionalExpr {
                        first_case: ConditionalCase {
                            when: Expr::Literal(Literal::Float(String::from("4.1"))),
                            then: Expr::Literal(Literal::Float(String::from("4.2")))
                        },
                        other_cases: vec![ConditionalCase {
                            when: Expr::Literal(Literal::Float(String::from("4.3"))),
                            then: Expr::Literal(Literal::Float(String::from("4.4")))
                        }],
                        otherwise: Expr::Literal(Literal::Float(String::from("4.5"))),
                    })),
                }],
                otherwise: Expr::Conditional(Box::new(ConditionalExpr {
                    first_case: ConditionalCase {
                        when: Expr::Literal(Literal::Float(String::from("5.1"))),
                        then: Expr::Literal(Literal::Float(String::from("5.2")))
                    },
                    other_cases: vec![ConditionalCase {
                        when: Expr::Literal(Literal::Float(String::from("5.3"))),
                        then: Expr::Literal(Literal::Float(String::from("5.4")))
                    }],
                    otherwise: Expr::Literal(Literal::Float(String::from("5.5"))),
                })),
            }))
        );
    }
}

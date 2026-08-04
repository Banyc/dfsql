use crate::sql::expr::{Expr, can_start_expression, expr, lax_col_name};

use crate::sql::TokenParseError;
use crate::sql::lexer::{ExprKeyword, Literal, StatKeyword, Token};

use crate::sql::{Program, SortOrder, Tokens};

#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Select(SelectStat),
    GroupAgg(GroupAggStat),
    Filter(FilterStat),
    Limit(LimitStat),
    Reverse,
    Sort(SortStat),
    Join(JoinStat),
    UseFrame(UseStat),
    CloneFrame(CloneStat),
}

pub(crate) fn parse_detailed(tokens: &[Token]) -> Result<Program, TokenParseError> {
    let mut input = tokens;
    let mut statements = Vec::new();
    while !input.is_empty() {
        let token_idx = tokens.len() - input.len();
        match stmt(&mut input) {
            Ok(s) => statements.push(s),
            Err(mut error) => {
                error.offset += token_idx;
                return Err(error);
            }
        }
    }
    Ok(Program { statements })
}

fn stmt(input: &mut Tokens<'_>) -> Result<Stmt, TokenParseError> {
    let start = *input;
    let mut best = TokenParseError::new(0, format!("unexpected token {:?}", input.first()));
    macro_rules! attempt {
        ($parser:ident, $map:expr) => {{
            *input = start;
            match $parser(input) {
                Ok(value) => return Ok(($map)(value)),
                Err(detail) => {
                    let offset = start.len() - input.len();
                    if offset > best.offset {
                        best = TokenParseError::new(offset, detail);
                    }
                }
            }
        }};
    }
    attempt!(clone_stat, Stmt::CloneFrame);
    attempt!(select_stat, Stmt::Select);
    attempt!(group_agg_stat, Stmt::GroupAgg);
    attempt!(filter_stat, Stmt::Filter);
    attempt!(limit_stat, Stmt::Limit);
    attempt!(reverse_stat, |stmt| stmt);
    attempt!(sort_stat, Stmt::Sort);
    attempt!(join_stat, Stmt::Join);
    attempt!(use_stat, Stmt::UseFrame);
    *input = start;
    Err(best)
}

fn expect_token(input: &mut Tokens<'_>, expected: &Token) -> Result<(), String> {
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

// ---- Select ----

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStat {
    pub columns: Vec<Expr>,
}

fn select_stat(input: &mut Tokens<'_>) -> Result<SelectStat, String> {
    expect_token(input, &Token::Stat(StatKeyword::Select))?;
    let mut columns = Vec::new();
    while can_start_statement_expression(input) {
        columns.push(expr(input)?);
    }
    Ok(SelectStat { columns })
}

// ---- GroupAgg ----

#[derive(Debug, Clone, PartialEq)]
pub struct GroupAggStat {
    pub group_by: Vec<String>,
    pub agg: Vec<Expr>,
}

fn group_agg_stat(input: &mut Tokens<'_>) -> Result<GroupAggStat, String> {
    expect_token(input, &Token::Stat(StatKeyword::GroupBy))?;
    let group_by = column_names(input)?;
    expect_token(input, &Token::Stat(StatKeyword::Agg))?;
    let mut agg = Vec::new();
    while can_start_statement_expression(input) {
        agg.push(expr(input)?);
    }
    Ok(GroupAggStat { group_by, agg })
}

fn column_names(input: &mut Tokens<'_>) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    loop {
        match input.first() {
            Some(Token::ExprKeyword(ExprKeyword::Col)) => {
                names.push(lax_col_name(input)?);
            }
            Some(Token::Variable(name)) | Some(Token::Literal(Literal::String(name))) => {
                names.push(name.clone());
                *input = &input[1..];
            }
            _ => return Ok(names),
        }
    }
}

// ---- Filter ----

#[derive(Debug, Clone, PartialEq)]
pub struct FilterStat {
    pub condition: Expr,
}

fn filter_stat(input: &mut Tokens<'_>) -> Result<FilterStat, String> {
    expect_token(input, &Token::Stat(StatKeyword::Filter))?;
    let condition = expr(input)?;
    Ok(FilterStat { condition })
}

// ---- Limit ----

#[derive(Debug, Clone, PartialEq)]
pub struct LimitStat {
    pub rows: String,
}

fn limit_stat(input: &mut Tokens<'_>) -> Result<LimitStat, String> {
    expect_token(input, &Token::Stat(StatKeyword::Limit))?;
    let t = input
        .first()
        .ok_or("expected integer literal after limit")?;
    match t {
        Token::Literal(Literal::Int(rows)) => {
            let rows = rows.clone();
            *input = &input[1..];
            Ok(LimitStat { rows })
        }
        _ => Err(format!("expected integer, got {t:?}")),
    }
}

// ---- Reverse ----

fn reverse_stat(input: &mut Tokens<'_>) -> Result<Stmt, String> {
    expect_token(input, &Token::Stat(StatKeyword::Reverse))?;
    Ok(Stmt::Reverse)
}

// ---- Sort ----

#[derive(Debug, Clone, PartialEq)]
pub struct SortStat {
    pub pairs: Vec<(SortOrder, String)>,
}

fn sort_stat(input: &mut Tokens<'_>) -> Result<SortStat, String> {
    expect_token(input, &Token::Stat(StatKeyword::Sort))?;
    let mut pairs = Vec::new();
    while can_start_sort_pair(input.first()) {
        let order = sort_order(input)?;
        let name = column_name(input)?;
        pairs.push((order, name));
    }
    Ok(SortStat { pairs })
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

fn column_name(input: &mut Tokens<'_>) -> Result<String, String> {
    match input.first() {
        Some(Token::Variable(s)) => {
            let s = s.clone();
            *input = &input[1..];
            Ok(s)
        }
        Some(Token::Literal(Literal::String(s))) => {
            let s = s.clone();
            *input = &input[1..];
            Ok(s)
        }
        Some(Token::ExprKeyword(ExprKeyword::Col)) => lax_col_name(input),
        _ => Err(format!("expected column name, got {:?}", input.first())),
    }
}

fn can_start_column_name(t: Option<&Token>) -> bool {
    matches!(
        t,
        Some(Token::Variable(_))
            | Some(Token::Literal(Literal::String(_)))
            | Some(Token::ExprKeyword(ExprKeyword::Col))
    )
}

fn can_start_sort_pair(t: Option<&Token>) -> bool {
    can_start_column_name(t)
        || matches!(
            t,
            Some(Token::ExprKeyword(ExprKeyword::Asc | ExprKeyword::Desc))
        )
}

// ---- Join ----

#[derive(Debug, Clone, PartialEq)]
pub enum JoinStat {
    SingleCol(SingleColJoinStat),
}

fn join_stat(input: &mut Tokens<'_>) -> Result<JoinStat, String> {
    single_col_join_stat(input).map(JoinStat::SingleCol)
}

#[derive(Debug, Clone, PartialEq)]
pub struct SingleColJoinStat {
    pub other: String,
    pub ty: SingleColJoinType,
    pub left_on: Expr,
    pub right_on: Option<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SingleColJoinType {
    Left,
    Right,
    Inner,
    Full,
}

fn single_col_join_stat(input: &mut Tokens<'_>) -> Result<SingleColJoinStat, String> {
    let ty = match input.first() {
        Some(Token::Stat(StatKeyword::Left)) => {
            *input = &input[1..];
            SingleColJoinType::Left
        }
        Some(Token::Stat(StatKeyword::Right)) => {
            *input = &input[1..];
            SingleColJoinType::Right
        }
        Some(Token::Stat(StatKeyword::Inner)) => {
            *input = &input[1..];
            SingleColJoinType::Inner
        }
        Some(Token::Stat(StatKeyword::Full)) => {
            *input = &input[1..];
            SingleColJoinType::Full
        }
        _ => return Err("expected join type (left/right/inner/full)".to_string()),
    };
    expect_token(input, &Token::Stat(StatKeyword::Join))?;
    let t = input.first().ok_or("expected table name after join type")?;
    let other = match t {
        Token::Variable(s) => s.clone(),
        _ => return Err(format!("expected table name, got {t:?}")),
    };
    *input = &input[1..];
    expect_token(input, &Token::Stat(StatKeyword::On))?;
    let left_on = expr(input)?;
    let right_on = if can_start_statement_expression(input) {
        Some(expr(input)?)
    } else {
        None
    };
    Ok(SingleColJoinStat {
        other,
        ty,
        left_on,
        right_on,
    })
}

// ---- Use ----

#[derive(Debug, Clone, PartialEq)]
pub struct UseStat {
    pub df_name: String,
}

fn use_stat(input: &mut Tokens<'_>) -> Result<UseStat, String> {
    expect_token(input, &Token::Stat(StatKeyword::Use))?;
    let t = input.first().ok_or("expected dataframe name after use")?;
    match t {
        Token::Variable(name) => {
            let name = name.clone();
            *input = &input[1..];
            Ok(UseStat { df_name: name })
        }
        _ => Err(format!("expected variable, got {t:?}")),
    }
}

// ---- Clone ----

#[derive(Debug, Clone, PartialEq)]
pub struct CloneStat {
    pub df_name: String,
}

fn clone_stat(input: &mut Tokens<'_>) -> Result<CloneStat, String> {
    expect_token(input, &Token::Stat(StatKeyword::Clone))?;
    let t = input.first().ok_or("expected dataframe name after clone")?;
    match t {
        Token::Variable(name) => {
            let name = name.clone();
            *input = &input[1..];
            Ok(CloneStat { df_name: name })
        }
        _ => Err(format!("expected variable, got {t:?}")),
    }
}

// ---- Sort boundary probe ----

pub(crate) fn can_start_statement_expression(input: &Tokens<'_>) -> bool {
    match input.first() {
        Some(Token::Stat(StatKeyword::Sort)) => {
            let mut probe = *input;
            expr(&mut probe).is_ok()
        }
        Some(Token::Stat(_)) => false,
        token => can_start_expression(token),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_expr() {
        let src = r#"select col a exclude b col c"#;
        let s = crate::sql::parse(src).unwrap();
        assert_eq!(
            s.statements,
            vec![Stmt::Select(SelectStat {
                columns: vec![
                    Expr::Col(String::from("a")),
                    Expr::Exclude(crate::sql::expr::ExcludeExpr {
                        columns: vec![String::from("b"), String::from("c")],
                    }),
                ]
            })]
        );
    }

    #[test]
    fn test_group_agg_stat() {
        let src = r#"group col "foo" "bar" agg sum col "foo" count col "bar""#;
        let s = crate::sql::parse(src).unwrap();
        assert_eq!(
            s.statements,
            vec![Stmt::GroupAgg(GroupAggStat {
                group_by: vec![String::from("foo"), String::from("bar")],
                agg: vec![
                    Expr::Unary(Box::new(crate::sql::expr::UnaryExpr {
                        operator: crate::sql::expr::UnaryOperator::Sum,
                        expr: Expr::Col(String::from("foo")),
                    })),
                    Expr::Unary(Box::new(crate::sql::expr::UnaryExpr {
                        operator: crate::sql::expr::UnaryOperator::Count,
                        expr: Expr::Col(String::from("bar")),
                    })),
                ],
            })]
        );
    }

    #[test]
    fn test_filter_stat() {
        let src = r#"filter col "foo" = 42"#;
        let s = crate::sql::parse(src).unwrap();
        assert_eq!(
            s.statements,
            vec![Stmt::Filter(FilterStat {
                condition: Expr::Binary(Box::new(crate::sql::expr::BinaryExpr {
                    operator: crate::sql::expr::BinaryOperator::Eq,
                    left: Expr::Col(String::from("foo")),
                    right: Expr::Literal(Literal::Int(String::from("42"))),
                })),
            })]
        );
    }

    #[test]
    fn distinguishes_sort_expression_from_statement() {
        let parsed = crate::sql::parse("select sort value by order").unwrap();
        assert!(
            matches!(parsed.statements.as_slice(), [Stmt::Select(SelectStat { columns })] if matches!(columns.as_slice(), [Expr::SortBy(_)]))
        );
        let parsed = crate::sql::parse("select id sort id").unwrap();
        assert!(matches!(
            parsed.statements.as_slice(),
            [Stmt::Select(_), Stmt::Sort(_)]
        ));
    }
}

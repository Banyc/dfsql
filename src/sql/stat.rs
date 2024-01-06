use chumsky::prelude::*;

use super::{
    expr::{expr, lax_col_name, Expr},
    lexer::{Literal, StatKeyword, Token},
    sort_order, string_token, variable_token, SortOrder, S,
};

#[derive(Debug, Clone, PartialEq)]
pub enum Stat {
    Select(SelectStat),
    GroupAgg(GroupAggStat),
    Filter(FilterStat),
    Limit(LimitStat),
    Reverse,
    Sort(SortStat),
    Join(JoinStat),
}

pub fn parser<'a>() -> impl Parser<'a, &'a [Token], S, extra::Err<Rich<'a, Token>>> + Clone {
    let select = select_stat().map(Stat::Select);
    let group_agg = group_agg_stat().map(Stat::GroupAgg);
    let filter = filter_stat().map(Stat::Filter);
    let limit = limit_stat().map(Stat::Limit);
    let reverse = just(Token::Stat(StatKeyword::Reverse)).to(Stat::Reverse);
    let sort = sort_stat().map(Stat::Sort);
    let join = join_stat().map(Stat::Join);
    let stat = choice((select, group_agg, filter, limit, reverse, sort, join));

    stat.repeated()
        .collect()
        .map(|statements| S { statements })
        .boxed()
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStat {
    pub columns: Vec<Expr>,
}

fn select_stat<'a>() -> impl Parser<'a, &'a [Token], SelectStat, extra::Err<Rich<'a, Token>>> + Clone
{
    just(Token::Stat(StatKeyword::Select))
        .ignore_then(expr().repeated().collect())
        .map(|columns| SelectStat { columns })
        .boxed()
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupAggStat {
    pub group_by: Vec<String>,
    pub agg: Vec<Expr>,
}

fn group_agg_stat<'a>(
) -> impl Parser<'a, &'a [Token], GroupAggStat, extra::Err<Rich<'a, Token>>> + Clone {
    // let columns =
    //     column_names().nested_in(select_ref! { Token::Stat(StatKeyword::Brackets)(columns) => columns.as_slice() });
    let columns = column_names();

    just(Token::Stat(StatKeyword::GroupBy))
        .ignore_then(columns)
        .then_ignore(just(Token::Stat(StatKeyword::Agg)))
        .then(expr().repeated().collect())
        .map(|(group_by, agg)| GroupAggStat { group_by, agg })
        .boxed()
}

fn column_names<'a>(
) -> impl Parser<'a, &'a [Token], Vec<String>, extra::Err<Rich<'a, Token>>> + Clone {
    let column = choice((lax_col_name(), string_token()));
    column.repeated().collect().boxed()
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterStat {
    pub condition: Expr,
}

fn filter_stat<'a>() -> impl Parser<'a, &'a [Token], FilterStat, extra::Err<Rich<'a, Token>>> + Clone
{
    just(Token::Stat(StatKeyword::Filter))
        .ignore_then(expr())
        .map(|condition| FilterStat { condition })
        .boxed()
}

#[derive(Debug, Clone, PartialEq)]
pub struct LimitStat {
    pub rows: String,
}

fn limit_stat<'a>() -> impl Parser<'a, &'a [Token], LimitStat, extra::Err<Rich<'a, Token>>> + Clone
{
    just(Token::Stat(StatKeyword::Limit))
        .ignore_then(select_ref! { Token::Literal(Literal::Int(rows)) => rows.clone() })
        .map(|rows| LimitStat { rows })
        .boxed()
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortStat {
    pub column: String,
    pub order: SortOrder,
}

fn sort_stat<'a>() -> impl Parser<'a, &'a [Token], SortStat, extra::Err<Rich<'a, Token>>> + Clone {
    just(Token::Stat(StatKeyword::Sort))
        .ignore_then(sort_order())
        .then(lax_col_name())
        .map(|(order, column)| SortStat { column, order })
        .boxed()
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinStat {
    SingleCol(SingleColJoinStat),
}

fn join_stat<'a>() -> impl Parser<'a, &'a [Token], JoinStat, extra::Err<Rich<'a, Token>>> + Clone {
    let single_col = single_col_join_stat().map(JoinStat::SingleCol);
    choice((single_col,)).boxed()
}

#[derive(Debug, Clone, PartialEq)]
pub struct SingleColJoinStat {
    pub other: String,
    pub ty: SingleColJoinType,
    // pub left_on: Vec<Expr>,
    // pub right_on: Vec<Expr>,
    pub left_on: Expr,
    pub right_on: Option<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SingleColJoinType {
    Left,
    Right,
    Inner,
    Outer,
}

fn single_col_join_stat<'a>(
) -> impl Parser<'a, &'a [Token], SingleColJoinStat, extra::Err<Rich<'a, Token>>> + Clone {
    let left = just(Token::Stat(StatKeyword::Left)).to(SingleColJoinType::Left);
    let right = just(Token::Stat(StatKeyword::Right)).to(SingleColJoinType::Right);
    let inner = just(Token::Stat(StatKeyword::Inner)).to(SingleColJoinType::Inner);
    let outer = just(Token::Stat(StatKeyword::Outer)).to(SingleColJoinType::Outer);
    let ty = choice((left, right, inner, outer));
    // let on = just(Token::Stat(StatKeyword::On)).ignore_then(expr().repeated().collect());
    let on = just(Token::Stat(StatKeyword::On))
        .ignore_then(expr())
        .then(expr().or_not());
    ty.then_ignore(just(Token::Stat(StatKeyword::Join)))
        .then(variable_token())
        .then(on.clone())
        .map(|((ty, other), (left_on, right_on))| SingleColJoinStat {
            other,
            ty,
            left_on,
            right_on,
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use crate::sql::{
        expr::{BinaryExpr, BinaryOperator, ExcludeExpr, UnaryExpr, UnaryOperator},
        lexer::lexer,
    };

    use super::*;

    #[test]
    fn test_select_expr() {
        let src = r#"select col a exclude b col c"#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        let parser = select_stat();
        let expr = parser.parse(&tokens).unwrap();
        assert_eq!(
            expr,
            SelectStat {
                columns: vec![
                    Expr::Col(String::from("a")),
                    Expr::Exclude(ExcludeExpr {
                        columns: vec![String::from("b"), String::from("c")],
                    }),
                ]
            }
        );
    }

    #[test]
    fn test_group_agg_stat() {
        let src = r#"group col "foo" "bar" agg sum col "foo" count col "bar""#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        let parser = group_agg_stat();
        let stat = parser.parse(&tokens).unwrap();
        assert_eq!(
            stat,
            GroupAggStat {
                group_by: vec![String::from("foo"), String::from("bar")],
                agg: vec![
                    Expr::Unary(Box::new(UnaryExpr {
                        operator: UnaryOperator::Sum,
                        expr: Expr::Col(String::from("foo")),
                    })),
                    Expr::Unary(Box::new(UnaryExpr {
                        operator: UnaryOperator::Count,
                        expr: Expr::Col(String::from("bar")),
                    })),
                ],
            }
        );
    }

    #[test]
    fn test_filter_stat() {
        let src = r#"filter col "foo" = 42"#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        let parser = filter_stat();
        let stat = parser.parse(&tokens).unwrap();
        assert_eq!(
            stat,
            FilterStat {
                condition: Expr::Binary(Box::new(BinaryExpr {
                    operator: BinaryOperator::Eq,
                    left: Expr::Col(String::from("foo")),
                    right: Expr::Literal(Literal::Int(String::from("42"))),
                })),
            }
        );
    }
}

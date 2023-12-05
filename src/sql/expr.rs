use chumsky::prelude::*;

use super::{
    lexer::{AggOperator, Conditional, Literal, StatKeyword, StringFunctor, Token, Type},
    string_token, variable_token,
};

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Col(String),
    Exclude(ExcludeExpr),
    Literal(Literal),
    Binary(Box<BinaryExpr>),
    Unary(Box<UnaryExpr>),
    Agg(Box<AggExpr>),
    Alias(Box<AliasExpr>),
    Conditional(Box<ConditionalExpr>),
    Cast(Box<CastExpr>),
    Str(Box<StrExpr>),
}

pub fn expr<'a>() -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
    recursive(|expr| {
        let parens = expr
            .clone()
            .nested_in(select_ref! { Token::Parens(parens) => parens.as_slice() });
        // let col = col_expr().map(Expr::Col);
        let col = lax_col_name().map(Expr::Col);
        let exclude = exclude_expr().map(Expr::Exclude);
        let literal = select_ref! { Token::Literal(lit) => lit.clone() }.map(Expr::Literal);
        let atom = choice((col, exclude, literal, parens));
        let agg = agg_expr(expr.clone()).map(Box::new).map(Expr::Agg);
        let alias = alias_expr(expr.clone()).map(Box::new).map(Expr::Alias);
        let unary = unary_expr(expr.clone()).map(Box::new).map(Expr::Unary);
        let conditional = conditional_expr(expr.clone())
            .map(Box::new)
            .map(Expr::Conditional);
        let cast = cast_expr(expr.clone()).map(Box::new).map(Expr::Cast);
        let str = str_expr(expr.clone()).map(Box::new).map(Expr::Str);
        let atom = choice((atom, agg, alias, unary, conditional, cast, str)).boxed();

        let term = term_expr(atom);
        let sum = sum_expr(term);
        let cmp = cmp_expr(sum);
        logic_expr(cmp)
    })
    .boxed()
}

pub fn lax_col_name<'a>(
) -> impl Parser<'a, &'a [Token], String, extra::Err<Rich<'a, Token>>> + Clone {
    choice((col_expr(), variable_token()))
}

fn col_expr<'a>() -> impl Parser<'a, &'a [Token], String, extra::Err<Rich<'a, Token>>> + Clone {
    let any = just(Token::Mul).to(String::from("*"));
    let name = choice((string_token(), variable_token(), any));
    just(Token::Col).ignore_then(name)
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExcludeExpr {
    pub columns: Vec<String>,
}

fn exclude_expr<'a>(
) -> impl Parser<'a, &'a [Token], ExcludeExpr, extra::Err<Rich<'a, Token>>> + Clone {
    just(Token::Exclude)
        .ignore_then(lax_col_name().repeated().collect())
        .map(|columns| ExcludeExpr { columns })
}

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
    Eq,
    LtEq,
    Lt,
    GtEq,
    Gt,
    And,
    Or,
}

fn term_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
    let mul = just(Token::Mul).to(BinaryOperator::Mul);
    let div = just(Token::Div).to(BinaryOperator::Div);
    let operator = choice((mul, div));
    binary_expr(operator, expr)
}

fn sum_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
    let add = just(Token::Add).to(BinaryOperator::Add);
    let sub = just(Token::Sub).to(BinaryOperator::Sub);
    let operator = choice((add, sub));
    binary_expr(operator, expr)
}

fn cmp_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
    let eq = just(Token::Eq).to(BinaryOperator::Eq);
    let lt = just(Token::LeftAngle).to(BinaryOperator::Lt);
    let lt_eq = just(Token::LeftAngle)
        .then(just(Token::Eq))
        .to(BinaryOperator::LtEq);
    let gt = just(Token::RightAngle).to(BinaryOperator::Gt);
    let gt_eq = just(Token::RightAngle)
        .then(just(Token::Eq))
        .to(BinaryOperator::GtEq);
    let operator = choice((eq, lt_eq, lt, gt_eq, gt));
    binary_expr(operator, expr)
}

fn logic_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
    let and = just(Token::Ampersand).to(BinaryOperator::And);
    let or = just(Token::Pipe).to(BinaryOperator::Or);
    let operator = choice((and, or));
    binary_expr(operator, expr)
}

fn binary_expr<'a>(
    operator: impl Parser<'a, &'a [Token], BinaryOperator, extra::Err<Rich<'a, Token>>> + Clone,
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
    expr.clone()
        .foldl(operator.then(expr).repeated(), |left, (operator, right)| {
            Expr::Binary(Box::new(BinaryExpr {
                operator,
                left,
                right,
            }))
        })
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnaryExpr {
    pub operator: UnaryOperator,
    pub expr: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnaryOperator {
    Neg,
    Not,
    Abs,
}

fn unary_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], UnaryExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let sub = just(Token::Sub).to(UnaryOperator::Neg);
    let not = just(Token::Bang).to(UnaryOperator::Not);
    let abs = just(Token::Abs).to(UnaryOperator::Abs);

    choice((sub, not, abs))
        .then(expr)
        .map(|(operator, expr)| UnaryExpr { operator, expr })
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggExpr {
    Unary(UnaryAggExpr),
    Standalone(StandaloneAggExpr),
    SortBy(SortByExpr),
}

fn agg_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], AggExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let sort_by = sort_by_expr(expr.clone()).map(AggExpr::SortBy);
    let unary = unary_agg_expr(expr.clone()).map(AggExpr::Unary);
    let standalone = standalone_agg_expr().map(AggExpr::Standalone);
    choice((sort_by, unary, standalone))
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortByExpr {
    pub pairs: Vec<(Expr, bool)>,
    pub expr: Expr,
}

fn sort_by_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], SortByExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let pair = expr
        .clone()
        .then(select_ref! { Token::Literal(Literal::Bool(descending)) => *descending });
    just(Token::Stat(StatKeyword::Sort))
        .ignore_then(expr)
        .then_ignore(just(Token::By))
        .then(pair.repeated().collect())
        .map(|(expr, pairs)| SortByExpr { pairs, expr })
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnaryAggExpr {
    pub operator: AggOperator,
    pub expr: Expr,
}

fn unary_agg_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], UnaryAggExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let operator = select_ref! { Token::AggOperator(operator) => *operator };
    operator
        .then(expr)
        .map(|(operator, expr)| UnaryAggExpr { operator, expr })
}

#[derive(Debug, Clone, PartialEq)]
pub struct StandaloneAggExpr {
    pub operator: StandaloneAggOperator,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StandaloneAggOperator {
    Count,
}

fn standalone_agg_expr<'a>(
) -> impl Parser<'a, &'a [Token], StandaloneAggExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let count =
        select_ref! { Token::AggOperator(AggOperator::Count) => StandaloneAggOperator::Count };
    let operator = choice((count,));
    operator.map(|operator| StandaloneAggExpr { operator })
}

#[derive(Debug, Clone, PartialEq)]
pub struct AliasExpr {
    pub name: String,
    pub expr: Expr,
}

fn alias_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], AliasExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let alias = just(Token::Alias);
    let name = choice((lax_col_name(), string_token()));
    alias
        .ignore_then(name.then(expr))
        .map(|(name, expr)| AliasExpr { name, expr })
}

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

fn conditional_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], ConditionalExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let case = just(Token::Conditional(Conditional::If))
        .ignore_then(expr.clone())
        .then_ignore(just(Token::Conditional(Conditional::Then)))
        .then(expr.clone())
        .map(|(when, then)| ConditionalCase { when, then });
    let first_case = case.clone();
    let other_cases = case.repeated().collect();
    let otherwise = just(Token::Conditional(Conditional::Else)).ignore_then(expr.clone());
    first_case
        .then(other_cases)
        .then(otherwise)
        .map(|((first_case, other_cases), otherwise)| ConditionalExpr {
            first_case,
            other_cases,
            otherwise,
        })
}

#[derive(Debug, Clone, PartialEq)]
pub struct CastExpr {
    pub expr: Expr,
    pub ty: Type,
}

fn cast_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], CastExpr, extra::Err<Rich<'a, Token>>> + Clone {
    just(Token::Cast)
        .ignore_then(select_ref! { Token::Type(ty) => *ty })
        .then(expr)
        .map(|(ty, expr)| CastExpr { expr, ty })
}

#[derive(Debug, Clone, PartialEq)]
pub enum StrExpr {
    Contains(Contains),
}

fn str_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], StrExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let contains = contains(expr.clone()).map(StrExpr::Contains);
    choice((contains,))
}

#[derive(Debug, Clone, PartialEq)]
pub struct Contains {
    pub str: Expr,
    pub pattern: Expr,
}

fn contains<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Contains, extra::Err<Rich<'a, Token>>> + Clone {
    just(Token::StringFunctor(StringFunctor::Contains))
        .ignore_then(expr.clone())
        .then(expr.clone())
        .map(|(pattern, str)| Contains { str, pattern })
}

#[cfg(test)]
mod tests {
    use crate::sql::lexer::lexer;

    use super::*;
    #[test]
    fn test_cmp_expr() {
        let src = r#"42 >= -sum (col "foo")"#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        let parser = expr();
        let expr = parser.parse(&tokens).unwrap();
        assert_eq!(
            expr,
            Expr::Binary(Box::new(BinaryExpr {
                operator: BinaryOperator::GtEq,
                left: Expr::Literal(Literal::Int(String::from("42"))),
                right: Expr::Unary(Box::new(UnaryExpr {
                    operator: UnaryOperator::Neg,
                    expr: Expr::Agg(Box::new(AggExpr::Unary(UnaryAggExpr {
                        operator: AggOperator::Sum,
                        expr: Expr::Col(String::from("foo")),
                    }))),
                })),
            }))
        );
    }

    #[test]
    fn test_sum_expr() {
        let src = r#"(42 + -sum (col "foo"))"#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        let parser = expr();
        let expr = parser.parse(&tokens).unwrap();
        assert_eq!(
            expr,
            Expr::Binary(Box::new(BinaryExpr {
                operator: BinaryOperator::Add,
                left: Expr::Literal(Literal::Int(String::from("42"))),
                right: Expr::Unary(Box::new(UnaryExpr {
                    operator: UnaryOperator::Neg,
                    expr: Expr::Agg(Box::new(AggExpr::Unary(UnaryAggExpr {
                        operator: AggOperator::Sum,
                        expr: Expr::Col(String::from("foo")),
                    }))),
                })),
            }))
        );
    }

    #[test]
    fn test_term_expr() {
        let src = r#"(42 + 1 * -sum (col "foo"))"#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        let parser = expr();
        let expr = parser.parse(&tokens).unwrap();
        assert_eq!(
            expr,
            Expr::Binary(Box::new(BinaryExpr {
                operator: BinaryOperator::Add,
                left: Expr::Literal(Literal::Int(String::from("42"))),
                right: Expr::Binary(Box::new(BinaryExpr {
                    operator: BinaryOperator::Mul,
                    left: Expr::Literal(Literal::Int(String::from("1"))),
                    right: Expr::Unary(Box::new(UnaryExpr {
                        operator: UnaryOperator::Neg,
                        expr: Expr::Agg(Box::new(AggExpr::Unary(UnaryAggExpr {
                            operator: AggOperator::Sum,
                            expr: Expr::Col(String::from("foo")),
                        }))),
                    })),
                })),
            }))
        );
    }

    #[test]
    fn test_unary_expr() {
        let src = r#"(-sum (col "foo"))"#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        let parser = expr();
        let expr = parser.parse(&tokens).unwrap();
        assert_eq!(
            expr,
            Expr::Unary(Box::new(UnaryExpr {
                operator: UnaryOperator::Neg,
                expr: Expr::Agg(Box::new(AggExpr::Unary(UnaryAggExpr {
                    operator: AggOperator::Sum,
                    expr: Expr::Col(String::from("foo")),
                }))),
            }))
        );
    }

    #[test]
    fn test_agg_expr() {
        let src = r#"(sum (col "foo"))"#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        let parser = expr();
        let expr = parser.parse(&tokens).unwrap();
        assert_eq!(
            expr,
            Expr::Agg(Box::new(AggExpr::Unary(UnaryAggExpr {
                operator: AggOperator::Sum,
                expr: Expr::Col(String::from("foo")),
            }))),
        );
    }

    #[test]
    fn test_alias_expr() {
        let src = r#"(alias "foo" 42)"#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        let parser = expr();
        let expr = parser.parse(&tokens).unwrap();
        assert_eq!(
            expr,
            Expr::Alias(Box::new(AliasExpr {
                name: String::from("foo"),
                expr: Expr::Literal(Literal::Int(String::from("42"))),
            }))
        );
    }
}
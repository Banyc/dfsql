use chumsky::prelude::*;

use super::{
    lexer::{Conditional, ExprKeyword, Literal, StatKeyword, StringFunctor, Symbol, Token, Type},
    string_token, variable_token,
};

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
    Str(Box<StrExpr>),
    Standalone(Box<StandaloneExpr>),
    SortBy(Box<SortByExpr>),
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
        let alias = alias_expr(expr.clone()).map(Box::new).map(Expr::Alias);
        let unary = unary_expr(expr.clone()).map(Box::new).map(Expr::Unary);
        let standalone = standalone_expr().map(Box::new).map(Expr::Standalone);
        let sort_by = sort_by_expr(expr.clone()).map(Box::new).map(Expr::SortBy);
        let conditional = conditional_expr(expr.clone())
            .map(Box::new)
            .map(Expr::Conditional);
        let cast = cast_expr(expr.clone()).map(Box::new).map(Expr::Cast);
        let str = str_expr(expr.clone()).map(Box::new).map(Expr::Str);
        let atom = choice((
            atom,
            alias,
            unary,
            standalone,
            sort_by,
            conditional,
            cast,
            str,
        ))
        .boxed();

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
    let any = just(Token::Symbol(Symbol::Mul)).to(String::from("*"));
    let name = choice((string_token(), variable_token(), any));
    just(Token::ExprKeyword(ExprKeyword::Col)).ignore_then(name)
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExcludeExpr {
    pub columns: Vec<String>,
}

fn exclude_expr<'a>(
) -> impl Parser<'a, &'a [Token], ExcludeExpr, extra::Err<Rich<'a, Token>>> + Clone {
    just(Token::ExprKeyword(ExprKeyword::Exclude))
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
    let mul = just(Token::Symbol(Symbol::Mul)).to(BinaryOperator::Mul);
    let div = just(Token::Symbol(Symbol::Div)).to(BinaryOperator::Div);
    let operator = choice((mul, div));
    binary_expr(operator, expr)
}

fn sum_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
    let add = just(Token::Symbol(Symbol::Add)).to(BinaryOperator::Add);
    let sub = just(Token::Symbol(Symbol::Sub)).to(BinaryOperator::Sub);
    let operator = choice((add, sub));
    binary_expr(operator, expr)
}

fn cmp_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
    let eq = just(Token::Symbol(Symbol::Eq)).to(BinaryOperator::Eq);
    let lt = just(Token::Symbol(Symbol::LeftAngle)).to(BinaryOperator::Lt);
    let lt_eq = just(Token::Symbol(Symbol::LeftAngle))
        .then(just(Token::Symbol(Symbol::Eq)))
        .to(BinaryOperator::LtEq);
    let gt = just(Token::Symbol(Symbol::RightAngle)).to(BinaryOperator::Gt);
    let gt_eq = just(Token::Symbol(Symbol::RightAngle))
        .then(just(Token::Symbol(Symbol::Eq)))
        .to(BinaryOperator::GtEq);
    let operator = choice((eq, lt_eq, lt, gt_eq, gt));
    binary_expr(operator, expr)
}

fn logic_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
    let and = just(Token::Symbol(Symbol::Ampersand)).to(BinaryOperator::And);
    let or = just(Token::Symbol(Symbol::Pipe)).to(BinaryOperator::Or);
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
        .then_ignore(just(Token::ExprKeyword(ExprKeyword::By)))
        .then(pair.repeated().collect())
        .map(|(expr, pairs)| SortByExpr { pairs, expr })
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnaryExpr {
    pub operator: UnaryOperator,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Sum,
    Count,
    First,
    Last,
    Sort,
    Reverse,
    Mean,
    Median,
    Abs,
    Unique,
    Not,
    Neg,
    IsNull,
}

macro_rules! select_map_named_unary_op {
    ($name:ident) => {
        select_ref! { Token::ExprKeyword(ExprKeyword::$name) => UnaryOperator::$name }
    };
}

macro_rules! choice_named_unary_op {
    ($($name:ident),*) => {
        {
            $(
                let $name = select_map_named_unary_op!($name);
            )*
            choice(( $($name,)* ))
        }
    };
}

fn unary_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], UnaryExpr, extra::Err<Rich<'a, Token>>> + Clone {
    #[allow(non_snake_case)]
    let named =
        choice_named_unary_op!(Sum, Count, First, Last, Sort, Reverse, Mean, Median, Abs, Unique);
    let neg = select_ref! { Token::Symbol(Symbol::Sub) => UnaryOperator::Neg };
    let not = select_ref! { Token::Symbol(Symbol::Bang) => UnaryOperator::Not };
    let is_null = just(Token::ExprKeyword(ExprKeyword::Is))
        .ignore_then(just(Token::Literal(Literal::Null)))
        .to(UnaryOperator::IsNull);
    let operator = choice((named, neg, not, is_null));

    operator
        .then(expr)
        .map(|(operator, expr)| UnaryExpr { operator, expr })
}

#[derive(Debug, Clone, PartialEq)]
pub struct StandaloneExpr {
    pub operator: StandaloneOperator,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StandaloneOperator {
    Count,
}

fn standalone_expr<'a>(
) -> impl Parser<'a, &'a [Token], StandaloneExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let count = select_ref! { Token::ExprKeyword(ExprKeyword::Count) => StandaloneOperator::Count };
    let operator = choice((count,));
    operator.map(|operator| StandaloneExpr { operator })
}

#[derive(Debug, Clone, PartialEq)]
pub struct AliasExpr {
    pub name: String,
    pub expr: Expr,
}

fn alias_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], AliasExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let alias = just(Token::ExprKeyword(ExprKeyword::Alias));
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
    just(Token::ExprKeyword(ExprKeyword::Cast))
        .ignore_then(select_ref! { Token::Type(ty) => *ty })
        .then(expr)
        .map(|(ty, expr)| CastExpr { expr, ty })
}

#[derive(Debug, Clone, PartialEq)]
pub enum StrExpr {
    Contains(Contains),
    Extract(Extract),
    ExtractAll(ExtractAll),
}

fn str_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], StrExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let contains = contains(expr.clone()).map(StrExpr::Contains);
    let extract = extract(expr.clone()).map(StrExpr::Extract);
    let extract_all = extract_all(expr.clone()).map(StrExpr::ExtractAll);
    choice((contains, extract, extract_all))
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

#[derive(Debug, Clone, PartialEq)]
pub struct Extract {
    pub str: Expr,
    pub pattern: String,
    pub group: usize,
}

fn extract<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Extract, extra::Err<Rich<'a, Token>>> + Clone {
    let group = select_ref! { Token::Literal(Literal::Int(group)) => group }
        .map(|group| group.parse::<usize>().unwrap());
    just(Token::StringFunctor(StringFunctor::Extract))
        .ignore_then(string_token())
        .then(group)
        .then(expr)
        .map(|((pattern, group), str)| Extract {
            str,
            pattern,
            group,
        })
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExtractAll {
    pub str: Expr,
    pub pattern: Expr,
}

fn extract_all<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], ExtractAll, extra::Err<Rich<'a, Token>>> + Clone {
    just(Token::StringFunctor(StringFunctor::Extract))
        .ignore_then(just(Token::StringFunctor(StringFunctor::All)))
        .ignore_then(expr.clone())
        .then(expr)
        .map(|(pattern, str)| ExtractAll { str, pattern })
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
        let src = r#"(-sum (col "foo"))"#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        let parser = expr();
        let expr = parser.parse(&tokens).unwrap();
        assert_eq!(
            expr,
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
        let src = r#"(sum (col "foo"))"#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        let parser = expr();
        let expr = parser.parse(&tokens).unwrap();
        assert_eq!(
            expr,
            Expr::Unary(Box::new(UnaryExpr {
                operator: UnaryOperator::Sum,
                expr: Expr::Col(String::from("foo")),
            })),
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

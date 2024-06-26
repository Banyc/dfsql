use chumsky::prelude::*;

use super::{
    lexer::{Conditional, ExprKeyword, Literal, StatKeyword, StringKeyword, Symbol, Token, Type},
    sort_order, string_token, variable_token, SortOrder,
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
    Log(Box<LogExpr>),
    Str(Box<StrExpr>),
    Standalone(Box<StandaloneExpr>),
    SortBy(Box<SortByExpr>),
    Sort(Box<SortExpr>),
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
        let sort = sort_expr(expr.clone()).map(Box::new).map(Expr::Sort);
        let conditional = conditional_expr(expr.clone())
            .map(Box::new)
            .map(Expr::Conditional);
        let cast = cast_expr(expr.clone()).map(Box::new).map(Expr::Cast);
        let log = log_expr(expr.clone()).map(Box::new).map(Expr::Log);
        let str = str_expr(expr.clone()).map(Box::new).map(Expr::Str);
        let atom = choice((
            atom,
            alias,
            unary,
            standalone,
            sort_by,
            sort,
            conditional,
            cast,
            log,
            str,
        ))
        .boxed();

        let power = power_expr(atom);
        let term = term_expr(power);
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
fn power_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
    let pow = just(Token::ExprKeyword(ExprKeyword::Pow)).to(BinaryOperator::Pow);
    let operator = choice((pow,));
    binary_expr(operator, expr)
}
fn term_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
    let mul = just(Token::Symbol(Symbol::Mul)).to(BinaryOperator::Mul);
    let div = just(Token::Symbol(Symbol::Div)).to(BinaryOperator::Div);
    let modulo = just(Token::Symbol(Symbol::Percent)).to(BinaryOperator::Modulo);
    let operator = choice((mul, div, modulo));
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
    let not_eq = just(Token::Symbol(Symbol::Bang))
        .then(just(Token::Symbol(Symbol::Eq)))
        .to(BinaryOperator::NotEq);
    let lt = just(Token::Symbol(Symbol::LeftAngle)).to(BinaryOperator::Lt);
    let lt_eq = just(Token::Symbol(Symbol::LeftAngle))
        .then(just(Token::Symbol(Symbol::Eq)))
        .to(BinaryOperator::LtEq);
    let gt = just(Token::Symbol(Symbol::RightAngle)).to(BinaryOperator::Gt);
    let gt_eq = just(Token::Symbol(Symbol::RightAngle))
        .then(just(Token::Symbol(Symbol::Eq)))
        .to(BinaryOperator::GtEq);
    let operator = choice((eq, not_eq, lt_eq, lt, gt_eq, gt));
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
    pub pairs: Vec<(SortOrder, Expr)>,
    pub expr: Expr,
}
fn sort_by_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], SortByExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let pair = sort_order().then(expr.clone());
    just(Token::Stat(StatKeyword::Sort))
        .ignore_then(expr)
        .then_ignore(just(Token::ExprKeyword(ExprKeyword::By)))
        .then(pair.repeated().collect())
        .map(|(expr, pairs)| SortByExpr { pairs, expr })
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortExpr {
    pub expr: Expr,
    pub order: SortOrder,
}
fn sort_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], SortExpr, extra::Err<Rich<'a, Token>>> + Clone {
    just(Token::ExprKeyword(ExprKeyword::Sort))
        .ignore_then(sort_order())
        .then(expr)
        .map(|(order, expr)| SortExpr { expr, order })
}

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
macro_rules! is {
    ($token:expr => $op:expr) => {
        just(Token::ExprKeyword(ExprKeyword::Is))
            .then(just($token))
            .to($op)
    };
}
fn unary_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], UnaryExpr, extra::Err<Rich<'a, Token>>> + Clone {
    #[allow(non_snake_case)]
    let named = choice_named_unary_op!(
        Sum, Sqrt, Count, First, Last, Reverse, Mean, Median, Max, Min, Var, Std, Abs, Unique
    );
    let neg = select_ref! { Token::Symbol(Symbol::Sub) => UnaryOperator::Neg };
    let not = select_ref! { Token::Symbol(Symbol::Bang) => UnaryOperator::Not };
    let is_null = is!(Token::Literal(Literal::Null) => UnaryOperator::IsNull);
    let is_nan = is!(Token::ExprKeyword(ExprKeyword::Nan) => UnaryOperator::IsNan);
    let is = choice((is_null, is_nan));
    let all = just(Token::ExprKeyword(ExprKeyword::All)).to(UnaryOperator::All);
    let any = just(Token::ExprKeyword(ExprKeyword::Any)).to(UnaryOperator::Any);
    let operator = choice((named, neg, not, is, all, any));

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
    Len,
}
fn standalone_expr<'a>(
) -> impl Parser<'a, &'a [Token], StandaloneExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let len = select_ref! { Token::ExprKeyword(ExprKeyword::Len) => StandaloneOperator::Len };
    let operator = choice((len,));
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
pub struct LogExpr {
    pub expr: Expr,
    pub base: f64,
}
fn log_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], LogExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let float = select_ref! { Token::Literal(Literal::Float(float)) => float };
    let int = select_ref! { Token::Literal(Literal::Int(int)) => int };
    let base = choice((float, int)).map(|s| s.parse::<f64>().unwrap());

    just(Token::ExprKeyword(ExprKeyword::Log))
        .ignore_then(base)
        .then(expr.clone())
        .map(|(base, expr)| LogExpr { expr, base })
}

#[derive(Debug, Clone, PartialEq)]
pub enum StrExpr {
    Contains(Contains),
    Extract(Extract),
    ExtractAll(ExtractAll),
    Split(Split),
}
fn str_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], StrExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let contains = contains(expr.clone()).map(StrExpr::Contains);
    let extract = extract(expr.clone()).map(StrExpr::Extract);
    let extract_all = extract_all(expr.clone()).map(StrExpr::ExtractAll);
    let split = split(expr.clone()).map(StrExpr::Split);
    choice((contains, extract, extract_all, split))
}

#[derive(Debug, Clone, PartialEq)]
pub struct Contains {
    pub str: Expr,
    pub pattern: Expr,
}
fn contains<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Contains, extra::Err<Rich<'a, Token>>> + Clone {
    just(Token::StringKeyword(StringKeyword::Contains))
        .ignore_then(expr.clone())
        .then(expr.clone())
        .map(|(pattern, str)| Contains { str, pattern })
}

#[derive(Debug, Clone, PartialEq)]
pub struct Extract {
    pub str: Expr,
    pub pattern: Expr,
    pub group: usize,
}
fn extract<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Extract, extra::Err<Rich<'a, Token>>> + Clone {
    let group = select_ref! { Token::Literal(Literal::Int(group)) => group }
        .map(|group| group.parse::<usize>().unwrap());
    just(Token::StringKeyword(StringKeyword::Extract))
        .ignore_then(expr.clone())
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
    just(Token::StringKeyword(StringKeyword::Extract))
        .ignore_then(just(Token::StringKeyword(StringKeyword::All)))
        .ignore_then(expr.clone())
        .then(expr)
        .map(|(pattern, str)| ExtractAll { str, pattern })
}

#[derive(Debug, Clone, PartialEq)]
pub struct Split {
    pub str: Expr,
    pub pattern: Expr,
}
fn split<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], Split, extra::Err<Rich<'a, Token>>> + Clone {
    just(Token::StringKeyword(StringKeyword::Split))
        .ignore_then(expr.clone())
        .then(expr)
        .map(|(pattern, str)| Split { str, pattern })
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

    #[test]
    fn test_nested_conditional_expr() {
        let src = r#"(if (if 1.1 then 1.2 if 1.3 then 1.4 else 1.5) then (if 2.1 then 2.2 if 2.3 then 2.4 else 2.5) if (if 3.1 then 3.2 if 3.3 then 3.4 else 3.5) then (if 4.1 then 4.2 if 4.3 then 4.4 else 4.5) else (if 5.1 then 5.2 if 5.3 then 5.4 else 5.5))"#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        let parser = expr();
        let expr = parser.parse(&tokens).unwrap();
        assert_eq!(
            expr,
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

use chumsky::prelude::*;

#[derive(Debug, Clone, PartialEq)]
pub struct S {
    pub statements: Vec<Stat>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Stat {
    Select(SelectStat),
    GroupAgg(GroupAggStat),
    Filter(FilterStat),
}

fn parser<'a>() -> impl Parser<'a, &'a [Token], S, extra::Err<Rich<'a, Token>>> + Clone {
    let select = select_stat().map(Stat::Select);
    let group_agg = group_agg_stat().map(Stat::GroupAgg);
    let filter = filter_stat().map(Stat::Filter);
    let stat = choice((select, group_agg, filter));

    stat.repeated().collect().map(|statements| S { statements })
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStat {
    pub columns: Vec<Expr>,
}

fn select_stat<'a>() -> impl Parser<'a, &'a [Token], SelectStat, extra::Err<Rich<'a, Token>>> + Clone
{
    just(Token::Select)
        .ignore_then(expr().repeated().collect())
        .map(|columns| SelectStat { columns })
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupAggStat {
    pub group_by: Vec<String>,
    pub agg: Vec<Expr>,
}

fn group_agg_stat<'a>(
) -> impl Parser<'a, &'a [Token], GroupAggStat, extra::Err<Rich<'a, Token>>> + Clone {
    // let columns =
    //     column_names().nested_in(select_ref! { Token::Brackets(columns) => columns.as_slice() });
    let columns = column_names();

    just(Token::GroupBy)
        .ignore_then(columns)
        .then_ignore(just(Token::Agg))
        .then(expr().repeated().collect())
        .map(|(group_by, agg)| GroupAggStat { group_by, agg })
}

fn column_names<'a>(
) -> impl Parser<'a, &'a [Token], Vec<String>, extra::Err<Rich<'a, Token>>> + Clone {
    let col = just(Token::Col).ignore_then(string_token());
    let literal = string_token();
    let column = choice((col, literal));
    column.repeated().collect()
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterStat {
    condition: Expr,
}

fn filter_stat<'a>() -> impl Parser<'a, &'a [Token], FilterStat, extra::Err<Rich<'a, Token>>> + Clone
{
    just(Token::Filter)
        .ignore_then(expr())
        .map(|condition| FilterStat { condition })
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Col(String),
    Literal(Literal),
    Binary(Box<BinaryExpr>),
    Unary(Box<UnaryExpr>),
    Agg(Box<AggExpr>),
    Alias(Box<AliasExpr>),
}

fn expr<'a>() -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
    recursive(|expr| {
        let parens = expr
            .clone()
            .nested_in(select_ref! { Token::Parens(parens) => parens.as_slice() });
        let col = just(Token::Col).ignore_then(string_token()).map(Expr::Col);
        let literal = select_ref! { Token::Literal(lit) => lit.clone() }.map(Expr::Literal);
        let atom = choice((col, literal, parens));
        let agg = agg_expr(expr.clone()).map(Box::new).map(Expr::Agg);
        let alias = alias_expr(expr.clone()).map(Box::new).map(Expr::Alias);
        let unary = unary_expr(expr.clone()).map(Box::new).map(Expr::Unary);
        let atom = choice((atom, agg, alias, unary));

        let term = term_expr(atom);
        let sum = sum_expr(term);
        cmp_expr(sum)
    })
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
    Sub,
    Not,
}

fn unary_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], UnaryExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let sub = just(Token::Sub).to(UnaryOperator::Sub);
    let not = just(Token::Bang).to(UnaryOperator::Not);

    choice((sub, not))
        .then(expr)
        .map(|(operator, expr)| UnaryExpr { operator, expr })
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggExpr {
    pub operator: AggOperator,
    pub expr: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggOperator {
    Sum,
    Count,
}

fn agg_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], AggExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let operator = select_ref! { Token::AggOperator(operator) => *operator };
    operator
        .then(expr)
        .map(|(operator, expr)| AggExpr { operator, expr })
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
    let name = string_token();
    alias
        .ignore_then(name.then(expr))
        .map(|(name, expr)| AliasExpr { name, expr })
}

fn string_token<'a>() -> impl Parser<'a, &'a [Token], String, extra::Err<Rich<'a, Token>>> + Clone {
    select_ref! { Token::Literal(Literal::String(name)) => name.clone() }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Select,
    GroupBy,
    Agg,
    AggOperator(AggOperator),
    Filter,
    Alias,
    Col,
    Parens(Vec<Token>),
    Brackets(Vec<Token>),
    LeftAngle,
    RightAngle,
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Bang,
    Comma,
    Variable(String),
    Literal(Literal),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Number(String),
}

fn lexer<'a>() -> impl Parser<'a, &'a str, Vec<Token>, extra::Err<Rich<'a, char>>> + Clone {
    recursive(|tokens| {
        let select = text::keyword("select").to(Token::Select);
        let group_by = text::keyword("group").to(Token::GroupBy);
        let agg = text::keyword("agg").to(Token::Agg);
        let sum = text::keyword("sum").to(AggOperator::Sum);
        let count = text::keyword("count").to(AggOperator::Count);
        let agg_op = choice((sum, count)).map(Token::AggOperator);
        let filter = text::keyword("filter").to(Token::Filter);
        let alias = text::keyword("alias").to(Token::Alias);
        let col = text::keyword("col").to(Token::Col);
        let parens = tokens
            .clone()
            .delimited_by(just('(').padded(), just(')').padded())
            .map(Token::Parens);
        let brackets = tokens
            .clone()
            .delimited_by(just('[').padded(), just(']').padded())
            .map(Token::Brackets);
        let left_angle = just('<').to(Token::LeftAngle);
        let right_angle = just('>').to(Token::RightAngle);
        let add = just('+').to(Token::Add);
        let sub = just('-').to(Token::Sub);
        let mul = just('*').to(Token::Mul);
        let div = just('/').to(Token::Div);
        let eq = just('=').to(Token::Eq);
        let bang = just('!').to(Token::Bang);
        let comma = just(',').to(Token::Comma);
        let ident = text::ident().map(ToString::to_string).map(Token::Variable);
        let pos_number = text::digits(10)
            .then(just('.').then(text::digits(10)).or_not())
            .to_slice()
            .map(ToString::to_string)
            .map(Literal::Number)
            .map(Token::Literal);
        let string = string().map(Literal::String).map(Token::Literal);
        let token = choice((
            select,
            group_by,
            agg,
            agg_op,
            filter,
            alias,
            col,
            brackets,
            parens,
            left_angle,
            right_angle,
            add,
            sub,
            mul,
            div,
            eq,
            bang,
            comma,
            ident,
            pos_number,
            string,
        ));
        token.padded().repeated().collect()
    })
}

/// Ref: <https://github.com/zesterer/chumsky/blob/dce5918bd2dad591ab399d2e191254640a9ed14f/examples/json.rs#L64>
fn string<'a>() -> impl Parser<'a, &'a str, String, extra::Err<Rich<'a, char>>> + Clone {
    let escape = just('\\')
        .then(choice((
            just('\\'),
            just('/'),
            just('"'),
            just('b').to('\x08'),
            just('f').to('\x0C'),
            just('n').to('\n'),
            just('r').to('\r'),
            just('t').to('\t'),
            just('u').ignore_then(text::digits(16).exactly(4).to_slice().validate(
                |digits, e, emitter| {
                    char::from_u32(u32::from_str_radix(digits, 16).unwrap()).unwrap_or_else(|| {
                        emitter.emit(Rich::custom(e.span(), "invalid unicode character"));
                        '\u{FFFD}' // unicode replacement character
                    })
                },
            )),
        )))
        .ignored()
        .boxed();

    none_of("\\\"")
        .ignored()
        .or(escape)
        .repeated()
        .to_slice()
        .map(ToString::to_string)
        .delimited_by(just('"'), just('"'))
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_agg_stat() {
        // let src = r#"group [col "foo" "bar"] agg sum col "foo" count col "bar""#;
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
                    Expr::Agg(Box::new(AggExpr {
                        operator: AggOperator::Sum,
                        expr: Expr::Col(String::from("foo")),
                    })),
                    Expr::Agg(Box::new(AggExpr {
                        operator: AggOperator::Count,
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
                    right: Expr::Literal(Literal::Number(String::from("42"))),
                })),
            }
        );
    }

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
                left: Expr::Literal(Literal::Number(String::from("42"))),
                right: Expr::Unary(Box::new(UnaryExpr {
                    operator: UnaryOperator::Sub,
                    expr: Expr::Agg(Box::new(AggExpr {
                        operator: AggOperator::Sum,
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
                left: Expr::Literal(Literal::Number(String::from("42"))),
                right: Expr::Unary(Box::new(UnaryExpr {
                    operator: UnaryOperator::Sub,
                    expr: Expr::Agg(Box::new(AggExpr {
                        operator: AggOperator::Sum,
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
                left: Expr::Literal(Literal::Number(String::from("42"))),
                right: Expr::Binary(Box::new(BinaryExpr {
                    operator: BinaryOperator::Mul,
                    left: Expr::Literal(Literal::Number(String::from("1"))),
                    right: Expr::Unary(Box::new(UnaryExpr {
                        operator: UnaryOperator::Sub,
                        expr: Expr::Agg(Box::new(AggExpr {
                            operator: AggOperator::Sum,
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
                operator: UnaryOperator::Sub,
                expr: Expr::Agg(Box::new(AggExpr {
                    operator: AggOperator::Sum,
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
            Expr::Agg(Box::new(AggExpr {
                operator: AggOperator::Sum,
                expr: Expr::Col(String::from("foo")),
            }))
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
                expr: Expr::Literal(Literal::Number(String::from("42"))),
            }))
        );
    }

    #[test]
    fn test_lexer_empty() {
        let src = "";
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_lexer() {
        let src =
            r#"select group agg sum count filter alias col [ ] ( ) < > hi + - * / = -42 0.1 "hi""#;
        let lexer = lexer();
        let tokens = lexer.parse(src).unwrap();
        assert_eq!(
            tokens,
            [
                Token::Select,
                Token::GroupBy,
                Token::Agg,
                Token::AggOperator(AggOperator::Sum),
                Token::AggOperator(AggOperator::Count),
                Token::Filter,
                Token::Alias,
                Token::Col,
                Token::Brackets(vec![]),
                Token::Parens(vec![]),
                Token::LeftAngle,
                Token::RightAngle,
                Token::Variable(String::from("hi")),
                Token::Add,
                Token::Sub,
                Token::Mul,
                Token::Div,
                Token::Eq,
                Token::Sub,
                Token::Literal(Literal::Number(String::from("42"))),
                Token::Literal(Literal::Number(String::from("0.1"))),
                Token::Literal(Literal::String(String::from("hi"))),
            ]
        );
    }
}

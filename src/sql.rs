use chumsky::prelude::*;

pub fn parse(src: &str) -> Option<S> {
    let lexer = lexer();
    let Some(tokens) = lexer
        .parse(src)
        .into_result()
        .map_err(|e| eprint!("{e:?}"))
        .ok()
    else {
        return None;
    };
    let parser = parser();
    let Some(ast) = parser
        .parse(&tokens)
        .into_result()
        .map_err(|e| eprintln!("{e:?}"))
        .ok()
    else {
        return None;
    };
    Some(ast)
}

#[derive(Debug, Clone, PartialEq)]
pub struct S {
    pub statements: Vec<Stat>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Stat {
    Select(SelectStat),
    GroupAgg(GroupAggStat),
    Filter(FilterStat),
    Limit(LimitStat),
    Reverse,
}

fn parser<'a>() -> impl Parser<'a, &'a [Token], S, extra::Err<Rich<'a, Token>>> + Clone {
    let select = select_stat().map(Stat::Select);
    let group_agg = group_agg_stat().map(Stat::GroupAgg);
    let filter = filter_stat().map(Stat::Filter);
    let limit = limit_stat().map(Stat::Limit);
    let reverse = just(Token::Reverse).to(Stat::Reverse);
    let stat = choice((select, group_agg, filter, limit, reverse));

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
    let column = lax_col_name();
    column.repeated().collect()
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterStat {
    pub condition: Expr,
}

fn filter_stat<'a>() -> impl Parser<'a, &'a [Token], FilterStat, extra::Err<Rich<'a, Token>>> + Clone
{
    just(Token::Filter)
        .ignore_then(expr())
        .map(|condition| FilterStat { condition })
}

#[derive(Debug, Clone, PartialEq)]
pub struct LimitStat {
    pub rows: String,
}

fn limit_stat<'a>() -> impl Parser<'a, &'a [Token], LimitStat, extra::Err<Rich<'a, Token>>> + Clone
{
    just(Token::Limit)
        .ignore_then(select_ref! { Token::Literal(Literal::Int(rows)) => rows.clone() })
        .map(|rows| LimitStat { rows })
}

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
}

fn expr<'a>() -> impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone {
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
        let atom = choice((atom, agg, alias, unary, conditional, cast)).boxed();

        let term = term_expr(atom);
        let sum = sum_expr(term);
        let cmp = cmp_expr(sum);
        logic_expr(cmp)
    })
    .boxed()
}

fn lax_col_name<'a>() -> impl Parser<'a, &'a [Token], String, extra::Err<Rich<'a, Token>>> + Clone {
    choice((col_expr(), string_token(), variable_token()))
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
}

fn unary_expr<'a>(
    expr: impl Parser<'a, &'a [Token], Expr, extra::Err<Rich<'a, Token>>> + Clone,
) -> impl Parser<'a, &'a [Token], UnaryExpr, extra::Err<Rich<'a, Token>>> + Clone {
    let sub = just(Token::Sub).to(UnaryOperator::Neg);
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
    let name = lax_col_name();
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
    let case = just(Token::If)
        .ignore_then(expr.clone())
        .then_ignore(just(Token::Then))
        .then(expr.clone())
        .map(|(when, then)| ConditionalCase { when, then });
    let first_case = case.clone();
    let other_cases = case.repeated().collect();
    let otherwise = just(Token::Else).ignore_then(expr.clone());
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

fn string_token<'a>() -> impl Parser<'a, &'a [Token], String, extra::Err<Rich<'a, Token>>> + Clone {
    select_ref! { Token::Literal(Literal::String(name)) => name.clone() }
}

fn variable_token<'a>() -> impl Parser<'a, &'a [Token], String, extra::Err<Rich<'a, Token>>> + Clone
{
    select_ref! { Token::Variable(name) => name.clone() }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Select,
    GroupBy,
    Agg,
    Filter,
    Limit,
    Reverse,
    AggOperator(AggOperator),
    Alias,
    Col,
    Exclude,
    If,
    Then,
    Else,
    Type(Type),
    Cast,
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
    Ampersand,
    Pipe,
    Comma,
    Variable(String),
    Literal(Literal),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Int(String),
    Float(String),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
    Str,
    Int,
    Float,
}

fn lexer<'a>() -> impl Parser<'a, &'a str, Vec<Token>, extra::Err<Rich<'a, char>>> + Clone {
    recursive(|tokens| {
        let select = text::keyword("select").to(Token::Select);
        let group_by = text::keyword("group").to(Token::GroupBy);
        let agg = text::keyword("agg").to(Token::Agg);
        let filter = text::keyword("filter").to(Token::Filter);
        let limit = text::keyword("limit").to(Token::Limit);
        let reverse = text::keyword("reverse").to(Token::Reverse);
        let statement = choice((select, group_by, agg, filter, limit, reverse));

        let sum = text::keyword("sum").to(AggOperator::Sum);
        let count = text::keyword("count").to(AggOperator::Count);
        let agg_op = choice((sum, count)).map(Token::AggOperator);

        let alias = text::keyword("alias").to(Token::Alias);
        let col = text::keyword("col").to(Token::Col);
        let exclude = text::keyword("exclude").to(Token::Exclude);
        let when = text::keyword("if").to(Token::If);
        let then = text::keyword("then").to(Token::Then);
        let otherwise = text::keyword("else").to(Token::Else);
        let conditional = choice((when, then, otherwise));
        let str = text::keyword("str").to(Type::Str);
        let int = text::keyword("int").to(Type::Int);
        let float = text::keyword("float").to(Type::Float);
        let ty = choice((str, int, float)).map(Token::Type);
        let cast = text::keyword("cast").to(Token::Cast);
        let expr_functor = choice((alias, col, exclude, conditional, ty, cast));

        let parens = tokens
            .clone()
            .delimited_by(just('(').padded(), just(')').padded())
            .map(Token::Parens);
        let brackets = tokens
            .clone()
            .delimited_by(just('[').padded(), just(']').padded())
            .map(Token::Brackets);
        let group = choice((parens, brackets));

        let left_angle = just('<').to(Token::LeftAngle);
        let right_angle = just('>').to(Token::RightAngle);
        let add = just('+').to(Token::Add);
        let sub = just('-').to(Token::Sub);
        let mul = just('*').to(Token::Mul);
        let div = just('/').to(Token::Div);
        let eq = just('=').to(Token::Eq);
        let bang = just('!').to(Token::Bang);
        let ampersand = just('&').to(Token::Ampersand);
        let pipe = just('|').to(Token::Pipe);
        let comma = just(',').to(Token::Comma);

        let pos_float = text::digits(10)
            .then(just('.').then(text::digits(10)))
            .to_slice()
            .map(ToString::to_string)
            .map(Literal::Float);
        let pos_int = text::digits(10)
            .to_slice()
            .map(ToString::to_string)
            .map(Literal::Int);
        let string = string().map(Literal::String);
        let bool_true = text::keyword("true").to(Literal::Bool(true));
        let bool_false = text::keyword("false").to(Literal::Bool(false));
        let bool = choice((bool_true, bool_false));
        let literal = choice((pos_float, pos_int, bool, string)).map(Token::Literal);

        let ident = text::ident().map(ToString::to_string).map(Token::Variable);

        let token = choice((
            statement,
            agg_op,
            expr_functor,
            group,
            left_angle,
            right_angle,
            add,
            sub,
            mul,
            div,
            eq,
            bang,
            ampersand,
            pipe,
            comma,
            literal,
            ident,
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
                    right: Expr::Literal(Literal::Int(String::from("42"))),
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
                left: Expr::Literal(Literal::Int(String::from("42"))),
                right: Expr::Unary(Box::new(UnaryExpr {
                    operator: UnaryOperator::Neg,
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
                left: Expr::Literal(Literal::Int(String::from("42"))),
                right: Expr::Unary(Box::new(UnaryExpr {
                    operator: UnaryOperator::Neg,
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
                left: Expr::Literal(Literal::Int(String::from("42"))),
                right: Expr::Binary(Box::new(BinaryExpr {
                    operator: BinaryOperator::Mul,
                    left: Expr::Literal(Literal::Int(String::from("1"))),
                    right: Expr::Unary(Box::new(UnaryExpr {
                        operator: UnaryOperator::Neg,
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
                operator: UnaryOperator::Neg,
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
                expr: Expr::Literal(Literal::Int(String::from("42"))),
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
                Token::Literal(Literal::Int(String::from("42"))),
                Token::Literal(Literal::Float(String::from("0.1"))),
                Token::Literal(Literal::String(String::from("hi"))),
            ]
        );
    }
}

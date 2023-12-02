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

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStat {
    pub columns: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupAggStat {
    pub group_by: Vec<String>,
    pub agg: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterStat {
    condition: Expr,
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

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryExpr {
    pub operator: BinaryOperator,
    pub left: Expr,
    pub right: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnaryExpr {
    pub operator: BinaryOperator,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Sub,
    Not,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggExpr {
    pub operator: AggOperator,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggOperator {
    Sum,
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AliasExpr {
    pub name: String,
    pub expr: Expr,
}

fn parser<'a>() -> impl Parser<'a, &'a Token, S, extra::Err<Rich<'a, Token>>> {
    todo!()
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Select,
    GroupBy,
    Agg,
    Filter,
    AggOperator(AggOperator),
    LeftBracket,
    RightBracket,
    LeftParenthesis,
    RightParenthesis,
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

fn lexer<'a>() -> impl Parser<'a, &'a str, Vec<Token>, extra::Err<Rich<'a, char>>> {
    let select = text::keyword("select").to(Token::Select);
    let group_by = text::keyword("group").to(Token::GroupBy);
    let agg = text::keyword("agg").to(Token::Agg);
    let sum = text::keyword("sum").to(AggOperator::Sum);
    let count = text::keyword("count").to(AggOperator::Count);
    let agg_op = choice((sum, count)).map(Token::AggOperator);
    let filter = text::keyword("filter").to(Token::Filter);
    let left_bracket = just('[').to(Token::LeftBracket);
    let right_bracket = just(']').to(Token::RightBracket);
    let left_parenthesis = just('(').to(Token::LeftParenthesis);
    let right_parenthesis = just(')').to(Token::RightParenthesis);
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
        left_bracket,
        right_bracket,
        left_parenthesis,
        right_parenthesis,
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
}

/// Ref: <https://github.com/zesterer/chumsky/blob/dce5918bd2dad591ab399d2e191254640a9ed14f/examples/json.rs#L64>
fn string<'a>() -> impl Parser<'a, &'a str, String, extra::Err<Rich<'a, char>>> {
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
    fn test_lexer() {
        let src = r#"select group agg sum count filter [ ] ( ) < > hi + - * / = -42 0.1 "hi""#;
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
                Token::LeftBracket,
                Token::RightBracket,
                Token::LeftParenthesis,
                Token::RightParenthesis,
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

// `group "foo" agg [(sum "bar"), (mean "bar")]`

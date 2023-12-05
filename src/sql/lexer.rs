use chumsky::prelude::*;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    By,
    Stat(StatKeyword),
    AggOperator(AggOperator),
    Alias,
    Col,
    Exclude,
    Conditional(Conditional),
    Type(Type),
    Cast,
    StringFunctor(StringFunctor),
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
    Abs,
    Variable(String),
    Literal(Literal),
}

pub fn lexer<'a>() -> impl Parser<'a, &'a str, Vec<Token>, extra::Err<Rich<'a, char>>> + Clone {
    recursive(|tokens| {
        let by = text::keyword("by").to(Token::By);

        let alias = text::keyword("alias").to(Token::Alias);
        let col = text::keyword("col").to(Token::Col);
        let exclude = text::keyword("exclude").to(Token::Exclude);
        let cast = text::keyword("cast").to(Token::Cast);
        let abs = just("abs").to(Token::Abs);
        let functor = choice((alias, col, exclude, cast, abs));

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

        let ident = text::ident().map(ToString::to_string).map(Token::Variable);

        let token = choice((
            by,
            stat_keyword().map(Token::Stat),
            agg_operator().map(Token::AggOperator),
            type_keyword().map(Token::Type),
            conditional().map(Token::Conditional),
            functor,
            string_functor().map(Token::StringFunctor),
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
            literal().map(Token::Literal),
            ident,
        ));
        token.padded().repeated().collect()
    })
    .boxed()
}

#[derive(Debug, Clone, PartialEq)]
pub enum StatKeyword {
    Select,
    GroupBy,
    Agg,
    Filter,
    Limit,
    Reverse,
    Sort,
    Describe,
    Join,
    On,
    Left,
    Right,
    Inner,
    Outer,
}

fn stat_keyword<'a>() -> impl Parser<'a, &'a str, StatKeyword, extra::Err<Rich<'a, char>>> + Clone {
    let select = text::keyword("select").to(StatKeyword::Select);
    let group_by = text::keyword("group").to(StatKeyword::GroupBy);
    let agg = text::keyword("agg").to(StatKeyword::Agg);
    let filter = text::keyword("filter").to(StatKeyword::Filter);
    let limit = text::keyword("limit").to(StatKeyword::Limit);
    let reverse = text::keyword("reverse").to(StatKeyword::Reverse);
    let sort = text::keyword("sort").to(StatKeyword::Sort);
    let describe = text::keyword("describe").to(StatKeyword::Describe);
    let join = text::keyword("join").to(StatKeyword::Join);
    let on = text::keyword("on").to(StatKeyword::On);
    let outer = text::keyword("outer").to(StatKeyword::Outer);
    let inner = text::keyword("inner").to(StatKeyword::Inner);
    let left = text::keyword("left").to(StatKeyword::Left);
    let right = text::keyword("right").to(StatKeyword::Right);
    let join_type = choice((outer, inner, left, right));
    choice((
        select, group_by, agg, filter, limit, reverse, sort, describe, join, on, join_type,
    ))
    .boxed()
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggOperator {
    Sum,
    Count,
    First,
    Last,
    Sort,
    Reverse,
    Mean,
    Median,
}

fn agg_operator<'a>() -> impl Parser<'a, &'a str, AggOperator, extra::Err<Rich<'a, char>>> + Clone {
    let sum = text::keyword("sum").to(AggOperator::Sum);
    let count = text::keyword("count").to(AggOperator::Count);
    let sort = text::keyword("col_sort").to(AggOperator::Sort);
    let reverse = text::keyword("col_reverse").to(AggOperator::Sort);
    let first = text::keyword("first").to(AggOperator::First);
    let last = text::keyword("last").to(AggOperator::Last);
    let mean = text::keyword("mean").to(AggOperator::Mean);
    let median = text::keyword("median").to(AggOperator::Median);
    choice((sum, count, sort, reverse, first, last, mean, median)).boxed()
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Int(String),
    Float(String),
    Bool(bool),
    Null,
}

fn literal<'a>() -> impl Parser<'a, &'a str, Literal, extra::Err<Rich<'a, char>>> + Clone {
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
    let null = text::keyword("null").to(Literal::Null);
    choice((pos_float, pos_int, bool, null, string)).boxed()
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
    Str,
    Int,
    Float,
}

fn type_keyword<'a>() -> impl Parser<'a, &'a str, Type, extra::Err<Rich<'a, char>>> + Clone {
    let str = text::keyword("str").to(Type::Str);
    let int = text::keyword("int").to(Type::Int);
    let float = text::keyword("float").to(Type::Float);
    choice((str, int, float)).boxed()
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Conditional {
    If,
    Then,
    Else,
}

fn conditional<'a>() -> impl Parser<'a, &'a str, Conditional, extra::Err<Rich<'a, char>>> + Clone {
    let when = text::keyword("if").to(Conditional::If);
    let then = text::keyword("then").to(Conditional::Then);
    let otherwise = text::keyword("else").to(Conditional::Else);
    choice((when, then, otherwise)).boxed()
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StringFunctor {
    Contains,
}

fn string_functor<'a>(
) -> impl Parser<'a, &'a str, StringFunctor, extra::Err<Rich<'a, char>>> + Clone {
    let contains = text::keyword("contains").to(StringFunctor::Contains);
    choice((contains,)).boxed()
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
                Token::Stat(StatKeyword::Select),
                Token::Stat(StatKeyword::GroupBy),
                Token::Stat(StatKeyword::Agg),
                Token::AggOperator(AggOperator::Sum),
                Token::AggOperator(AggOperator::Count),
                Token::Stat(StatKeyword::Filter),
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

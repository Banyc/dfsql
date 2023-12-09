use chumsky::prelude::*;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Stat(StatKeyword),
    Conditional(Conditional),
    Type(Type),
    ExprKeyword(ExprKeyword),
    StringFunctor(StringFunctor),
    Parens(Vec<Token>),
    Brackets(Vec<Token>),
    Variable(String),
    Literal(Literal),
    Symbol(Symbol),
}

pub fn lexer<'a>() -> impl Parser<'a, &'a str, Vec<Token>, extra::Err<Rich<'a, char>>> + Clone {
    recursive(|tokens| {
        let parens = tokens
            .clone()
            .delimited_by(just('(').padded(), just(')').padded())
            .map(Token::Parens);
        let brackets = tokens
            .clone()
            .delimited_by(just('[').padded(), just(']').padded())
            .map(Token::Brackets);
        let group = choice((parens, brackets));

        let ident = text::ident().map(ToString::to_string).map(Token::Variable);

        let token = choice((
            stat_keyword().map(Token::Stat),
            expr_keyword().map(Token::ExprKeyword),
            type_keyword().map(Token::Type),
            conditional().map(Token::Conditional),
            string_functor().map(Token::StringFunctor),
            group,
            symbol().map(Token::Symbol),
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
pub enum Symbol {
    Bang,
    LeftAngle,
    RightAngle,
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Ampersand,
    Pipe,
    Comma,
}

fn symbol<'a>() -> impl Parser<'a, &'a str, Symbol, extra::Err<Rich<'a, char>>> + Clone {
    let left_angle = just('<').to(Symbol::LeftAngle);
    let right_angle = just('>').to(Symbol::RightAngle);
    let add = just('+').to(Symbol::Add);
    let sub = just('-').to(Symbol::Sub);
    let mul = just('*').to(Symbol::Mul);
    let div = just('/').to(Symbol::Div);
    let eq = just('=').to(Symbol::Eq);
    let ampersand = just('&').to(Symbol::Ampersand);
    let pipe = just('|').to(Symbol::Pipe);
    let comma = just(',').to(Symbol::Comma);
    let bang = just('!').to(Symbol::Bang);
    choice((
        left_angle,
        right_angle,
        add,
        sub,
        mul,
        div,
        eq,
        ampersand,
        pipe,
        comma,
        bang,
    ))
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExprKeyword {
    Sum,
    Count,
    First,
    Last,
    Sort,
    Asc,
    Desc,
    Reverse,
    Mean,
    Median,
    Abs,
    Unique,
    By,
    Is,
    Alias,
    Col,
    Exclude,
    Cast,
    Nan,
    All,
    Any,
}

fn expr_keyword<'a>() -> impl Parser<'a, &'a str, ExprKeyword, extra::Err<Rich<'a, char>>> + Clone {
    let sum = text::keyword("sum").to(ExprKeyword::Sum);
    let count = text::keyword("count").to(ExprKeyword::Count);
    let sort = text::keyword("col_sort").to(ExprKeyword::Sort);
    let asc = text::keyword("asc").to(ExprKeyword::Asc);
    let desc = text::keyword("desc").to(ExprKeyword::Desc);
    let reverse = text::keyword("col_reverse").to(ExprKeyword::Sort);
    let first = text::keyword("first").to(ExprKeyword::First);
    let last = text::keyword("last").to(ExprKeyword::Last);
    let mean = text::keyword("mean").to(ExprKeyword::Mean);
    let median = text::keyword("median").to(ExprKeyword::Median);
    let abs = text::keyword("abs").to(ExprKeyword::Abs);
    let unique = text::keyword("unique").to(ExprKeyword::Unique);
    let by = text::keyword("by").to(ExprKeyword::By);
    let is = text::keyword("is").to(ExprKeyword::Is);
    let alias = text::keyword("alias").to(ExprKeyword::Alias);
    let col = text::keyword("col").to(ExprKeyword::Col);
    let exclude = text::keyword("exclude").to(ExprKeyword::Exclude);
    let cast = text::keyword("cast").to(ExprKeyword::Cast);
    let nan = text::keyword("nan").to(ExprKeyword::Nan);
    let all = text::keyword("all").to(ExprKeyword::All);
    let any = text::keyword("any").to(ExprKeyword::Any);
    choice((
        sum, count, sort, asc, desc, reverse, first, last, mean, median, abs, unique, by, is,
        alias, col, exclude, cast, nan, all, any,
    ))
    .boxed()
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
    Extract,
    All,
}

fn string_functor<'a>(
) -> impl Parser<'a, &'a str, StringFunctor, extra::Err<Rich<'a, char>>> + Clone {
    let contains = text::keyword("contains").to(StringFunctor::Contains);
    let extract = text::keyword("extract").to(StringFunctor::Extract);
    let all = text::keyword("all").to(StringFunctor::All);
    choice((contains, extract, all)).boxed()
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
                Token::ExprKeyword(ExprKeyword::Sum),
                Token::ExprKeyword(ExprKeyword::Count),
                Token::Stat(StatKeyword::Filter),
                Token::ExprKeyword(ExprKeyword::Alias),
                Token::ExprKeyword(ExprKeyword::Col),
                Token::Brackets(vec![]),
                Token::Parens(vec![]),
                Token::Symbol(Symbol::LeftAngle),
                Token::Symbol(Symbol::RightAngle),
                Token::Variable(String::from("hi")),
                Token::Symbol(Symbol::Add),
                Token::Symbol(Symbol::Sub),
                Token::Symbol(Symbol::Mul),
                Token::Symbol(Symbol::Div),
                Token::Symbol(Symbol::Eq),
                Token::Symbol(Symbol::Sub),
                Token::Literal(Literal::Int(String::from("42"))),
                Token::Literal(Literal::Float(String::from("0.1"))),
                Token::Literal(Literal::String(String::from("hi"))),
            ]
        );
    }
}

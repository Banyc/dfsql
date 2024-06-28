use chumsky::prelude::*;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Stat(StatKeyword),
    Conditional(Conditional),
    Type(Type),
    ExprKeyword(ExprKeyword),
    StringKeyword(StringKeyword),
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
            string_functor().map(Token::StringKeyword),
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
    Join,
    On,
    Left,
    Right,
    Inner,
    Full,
    Use,
}
fn stat_keyword<'a>() -> impl Parser<'a, &'a str, StatKeyword, extra::Err<Rich<'a, char>>> + Clone {
    let select = text::keyword("select").to(StatKeyword::Select);
    let group_by = text::keyword("group").to(StatKeyword::GroupBy);
    let agg = text::keyword("agg").to(StatKeyword::Agg);
    let filter = text::keyword("filter").to(StatKeyword::Filter);
    let limit = text::keyword("limit").to(StatKeyword::Limit);
    let reverse = text::keyword("reverse").to(StatKeyword::Reverse);
    let sort = text::keyword("sort").to(StatKeyword::Sort);
    let join = text::keyword("join").to(StatKeyword::Join);
    let on = text::keyword("on").to(StatKeyword::On);
    let full = text::keyword("full").to(StatKeyword::Full);
    let inner = text::keyword("inner").to(StatKeyword::Inner);
    let left = text::keyword("left").to(StatKeyword::Left);
    let right = text::keyword("right").to(StatKeyword::Right);
    let r#use = text::keyword("use").to(StatKeyword::Use);
    let join_type = choice((full, inner, left, right));
    choice((
        select, group_by, agg, filter, limit, reverse, sort, join, on, join_type, r#use,
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
    Percent,
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
    let percent = just('%').to(Symbol::Percent);
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
        percent,
    ))
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExprKeyword {
    Sum,
    Sqrt,
    Count,
    Len,
    First,
    Last,
    Sort,
    Asc,
    Desc,
    Reverse,
    Mean,
    Median,
    Max,
    Min,
    Var,
    Std,
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
    Pow,
    Log,
}
fn expr_keyword<'a>() -> impl Parser<'a, &'a str, ExprKeyword, extra::Err<Rich<'a, char>>> + Clone {
    let sum = text::keyword("sum").to(ExprKeyword::Sum);
    let sqrt = text::keyword("sqrt").to(ExprKeyword::Sqrt);
    let count = text::keyword("count").to(ExprKeyword::Count);
    let len = text::keyword("len").to(ExprKeyword::Len);
    let sort = text::keyword("col_sort").to(ExprKeyword::Sort);
    let asc = text::keyword("asc").to(ExprKeyword::Asc);
    let desc = text::keyword("desc").to(ExprKeyword::Desc);
    let reverse = text::keyword("col_reverse").to(ExprKeyword::Sort);
    let first = text::keyword("first").to(ExprKeyword::First);
    let last = text::keyword("last").to(ExprKeyword::Last);
    let mean = text::keyword("mean").to(ExprKeyword::Mean);
    let median = text::keyword("median").to(ExprKeyword::Median);
    let max = text::keyword("max").to(ExprKeyword::Max);
    let min = text::keyword("min").to(ExprKeyword::Min);
    let var = text::keyword("var").to(ExprKeyword::Var);
    let std = text::keyword("std").to(ExprKeyword::Std);
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
    let pow = text::keyword("pow").to(ExprKeyword::Pow);
    let log = text::keyword("log").to(ExprKeyword::Log);
    let a = choice((
        sum, sqrt, count, len, sort, asc, desc, reverse, first, last, mean, median, max, min, var,
        std, abs, unique, by, is, alias, col, exclude, cast, nan, all,
    ));
    let b = choice((any, pow, log));
    choice((a, b)).boxed()
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
    UInt,
    Int,
    Float,
}
fn type_keyword<'a>() -> impl Parser<'a, &'a str, Type, extra::Err<Rich<'a, char>>> + Clone {
    let str = text::keyword("str").to(Type::Str);
    let uint = text::keyword("uint").to(Type::UInt);
    let int = text::keyword("int").to(Type::Int);
    let float = text::keyword("float").to(Type::Float);
    choice((str, uint, int, float)).boxed()
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
pub enum StringKeyword {
    Contains,
    Extract,
    All,
    Split,
}
fn string_functor<'a>(
) -> impl Parser<'a, &'a str, StringKeyword, extra::Err<Rich<'a, char>>> + Clone {
    let contains = text::keyword("contains").to(StringKeyword::Contains);
    let extract = text::keyword("extract").to(StringKeyword::Extract);
    let all = text::keyword("all").to(StringKeyword::All);
    let split = text::keyword("split").to(StringKeyword::Split);
    choice((contains, extract, all, split)).boxed()
}

/// Ref: <https://github.com/zesterer/chumsky/blob/dce5918bd2dad591ab399d2e191254640a9ed14f/examples/json.rs#L64>
fn string<'a>() -> impl Parser<'a, &'a str, String, extra::Err<Rich<'a, char>>> + Clone {
    let escaped = just('\\')
        .ignore_then(choice((
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
        .boxed();

    choice((escaped, none_of('"')))
        .repeated()
        .collect::<String>()
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

    #[test]
    fn test_string() {
        let s = string();
        let s = s.parse(r#""\\""#).unwrap();
        assert_eq!(s, r#"\"#);

        let src = r#" "\"" "#;
        let l = lexer();
        let tokens = l.parse(src).unwrap();
        assert_eq!(tokens.len(), 1);
        let Token::Literal(Literal::String(s)) = &tokens[0] else {
            panic!();
        };
        assert_eq!(s, r#"""#);

        let src = r#" "\\." "#;
        let l = lexer();
        let tokens = l.parse(src).unwrap();
        assert_eq!(tokens.len(), 1);
        let Token::Literal(Literal::String(s)) = &tokens[0] else {
            panic!();
        };
        assert_eq!(s, r#"\."#);
    }
}

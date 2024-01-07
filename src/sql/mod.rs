use chumsky::prelude::*;
use thiserror::Error;

use self::{
    lexer::{lexer, Literal, Token},
    stat::{parser, Stat},
};

pub mod expr;
pub mod lexer;
pub mod stat;

pub fn parse(src: &str) -> Result<S, ParseError> {
    let lexer = lexer();
    let tokens = lexer
        .parse(src)
        .into_result()
        .map_err(|e| format!("{e:?}"))
        .map_err(ParseError::Lexer)?;
    let parser = parser();
    let ast = parser
        .parse(&tokens)
        .into_result()
        .map_err(|e| format!("{e:?}"))
        .map_err(ParseError::Parser)?;
    Ok(ast)
}
#[derive(Debug, Error, Clone)]
pub enum ParseError {
    #[error("{0}")]
    Lexer(String),
    #[error("{0}")]
    Parser(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct S {
    pub statements: Vec<Stat>,
}

fn string_token<'a>() -> impl Parser<'a, &'a [Token], String, extra::Err<Rich<'a, Token>>> + Clone {
    select_ref! { Token::Literal(Literal::String(name)) => name.clone() }
}

fn variable_token<'a>() -> impl Parser<'a, &'a [Token], String, extra::Err<Rich<'a, Token>>> + Clone
{
    select_ref! { Token::Variable(name) => name.clone() }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

fn sort_order<'a>() -> impl Parser<'a, &'a [Token], SortOrder, extra::Err<Rich<'a, Token>>> + Clone
{
    let empty = empty().to(SortOrder::Asc);
    let asc = just(Token::ExprKeyword(lexer::ExprKeyword::Asc)).to(SortOrder::Asc);
    let desc = just(Token::ExprKeyword(lexer::ExprKeyword::Desc)).to(SortOrder::Desc);
    choice((asc, desc, empty))
}

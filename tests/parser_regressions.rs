use dfsql::sql::{
    ParseError, SortOrder,
    expr::{Expr, StrExpr},
    lexer::{Literal, Token, lex},
    parse,
    stat::{SelectStat, Stat},
};

#[rustfmt::skip]
#[test]
fn parser_regressions() {
    let parsed = parse("select col_sort value").unwrap();
    assert!(matches!(parsed.statements.as_slice(), [Stat::Select(SelectStat { columns })] if matches!(columns.as_slice(), [Expr::Sort(_)])));

    assert!(matches!(parse("select (a b)"), Err(ParseError::Parser(message)) if message.contains("column 11")));
    assert!(matches!(parse("SELECT foo LIMIT"), Err(ParseError::Parser(message)) if message.contains("column 17")));
    assert!(matches!(parse(r#"select "aln" LIMIT"#), Err(ParseError::Parser(message)) if message.contains("column 19")));
    assert!(matches!(parse("select (a) LIMIT"), Err(ParseError::Parser(message)) if message.contains("column 17")));
}

#[test]
fn parser_recognizes_literal_keywords_before_expression_keywords() {
    assert_eq!(
        lex("true false null").unwrap(),
        vec![
            Token::Literal(Literal::Bool(true)),
            Token::Literal(Literal::Bool(false)),
            Token::Literal(Literal::Null),
        ]
    );
}

#[test]
fn parser_accepts_descending_sort_statement_pairs() {
    let parsed = parse("sort desc value asc other").unwrap();
    assert!(matches!(parsed.statements.as_slice(),
        [Stat::Sort(stat)] if stat.pairs == vec![
            (SortOrder::Desc, "value".into()),
            (SortOrder::Asc, "other".into()),
        ]
    ));
}

#[test]
fn lexer_accepts_regex_backslash_in_string() {
    // Regression: \d, \., etc in regex patterns triggered Backtrack error
    assert!(lex(r#""\d""#).is_ok());
    assert!(lex(r#""\.""#).is_ok());
    assert!(lex(r#""\+""#).is_ok());
    assert!(lex(r#""([^.]+\.[^.\d]+|\d+\.\d+\.\d+\.\d+)$""#).is_ok());
}

#[test]
fn parser_accepts_extract_all_after_lexer_classifies_all_as_expression_keyword() {
    let parsed = parse(r#"select extract all "[a-z]+" col text"#).unwrap();
    assert!(matches!(parsed.statements.as_slice(),
        [Stat::Select(stat)] if matches!(stat.columns.as_slice(),
            [Expr::Str(value)] if matches!(value.as_ref(),
                StrExpr::ExtractAll(_)
            )
        )
    ));
}

#[test]
fn top_level_parser_errors_point_to_the_furthest_token() {
    assert!(
        matches!(parse("select value +"), Err(ParseError::Parser(message)) if message.contains("line 1, column 15") && message.contains("unexpected end of input"))
    );
}

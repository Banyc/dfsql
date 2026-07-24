use dfsql::sql::{
    ParseError,
    expr::Expr,
    parse,
    stat::{SelectStat, Stat},
};

#[rustfmt::skip]
#[test]
fn parser_regressions() {
    let parsed = parse("select col_sort value").unwrap();
    assert!(matches!(parsed.statements.as_slice(), [Stat::Select(SelectStat { columns })] if matches!(columns.as_slice(), [Expr::Sort(_)])));

    assert!(matches!(parse("select (a b)"), Err(ParseError::Parser(message)) if message.contains("column 11")));
    assert!(matches!(parse("SELECT foo LIMIT"), Err(ParseError::Parser(message)) if message.contains("column 12")));
    assert!(matches!(parse(r#"select "a\n" Limit"#), Err(ParseError::Parser(message)) if message.contains("column 14")));
    assert!(matches!(parse("select (a) limit"), Err(ParseError::Parser(message)) if message.contains("column 12")));
}

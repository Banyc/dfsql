use std::collections::HashSet;

use crate::sql::lexer::{
    CONDITIONAL_KEYWORDS, EXPR_KEYWORDS, LITERAL_KEYWORDS, STAT_KEYWORDS, STRING_KEYWORDS,
    TYPE_KEYWORDS,
};
use rustyline::{Completer, Helper, Hinter, Validator, highlight::Highlighter};

use super::terminal_color::{KeywordColor, TerminalColor, TerminalKeywordHighlighter};

#[derive(Debug, Helper, Completer, Hinter, Validator)]
pub struct SqlHelper {
    color: TerminalKeywordHighlighter,
}
impl SqlHelper {
    pub fn new() -> Self {
        let mut seen = HashSet::new();
        let rules = STAT_KEYWORDS
            .iter()
            .map(|(keyword, _)| (*keyword, color_keyword()))
            .chain(
                EXPR_KEYWORDS
                    .iter()
                    .map(|(keyword, _)| (*keyword, color_functor())),
            )
            .chain(
                STRING_KEYWORDS
                    .iter()
                    .map(|(keyword, _)| (*keyword, color_functor())),
            )
            .chain(
                LITERAL_KEYWORDS
                    .iter()
                    .map(|(keyword, _)| (*keyword, color_functor())),
            )
            .chain(
                CONDITIONAL_KEYWORDS
                    .iter()
                    .map(|(keyword, _)| (*keyword, color_control_flow())),
            )
            .chain(
                TYPE_KEYWORDS
                    .iter()
                    .map(|(keyword, _)| (*keyword, color_type())),
            )
            .filter(move |(keyword, _)| seen.insert(*keyword))
            .map(|(keyword, color)| KeywordColor {
                keyword: keyword.to_owned(),
                color,
            });
        let color = TerminalKeywordHighlighter::new(rules);
        Self { color }
    }
}
impl Default for SqlHelper {
    fn default() -> Self {
        Self::new()
    }
}
impl Highlighter for SqlHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> std::borrow::Cow<'l, str> {
        self.color.replace(line).into()
    }

    fn highlight_char(
        &self,
        _line: &str,
        _pos: usize,
        _forced: rustyline::highlight::CmdKind,
    ) -> bool {
        true
    }
}

const fn color_functor() -> TerminalColor {
    TerminalColor::Yellow
}

const fn color_keyword() -> TerminalColor {
    TerminalColor::Blue
}

const fn color_control_flow() -> TerminalColor {
    TerminalColor::Magenta
}

const fn color_type() -> TerminalColor {
    TerminalColor::Green
}

#[rustfmt::skip]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn highlighter_follows_lexer_boundaries_and_strings() {
        let helper = SqlHelper::new();
        assert_eq!(helper.highlight(r#"SELECT,sum*value "(select)" selected"#, 0), "\x1b[1;34mSELECT\x1b[0m,\x1b[1;33msum\x1b[0m*value \"(select)\" selected");
    }

    #[test]
    fn sql_helper_uses_the_lexer_keyword_inventory() {
        let helper = SqlHelper::new();
        for keyword in STAT_KEYWORDS.iter().map(|(keyword, _)| *keyword)
            .chain(EXPR_KEYWORDS.iter().map(|(keyword, _)| *keyword))
            .chain(STRING_KEYWORDS.iter().map(|(keyword, _)| *keyword))
            .chain(LITERAL_KEYWORDS.iter().map(|(keyword, _)| *keyword))
            .chain(CONDITIONAL_KEYWORDS.iter().map(|(keyword, _)| *keyword))
            .chain(TYPE_KEYWORDS.iter().map(|(keyword, _)| *keyword))
        {
            assert_ne!(helper.highlight(keyword, 0), keyword, "{keyword} was not highlighted");
            let uppercase = keyword.to_ascii_uppercase();
            assert_ne!(helper.highlight(&uppercase, 0), uppercase, "{} was not highlighted", uppercase);
        }
        assert_eq!(helper.highlight("describe", 0), "describe");
    }
}

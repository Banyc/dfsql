use std::{borrow::Cow, collections::HashSet};

use crate::sql::lexer::{
    CONDITIONAL_KEYWORDS, EXPR_KEYWORDS, LITERAL_KEYWORDS, STAT_KEYWORDS, STRING_KEYWORDS,
    TYPE_KEYWORDS,
};
use fancy_regex::Regex;
use rustyline::{Completer, Helper, Hinter, Validator, highlight::Highlighter};

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

#[derive(Debug)]
pub struct TerminalKeywordHighlighter {
    rules: Vec<(KeywordColor, Regex)>,
}
impl TerminalKeywordHighlighter {
    pub fn new(keyword_color_pairs: impl Iterator<Item = KeywordColor>) -> Self {
        let rules = keyword_color_pairs
            .map(|pair| {
                let keyword = regex::escape(&pair.keyword);
                let pattern = format!(r"(?<=\s|^|\(|\))({})(?=\s|$|\(|\)|\+|/)", keyword);
                let regex = Regex::new(&pattern).unwrap();
                (pair, regex)
            })
            .collect();
        Self { rules }
    }

    pub fn replace(&self, string: &str) -> String {
        let mut string: Cow<str> = string.into();
        for (pair, regex) in &self.rules {
            let replacer = format!(
                "\x1b[1;{color}m{keyword}\x1b[0m",
                color = pair.color.code(),
                keyword = "$1"
            );
            string = regex.replace_all(&string, replacer).to_string().into();
        }
        string.into()
    }
}

#[derive(Debug)]
pub struct KeywordColor {
    pub keyword: String,
    pub color: TerminalColor,
}

#[derive(Debug)]
pub enum TerminalColor {
    Green,
    Yellow,
    Blue,
    Magenta,
}
impl TerminalColor {
    pub fn code(&self) -> usize {
        match self {
            TerminalColor::Green => 32,
            TerminalColor::Yellow => 33,
            TerminalColor::Blue => 34,
            TerminalColor::Magenta => 35,
        }
    }
}

#[rustfmt::skip]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn highlighter_treats_public_keywords_as_literals() {
        let highlighter = TerminalKeywordHighlighter::new([KeywordColor { keyword: "a+b".into(), color: TerminalColor::Yellow }].into_iter());
        assert_eq!(highlighter.replace("a+b ab"), "\x1b[1;33ma+b\x1b[0m ab");
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
        }
        assert_eq!(helper.highlight("describe", 0), "describe");
    }
}

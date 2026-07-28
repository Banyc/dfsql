use std::{collections::HashSet, fmt::Write};

use crate::sql::lexer::{
    CONDITIONAL_KEYWORDS, EXPR_KEYWORDS, LITERAL_KEYWORDS, STAT_KEYWORDS, STRING_KEYWORDS,
    TYPE_KEYWORDS,
};
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
    rules: Vec<KeywordColor>,
}
impl TerminalKeywordHighlighter {
    pub fn new(keyword_color_pairs: impl Iterator<Item = KeywordColor>) -> Self {
        let rules = keyword_color_pairs
            .filter(|pair| !pair.keyword.is_empty())
            .collect();
        Self { rules }
    }

    pub fn replace(&self, string: &str) -> String {
        let mut output = String::with_capacity(string.len());
        let mut offset = 0;
        let mut quoted = false;
        let mut escaped = false;
        while let Some(character) = string[offset..].chars().next() {
            if quoted {
                output.push(character);
                offset += character.len_utf8();
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == '"' {
                    quoted = false;
                }
            } else if character == '"' {
                quoted = true;
                output.push(character);
                offset += 1;
            } else if let Some((rule, matched)) = self
                .rules
                .iter()
                .find_map(|rule| match_rule(string, offset, rule).map(|matched| (rule, matched)))
            {
                write!(output, "\x1b[1;{}m{}\x1b[0m", rule.color.code(), matched).unwrap();
                offset += matched.len();
            } else {
                output.push(character);
                offset += character.len_utf8();
            }
        }
        output
    }
}

fn match_rule<'a>(input: &'a str, offset: usize, rule: &KeywordColor) -> Option<&'a str> {
    let end = offset.checked_add(rule.keyword.len())?;
    let matched = input.get(offset..end)?;
    if !matched.eq_ignore_ascii_case(&rule.keyword)
        || !identifier_boundary(
            input[..offset].chars().next_back(),
            rule.keyword.chars().next(),
        )
        || !identifier_boundary(
            input[end..].chars().next(),
            rule.keyword.chars().next_back(),
        )
    {
        return None;
    }
    Some(matched)
}

fn identifier_boundary(neighbor: Option<char>, edge: Option<char>) -> bool {
    !edge.is_some_and(identifier_char) || !neighbor.is_some_and(identifier_char)
}

fn identifier_char(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

#[derive(Debug)]
pub struct KeywordColor {
    pub keyword: String,
    pub color: TerminalColor,
}

#[derive(Clone, Copy, Debug)]
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

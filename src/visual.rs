use std::borrow::Cow;

use fancy_regex::Regex;
use rustyline::{highlight::Highlighter, Completer, Helper, Hinter, Validator};

#[derive(Debug, Helper, Completer, Hinter, Validator)]
pub struct SqlHelper {
    color: TerminalKeywordHighlighter,
}
impl SqlHelper {
    pub fn new() -> Self {
        let rules = [
            ("select", color_keyword()),
            ("group", color_keyword()),
            ("agg", color_keyword()),
            ("filter", color_keyword()),
            ("limit", color_keyword()),
            ("reverse", color_keyword()),
            ("sort", color_keyword()),
            ("describe", color_keyword()),
            ("join", color_keyword()),
            ("on", color_keyword()),
            ("left", color_keyword()),
            ("right", color_keyword()),
            ("inner", color_keyword()),
            ("outer", color_keyword()),
            ("abs", color_functor()),
            ("sum", color_functor()),
            ("sqrt", color_functor()),
            ("count", color_functor()),
            ("col_sort", color_functor()),
            ("asc", color_functor()),
            ("desc", color_functor()),
            ("col_reverse", color_functor()),
            ("mean", color_functor()),
            ("median", color_functor()),
            ("max", color_functor()),
            ("min", color_functor()),
            ("var", color_functor()),
            ("std", color_functor()),
            ("first", color_functor()),
            ("last", color_functor()),
            ("by", color_functor()),
            ("is", color_functor()),
            ("alias", color_functor()),
            ("col", color_functor()),
            ("exclude", color_functor()),
            ("cast", color_functor()),
            ("contains", color_functor()),
            ("extract", color_functor()),
            ("all", color_functor()),
            ("split", color_functor()),
            ("unique", color_functor()),
            ("nan", color_functor()),
            ("all", color_functor()),
            ("any", color_functor()),
            ("pow", color_functor()),
            ("log", color_functor()),
            ("if", color_control_flow()),
            ("then", color_control_flow()),
            ("else", color_control_flow()),
            ("str", color_type()),
            ("int", color_type()),
            ("float", color_type()),
        ];
        let rules = rules.into_iter().map(|(keyword, color)| KeywordColor {
            keyword: keyword.to_string(),
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

    fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
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
                let pattern = format!(
                    "(?<=\\s|^|\\()({keyword})(?=\\s|$|\\))",
                    keyword = pair.keyword
                );
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

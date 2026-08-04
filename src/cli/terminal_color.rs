use std::fmt::Write;

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
        assert_eq!(highlighter.replace("A+B ab"), "\x1b[1;33mA+B\x1b[0m ab");
    }
}

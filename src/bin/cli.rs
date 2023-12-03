use std::{
    borrow::Cow,
    io::Write,
    path::{Path, PathBuf},
};

use clap::Parser;
use fancy_regex::Regex;
use handler::{HandleLineResult, LineHandler};
use polars::{
    frame::DataFrame,
    io::{csv::CsvWriter, SerWriter},
    lazy::frame::{LazyCsvReader, LazyFileListReader, LazyFrame},
};
use rustyline::{
    error::ReadlineError, highlight::Highlighter, Completer, Editor, Helper, Hinter, Validator,
};

#[derive(Debug, Parser)]
pub struct Cli {
    /// Input file containing a data frame
    #[clap(short, long)]
    input: PathBuf,
    /// Output file storing the modified data frame
    #[clap(short, long)]
    output: Option<PathBuf>,
    /// Output file storing the effective SQL statements
    #[clap(short, long)]
    sql_output: Option<PathBuf>,
    /// Evaluate the data frame for every input line
    #[clap(short, long, default_value_t = false)]
    eager: bool,
}

impl Cli {
    pub fn run(self) -> anyhow::Result<()> {
        let write_repl_output = |df: LazyFrame, handler: &LineHandler| -> anyhow::Result<()> {
            let df = df.collect()?;
            println!("{df}");
            if let Some(output) = &self.output {
                write_df_output(df.clone(), output)?;
            }
            if let Some(output) = &self.sql_output {
                write_sql_output(handler.history().iter(), output)?;
            }
            Ok(())
        };
        let mut df = LazyCsvReader::new(self.input).has_header(true).finish()?;
        let mut handler = LineHandler::new(df.clone());
        if self.eager {
            write_repl_output(df.clone(), &handler)?;
        }
        let mut rl = Editor::new()?;
        rl.set_helper(Some(SqlHelper::new()));
        loop {
            let line = rl.readline("> ");
            let line = match line {
                Ok(line) => line,
                Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => {
                    break;
                }
                Err(e) => {
                    eprintln!("{e}");
                    break;
                }
            };
            let _ = rl.add_history_entry(&line);
            df = match handler.handle_line(df.clone(), line) {
                Ok(HandleLineResult::Exit) => break,
                Ok(HandleLineResult::Updated(new)) => new,
                Ok(HandleLineResult::Continue) => continue,
                Ok(HandleLineResult::Schema(schema)) => {
                    println!("{schema:?}");
                    continue;
                }
                Err(e) => {
                    eprintln!("{e}");
                    break;
                }
            };
            if self.eager {
                if let Err(e) = write_repl_output(df.clone(), &handler) {
                    eprintln!("{e}");
                    // Rollback
                    df = match handler.handle_line(df, String::from("undo")) {
                        Ok(HandleLineResult::Updated(new)) => new,
                        _ => panic!(),
                    };
                }
            }
        }
        if !self.eager {
            write_repl_output(df, &handler)?;
        }
        Ok(())
    }
}

pub mod handler {
    use polars::{lazy::frame::LazyFrame, prelude::SchemaRef};
    use sql_repl::{df::apply, sql};

    pub struct LineHandler {
        history: Vec<String>,
        original_df: LazyFrame,
    }

    impl LineHandler {
        pub fn new(original_df: LazyFrame) -> Self {
            Self {
                history: vec![],
                original_df,
            }
        }

        pub fn handle_line(
            &mut self,
            df: LazyFrame,
            line: String,
        ) -> anyhow::Result<HandleLineResult> {
            let trimmed_line = line.trim();
            if trimmed_line == "exit" || trimmed_line == "quit" {
                return Ok(HandleLineResult::Exit);
            }
            if trimmed_line == "undo" || trimmed_line == "reset" {
                self.history.pop();
                if trimmed_line == "reset" {
                    self.history.clear();
                }
                if self.history.is_empty() {
                    return Ok(HandleLineResult::Updated(self.original_df.clone()));
                }
                let sql = self.history.iter().map(|s| sql::parse(s).unwrap());
                let df = apply_history(self.original_df.clone(), sql);
                return Ok(HandleLineResult::Updated(df));
            }
            if trimmed_line == "schema" {
                let schema = df.schema()?;
                return Ok(HandleLineResult::Schema(schema));
            }
            let Some(sql) = sql::parse(&line) else {
                return Ok(HandleLineResult::Continue);
            };
            let df = apply(df, &sql);
            if !trimmed_line.is_empty() {
                self.history.push(line);
            }
            Ok(HandleLineResult::Updated(df))
        }

        pub fn history(&self) -> &Vec<String> {
            &self.history
        }
    }

    pub enum HandleLineResult {
        Exit,
        Updated(LazyFrame),
        Continue,
        Schema(SchemaRef),
    }

    fn apply_history(df: LazyFrame, sql: impl Iterator<Item = sql::S>) -> LazyFrame {
        sql.fold(df, |df, sql| apply(df, &sql))
    }
}

fn write_df_output(mut df: DataFrame, path: impl AsRef<Path>) -> anyhow::Result<()> {
    let _ = std::fs::remove_file(&path);
    let output = std::fs::File::options()
        .write(true)
        .create(true)
        .open(path)?;
    CsvWriter::new(output).finish(&mut df)?;
    Ok(())
}

fn write_sql_output<'a>(
    sql: impl Iterator<Item = &'a String>,
    path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let _ = std::fs::remove_file(&path);
    let mut output = std::fs::File::options()
        .write(true)
        .create(true)
        .open(path)?;
    for s in sql {
        output.write_all(s.as_bytes())?;
        output.write_all("\n".as_bytes())?;
    }
    Ok(())
}

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
            ("sum", color_expr_functor()),
            ("count", color_expr_functor()),
            ("first", color_expr_functor()),
            ("last", color_expr_functor()),
            ("by", color_expr_functor()),
            ("alias", color_expr_functor()),
            ("col", color_expr_functor()),
            ("exclude", color_expr_functor()),
            ("cast", color_expr_functor()),
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

    fn highlight_char(&self, _line: &str, _pos: usize) -> bool {
        true
    }
}

fn color_expr_functor() -> TerminalColor {
    TerminalColor::Yellow
}

fn color_keyword() -> TerminalColor {
    TerminalColor::Blue
}

fn color_control_flow() -> TerminalColor {
    TerminalColor::Magenta
}

fn color_type() -> TerminalColor {
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

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.run()?;
    Ok(())
}

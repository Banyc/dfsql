use std::{
    borrow::Cow,
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use clap::Parser;
use fancy_regex::Regex;
use handler::{HandleLineResult, LineExecutor};
use polars::prelude::*;
use rustyline::{
    error::ReadlineError, highlight::Highlighter, history::History, Completer, Editor, Helper,
    Hinter, Validator,
};

const SQL_EXTENSION: &str = "dfsql";

#[derive(Debug, Parser)]
pub struct Cli {
    /// Input file containing a data frame
    #[clap(short, long)]
    input: PathBuf,
    /// Input files each containing a data frame labeled as a variable for join operations
    ///
    /// Format: `name,path`
    #[clap(short, long)]
    join: Vec<String>,
    /// Output file storing the modified data frame
    #[clap(short, long)]
    output: Option<PathBuf>,
    /// Only evaluate the data frame on exit
    #[clap(short, long, default_value_t = false)]
    lazy: bool,
    /// Set the number of rows to use when inferring the csv schema.
    #[clap(long, default_value_t = 100)]
    infer_schema_length: usize,
}

impl Cli {
    pub fn run(self) -> anyhow::Result<()> {
        let mut df = read_df_file(&self.input, self.infer_schema_length)?;
        let mut others = HashMap::new();
        for other in &self.join {
            let (name, path) = other.split_once(',').context("name,path")?;
            let df = read_df_file(path, self.infer_schema_length)?;
            others.insert(name.to_string(), df);
        }
        let mut handler = LineExecutor::new(df.clone(), others);
        let mut rl = Editor::new()?;
        if !self.lazy {
            let lines = if let Some(output) = &self.output {
                let mut output = output.clone();
                output.set_extension(SQL_EXTENSION);
                if let Ok(file) = std::fs::File::options().read(true).open(output) {
                    let mut reader = BufReader::new(file);
                    let mut lines = vec![];
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line)? == 0 {
                            // EOF
                            break;
                        }
                        if line.ends_with('\n') {
                            line.pop();
                        }
                        lines.push(line);
                    }
                    lines
                } else {
                    vec![]
                }
            } else {
                vec![]
            };

            self.write_repl_output(df.clone(), &handler)?;
            for line in lines {
                println!("> {line}");
                match self.handle_line(line, df.clone(), &mut handler, &mut rl) {
                    Ok(new) => df = new,
                    Err(_) => break,
                };
            }
        }
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
            match self.handle_line(line, df.clone(), &mut handler, &mut rl) {
                Ok(new) => df = new,
                Err(_) => break,
            };
        }
        if self.lazy {
            self.write_repl_output(df, &handler)?;
        }
        Ok(())
    }

    fn handle_line<H: Helper, I: History>(
        &self,
        line: String,
        df: LazyFrame,
        handler: &mut LineExecutor,
        rl: &mut Editor<H, I>,
    ) -> Result<LazyFrame, ()> {
        let _ = rl.add_history_entry(&line);
        let df = match handler.handle_line(df.clone(), line) {
            Ok(HandleLineResult::Exit) => return Err(()),
            Ok(HandleLineResult::Updated(new)) => new,
            Ok(HandleLineResult::Continue) => return Ok(df),
            Ok(HandleLineResult::Schema(schema)) => {
                println!("{schema:?}");
                return Ok(df);
            }
            Err(e) => {
                eprintln!("{e}");
                return Err(());
            }
        };
        if !self.lazy {
            if let Err(e) = self.write_repl_output(df.clone(), handler) {
                eprintln!("{e}");
                // Rollback
                let df = match handler.handle_line(df, String::from("undo")) {
                    Ok(HandleLineResult::Updated(new)) => new,
                    _ => panic!(),
                };
                return Ok(df);
            }
        }
        Ok(df)
    }

    fn write_repl_output(&self, df: LazyFrame, handler: &LineExecutor) -> anyhow::Result<()> {
        let df = df.collect()?;
        println!("{df}");
        if let Some(output) = &self.output {
            write_df_output(df.clone(), output)?;

            let mut output = output.clone();
            output.set_extension(SQL_EXTENSION);
            write_sql_output(handler.history().iter(), output)?;
        }
        Ok(())
    }
}

fn read_df_file(path: impl AsRef<Path>, infer_schema_length: usize) -> anyhow::Result<LazyFrame> {
    let Some(extension) = path.as_ref().extension() else {
        bail!(
            "No extension at the name of the file `{}`",
            path.as_ref().to_string_lossy()
        );
    };
    Ok(match extension.to_string_lossy().as_ref() {
        "csv" => LazyCsvReader::new(&path)
            .has_header(true)
            .with_infer_schema_length(Some(infer_schema_length))
            .finish()?,
        "json" => {
            let file = std::fs::File::options().read(true).open(&path)?;
            JsonReader::new(file).finish()?.lazy()
        }
        "ndjson" | "jsonl" => LazyJsonLineReader::new(&path)
            .with_infer_schema_length(Some(infer_schema_length))
            .finish()?,
        _ => bail!(
            "Unknown extension `{}` at the name of the file `{}`",
            extension.to_string_lossy(),
            path.as_ref().to_string_lossy()
        ),
    })
}

pub mod handler {
    use std::collections::HashMap;

    use dfsql::{
        df::{apply, ApplyStatError},
        sql,
    };
    use polars::{lazy::frame::LazyFrame, prelude::SchemaRef};

    pub struct LineExecutor {
        history: Vec<String>,
        original_df: LazyFrame,
        others: HashMap<String, LazyFrame>,
    }

    impl LineExecutor {
        pub fn new(original_df: LazyFrame, others: HashMap<String, LazyFrame>) -> Self {
            Self {
                history: vec![],
                original_df,
                others,
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
                let df = apply_history(self.original_df.clone(), sql, &self.others)?;
                return Ok(HandleLineResult::Updated(df));
            }
            if trimmed_line == "schema" {
                let schema = df.schema()?;
                return Ok(HandleLineResult::Schema(schema));
            }
            let Some(sql) = sql::parse(&line) else {
                return Ok(HandleLineResult::Continue);
            };
            let df = apply(df, &sql, &self.others)?;
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

    fn apply_history(
        df: LazyFrame,
        sql: impl Iterator<Item = sql::S>,
        others: &HashMap<String, LazyFrame>,
    ) -> Result<LazyFrame, ApplyStatError> {
        let mut df = df;
        for s in sql {
            df = apply(df, &s, others)?;
        }
        Ok(df)
    }
}

fn write_df_output(mut df: DataFrame, path: impl AsRef<Path>) -> anyhow::Result<()> {
    let Some(extension) = path.as_ref().extension() else {
        bail!(
            "No extension at the name of the file `{}`",
            path.as_ref().to_string_lossy()
        );
    };
    let _ = std::fs::remove_file(&path);
    let output = std::fs::File::options()
        .write(true)
        .create(true)
        .open(&path)?;
    match extension.to_string_lossy().as_ref() {
        "csv" => CsvWriter::new(output).finish(&mut df)?,
        "json" => JsonWriter::new(output).finish(&mut df)?,
        "ndjson" | "jsonl" => {
            bail!(
                "No `JsonLineWriter` available to write `{}`",
                path.as_ref().to_string_lossy()
            );
        }
        _ => bail!(
            "Unknown extension `{}` at the name of the file `{}`",
            extension.to_string_lossy(),
            path.as_ref().to_string_lossy()
        ),
    }
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
            ("join", color_keyword()),
            ("on", color_keyword()),
            ("left", color_keyword()),
            ("right", color_keyword()),
            ("inner", color_keyword()),
            ("outer", color_keyword()),
            ("abs", color_functor()),
            ("sum", color_functor()),
            ("count", color_functor()),
            ("col_sort", color_functor()),
            ("asc", color_functor()),
            ("desc", color_functor()),
            ("col_reverse", color_functor()),
            ("mean", color_functor()),
            ("median", color_functor()),
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
            ("unique", color_functor()),
            ("nan", color_functor()),
            ("all", color_functor()),
            ("any", color_functor()),
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

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.run()?;
    Ok(())
}

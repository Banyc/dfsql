use std::{
    io::Write,
    path::{Path, PathBuf},
};

use clap::Parser;
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
    #[clap(short, long)]
    input: PathBuf,
    #[clap(short, long)]
    output: Option<PathBuf>,
    #[clap(short, long)]
    sql_output: Option<PathBuf>,
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
                    handler.pop_history();
                    eprintln!("{e}");
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
            if trimmed_line == "undo" {
                self.history.pop();
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

        pub fn pop_history(&mut self) {
            self.history.pop();
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
pub struct SqlHelper {}
impl SqlHelper {
    pub fn new() -> Self {
        Self {}
    }
}
impl Default for SqlHelper {
    fn default() -> Self {
        Self::new()
    }
}
impl Highlighter for SqlHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> std::borrow::Cow<'l, str> {
        let line = color_keyword(line, "select");
        let line = color_keyword(&line, "group");
        let line = color_keyword(&line, "agg");
        let line = color_keyword(&line, "filter");
        let line = color_keyword(&line, "limit");
        let line = color_keyword(&line, "reverse");
        let line = color_expr(&line, "sum");
        let line = color_expr(&line, "count");
        let line = color_expr(&line, "alias");
        let line = color_expr(&line, "col");
        let line = color_expr(&line, "exclude");
        let line = color_control_flow(&line, "if");
        let line = color_control_flow(&line, "then");
        let line = color_control_flow(&line, "else");
        line.into()
    }

    fn highlight_char(&self, _line: &str, _pos: usize) -> bool {
        true
    }
}

fn color_expr(src: &str, keyword: &str) -> String {
    src.replace(keyword, &color_string_yellow(keyword))
}

fn color_keyword(src: &str, keyword: &str) -> String {
    src.replace(keyword, &color_string_blue(keyword))
}

fn color_control_flow(src: &str, keyword: &str) -> String {
    src.replace(keyword, &color_string_magenta(keyword))
}

fn color_string_yellow(string: &str) -> String {
    format!("\x1b[1;33m{string}\x1b[0m")
}

fn color_string_blue(string: &str) -> String {
    format!("\x1b[1;34m{string}\x1b[0m")
}

fn color_string_magenta(string: &str) -> String {
    format!("\x1b[1;35m{string}\x1b[0m")
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.run()?;
    Ok(())
}

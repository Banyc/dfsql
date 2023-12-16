use std::{collections::HashMap, path::PathBuf};

use anyhow::{bail, Context};
use clap::Parser;
use dfsql::{
    df::apply,
    io::{read_df_file, read_repl_sql_file, read_sql_file, write_df_output, write_repl_sql_output},
    visual::SqlHelper,
};
use handler::{HandleLineResult, LineExecutor};
use polars::prelude::*;
use rustyline::{error::ReadlineError, history::History, Editor, Helper};

const SQL_EXTENSION: &str = "dfsql";

#[derive(Debug, Parser)]
pub struct Cli {
    /// `.dfsql` file to execute
    sql: Option<PathBuf>,
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
    fn infer_schema_length(&self) -> Option<usize> {
        let lazy = self.sql.is_some() || self.lazy;
        match lazy {
            true => Some(self.infer_schema_length),
            false => None,
        }
    }

    pub fn run(self) -> anyhow::Result<()> {
        let mut df = read_df_file(&self.input, self.infer_schema_length())?;
        let mut others = HashMap::new();
        for other in &self.join {
            let (name, path) = other.split_once(',').context("name,path")?;
            let df = read_df_file(path, self.infer_schema_length())?;
            others.insert(name.to_string(), df);
        }
        if let Some(sql_file) = &self.sql {
            // Non-interactive mode
            if self.lazy {
                bail!(
                    "`lazy` option is unavailable if a `.{SQL_EXTENSION}` is provided via the argument `sql`"
                );
            }

            let s = read_sql_file(sql_file)?;
            let df = apply(df, &s, &others)?.collect()?;
            match &self.output {
                Some(output) => write_df_output(df, output)?,
                None => println!("{df}"),
            }
            return Ok(());
        }
        let mut handler = LineExecutor::new(df.clone(), others);
        let mut rl = Editor::new()?;
        if !self.lazy {
            let lines = if let Some(output) = &self.output {
                let mut output = output.clone();
                output.set_extension(SQL_EXTENSION);
                read_repl_sql_file(&output).ok().unwrap_or_default()
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
            write_repl_sql_output(handler.history().iter(), output)?;
        }
        Ok(())
    }
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

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.run()?;
    Ok(())
}

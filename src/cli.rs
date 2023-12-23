use std::{collections::HashMap, path::PathBuf};

use crate::{
    df::apply,
    handler::LineExecutor,
    io::{read_df_file, read_repl_sql_file, read_sql_file, write_df_output, write_repl_sql_output},
    visual::SqlHelper,
};
use anyhow::{bail, Context};
use clap::Parser;
use polars::prelude::*;
use rustyline::{error::ReadlineError, Editor};

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

            self.display_and_write_repl_output(df.clone(), &handler)?;
            for line in lines {
                println!("> {line}");
                let _ = rl.add_history_entry(&line);
                match handler.execute(df.clone(), line) {
                    Ok(new) => df = new,
                    Err(e) => {
                        eprintln!("{e}");
                        break;
                    }
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
            if line.trim() == "exit" || line.trim() == "quit" {
                break;
            }
            let _ = rl.add_history_entry(&line);

            if line.trim() == "schema" {
                match df.schema() {
                    Ok(schema) => println!("{schema:?}"),
                    Err(e) => eprintln!("{e}"),
                }
                continue;
            }
            if line.trim().starts_with("save") {
                let path = line
                    .trim()
                    .split_once(' ')
                    .and_then(|(cmd, path)| match cmd {
                        "save" => Some(path),
                        _ => None,
                    });
                let Some(path) = path else {
                    eprintln!("save <PATH>");
                    continue;
                };
                if let Err(e) = save(df.clone(), &handler, path) {
                    eprintln!("{e}");
                }
                continue;
            }
            match upgrade_df(line, df.clone(), &mut handler) {
                Ok(new) => df = new,
                Err(e) => {
                    eprintln!("{e}");
                    continue;
                }
            };
            if !self.lazy {
                if let Err(e) = self.display_and_write_repl_output(df.clone(), &handler) {
                    eprintln!("{e}");
                    // Rollback
                    df = handler.undo().unwrap();
                }
            }
        }
        if self.lazy {
            self.display_and_write_repl_output(df, &handler)?;
        }
        Ok(())
    }

    fn display_and_write_repl_output(
        &self,
        df: LazyFrame,
        handler: &LineExecutor,
    ) -> anyhow::Result<()> {
        let df = df.collect()?;
        println!("{df}");
        if let Some(output) = &self.output {
            write_repl_output(df, handler, output.clone())?;
        }
        Ok(())
    }
}

fn upgrade_df(
    line: String,
    df: LazyFrame,
    handler: &mut LineExecutor,
) -> anyhow::Result<LazyFrame> {
    if line.trim() == "undo" {
        return handler.undo();
    }
    if line.trim() == "reset" {
        return Ok(handler.reset());
    }
    handler.execute(df, line)
}

fn save(df: LazyFrame, handler: &LineExecutor, path: &str) -> anyhow::Result<()> {
    let path = PathBuf::from(path);
    let collected = df.collect()?;
    write_repl_output(collected, handler, path)?;
    Ok(())
}

fn write_repl_output(
    df: DataFrame,
    handler: &LineExecutor,
    mut path: PathBuf,
) -> anyhow::Result<()> {
    write_df_output(df.clone(), &path)?;

    path.set_extension(SQL_EXTENSION);
    write_repl_sql_output(handler.history().iter(), path)?;

    Ok(())
}

use std::{collections::HashMap, path::PathBuf};

use crate::{
    df::DfExecutor,
    handler::LineExecutor,
    io::{read_repl_sql_file, read_sql_file, write_repl_sql_output},
    visual::SqlHelper,
};
use anyhow::{anyhow, bail};
use banyc_polars_util::{read_df_file, write_df_output};
use clap::Parser;
use polars::prelude::*;
use rustyline::{error::ReadlineError, Editor};

const SQL_EXTENSION: &str = "dfsql";

#[derive(Debug, Parser)]
pub struct Cli {
    /// `.dfsql` file to execute
    sql: Option<PathBuf>,
    /// Input files each containing a data frame labeled as a variable available for join operations
    ///
    /// Format: `name,path`
    #[clap(short, long)]
    input: Vec<String>,
    /// Output file storing the modified data frame
    #[clap(short, long)]
    output: Option<PathBuf>,
    /// Only evaluate the data frame on exit
    #[clap(short, long, default_value_t = false)]
    lazy: bool,
}

impl Cli {
    pub fn run(self) -> anyhow::Result<()> {
        let mut input = HashMap::new();
        let mut first_input_name = None;
        for inp in &self.input {
            let (name, path) = inp
                .split_once(',')
                .map(|(n, p)| (n.to_owned(), p))
                .unwrap_or_else(|| {
                    let p = PathBuf::from(inp);
                    let name = p.file_stem().and_then(|n| n.to_str()).unwrap_or(inp);
                    (name.to_owned(), inp)
                });
            let df = read_df_file(path)?;
            if first_input_name.is_none() {
                first_input_name = Some(name.clone());
            }
            input.insert(name, df);
        }
        let first_input_name = first_input_name.ok_or_else(|| {
            anyhow!("Require at least one input data frame from option `--input`")
        })?;
        let mut executor = DfExecutor::new(first_input_name, input).unwrap();

        if let Some(sql_file) = &self.sql {
            // Non-interactive mode
            if self.lazy {
                bail!(
                    "`lazy` option is unavailable if a `.{SQL_EXTENSION}` is provided via the argument `sql`"
                );
            }

            let s = read_sql_file(sql_file)?;
            executor.execute(&s)?;
            let df = executor.df().clone().collect()?;
            match &self.output {
                Some(output) => write_df_output(df, output)?,
                None => println!("{df}"),
            }
            return Ok(());
        }
        let mut handler = LineExecutor::new(executor);
        let mut rl = Editor::new()?;
        if !self.lazy {
            let lines = if let Some(output) = &self.output {
                let mut output = output.clone();
                output.set_extension(SQL_EXTENSION);
                read_repl_sql_file(&output).ok().unwrap_or_default()
            } else {
                vec![]
            };

            self.display_and_write_repl_output(&handler)?;
            for line in lines {
                println!("> {line}");
                let _ = rl.add_history_entry(&line);
                if let Err(e) = handler.execute(line) {
                    eprintln!("{e}");
                    break;
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
                match handler.df_mut().collect_schema() {
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
                if let Err(e) = save(&handler, path) {
                    eprintln!("{e}");
                }
                continue;
            }
            if let Err(e) = upgrade_df(line, &mut handler) {
                eprintln!("{e}");
                continue;
            };
            if !self.lazy {
                if let Err(e) = self.display_and_write_repl_output(&handler) {
                    eprintln!("{e}");
                    // Rollback
                    handler.undo().unwrap();
                }
            }
        }
        if self.lazy {
            self.display_and_write_repl_output(&handler)?;
        }
        Ok(())
    }

    fn display_and_write_repl_output(&self, handler: &LineExecutor) -> anyhow::Result<()> {
        let df = handler.df().clone().collect()?;
        println!("{df}");
        if let Some(output) = &self.output {
            write_repl_output(df, handler, output.clone())?;
        }
        Ok(())
    }
}

fn upgrade_df(line: String, handler: &mut LineExecutor) -> anyhow::Result<()> {
    if line.trim() == "undo" {
        return handler.undo();
    }
    if line.trim() == "reset" {
        handler.reset();
        return Ok(());
    }
    handler.execute(line)
}

fn save(handler: &LineExecutor, path: &str) -> anyhow::Result<()> {
    let path = PathBuf::from(path);
    let collected = handler.df().clone().collect()?;
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

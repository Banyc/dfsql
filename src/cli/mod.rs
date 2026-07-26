pub mod handler;
pub mod visual;

use std::{collections::HashMap, path::PathBuf};

use crate::cli::{handler::LineExecutor, visual::SqlHelper};
use crate::file_ops::{
    atomic_file::StagedFile,
    read_df_file,
    sql_file::{read_repl_sql_file, read_sql_file, stage_repl_sql_output},
    stage_df_output, write_df_output,
};
use crate::{Executor, MaterializedFrame};
use anyhow::{anyhow, bail};
use clap::Parser;
use rustyline::{Editor, error::ReadlineError};

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
                .split_once('.')
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
        let first_input_name = first_input_name
            .ok_or_else(|| anyhow!("Require at least one input data frame from option --input"))?;
        let mut executor = Executor::new(first_input_name, input).unwrap();
        if let Some(sql_file) = &self.sql {
            if self.lazy {
                bail!(
                    "lazy option is unavailable if a {SQL_EXTENSION} is provided via the argument sql"
                );
            }
            let s = read_sql_file(sql_file)?;
            executor.execute(&s)?;
            let df = executor.collect()?;
            match &self.output {
                Some(output) => write_df_output(df, output)?,
                None => println!("{}", df.inner()),
            }
            return Ok(());
        }
        let mut handler = LineExecutor::new(executor);
        let mut rl = Editor::new()?;
        if !self.lazy {
            let lines = if let Some(output) = &self.output {
                let mut output = output.clone();
                output.set_extension(SQL_EXTENSION);
                if output.try_exists()? {
                    read_repl_sql_file(&output)?
                } else {
                    vec![]
                }
            } else {
                vec![]
            };
            self.restore_repl_session(&mut handler, lines, |line| {
                let _ = rl.add_history_entry(line);
            })?;
        }
        rl.set_helper(Some(SqlHelper::new()));
        loop {
            let line = rl.readline("> ");
            let line = match line {
                Ok(line) => line,
                Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => break,
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
                match handler.frame_mut().collect_schema() {
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
            }
            if !self.lazy
                && let Err(e) = self.display_and_write_repl_output(&handler)
            {
                eprintln!("{e}");
                handler.undo().unwrap();
            }
        }
        if self.lazy {
            self.display_and_write_repl_output(&handler)?;
        }
        Ok(())
    }

    fn restore_repl_session(
        &self,
        handler: &mut LineExecutor,
        lines: impl IntoIterator<Item = String>,
        mut record_line: impl FnMut(&str),
    ) -> anyhow::Result<()> {
        for line in lines {
            println!("> {line}");
            record_line(&line);
            handler.execute(line)?;
        }
        self.display_and_write_repl_output(handler)
    }

    fn display_and_write_repl_output(&self, handler: &LineExecutor) -> anyhow::Result<()> {
        let df = handler.frame().clone().collect()?;
        println!("{}", df.inner());
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

fn upgrade_df_and_persist(
    line: String,
    handler: &mut LineExecutor,
    persist: impl FnOnce(&LineExecutor) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let checkpoint = handler.checkpoint();
    let result = upgrade_df(line, handler).and_then(|()| persist(handler));
    if result.is_err() {
        handler.restore(checkpoint);
    }
    result
}

fn save(handler: &LineExecutor, path: &str) -> anyhow::Result<()> {
    let path = PathBuf::from(path);
    let collected = handler.frame().clone().collect()?;
    write_repl_output(collected, handler, path)?;
    Ok(())
}

fn write_repl_output(
    df: MaterializedFrame,
    handler: &LineExecutor,
    mut path: PathBuf,
) -> anyhow::Result<()> {
    let df_output = stage_df_output(df, &path)?;
    path.set_extension(SQL_EXTENSION);
    let sql_output = stage_repl_sql_output(handler.history().iter(), path)?;
    StagedFile::commit_pair(df_output, sql_output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::IntoLazy;

    fn handler() -> LineExecutor {
        let frame = crate::Frame::from_inner(polars::df!("id" => [2_i64, 2]).unwrap().lazy());
        LineExecutor::new(Executor::from_frame("input", frame))
    }

    #[test]
    fn invalid_restored_history_does_not_replace_the_checkpoint() {
        let output = std::env::temp_dir().join(format!("dfsql-cli-{}.csv", std::process::id()));
        let mut history = output.clone();
        history.set_extension(SQL_EXTENSION);
        std::fs::write(&output, "existing output").unwrap();
        std::fs::write(&history, "existing history\n").unwrap();
        let mut handler = handler();
        let cli = Cli {
            sql: None,
            input: vec![],
            output: Some(output.clone()),
            lazy: false,
        };
        assert!(
            cli.restore_repl_session(&mut handler, ["select +".to_owned()], |_| {})
                .is_err()
        );
        assert_eq!(std::fs::read_to_string(&output).unwrap(), "existing output");
        assert_eq!(
            std::fs::read_to_string(&history).unwrap(),
            "existing history\n"
        );
        std::fs::remove_file(output).unwrap();
        std::fs::remove_file(history).unwrap();
    }

    #[test]
    fn persistence_failure_restores_undo_and_reset() {
        for command in ["undo", "reset"] {
            let mut handler = handler();
            handler.execute("sort id".to_owned()).unwrap();
            handler.execute("limit 1".to_owned()).unwrap();
            let expected_history = handler.history().clone();
            let expected = handler.frame().clone().collect().unwrap();
            let error = upgrade_df_and_persist(command.to_owned(), &mut handler, |_| {
                bail!("persistence failed")
            })
            .unwrap_err();
            assert_eq!(error.to_string(), "persistence failed");
            assert_eq!(handler.history(), &expected_history);
            let actual = handler.frame().clone().collect().unwrap();
            assert!(actual.inner().equals_missing(expected.inner()));
        }
    }
}

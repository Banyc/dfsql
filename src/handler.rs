use std::collections::HashMap;

use crate::{
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

    pub fn handle_line(&mut self, df: LazyFrame, line: String) -> anyhow::Result<HandleLineResult> {
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

use std::collections::HashMap;

use crate::{
    df::{apply, ApplyStatError},
    sql,
};
use anyhow::bail;
use polars::lazy::frame::LazyFrame;

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

    pub fn reset(&mut self) -> LazyFrame {
        self.history.clear();
        self.original_df.clone()
    }

    pub fn undo(&mut self) -> anyhow::Result<LazyFrame> {
        self.history.pop();
        let sql = self.history.iter().map(|s| sql::parse(s).unwrap());
        let df = apply_history(self.original_df.clone(), sql, &self.others)?;
        Ok(df)
    }

    pub fn execute(&mut self, df: LazyFrame, line: String) -> anyhow::Result<LazyFrame> {
        let Some(sql) = sql::parse(&line) else {
            bail!("Failed to parse SQL");
        };
        let df = apply(df, &sql, &self.others)?;
        if !line.trim().is_empty() {
            self.history.push(line);
        }
        Ok(df)
    }

    pub fn history(&self) -> &Vec<String> {
        &self.history
    }
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

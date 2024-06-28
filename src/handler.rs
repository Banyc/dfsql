use std::collections::HashMap;

use crate::{
    df::{ApplyStatError, DfExecutor},
    sql,
};
use polars::lazy::frame::LazyFrame;

pub struct LineExecutor {
    history: Vec<String>,
    original_df_name: String,
    original_input: HashMap<String, LazyFrame>,
    executor: DfExecutor,
}

impl LineExecutor {
    pub fn new(executor: DfExecutor) -> Self {
        let original_df_name = executor.df_name().clone();
        let original_input = executor.input().clone();
        Self {
            history: vec![],
            original_df_name,
            original_input,
            executor,
        }
    }

    pub fn reset(&mut self) {
        self.executor =
            DfExecutor::new(self.original_df_name.clone(), self.original_input.clone()).unwrap();
        self.history.clear();
    }

    pub fn undo(&mut self) -> anyhow::Result<()> {
        self.executor =
            DfExecutor::new(self.original_df_name.clone(), self.original_input.clone()).unwrap();
        self.history.pop();
        let sql = self.history.iter().map(|s| sql::parse(s).unwrap());
        apply_history(sql, &mut self.executor)?;
        Ok(())
    }

    pub fn execute(&mut self, line: String) -> anyhow::Result<()> {
        let s = sql::parse(&line)?;
        self.executor.execute(&s)?;
        if !line.trim().is_empty() {
            self.history.push(line);
        }
        Ok(())
    }

    pub fn df(&self) -> &LazyFrame {
        self.executor.df()
    }
    pub fn df_mut(&mut self) -> &mut LazyFrame {
        self.executor.df_mut()
    }

    pub fn history(&self) -> &Vec<String> {
        &self.history
    }
}

fn apply_history(
    sql: impl Iterator<Item = sql::S>,
    executor: &mut DfExecutor,
) -> Result<(), ApplyStatError> {
    for s in sql {
        executor.execute(&s)?;
    }
    Ok(())
}

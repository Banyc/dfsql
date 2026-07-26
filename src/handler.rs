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
        let df_name = self.executor.df_name().clone();
        let input = self.executor.input().clone();
        if let Err(error) = self.executor.execute(&s) {
            self.executor = DfExecutor::new(df_name, input)
                .expect("the active data frame existed before line execution");
            return Err(error.into());
        }
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

#[rustfmt::skip]
#[cfg(test)]
mod tests {
    use polars::prelude::IntoLazy;
    use super::*;

    #[test]
    fn failed_line_restores_all_executor_state() {
        let first = polars::df!("id" => [1_i64]).unwrap().lazy();
        let other = polars::df!("id" => [2_i64]).unwrap().lazy();
        let executor = DfExecutor::new("first".to_string(), HashMap::from([("first".to_string(), first), ("other".to_string(), other)])).unwrap();
        let mut handler = LineExecutor::new(executor);
        let error = handler.execute("use other clone leaked use missing".into()).unwrap_err();
        assert!(error.to_string().contains("missing"));
        assert_eq!(handler.executor.df_name(), "first");
        assert!(!handler.executor.input().contains_key("leaked"));
        assert!(handler.history().is_empty());
    }
}

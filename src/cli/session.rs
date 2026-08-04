use std::collections::HashMap;

use crate::{Error, Frame, backend::PolarsExecutor, sql};

pub(crate) struct Checkpoint {
    history: Vec<String>,
    frame_name: String,
    input: HashMap<String, Frame>,
}

pub struct ReplSession {
    history: Vec<String>,
    original_frame_name: String,
    original_input: HashMap<String, Frame>,
    executor: PolarsExecutor,
}

impl ReplSession {
    pub fn new(executor: PolarsExecutor) -> Self {
        let original_frame_name = executor.frame_name().to_owned();
        let original_input = executor.input().clone();
        Self {
            history: vec![],
            original_frame_name,
            original_input,
            executor,
        }
    }

    pub fn reset(&mut self) {
        self.executor = PolarsExecutor::new(
            self.original_frame_name.clone(),
            self.original_input.clone(),
        )
        .unwrap();
        self.history.clear();
    }

    pub fn undo(&mut self) -> anyhow::Result<()> {
        self.executor = PolarsExecutor::new(
            self.original_frame_name.clone(),
            self.original_input.clone(),
        )
        .unwrap();
        self.history.pop();
        let sql = self.history.iter().map(|s| sql::parse(s).unwrap());
        apply_history(sql, &mut self.executor)?;
        Ok(())
    }

    pub fn execute(&mut self, line: String) -> anyhow::Result<()> {
        let s = sql::parse(&line)?;
        let frame_name = self.executor.frame_name().to_owned();
        let input = self.executor.input().clone();
        if let Err(error) = self.executor.execute(&s) {
            self.executor = PolarsExecutor::new(frame_name, input)
                .expect("the active data frame existed before line execution");
            return Err(error.into());
        }
        if !line.trim().is_empty() {
            self.history.push(line);
        }
        Ok(())
    }

    pub fn frame(&self) -> &Frame {
        self.executor.frame()
    }

    pub fn frame_mut(&mut self) -> &mut Frame {
        self.executor.frame_mut()
    }

    pub fn history(&self) -> &Vec<String> {
        &self.history
    }

    pub(crate) fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            history: self.history.clone(),
            frame_name: self.executor.frame_name().to_owned(),
            input: self.executor.input().clone(),
        }
    }

    pub(crate) fn restore(&mut self, checkpoint: Checkpoint) {
        self.history = checkpoint.history;
        self.executor = PolarsExecutor::new(checkpoint.frame_name, checkpoint.input)
            .expect("the checkpoint contains its active data frame");
    }
}

fn apply_history(
    sql: impl Iterator<Item = sql::Program>,
    executor: &mut PolarsExecutor,
) -> Result<(), Error> {
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
        let first = Frame::from_inner(polars::df!("id" => [1_i64]).unwrap().lazy());
        let other = Frame::from_inner(polars::df!("id" => [2_i64]).unwrap().lazy());
        let executor = PolarsExecutor::new("first", HashMap::from([("first".to_string(), first), ("other".to_string(), other)])).unwrap();
        let mut session = ReplSession::new(executor);
        let error = session.execute("use other clone leaked use missing".into()).unwrap_err();
        assert!(error.to_string().contains("missing"));
        assert_eq!(session.executor.frame_name(), "first");
        assert!(!session.executor.input().contains_key("leaked"));
        assert!(session.history().is_empty());
    }
}

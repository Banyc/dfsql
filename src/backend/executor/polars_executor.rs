use std::collections::HashMap;

use crate::backend::Error;
use crate::backend::PolarsFrame;
use crate::backend::PolarsMaterializedFrame;
use crate::backend::polars_backend::apply_stat;
use crate::backend::polars_backend::map_polars_backend_error;
use crate::backend::polars_backend::map_polars_executor_error;
use crate::sql;

pub struct Executor {
    frame_name: String,
    input: HashMap<String, PolarsFrame>,
}
impl Executor {
    pub fn from_frame(frame_name: impl Into<String>, frame: PolarsFrame) -> Self {
        let frame_name = frame_name.into();
        Self {
            input: HashMap::from([(frame_name.clone(), frame)]),
            frame_name,
        }
    }

    pub fn new(frame_name: impl Into<String>, input: HashMap<String, PolarsFrame>) -> Option<Self> {
        let frame_name = frame_name.into();
        input
            .contains_key(&frame_name)
            .then_some(Self { frame_name, input })
    }

    pub fn input(&self) -> &HashMap<String, PolarsFrame> {
        &self.input
    }

    pub fn into_input(self) -> HashMap<String, PolarsFrame> {
        self.input
    }

    pub fn insert_frame(
        &mut self,
        frame_name: impl Into<String>,
        frame: PolarsFrame,
    ) -> Option<PolarsFrame> {
        self.input.insert(frame_name.into(), frame)
    }

    pub fn frame_name(&self) -> &str {
        &self.frame_name
    }

    pub fn frame(&self) -> &PolarsFrame {
        &self.input[&self.frame_name]
    }

    pub fn frame_mut(&mut self) -> &mut PolarsFrame {
        self.input
            .get_mut(&self.frame_name)
            .expect("the active frame is always present")
    }

    pub fn set_frame_name(&mut self, frame_name: impl Into<String>) -> Result<(), Error> {
        let frame_name = frame_name.into();
        if !self.input.contains_key(&frame_name) {
            return Err(Error::FrameNotFound(frame_name));
        }
        self.frame_name = frame_name;
        Ok(())
    }

    pub fn set_frame(&mut self, frame: PolarsFrame) {
        self.input.insert(self.frame_name.clone(), frame);
    }

    pub fn execute(&mut self, statements: &sql::S) -> Result<(), Error> {
        let mut next = Self {
            frame_name: self.frame_name.clone(),
            input: self.input.clone(),
        };
        let mut frame = next.frame().inner().clone();
        for stat in &statements.statements {
            frame = apply_stat(frame, stat, &mut next.input).map_err(map_polars_executor_error)?;
            if let sql::stat::Stat::Use(r#use) = stat {
                next.set_frame_name(r#use.df_name.clone())?;
            }
            next.set_frame(PolarsFrame::from_inner(frame.clone()));
        }
        *self = next;
        Ok(())
    }

    pub fn collect(&self) -> Result<PolarsMaterializedFrame, Error> {
        let df = self
            .frame()
            .inner()
            .clone()
            .collect()
            .map_err(map_polars_backend_error)?;
        Ok(PolarsMaterializedFrame::from_inner(df))
    }
}

#[rustfmt::skip]
#[cfg(test)]
mod tests {
    use polars::prelude::*;

    use super::*;

    /// ref: <https://github.com/pola-rs/polars/issues/22733>
    #[test]
    fn test_i128() {
        let s = "filter x = 0";
        let s = sql::parse(s).unwrap();
        let df = df!("x" => [0, 1]).unwrap();
        let mut executor = Executor::new("a".to_string(), HashMap::from_iter([("a".to_string(), super::PolarsFrame::from_inner(df.lazy()))])).unwrap();
        executor.execute(&s).unwrap();
        executor.collect().unwrap();
    }

    #[test]
    fn selector_exclusion_and_log_use_current_polars_expressions() {
      let frame = df!("x" => [1.0_f64, 10.0], "drop" => [false, true]).unwrap().lazy();
      let mut executor = Executor::from_frame("input", super::PolarsFrame::from_inner(frame));
      executor.execute(&sql::parse("select exclude drop alias log_x log 10 x").unwrap()).unwrap();
      let output = executor.collect().unwrap();
      assert_eq!(output.width(), 2);
      assert!(output.inner().column("x").is_ok());
      assert!(output.inner().column("drop").is_err());
      assert_eq!(
        output.inner().column("log_x").unwrap().f64().unwrap().into_no_null_iter().collect::<Vec<_>>(),
        [0.0, 1.0]
      );
    }

    #[test]
    fn right_join_uses_each_side_key_after_swapping_inputs() {
        let left = df!("left_id" => [1, 2], "left_value" => ["one", "two"]).unwrap().lazy();
        let right = df!("right_id" => [2, 3], "right_value" => ["two", "three"]).unwrap().lazy();
        let mut executor = Executor::new("left".to_string(),
            HashMap::from_iter([("left".to_string(), super::PolarsFrame::from_inner(left)), ("other".to_string(), super::PolarsFrame::from_inner(right))])).unwrap();
        executor.execute(&sql::parse("right join other on left_id right_id").unwrap()).unwrap();
        let joined = executor.collect().unwrap();
        assert_eq!(joined.height(), 2);
        assert_eq!(joined.inner().column("right_id").unwrap().i32().unwrap().into_no_null_iter().collect::<Vec<_>>(), [2, 3]);
    }
}

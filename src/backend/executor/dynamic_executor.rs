use crate::Error;
use crate::backend::{DynamicFrame, DynamicMaterializedFrame, dynamic};
use crate::sql;
use std::collections::HashMap;

pub struct Executor {
    frame_name: String,
    input: HashMap<String, DynamicFrame>,
}

impl Executor {
    pub fn from_frame(frame_name: impl Into<String>, frame: DynamicFrame) -> Self {
        let frame_name = frame_name.into();
        Self {
            input: HashMap::from([(frame_name.clone(), frame)]),
            frame_name,
        }
    }

    pub fn new(
        frame_name: impl Into<String>,
        input: HashMap<String, DynamicFrame>,
    ) -> Option<Self> {
        let frame_name = frame_name.into();
        input
            .contains_key(&frame_name)
            .then_some(Self { frame_name, input })
    }

    pub fn input(&self) -> &HashMap<String, DynamicFrame> {
        &self.input
    }

    pub fn into_input(self) -> HashMap<String, DynamicFrame> {
        self.input
    }

    pub fn insert_frame(
        &mut self,
        frame_name: impl Into<String>,
        frame: DynamicFrame,
    ) -> Option<DynamicFrame> {
        self.input.insert(frame_name.into(), frame)
    }

    pub fn frame_name(&self) -> &str {
        &self.frame_name
    }

    pub fn frame(&self) -> &DynamicFrame {
        &self.input[&self.frame_name]
    }

    pub fn frame_mut(&mut self) -> &mut DynamicFrame {
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

    pub fn set_frame(&mut self, frame: DynamicFrame) {
        self.input.insert(self.frame_name.clone(), frame);
    }

    pub fn execute(&mut self, statements: &sql::S) -> Result<(), Error> {
        let input = self
            .input
            .iter()
            .map(|(name, frame)| (name.clone(), frame.inner().clone()))
            .collect();
        let mut executor = dynamic::Executor::new(self.frame_name.clone(), input)
            .expect("the active frame is always present");
        for statement in &statements.statements {
            executor.execute_statement(statement).map_err(|e| match e {
                dynamic::Error::FrameNotFound(name) => Error::FrameNotFound(name),
                e => Error::Dynamic(e),
            })?;
        }
        self.frame_name = executor.frame_name().to_owned();
        self.input = executor
            .into_input()
            .into_iter()
            .map(|(name, frame)| (name, DynamicFrame::from_inner(frame)))
            .collect();
        Ok(())
    }

    pub fn collect(&self) -> Result<DynamicMaterializedFrame, Error> {
        Ok(DynamicMaterializedFrame::from_inner(
            self.frame().inner().clone(),
        ))
    }
}

use super::{Frame, MaterializedFrame, dynamic};
use crate::sql;
use std::collections::HashMap;

pub(super) struct Executor {
    frame_name: String,
    input: HashMap<String, Frame>,
}

impl Executor {
    pub(super) fn from_frame(frame_name: impl Into<String>, frame: Frame) -> Self {
        let frame_name = frame_name.into();
        Self {
            input: HashMap::from([(frame_name.clone(), frame)]),
            frame_name,
        }
    }

    pub(super) fn new(
        frame_name: impl Into<String>,
        input: HashMap<String, Frame>,
    ) -> Option<Self> {
        let frame_name = frame_name.into();
        input
            .contains_key(&frame_name)
            .then_some(Self { frame_name, input })
    }

    pub(super) fn input(&self) -> &HashMap<String, Frame> {
        &self.input
    }

    pub(super) fn into_input(self) -> HashMap<String, Frame> {
        self.input
    }

    pub(super) fn insert_frame(
        &mut self,
        frame_name: impl Into<String>,
        frame: Frame,
    ) -> Option<Frame> {
        self.input.insert(frame_name.into(), frame)
    }

    pub(super) fn frame_name(&self) -> &str {
        &self.frame_name
    }

    pub(super) fn frame(&self) -> &Frame {
        &self.input[&self.frame_name]
    }

    pub(super) fn frame_mut(&mut self) -> &mut Frame {
        self.input
            .get_mut(&self.frame_name)
            .expect("the active frame is always present")
    }

    pub(super) fn set_frame_name(
        &mut self,
        frame_name: impl Into<String>,
    ) -> Result<(), dynamic::Error> {
        let frame_name = frame_name.into();
        if !self.input.contains_key(&frame_name) {
            return Err(dynamic::Error::FrameNotFound(frame_name));
        }
        self.frame_name = frame_name;
        Ok(())
    }

    pub(super) fn set_frame(&mut self, frame: Frame) {
        self.input.insert(self.frame_name.clone(), frame);
    }

    pub(super) fn execute(&mut self, statements: &sql::S) -> Result<(), dynamic::Error> {
        let input = self
            .input
            .iter()
            .map(|(name, frame)| (name.clone(), frame.inner().clone()))
            .collect();
        let mut executor = dynamic::Executor::new(self.frame_name.clone(), input)
            .expect("the active frame is always present");
        for statement in &statements.statements {
            executor.execute_statement(statement)?;
        }
        self.frame_name = executor.frame_name().to_owned();
        self.input = executor
            .into_input()
            .into_iter()
            .map(|(name, frame)| (name, Frame::from_inner(frame)))
            .collect();
        Ok(())
    }

    pub(super) fn collect(&self) -> Result<MaterializedFrame, dynamic::Error> {
        Ok(MaterializedFrame::from_inner(self.frame().inner().clone()))
    }
}

pub mod dynamic;
#[cfg(not(feature = "polars-backend"))]
mod dynamic_backend;
#[cfg(feature = "polars-backend")]
mod polars_backend;

use crate::sql;
use std::collections::HashMap;
use thiserror::Error;

#[cfg(feature = "polars-backend")]
use polars::prelude::IntoLazy;

#[cfg(feature = "polars-backend")]
#[derive(Clone)]
pub struct Frame {
    inner: polars::lazy::frame::LazyFrame,
}

#[cfg(feature = "polars-backend")]
#[derive(Clone, Debug)]
pub struct MaterializedFrame {
    inner: polars::frame::DataFrame,
}

#[cfg(not(feature = "polars-backend"))]
#[derive(Clone)]
pub struct Frame {
    inner: dynamic::Frame,
}

#[cfg(not(feature = "polars-backend"))]
#[derive(Clone, Debug)]
pub struct MaterializedFrame {
    inner: dynamic::Frame,
}

#[cfg(not(feature = "polars-backend"))]
type InnerExecutor = dynamic_backend::Executor;

#[cfg(feature = "polars-backend")]
type InnerExecutor = polars_backend::Executor;

pub struct Executor {
    inner: InnerExecutor,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Dynamic(#[from] dynamic::Error),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("data frame does not exist: {0}")]
    FrameNotFound(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(feature = "polars-backend")]
fn map_backend_error(error: impl ToString) -> Error {
    Error::Backend(error.to_string())
}

#[cfg(feature = "polars-backend")]
impl Frame {
    pub fn new(columns: Vec<dynamic::Column>) -> Result<Self> {
        Self::from_dynamic(dynamic::Frame::new(columns)?)
    }

    pub fn from_dynamic(frame: dynamic::Frame) -> Result<Self> {
        polars_backend::frame_from_dynamic(frame)
            .map(Self::from_inner)
            .map_err(map_backend_error)
    }

    pub fn collect(&self) -> Result<MaterializedFrame> {
        self.inner
            .clone()
            .collect()
            .map(MaterializedFrame::from_inner)
            .map_err(|error| Error::Backend(error.to_string()))
    }

    pub(crate) fn from_inner(inner: polars::lazy::frame::LazyFrame) -> Self {
        Self { inner }
    }

    pub(crate) fn inner(&self) -> &polars::lazy::frame::LazyFrame {
        &self.inner
    }

    pub(crate) fn collect_schema(
        &self,
    ) -> polars::prelude::PolarsResult<polars::prelude::SchemaRef> {
        self.inner.clone().collect_schema()
    }
}

#[cfg(not(feature = "polars-backend"))]
impl Frame {
    pub fn new(columns: Vec<dynamic::Column>) -> Result<Self> {
        Self::from_dynamic(dynamic::Frame::new(columns)?)
    }

    pub fn from_dynamic(frame: dynamic::Frame) -> Result<Self> {
        Ok(Self::from_inner(frame))
    }

    pub fn collect(&self) -> Result<MaterializedFrame> {
        Ok(MaterializedFrame::from_inner(self.inner.clone()))
    }

    pub(crate) fn from_inner(inner: dynamic::Frame) -> Self {
        Self { inner }
    }

    pub(crate) fn inner(&self) -> &dynamic::Frame {
        &self.inner
    }
}

#[cfg(feature = "polars-backend")]
impl MaterializedFrame {
    pub fn height(&self) -> usize {
        self.inner.height()
    }

    pub fn width(&self) -> usize {
        self.inner.width()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.height() == 0
    }

    pub fn column_names(&self) -> Vec<String> {
        self.inner
            .get_column_names()
            .into_iter()
            .map(ToString::to_string)
            .collect()
    }

    pub fn to_dynamic(&self) -> Result<dynamic::Frame> {
        polars_backend::frame_to_dynamic(&self.inner).map_err(map_backend_error)
    }

    pub fn into_frame(self) -> Frame {
        Frame::from_inner(self.inner.lazy())
    }

    pub(crate) fn from_inner(inner: polars::frame::DataFrame) -> Self {
        Self { inner }
    }

    #[cfg(feature = "file-ops")]
    pub(crate) fn inner(&self) -> &polars::frame::DataFrame {
        &self.inner
    }

    #[cfg(feature = "file-ops")]
    pub(crate) fn inner_mut(&mut self) -> &mut polars::frame::DataFrame {
        &mut self.inner
    }
}

#[cfg(not(feature = "polars-backend"))]
impl MaterializedFrame {
    pub fn height(&self) -> usize {
        self.inner.height()
    }

    pub fn width(&self) -> usize {
        self.inner.width()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn column_names(&self) -> Vec<String> {
        self.inner
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect()
    }

    pub fn to_dynamic(&self) -> Result<dynamic::Frame> {
        Ok(self.inner.clone())
    }

    pub fn into_frame(self) -> Frame {
        Frame::from_inner(self.inner)
    }

    pub(crate) fn from_inner(inner: dynamic::Frame) -> Self {
        Self { inner }
    }
}

#[cfg(not(feature = "polars-backend"))]
fn map_executor_error(error: dynamic::Error) -> Error {
    match error {
        dynamic::Error::FrameNotFound(name) => Error::FrameNotFound(name),
        error => Error::Dynamic(error),
    }
}

#[cfg(feature = "polars-backend")]
fn map_executor_error(error: polars_backend::Error) -> Error {
    match error {
        polars_backend::Error::FrameNotFound(name) => Error::FrameNotFound(name),
        error => Error::Backend(error.to_string()),
    }
}

impl Executor {
    pub fn from_frame(frame_name: impl Into<String>, frame: Frame) -> Self {
        Self {
            inner: InnerExecutor::from_frame(frame_name, frame),
        }
    }

    pub fn new(frame_name: impl Into<String>, input: HashMap<String, Frame>) -> Option<Self> {
        InnerExecutor::new(frame_name, input).map(|inner| Self { inner })
    }

    pub fn input(&self) -> &HashMap<String, Frame> {
        self.inner.input()
    }

    pub fn into_input(self) -> HashMap<String, Frame> {
        self.inner.into_input()
    }

    pub fn insert_frame(&mut self, frame_name: impl Into<String>, frame: Frame) -> Option<Frame> {
        self.inner.insert_frame(frame_name, frame)
    }

    pub fn frame_name(&self) -> &str {
        self.inner.frame_name()
    }

    pub fn frame(&self) -> &Frame {
        self.inner.frame()
    }

    pub fn frame_mut(&mut self) -> &mut Frame {
        self.inner.frame_mut()
    }

    pub fn set_frame_name(&mut self, frame_name: impl Into<String>) -> Result<()> {
        self.inner
            .set_frame_name(frame_name)
            .map_err(map_executor_error)
    }

    pub fn set_frame(&mut self, frame: Frame) {
        self.inner.set_frame(frame);
    }

    pub fn execute(&mut self, statements: &sql::S) -> Result<()> {
        self.inner.execute(statements).map_err(map_executor_error)
    }

    pub fn collect(&self) -> Result<MaterializedFrame> {
        self.inner.collect().map_err(map_executor_error)
    }
}

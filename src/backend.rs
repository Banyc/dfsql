use crate::sql;
use std::collections::HashMap;
use thiserror::Error;
#[cfg(not(feature = "polars-backend"))]
pub type Frame = crate::dynamic::Frame;
#[cfg(feature = "polars-backend")]
pub type Frame = polars::lazy::frame::LazyFrame;
#[cfg(not(feature = "polars-backend"))]
pub type MaterializedFrame = crate::dynamic::Frame;
#[cfg(feature = "polars-backend")]
pub type MaterializedFrame = polars::frame::DataFrame;
#[cfg(not(feature = "polars-backend"))]
pub type Executor = BackendExecutor<DynamicBackend>;
#[cfg(feature = "polars-backend")]
pub type Executor = BackendExecutor<PolarsBackend>;
#[cfg(feature = "polars-backend")]
pub type PolarsFrame = Frame;
#[cfg(feature = "polars-backend")]
pub type PolarsMaterializedFrame = MaterializedFrame;
#[cfg(feature = "polars-backend")]
pub type PolarsExecutor = Executor;
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Dynamic(#[from] crate::dynamic::Error),
    #[cfg(feature = "polars-backend")]
    #[error(transparent)]
    Polars(#[from] crate::polars_backend::Error),
    #[error("data frame does not exist: {0}")]
    FrameNotFound(String),
}
pub type Result<T> = std::result::Result<T, Error>;
mod private {
    pub trait Sealed {}
}
#[doc(hidden)]
pub trait Backend: private::Sealed {
    type Frame;
    type MaterializedFrame;
    type Inner;
    fn from_frame(frame_name: String, frame: Self::Frame) -> Self::Inner;
    fn new(frame_name: String, input: HashMap<String, Self::Frame>) -> Option<Self::Inner>;
    fn input(inner: &Self::Inner) -> &HashMap<String, Self::Frame>;
    fn into_input(inner: Self::Inner) -> HashMap<String, Self::Frame>;
    fn insert_frame(
        inner: &mut Self::Inner,
        frame_name: String,
        frame: Self::Frame,
    ) -> Option<Self::Frame>;
    fn frame_name(inner: &Self::Inner) -> &str;
    fn frame(inner: &Self::Inner) -> &Self::Frame;
    fn frame_mut(inner: &mut Self::Inner) -> &mut Self::Frame;
    fn set_frame_name(inner: &mut Self::Inner, frame_name: String) -> Result<()>;
    fn set_frame(inner: &mut Self::Inner, frame: Self::Frame);
    fn execute(inner: &mut Self::Inner, statements: &sql::S) -> Result<()>;
    fn collect(inner: &Self::Inner) -> Result<Self::MaterializedFrame>;
}
pub struct BackendExecutor<B: Backend> {
    inner: B::Inner,
}
impl<B: Backend> BackendExecutor<B> {
    pub fn from_frame(frame_name: impl Into<String>, frame: B::Frame) -> Self {
        Self {
            inner: B::from_frame(frame_name.into(), frame),
        }
    }
    pub fn new(frame_name: impl Into<String>, input: HashMap<String, B::Frame>) -> Option<Self> {
        B::new(frame_name.into(), input).map(|inner| Self { inner })
    }
    pub fn input(&self) -> &HashMap<String, B::Frame> {
        B::input(&self.inner)
    }
    pub fn into_input(self) -> HashMap<String, B::Frame> {
        B::into_input(self.inner)
    }
    pub fn insert_frame(
        &mut self,
        frame_name: impl Into<String>,
        frame: B::Frame,
    ) -> Option<B::Frame> {
        B::insert_frame(&mut self.inner, frame_name.into(), frame)
    }
    pub fn frame_name(&self) -> &str {
        B::frame_name(&self.inner)
    }
    pub fn frame(&self) -> &B::Frame {
        B::frame(&self.inner)
    }
    pub fn frame_mut(&mut self) -> &mut B::Frame {
        B::frame_mut(&mut self.inner)
    }
    pub fn set_frame_name(&mut self, frame_name: impl Into<String>) -> Result<()> {
        B::set_frame_name(&mut self.inner, frame_name.into())
    }
    pub fn set_frame(&mut self, frame: B::Frame) {
        B::set_frame(&mut self.inner, frame);
    }
    pub fn execute(&mut self, statements: &sql::S) -> Result<()> {
        B::execute(&mut self.inner, statements)
    }
    pub fn collect(&self) -> Result<B::MaterializedFrame> {
        B::collect(&self.inner)
    }
}
pub enum DynamicBackend {}
impl private::Sealed for DynamicBackend {}
impl Backend for DynamicBackend {
    type Frame = crate::dynamic::Frame;
    type MaterializedFrame = crate::dynamic::Frame;
    type Inner = crate::dynamic::Executor;
    fn from_frame(frame_name: String, frame: Self::Frame) -> Self::Inner {
        Self::Inner::from_frame(frame_name, frame)
    }
    fn new(frame_name: String, input: HashMap<String, Self::Frame>) -> Option<Self::Inner> {
        Self::Inner::new(frame_name, input)
    }
    fn input(inner: &Self::Inner) -> &HashMap<String, Self::Frame> {
        inner.input()
    }
    fn into_input(inner: Self::Inner) -> HashMap<String, Self::Frame> {
        inner.into_input()
    }
    fn insert_frame(
        inner: &mut Self::Inner,
        frame_name: String,
        frame: Self::Frame,
    ) -> Option<Self::Frame> {
        inner.insert_frame(frame_name, frame)
    }
    fn frame_name(inner: &Self::Inner) -> &str {
        inner.frame_name()
    }
    fn frame(inner: &Self::Inner) -> &Self::Frame {
        inner.frame()
    }
    fn frame_mut(inner: &mut Self::Inner) -> &mut Self::Frame {
        inner.frame_mut()
    }
    fn set_frame_name(inner: &mut Self::Inner, frame_name: String) -> Result<()> {
        inner
            .set_frame_name(frame_name.clone())
            .map_err(|error| match error {
                crate::dynamic::Error::FrameNotFound(_) => Error::FrameNotFound(frame_name),
                error => Error::Dynamic(error),
            })
    }
    fn set_frame(inner: &mut Self::Inner, frame: Self::Frame) {
        inner.set_frame(frame);
    }
    fn execute(inner: &mut Self::Inner, statements: &sql::S) -> Result<()> {
        inner.execute(statements).map_err(|error| match error {
            crate::dynamic::Error::FrameNotFound(name) => Error::FrameNotFound(name),
            error => Error::Dynamic(error),
        })
    }
    fn collect(inner: &Self::Inner) -> Result<Self::MaterializedFrame> {
        Ok(inner.frame().clone())
    }
}
#[cfg(feature = "polars-backend")]
pub enum PolarsBackend {}
#[cfg(feature = "polars-backend")]
impl private::Sealed for PolarsBackend {}
#[cfg(feature = "polars-backend")]
impl Backend for PolarsBackend {
    type Frame = polars::lazy::frame::LazyFrame;
    type MaterializedFrame = polars::frame::DataFrame;
    type Inner = crate::polars_backend::Executor;
    fn from_frame(frame_name: String, frame: Self::Frame) -> Self::Inner {
        Self::Inner::from_frame(frame_name, frame)
    }
    fn new(frame_name: String, input: HashMap<String, Self::Frame>) -> Option<Self::Inner> {
        Self::Inner::new(frame_name, input)
    }
    fn input(inner: &Self::Inner) -> &HashMap<String, Self::Frame> {
        inner.input()
    }
    fn into_input(inner: Self::Inner) -> HashMap<String, Self::Frame> {
        inner.into_input()
    }
    fn insert_frame(
        inner: &mut Self::Inner,
        frame_name: String,
        frame: Self::Frame,
    ) -> Option<Self::Frame> {
        inner.insert_frame(frame_name, frame)
    }
    fn frame_name(inner: &Self::Inner) -> &str {
        inner.frame_name()
    }
    fn frame(inner: &Self::Inner) -> &Self::Frame {
        inner.frame()
    }
    fn frame_mut(inner: &mut Self::Inner) -> &mut Self::Frame {
        inner.frame_mut()
    }
    fn set_frame_name(inner: &mut Self::Inner, frame_name: String) -> Result<()> {
        inner
            .set_frame_name(frame_name.clone())
            .map_err(|_| Error::FrameNotFound(frame_name))
    }
    fn set_frame(inner: &mut Self::Inner, frame: Self::Frame) {
        inner.set_frame(frame);
    }
    fn execute(inner: &mut Self::Inner, statements: &sql::S) -> Result<()> {
        inner.execute(statements).map_err(|error| match error {
            crate::polars_backend::Error::FrameNotFound(name) => Error::FrameNotFound(name),
            error => Error::Polars(error),
        })
    }
    fn collect(inner: &Self::Inner) -> Result<Self::MaterializedFrame> {
        inner.collect().map_err(Error::from)
    }
}

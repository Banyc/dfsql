#[cfg(feature = "polars-backend")]
use crate::backend::PolarsMaterializedFrame;
use crate::backend::{Result, dynamic};

#[cfg(feature = "polars-backend")]
pub type Frame = PolarsFrame;
#[cfg(not(feature = "polars-backend"))]
pub type Frame = DynamicFrame;

#[cfg(feature = "polars-backend")]
#[derive(Clone)]
pub struct PolarsFrame {
    inner: polars::lazy::frame::LazyFrame,
}
#[cfg(feature = "polars-backend")]
impl PolarsFrame {
    pub fn new(columns: Vec<dynamic::Column>) -> Result<Self> {
        Self::from_dynamic(dynamic::Frame::new(columns)?)
    }

    pub fn from_dynamic(frame: dynamic::Frame) -> Result<Self> {
        use crate::backend::polars_backend;

        polars_backend::frame_from_dynamic(frame)
            .map(Self::from_inner)
            .map_err(polars_backend::map_polars_backend_error)
    }

    pub fn collect(&self) -> Result<PolarsMaterializedFrame> {
        use crate::Error;

        self.inner
            .clone()
            .collect()
            .map(PolarsMaterializedFrame::from_inner)
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

#[derive(Clone)]
pub struct DynamicFrame {
    inner: dynamic::Frame,
}
impl DynamicFrame {
    pub fn new(columns: Vec<dynamic::Column>) -> Result<Self> {
        Self::from_dynamic(dynamic::Frame::new(columns)?)
    }

    pub fn from_dynamic(frame: dynamic::Frame) -> Result<Self> {
        Ok(Self::from_inner(frame))
    }

    pub fn collect(&self) -> Result<dynamic::MaterializedFrame> {
        Ok(self.inner.clone())
    }

    pub(crate) fn from_inner(inner: dynamic::Frame) -> Self {
        Self { inner }
    }

    pub(crate) fn inner(&self) -> &dynamic::Frame {
        &self.inner
    }
}

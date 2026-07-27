use crate::backend::{Result, dynamic};

#[cfg(feature = "polars-backend")]
pub type MaterializedFrame = PolarsMaterializedFrame;
#[cfg(not(feature = "polars-backend"))]
pub type MaterializedFrame = DynamicMaterializedFrame;

#[cfg(feature = "polars-backend")]
#[derive(Clone, Debug)]
pub struct PolarsMaterializedFrame {
    inner: polars::frame::DataFrame,
}
#[cfg(feature = "polars-backend")]
impl PolarsMaterializedFrame {
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
        use crate::backend::polars_backend;

        polars_backend::frame_to_dynamic(&self.inner)
            .map_err(polars_backend::map_polars_backend_error)
    }
    pub fn into_frame(self) -> crate::backend::Frame {
        use polars::prelude::IntoLazy;

        crate::backend::Frame::from_inner(self.inner.lazy())
    }
    pub(crate) fn from_inner(inner: polars::frame::DataFrame) -> Self {
        Self { inner }
    }
    pub(crate) fn inner(&self) -> &polars::frame::DataFrame {
        &self.inner
    }
    #[cfg(feature = "file-ops")]
    pub(crate) fn inner_mut(&mut self) -> &mut polars::frame::DataFrame {
        &mut self.inner
    }
}

#[derive(Clone, Debug)]
pub struct DynamicMaterializedFrame {
    inner: dynamic::Frame,
}
impl DynamicMaterializedFrame {
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
    pub fn into_frame(self) -> crate::backend::DynamicFrame {
        crate::backend::DynamicFrame::from_inner(self.inner)
    }
    pub(crate) fn from_inner(inner: dynamic::Frame) -> Self {
        Self { inner }
    }
}

pub mod dynamic;
mod executor;
mod frame;
mod materialized_frame;
#[cfg(feature = "polars-backend")]
mod polars_backend;

use thiserror::Error;

pub use executor::*;
pub use frame::*;
pub use materialized_frame::*;

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

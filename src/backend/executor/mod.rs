mod dynamic_executor;
#[cfg(feature = "polars-backend")]
mod polars_executor;

pub type DynamicExecutor = dynamic_executor::Executor;
#[cfg(feature = "polars-backend")]
pub type PolarsExecutor = polars_executor::Executor;

#[cfg(not(feature = "polars-backend"))]
pub type Executor = DynamicExecutor;
#[cfg(feature = "polars-backend")]
pub type Executor = PolarsExecutor;

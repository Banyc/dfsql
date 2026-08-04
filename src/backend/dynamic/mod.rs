//! The dynamically-typed built-in engine.

mod executor;
pub(crate) mod expression;
mod frame;
mod value;

pub use executor::Engine;
pub use frame::{Column, ColumnData, Frame};
pub use value::{IntoValue, Value, ValueType};

#[derive(Debug, thiserror::Error, PartialEq)]
#[non_exhaustive]
pub enum Error {
    #[error("frame not found: {0}")]
    FrameNotFound(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("duplicate column: {0}")]
    DuplicateColumn(String),
    #[error("column `{column}` length mismatch: expected {expected}, got {actual}")]
    ColumnLength {
        column: String,
        expected: usize,
        actual: usize,
    },
    #[error("invalid type for {operation}: {kind}")]
    InvalidType {
        operation: &'static str,
        kind: &'static str,
    },
    #[error("length mismatch for {operation}: left={left}, right={right}")]
    LengthMismatch {
        operation: &'static str,
        left: usize,
        right: usize,
    },
    #[error("invalid value for {operation}: {value}")]
    InvalidValue {
        operation: &'static str,
        value: String,
    },
    #[error("invalid regex pattern `{pattern}`: {message}")]
    InvalidRegex { pattern: String, message: String },
    #[error("selector in scalar expression")]
    SelectorInScalarExpression,
}

pub type Result<T> = std::result::Result<T, Error>;

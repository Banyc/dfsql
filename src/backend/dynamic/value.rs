#![allow(dead_code)]

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use super::{Error, Result};

#[derive(Clone, Debug, Default, PartialEq)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    UInt(u64),
    Int(i64),
    Float(f64),
    String(Arc<str>),
    List(Arc<[Value]>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueType {
    Bool,
    UInt,
    Int,
    Float,
    String,
    List,
}

impl ValueType {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::UInt => "uint",
            Self::Int => "int",
            Self::Float => "float",
            Self::String => "string",
            Self::List => "list",
        }
    }
}

pub trait IntoValue: Into<Value> {
    fn value_type() -> Option<ValueType> {
        None
    }
}

// --- From impls ---

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<u8> for Value {
    fn from(v: u8) -> Self {
        Value::UInt(v as u64)
    }
}

impl From<u16> for Value {
    fn from(v: u16) -> Self {
        Value::UInt(v as u64)
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::UInt(v as u64)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::UInt(v)
    }
}

impl From<i8> for Value {
    fn from(v: i8) -> Self {
        Value::Int(v as i64)
    }
}

impl From<i16> for Value {
    fn from(v: i16) -> Self {
        Value::Int(v as i64)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float(v as f64)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v.into())
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.into())
    }
}

impl From<Arc<str>> for Value {
    fn from(v: Arc<str>) -> Self {
        Value::String(v)
    }
}

impl<T: IntoValue> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(val) => val.into(),
            None => Value::Null,
        }
    }
}

// --- IntoValue impls ---

impl IntoValue for bool {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::Bool)
    }
}

impl IntoValue for u8 {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::UInt)
    }
}

impl IntoValue for u16 {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::UInt)
    }
}

impl IntoValue for u32 {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::UInt)
    }
}

impl IntoValue for u64 {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::UInt)
    }
}

impl IntoValue for i8 {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::Int)
    }
}

impl IntoValue for i16 {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::Int)
    }
}

impl IntoValue for i32 {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::Int)
    }
}

impl IntoValue for i64 {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::Int)
    }
}

impl IntoValue for f32 {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::Float)
    }
}

impl IntoValue for f64 {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::Float)
    }
}

impl IntoValue for String {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::String)
    }
}

impl IntoValue for &str {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::String)
    }
}

impl IntoValue for Arc<str> {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::String)
    }
}

impl IntoValue for Value {
    fn value_type() -> Option<ValueType> {
        None
    }
}

impl<T: IntoValue> IntoValue for Option<T> {
    fn value_type() -> Option<ValueType> {
        T::value_type()
    }
}

impl From<Vec<Value>> for Value {
    fn from(value: Vec<Value>) -> Self {
        Self::List(value.into())
    }
}

impl IntoValue for Vec<Value> {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::List)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Number {
    UInt(u64),
    Int(i64),
    Float(f64),
}

impl Number {
    pub(crate) fn as_f64(self) -> f64 {
        match self {
            Self::UInt(value) => value as f64,
            Self::Int(value) => value as f64,
            Self::Float(value) => value,
        }
    }
}

pub(crate) fn number_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

impl Value {
    pub(crate) fn value_type(&self) -> Option<ValueType> {
        match self {
            Self::Null => None,
            Self::Bool(_) => Some(ValueType::Bool),
            Self::UInt(_) => Some(ValueType::UInt),
            Self::Int(_) => Some(ValueType::Int),
            Self::Float(_) => Some(ValueType::Float),
            Self::String(_) => Some(ValueType::String),
            Self::List(_) => Some(ValueType::List),
        }
    }

    pub fn kind(&self) -> &'static str {
        self.value_type().map_or("null", |t| t.name())
    }

    pub(crate) fn bool(&self, operation: &'static str) -> Result<Option<bool>> {
        match self {
            Self::Null => Ok(None),
            Self::Bool(value) => Ok(Some(*value)),
            value => Err(value.invalid_type(operation)),
        }
    }

    pub(crate) fn string(&self, operation: &'static str) -> Result<Option<&str>> {
        match self {
            Self::Null => Ok(None),
            Self::String(value) => Ok(Some(value)),
            value => Err(value.invalid_type(operation)),
        }
    }

    pub(crate) fn number(&self, operation: &'static str) -> Result<Option<Number>> {
        match self {
            Self::Null => Ok(None),
            Self::UInt(value) => Ok(Some(Number::UInt(*value))),
            Self::Int(value) => Ok(Some(Number::Int(*value))),
            Self::Float(value) => Ok(Some(Number::Float(*value))),
            value => Err(value.invalid_type(operation)),
        }
    }

    pub(crate) fn compare(&self, other: &Self, operation: &'static str) -> Result<Ordering> {
        match (self, other) {
            (Self::Null, Self::Null) => Ok(Ordering::Equal),
            (Self::Null, _) => Ok(Ordering::Less),
            (_, Self::Null) => Ok(Ordering::Greater),
            (Self::Bool(left), Self::Bool(right)) => Ok(left.cmp(right)),
            (Self::String(left), Self::String(right)) => Ok(left.cmp(right)),
            (
                left @ (Self::UInt(_) | Self::Int(_) | Self::Float(_)),
                right @ (Self::UInt(_) | Self::Int(_) | Self::Float(_)),
            ) => Ok(left
                .number(operation)?
                .unwrap()
                .as_f64()
                .total_cmp(&right.number(operation)?.unwrap().as_f64())),
            (left, _) => Err(left.invalid_type(operation)),
        }
    }

    pub(crate) fn equal(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::String(left), Self::String(right)) => left == right,
            (Self::List(left), Self::List(right)) => left == right,
            (left, right) => match (left.number("equality"), right.number("equality")) {
                (Ok(Some(left)), Ok(Some(right))) => left.as_f64() == right.as_f64(),
                _ => false,
            },
        }
    }

    pub(crate) fn invalid_type(&self, operation: &'static str) -> Error {
        Error::InvalidType {
            operation,
            kind: self.kind(),
        }
    }

    pub(crate) fn key(&self) -> ValueKey {
        match self {
            Self::Null => ValueKey::Null,
            Self::Bool(value) => ValueKey::Bool(*value),
            Self::UInt(value) => ValueKey::Number(number_bits(*value as f64)),
            Self::Int(value) => ValueKey::Number(number_bits(*value as f64)),
            Self::Float(value) => ValueKey::Number(number_bits(*value)),
            Self::String(value) => ValueKey::String(value.clone()),
            Self::List(values) => {
                ValueKey::List(values.iter().map(Self::key).collect::<Vec<_>>().into())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ValueKey {
    Null,
    Bool(bool),
    Number(u64),
    String(Arc<str>),
    List(Arc<[ValueKey]>),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::UInt(u) => write!(f, "{u}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Float(fl) => write!(f, "{fl}"),
            Value::String(s) => write!(f, "{s}"),
            Value::List(l) => {
                write!(f, "[")?;
                for (i, v) in l.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn cross_numeric_equality() {
        assert!(Value::UInt(42).equal(&Value::Int(42)));
        assert!(Value::Int(42).equal(&Value::Float(42.0)));
        assert!(Value::Float(42.0).equal(&Value::UInt(42)));
        assert!(!Value::UInt(42).equal(&Value::Int(43)));
    }

    #[test]
    fn cross_numeric_comparison() {
        assert_eq!(
            Value::UInt(42).compare(&Value::Int(99), "test").unwrap(),
            Ordering::Less
        );
        assert_eq!(
            Value::Float(99.5)
                .compare(&Value::UInt(42), "test")
                .unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn cross_numeric_key_consistency() {
        let k1 = Value::UInt(42).key();
        let k2 = Value::Int(42).key();
        let k3 = Value::Float(42.0).key();
        assert_eq!(k1, k2);
        assert_eq!(k2, k3);
        assert_eq!(k1, k3);
    }

    #[test]
    fn numeric_key_preserves_sign() {
        assert_ne!(Value::Int(-42).key(), Value::Int(42).key());
    }
}

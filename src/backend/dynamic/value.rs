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
    Bytes(Arc<[u8]>),
    List(Arc<[Value]>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueType {
    Bool,
    UInt,
    Int,
    Float,
    String,
    Bytes,
    List,
}

impl ValueType {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::UInt => "uint",
            Self::Int => "int",
            Self::Float => "float",
            Self::String => "string",
            Self::Bytes => "bytes",
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

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v.into())
    }
}

impl From<Arc<[u8]>> for Value {
    fn from(v: Arc<[u8]>) -> Self {
        Value::Bytes(v)
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

impl IntoValue for Vec<u8> {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::Bytes)
    }
}

impl IntoValue for Arc<[u8]> {
    fn value_type() -> Option<ValueType> {
        Some(ValueType::Bytes)
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

    pub(crate) fn compare(self, other: Self) -> Ordering {
        match (self, other) {
            (Self::UInt(left), Self::UInt(right)) => left.cmp(&right),
            (Self::Int(left), Self::Int(right)) => left.cmp(&right),
            (Self::UInt(left), Self::Int(right)) => {
                if right < 0 {
                    Ordering::Greater
                } else {
                    left.cmp(&(right as u64))
                }
            }
            (Self::Int(left), Self::UInt(right)) => {
                if left < 0 {
                    Ordering::Less
                } else {
                    (left as u64).cmp(&right)
                }
            }
            (Self::Float(left), Self::Float(right)) => left.total_cmp(&right),
            (Self::UInt(left), Self::Float(right)) => compare_uint_float(left, right),
            (Self::Float(left), Self::UInt(right)) => compare_uint_float(right, left).reverse(),
            (Self::Int(left), Self::Float(right)) => compare_int_float(left, right),
            (Self::Float(left), Self::Int(right)) => compare_int_float(right, left).reverse(),
        }
    }

    pub(crate) fn equal(self, other: Self) -> bool {
        match (self, other) {
            (Self::Float(left), Self::Float(right)) => left == right,
            (Self::Float(value), _) | (_, Self::Float(value)) if value.is_nan() => false,
            _ => self.compare(other) == Ordering::Equal,
        }
    }

    fn key(self) -> NumericKey {
        match self {
            Self::UInt(value) => NumericKey::UInt(value),
            Self::Int(value) if value >= 0 => NumericKey::UInt(value as u64),
            Self::Int(value) => NumericKey::Int(value),
            Self::Float(value) => float_key(value),
        }
    }
}

fn compare_uint_float(integer: u64, float: f64) -> Ordering {
    const U64_EXCLUSIVE_MAX: f64 = 18_446_744_073_709_551_616.0;
    if !float.is_finite() {
        return (integer as f64).total_cmp(&float);
    }
    if float < 0.0 {
        return Ordering::Greater;
    }
    if float >= U64_EXCLUSIVE_MAX {
        return Ordering::Less;
    }
    let truncated = float.trunc() as u64;
    match integer.cmp(&truncated) {
        Ordering::Equal if float.fract() > 0.0 => Ordering::Less,
        ordering => ordering,
    }
}

fn compare_int_float(integer: i64, float: f64) -> Ordering {
    const I64_EXCLUSIVE_MAX: f64 = 9_223_372_036_854_775_808.0;
    if !float.is_finite() {
        return (integer as f64).total_cmp(&float);
    }
    if float < i64::MIN as f64 {
        return Ordering::Greater;
    }
    if float >= I64_EXCLUSIVE_MAX {
        return Ordering::Less;
    }
    let truncated = float.trunc() as i64;
    match integer.cmp(&truncated) {
        Ordering::Equal if float.fract() > 0.0 => Ordering::Less,
        Ordering::Equal if float.fract() < 0.0 => Ordering::Greater,
        ordering => ordering,
    }
}

fn number_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

fn float_key(value: f64) -> NumericKey {
    const U64_EXCLUSIVE_MAX: f64 = 18_446_744_073_709_551_616.0;
    if (0.0..U64_EXCLUSIVE_MAX).contains(&value) && value.fract() == 0.0 {
        return NumericKey::UInt(value as u64);
    }
    if value < 0.0 && value >= i64::MIN as f64 && value.fract() == 0.0 {
        return NumericKey::Int(value as i64);
    }
    NumericKey::Float(number_bits(value))
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
            Self::Bytes(_) => Some(ValueType::Bytes),
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
            (Self::Bytes(left), Self::Bytes(right)) => Ok(left.cmp(right)),
            (
                left @ (Self::UInt(_) | Self::Int(_) | Self::Float(_)),
                right @ (Self::UInt(_) | Self::Int(_) | Self::Float(_)),
            ) => Ok(left
                .number(operation)?
                .unwrap()
                .compare(right.number(operation)?.unwrap())),
            (left, _) => Err(left.invalid_type(operation)),
        }
    }

    pub(crate) fn equal(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Bytes(left), Self::Bytes(right)) => left == right,
            (Self::List(left), Self::List(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right.iter())
                        .all(|(left, right)| left.equal(right))
            }
            (left, right) => match (left.number("equality"), right.number("equality")) {
                (Ok(Some(left)), Ok(Some(right))) => left.equal(right),
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
            Self::UInt(value) => ValueKey::Number(Number::UInt(*value).key()),
            Self::Int(value) => ValueKey::Number(Number::Int(*value).key()),
            Self::Float(value) => ValueKey::Number(Number::Float(*value).key()),
            Self::String(value) => ValueKey::String(value.clone()),
            Self::Bytes(value) => ValueKey::Bytes(value.clone()),
            Self::List(values) => {
                ValueKey::List(values.iter().map(Self::key).collect::<Vec<_>>().into())
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum NumericKey {
    UInt(u64),
    Int(i64),
    Float(u64),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ValueKey {
    Null,
    Bool(bool),
    Number(NumericKey),
    String(Arc<str>),
    Bytes(Arc<[u8]>),
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
            Value::Bytes(bytes) => {
                write!(f, "0x")?;
                for byte in bytes.iter() {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
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
    fn list_equality_matches_key_normalization() {
        let left = Value::List(vec![Value::UInt(42)].into());
        let right = Value::List(vec![Value::Int(42)].into());
        assert!(left.equal(&right));
        assert_eq!(left.key(), right.key());
    }

    #[test]
    fn numeric_key_preserves_sign() {
        assert_ne!(Value::Int(-42).key(), Value::Int(42).key());
    }

    #[test]
    fn large_integers_do_not_collapse_through_float_conversion() {
        let lower = Value::UInt(9_007_199_254_740_992);
        let higher = Value::UInt(9_007_199_254_740_993);
        assert!(!lower.equal(&higher));
        assert_ne!(lower.key(), higher.key());
        assert_eq!(lower.compare(&higher, "test").unwrap(), Ordering::Less);
    }

    #[test]
    fn exactly_representable_cross_numeric_values_share_identity() {
        let integer = Value::UInt(9_007_199_254_740_992);
        let float = Value::Float(9_007_199_254_740_992.0);
        assert!(integer.equal(&float));
        assert_eq!(integer.key(), float.key());
    }

    #[test]
    fn integer_float_ordering_is_exact_at_numeric_boundaries() {
        assert_eq!(
            Value::UInt(u64::MAX)
                .compare(&Value::Float(18_446_744_073_709_551_616.0), "test")
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            Value::Int(i64::MAX)
                .compare(&Value::Float(9_223_372_036_854_775_808.0), "test")
                .unwrap(),
            Ordering::Less
        );
    }
}

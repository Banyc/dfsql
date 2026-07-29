use crate::sql::expr::CastExpr;
use crate::sql::lexer::Type;

use crate::backend::dynamic::value::Number;
use crate::backend::dynamic::{Column, Result, Value, ValueType};

use super::helpers::{invalid_value, map_column};

pub(super) fn apply_cast(cast: &CastExpr, column: Column) -> Result<Column> {
    let value_type = Some(match cast.ty {
        Type::Str => ValueType::String,
        Type::UInt => ValueType::UInt,
        Type::Int => ValueType::Int,
        Type::Float => ValueType::Float,
    });
    map_column(column, value_type, |val| cast_value(val, cast.ty))
}

pub(super) fn cast_value(value: &Value, ty: Type) -> Result<Value> {
    if ty == Type::Str {
        return Ok(Value::String(value.to_string().into()));
    }
    let number = match value {
        Value::Bool(value) => Number::UInt(u64::from(*value)),
        Value::UInt(value) => Number::UInt(*value),
        Value::Int(value) => Number::Int(*value),
        Value::Float(value) => Number::Float(*value),
        Value::String(value) => {
            return match ty {
                Type::UInt => value
                    .parse()
                    .map(Value::UInt)
                    .map_err(|_| invalid_value("cast", value)),
                Type::Int => value
                    .parse()
                    .map(Value::Int)
                    .map_err(|_| invalid_value("cast", value)),
                Type::Float => value
                    .parse()
                    .map(Value::Float)
                    .map_err(|_| invalid_value("cast", value)),
                Type::Str => unreachable!(),
            };
        }
        value => return Err(value.invalid_type("cast")),
    };
    Ok(match ty {
        Type::UInt => Value::UInt(number_to_u64(number)?),
        Type::Int => Value::Int(number_to_i64(number)?),
        Type::Float => Value::Float(number.as_f64()),
        Type::Str => unreachable!(),
    })
}

fn number_to_u64(number: Number) -> Result<u64> {
    const U64_EXCLUSIVE_MAX: f64 = 18_446_744_073_709_551_616.0;
    match number {
        Number::UInt(value) => Ok(value),
        Number::Int(value) => u64::try_from(value).map_err(|_| invalid_value("cast", value)),
        Number::Float(value) if value.is_finite() && (0.0..U64_EXCLUSIVE_MAX).contains(&value) => {
            Ok(value as u64)
        }
        Number::Float(value) => Err(invalid_value("cast", value)),
    }
}

fn number_to_i64(number: Number) -> Result<i64> {
    const I64_EXCLUSIVE_MAX: f64 = 9_223_372_036_854_775_808.0;
    match number {
        Number::UInt(value) => i64::try_from(value).map_err(|_| invalid_value("cast", value)),
        Number::Int(value) => Ok(value),
        Number::Float(value)
            if value.is_finite() && value >= i64::MIN as f64 && value < I64_EXCLUSIVE_MAX =>
        {
            Ok(value as i64)
        }
        Number::Float(value) => Err(invalid_value("cast", value)),
    }
}

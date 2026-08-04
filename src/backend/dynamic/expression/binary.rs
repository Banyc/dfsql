use std::cmp::Ordering;

use crate::sql::expr::BinaryOperator;

use crate::backend::dynamic::value::Number;
use crate::backend::dynamic::{Column, Error, Result, Value, ValueType};

use super::{Evaluated, combined_len};

pub(super) fn apply_binary(
    operator: BinaryOperator,
    left: Evaluated,
    right: Evaluated,
) -> Result<Evaluated> {
    let name = left.column.name().to_owned();
    let value_type = binary_value_type(
        operator,
        left.column.value_type(),
        right.column.value_type(),
    );
    zip_evaluated(name, value_type, left, right, |left, right| {
        binary_value(operator, left, right)
    })
}

fn binary_value_type(
    operator: BinaryOperator,
    left: Option<ValueType>,
    right: Option<ValueType>,
) -> Option<ValueType> {
    match operator {
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq
        | BinaryOperator::And
        | BinaryOperator::Or => Some(ValueType::Bool),
        BinaryOperator::Add
        | BinaryOperator::Sub
        | BinaryOperator::Mul
        | BinaryOperator::Div
        | BinaryOperator::Modulo
        | BinaryOperator::Pow => {
            if left == Some(ValueType::Float) || right == Some(ValueType::Float) {
                Some(ValueType::Float)
            } else if left == Some(ValueType::Int) || right == Some(ValueType::Int) {
                Some(ValueType::Int)
            } else if left == Some(ValueType::UInt) || right == Some(ValueType::UInt) {
                Some(ValueType::UInt)
            } else {
                None
            }
        }
    }
}

pub(super) fn zip_evaluated(
    name: impl Into<String>,
    value_type: Option<ValueType>,
    left: Evaluated,
    right: Evaluated,
    mut function: impl FnMut(&Value, &Value) -> Result<Value>,
) -> Result<Evaluated> {
    let len = combined_len("expression", [&left, &right])?;
    let values = (0..len)
        .map(|index| {
            function(
                &left.value(index, len, "expression")?,
                &right.value(index, len, "expression")?,
            )
        })
        .collect::<Result<_>>()?;
    Ok(Evaluated {
        column: Column::from_values_with_hint(name, values, value_type),
        arity: left.arity.merge(right.arity),
    })
}

pub(super) fn binary_value(operator: BinaryOperator, left: &Value, right: &Value) -> Result<Value> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match operator {
        BinaryOperator::Eq => Ok(Value::Bool(left.equal(right))),
        BinaryOperator::NotEq => Ok(Value::Bool(!left.equal(right))),
        BinaryOperator::Lt => compare(left, right, |value| value == Ordering::Less),
        BinaryOperator::LtEq => compare(left, right, |value| value != Ordering::Greater),
        BinaryOperator::Gt => compare(left, right, |value| value == Ordering::Greater),
        BinaryOperator::GtEq => compare(left, right, |value| value != Ordering::Less),
        BinaryOperator::And | BinaryOperator::Or => logical_value(operator, left, right),
        BinaryOperator::Add
        | BinaryOperator::Sub
        | BinaryOperator::Mul
        | BinaryOperator::Div
        | BinaryOperator::Modulo
        | BinaryOperator::Pow => numeric_value(operator, left, right),
    }
}

fn compare(left: &Value, right: &Value, predicate: impl FnOnce(Ordering) -> bool) -> Result<Value> {
    Ok(Value::Bool(predicate(left.compare(right, "comparison")?)))
}

fn logical_value(operator: BinaryOperator, left: &Value, right: &Value) -> Result<Value> {
    if let (Some(left), Some(right)) = (left.bool("logical")?, right.bool("logical")?) {
        return Ok(Value::Bool(match operator {
            BinaryOperator::And => left & right,
            BinaryOperator::Or => left | right,
            _ => unreachable!(),
        }));
    }
    unreachable!("nulls are handled before logical evaluation")
}

fn numeric_value(operator: BinaryOperator, left: &Value, right: &Value) -> Result<Value> {
    let left = left.number("arithmetic")?.expect("null handled by caller");
    let right = right.number("arithmetic")?.expect("null handled by caller");
    if right.as_f64() == 0.0 && matches!(operator, BinaryOperator::Div | BinaryOperator::Modulo) {
        return Err(Error::InvalidValue {
            operation: "arithmetic",
            value: "division by zero".into(),
        });
    }
    if matches!(left, Number::Float(_)) || matches!(right, Number::Float(_)) {
        let (left, right) = (left.as_f64(), right.as_f64());
        return Ok(Value::Float(match operator {
            BinaryOperator::Add => left + right,
            BinaryOperator::Sub => left - right,
            BinaryOperator::Mul => left * right,
            BinaryOperator::Div => left / right,
            BinaryOperator::Modulo => left % right,
            BinaryOperator::Pow => left.powf(right),
            _ => unreachable!(),
        }));
    }
    if matches!(left, Number::Int(_)) || matches!(right, Number::Int(_)) {
        return signed_value(operator, as_i64(left)?, as_i64(right)?);
    }
    unsigned_value(operator, as_u64(left), as_u64(right))
}

fn signed_value(operator: BinaryOperator, left: i64, right: i64) -> Result<Value> {
    let value = match operator {
        BinaryOperator::Add => left.checked_add(right),
        BinaryOperator::Sub => left.checked_sub(right),
        BinaryOperator::Mul => left.checked_mul(right),
        BinaryOperator::Div => left.checked_div(right),
        BinaryOperator::Modulo => left.checked_rem(right),
        BinaryOperator::Pow if right >= 0 => u32::try_from(right)
            .ok()
            .and_then(|right| left.checked_pow(right)),
        BinaryOperator::Pow => {
            return Err(Error::InvalidValue {
                operation: "power",
                value: "negative integer exponent".into(),
            });
        }
        _ => unreachable!(),
    }
    .ok_or_else(|| arithmetic_overflow(operator, left, right))?;
    Ok(Value::Int(value))
}

fn unsigned_value(operator: BinaryOperator, left: u64, right: u64) -> Result<Value> {
    let value = match operator {
        BinaryOperator::Add => left.checked_add(right),
        BinaryOperator::Sub => left.checked_sub(right),
        BinaryOperator::Mul => left.checked_mul(right),
        BinaryOperator::Div => left.checked_div(right),
        BinaryOperator::Modulo => left.checked_rem(right),
        BinaryOperator::Pow => u32::try_from(right)
            .ok()
            .and_then(|right| left.checked_pow(right)),
        _ => unreachable!(),
    }
    .ok_or_else(|| arithmetic_overflow(operator, left, right))?;
    Ok(Value::UInt(value))
}

fn as_i64(value: Number) -> Result<i64> {
    match value {
        Number::UInt(value) => i64::try_from(value).map_err(|_| Error::InvalidValue {
            operation: "arithmetic",
            value: format!("{value} cannot be represented as a signed integer"),
        }),
        Number::Int(value) => Ok(value),
        Number::Float(_) => unreachable!(),
    }
}

fn as_u64(value: Number) -> u64 {
    match value {
        Number::UInt(value) => value,
        Number::Int(_) | Number::Float(_) => unreachable!(),
    }
}

fn arithmetic_overflow(
    operator: BinaryOperator,
    left: impl std::fmt::Display,
    right: impl std::fmt::Display,
) -> Error {
    Error::InvalidValue {
        operation: "arithmetic",
        value: format!("{left} {operator:?} {right} overflows"),
    }
}

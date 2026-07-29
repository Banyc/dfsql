use std::{cmp::Ordering, collections::HashSet};

use crate::sql::expr::{BinaryOperator, UnaryOperator};

use crate::backend::dynamic::value::Number;
use crate::backend::dynamic::{Column, Error, Result, Value, ValueType};

use super::binary::binary_value;
use super::helpers::{map_all, map_column};

pub(super) fn is_reduction(operator: UnaryOperator) -> bool {
    matches!(
        operator,
        UnaryOperator::Sum
            | UnaryOperator::Count
            | UnaryOperator::First
            | UnaryOperator::Last
            | UnaryOperator::Mean
            | UnaryOperator::Median
            | UnaryOperator::Max
            | UnaryOperator::Min
            | UnaryOperator::Var
            | UnaryOperator::Std
            | UnaryOperator::All
            | UnaryOperator::Any
    )
}

pub(super) fn apply_unary(operator: UnaryOperator, column: Column) -> Result<Column> {
    match operator {
        UnaryOperator::Sum => reduce_sum(column),
        UnaryOperator::Count => Ok(Column::new(
            column.name(),
            vec![
                column
                    .iter()
                    .filter(|val| !matches!(val, Value::Null))
                    .count() as u64,
            ],
        )),
        UnaryOperator::First => reduce_edge(column, false),
        UnaryOperator::Last => reduce_edge(column, true),
        UnaryOperator::Mean => reduce_mean(column),
        UnaryOperator::Median => reduce_median(column),
        UnaryOperator::Max => reduce_extreme(column, false),
        UnaryOperator::Min => reduce_extreme(column, true),
        UnaryOperator::Var => reduce_variance(column, false),
        UnaryOperator::Std => reduce_variance(column, true),
        UnaryOperator::All => reduce_bool(column, true),
        UnaryOperator::Any => reduce_bool(column, false),
        UnaryOperator::Unique => unique(column),
        UnaryOperator::Reverse => {
            let name = column.name().to_owned();
            let value_type = column.value_type();
            Ok(Column::from_values_with_hint(
                name,
                column.iter().rev().collect(),
                value_type,
            ))
        }
        UnaryOperator::Sqrt => map_column(column, Some(ValueType::Float), |val| {
            Ok(Value::Float(
                val.number("sqrt")?
                    .expect("null handled by map")
                    .as_f64()
                    .sqrt(),
            ))
        }),
        UnaryOperator::Abs => {
            let value_type = column.value_type();
            map_column(column, value_type, absolute)
        }
        UnaryOperator::Neg => {
            let value_type = match column.value_type() {
                Some(ValueType::UInt | ValueType::Int) => Some(ValueType::Int),
                value_type => value_type,
            };
            map_column(column, value_type, negate)
        }
        UnaryOperator::Not => map_column(column, Some(ValueType::Bool), |val| {
            Ok(Value::Bool(!val.bool("not")?.expect("null handled by map")))
        }),
        UnaryOperator::IsNull => map_all(column, Some(ValueType::Bool), |val| {
            Ok(Value::Bool(matches!(val, Value::Null)))
        }),
        UnaryOperator::IsNan => map_all(column, Some(ValueType::Bool), |val| {
            Ok(Value::Bool(matches!(val, Value::Float(v) if v.is_nan())))
        }),
    }
}

fn absolute(value: &Value) -> Result<Value> {
    Ok(match value {
        Value::UInt(value) => Value::UInt(*value),
        Value::Int(value) => {
            Value::Int(value.checked_abs().ok_or_else(|| Error::InvalidValue {
                operation: "abs",
                value: format!("{value} overflows"),
            })?)
        }
        Value::Float(value) => Value::Float(value.abs()),
        value => return Err(value.invalid_type("abs")),
    })
}

fn negate(value: &Value) -> Result<Value> {
    Ok(match value {
        Value::UInt(value) => Value::Int(
            i64::try_from(*value)
                .ok()
                .and_then(i64::checked_neg)
                .ok_or_else(|| Error::InvalidValue {
                    operation: "negate",
                    value: format!("{value} cannot be represented as a negative integer"),
                })?,
        ),
        Value::Int(value) => {
            Value::Int(value.checked_neg().ok_or_else(|| Error::InvalidValue {
                operation: "negate",
                value: format!("{value} overflows"),
            })?)
        }
        Value::Float(value) => Value::Float(-value),
        value => return Err(value.invalid_type("negate")),
    })
}

fn reduce_sum(column: Column) -> Result<Column> {
    let name = column.name().to_owned();
    let value_type = column.value_type();
    let value = column
        .iter()
        .filter(|val| !matches!(val, Value::Null))
        .try_fold(None, |sum, val| {
            sum.map_or(Ok(Some(val.clone())), |s| {
                binary_value(BinaryOperator::Add, &s, &val).map(Some)
            })
        })?
        .unwrap_or_default();
    Ok(Column::from_values_with_hint(name, vec![value], value_type))
}

fn reduce_edge(column: Column, last: bool) -> Result<Column> {
    let value_type = column.value_type();
    let mut values = column.iter().filter(|val| !matches!(val, Value::Null));
    let value = if last {
        values.next_back()
    } else {
        values.next()
    }
    .unwrap_or_default();
    Ok(Column::from_values_with_hint(
        column.name(),
        vec![value],
        value_type,
    ))
}

fn numbers(column: &Column, operation: &'static str) -> Result<Vec<f64>> {
    column
        .iter()
        .filter(|value| !matches!(value, Value::Null))
        .map(|value| {
            value
                .number(operation)?
                .map(Number::as_f64)
                .ok_or_else(|| value.invalid_type(operation))
        })
        .collect()
}

fn reduce_mean(column: Column) -> Result<Column> {
    let values = numbers(&column, "mean")?;
    let value = if values.is_empty() {
        Value::Null
    } else {
        Value::Float(values.iter().sum::<f64>() / values.len() as f64)
    };
    Ok(Column::from_values_with_hint(
        column.name(),
        vec![value],
        Some(ValueType::Float),
    ))
}

fn reduce_median(column: Column) -> Result<Column> {
    let mut values = numbers(&column, "median")?;
    values.sort_by(f64::total_cmp);
    let value = match values.len() {
        0 => Value::Null,
        len if len % 2 == 1 => Value::Float(values[len / 2]),
        len => Value::Float((values[len / 2 - 1] + values[len / 2]) / 2.0),
    };
    Ok(Column::from_values_with_hint(
        column.name(),
        vec![value],
        Some(ValueType::Float),
    ))
}

fn reduce_extreme(column: Column, minimum: bool) -> Result<Column> {
    let value_type = column.value_type();
    let mut result: Option<Value> = None;
    for value in column.iter().filter(|value| !matches!(value, Value::Null)) {
        let replace = match &result {
            None => true,
            Some(current) => {
                value.compare(current, if minimum { "min" } else { "max" })?
                    == if minimum {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
            }
        };
        if replace {
            result = Some(value);
        }
    }
    Ok(Column::from_values_with_hint(
        column.name(),
        vec![result.unwrap_or_default()],
        value_type,
    ))
}

fn reduce_variance(column: Column, standard_deviation: bool) -> Result<Column> {
    let values = numbers(&column, if standard_deviation { "std" } else { "var" })?;
    let value = if values.len() < 2 {
        Value::Null
    } else {
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance =
            values.iter().map(|val| (val - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
        Value::Float(if standard_deviation {
            variance.sqrt()
        } else {
            variance
        })
    };
    Ok(Column::from_values_with_hint(
        column.name(),
        vec![value],
        Some(ValueType::Float),
    ))
}

fn reduce_bool(column: Column, all: bool) -> Result<Column> {
    let mut values = column
        .iter()
        .filter(|value| !matches!(value, Value::Null))
        .map(|value| value.bool(if all { "all" } else { "any" }));
    let value = if all {
        values.try_fold(true, |result, value| Ok(result && value?.unwrap()))?
    } else {
        values.try_fold(false, |result, value| Ok(result || value?.unwrap()))?
    };
    Ok(Column::new(column.name(), [value]))
}

fn unique(column: Column) -> Result<Column> {
    let name = column.name().to_owned();
    let value_type = column.value_type();
    let mut seen = HashSet::new();
    Ok(Column::from_values_with_hint(
        name,
        column.iter().filter(|val| seen.insert(val.key())).collect(),
        value_type,
    ))
}

use std::cmp::Ordering;

use crate::sql::expr::{
    BinaryOperator, CastExpr, ConditionalExpr, Expr, StandaloneOperator, StrExpr, UnaryOperator,
};
use crate::sql::lexer::{Literal, Type};

use super::value::Number;
use super::{Column, Error, Frame, Result, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Shape {
    Scalar,
    Rows,
}

impl Shape {
    fn merge(self, other: Self) -> Self {
        if self == Self::Scalar && other == Self::Scalar {
            Self::Scalar
        } else {
            Self::Rows
        }
    }
}

#[derive(Debug)]
pub(crate) struct Evaluated {
    pub(crate) column: Column,
    pub(crate) shape: Shape,
}

impl Evaluated {
    fn value(&self, index: usize, len: usize, operation: &'static str) -> Result<Value> {
        match self.shape {
            Shape::Scalar if self.column.len() == 1 => Ok(self.column.get(0).unwrap()),
            Shape::Rows if self.column.len() == len => Ok(self.column.get(index).unwrap()),
            _ => Err(Error::LengthMismatch {
                operation,
                left: self.column.len(),
                right: len,
            }),
        }
    }

    pub(crate) fn materialize(self, len: usize, operation: &'static str) -> Result<Column> {
        match self.shape {
            Shape::Scalar => self.column.broadcast(len),
            Shape::Rows if self.column.len() == len => Ok(self.column),
            Shape::Rows => Err(Error::LengthMismatch {
                operation,
                left: self.column.len(),
                right: len,
            }),
        }
    }
}

fn literal_value(literal: &Literal) -> Result<Value> {
    match literal {
        Literal::String(value) => Ok(Value::from(value.clone())),
        Literal::Int(value) => {
            value
                .parse::<i64>()
                .map(Value::Int)
                .map_err(|_| Error::InvalidValue {
                    operation: "literal",
                    value: value.clone(),
                })
        }
        Literal::Float(value) => {
            value
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| Error::InvalidValue {
                    operation: "literal",
                    value: value.clone(),
                })
        }
        Literal::Bool(value) => Ok(Value::Bool(*value)),
        Literal::Null => Ok(Value::Null),
    }
}

pub(crate) fn expression_name(expression: &Expr) -> String {
    match expression {
        Expr::Col(name) => name.clone(),
        Expr::Exclude(_) => "*".into(),
        Expr::Literal(_) => "literal".into(),
        Expr::Binary(value) => expression_name(&value.left),
        Expr::Unary(value) => expression_name(&value.expr),
        Expr::Alias(value) => value.name.clone(),
        Expr::Conditional(value) => expression_name(&value.first_case.then),
        Expr::Cast(value) => expression_name(&value.expr),
        Expr::Log(value) => expression_name(&value.expr),
        Expr::Str(value) => expression_name(string_parts(value).0),
        Expr::Standalone(_) => "len".into(),
        Expr::SortBy(value) => expression_name(&value.expr),
        Expr::Sort(value) => expression_name(&value.expr),
    }
}

pub(crate) fn expand_selectors(
    frame: &Frame,
    expressions: &[Expr],
    excluded: &[String],
) -> Vec<Expr> {
    expressions
        .iter()
        .flat_map(|expression| {
            if !has_selector(expression) {
                return vec![expression.clone()];
            }
            frame
                .columns()
                .iter()
                .filter(|column| !excluded.iter().any(|name| name == column.name()))
                .filter_map(|column| {
                    let mut expression = expression.clone();
                    bind_selector(&mut expression, column.name()).then_some(expression)
                })
                .collect()
        })
        .collect()
}

fn has_selector(expression: &Expr) -> bool {
    match expression {
        Expr::Col(name) => name == "*",
        Expr::Exclude(_) => true,
        Expr::Literal(_) | Expr::Standalone(_) => false,
        Expr::Binary(value) => has_selector(&value.left) || has_selector(&value.right),
        Expr::Unary(value) => has_selector(&value.expr),
        Expr::Alias(value) => has_selector(&value.expr),
        Expr::Conditional(value) => {
            has_selector(&value.first_case.when)
                || has_selector(&value.first_case.then)
                || value
                    .other_cases
                    .iter()
                    .any(|case| has_selector(&case.when) || has_selector(&case.then))
                || has_selector(&value.otherwise)
        }
        Expr::Cast(value) => has_selector(&value.expr),
        Expr::Log(value) => has_selector(&value.expr),
        Expr::Str(value) => {
            let (string, pattern) = string_parts(value);
            has_selector(string) || has_selector(pattern)
        }
        Expr::SortBy(value) => {
            has_selector(&value.expr) || value.pairs.iter().any(|(_, value)| has_selector(value))
        }
        Expr::Sort(value) => has_selector(&value.expr),
    }
}

fn bind_selector(expression: &mut Expr, column: &str) -> bool {
    match expression {
        Expr::Col(name) if name == "*" => {
            *name = column.into();
            true
        }
        Expr::Exclude(value) => {
            if value.columns.iter().any(|name| name == column) {
                false
            } else {
                *expression = Expr::Col(column.into());
                true
            }
        }
        Expr::Col(_) | Expr::Literal(_) | Expr::Standalone(_) => true,
        Expr::Binary(value) => {
            bind_selector(&mut value.left, column) && bind_selector(&mut value.right, column)
        }
        Expr::Unary(value) => bind_selector(&mut value.expr, column),
        Expr::Alias(value) => bind_selector(&mut value.expr, column),
        Expr::Conditional(value) => {
            bind_selector(&mut value.first_case.when, column)
                && bind_selector(&mut value.first_case.then, column)
                && value.other_cases.iter_mut().all(|case| {
                    bind_selector(&mut case.when, column) && bind_selector(&mut case.then, column)
                })
                && bind_selector(&mut value.otherwise, column)
        }
        Expr::Cast(value) => bind_selector(&mut value.expr, column),
        Expr::Log(value) => bind_selector(&mut value.expr, column),
        Expr::Str(value) => {
            let (string, pattern) = string_parts_mut(value);
            bind_selector(string, column) && bind_selector(pattern, column)
        }
        Expr::SortBy(value) => {
            bind_selector(&mut value.expr, column)
                && value
                    .pairs
                    .iter_mut()
                    .all(|(_, value)| bind_selector(value, column))
        }
        Expr::Sort(value) => bind_selector(&mut value.expr, column),
    }
}

fn string_parts(expression: &StrExpr) -> (&Expr, &Expr) {
    match expression {
        StrExpr::Contains(value) => (&value.str, &value.pattern),
        StrExpr::Extract(value) => (&value.str, &value.pattern),
        StrExpr::ExtractAll(value) => (&value.str, &value.pattern),
        StrExpr::Split(value) => (&value.str, &value.pattern),
    }
}

fn string_parts_mut(expression: &mut StrExpr) -> (&mut Expr, &mut Expr) {
    match expression {
        StrExpr::Contains(value) => (&mut value.str, &mut value.pattern),
        StrExpr::Extract(value) => (&mut value.str, &mut value.pattern),
        StrExpr::ExtractAll(value) => (&mut value.str, &mut value.pattern),
        StrExpr::Split(value) => (&mut value.str, &mut value.pattern),
    }
}

pub(crate) fn evaluate_shaped(frame: &Frame, expression: &Expr) -> Result<Evaluated> {
    let (column, shape) = match expression {
        Expr::Col(name) if name == "*" => return Err(Error::SelectorInScalarExpression),
        Expr::Col(name) => (frame.column(name)?.clone(), Shape::Rows),
        Expr::Literal(value) => (
            Column::from_values("literal", vec![literal_value(value)?]),
            Shape::Scalar,
        ),
        Expr::Binary(value) => {
            let left = evaluate_shaped(frame, &value.left)?;
            let right = evaluate_shaped(frame, &value.right)?;
            let result = apply_binary(value.operator, left, right)?;
            (result.column, result.shape)
        }
        Expr::Unary(value) => {
            let inner = evaluate_shaped(frame, &value.expr)?;
            let shape = if is_reduction(value.operator.clone()) {
                Shape::Scalar
            } else {
                inner.shape
            };
            (apply_unary(value.operator.clone(), inner.column)?, shape)
        }
        Expr::Alias(value) => {
            let evaluated = evaluate_shaped(frame, &value.expr)?;
            (evaluated.column.rename(value.name.clone()), evaluated.shape)
        }
        Expr::Conditional(value) => {
            let result = evaluate_conditional(frame, value)?;
            (result.column, result.shape)
        }
        Expr::Cast(value) => {
            let result = evaluate_shaped(frame, &value.expr)?;
            (apply_cast(value, result.column)?, result.shape)
        }
        Expr::Standalone(value) if value.operator == StandaloneOperator::Len => {
            (Column::new("len", [frame.height() as u64]), Shape::Scalar)
        }
        Expr::Exclude(_) => return Err(Error::SelectorInScalarExpression),
        value => {
            return Err(Error::InvalidValue {
                operation: "evaluate",
                value: format!("{value:?}"),
            });
        }
    };
    Ok(Evaluated { column, shape })
}

fn combined_len<'a>(
    operation: &'static str,
    values: impl IntoIterator<Item = &'a Evaluated>,
) -> Result<usize> {
    let mut rows = None;
    for value in values {
        if value.shape == Shape::Rows {
            if let Some(len) = rows {
                if len != value.column.len() {
                    return Err(Error::LengthMismatch {
                        operation,
                        left: len,
                        right: value.column.len(),
                    });
                }
            } else {
                rows = Some(value.column.len());
            }
        } else if value.column.len() != 1 {
            return Err(Error::LengthMismatch {
                operation,
                left: value.column.len(),
                right: 1,
            });
        }
    }
    Ok(rows.unwrap_or(1))
}

fn apply_binary(operator: BinaryOperator, left: Evaluated, right: Evaluated) -> Result<Evaluated> {
    let name = left.column.name().to_owned();
    zip_evaluated(name, left, right, |left, right| {
        binary_value(operator, left, right)
    })
}

fn zip_evaluated(
    name: impl Into<String>,
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
        column: Column::from_values(name, values),
        shape: left.shape.merge(right.shape),
    })
}

fn binary_value(operator: BinaryOperator, left: &Value, right: &Value) -> Result<Value> {
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
        return signed_value(operator, as_i64(left), as_i64(right));
    }
    unsigned_value(operator, as_u64(left), as_u64(right))
}

fn signed_value(operator: BinaryOperator, left: i64, right: i64) -> Result<Value> {
    let value = match operator {
        BinaryOperator::Add => left.wrapping_add(right),
        BinaryOperator::Sub => left.wrapping_sub(right),
        BinaryOperator::Mul => left.wrapping_mul(right),
        BinaryOperator::Div => left.wrapping_div(right),
        BinaryOperator::Modulo => left.wrapping_rem(right),
        BinaryOperator::Pow if right >= 0 => left.wrapping_pow(right as u32),
        BinaryOperator::Pow => {
            return Err(Error::InvalidValue {
                operation: "power",
                value: "negative integer exponent".into(),
            });
        }
        _ => unreachable!(),
    };
    Ok(Value::Int(value))
}

fn unsigned_value(operator: BinaryOperator, left: u64, right: u64) -> Result<Value> {
    Ok(Value::UInt(match operator {
        BinaryOperator::Add => left.wrapping_add(right),
        BinaryOperator::Sub => left.wrapping_sub(right),
        BinaryOperator::Mul => left.wrapping_mul(right),
        BinaryOperator::Div => left / right,
        BinaryOperator::Modulo => left % right,
        BinaryOperator::Pow => left.wrapping_pow(right as u32),
        _ => unreachable!(),
    }))
}

fn as_i64(value: Number) -> i64 {
    match value {
        Number::UInt(value) => value as i64,
        Number::Int(value) => value,
        Number::Float(_) => unreachable!(),
    }
}

fn as_u64(value: Number) -> u64 {
    match value {
        Number::UInt(value) => value,
        Number::Int(_) | Number::Float(_) => unreachable!(),
    }
}

fn invalid_value(operation: &'static str, value: impl ToString) -> Error {
    Error::InvalidValue {
        operation,
        value: value.to_string(),
    }
}

fn is_reduction(operator: UnaryOperator) -> bool {
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

fn apply_unary(operator: UnaryOperator, column: Column) -> Result<Column> {
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
        UnaryOperator::Reverse => {
            let name = column.name().to_owned();
            Ok(Column::from_values(name, column.iter().rev().collect()))
        }
        UnaryOperator::Sqrt => map_column(column, |val| {
            Ok(Value::Float(
                val.number("sqrt")?
                    .expect("null handled by map")
                    .as_f64()
                    .sqrt(),
            ))
        }),
        UnaryOperator::Abs => map_column(column, absolute),
        UnaryOperator::Neg => map_column(column, negate),
        UnaryOperator::Not => map_column(column, |val| {
            Ok(Value::Bool(!val.bool("not")?.expect("null handled by map")))
        }),
        UnaryOperator::IsNull => map_all(column, |val| Ok(Value::Bool(matches!(val, Value::Null)))),
        UnaryOperator::IsNan => map_all(column, |val| {
            Ok(Value::Bool(matches!(val, Value::Float(v) if v.is_nan())))
        }),
        unsupported => Err(Error::InvalidValue {
            operation: "unary",
            value: format!("{unsupported:?}"),
        }),
    }
}

fn map_column(column: Column, mut function: impl FnMut(&Value) -> Result<Value>) -> Result<Column> {
    map_all(column, |val| {
        if matches!(val, Value::Null) {
            Ok(Value::Null)
        } else {
            function(val)
        }
    })
}

fn map_all(column: Column, mut function: impl FnMut(&Value) -> Result<Value>) -> Result<Column> {
    let name = column.name().to_owned();
    Ok(Column::from_values(
        name,
        column
            .iter()
            .map(|val| function(&val))
            .collect::<Result<_>>()?,
    ))
}

fn absolute(value: &Value) -> Result<Value> {
    Ok(match value {
        Value::UInt(value) => Value::UInt(*value),
        Value::Int(value) => Value::Int(value.wrapping_abs()),
        Value::Float(value) => Value::Float(value.abs()),
        value => return Err(value.invalid_type("abs")),
    })
}

fn negate(value: &Value) -> Result<Value> {
    Ok(match value {
        Value::UInt(value) => Value::Int(-(*value as i64)),
        Value::Int(value) => Value::Int(value.wrapping_neg()),
        Value::Float(value) => Value::Float(-value),
        value => return Err(value.invalid_type("negate")),
    })
}

fn reduce_sum(column: Column) -> Result<Column> {
    let name = column.name().to_owned();
    let value = column
        .iter()
        .filter(|val| !matches!(val, Value::Null))
        .try_fold(None, |sum, val| {
            sum.map_or(Ok(Some(val.clone())), |s| {
                binary_value(BinaryOperator::Add, &s, &val).map(Some)
            })
        })?
        .unwrap_or_default();
    Ok(Column::from_values(name, vec![value]))
}

fn reduce_edge(column: Column, last: bool) -> Result<Column> {
    let mut values = column.iter().filter(|val| !matches!(val, Value::Null));
    let value = if last {
        values.next_back()
    } else {
        values.next()
    }
    .unwrap_or_default();
    Ok(Column::from_values(column.name(), vec![value]))
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
    Ok(Column::from_values(column.name(), vec![value]))
}

fn reduce_median(column: Column) -> Result<Column> {
    let mut values = numbers(&column, "median")?;
    values.sort_by(f64::total_cmp);
    let value = match values.len() {
        0 => Value::Null,
        len if len % 2 == 1 => Value::Float(values[len / 2]),
        len => Value::Float((values[len / 2 - 1] + values[len / 2]) / 2.0),
    };
    Ok(Column::from_values(column.name(), vec![value]))
}

fn reduce_extreme(column: Column, minimum: bool) -> Result<Column> {
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
    Ok(Column::from_values(
        column.name(),
        vec![result.unwrap_or_default()],
    ))
}

fn reduce_variance(column: Column, standard_deviation: bool) -> Result<Column> {
    let values = numbers(&column, if standard_deviation { "std" } else { "var" })?;
    let value = if values.len() < 2 {
        Value::Null
    } else {
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / (values.len() - 1) as f64;
        Value::Float(if standard_deviation {
            variance.sqrt()
        } else {
            variance
        })
    };
    Ok(Column::from_values(column.name(), vec![value]))
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

fn evaluate_conditional(frame: &Frame, conditional: &ConditionalExpr) -> Result<Evaluated> {
    let mut cases = Vec::with_capacity(conditional.other_cases.len() + 1);
    for case in std::iter::once(&conditional.first_case).chain(&conditional.other_cases) {
        cases.push((
            evaluate_shaped(frame, &case.when)?,
            evaluate_shaped(frame, &case.then)?,
        ));
    }
    let otherwise = evaluate_shaped(frame, &conditional.otherwise)?;
    let shape = cases.iter().fold(otherwise.shape, |shape, (when, then)| {
        shape.merge(when.shape).merge(then.shape)
    });
    let len = combined_len(
        "conditional",
        std::iter::once(&otherwise).chain(cases.iter().flat_map(|(when, then)| [when, then])),
    )?;
    let name = expression_name(&conditional.first_case.then);
    let values = (0..len)
        .map(|index| {
            for (when, then) in &cases {
                if when
                    .value(index, len, "conditional")?
                    .bool("conditional")?
                    .unwrap_or(false)
                {
                    return then.value(index, len, "conditional");
                }
            }
            otherwise.value(index, len, "conditional")
        })
        .collect::<Result<_>>()?;
    Ok(Evaluated {
        column: Column::from_values(name, values),
        shape,
    })
}

fn apply_cast(cast: &CastExpr, column: Column) -> Result<Column> {
    map_column(column, |value| cast_value(value, cast.ty))
}

fn cast_value(value: &Value, ty: Type) -> Result<Value> {
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
        Type::UInt => Value::UInt(number.as_f64() as u64),
        Type::Int => Value::Int(number.as_f64() as i64),
        Type::Float => Value::Float(number.as_f64()),
        Type::Str => unreachable!(),
    })
}

pub(crate) fn select(frame: &Frame, expressions: &[Expr]) -> Result<Frame> {
    let values = expand_selectors(frame, expressions, &[])
        .iter()
        .map(|expression| evaluate_shaped(frame, expression))
        .collect::<Result<Vec<_>>>()?;
    let height = if values.is_empty() {
        0
    } else {
        combined_len("select", values.iter())?
    };
    Frame::with_height(
        values
            .into_iter()
            .map(|value| value.materialize(height, "select"))
            .collect::<Result<Vec<_>>>()?,
        height,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::expr::{
        AliasExpr, BinaryExpr, BinaryOperator, ConditionalCase, ExcludeExpr, StandaloneExpr,
        UnaryExpr, UnaryOperator,
    };

    fn binary(operator: BinaryOperator, left: Expr, right: Expr) -> Expr {
        Expr::Binary(Box::new(BinaryExpr {
            operator,
            left,
            right,
        }))
    }

    fn unary(operator: UnaryOperator, expr: Expr) -> Expr {
        Expr::Unary(Box::new(UnaryExpr { operator, expr }))
    }

    #[test]
    fn select_columns_literals_aliases_and_len() {
        let frame = Frame::new(vec![Column::new("a", vec![1_i64, 2])]).unwrap();
        let result = select(
            &frame,
            &[
                Expr::Col("a".into()),
                Expr::Alias(Box::new(AliasExpr {
                    name: "renamed".into(),
                    expr: Expr::Col("a".into()),
                })),
                Expr::Literal(Literal::Int("7".into())),
                Expr::Standalone(Box::new(StandaloneExpr {
                    operator: StandaloneOperator::Len,
                })),
            ],
        )
        .unwrap();
        assert_eq!(result.height(), 2);
        assert_eq!(
            result.column("a").unwrap().values(),
            vec![Value::Int(1), Value::Int(2)]
        );
        assert_eq!(
            result.column("renamed").unwrap().values(),
            vec![Value::Int(1), Value::Int(2)]
        );
        assert_eq!(
            result.column("literal").unwrap().values(),
            vec![Value::Int(7), Value::Int(7)]
        );
        assert_eq!(
            result.column("len").unwrap().values(),
            vec![Value::UInt(2), Value::UInt(2)]
        );
    }

    #[test]
    fn scalar_only_select_has_one_row() {
        let frame = Frame::default();
        let result = select(&frame, &[Expr::Literal(Literal::String("x".into()))]).unwrap();
        assert_eq!(result.width(), 1);
        assert_eq!(result.height(), 1);
        assert_eq!(result.row(0), Some(vec![Value::from("x")]));
    }

    #[test]
    fn selector_is_rejected_before_execution() {
        let frame = Frame::default();
        let err = evaluate_shaped(&frame, &Expr::Col("*".into())).unwrap_err();
        assert_eq!(err, Error::SelectorInScalarExpression);
    }

    #[test]
    fn invalid_literal_reports_value() {
        let err = literal_value(&Literal::Int("not-an-int".into())).unwrap_err();
        assert_eq!(
            err,
            Error::InvalidValue {
                operation: "literal",
                value: "not-an-int".into()
            }
        );
    }

    #[test]
    fn binary_broadcasts_scalars_and_propagates_null() {
        let frame = Frame::new(vec![Column::new("a", vec![Some(1_i64), None, Some(3)])]).unwrap();
        let result = select(
            &frame,
            &[binary(
                BinaryOperator::Add,
                Expr::Col("a".into()),
                Expr::Literal(Literal::Int("2".into())),
            )],
        )
        .unwrap();
        assert_eq!(
            result.column("a").unwrap().values(),
            vec![Value::Int(3), Value::Null, Value::Int(5)]
        );
    }

    #[test]
    fn binary_numeric_promotion_is_float_then_int_then_uint() {
        let left_val = Value::UInt(5);
        let right_val = Value::UInt(3);
        let result = binary_value(BinaryOperator::Add, &left_val, &right_val).unwrap();
        assert_eq!(result, Value::UInt(8));

        let result = binary_value(BinaryOperator::Add, &Value::UInt(5), &Value::Int(-2)).unwrap();
        assert_eq!(result, Value::Int(3));

        let result =
            binary_value(BinaryOperator::Add, &Value::UInt(5), &Value::Float(2.5)).unwrap();
        assert_eq!(result, Value::Float(7.5));
    }

    #[test]
    fn binary_comparison_and_logic_validate_types() {
        assert!(
            binary_value(BinaryOperator::Lt, &Value::Int(1), &Value::Float(2.0))
                .unwrap()
                .bool("test")
                .unwrap()
                .unwrap()
        );
        assert!(
            !binary_value(BinaryOperator::And, &Value::Bool(true), &Value::Bool(false))
                .unwrap()
                .bool("test")
                .unwrap()
                .unwrap()
        );
        let err =
            binary_value(BinaryOperator::Add, &Value::Bool(true), &Value::Int(1)).unwrap_err();
        assert_eq!(
            err,
            Error::InvalidType {
                operation: "arithmetic",
                kind: "bool"
            }
        );
    }

    #[test]
    fn binary_division_by_zero_returns_error() {
        let err = binary_value(BinaryOperator::Div, &Value::Int(5), &Value::Int(0)).unwrap_err();
        assert_eq!(
            err,
            Error::InvalidValue {
                operation: "arithmetic",
                value: "division by zero".into()
            }
        );
        let err = binary_value(BinaryOperator::Modulo, &Value::Int(5), &Value::Int(0)).unwrap_err();
        assert_eq!(
            err,
            Error::InvalidValue {
                operation: "arithmetic",
                value: "division by zero".into()
            }
        );
    }

    #[test]
    fn binary_rejects_mismatched_row_shapes() {
        let left = Evaluated {
            column: Column::new("x", [1_i64, 2]),
            shape: Shape::Rows,
        };
        let right = Evaluated {
            column: Column::new("y", [1_i64, 2, 3]),
            shape: Shape::Rows,
        };
        let err = apply_binary(BinaryOperator::Add, left, right).unwrap_err();
        assert_eq!(
            err,
            Error::LengthMismatch {
                operation: "expression",
                left: 2,
                right: 3
            }
        );
    }

    #[test]
    fn unary_reverse_preserves_shape_and_propagates_null() {
        let frame = Frame::new(vec![Column::new("a", vec![Some(1_i64), None, Some(3)])]).unwrap();
        let result = select(
            &frame,
            &[unary(UnaryOperator::Reverse, Expr::Col("a".into()))],
        )
        .unwrap();
        let col = result.column("a").unwrap();
        assert_eq!(result.height(), 3);
        assert_eq!(
            col.values(),
            vec![Value::Int(3), Value::Null, Value::Int(1)]
        );
    }

    #[test]
    fn unary_boolean_not_validates_type() {
        let frame =
            Frame::new(vec![Column::new("a", vec![Some(true), None, Some(false)])]).unwrap();
        let result = select(&frame, &[unary(UnaryOperator::Not, Expr::Col("a".into()))]).unwrap();
        assert_eq!(
            result.column("a").unwrap().values(),
            vec![Value::Bool(false), Value::Null, Value::Bool(true)]
        );

        let frame = Frame::new(vec![Column::new("a", vec![1_i64])]).unwrap();
        let err = select(&frame, &[unary(UnaryOperator::Not, Expr::Col("a".into()))]).unwrap_err();
        assert_eq!(
            err,
            Error::InvalidType {
                operation: "not",
                kind: "int"
            }
        );
    }

    #[test]
    fn unary_null_and_nan_predicates_do_not_propagate_null() {
        use crate::dynamic::Value;
        let frame = Frame::new(vec![Column::new(
            "a",
            vec![Value::Null, Value::Int(1), Value::Float(f64::NAN)],
        )])
        .unwrap();
        let result = select(
            &frame,
            &[unary(UnaryOperator::IsNull, Expr::Col("a".into()))],
        )
        .unwrap();
        assert_eq!(
            result.column("a").unwrap().values(),
            vec![Value::Bool(true), Value::Bool(false), Value::Bool(false)]
        );

        let frame = Frame::new(vec![Column::new(
            "a",
            vec![Value::Null, Value::Int(1), Value::Float(f64::NAN)],
        )])
        .unwrap();
        let result = select(
            &frame,
            &[unary(UnaryOperator::IsNan, Expr::Col("a".into()))],
        )
        .unwrap();
        assert_eq!(
            result.column("a").unwrap().values(),
            vec![Value::Bool(false), Value::Bool(false), Value::Bool(true)]
        );
    }

    #[test]
    fn simple_reductions_ignore_nulls_and_become_scalars() {
        let frame = Frame::new(vec![Column::new(
            "a",
            vec![
                Value::Null,
                Value::Int(1),
                Value::Null,
                Value::Int(3),
                Value::Null,
            ],
        )])
        .unwrap();

        let result = select(&frame, &[unary(UnaryOperator::Sum, Expr::Col("a".into()))]).unwrap();
        assert_eq!(result.height(), 1);
        assert_eq!(result.column("a").unwrap().get(0), Some(Value::Int(4)));

        let result = select(
            &frame,
            &[unary(UnaryOperator::Count, Expr::Col("a".into()))],
        )
        .unwrap();
        assert_eq!(result.column("a").unwrap().get(0), Some(Value::UInt(2)));

        let result = select(
            &frame,
            &[unary(UnaryOperator::First, Expr::Col("a".into()))],
        )
        .unwrap();
        assert_eq!(result.column("a").unwrap().get(0), Some(Value::Int(1)));

        let result = select(&frame, &[unary(UnaryOperator::Last, Expr::Col("a".into()))]).unwrap();
        assert_eq!(result.column("a").unwrap().get(0), Some(Value::Int(3)));
    }

    #[test]
    fn empty_reductions_have_defined_results() {
        let frame = Frame::new(vec![Column::new(
            "a",
            vec![Value::Null, Value::Null, Value::Null],
        )])
        .unwrap();

        let result = select(&frame, &[unary(UnaryOperator::Sum, Expr::Col("a".into()))]).unwrap();
        assert_eq!(result.column("a").unwrap().get(0), Some(Value::Null));

        let result = select(
            &frame,
            &[unary(UnaryOperator::First, Expr::Col("a".into()))],
        )
        .unwrap();
        assert_eq!(result.column("a").unwrap().get(0), Some(Value::Null));

        let result = select(&frame, &[unary(UnaryOperator::Last, Expr::Col("a".into()))]).unwrap();
        assert_eq!(result.column("a").unwrap().get(0), Some(Value::Null));

        let result = select(
            &frame,
            &[unary(UnaryOperator::Count, Expr::Col("a".into()))],
        )
        .unwrap();
        assert_eq!(result.column("a").unwrap().get(0), Some(Value::UInt(0)));
    }

    #[test]
    fn unique_is_explicitly_deferred() {
        let frame = Frame::new(vec![Column::new("a", vec![1_i64])]).unwrap();
        let err = evaluate_shaped(&frame, &unary(UnaryOperator::Unique, Expr::Col("a".into())))
            .unwrap_err();
        assert_eq!(
            err,
            Error::InvalidValue {
                operation: "unary",
                value: "Unique".into()
            }
        );
    }

    #[test]
    fn statistical_reductions_ignore_nulls() {
        let frame = Frame::new(vec![Column::new(
            "a",
            vec![Value::Null, Value::Int(1), Value::Int(3), Value::Null],
        )])
        .unwrap();

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Mean, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.shape, Shape::Scalar);
        assert_eq!(result.column.get(0), Some(Value::Float(2.0)));

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Median, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.shape, Shape::Scalar);
        assert_eq!(result.column.get(0), Some(Value::Float(2.0)));

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Var, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.shape, Shape::Scalar);
        assert_eq!(result.column.get(0), Some(Value::Float(2.0)));

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Std, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.shape, Shape::Scalar);
        assert_eq!(result.column.get(0), Some(Value::Float(2.0_f64.sqrt())));
    }

    #[test]
    fn statistical_reductions_require_enough_values() {
        let frame = Frame::new(vec![Column::new("a", vec![Value::Null, Value::Null])]).unwrap();

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Mean, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Null));

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Median, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Null));

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Var, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Null));

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Std, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Null));

        let frame = Frame::new(vec![Column::new("a", vec![Value::Int(5)])]).unwrap();

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Mean, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Float(5.0)));

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Median, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Float(5.0)));

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Var, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Null));

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Std, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Null));
    }

    #[test]
    fn extreme_reductions_preserve_selected_values() {
        let frame = Frame::new(vec![Column::new(
            "a",
            vec![
                Value::UInt(1),
                Value::Float(2.5),
                Value::Int(-3),
                Value::Null,
            ],
        )])
        .unwrap();

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Min, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Int(-3)));

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Max, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Float(2.5)));

        let frame = Frame::new(vec![Column::new("a", vec![Value::Null, Value::Null])]).unwrap();

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Min, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Null));

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Max, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Null));
    }

    #[test]
    fn boolean_reductions_ignore_nulls() {
        let frame = Frame::new(vec![Column::new(
            "a",
            vec![Value::Bool(true), Value::Null, Value::Bool(true)],
        )])
        .unwrap();

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::All, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Bool(true)));

        let frame = Frame::new(vec![Column::new(
            "a",
            vec![Value::Bool(false), Value::Null, Value::Bool(true)],
        )])
        .unwrap();

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Any, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Bool(true)));

        let frame = Frame::new(vec![Column::new("a", vec![Value::Null, Value::Null])]).unwrap();

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::All, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Bool(true)));

        let result =
            evaluate_shaped(&frame, &unary(UnaryOperator::Any, Expr::Col("a".into()))).unwrap();
        assert_eq!(result.column.get(0), Some(Value::Bool(false)));
    }

    #[test]
    fn boolean_reductions_reject_non_boolean_values() {
        let frame = Frame::new(vec![Column::new("a", vec![Value::Int(5)])]).unwrap();

        let err =
            evaluate_shaped(&frame, &unary(UnaryOperator::All, Expr::Col("a".into()))).unwrap_err();
        assert_eq!(
            err,
            Error::InvalidType {
                operation: "all",
                kind: "int"
            }
        );

        let err =
            evaluate_shaped(&frame, &unary(UnaryOperator::Any, Expr::Col("a".into()))).unwrap_err();
        assert_eq!(
            err,
            Error::InvalidType {
                operation: "any",
                kind: "int"
            }
        );
    }

    #[test]
    fn conditional_selects_first_true_case_and_broadcasts_branches() {
        let frame = Frame::new(vec![
            Column::new("first", [Some(true), Some(false), None, Some(true)]),
            Column::new("second", [false, true, true, true]),
        ])
        .unwrap();
        let expression = Expr::Conditional(Box::new(ConditionalExpr {
            first_case: ConditionalCase {
                when: Expr::Col("first".into()),
                then: Expr::Literal(Literal::Int("1".into())),
            },
            other_cases: vec![ConditionalCase {
                when: Expr::Col("second".into()),
                then: Expr::Literal(Literal::Int("2".into())),
            }],
            otherwise: Expr::Literal(Literal::Int("3".into())),
        }));
        let result = evaluate_shaped(&frame, &expression).unwrap();
        assert_eq!(result.shape, Shape::Rows);
        assert_eq!(
            result.column.values(),
            vec![Value::Int(1), Value::Int(2), Value::Int(2), Value::Int(1)]
        );
    }

    #[test]
    fn conditional_rejects_non_boolean_conditions() {
        let frame = Frame::new(vec![Column::new("when", [1_i64])]).unwrap();
        let expression = Expr::Conditional(Box::new(ConditionalExpr {
            first_case: ConditionalCase {
                when: Expr::Col("when".into()),
                then: Expr::Literal(Literal::Int("1".into())),
            },
            other_cases: vec![],
            otherwise: Expr::Literal(Literal::Int("0".into())),
        }));
        assert_eq!(
            evaluate_shaped(&frame, &expression).unwrap_err(),
            Error::InvalidType {
                operation: "conditional",
                kind: "int"
            }
        );
    }

    #[test]
    fn casts_preserve_shape_propagate_null_and_validate_strings() {
        let frame = Frame::new(vec![Column::new("number", [Some(2_i64), None])]).unwrap();
        let float = evaluate_shaped(
            &frame,
            &Expr::Cast(Box::new(CastExpr {
                expr: Expr::Col("number".into()),
                ty: Type::Float,
            })),
        )
        .unwrap();
        assert_eq!(float.shape, Shape::Rows);
        assert_eq!(float.column.values(), vec![Value::Float(2.0), Value::Null]);
        let string = evaluate_shaped(
            &frame,
            &Expr::Cast(Box::new(CastExpr {
                expr: Expr::Col("number".into()),
                ty: Type::Str,
            })),
        )
        .unwrap();
        assert_eq!(string.column.values(), vec![Value::from("2"), Value::Null]);
        assert_eq!(
            cast_value(&Value::from("7"), Type::UInt).unwrap(),
            Value::UInt(7)
        );
        assert_eq!(
            cast_value(&Value::Bool(true), Type::Int).unwrap(),
            Value::Int(1)
        );
        assert_eq!(
            cast_value(&Value::from("not-a-number"), Type::Float).unwrap_err(),
            Error::InvalidValue {
                operation: "cast",
                value: "not-a-number".into()
            }
        );
    }

    #[test]
    fn selector_expansion_preserves_order_and_honors_exclusions() {
        let frame = Frame::new(vec![
            Column::new("a", [1_i64, 2]),
            Column::new("b", [3_i64, 4]),
            Column::new("c", [5_i64, 6]),
        ])
        .unwrap();
        assert_eq!(
            expand_selectors(&frame, &[Expr::Col("*".into())], &["b".into()]),
            vec![Expr::Col("a".into()), Expr::Col("c".into())]
        );
        let result = select(
            &frame,
            &[Expr::Exclude(ExcludeExpr {
                columns: vec!["b".into()],
            })],
        )
        .unwrap();
        assert_eq!(
            result
                .columns()
                .iter()
                .map(|column| column.name())
                .collect::<Vec<_>>(),
            vec!["a", "c"]
        );
        assert_eq!(
            result.column("a").unwrap().values(),
            vec![Value::Int(1), Value::Int(2)]
        );
        assert_eq!(
            result.column("c").unwrap().values(),
            vec![Value::Int(5), Value::Int(6)]
        );
    }

    #[test]
    fn selector_expands_inside_nested_expressions() {
        let frame = Frame::new(vec![
            Column::new("a", [1_i64, 2]),
            Column::new("b", [3_i64, 4]),
        ])
        .unwrap();
        let expression = Expr::Cast(Box::new(CastExpr {
            expr: binary(
                BinaryOperator::Add,
                Expr::Col("*".into()),
                Expr::Literal(Literal::Int("10".into())),
            ),
            ty: Type::Float,
        }));
        let result = select(&frame, &[expression]).unwrap();
        assert_eq!(
            result.column("a").unwrap().values(),
            vec![Value::Float(11.0), Value::Float(12.0)]
        );
        assert_eq!(
            result.column("b").unwrap().values(),
            vec![Value::Float(13.0), Value::Float(14.0)]
        );
    }
}

use crate::sql::expr::{Expr, StrExpr};

use crate::backend::dynamic::{Column, Error, Result, Value, ValueType};

pub(super) fn map_column(
    column: Column,
    value_type: Option<ValueType>,
    mut function: impl FnMut(&Value) -> Result<Value>,
) -> Result<Column> {
    map_all(column, value_type, |val| {
        if matches!(val, Value::Null) {
            Ok(Value::Null)
        } else {
            function(val)
        }
    })
}

pub(super) fn map_all(
    column: Column,
    value_type: Option<ValueType>,
    mut function: impl FnMut(&Value) -> Result<Value>,
) -> Result<Column> {
    let name = column.name().to_owned();
    Ok(Column::from_values_with_hint(
        name,
        column
            .iter()
            .map(|val| function(&val))
            .collect::<Result<_>>()?,
        value_type,
    ))
}

pub(super) fn invalid_value(operation: &'static str, value: impl ToString) -> Error {
    Error::InvalidValue {
        operation,
        value: value.to_string(),
    }
}

pub(super) fn string_parts(expression: &StrExpr) -> (&Expr, &Expr) {
    match expression {
        StrExpr::Contains(value) => (&value.str, &value.pattern),
        StrExpr::Extract(value) => (&value.str, &value.pattern),
        StrExpr::ExtractAll(value) => (&value.str, &value.pattern),
        StrExpr::Split(value) => (&value.str, &value.pattern),
    }
}

pub(super) fn string_parts_mut(expression: &mut StrExpr) -> (&mut Expr, &mut Expr) {
    match expression {
        StrExpr::Contains(value) => (&mut value.str, &mut value.pattern),
        StrExpr::Extract(value) => (&mut value.str, &mut value.pattern),
        StrExpr::ExtractAll(value) => (&mut value.str, &mut value.pattern),
        StrExpr::Split(value) => (&mut value.str, &mut value.pattern),
    }
}

use regex::Regex;

use crate::sql::expr::StrExpr;

use crate::backend::dynamic::{Error, Frame, Result, Value, ValueType};

use super::binary::zip_evaluated;
use super::helpers::string_parts;
use super::{Evaluated, evaluate_shaped};

pub(super) fn evaluate_string(frame: &Frame, expression: &StrExpr) -> Result<Evaluated> {
    let (string, pattern) = string_parts(expression);
    let strings = evaluate_shaped(frame, string)?;
    let patterns = evaluate_shaped(frame, pattern)?;
    let name = strings.column.name().to_owned();
    let value_type = Some(match expression {
        StrExpr::Contains(_) => ValueType::Bool,
        StrExpr::Extract(_) => ValueType::String,
        StrExpr::ExtractAll(_) | StrExpr::Split(_) => ValueType::List,
    });
    zip_evaluated(name, value_type, strings, patterns, |string, pattern| {
        if matches!(string, Value::Null) || matches!(pattern, Value::Null) {
            return Ok(Value::Null);
        }
        let string = string.string("string expression")?.unwrap();
        let pattern = pattern.string("string expression")?.unwrap();
        if matches!(expression, StrExpr::Split(_)) && pattern.is_empty() {
            return Ok(Value::List(
                string
                    .chars()
                    .map(|val| Value::String(val.to_string().into()))
                    .collect::<Vec<_>>()
                    .into(),
            ));
        }
        let regex = Regex::new(pattern).map_err(|error| Error::InvalidRegex {
            pattern: pattern.into(),
            message: error.to_string(),
        })?;
        Ok(match expression {
            StrExpr::Contains(_) => Value::Bool(regex.is_match(string)),
            StrExpr::Extract(value) => regex
                .captures(string)
                .and_then(|captures| captures.get(value.group))
                .map(|val| Value::String(val.as_str().into()))
                .unwrap_or_default(),
            StrExpr::ExtractAll(_) => Value::List(
                regex
                    .find_iter(string)
                    .map(|val| Value::String(val.as_str().into()))
                    .collect::<Vec<_>>()
                    .into(),
            ),
            StrExpr::Split(_) => Value::List(
                regex
                    .split(string)
                    .map(|val| Value::String(val.into()))
                    .collect::<Vec<_>>()
                    .into(),
            ),
        })
    })
}

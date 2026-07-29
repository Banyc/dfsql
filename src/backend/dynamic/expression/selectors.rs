use crate::sql::expr::Expr;

use crate::backend::dynamic::{Frame, Result};

use super::helpers::{string_parts, string_parts_mut};
use super::{combined_len, evaluate_shaped};

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

use crate::sql::expr::{Expr, StandaloneOperator};
use crate::sql::lexer::Literal;

use super::{Column, Error, Frame, Result, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Shape {
    Scalar,
    Rows,
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
        Expr::Literal(_) => "literal".into(),
        Expr::Alias(value) => value.name.clone(),
        Expr::Standalone(_) => "len".into(),
        _ => "expression".into(),
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
        Expr::Alias(value) => {
            let evaluated = evaluate_shaped(frame, &value.expr)?;
            (evaluated.column.rename(value.name.clone()), evaluated.shape)
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

pub(crate) fn select(frame: &Frame, expressions: &[Expr]) -> Result<Frame> {
    let evaluated = expressions
        .iter()
        .map(|expression| evaluate_shaped(frame, expression))
        .collect::<Result<Vec<_>>>()?;
    let height = if evaluated.is_empty() {
        0
    } else if evaluated.iter().any(|val| val.shape == Shape::Rows) {
        frame.height()
    } else {
        1
    };
    let columns = evaluated
        .into_iter()
        .map(|val| val.materialize(height, "select"))
        .collect::<Result<Vec<_>>>()?;
    Frame::with_height(columns, height)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::expr::{AliasExpr, StandaloneExpr};

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
}

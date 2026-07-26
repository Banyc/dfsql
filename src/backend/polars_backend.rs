use std::{collections::HashMap, ops::Neg};

use polars::prelude::*;
use thiserror::Error;

use crate::backend::dynamic;
use crate::backend::dynamic::ColumnData;
use crate::sql::{self, SortOrder};

#[derive(Debug, Error)]
pub(super) enum Error {
    #[error("PolarsError: {0}")]
    Collect(#[from] PolarsError),
    #[error("conversion error: {0}")]
    Conversion(String),
    #[error("data frame does not exist: {0}")]
    FrameNotFound(String),
}

pub(super) struct Executor {
    frame_name: String,
    input: HashMap<String, super::Frame>,
}

impl Executor {
    pub(super) fn from_frame(frame_name: impl Into<String>, frame: super::Frame) -> Self {
        let frame_name = frame_name.into();
        Self {
            input: HashMap::from([(frame_name.clone(), frame)]),
            frame_name,
        }
    }

    pub(super) fn new(
        frame_name: impl Into<String>,
        input: HashMap<String, super::Frame>,
    ) -> Option<Self> {
        let frame_name = frame_name.into();
        input
            .contains_key(&frame_name)
            .then_some(Self { frame_name, input })
    }

    pub(super) fn input(&self) -> &HashMap<String, super::Frame> {
        &self.input
    }

    pub(super) fn into_input(self) -> HashMap<String, super::Frame> {
        self.input
    }

    pub(super) fn insert_frame(
        &mut self,
        frame_name: impl Into<String>,
        frame: super::Frame,
    ) -> Option<super::Frame> {
        self.input.insert(frame_name.into(), frame)
    }

    pub(super) fn frame_name(&self) -> &str {
        &self.frame_name
    }

    pub(super) fn frame(&self) -> &super::Frame {
        &self.input[&self.frame_name]
    }

    pub(super) fn frame_mut(&mut self) -> &mut super::Frame {
        self.input
            .get_mut(&self.frame_name)
            .expect("the active frame is always present")
    }

    pub(super) fn set_frame_name(&mut self, frame_name: impl Into<String>) -> Result<(), Error> {
        let frame_name = frame_name.into();
        if !self.input.contains_key(&frame_name) {
            return Err(Error::FrameNotFound(frame_name));
        }
        self.frame_name = frame_name;
        Ok(())
    }

    pub(super) fn set_frame(&mut self, frame: super::Frame) {
        self.input.insert(self.frame_name.clone(), frame);
    }

    pub(super) fn execute(&mut self, statements: &sql::S) -> Result<(), Error> {
        let mut frame = self.frame().inner().clone();
        for stat in &statements.statements {
            frame = apply_stat(frame, stat, &mut self.input)?;
            if let sql::stat::Stat::Use(r#use) = stat {
                self.set_frame_name(r#use.df_name.clone())?;
            }
            self.set_frame(super::Frame::from_inner(frame.clone()));
        }
        Ok(())
    }

    pub(super) fn collect(&self) -> Result<super::MaterializedFrame, Error> {
        let df = self.frame().inner().clone().collect()?;
        Ok(super::MaterializedFrame::from_inner(df))
    }
}

fn apply_stat(
    df: LazyFrame,
    stat: &sql::stat::Stat,
    others: &mut HashMap<String, super::Frame>,
) -> Result<LazyFrame, Error> {
    Ok(match stat {
        sql::stat::Stat::Select(select) => {
            let columns: Vec<_> = select.columns.iter().map(convert_expr).collect();
            df.select(columns)
        }
        sql::stat::Stat::GroupAgg(group_agg) => {
            let group_by: Vec<_> = group_agg.group_by.iter().map(String::as_str).collect();
            let agg: Vec<_> = group_agg.agg.iter().map(convert_expr).collect();
            df.group_by(group_by).agg(agg)
        }
        sql::stat::Stat::Filter(filter) => {
            let condition = convert_expr(&filter.condition);
            df.filter(condition)
        }
        sql::stat::Stat::Limit(limit) => {
            let rows = limit.rows.parse().unwrap();
            df.limit(rows)
        }
        sql::stat::Stat::Reverse => df.reverse(),
        sql::stat::Stat::Sort(sort) => {
            let columns: Vec<_> = sort.pairs.iter().map(|(_, c)| c).collect();
            let descending = sort.pairs.iter().map(|(o, _)| matches!(o, SortOrder::Desc));
            let options = SortMultipleOptions::default().with_order_descending_multi(descending);
            df.sort(columns, options)
        }
        sql::stat::Stat::Join(join) => match join {
            sql::stat::JoinStat::SingleCol(join) => {
                let other = others
                    .get(&join.other)
                    .ok_or_else(|| Error::FrameNotFound(join.other.to_string()))?
                    .inner()
                    .clone();
                let left_on = convert_expr(&join.left_on);
                let right_on = match &join.right_on {
                    Some(right_on) => convert_expr(right_on),
                    None => left_on.clone(),
                };
                match join.ty {
                    sql::stat::SingleColJoinType::Left => df.left_join(other, left_on, right_on),
                    sql::stat::SingleColJoinType::Right => other.left_join(df, right_on, left_on),
                    sql::stat::SingleColJoinType::Inner => df.inner_join(other, left_on, right_on),
                    sql::stat::SingleColJoinType::Full => df.full_join(other, left_on, right_on),
                }
            }
        },
        sql::stat::Stat::Use(r#use) => others
            .get(&r#use.df_name)
            .ok_or_else(|| Error::FrameNotFound(r#use.df_name.clone()))?
            .inner()
            .clone(),
        sql::stat::Stat::Clone(clone) => {
            let df_clone = df.clone();
            others.insert(clone.df_name.clone(), super::Frame::from_inner(df_clone));
            df
        }
    })
}

fn convert_expr(expr: &sql::expr::Expr) -> polars::lazy::dsl::Expr {
    match expr {
        sql::expr::Expr::Col(name) => col(name),
        sql::expr::Expr::Exclude(exclude) => all().exclude_cols(&exclude.columns).as_expr(),
        sql::expr::Expr::Literal(literal) => match literal {
            sql::lexer::Literal::String(string) => lit(string.clone()),
            sql::lexer::Literal::Int(number) => lit(number.parse::<i64>().unwrap()),
            sql::lexer::Literal::Float(number) => lit(number.parse::<f64>().unwrap()),
            sql::lexer::Literal::Bool(bool) => lit(*bool),
            sql::lexer::Literal::Null => lit(NULL),
        },
        sql::expr::Expr::Binary(binary) => {
            let left = convert_expr(&binary.left);
            let right = convert_expr(&binary.right);
            match binary.operator {
                sql::expr::BinaryOperator::Add => left + right,
                sql::expr::BinaryOperator::Sub => left - right,
                sql::expr::BinaryOperator::Mul => left * right,
                sql::expr::BinaryOperator::Div => left / right,
                sql::expr::BinaryOperator::Modulo => left % right,
                sql::expr::BinaryOperator::Eq => left.eq(right),
                sql::expr::BinaryOperator::NotEq => left.neq(right),
                sql::expr::BinaryOperator::LtEq => left.lt_eq(right),
                sql::expr::BinaryOperator::Lt => left.lt(right),
                sql::expr::BinaryOperator::GtEq => left.gt_eq(right),
                sql::expr::BinaryOperator::Gt => left.gt(right),
                sql::expr::BinaryOperator::And => left.and(right),
                sql::expr::BinaryOperator::Or => left.or(right),
                sql::expr::BinaryOperator::Pow => left.pow(right),
            }
        }
        sql::expr::Expr::Unary(unary) => {
            let expr = convert_expr(&unary.expr);
            match unary.operator {
                sql::expr::UnaryOperator::Neg => expr.neg(),
                sql::expr::UnaryOperator::Not => expr.not(),
                sql::expr::UnaryOperator::Abs => expr.abs(),
                sql::expr::UnaryOperator::Sum => expr.sum(),
                sql::expr::UnaryOperator::Sqrt => expr.sqrt(),
                sql::expr::UnaryOperator::Count => expr.count(),
                sql::expr::UnaryOperator::First => expr.first(),
                sql::expr::UnaryOperator::Last => expr.last(),
                sql::expr::UnaryOperator::Reverse => expr.reverse(),
                sql::expr::UnaryOperator::Mean => expr.mean(),
                sql::expr::UnaryOperator::Median => expr.median(),
                sql::expr::UnaryOperator::Max => expr.max(),
                sql::expr::UnaryOperator::Min => expr.min(),
                sql::expr::UnaryOperator::Var => expr.var(0),
                sql::expr::UnaryOperator::Std => expr.std(0),
                sql::expr::UnaryOperator::Unique => expr.unique(),
                sql::expr::UnaryOperator::IsNull => expr.is_null(),
                sql::expr::UnaryOperator::IsNan => expr.is_nan(),
                sql::expr::UnaryOperator::All => expr.all(false),
                sql::expr::UnaryOperator::Any => expr.any(false),
            }
        }
        sql::expr::Expr::Standalone(standalone) => match standalone.operator {
            sql::expr::StandaloneOperator::Len => len(),
        },
        sql::expr::Expr::SortBy(sort_by) => {
            let columns: Vec<_> = sort_by.pairs.iter().map(|(_, c)| convert_expr(c)).collect();
            let descending = sort_by
                .pairs
                .iter()
                .map(|(o, _)| matches!(o, SortOrder::Desc));
            let expr = convert_expr(&sort_by.expr);
            let options = SortMultipleOptions::default().with_order_descending_multi(descending);
            expr.sort_by(columns, options)
        }
        sql::expr::Expr::Sort(sort) => {
            let expr = convert_expr(&sort.expr);
            let options =
                SortOptions::default().with_order_descending(matches!(sort.order, SortOrder::Desc));
            expr.sort(options)
        }
        sql::expr::Expr::Alias(alias) => {
            let expr = convert_expr(&alias.expr);
            expr.alias(&alias.name)
        }
        sql::expr::Expr::Conditional(conditional) => {
            #[allow(clippy::large_enum_variant)]
            enum Case {
                Then(polars::lazy::dsl::Then),
                ChainedThen(polars::lazy::dsl::ChainedThen),
            }
            let when_expr = convert_expr(&conditional.first_case.when);
            let then_expr = convert_expr(&conditional.first_case.then);
            let mut case = Case::Then(when(when_expr).then(then_expr));
            for case_expr in &conditional.other_cases {
                let when_expr = convert_expr(&case_expr.when);
                let then_expr = convert_expr(&case_expr.then);
                case = Case::ChainedThen(match case {
                    Case::Then(case) => case.when(when_expr).then(then_expr),
                    Case::ChainedThen(case) => case.when(when_expr).then(then_expr),
                });
            }
            let otherwise = convert_expr(&conditional.otherwise);
            match case {
                Case::Then(case) => case.otherwise(otherwise),
                Case::ChainedThen(case) => case.otherwise(otherwise),
            }
        }
        sql::expr::Expr::Cast(cast) => {
            let ty = match cast.ty {
                sql::lexer::Type::Str => DataType::String,
                sql::lexer::Type::UInt => DataType::UInt64,
                sql::lexer::Type::Int => DataType::Int64,
                sql::lexer::Type::Float => DataType::Float64,
            };
            let expr = convert_expr(&cast.expr);
            expr.cast(ty)
        }
        sql::expr::Expr::Log(log) => {
            let expr = convert_expr(&log.expr);
            expr.log(lit(log.base))
        }
        sql::expr::Expr::Str(str) => match str.as_ref() {
            sql::expr::StrExpr::Contains(contains) => {
                let str = convert_expr(&contains.str);
                let pattern = convert_expr(&contains.pattern);
                str.str().contains(pattern, true)
            }
            sql::expr::StrExpr::Extract(extract) => {
                let str = convert_expr(&extract.str);
                let pattern = convert_expr(&extract.pattern);
                str.str().extract(pattern, extract.group)
            }
            sql::expr::StrExpr::ExtractAll(extract_all) => {
                let str = convert_expr(&extract_all.str);
                let pattern = convert_expr(&extract_all.pattern);
                str.str().extract_all(pattern)
            }
            sql::expr::StrExpr::Split(split) => {
                let str = convert_expr(&split.str);
                let pattern = convert_expr(&split.pattern);
                str.str().split(pattern)
            }
        },
    }
}

pub(super) fn frame_from_dynamic(frame: dynamic::Frame) -> std::result::Result<LazyFrame, Error> {
    let height = frame.height();
    let columns = frame
        .into_columns()
        .into_iter()
        .map(|column| {
            let name = column.name().into();
            let column = match column.into_data() {
                ColumnData::Bool(values) => Column::new(name, values),
                ColumnData::UInt(values) => Column::new(name, values),
                ColumnData::Int(values) => Column::new(name, values),
                ColumnData::Float(values) => Column::new(name, values),
                ColumnData::String(values) => {
                    let values = values
                        .into_iter()
                        .map(|value| {
                            value.map_or(AnyValue::Null, |value| {
                                AnyValue::StringOwned(value.as_ref().into())
                            })
                        })
                        .collect::<Vec<_>>();
                    Series::from_any_values(name, &values, true)?.into_column()
                }
                ColumnData::List(values) => {
                    let values = values
                        .into_iter()
                        .map(|value| match value {
                            Some(value) => value_to_any(dynamic::Value::List(value)),
                            None => Ok(AnyValue::Null),
                        })
                        .collect::<std::result::Result<Vec<_>, Error>>()?;
                    Series::from_any_values(name, &values, false)?.into_column()
                }
                ColumnData::Mixed(values) => {
                    let values = values
                        .into_iter()
                        .map(value_to_any)
                        .collect::<std::result::Result<Vec<_>, Error>>()?;
                    Series::from_any_values(name, &values, false)?.into_column()
                }
            };
            Ok(column)
        })
        .collect::<std::result::Result<Vec<_>, Error>>()?;
    Ok(DataFrame::new(height, columns)?.lazy())
}

pub(super) fn frame_to_dynamic(frame: &DataFrame) -> std::result::Result<dynamic::Frame, Error> {
    let columns = frame
        .columns()
        .iter()
        .map(|column| {
            let name = column.name().to_string();
            let data = match column.dtype() {
                DataType::Boolean => ColumnData::Bool(column.bool()?.iter().collect()),
                DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::UInt128 => {
                    let column = column.cast(&DataType::UInt64)?;
                    ColumnData::UInt(column.u64()?.iter().collect())
                }
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Int128 => {
                    let column = column.cast(&DataType::Int64)?;
                    ColumnData::Int(column.i64()?.iter().collect())
                }
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    let column = column.cast(&DataType::Float64)?;
                    ColumnData::Float(column.f64()?.iter().collect())
                }
                DataType::String => ColumnData::String(
                    column
                        .str()?
                        .iter()
                        .map(|value| value.map(Into::into))
                        .collect(),
                ),
                DataType::List(_) => {
                    let column = column.list()?;
                    let values = (0..column.len())
                        .map(|index| {
                            column
                                .get_as_series(index)
                                .map(|series| {
                                    series
                                        .iter()
                                        .map(value_from_any)
                                        .collect::<std::result::Result<Vec<_>, Error>>()
                                })
                                .transpose()
                        })
                        .collect::<std::result::Result<Vec<_>, Error>>()?;
                    ColumnData::List(
                        values
                            .into_iter()
                            .map(|value| value.map(Into::into))
                            .collect(),
                    )
                }
                _ => ColumnData::Mixed(
                    (0..column.len())
                        .map(|index| value_from_any(column.get(index)?))
                        .collect::<std::result::Result<Vec<_>, Error>>()?,
                ),
            };
            Ok(dynamic::Column::from_data(name, data))
        })
        .collect::<std::result::Result<Vec<_>, Error>>()?;
    dynamic::Frame::new(columns).map_err(|error| Error::Conversion(error.to_string()))
}

fn value_to_any(value: dynamic::Value) -> std::result::Result<AnyValue<'static>, Error> {
    Ok(match value {
        dynamic::Value::Null => AnyValue::Null,
        dynamic::Value::Bool(v) => AnyValue::Boolean(v),
        dynamic::Value::UInt(v) => AnyValue::UInt64(v),
        dynamic::Value::Int(v) => AnyValue::Int64(v),
        dynamic::Value::Float(v) => AnyValue::Float64(v),
        dynamic::Value::String(v) => AnyValue::StringOwned(v.to_string().into()),
        dynamic::Value::List(v) => {
            let values = v
                .iter()
                .map(|v| value_to_any(v.clone()))
                .collect::<std::result::Result<Vec<_>, Error>>()?;
            AnyValue::List(Series::from_any_values("".into(), &values, false)?)
        }
    })
}

fn value_from_any(any: AnyValue) -> std::result::Result<dynamic::Value, Error> {
    Ok(match any {
        AnyValue::Null => dynamic::Value::Null,
        AnyValue::Boolean(v) => dynamic::Value::Bool(v),
        AnyValue::UInt64(v) => dynamic::Value::UInt(v),
        AnyValue::Int64(v) => dynamic::Value::Int(v),
        AnyValue::Float64(v) => dynamic::Value::Float(v),
        AnyValue::String(v) => dynamic::Value::String(v.to_string().into()),
        AnyValue::StringOwned(v) => dynamic::Value::String(v.to_string().into()),
        AnyValue::List(s) => {
            let values = s
                .iter()
                .map(value_from_any)
                .collect::<std::result::Result<Vec<_>, Error>>()?;
            dynamic::Value::List(values.into())
        }
        _ => {
            return Err(Error::Collect(PolarsError::ComputeError(
                "unsupported AnyValue variant".into(),
            )));
        }
    })
}

#[rustfmt::skip]
#[cfg(test)]
mod tests {
    use super::*;

    /// ref: <https://github.com/pola-rs/polars/issues/22733>
    #[test]
    fn test_i128() {
        let s = "filter x = 0";
        let s = sql::parse(s).unwrap();
        let df = df!("x" => [0, 1]).unwrap();
        let mut executor = Executor::new("a".to_string(), HashMap::from_iter([("a".to_string(), super::super::Frame::from_inner(df.lazy()))])).unwrap();
        executor.execute(&s).unwrap();
        executor.collect().unwrap();
    }

    #[test]
    fn selector_exclusion_and_log_use_current_polars_expressions() {
      let frame = df!("x" => [1.0_f64, 10.0], "drop" => [false, true]).unwrap().lazy();
      let mut executor = Executor::from_frame("input", super::super::Frame::from_inner(frame));
      executor.execute(&sql::parse("select exclude drop alias log_x log 10 x").unwrap()).unwrap();
      let output = executor.collect().unwrap();
      assert_eq!(output.width(), 2);
      assert!(output.inner.column("x").is_ok());
      assert!(output.inner.column("drop").is_err());
      assert_eq!(
        output.inner.column("log_x").unwrap().f64().unwrap().into_no_null_iter().collect::<Vec<_>>(),
        [0.0, 1.0]
      );
    }

    #[test]
    fn right_join_uses_each_side_key_after_swapping_inputs() {
        let left = df!("left_id" => [1, 2], "left_value" => ["one", "two"]).unwrap().lazy();
        let right = df!("right_id" => [2, 3], "right_value" => ["two", "three"]).unwrap().lazy();
        let mut executor = Executor::new("left".to_string(),
            HashMap::from_iter([("left".to_string(), super::super::Frame::from_inner(left)), ("other".to_string(), super::super::Frame::from_inner(right))])).unwrap();
        executor.execute(&sql::parse("right join other on left_id right_id").unwrap()).unwrap();
        let joined = executor.collect().unwrap();
        assert_eq!(joined.height(), 2);
        assert_eq!(joined.inner.column("right_id").unwrap().i32().unwrap().into_no_null_iter().collect::<Vec<_>>(), [2, 3]);
    }
}

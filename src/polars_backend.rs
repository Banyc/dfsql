use std::{collections::HashMap, ops::Neg};

use polars::prelude::*;
use thiserror::Error;

use crate::sql::{self, SortOrder};

pub type Frame = LazyFrame;
pub type MaterializedFrame = DataFrame;
pub type Result<T> = std::result::Result<T, Error>;

pub struct Executor {
    frame_name: String,
    input: HashMap<String, Frame>,
}

impl Executor {
    pub fn from_frame(frame_name: impl Into<String>, frame: Frame) -> Self {
        let frame_name = frame_name.into();
        Self {
            input: HashMap::from([(frame_name.clone(), frame)]),
            frame_name,
        }
    }

    pub fn new(frame_name: impl Into<String>, input: HashMap<String, Frame>) -> Option<Self> {
        let frame_name = frame_name.into();
        input
            .contains_key(&frame_name)
            .then_some(Self { frame_name, input })
    }

    pub fn input(&self) -> &HashMap<String, Frame> {
        &self.input
    }

    pub fn into_input(self) -> HashMap<String, Frame> {
        self.input
    }

    pub fn insert_frame(&mut self, frame_name: impl Into<String>, frame: Frame) -> Option<Frame> {
        self.input.insert(frame_name.into(), frame)
    }

    pub fn frame_name(&self) -> &str {
        &self.frame_name
    }

    pub fn frame(&self) -> &Frame {
        &self.input[&self.frame_name]
    }

    pub fn frame_mut(&mut self) -> &mut Frame {
        self.input
            .get_mut(&self.frame_name)
            .expect("the active frame is always present")
    }

    pub fn set_frame_name(&mut self, frame_name: impl Into<String>) -> Result<()> {
        let frame_name = frame_name.into();
        if !self.input.contains_key(&frame_name) {
            return Err(Error::FrameNotFound(frame_name));
        }
        self.frame_name = frame_name;
        Ok(())
    }

    pub fn set_frame(&mut self, frame: Frame) {
        self.input.insert(self.frame_name.clone(), frame);
    }

    pub fn execute(&mut self, statements: &sql::S) -> Result<()> {
        let mut frame = self.frame().clone();
        for stat in &statements.statements {
            frame = apply_stat(frame, stat, &mut self.input)?;
            if let sql::stat::Stat::Use(r#use) = stat {
                self.set_frame_name(r#use.df_name.clone())?;
            }
            self.set_frame(frame.clone());
        }
        Ok(())
    }

    pub fn collect(&self) -> Result<MaterializedFrame> {
        self.frame().clone().collect().map_err(Error::from)
    }
}

fn apply_stat(
    df: Frame,
    stat: &sql::stat::Stat,
    others: &mut HashMap<String, Frame>,
) -> Result<Frame> {
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
        sql::stat::Stat::Use(r#use) => {
            let df_name = &r#use.df_name;
            others
                .get(df_name)
                .ok_or_else(|| Error::FrameNotFound(df_name.clone()))?
                .clone()
        }
        sql::stat::Stat::Clone(clone) => {
            let df_name = &clone.df_name;
            let df_clone = df.clone();
            others.insert(df_name.into(), df_clone);
            df
        }
    })
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("LazyFrame::collect: {0}")]
    Collect(#[from] PolarsError),
    #[error("data frame does not exist: {0}")]
    FrameNotFound(String),
}

fn convert_expr(expr: &sql::expr::Expr) -> polars::lazy::dsl::Expr {
    match expr {
        sql::expr::Expr::Col(name) => col(name),
        sql::expr::Expr::Exclude(exclude) => {
            let any = col("*");
            any.exclude(&exclude.columns)
        }
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
            expr.log(log.base)
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
        let mut executor = Executor::new("a".to_string(), HashMap::from_iter([("a".to_string(), df.lazy())])).unwrap();
        executor.execute(&s).unwrap();
        executor.collect().unwrap();
    }

    #[test]
    fn right_join_uses_each_side_key_after_swapping_inputs() {
        let left = df!("left_id" => [1, 2], "left_value" => ["one", "two"]).unwrap().lazy();
        let right = df!("right_id" => [2, 3], "right_value" => ["two", "three"]).unwrap().lazy();
        let mut executor = Executor::new("left".to_string(),
            HashMap::from_iter([("left".to_string(), left), ("other".to_string(), right)])).unwrap();
        executor.execute(&sql::parse("right join other on left_id right_id").unwrap()).unwrap();
        let joined = executor.collect().unwrap();
        assert_eq!(joined.height(), 2);
        assert_eq!(joined.column("right_id").unwrap().i32().unwrap().into_no_null_iter().collect::<Vec<_>>(), [2, 3]);
    }
}

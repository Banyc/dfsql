use std::collections::HashMap;

use polars::prelude::*;
use thiserror::Error;

use crate::sql::{self, SortOrder};

pub struct DfExecutor {
    df_name: String,
    input: HashMap<String, LazyFrame>,
}
impl DfExecutor {
    pub fn new(df_name: String, input: HashMap<String, LazyFrame>) -> Option<Self> {
        input.get(&df_name)?;
        Some(Self { df_name, input })
    }

    pub fn input(&self) -> &HashMap<String, LazyFrame> {
        &self.input
    }

    pub fn df_name(&self) -> &String {
        &self.df_name
    }

    pub fn df(&self) -> &LazyFrame {
        &self.input[&self.df_name]
    }

    pub fn set_df_name(&mut self, df_name: String) -> Result<(), DfNotExists> {
        self.input.get(&df_name).ok_or(DfNotExists)?;
        self.df_name = df_name;
        Ok(())
    }

    pub fn set_df(&mut self, df: LazyFrame) {
        *self.input.get_mut(&self.df_name).unwrap() = df;
    }

    pub fn execute(&mut self, s: &sql::S) -> Result<(), ApplyStatError> {
        let mut df = self.df().clone();
        for stat in &s.statements {
            df = apply_stat(df, stat, &mut self.input)?;
            if let sql::stat::Stat::Use(r#use) = stat {
                self.df_name = r#use.df_name.clone();
            }
        }
        *self.input.get_mut(&self.df_name).unwrap() = df;
        Ok(())
    }
}
#[derive(Debug, Error, Clone)]
#[error("Data frame does not exist")]
pub struct DfNotExists;

fn apply_stat(
    df: LazyFrame,
    stat: &sql::stat::Stat,
    others: &mut HashMap<String, LazyFrame>,
) -> Result<LazyFrame, ApplyStatError> {
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
            let options = SortOptions {
                descending: matches!(sort.order, SortOrder::Desc),
                ..Default::default()
            };
            df.sort(&sort.column, options)
        }
        sql::stat::Stat::Join(join) => match join {
            sql::stat::JoinStat::SingleCol(join) => {
                let other = others
                    .get(&join.other)
                    .ok_or_else(|| ApplyStatError::DfNotExists(join.other.to_string()))?
                    .clone();
                // let left_on: Vec<_> = join.left_on.iter().map(convert_expr).collect();
                // let right_on: Vec<_> = join.right_on.iter().map(convert_expr).collect();
                let left_on = convert_expr(&join.left_on);
                let right_on = match &join.right_on {
                    Some(right_on) => convert_expr(right_on),
                    None => left_on.clone(),
                };
                match join.ty {
                    sql::stat::SingleColJoinType::Left => df.left_join(other, left_on, right_on),
                    sql::stat::SingleColJoinType::Right => other.left_join(df, left_on, right_on),
                    sql::stat::SingleColJoinType::Inner => df.inner_join(other, left_on, right_on),
                    sql::stat::SingleColJoinType::Outer => df.outer_join(other, left_on, right_on),
                }
            }
        },
        sql::stat::Stat::Use(r#use) => {
            let df_name = &r#use.df_name;
            others
                .get(df_name)
                .ok_or_else(|| ApplyStatError::DfNotExists(df_name.clone()))?
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
pub enum ApplyStatError {
    #[error("LazyFrame::collect: {0}")]
    DfCollect(#[from] PolarsError),
    #[error("LazyFrame not exists: {0}")]
    DfNotExists(String),
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
                sql::expr::UnaryOperator::Neg => expr.clone() - expr.clone() - expr,
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
            sql::expr::StandaloneOperator::Count => count(),
        },
        sql::expr::Expr::SortBy(sort_by) => {
            let columns: Vec<_> = sort_by.pairs.iter().map(|(c, _)| convert_expr(c)).collect();
            let descending: Vec<_> = sort_by
                .pairs
                .iter()
                .map(|(_, o)| matches!(o, SortOrder::Desc))
                .collect();
            let expr = convert_expr(&sort_by.expr);
            expr.sort_by(columns, descending)
        }
        sql::expr::Expr::Sort(sort) => {
            let expr = convert_expr(&sort.expr);
            expr.sort(matches!(sort.order, SortOrder::Desc))
        }
        sql::expr::Expr::Alias(alias) => {
            let expr = convert_expr(&alias.expr);
            expr.alias(&alias.name)
        }
        sql::expr::Expr::Conditional(conditional) => {
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
                str.str().extract(&extract.pattern, extract.group)
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

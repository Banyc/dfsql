use polars::prelude::*;

use crate::sql;

pub fn apply(df: LazyFrame, s: &sql::S) -> LazyFrame {
    s.statements.iter().fold(df, apply_stat)
}

fn apply_stat(df: LazyFrame, stat: &sql::Stat) -> LazyFrame {
    match stat {
        sql::Stat::Select(select) => {
            let columns: Vec<_> = select.columns.iter().map(convert_expr).collect();
            df.select(columns)
        }
        sql::Stat::GroupAgg(group_agg) => {
            let group_by: Vec<_> = group_agg.group_by.iter().map(|s| s.as_str()).collect();
            let agg: Vec<_> = group_agg.agg.iter().map(convert_expr).collect();
            df.group_by(group_by).agg(agg)
        }
        sql::Stat::Filter(filter) => {
            let condition = convert_expr(&filter.condition);
            df.filter(condition)
        }
        sql::Stat::Limit(limit) => {
            let rows = limit.rows.parse().unwrap();
            df.limit(rows)
        }
        sql::Stat::Reverse => df.reverse(),
        sql::Stat::Sort(sort) => df.sort(&sort.column, Default::default()),
    }
}

fn convert_expr(expr: &sql::Expr) -> polars::lazy::dsl::Expr {
    match expr {
        sql::Expr::Col(name) => col(name),
        sql::Expr::Exclude(exclude) => {
            let any = col("*");
            any.exclude(&exclude.columns)
        }
        sql::Expr::Literal(literal) => match literal {
            sql::Literal::String(string) => lit(string.clone()),
            sql::Literal::Int(number) => lit(number.parse::<i64>().unwrap()),
            sql::Literal::Float(number) => lit(number.parse::<f64>().unwrap()),
            sql::Literal::Bool(bool) => lit(*bool),
        },
        sql::Expr::Binary(binary) => {
            let left = convert_expr(&binary.left);
            let right = convert_expr(&binary.right);
            match binary.operator {
                sql::BinaryOperator::Add => left + right,
                sql::BinaryOperator::Sub => left - right,
                sql::BinaryOperator::Mul => left * right,
                sql::BinaryOperator::Div => left / right,
                sql::BinaryOperator::Eq => left.eq(right),
                sql::BinaryOperator::LtEq => left.lt_eq(right),
                sql::BinaryOperator::Lt => left.lt(right),
                sql::BinaryOperator::GtEq => left.gt_eq(right),
                sql::BinaryOperator::Gt => left.gt(right),
                sql::BinaryOperator::And => left.and(right),
                sql::BinaryOperator::Or => left.or(right),
            }
        }
        sql::Expr::Unary(unary) => {
            let expr = convert_expr(&unary.expr);
            match unary.operator {
                sql::UnaryOperator::Neg => expr.clone() - expr.clone() - expr,
                sql::UnaryOperator::Not => expr.not(),
            }
        }
        sql::Expr::Agg(agg) => match agg.as_ref() {
            sql::AggExpr::Unary(unary) => {
                let expr = convert_expr(&unary.expr);
                match unary.operator {
                    sql::AggOperator::Sum => expr.sum(),
                    sql::AggOperator::Count => expr.count(),
                }
            }
            sql::AggExpr::Standalone(standalone) => match standalone.operator {
                sql::StandaloneAggOperator::Count => count(),
            },
        },
        sql::Expr::Alias(alias) => {
            let expr = convert_expr(&alias.expr);
            expr.alias(&alias.name)
        }
        sql::Expr::Conditional(conditional) => {
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
        sql::Expr::Cast(cast) => {
            let ty = match cast.ty {
                sql::Type::Str => DataType::Utf8,
                sql::Type::Int => DataType::Int64,
                sql::Type::Float => DataType::Float64,
            };
            let expr = convert_expr(&cast.expr);
            expr.cast(ty)
        }
    }
}

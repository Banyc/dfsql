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
    }
}

fn convert_expr(expr: &sql::Expr) -> polars::lazy::dsl::Expr {
    match expr {
        sql::Expr::Col(name) => col(name),
        sql::Expr::Literal(literal) => match literal {
            sql::Literal::String(string) => lit(string.clone()),
            sql::Literal::Number(number) => {
                if number.contains('.') {
                    lit(number.parse::<f64>().unwrap())
                } else {
                    lit(number.parse::<i64>().unwrap())
                }
            }
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
            }
        }
        sql::Expr::Unary(unary) => {
            let expr = convert_expr(&unary.expr);
            match unary.operator {
                sql::UnaryOperator::Neg => expr.clone() - expr.clone() - expr,
                sql::UnaryOperator::Not => expr.not(),
            }
        }
        sql::Expr::Agg(agg) => {
            let expr = convert_expr(&agg.expr);
            match agg.operator {
                sql::AggOperator::Sum => expr.sum(),
                sql::AggOperator::Count => expr.count(),
            }
        }
        sql::Expr::Alias(alias) => {
            let expr = convert_expr(&alias.expr);
            expr.alias(&alias.name)
        }
    }
}

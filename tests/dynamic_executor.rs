use std::collections::HashMap;

use dfsql::backend::dynamic::{Column, Error, Executor, Frame, Value};
use dfsql::sql::{
    S, SortOrder,
    expr::{Expr, SortByExpr, UnaryExpr, UnaryOperator},
    stat::{
        CloneStat, FilterStat, GroupAggStat, JoinStat, LimitStat, SelectStat, SingleColJoinStat,
        SingleColJoinType, SortStat, Stat, UseStat,
    },
};

fn run(frame: Frame, statements: Vec<Stat>) -> Result<Frame, Error> {
    let mut executor = Executor::from_frame("table", frame);
    executor.execute(&S { statements })?;
    Ok(executor.frame().clone())
}

fn values(frame: &Frame, name: &str) -> Vec<Value> {
    frame.column(name).unwrap().values()
}

#[test]
fn executor_applies_row_and_select_statements_in_order() {
    let frame = Frame::new(vec![
        Column::new("value", [3_i64, 1, 4, 2]),
        Column::new("keep", [true, false, true, true]),
    ])
    .unwrap();
    let output = run(
        frame,
        vec![
            Stat::Filter(FilterStat {
                condition: Expr::Col("keep".into()),
            }),
            Stat::Sort(SortStat {
                pairs: vec![(SortOrder::Desc, "value".into())],
            }),
            Stat::Limit(LimitStat { rows: "2".into() }),
            Stat::Reverse,
            Stat::Select(SelectStat {
                columns: vec![Expr::Col("value".into())],
            }),
        ],
    )
    .unwrap();
    assert_eq!(values(&output, "value"), [Value::Int(3), Value::Int(4)]);
}

#[test]
fn executor_clone_and_use_manage_named_frames_without_aliasing() {
    let original = Frame::new(vec![Column::new("value", [1_i64, 2, 3])]).unwrap();
    let mut executor = Executor::new(
        "left",
        HashMap::from([
            ("left".into(), original),
            (
                "right".into(),
                Frame::new(vec![Column::new("value", [9_i64])]).unwrap(),
            ),
        ]),
    )
    .unwrap();
    executor
        .execute(&S {
            statements: vec![
                Stat::Clone(CloneStat {
                    df_name: "saved".into(),
                }),
                Stat::Limit(LimitStat { rows: "1".into() }),
                Stat::Use(UseStat {
                    df_name: "right".into(),
                }),
            ],
        })
        .unwrap();
    assert_eq!(executor.frame_name(), "right");
    assert_eq!(values(executor.frame(), "value"), [Value::Int(9)]);
    assert_eq!(
        values(&executor.input()["saved"], "value"),
        [Value::Int(1), Value::Int(2), Value::Int(3)]
    );
    assert_eq!(values(&executor.input()["left"], "value"), [Value::Int(1)]);
    assert_eq!(
        executor.set_frame_name("missing").unwrap_err(),
        Error::FrameNotFound("missing".into())
    );
}

#[test]
fn executor_groups_in_first_seen_order_and_excludes_group_keys_from_selectors() {
    let frame = Frame::new(vec![
        Column::new("group", ["b", "a", "b"]),
        Column::new("value", [1_i64, 2, 3]),
    ])
    .unwrap();
    let output = run(
        frame,
        vec![Stat::GroupAgg(GroupAggStat {
            group_by: vec!["group".into()],
            agg: vec![Expr::Unary(Box::new(UnaryExpr {
                operator: UnaryOperator::Sum,
                expr: Expr::Col("*".into()),
            }))],
        })],
    )
    .unwrap();
    assert_eq!(
        values(&output, "group"),
        [Value::from("b"), Value::from("a")]
    );
    assert_eq!(values(&output, "value"), [Value::Int(4), Value::Int(2)]);
}

#[test]
fn executor_full_join_keeps_unmatched_rows_and_renames_collisions() {
    let left = Frame::new(vec![
        Column::new("id", [Some(1_i64), None, Some(2)]),
        Column::new("value", ["left-1", "left-2", "left-3"]),
    ])
    .unwrap();
    let right = Frame::new(vec![
        Column::new("id", [Some(2_i64), None, Some(3)]),
        Column::new("value", ["right-2", "right-null", "right-3"]),
    ])
    .unwrap();
    let mut executor = Executor::new(
        "left",
        HashMap::from([("left".into(), left), ("right".into(), right)]),
    )
    .unwrap();
    executor
        .execute(&S {
            statements: vec![Stat::Join(JoinStat::SingleCol(SingleColJoinStat {
                other: "right".into(),
                ty: SingleColJoinType::Full,
                left_on: Expr::Col("id".into()),
                right_on: None,
            }))],
        })
        .unwrap();
    let output = executor.frame().clone();
    let output_column_names: Vec<&str> = output.column_names();
    assert!(output_column_names.contains(&"id"));
    assert!(output_column_names.contains(&"value"));
    assert!(output_column_names.contains(&"id_right"));
    assert!(output_column_names.contains(&"value_right"));
    assert_eq!(
        values(&output, "id"),
        [
            Value::Int(1),
            Value::Null,
            Value::Int(2),
            Value::Null,
            Value::Null
        ]
    );
    assert_eq!(
        values(&output, "id_right"),
        [
            Value::Null,
            Value::Null,
            Value::Int(2),
            Value::Null,
            Value::Int(3)
        ]
    );
}

#[test]
fn executor_rejects_sort_by_without_keys() {
    let frame = Frame::new(vec![Column::new("value", [2_i64, 1])]).unwrap();
    assert_eq!(
        run(
            frame,
            vec![Stat::Select(SelectStat {
                columns: vec![Expr::SortBy(Box::new(SortByExpr {
                    expr: Expr::Col("value".into()),
                    pairs: Vec::new(),
                }))],
            })]
        )
        .unwrap_err(),
        Error::InvalidValue {
            operation: "sort by",
            value: "no sort keys".into(),
        }
    );
}

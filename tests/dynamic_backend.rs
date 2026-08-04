use dfsql::{
    backend::dynamic::{Column, ColumnData, Engine, Error, Frame, Value},
    sql,
};
use std::{collections::HashMap, sync::Arc};

fn run(frame: Frame, query: &str) -> Result<Frame, Error> {
    let mut executor = Engine::from_frame("table", frame);
    executor.execute(&sql::parse(query).unwrap())?;
    Ok(executor.frame().clone())
}

fn values(frame: &Frame, name: &str) -> Vec<Value> {
    frame.column(name).unwrap().values()
}

fn list(values: impl IntoIterator<Item = Value>) -> Value {
    Value::List(values.into_iter().collect::<Vec<_>>().into())
}

#[test]
fn columns_store_homogeneous_values_in_typed_vectors() {
    let columns = [
        Column::new("bool", [Some(true), None]),
        Column::new("uint", [1_u64, 2]),
        Column::new("int", [1_i64, 2]),
        Column::new("float", [1.0_f64, 2.0]),
        Column::new("string", ["a", "b"]),
        Column::new("list", [list([Value::Int(1)]), list([Value::Int(2)])]),
    ];
    assert!(matches!(columns[0].data(), ColumnData::Bool(_)));
    assert!(matches!(columns[1].data(), ColumnData::UInt(_)));
    assert!(matches!(columns[2].data(), ColumnData::Int(_)));
    assert!(matches!(columns[3].data(), ColumnData::Float(_)));
    assert!(matches!(columns[4].data(), ColumnData::String(_)));
    assert!(matches!(columns[5].data(), ColumnData::List(_)));
    assert!(matches!(
        Column::new("values", [Value::Int(1), Value::Int(2)]).data(),
        ColumnData::Int(_)
    ));
    assert!(matches!(
        Column::new("mixed", [Value::Int(1), Value::Float(2.0)]).data(),
        ColumnData::Mixed(_)
    ));
}

#[test]
fn operations_preserve_known_types_without_non_null_values() {
    for values in [
        ColumnData::Int(Vec::new()),
        ColumnData::Int(vec![None, None]),
    ] {
        let frame = Frame::new(vec![Column::from_data("value", values)]).unwrap();
        let output = run(
            frame.clone(),
            "select alias negated -value alias compared value = 1",
        )
        .unwrap();
        assert!(matches!(
            output.column("negated").unwrap().data(),
            ColumnData::Int(_)
        ));
        assert!(matches!(
            output.column("compared").unwrap().data(),
            ColumnData::Bool(_)
        ));
        let distinct = run(frame, "select alias distinct unique value").unwrap();
        assert!(matches!(
            distinct.column("distinct").unwrap().data(),
            ColumnData::Int(_)
        ));
    }
    let frame = Frame::new(vec![
        Column::new("key", Vec::<String>::new()),
        Column::new("value", Vec::<i64>::new()),
    ])
    .unwrap();
    let output = run(frame, "group key agg alias total sum value").unwrap();
    assert!(matches!(
        output.column("key").unwrap().data(),
        ColumnData::String(_)
    ));
    assert!(matches!(
        output.column("total").unwrap().data(),
        ColumnData::Int(_)
    ));
}

#[test]
fn frames_validate_shape_and_names() {
    assert!(matches!(
        Frame::new(vec![
            Column::new("a", [1_i64]),
            Column::new("b", [1_i64, 2])
        ]),
        Err(Error::ColumnLength { .. })
    ));
    assert!(matches!(
        Frame::new(vec![
            Column::new("a", [1_i64]),
            Column::new("a", [2_i64])
        ]),
        Err(Error::DuplicateColumn(name)) if name == "a"
    ));
    let frame = Frame::from_rows(
        ["name", "score"],
        [
            vec![Value::from("a"), Value::Int(1)],
            vec![Value::from("b"), Value::Int(2)],
        ],
    )
    .unwrap();
    assert_eq!(frame.height(), 2);
    assert_eq!(frame.row(1), Some(vec![Value::from("b"), Value::Int(2)]));
}

#[test]
fn expressions_use_one_scalar_broadcast_and_null_rule() {
    let frame = Frame::new(vec![Column::new("a", [Some(1_i64), None, Some(3)])]).unwrap();
    let output = run(
        frame,
        "select alias plus a + 2 alias greater a > 1 alias missing is null a",
    )
    .unwrap();
    assert_eq!(
        values(&output, "plus"),
        [Value::Int(3), Value::Null, Value::Int(5)]
    );
    assert_eq!(
        values(&output, "greater"),
        [Value::Bool(false), Value::Null, Value::Bool(true)]
    );
    assert_eq!(
        values(&output, "missing"),
        [Value::Bool(false), Value::Bool(true), Value::Bool(false)]
    );
}

#[test]
fn numeric_results_follow_float_then_int_then_uint_promotion() {
    let frame = Frame::new(vec![
        Column::new("u", [2_u64, 4]),
        Column::new("i", [-1_i64, 3]),
        Column::new("f", [0.5_f64, 1.5]),
    ])
    .unwrap();
    let output = run(
        frame,
        "select alias signed_value u + i alias decimal_value u + f alias quotient u / 2",
    )
    .unwrap();
    assert_eq!(
        values(&output, "signed_value"),
        [Value::Int(1), Value::Int(7)]
    );
    assert_eq!(
        values(&output, "decimal_value"),
        [Value::Float(2.5), Value::Float(5.5)]
    );
    assert_eq!(values(&output, "quotient"), [Value::Int(1), Value::Int(2)]);
}

#[test]
fn integer_arithmetic_reports_overflow_and_invalid_mixed_ranges() {
    let overflow = Frame::new(vec![Column::new("value", [u64::MAX])]).unwrap();
    assert!(matches!(
        run(overflow, "select value + 1"),
        Err(Error::InvalidValue {
            operation: "arithmetic",
            ..
        })
    ));
    let mixed = Frame::new(vec![
        Column::new("unsigned", [u64::MAX]),
        Column::new("signed", [1_i64]),
    ])
    .unwrap();
    assert!(matches!(
        run(mixed, "select unsigned + signed"),
        Err(Error::InvalidValue {
            operation: "arithmetic",
            ..
        })
    ));
}

#[test]
fn filter_sort_limit_and_reverse_are_row_operations() {
    let frame = Frame::new(vec![
        Column::new("value", [3_i64, 1, 4, 2]),
        Column::new("keep", [true, false, true, true]),
    ])
    .unwrap();
    let output = run(
        frame,
        "filter keep sort desc value limit 2 reverse select value",
    )
    .unwrap();
    assert_eq!(values(&output, "value"), [Value::Int(3), Value::Int(4)]);
}

#[test]
fn conditionals_and_casts_share_normal_broadcasting() {
    let frame = Frame::new(vec![
        Column::new("flag", [true, false]),
        Column::new("number", [2_i64, 3]),
    ])
    .unwrap();
    let output = run(
        frame,
        r#"select alias label if flag then "yes" else "no" alias decimal_value cast float number alias text cast str number"#,
    )
    .unwrap();
    assert_eq!(
        values(&output, "label"),
        [Value::from("yes"), Value::from("no")]
    );
    assert_eq!(
        values(&output, "decimal_value"),
        [Value::Float(2.0), Value::Float(3.0)]
    );
    assert_eq!(
        values(&output, "text"),
        [Value::from("2"), Value::from("3")]
    );
}

#[test]
fn reductions_ignore_nulls_and_return_scalars() {
    let frame = Frame::new(vec![Column::new(
        "value",
        [None, Some(1_i64), None, Some(3), None],
    )])
    .unwrap();
    let output = run(
        frame,
        "select alias total sum value alias non_null count value alias average mean value alias head first value alias tail last value",
    )
    .unwrap();
    assert_eq!(values(&output, "total"), [Value::Int(4)]);
    assert_eq!(values(&output, "non_null"), [Value::UInt(2)]);
    assert_eq!(values(&output, "average"), [Value::Float(2.0)]);
    assert_eq!(values(&output, "head"), [Value::Int(1)]);
    assert_eq!(values(&output, "tail"), [Value::Int(3)]);
}

#[test]
fn all_operators_use_the_same_value_kernels() {
    let frame = Frame::new(vec![
        Column::new("number", [-4_i64, 9]),
        Column::new("flag", [true, false]),
        Column::new("decimal", [f64::NAN, 4.0]),
    ])
    .unwrap();
    let output = run(
        frame,
        r#"select alias magnitude abs number alias negated -number alias root sqrt decimal alias inverted !flag alias nan_flag is nan decimal alias conjunction flag & true alias powered number pow 2 alias remainder number % 3"#,
    )
    .unwrap();
    assert_eq!(values(&output, "magnitude"), [Value::Int(4), Value::Int(9)]);
    assert_eq!(values(&output, "negated"), [Value::Int(4), Value::Int(-9)]);
    assert!(matches!(
        values(&output, "root")[0],
        Value::Float(value) if value.is_nan()
    ));
    assert_eq!(values(&output, "root")[1], Value::Float(2.0));
    assert_eq!(
        values(&output, "inverted"),
        [Value::Bool(false), Value::Bool(true)]
    );
    assert_eq!(
        values(&output, "nan_flag"),
        [Value::Bool(true), Value::Bool(false)]
    );
    assert_eq!(
        values(&output, "conjunction"),
        [Value::Bool(true), Value::Bool(false)]
    );
    assert_eq!(values(&output, "powered"), [Value::Int(16), Value::Int(81)]);
    assert_eq!(
        values(&output, "remainder"),
        [Value::Int(-1), Value::Int(0)]
    );
}

#[test]
fn statistical_and_boolean_reductions_are_uniform() {
    let frame = Frame::new(vec![
        Column::new("number", [1_i64, 2, 3]),
        Column::new("flag", [true, true, false]),
    ])
    .unwrap();
    let output = run(
        frame,
        "select alias middle median number alias maximum max number alias minimum min number alias variance var number alias deviation std number alias every all flag alias some any flag",
    )
    .unwrap();
    assert_eq!(values(&output, "middle"), [Value::Float(2.0)]);
    assert_eq!(values(&output, "maximum"), [Value::Int(3)]);
    assert_eq!(values(&output, "minimum"), [Value::Int(1)]);
    assert_eq!(values(&output, "variance"), [Value::Float(1.0)]);
    assert_eq!(values(&output, "deviation"), [Value::Float(1.0)]);
    assert_eq!(values(&output, "every"), [Value::Bool(false)]);
    assert_eq!(values(&output, "some"), [Value::Bool(true)]);
}

#[test]
fn sequence_transforms_have_direct_column_semantics() {
    let frame = Frame::new(vec![
        Column::new("value", [3_i64, 1, 3, 2]),
        Column::new("key", [1_i64, 4, 3, 2]),
    ])
    .unwrap();
    let unique = run(frame.clone(), "select unique value").unwrap();
    assert_eq!(
        values(&unique, "value"),
        [Value::Int(3), Value::Int(1), Value::Int(2)]
    );
    let sorted = run(frame.clone(), "select col_sort asc value").unwrap();
    assert_eq!(
        values(&sorted, "value"),
        [Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(3)]
    );
    let by = run(frame, "select sort value by desc key").unwrap();
    assert_eq!(
        values(&by, "value"),
        [Value::Int(1), Value::Int(3), Value::Int(2), Value::Int(3)]
    );
    let reversed = run(by, "select col_reverse value").unwrap();
    assert_eq!(
        values(&reversed, "value"),
        [Value::Int(3), Value::Int(2), Value::Int(3), Value::Int(1)]
    );
}

#[test]
fn only_scalar_expressions_broadcast() {
    let frame = Frame::new(vec![
        Column::new("same", [1_i64, 1, 1]),
        Column::new("value", [10_i64, 20, 30]),
    ])
    .unwrap();
    assert!(matches!(
        run(frame.clone(), "select unique same value"),
        Err(Error::LengthMismatch {
            operation: "select",
            left: 1,
            right: 3
        })
    ));
    let output = run(frame, "select sort 1 by desc value").unwrap();
    assert_eq!(
        values(&output, "literal"),
        [Value::Int(1), Value::Int(1), Value::Int(1)]
    );
}

#[test]
fn selectors_expand_uniformly() {
    let frame = Frame::new(vec![
        Column::new("a", [1_i64, 2]),
        Column::new("b", [3_i64, 4]),
    ])
    .unwrap();
    assert_eq!(frame.width(), 2);
    let excluded = run(frame.clone(), "select exclude b").unwrap();
    assert_eq!(excluded.width(), 1);
    assert_eq!(excluded.columns()[0].name(), "a");
    let mapped = run(frame, r#"select col "*" * 2"#).unwrap();
    assert_eq!(values(&mapped, "a"), [Value::Int(2), Value::Int(4)]);
    assert_eq!(values(&mapped, "b"), [Value::Int(6), Value::Int(8)]);
}

#[test]
fn string_functions_use_the_same_scalar_or_row_broadcast() {
    let frame = Frame::new(vec![Column::new("name", ["a-12", "c-7"])]).unwrap();
    let output = run(
        frame,
        r#"select alias hit (contains "^[ab]" name) alias digits (extract "([0-9]+)" 1 name) alias all_digits (extract all "[0-9]" name) alias parts (split "-" name)"#,
    )
    .unwrap();
    assert_eq!(
        values(&output, "hit"),
        [Value::Bool(true), Value::Bool(false)]
    );
    assert_eq!(
        values(&output, "digits"),
        [Value::from("12"), Value::from("7")]
    );
    assert_eq!(
        values(&output, "all_digits"),
        [
            list([Value::from("1"), Value::from("2")]),
            list([Value::from("7")])
        ]
    );
    assert_eq!(
        values(&output, "parts"),
        [
            list([Value::from("a"), Value::from("12")]),
            list([Value::from("c"), Value::from("7")])
        ]
    );
}

#[test]
fn groups_evaluate_the_same_expressions_on_each_subframe() {
    let frame = Frame::new(vec![
        Column::new("team", ["a", "a", "b"]),
        Column::new("score", [1_i64, 2, 4]),
    ])
    .unwrap();
    let output = run(
        frame,
        "group team agg alias total sum score alias scores score",
    )
    .unwrap();
    assert_eq!(
        values(&output, "team"),
        [Value::from("a"), Value::from("b")]
    );
    assert_eq!(values(&output, "total"), [Value::Int(3), Value::Int(4)]);
    assert_eq!(
        values(&output, "scores"),
        [list([Value::Int(1), Value::Int(2)]), list([Value::Int(4)])]
    );
}

#[test]
fn global_grouping_handles_empty_input_without_special_expression_rules() {
    let frame = Frame::new(vec![Column::new("score", Vec::<i64>::new())]).unwrap();
    let output = run(
        frame,
        "group agg alias total sum score alias non_null count score",
    )
    .unwrap();
    assert_eq!(output.height(), 1);
    assert_eq!(values(&output, "total"), [Value::Null]);
    assert_eq!(values(&output, "non_null"), [Value::UInt(0)]);
}

fn join_executor() -> Engine {
    let left = Frame::new(vec![
        Column::new("id", [1_i64, 2]),
        Column::new("name", ["one", "two"]),
    ])
    .unwrap();
    let right = Frame::new(vec![
        Column::new("id", [2_i64, 3]),
        Column::new("enabled", [true, false]),
    ])
    .unwrap();
    Engine::new(
        "left",
        HashMap::from([("left".into(), left), ("other".into(), right)]),
    )
    .unwrap()
}

#[test]
fn joins_apply_one_key_rule_and_keep_both_input_schemas() {
    let mut executor = join_executor();
    executor
        .execute(&sql::parse("left join other on id").unwrap())
        .unwrap();
    let output = executor.frame();
    assert_eq!(output.height(), 2);
    assert_eq!(values(output, "id"), [Value::Int(1), Value::Int(2)]);
    assert_eq!(values(output, "id_right"), [Value::Null, Value::Int(2)]);
    assert_eq!(values(output, "enabled"), [Value::Null, Value::Bool(true)]);
    let mut executor = join_executor();
    executor
        .execute(&sql::parse("full join other on id").unwrap())
        .unwrap();
    assert_eq!(executor.frame().height(), 3);
    let mut executor = join_executor();
    executor
        .execute(&sql::parse("inner join other on id").unwrap())
        .unwrap();
    assert_eq!(executor.frame().height(), 1);
    let mut executor = join_executor();
    executor
        .execute(&sql::parse("right join other on id").unwrap())
        .unwrap();
    assert_eq!(
        values(executor.frame(), "id"),
        [Value::Int(2), Value::Int(3)]
    );
}

#[test]
fn null_join_keys_never_match() {
    let left = Frame::new(vec![Column::new("id", [None::<i64>])]).unwrap();
    let right = Frame::new(vec![Column::new("id", [None::<i64>])]).unwrap();
    let mut executor = Engine::new(
        "left",
        HashMap::from([("left".into(), left), ("other".into(), right)]),
    )
    .unwrap();
    executor
        .execute(&sql::parse("inner join other on id").unwrap())
        .unwrap();
    assert_eq!(executor.frame().height(), 0);
    assert!(
        executor
            .frame()
            .columns()
            .iter()
            .all(|column| matches!(column.data(), ColumnData::Int(_)))
    );
}

#[test]
fn clone_and_use_only_change_executor_state() {
    let frame = Frame::new(vec![
        Column::new("id", [1_i64, 2]),
        Column::new("keep", [true, false]),
    ])
    .unwrap();
    let mut executor = Engine::from_frame("table", frame);
    executor
        .execute(&sql::parse("clone snapshot filter keep use snapshot").unwrap())
        .unwrap();
    assert_eq!(executor.frame_name(), "snapshot");
    assert_eq!(executor.frame().height(), 2);
    assert_eq!(executor.input()["table"].height(), 1);
}

#[test]
fn invalid_inputs_use_the_shared_type_length_and_value_errors() {
    let frame = Frame::new(vec![
        Column::new("number", [1_i64, 2]),
        Column::new("text", ["a", "b"]),
    ])
    .unwrap();
    assert!(matches!(
        run(frame.clone(), "filter number"),
        Err(Error::InvalidType {
            operation: "filter",
            kind: "int"
        })
    ));
    assert!(matches!(
        run(frame.clone(), "select number + text"),
        Err(Error::InvalidType {
            operation: "arithmetic",
            kind: "string"
        })
    ));
    assert!(matches!(
        run(frame, r#"select contains "[" text"#),
        Err(Error::InvalidRegex { .. })
    ));
}

#[test]
fn values_display_and_round_trip_lists() {
    let value = list([Value::Int(1), Value::from("two")]);
    assert_eq!(value.to_string(), "[1, two]");
    let column = Column::from_data(
        "list",
        ColumnData::List(vec![Some(Arc::from([value.clone()]))]),
    );
    assert_eq!(column.values(), [list([value])]);
}

#[test]
fn programmatic_sort_by_ast_rejects_an_empty_key_list() {
    let mut statements = sql::parse("select sort value by order").unwrap();
    let sql::stmt::Stmt::Select(select) = &mut statements.statements[0] else {
        panic!("expected select statement")
    };
    let sql::expr::Expr::SortBy(sort) = &mut select.columns[0] else {
        panic!("expected sort-by expression")
    };
    sort.pairs.clear();
    let frame = Frame::new(vec![
        Column::new("value", [2_i64, 1]),
        Column::new("order", [1_i64, 2]),
    ])
    .unwrap();
    let mut executor = Engine::from_frame("table", frame);
    assert_eq!(
        executor.execute(&statements).unwrap_err(),
        Error::InvalidValue {
            operation: "sort by",
            value: "no sort keys".into()
        }
    );
}

#[test]
fn empty_string_split_segments_unicode_characters_without_boundaries() {
    let frame = Frame::new(vec![Column::new("text", ["", "a", "é"])]).unwrap();
    let output = run(frame, r#"select (alias parts split "" text)"#).unwrap();
    assert_eq!(
        values(&output, "parts"),
        [list([]), list([Value::from("a")]), list([Value::from("é")])]
    );
}

use dfsql::sql;
#[cfg(feature = "file-ops")]
#[test]
fn file_ops_are_public_without_exposing_backend_types() {
    assert!(dfsql::file_ops::read_df_file("input.unsupported").is_err());
    let _write = |frame: dfsql::MaterializedFrame| {
        dfsql::file_ops::write_df_output(frame, "output.unsupported")
    };
}
#[test]
fn dynamic_columns_use_typed_data() {
    use dfsql::backend::dynamic::{Column, ColumnData, Value};
    let columns = [
        Column::new("bool", [true]),
        Column::new("uint", [1_u64]),
        Column::new("int", [Some(1_i64), None]),
        Column::new("float", [1.0]),
        Column::new("string", ["one"]),
        Column::new("list", [vec![Value::Int(1)]]),
    ];
    assert!(matches!(columns[0].data(), ColumnData::Bool(_)));
    assert!(matches!(columns[1].data(), ColumnData::UInt(_)));
    assert_eq!(columns[2].data(), &ColumnData::Int(vec![Some(1), None]));
    assert!(matches!(columns[3].data(), ColumnData::Float(_)));
    assert!(matches!(columns[4].data(), ColumnData::String(_)));
    assert!(matches!(columns[5].data(), ColumnData::List(_)));
    let data = ColumnData::Mixed(Vec::new());
    assert_eq!(
        Column::from_data("roundtrip", data.clone()).into_data(),
        data
    );
    assert!(matches!(
        Column::new("mixed", [Value::Int(1), Value::Float(2.0)]).data(),
        ColumnData::Mixed(..)
    ));
}
#[test]
fn dynamic_executor_collects_the_current_frame() {
    use dfsql::backend::dynamic::{Column, Executor, Frame, MaterializedFrame};
    let input = Frame::new(vec![Column::new("id", [1_i64, 2])]).unwrap();
    let executor = Executor::from_frame("table", input.clone());
    let output: MaterializedFrame = executor.collect().unwrap();
    assert_eq!(output, input);
}
#[test]
fn unary_operators_bind_before_binary_operators() {
    use dfsql::{
        Executor, Frame,
        backend::dynamic::{Column, Value},
    };
    let mut executor = Executor::from_frame(
        "table",
        Frame::new(vec![Column::new("input", [0_i64])]).unwrap(),
    );
    executor
        .execute(&sql::parse("select alias result -1 - 2").unwrap())
        .unwrap();
    assert_eq!(
        executor
            .collect()
            .unwrap()
            .to_dynamic()
            .unwrap()
            .column("result")
            .unwrap()
            .values(),
        [Value::Int(-3)]
    );
}
#[test]
fn executor_restore_all_state_after_a_failed_program() {
    use dfsql::{
        Executor, Frame,
        backend::dynamic::{Column, Executor as DynamicExecutor, Frame as DynamicFrame},
    };
    let input = DynamicFrame::new(vec![Column::new("id", [1_i64, 2])]).unwrap();
    let program = sql::parse("Limit 1 clone Leaked use missing").unwrap();
    let mut dynamic = DynamicExecutor::from_frame("input", input.clone());
    assert!(dynamic.execute(&program).is_err());
    assert_eq!(dynamic.frame().height(), 2);
    assert!(!dynamic.input().contains_key("Leaked"));
    let mut selected = Executor::from_frame("input", Frame::from_dynamic(input).unwrap());
    assert!(selected.execute(&program).is_err());
    assert_eq!(selected.collect().unwrap().height(), 2);
    assert!(!selected.input().contains_key("Leaked"));
}
#[cfg(not(feature = "polars-backend"))]
#[test]
fn root_facade_uses_dynamic_backend_without_polars() {
    use dfsql::{
        Executor, Frame,
        backend::dynamic::{Column, ColumnData},
    };
    let input = Frame::new(vec![
        Column::new("id", [3_i64, 1, 2]),
        Column::new("enabled", [true, false, true]),
    ])
    .unwrap();
    let mut executor = Executor::from_frame("table", input);
    executor
        .execute(&sql::parse("filter enabled sort id select id").unwrap())
        .unwrap();
    assert_eq!(executor.frame_name(), "table");
    assert_eq!(executor.collect().unwrap().height(), 2);
    let current = executor.collect().unwrap().to_dynamic().unwrap();
    assert!(matches!(
        current.column("id").unwrap().data(),
        ColumnData::Int(_)
    ));
    assert!(matches!(
        executor.set_frame_name("missing"),
        Err(dfsql::Error::FrameNotFound(name)) if name == "missing"
    ));
    assert!(matches!(
        executor.execute(&sql::parse("use missing").unwrap()),
        Err(dfsql::Error::FrameNotFound(name)) if name == "missing"
    ));
}
#[cfg(feature = "polars-backend")]
#[test]
fn polars_backend_shadows_the_root_facade() {
    use dfsql::backend::dynamic::Column;
    use dfsql::{Executor, Frame, MaterializedFrame};
    let input = Frame::new(vec![
        Column::new("id", [3_i64, 1, 2]),
        Column::new("enabled", [true, false, true]),
    ])
    .unwrap();
    let mut executor = Executor::from_frame("table", input);
    executor
        .execute(&sql::parse("filter enabled sort id select id").unwrap())
        .unwrap();
    let output: MaterializedFrame = executor.collect().unwrap();
    assert_eq!(executor.frame_name(), "table");
    assert_eq!(output.height(), 2);
    assert!(
        matches!(executor.set_frame_name("missing"), Err(dfsql::Error::FrameNotFound(name)) if name == "missing")
    );
    assert!(
        matches!(executor.execute(&sql::parse("use missing").unwrap()), Err(dfsql::Error::FrameNotFound(name)) if name == "missing")
    );
}
#[cfg(feature = "polars-backend")]
#[test]
fn dynamic_backend_remains_available_when_polars_is_selected() {
    use dfsql::backend::dynamic::{Column, Executor, Frame};
    let input = Frame::new(vec![Column::new("id", [2_i64, 1])]).unwrap();
    let mut executor = Executor::from_frame("table", input);
    executor.execute(&sql::parse("sort id").unwrap()).unwrap();
    assert_eq!(executor.frame().column("id").unwrap().values()[0], 1.into());
}
#[cfg(feature = "polars-backend")]
#[test]
fn polars_boundary_is_lossless_or_errors() {
    use dfsql::{
        Frame,
        backend::dynamic::{Column, Frame as DynamicFrame, Value},
    };
    let expected = DynamicFrame::new(vec![
        Column::new("bool", [Some(true), None]),
        Column::new("uint", [Some(1_u64), None]),
        Column::new("int", [Some(-1_i64), None]),
        Column::new("float", [Some(2.5_f64), None]),
        Column::new("string", [Some("one"), None]),
        Column::new("bytes", [Some(vec![1_u8, 2]), None]),
        Column::new("List", [vec![Value::Int(1), Value::Int(2)], vec![]]),
    ])
    .unwrap();
    let actual = Frame::from_dynamic(expected.clone())
        .unwrap()
        .collect()
        .unwrap()
        .to_dynamic()
        .unwrap();
    assert_eq!(actual, expected);
    for expected in [
        DynamicFrame::new(vec![
            Column::new("string", Vec::<String>::new()),
            Column::new("list", Vec::<Vec<Value>>::new()),
        ])
        .unwrap(),
        DynamicFrame::new(vec![
            Column::new("string", [None::<String>, None]),
            Column::new("list", [None::<Vec<Value>>, None]),
        ])
        .unwrap(),
    ] {
        let actual = Frame::from_dynamic(expected.clone())
            .unwrap()
            .collect()
            .unwrap()
            .to_dynamic()
            .unwrap();
        assert_eq!(actual, expected);
    }
    let input = DynamicFrame::new(vec![Column::new(
        "mixed",
        [Value::UInt(9_007_199_254_740_993), Value::Float(0.5)],
    )])
    .unwrap();
    assert!(Frame::from_dynamic(input).is_err());
    let expected = DynamicFrame::from_rows(Vec::<String>::new(), vec![vec![], vec![]]).unwrap();
    let actual = Frame::from_dynamic(expected.clone())
        .unwrap()
        .collect()
        .unwrap()
        .to_dynamic()
        .unwrap();
    assert_eq!(actual, expected);
}
#[cfg(feature = "polars-backend")]
#[test]
fn invalid_numeric_literals_return_errors_instead_of_panicking() {
    use dfsql::{Executor, Frame, backend::dynamic::Column};
    let input = Frame::new(vec![Column::new("id", [1_i64])]).unwrap();
    let mut executor = Executor::from_frame("table", input);
    assert!(
        executor
            .execute(&sql::parse("select 999999999999999999999999999").unwrap())
            .is_err()
    );
    assert!(
        executor
            .execute(&sql::parse("limit 184467440737095516160").unwrap())
            .is_err()
    );
}

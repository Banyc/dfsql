use dfsql::sql;
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
    assert!(matches!(
        executor.frame().column("id").unwrap().data(),
        ColumnData::Int(_)
    ));
    assert!(
        matches!(executor.set_frame_name("missing"), Err(dfsql::Error::FrameNotFound(name)) if name == "missing")
    );
    assert!(
        matches!(executor.execute(&sql::parse("use missing").unwrap()), Err(dfsql::Error::FrameNotFound(name)) if name == "missing")
    );
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
fn polars_boundary_round_trips_crate_owned_frame_types() {
    use dfsql::{
        Frame,
        backend::dynamic::{Column, Value},
    };
    let expected = dfsql::backend::dynamic::Frame::new(vec![
        Column::new("bool", [Some(true), None]),
        Column::new("uint", [Some(1_u64), None]),
        Column::new("int", [Some(-1_i64), None]),
        Column::new("float", [Some(1.5_f64), None]),
        Column::new("string", [Some("one"), None]),
        Column::new("list", [vec![Value::Int(1), Value::Int(2)], vec![]]),
    ])
    .unwrap();
    let actual = Frame::from_dynamic(expected.clone())
        .unwrap()
        .collect()
        .unwrap()
        .to_dynamic()
        .unwrap();
    assert_eq!(actual, expected);
}

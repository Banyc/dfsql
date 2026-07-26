use dfsql::sql;
#[test]
fn dynamic_columns_use_typed_data() {
    use dfsql::dynamic::{Column, ColumnData, Value};
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
#[cfg(not(feature = "polars-backend"))]
#[test]
fn root_facade_uses_dynamic_backend_without_polars() {
    use dfsql::{
        Executor, Frame,
        dynamic::{Column, ColumnData},
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
    use dfsql::{Executor, Frame, MaterializedFrame};
    use polars::prelude::IntoLazy;
    let input: Frame = polars::df!("id" => [3_i64, 1, 2], "enabled" => [true, false, true])
        .unwrap()
        .lazy();
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
fn explicit_polars_names_alias_the_shadowing_root_facade() {
    use dfsql::{Executor, Frame, PolarsExecutor, PolarsFrame};
    use polars::prelude::IntoLazy;
    fn same_frame_type(frame: Frame) -> PolarsFrame {
        frame
    }
    fn same_executor_type(executor: Executor) -> PolarsExecutor {
        executor
    }
    let frame = polars::df!("id" => [1_i64]).unwrap().lazy();
    let executor = Executor::from_frame("table", same_frame_type(frame));
    let _ = same_executor_type(executor);
}
#[cfg(feature = "polars-backend")]
#[test]
fn dynamic_backend_remains_available_when_polars_is_selected() {
    use dfsql::dynamic::{Column, Executor, Frame};
    let input = Frame::new(vec![Column::new("id", [2_i64, 1])]).unwrap();
    let mut executor = Executor::from_frame("table", input);
    executor.execute(&sql::parse("sort id").unwrap()).unwrap();
    assert_eq!(executor.frame().column("id").unwrap().values()[0], 1.into());
}

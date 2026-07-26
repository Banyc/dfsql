use dfsql::backend::dynamic::*;

#[test]
fn typed_columns_preserve_nulls() {
    let col = Column::new("test", vec![Some(true), None, Some(false)]);
    match col.data() {
        ColumnData::Bool(v) => {
            assert_eq!(v.len(), 3);
            assert_eq!(v[0], Some(true));
            assert_eq!(v[1], None);
            assert_eq!(v[2], Some(false));
        }
        other => panic!("expected Bool variant, got {other:?}"),
    }
}

#[test]
fn empty_typed_columns_retain_type() {
    let col = Column::new("test", Vec::<bool>::new());
    match col.data() {
        ColumnData::Bool(v) => assert!(v.is_empty()),
        other => panic!("expected Bool variant, got {other:?}"),
    }
}

#[test]
fn incompatible_values_use_mixed_storage() {
    let col = Column::new("test", vec![Value::Int(1), Value::String("hello".into())]);
    match col.data() {
        ColumnData::Mixed(v) => {
            assert_eq!(v.len(), 2);
        }
        other => panic!("expected Mixed variant, got {other:?}"),
    }
}

#[test]
fn frame_rejects_invalid_shapes() {
    let col1 = Column::new("a", vec![1, 2, 3]);
    let col2 = Column::new("b", vec![4, 5]);
    assert_eq!(
        Frame::new(vec![col1, col2]).unwrap_err(),
        Error::ColumnLength {
            column: "b".into(),
            expected: 3,
            actual: 2
        }
    );

    let col1 = Column::new("a", vec![1, 2]);
    let col2 = Column::new("a", vec![3, 4]);
    assert_eq!(
        Frame::new(vec![col1, col2]).unwrap_err(),
        Error::DuplicateColumn("a".into())
    );

    let result = Frame::from_rows(
        vec!["x", "y"],
        vec![vec![Value::Int(1), Value::Int(2)], vec![Value::Int(3)]],
    );
    assert_eq!(
        result.unwrap_err(),
        Error::ColumnLength {
            column: "row".into(),
            expected: 2,
            actual: 1
        }
    );
}

#[test]
fn all_null_typed_column() {
    let col = Column::new("test", vec![None::<bool>, None, None]);
    match col.data() {
        ColumnData::Bool(v) => {
            assert_eq!(v.len(), 3);
            assert!(v.iter().all(|x| x.is_none()));
        }
        other => panic!("expected Bool variant, got {other:?}"),
    }
}

#[test]
fn none_converts_to_null() {
    let v: Value = None::<bool>.into();
    assert_eq!(v, Value::Null);
}

#[test]
fn list_values_retain_type() {
    let value: Value = vec![Value::Int(1)].into();
    assert!(matches!(value, Value::List(_)));
}

#[test]
fn named_zero_row_frame_preserves_schema() {
    let frame = Frame::from_rows(vec!["a", "b"], Vec::<Vec<Value>>::new()).unwrap();
    assert_eq!(frame.width(), 2);
    assert_eq!(frame.height(), 0);
    assert!(frame.column("a").is_ok());
    assert!(frame.column("b").is_ok());
}

#[test]
fn column_get_is_bounds_checked() {
    let col = Column::new("x", vec![1, 2, 3]);
    assert_eq!(col.get(1), Some(Value::Int(2)));
    assert_eq!(col.get(3), None);
}

#[test]
fn column_into_data_round_trips_storage() {
    let col = Column::new("x", vec![1, 2, 3]);
    let data = col.into_data();
    match data {
        ColumnData::Int(values) => assert_eq!(values, vec![Some(1), Some(2), Some(3)]),
        other => panic!("expected Int variant, got {other:?}"),
    }
}

#[test]
fn column_iterator_is_double_ended_and_exact_size() {
    let col = Column::new("x", vec![1, 2, 3]);
    let mut iter = col.iter();
    assert_eq!(iter.len(), 3);
    assert_eq!(iter.next_back(), Some(Value::Int(3)));
    assert_eq!(iter.next(), Some(Value::Int(1)));
    assert_eq!(iter.len(), 1);
}

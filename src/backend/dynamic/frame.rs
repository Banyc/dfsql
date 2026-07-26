#![allow(dead_code)]

use std::sync::Arc;

use super::{Error, IntoValue, Result, Value, ValueType};

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnData {
    Bool(Vec<Option<bool>>),
    UInt(Vec<Option<u64>>),
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
    String(Vec<Option<Arc<str>>>),
    Bytes(Vec<Option<Arc<[u8]>>>),
    List(Vec<Option<Arc<[Value]>>>),
    Mixed(Vec<Value>),
}

impl ColumnData {
    pub fn from_values(values: Vec<Value>) -> Self {
        Self::from_values_with_hint(values, None)
    }

    fn from_values_with_hint(values: Vec<Value>, hint: Option<ValueType>) -> Self {
        let hint = hint.or_else(|| values.iter().find_map(|v| v.value_type()));
        let homogeneous = values
            .iter()
            .all(|val| val.value_type().is_none() || val.value_type() == hint);
        if !homogeneous {
            return Self::Mixed(values);
        }
        macro_rules! typed {
            ($variant:ident) => {
                Self::$variant(
                    values
                        .into_iter()
                        .map(|val| match val {
                            Value::Null => None,
                            Value::$variant(value) => Some(value),
                            _ => unreachable!(),
                        })
                        .collect(),
                )
            };
        }
        match hint {
            Some(ValueType::Bool) => typed!(Bool),
            Some(ValueType::UInt) => typed!(UInt),
            Some(ValueType::Int) => typed!(Int),
            Some(ValueType::Float) => typed!(Float),
            Some(ValueType::String) => typed!(String),
            Some(ValueType::Bytes) => typed!(Bytes),
            Some(ValueType::List) => typed!(List),
            None => Self::Mixed(values),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnData::Bool(v) => v.len(),
            ColumnData::UInt(v) => v.len(),
            ColumnData::Int(v) => v.len(),
            ColumnData::Float(v) => v.len(),
            ColumnData::String(v) => v.len(),
            ColumnData::Bytes(v) => v.len(),
            ColumnData::List(v) => v.len(),
            ColumnData::Mixed(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn value(&self, index: usize) -> Value {
        match self {
            ColumnData::Bool(v) => v[index].map_or(Value::Null, Value::Bool),
            ColumnData::UInt(v) => v[index].map_or(Value::Null, Value::UInt),
            ColumnData::Int(v) => v[index].map_or(Value::Null, Value::Int),
            ColumnData::Float(v) => v[index].map_or(Value::Null, Value::Float),
            ColumnData::String(v) => v[index]
                .as_ref()
                .map_or(Value::Null, |s| Value::String(s.clone())),
            ColumnData::Bytes(v) => v[index]
                .as_ref()
                .map_or(Value::Null, |b| Value::Bytes(b.clone())),
            ColumnData::List(v) => v[index]
                .as_ref()
                .map_or(Value::Null, |l| Value::List(l.clone())),
            ColumnData::Mixed(v) => v[index].clone(),
        }
    }

    fn value_type(&self) -> Option<ValueType> {
        match self {
            ColumnData::Bool(_) => Some(ValueType::Bool),
            ColumnData::UInt(_) => Some(ValueType::UInt),
            ColumnData::Int(_) => Some(ValueType::Int),
            ColumnData::Float(_) => Some(ValueType::Float),
            ColumnData::String(_) => Some(ValueType::String),
            ColumnData::Bytes(_) => Some(ValueType::Bytes),
            ColumnData::List(_) => Some(ValueType::List),
            ColumnData::Mixed(_) => None,
        }
    }

    fn take(&self, indices: &[usize]) -> Self {
        match self {
            ColumnData::Bool(v) => ColumnData::Bool(indices.iter().map(|&i| v[i]).collect()),
            ColumnData::UInt(v) => ColumnData::UInt(indices.iter().map(|&i| v[i]).collect()),
            ColumnData::Int(v) => ColumnData::Int(indices.iter().map(|&i| v[i]).collect()),
            ColumnData::Float(v) => ColumnData::Float(indices.iter().map(|&i| v[i]).collect()),
            ColumnData::String(v) => {
                ColumnData::String(indices.iter().map(|&i| v[i].clone()).collect())
            }
            ColumnData::Bytes(v) => {
                ColumnData::Bytes(indices.iter().map(|&i| v[i].clone()).collect())
            }
            ColumnData::List(v) => {
                ColumnData::List(indices.iter().map(|&i| v[i].clone()).collect())
            }
            ColumnData::Mixed(v) => {
                ColumnData::Mixed(indices.iter().map(|&i| v[i].clone()).collect())
            }
        }
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = Value> + ExactSizeIterator + '_ {
        (0..self.len()).map(move |index| self.value(index))
    }

    pub fn values(&self) -> Vec<Value> {
        self.iter().collect()
    }

    pub fn into_values(self) -> Vec<Value> {
        match self {
            ColumnData::Bool(v) => v
                .into_iter()
                .map(|opt| opt.map_or(Value::Null, Value::Bool))
                .collect(),
            ColumnData::UInt(v) => v
                .into_iter()
                .map(|opt| opt.map_or(Value::Null, Value::UInt))
                .collect(),
            ColumnData::Int(v) => v
                .into_iter()
                .map(|opt| opt.map_or(Value::Null, Value::Int))
                .collect(),
            ColumnData::Float(v) => v
                .into_iter()
                .map(|opt| opt.map_or(Value::Null, Value::Float))
                .collect(),
            ColumnData::String(v) => v
                .into_iter()
                .map(|opt| opt.map_or(Value::Null, Value::String))
                .collect(),
            ColumnData::Bytes(v) => v
                .into_iter()
                .map(|opt| opt.map_or(Value::Null, Value::Bytes))
                .collect(),
            ColumnData::List(v) => v
                .into_iter()
                .map(|opt| opt.map_or(Value::Null, Value::List))
                .collect(),
            ColumnData::Mixed(v) => v,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    name: String,
    data: ColumnData,
}

impl Column {
    pub fn new<T: IntoValue>(name: impl Into<String>, values: impl IntoIterator<Item = T>) -> Self {
        let values = values.into_iter().map(Into::into).collect();
        Self {
            name: name.into(),
            data: ColumnData::from_values_with_hint(values, T::value_type()),
        }
    }

    pub fn from_data(name: impl Into<String>, data: ColumnData) -> Self {
        Column {
            name: name.into(),
            data,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data(&self) -> &ColumnData {
        &self.data
    }

    pub fn into_data(self) -> ColumnData {
        self.data
    }

    pub(crate) fn from_values(name: impl Into<String>, values: Vec<Value>) -> Self {
        Self::from_data(name, ColumnData::from_values(values))
    }

    pub(crate) fn rename(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub(crate) fn value_type(&self) -> Option<ValueType> {
        self.data.value_type()
    }

    pub(crate) fn value(&self, index: usize, target_len: usize) -> Result<Value> {
        match self.len() {
            len if len == target_len => Ok(self.data.value(index)),
            1 => Ok(self.data.value(0)),
            len => Err(Error::LengthMismatch {
                operation: "broadcast",
                left: len,
                right: target_len,
            }),
        }
    }

    pub(crate) fn broadcast(self, len: usize) -> Result<Self> {
        if self.len() == len {
            return Ok(self);
        }
        if self.len() != 1 {
            return Err(Error::LengthMismatch {
                operation: "broadcast",
                left: self.len(),
                right: len,
            });
        }
        let hint = self.value_type();
        let value = self.data.value(0);
        Ok(Self {
            name: self.name,
            data: ColumnData::from_values_with_hint(vec![value; len], hint),
        })
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<Value> {
        (index < self.len()).then(|| self.data.value(index))
    }

    pub fn values(&self) -> Vec<Value> {
        self.data.values()
    }

    pub fn into_values(self) -> Vec<Value> {
        self.data.into_values()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = Value> + ExactSizeIterator + '_ {
        self.data.iter()
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Frame {
    columns: Vec<Column>,
    height: usize,
}

impl Frame {
    pub fn new(columns: Vec<Column>) -> Result<Self> {
        let height = columns.first().map_or(0, Column::len);
        Self::with_height(columns, height)
    }

    pub fn from_rows(
        names: impl IntoIterator<Item = impl Into<String>>,
        rows: impl IntoIterator<Item = Vec<Value>>,
    ) -> Result<Self> {
        let names: Vec<String> = names.into_iter().map(Into::into).collect();
        let mut values = vec![Vec::new(); names.len()];
        let mut height = 0;
        for row in rows {
            if row.len() != names.len() {
                return Err(Error::ColumnLength {
                    column: "row".into(),
                    expected: names.len(),
                    actual: row.len(),
                });
            }
            for (column, value) in values.iter_mut().zip(row) {
                column.push(value);
            }
            height += 1;
        }
        let columns = names
            .into_iter()
            .zip(values)
            .map(|(name, values)| Column::from_values(name, values))
            .collect();
        Self::with_height(columns, height)
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn height(&self) -> usize {
        self.height
    }

    pub fn is_empty(&self) -> bool {
        self.height == 0
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name()).collect()
    }

    pub fn width(&self) -> usize {
        self.columns.len()
    }

    pub fn column(&self, name: &str) -> Result<&Column> {
        self.columns
            .iter()
            .find(|c| c.name() == name)
            .ok_or_else(|| Error::ColumnNotFound(name.to_string()))
    }

    pub fn row(&self, index: usize) -> Option<Vec<Value>> {
        (index < self.height).then(|| {
            self.columns
                .iter()
                .map(|column| column.data.value(index))
                .collect()
        })
    }

    pub fn into_columns(self) -> Vec<Column> {
        self.columns
    }

    pub(crate) fn with_height(columns: Vec<Column>, height: usize) -> Result<Self> {
        for col in &columns {
            if col.len() != height {
                return Err(Error::ColumnLength {
                    column: col.name().to_string(),
                    expected: height,
                    actual: col.len(),
                });
            }
        }

        let mut seen = std::collections::HashSet::new();
        for col in &columns {
            if !seen.insert(col.name()) {
                return Err(Error::DuplicateColumn(col.name().to_string()));
            }
        }

        Ok(Frame { columns, height })
    }

    pub(crate) fn take(&self, indices: &[usize]) -> Self {
        let columns = self
            .columns
            .iter()
            .map(|c| Column {
                name: c.name.clone(),
                data: c.data.take(indices),
            })
            .collect();
        Frame {
            height: indices.len(),
            columns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_take_preserves_storage() {
        let frame = Frame::from_rows(
            vec!["a", "b"],
            vec![
                vec![Value::Bool(true), Value::Int(42)],
                vec![Value::Bool(false), Value::Int(99)],
                vec![Value::Bool(true), Value::Int(77)],
            ],
        )
        .unwrap();

        let taken = frame.take(&[0, 2]);
        assert_eq!(taken.height(), 2);

        assert_eq!(taken.row(0), Some(vec![Value::Bool(true), Value::Int(42)]));
        assert_eq!(taken.row(1), Some(vec![Value::Bool(true), Value::Int(77)]));

        match &taken.columns[0].data {
            ColumnData::Bool(_) => {}
            other => panic!("expected Bool variant, got {other:?}"),
        }
        match &taken.columns[1].data {
            ColumnData::Int(_) => {}
            other => panic!("expected Int variant, got {other:?}"),
        }
    }

    #[test]
    fn scalar_null_broadcast() {
        let col = Column::new("x", vec![None::<i64>]);
        let broadcast = col.broadcast(5).unwrap();
        assert_eq!(broadcast.len(), 5);
        match broadcast.data() {
            ColumnData::Int(v) => {
                assert!(v.iter().all(|x| x.is_none()));
            }
            other => panic!("expected Int variant, got {other:?}"),
        }
    }

    #[test]
    fn incompatible_broadcast_lengths() {
        let col = Column::new("x", vec![1, 2]);
        let err = col.broadcast(5).unwrap_err();
        assert_eq!(
            err,
            Error::LengthMismatch {
                operation: "broadcast",
                left: 2,
                right: 5
            }
        );
    }
}

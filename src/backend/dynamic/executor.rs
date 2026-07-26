use std::collections::HashMap;

use crate::sql::{
    SortOrder,
    expr::Expr,
    stat::{JoinStat, SingleColJoinStat, SingleColJoinType, Stat},
};

use super::{
    Column, Error, Frame, Result, Value,
    expression::{
        Shape, evaluate_shaped, expand_selectors, expression_name, select, sorted_indices,
    },
    value::ValueKey,
};

pub struct Executor {
    frame_name: String,
    input: HashMap<String, Frame>,
}

impl Executor {
    pub fn from_frame(frame_name: impl Into<String>, frame: Frame) -> Self {
        let frame_name = frame_name.into();
        Self {
            input: HashMap::from([(frame_name.clone(), frame)]),
            frame_name,
        }
    }

    pub fn new(frame_name: impl Into<String>, input: HashMap<String, Frame>) -> Option<Self> {
        let frame_name = frame_name.into();
        input
            .contains_key(&frame_name)
            .then_some(Self { frame_name, input })
    }

    pub fn input(&self) -> &HashMap<String, Frame> {
        &self.input
    }

    pub fn into_input(self) -> HashMap<String, Frame> {
        self.input
    }

    pub fn insert_frame(&mut self, frame_name: impl Into<String>, frame: Frame) -> Option<Frame> {
        self.input.insert(frame_name.into(), frame)
    }

    pub fn frame_name(&self) -> &str {
        &self.frame_name
    }

    pub fn frame(&self) -> &Frame {
        &self.input[&self.frame_name]
    }

    pub fn frame_mut(&mut self) -> &mut Frame {
        self.input
            .get_mut(&self.frame_name)
            .expect("the active frame is always present")
    }

    pub fn set_frame_name(&mut self, frame_name: impl Into<String>) -> Result<()> {
        let frame_name = frame_name.into();
        if !self.input.contains_key(&frame_name) {
            return Err(Error::FrameNotFound(frame_name));
        }
        self.frame_name = frame_name;
        Ok(())
    }

    pub fn set_frame(&mut self, frame: Frame) {
        self.input.insert(self.frame_name.clone(), frame);
    }

    pub fn collect(&self) -> Result<Frame> {
        Ok(self.frame().clone())
    }

    pub fn execute(&mut self, statements: &crate::sql::S) -> Result<()> {
        for statement in &statements.statements {
            self.execute_statement(statement)?;
        }
        Ok(())
    }

    pub fn execute_statement(&mut self, statement: &Stat) -> Result<()> {
        match statement {
            Stat::Use(value) => return self.set_frame_name(value.df_name.clone()),
            Stat::Clone(value) => {
                self.input
                    .insert(value.df_name.clone(), self.frame().clone());
                return Ok(());
            }
            _ => {}
        }
        let frame = match statement {
            Stat::Select(value) => select(self.frame(), &value.columns)?,
            Stat::GroupAgg(value) => group_aggregate(self.frame(), &value.group_by, &value.agg)?,
            Stat::Filter(value) => filter_frame(self.frame(), &value.condition)?,
            Stat::Limit(value) => {
                let rows = value
                    .rows
                    .parse::<usize>()
                    .map_err(|_| Error::InvalidValue {
                        operation: "limit",
                        value: value.rows.clone(),
                    })?;
                self.frame()
                    .take(&(0..rows.min(self.frame().height())).collect::<Vec<_>>())
            }
            Stat::Reverse => self
                .frame()
                .take(&(0..self.frame().height()).rev().collect::<Vec<_>>()),
            Stat::Sort(value) => sort_frame(self.frame(), &value.pairs)?,
            Stat::Join(JoinStat::SingleCol(value)) => {
                let right = self
                    .input
                    .get(&value.other)
                    .ok_or_else(|| Error::FrameNotFound(value.other.clone()))?;
                join_frames(self.frame(), right, value)?
            }
            Stat::Use(_) | Stat::Clone(_) => unreachable!(),
        };
        self.set_frame(frame);
        Ok(())
    }
}

fn filter_frame(frame: &Frame, expression: &Expr) -> Result<Frame> {
    let condition = row_column(frame, expression, "filter")?;
    let mut indices = Vec::new();
    for index in 0..frame.height() {
        if condition
            .get(index)
            .expect("row column has the frame height")
            .bool("filter")?
            .unwrap_or(false)
        {
            indices.push(index);
        }
    }
    Ok(frame.take(&indices))
}

fn sort_frame(frame: &Frame, pairs: &[(SortOrder, String)]) -> Result<Frame> {
    let columns = pairs
        .iter()
        .map(|(order, name)| Ok((frame.column(name)?, *order)))
        .collect::<Result<Vec<_>>>()?;
    Ok(frame.take(&sorted_indices(&columns, frame.height())?))
}

fn group_aggregate(frame: &Frame, group_by: &[String], expressions: &[Expr]) -> Result<Frame> {
    let expressions = expand_selectors(frame, expressions, group_by);
    if group_by.is_empty() && expressions.is_empty() {
        return Ok(Frame::default());
    }
    let keys = group_by
        .iter()
        .map(|name| frame.column(name))
        .collect::<Result<Vec<_>>>()?;
    let mut groups: Vec<(Vec<Value>, Vec<usize>)> = Vec::new();
    let mut lookup: HashMap<Vec<ValueKey>, usize> = HashMap::new();
    for row in 0..frame.height() {
        let values = keys
            .iter()
            .map(|column| column.get(row).unwrap())
            .collect::<Vec<_>>();
        let key = values.iter().map(Value::key).collect::<Vec<_>>();
        if let Some(group) = lookup.get(&key) {
            groups[*group].1.push(row);
        } else {
            lookup.insert(key, groups.len());
            groups.push((values, vec![row]));
        }
    }
    if group_by.is_empty() && groups.is_empty() {
        groups.push((Vec::new(), Vec::new()));
    }
    let mut output = group_by
        .iter()
        .enumerate()
        .map(|(index, name)| {
            Column::from_values(
                name,
                groups.iter().map(|(key, _)| key[index].clone()).collect(),
            )
        })
        .collect::<Vec<_>>();
    for expression in &expressions {
        let values = groups
            .iter()
            .map(|(_, rows)| {
                let result = evaluate_shaped(&frame.take(rows), expression)?;
                Ok(if result.shape == Shape::Scalar {
                    result
                        .column
                        .into_values()
                        .into_iter()
                        .next()
                        .unwrap_or_default()
                } else {
                    Value::List(result.column.into_values().into())
                })
            })
            .collect::<Result<_>>()?;
        output.push(Column::from_values(expression_name(expression), values));
    }
    Frame::with_height(output, groups.len())
}

fn join_frames(left: &Frame, right: &Frame, join: &SingleColJoinStat) -> Result<Frame> {
    if join.ty == SingleColJoinType::Right {
        return join_frames_inner(
            right,
            left,
            join.right_on.as_ref().unwrap_or(&join.left_on),
            &join.left_on,
            SingleColJoinType::Left,
        );
    }
    join_frames_inner(
        left,
        right,
        &join.left_on,
        join.right_on.as_ref().unwrap_or(&join.left_on),
        join.ty,
    )
}

fn join_frames_inner(
    left: &Frame,
    right: &Frame,
    left_on: &Expr,
    right_on: &Expr,
    ty: SingleColJoinType,
) -> Result<Frame> {
    let left_key = row_column(left, left_on, "join key")?;
    let right_key = row_column(right, right_on, "join key")?;
    let mut right_rows: HashMap<ValueKey, Vec<usize>> = HashMap::new();
    for row in 0..right.height() {
        let value = right_key.get(row).unwrap();
        if !matches!(value, Value::Null) {
            right_rows.entry(value.key()).or_default().push(row);
        }
    }
    let mut pairs = Vec::new();
    let mut matched_right = vec![false; right.height()];
    for left_row in 0..left.height() {
        let value = left_key.get(left_row).unwrap();
        let matches = (!matches!(value, Value::Null))
            .then(|| right_rows.get(&value.key()))
            .flatten();
        if let Some(matches) = matches {
            for right_row in matches {
                pairs.push((Some(left_row), Some(*right_row)));
                matched_right[*right_row] = true;
            }
        } else if matches!(ty, SingleColJoinType::Left | SingleColJoinType::Full) {
            pairs.push((Some(left_row), None));
        }
    }
    if ty == SingleColJoinType::Full {
        pairs.extend(
            matched_right
                .iter()
                .enumerate()
                .filter(|(_, matched)| !**matched)
                .map(|(row, _)| (None, Some(row))),
        );
    }
    build_join_frame(left, right, &pairs)
}

fn row_column(frame: &Frame, expression: &Expr, operation: &'static str) -> Result<Column> {
    evaluate_shaped(frame, expression)?.materialize(frame.height(), operation)
}

fn build_join_frame(
    left: &Frame,
    right: &Frame,
    pairs: &[(Option<usize>, Option<usize>)],
) -> Result<Frame> {
    let mut names = Vec::with_capacity(left.width() + right.width());
    let mut columns = Vec::with_capacity(left.width() + right.width());
    for (side, rows) in [
        (left, pairs.iter().map(|pair| pair.0).collect::<Vec<_>>()),
        (right, pairs.iter().map(|pair| pair.1).collect::<Vec<_>>()),
    ] {
        for column in side.columns() {
            let mut name = column.name().to_owned();
            while names.contains(&name) {
                name.push_str("_right");
            }
            names.push(name.clone());
            columns.push(Column::from_values(
                name,
                rows.iter()
                    .map(|row| row.map(|row| column.get(row).unwrap()).unwrap_or_default())
                    .collect(),
            ));
        }
    }
    Frame::with_height(columns, pairs.len())
}

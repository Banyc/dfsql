use std::cmp::Ordering;

use crate::sql::SortOrder;
use crate::sql::expr::SortByExpr;

use crate::backend::dynamic::{Column, Error, Frame, Result};

use super::{Evaluated, combined_len, evaluate_shaped};

pub(crate) fn sorted_indices(
    columns: &[(&Column, SortOrder)],
    height: usize,
) -> Result<Vec<usize>> {
    let mut indices: Vec<_> = (0..height).collect();
    let mut failure = None;
    indices.sort_by(|left, right| {
        if failure.is_some() {
            return Ordering::Equal;
        }
        for (column, order) in columns {
            let compared = column.value(*left, height).and_then(|left| {
                column
                    .value(*right, height)
                    .and_then(|right| left.compare(&right, "sort"))
            });
            let mut compared = match compared {
                Ok(compared) => compared,
                Err(error) => {
                    failure = Some(error);
                    return Ordering::Equal;
                }
            };
            if *order == SortOrder::Desc {
                compared = compared.reverse();
            }
            if compared != Ordering::Equal {
                return compared;
            }
        }
        Ordering::Equal
    });
    failure.map_or(Ok(indices), Err)
}

pub(super) fn evaluate_sort_by(frame: &Frame, sort: &SortByExpr) -> Result<Evaluated> {
    if sort.pairs.is_empty() {
        return Err(Error::InvalidValue {
            operation: "sort by",
            value: "no sort keys".into(),
        });
    }
    let value = evaluate_shaped(frame, &sort.expr)?;
    let keys: Result<Vec<_>> = sort
        .pairs
        .iter()
        .map(|(order, expression)| Ok((evaluate_shaped(frame, expression)?, *order)))
        .collect();
    let keys = keys?;
    let len = combined_len(
        "sort-by",
        std::iter::once(&value).chain(keys.iter().map(|(key, _)| key)),
    )?;
    let shape = keys
        .iter()
        .fold(value.shape, |shape, (key, _)| shape.merge(key.shape));
    let key_columns: Result<Vec<_>> = keys
        .into_iter()
        .map(|(key, order)| Ok((key.materialize(len, "sort-by")?, order)))
        .collect();
    let key_columns = key_columns?;
    let references: Vec<_> = key_columns
        .iter()
        .map(|(key, order)| (key, *order))
        .collect();
    let indices = sorted_indices(&references, len)?;
    let name = value.column.name().to_owned();
    let value_type = value.column.value_type();
    let column = Column::from_values_with_hint(
        name,
        indices
            .iter()
            .map(|index| value.value(*index, len, "sort-by"))
            .collect::<Result<_>>()?,
        value_type,
    );
    Ok(Evaluated { column, shape })
}

pub(super) fn apply_sort(column: Column, order: SortOrder) -> Result<Column> {
    let indices = sorted_indices(&[(&column, order)], column.len())?;
    let name = column.name().to_owned();
    let value_type = column.value_type();
    Ok(Column::from_values_with_hint(
        name,
        indices
            .iter()
            .map(|index| column.get(*index).unwrap())
            .collect(),
        value_type,
    ))
}

use crate::{Frame, MaterializedFrame};
use anyhow::{Context, anyhow, bail, ensure};
use hdv::format::{AtomScheme, AtomType, AtomValue, ValueRow};
use hdv::io::{
    bin::{HdvBinRawReader, HdvBinRawWriter},
    text::{HdvTextRawReader, HdvTextRawWriter, HdvTextWriterOptions},
};
use polars::prelude::*;
use std::{
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read},
    path::Path,
};

#[derive(Clone, Copy)]
enum FileFormat {
    Csv,
    Json,
    JsonLines,
    HdvBinary,
    HdvText,
}
impl FileFormat {
    fn from_path(path: &Path) -> anyhow::Result<Self> {
        let extension = path
            .extension()
            .ok_or_else(|| anyhow!("file '{}' has no extension", path.display()))?
            .to_string_lossy()
            .to_ascii_lowercase();
        match extension.as_str() {
            "csv" => Ok(Self::Csv),
            "json" => Ok(Self::Json),
            "ndjson" | "jsonl" => Ok(Self::JsonLines),
            "hdvb" => Ok(Self::HdvBinary),
            "hdvt" => Ok(Self::HdvText),
            _ => bail!(
                "unsupported extension '{}' for file '{}'",
                extension,
                path.display()
            ),
        }
    }
}

pub fn read_df_file(path: impl AsRef<Path>) -> anyhow::Result<Frame> {
    let path = path.as_ref();
    match FileFormat::from_path(path)? {
        FileFormat::Csv => Ok(LazyCsvReader::new(path)
            .with_has_header(true)
            .with_infer_schema_length(None)
            .finish()?),
        FileFormat::Json => {
            let input = open_input(path)?;
            Ok(JsonReader::new(input)
                .with_json_format(JsonFormat::Json)
                .finish()?
                .lazy())
        }
        FileFormat::JsonLines => Ok(LazyJsonLineReader::new(path)
            .with_infer_schema_length(None)
            .finish()?),
        FileFormat::HdvBinary => Ok(read_hdv_binary(open_input(path)?)?.lazy()),
        FileFormat::HdvText => Ok(read_hdv_text(open_input(path)?)?.lazy()),
    }
}

pub fn write_df_output(mut frame: MaterializedFrame, path: impl AsRef<Path>) -> anyhow::Result<()> {
    let path = path.as_ref();
    match FileFormat::from_path(path)? {
        FileFormat::Csv => {
            CsvWriter::new(create_output(path)?).finish(&mut frame)?;
        }
        FileFormat::Json => {
            JsonWriter::new(create_output(path)?)
                .with_json_format(JsonFormat::Json)
                .finish(&mut frame)?;
        }
        FileFormat::JsonLines => {
            JsonWriter::new(create_output(path)?)
                .with_json_format(JsonFormat::JsonLines)
                .finish(&mut frame)?;
        }
        FileFormat::HdvBinary => write_hdv_binary(&frame, path)?,
        FileFormat::HdvText => write_hdv_text(&frame, path)?,
    }
    Ok(())
}

fn open_input(path: &Path) -> anyhow::Result<File> {
    File::open(path).with_context(|| format!("failed to open '{}'", path.display()))
}

fn create_output(path: &Path) -> anyhow::Result<BufWriter<File>> {
    let file = File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("failed to create '{}'", path.display()))?;
    Ok(BufWriter::new(file))
}

fn write_hdv_binary(frame: &MaterializedFrame, path: &Path) -> anyhow::Result<()> {
    let (header, rows) = frame_to_hdv(frame)?;
    ensure!(
        !rows.is_empty(),
        "HDV cannot encode a data frame without rows"
    );
    let mut writer = HdvBinRawWriter::new(create_output(path)?, header);
    for row in &rows {
        writer.write(row)?;
    }
    writer.flush()?;
    Ok(())
}

fn write_hdv_text(frame: &MaterializedFrame, path: &Path) -> anyhow::Result<()> {
    let (header, rows) = frame_to_hdv(frame)?;
    ensure!(
        !rows.is_empty(),
        "HDV cannot encode a data frame without rows"
    );
    validate_hdv_text(&rows)?;
    let options = HdvTextWriterOptions {
        is_csv_header: false,
    };
    let mut writer = HdvTextRawWriter::new(create_output(path)?, header, options);
    for row in &rows {
        writer.write(row)?;
    }
    writer.flush()?;
    Ok(())
}

fn read_hdv_binary(input: File) -> anyhow::Result<MaterializedFrame> {
    let mut reader = HdvBinRawReader::new(BufReader::new(input));
    let mut rows = Vec::new();
    loop {
        match reader.read() {
            Ok(row) => rows.push(row),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error.into()),
        }
    }
    let Some(header) = reader.header() else {
        return Ok(MaterializedFrame::empty());
    };
    frame_from_hdv(header, &rows)
}

fn read_hdv_text(input: File) -> anyhow::Result<MaterializedFrame> {
    let input = UnexpectedEofReader(BufReader::new(input));
    let mut reader = HdvTextRawReader::new(input);
    let mut rows = Vec::new();
    loop {
        match reader.read() {
            Ok(row) => rows.push(row),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error.into()),
        }
    }
    let Some(header) = reader.header() else {
        return Ok(MaterializedFrame::empty());
    };
    frame_from_hdv(header, &rows)
}

struct UnexpectedEofReader<R>(R);

impl<R: Read> Read for UnexpectedEofReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0.read(buffer)
    }
}

impl<R: BufRead> BufRead for UnexpectedEofReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.0.fill_buf()
    }
    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
    }
    fn read_line(&mut self, buffer: &mut String) -> io::Result<usize> {
        match self.0.read_line(buffer)? {
            0 => Err(io::ErrorKind::UnexpectedEof.into()),
            bytes => Ok(bytes),
        }
    }
}

fn frame_to_hdv(frame: &MaterializedFrame) -> anyhow::Result<(Vec<AtomScheme>, Vec<ValueRow>)> {
    let mut header = Vec::with_capacity(frame.width());
    let mut columns: Vec<Vec<Option<AtomValue>>> = Vec::with_capacity(frame.width());
    for column in frame.get_columns() {
        let (atom_type, values) = match column.dtype() {
            DataType::Boolean => (
                AtomType::Bool,
                column
                    .bool()?
                    .iter()
                    .map(|value| value.map(AtomValue::Bool))
                    .collect(),
            ),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                let column = column.cast(&DataType::UInt64)?;
                (
                    AtomType::U64,
                    column
                        .u64()?
                        .iter()
                        .map(|value| value.map(AtomValue::U64))
                        .collect(),
                )
            }
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let column = column.cast(&DataType::Int64)?;
                (
                    AtomType::I64,
                    column
                        .i64()?
                        .iter()
                        .map(|value| value.map(AtomValue::I64))
                        .collect(),
                )
            }
            DataType::Float32 => (
                AtomType::F32,
                column
                    .f32()?
                    .iter()
                    .map(|value| value.map(AtomValue::F32))
                    .collect(),
            ),
            DataType::Float64 => (
                AtomType::F64,
                column
                    .f64()?
                    .iter()
                    .map(|value| value.map(AtomValue::F64))
                    .collect(),
            ),
            DataType::String => (
                AtomType::String,
                column
                    .str()?
                    .iter()
                    .map(|value| value.map(|value| AtomValue::String(value.into())))
                    .collect(),
            ),
            DataType::Binary => (
                AtomType::Bytes,
                column
                    .binary()?
                    .iter()
                    .map(|value| value.map(|value| AtomValue::Bytes(value.into())))
                    .collect(),
            ),
            data_type => bail!(
                "HDV does not support Polars data type '{data_type}' in column '{}'",
                column.name()
            ),
        };
        header.push(AtomScheme {
            name: column.name().to_string(),
            r#type: atom_type,
        });
        columns.push(values);
    }
    let rows = (0..frame.height())
        .map(|row| ValueRow::new(columns.iter().map(|column| column[row].clone()).collect()))
        .collect();
    Ok((header, rows))
}

fn frame_from_hdv(header: &[AtomScheme], rows: &[ValueRow]) -> anyhow::Result<MaterializedFrame> {
    let mut columns = Vec::with_capacity(header.len());
    for (index, scheme) in header.iter().enumerate() {
        let name = scheme.name.clone().into();
        let column = match scheme.r#type {
            AtomType::Bool => Column::new(
                name,
                hdv_column(rows, index, AtomValue::bool, AtomType::Bool)?,
            ),
            AtomType::U64 => Column::new(
                name,
                hdv_column(rows, index, AtomValue::u64, AtomType::U64)?,
            ),
            AtomType::I64 => Column::new(
                name,
                hdv_column(rows, index, AtomValue::i64, AtomType::I64)?,
            ),
            AtomType::F32 => Column::new(
                name,
                hdv_column(rows, index, AtomValue::f32, AtomType::F32)?,
            ),
            AtomType::F64 => Column::new(
                name,
                hdv_column(rows, index, AtomValue::f64, AtomType::F64)?,
            ),
            AtomType::String => Column::new(
                name,
                hdv_column(
                    rows,
                    index,
                    |value| value.string().map(|value| value.to_string()),
                    AtomType::String,
                )?,
            ),
            AtomType::Bytes => Column::new(
                name,
                hdv_column(
                    rows,
                    index,
                    |value| value.bytes().map(|value| value.to_vec()),
                    AtomType::Bytes,
                )?,
            ),
        };
        columns.push(column);
    }
    Ok(MaterializedFrame::new(columns)?)
}

fn hdv_column<T>(
    rows: &[ValueRow],
    index: usize,
    convert: impl Fn(&AtomValue) -> Option<T>,
    expected: AtomType,
) -> anyhow::Result<Vec<Option<T>>> {
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            let value = row.atoms().get(index).ok_or_else(|| {
                anyhow!("HDV row {row_index} does not contain column index {index}")
            })?;
            value
                .as_ref()
                .map(|value| {
                    convert(value).ok_or_else(|| {
                        anyhow!("HDV row {row_index}, column {index} is not of type '{expected:?}'")
                    })
                })
                .transpose()
        })
        .collect()
}

fn validate_hdv_text(rows: &[ValueRow]) -> anyhow::Result<()> {
    for row in rows {
        for value in row.atoms().iter().flatten() {
            match value {
                AtomValue::Bytes(_) => bail!("HDV text does not support binary values"),
                AtomValue::String(value)
                    if value.contains(',')
                        || value.contains('"')
                        || value.contains('\n')
                        || value.trim_start().len() != value.len() =>
                {
                    bail!("HDV text cannot encode string '{value}'")
                }
                _ => {}
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    struct TestPath(PathBuf);

    impl TestPath {
        fn new(extension: &str) -> Self {
            let sequence = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            let mut path = std::env::temp_dir();
            path.push(format!(
                "dfsql-file-ops-{}-{sequence}.{extension}",
                std::process::id()
            ));
            Self(path)
        }
        fn as_path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn round_trip(frame: &MaterializedFrame, extension: &str) -> MaterializedFrame {
        let path = TestPath::new(extension);
        write_df_output(frame.clone(), path.as_path()).unwrap();
        read_df_file(path.as_path()).unwrap().collect().unwrap()
    }

    #[test]
    fn tabular_text_formats_round_trip() {
        let frame =
            polars::df!("id" => [1_i64, 2], "enabled" => [true, false], "name" => ["one", "two"])
                .unwrap();
        for extension in ["csv", "json", "ndjson", "jsonl"] {
            let actual = round_trip(&frame, extension);
            assert!(
                frame.equals_missing(&actual),
                "{extension} round trip produced {actual}"
            );
        }
    }

    #[test]
    fn hdv_binary_round_trip_preserves_supported_types_and_nulls() {
        let frame = MaterializedFrame::new(vec![
            Column::new("bool".into(), vec![Some(true), None]),
            Column::new("uint".into(), vec![Some(1_u64), None]),
            Column::new("int".into(), vec![Some(-1_i64), None]),
            Column::new("f32".into(), vec![Some(1.5_f32), None]),
            Column::new("f64".into(), vec![Some(2.5_f64), None]),
            Column::new(
                "string".into(),
                vec![Some("one".to_owned()), None::<String>],
            ),
            Column::new("bytes".into(), vec![Some(vec![1_u8, 2]), None::<Vec<u8>>]),
        ])
        .unwrap();
        let actual = round_trip(&frame, "hdvb");
        assert!(frame.equals_missing(&actual));
    }

    #[test]
    fn hdv_text_round_trip_preserves_supported_text_types() {
        let frame = MaterializedFrame::new(vec![
            Column::new("bool".into(), vec![Some(true), None]),
            Column::new("uint".into(), vec![Some(1_u64), None]),
            Column::new("int".into(), vec![Some(-1_i64), None]),
            Column::new("f32".into(), vec![Some(1.5_f32), None]),
            Column::new("f64".into(), vec![Some(2.5_f64), None]),
            Column::new(
                "string".into(),
                vec![Some("one".to_owned()), None::<String>],
            ),
        ])
        .unwrap();
        let actual = round_trip(&frame, "hdvt");
        assert!(frame.equals_missing(&actual));
    }

    #[test]
    fn unsupported_extension_does_not_truncate_existing_file() {
        let path = TestPath::new("unknown");
        std::fs::write(path.as_path(), "keep").unwrap();
        let frame = polars::df!("id" => [1_i64]).unwrap();
        assert!(write_df_output(frame, path.as_path()).is_err());
        assert_eq!(std::fs::read_to_string(path.as_path()).unwrap(), "keep");
    }
}

pub(crate) mod atomic_file;
pub(crate) mod sql_file;

use crate::{Frame, MaterializedFrame};
use anyhow::{Context, anyhow, bail, ensure};
use atomic_file::{StagedFile, stage_file};
use hdv::format::{AtomScheme, AtomType, AtomValue, ValueRow};
use hdv::io::{
    bin::{HdvBinRawReader, HdvBinRawWriter},
    text::{HdvTextRawReader, HdvTextRawWriter, HdvTextWriterOptions},
};
use polars::prelude::*;
use std::{
    cell::Cell,
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read},
    path::Path,
    rc::Rc,
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

pub(crate) fn read_df_file(path: impl AsRef<Path>) -> anyhow::Result<Frame> {
    let path = path.as_ref();
    match FileFormat::from_path(path)? {
        FileFormat::Csv => Ok(Frame::from_inner(
            LazyCsvReader::new(PlRefPath::try_from_path(path)?)
                .with_has_header(true)
                .with_infer_schema_length(None)
                .finish()?,
        )),
        FileFormat::Json => {
            let input = open_input(path)?;
            Ok(Frame::from_inner(
                JsonReader::new(input)
                    .with_json_format(JsonFormat::Json)
                    .finish()?
                    .lazy(),
            ))
        }
        FileFormat::JsonLines => Ok(Frame::from_inner(
            LazyJsonLineReader::new(PlRefPath::try_from_path(path)?)
                .with_infer_schema_length(None)
                .finish()?,
        )),
        FileFormat::HdvBinary => Ok(Frame::from_inner(
            read_hdv_binary(open_input(path)?)?.lazy(),
        )),
        FileFormat::HdvText => Ok(Frame::from_inner(read_hdv_text(open_input(path)?)?.lazy())),
    }
}

pub(crate) fn write_df_output(frame: MaterializedFrame, path: impl AsRef<Path>) -> anyhow::Result<()> {
    stage_df_output(frame, path)?.commit()
}

pub(crate) fn stage_df_output(
    mut frame: MaterializedFrame,
    path: impl AsRef<Path>,
) -> anyhow::Result<StagedFile> {
    let path = path.as_ref();
    let format = FileFormat::from_path(path)?;
    stage_file(path, |output| {
        match format {
            FileFormat::Csv => {
                CsvWriter::new(output).finish(frame.inner_mut())?;
            }
            FileFormat::Json => {
                JsonWriter::new(output)
                    .with_json_format(JsonFormat::Json)
                    .finish(frame.inner_mut())?;
            }
            FileFormat::JsonLines => {
                JsonWriter::new(output)
                    .with_json_format(JsonFormat::JsonLines)
                    .finish(frame.inner_mut())?;
            }
            FileFormat::HdvBinary => write_hdv_binary(&frame, output)?,
            FileFormat::HdvText => write_hdv_text(&frame, output)?,
        }
        Ok(())
    })
}

fn open_input(path: &Path) -> anyhow::Result<File> {
    File::open(path).with_context(|| format!("failed to open '{}'", path.display()))
}

fn write_hdv_binary(frame: &MaterializedFrame, output: &mut BufWriter<File>) -> anyhow::Result<()> {
    let (header, rows) = frame_to_hdv(frame)?;
    ensure!(
        !rows.is_empty(),
        "HDV cannot encode a data frame without rows"
    );
    let mut writer = HdvBinRawWriter::new(output, header);
    for row in &rows {
        writer.write(row)?;
    }
    writer.flush()?;
    Ok(())
}

fn write_hdv_text(frame: &MaterializedFrame, output: &mut BufWriter<File>) -> anyhow::Result<()> {
    let (header, rows) = frame_to_hdv(frame)?;
    ensure!(
        !rows.is_empty(),
        "HDV cannot encode a data frame without rows"
    );
    validate_hdv_text(&rows)?;
    let options = HdvTextWriterOptions {
        is_csv_header: false,
    };
    let mut writer = HdvTextRawWriter::new(output, header, options);
    for row in &rows {
        writer.write(row)?;
    }
    writer.flush()?;
    Ok(())
}

fn read_hdv_binary(input: File) -> anyhow::Result<DataFrame> {
    let bytes_read = Rc::new(Cell::new(0));
    let input = CountingReader {
        inner: BufReader::new(input),
        bytes_read: bytes_read.clone(),
    };
    let mut reader = HdvBinRawReader::new(input);
    let mut rows = Vec::new();
    loop {
        let before = bytes_read.get();
        match reader.read() {
            Ok(row) => rows.push(row),
            Err(error)
                if error.kind() == std::io::ErrorKind::UnexpectedEof
                    && reader.header().is_some()
                    && bytes_read.get() == before =>
            {
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(error).context("truncated HDV binary input");
            }
            Err(error) => return Err(error.into()),
        }
    }
    let header = reader
        .header()
        .ok_or_else(|| anyhow!("HDV binary input has no header"))?;
    frame_from_hdv(header, &rows)
}

struct CountingReader<R> {
    inner: R,
    bytes_read: Rc<Cell<usize>>,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let bytes = self.inner.read(buffer)?;
        self.bytes_read.set(self.bytes_read.get() + bytes);
        Ok(bytes)
    }
}

fn read_hdv_text(input: File) -> anyhow::Result<DataFrame> {
    let line_shape = Rc::new(Cell::new((0, false)));
    let input = UnexpectedEofReader {
        inner: BufReader::new(input),
        line_shape: line_shape.clone(),
    };
    let mut reader = HdvTextRawReader::new(input);
    let mut rows = Vec::new();
    loop {
        match reader.read() {
            Ok(row) => {
                let (commas, terminated) = line_shape.get();
                ensure!(
                    commas == row.atoms().len() && (terminated || row.atoms().is_empty()),
                    "HDV text row {} has an unexpected number of fields",
                    rows.len()
                );
                rows.push(row);
            }
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error.into()),
        }
    }
    let header = reader
        .header()
        .ok_or_else(|| anyhow!("HDV text input has no header"))?;
    frame_from_hdv(header, &rows)
}

struct UnexpectedEofReader<R> {
    inner: R,
    line_shape: Rc<Cell<(usize, bool)>>,
}

impl<R: Read> Read for UnexpectedEofReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buffer)
    }
}

impl<R: BufRead> BufRead for UnexpectedEofReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf()
    }
    fn consume(&mut self, amount: usize) {
        self.inner.consume(amount);
    }
    fn read_line(&mut self, buffer: &mut String) -> io::Result<usize> {
        match self.inner.read_line(buffer)? {
            0 => Err(io::ErrorKind::UnexpectedEof.into()),
            bytes => {
                let line = buffer.trim_end_matches(['\r', '\n']);
                self.line_shape.set((
                    line.bytes().filter(|byte| *byte == b',').count(),
                    line.ends_with(','),
                ));
                Ok(bytes)
            }
        }
    }
}

fn frame_to_hdv(frame: &MaterializedFrame) -> anyhow::Result<(Vec<AtomScheme>, Vec<ValueRow>)> {
    let inner = frame.inner();
    let mut header = Vec::with_capacity(inner.width());
    let mut columns: Vec<Vec<Option<AtomValue>>> = Vec::with_capacity(inner.width());
    for column in inner.columns() {
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
                "HDV does not support Polars data type {} in column {}",
                data_type,
                column.name()
            ),
        };
        header.push(AtomScheme {
            name: column.name().to_string(),
            r#type: atom_type,
        });
        columns.push(values);
    }
    let rows = (0..inner.height())
        .map(|row| ValueRow::new(columns.iter().map(|column| column[row].clone()).collect()))
        .collect();
    Ok((header, rows))
}

fn frame_from_hdv(header: &[AtomScheme], rows: &[ValueRow]) -> anyhow::Result<DataFrame> {
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
    Ok(DataFrame::new(rows.len(), columns)?)
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
                    if value.is_empty()
                        || value.contains(',')
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
        let frame = MaterializedFrame::from_inner(
            polars::df!("id" => [1_i64, 2], "enabled" => [true, false], "name" => ["one", "two"])
                .unwrap(),
        );
        for extension in ["csv", "json", "ndjson", "jsonl"] {
            let actual = round_trip(&frame, extension);
            assert!(
                frame.inner().equals_missing(actual.inner()),
                "{extension} round trip produced {actual:?}"
            );
        }
    }

    #[test]
    fn hdv_binary_round_trip_preserves_supported_types_and_nulls() {
        let frame = MaterializedFrame::from_inner(
            DataFrame::new(
                2,
                vec![
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
                ],
            )
            .unwrap(),
        );
        let actual = round_trip(&frame, "hdvb");
        assert!(frame.inner().equals_missing(actual.inner()));
    }

    #[test]
    fn hdv_rejects_missing_headers_and_truncated_binary() {
        for extension in ["hdvb", "hdvt"] {
            let empty = TestPath::new(extension);
            std::fs::write(empty.as_path(), []).unwrap();
            assert!(read_df_file(empty.as_path()).is_err());
        }
        let truncated = TestPath::new("hdvb");
        let frame = MaterializedFrame::from_inner(polars::df!("id" => [1_i64, 2]).unwrap());
        write_df_output(frame, truncated.as_path()).unwrap();
        let mut bytes = std::fs::read(truncated.as_path()).unwrap();
        bytes.pop();
        std::fs::write(truncated.as_path(), bytes).unwrap();
        assert!(read_df_file(truncated.as_path()).is_err());
    }

    #[test]
    fn hdv_text_round_trip_preserves_supported_text_types() {
        let frame = MaterializedFrame::from_inner(
            DataFrame::new(
                2,
                vec![
                    Column::new("bool".into(), vec![Some(true), None]),
                    Column::new("uint".into(), vec![Some(1_u64), None]),
                    Column::new("int".into(), vec![Some(-1_i64), None]),
                    Column::new("f32".into(), vec![Some(1.5_f32), None]),
                    Column::new("f64".into(), vec![Some(2.5_f64), None]),
                    Column::new(
                        "string".into(),
                        vec![Some("one".to_owned()), None::<String>],
                    ),
                ],
            )
            .unwrap(),
        );
        let actual = round_trip(&frame, "hdvt");
        assert!(frame.inner().equals_missing(actual.inner()));
        let frame = MaterializedFrame::from_inner(DataFrame::new(2, Vec::<Column>::new()).unwrap());
        let actual = round_trip(&frame, "hdvt");
        assert_eq!((actual.height(), actual.width()), (2, 0));
    }

    #[test]
    fn hdv_text_rejects_empty_strings() {
        let path = TestPath::new("hdvt");
        let frame = MaterializedFrame::from_inner(polars::df!("value" => [""]).unwrap());
        assert!(write_df_output(frame, path.as_path()).is_err());
    }

    #[test]
    fn hdv_text_rejects_extra_fields() {
        let path = TestPath::new("hdvt");
        let frame = MaterializedFrame::from_inner(polars::df!("value" => [1_i64]).unwrap());
        write_df_output(frame, path.as_path()).unwrap();
        let contents = std::fs::read_to_string(path.as_path())
            .unwrap()
            .replace("1,\n", "1, discarded,\n");
        std::fs::write(path.as_path(), contents).unwrap();
        assert!(read_df_file(path.as_path()).is_err());
    }

    #[test]
    fn unsupported_extension_does_not_truncate_existing_file() {
        let path = TestPath::new("unknown");
        std::fs::write(path.as_path(), "keep").unwrap();
        let frame = MaterializedFrame::from_inner(polars::df!("id" => [1_i64]).unwrap());
        assert!(write_df_output(frame, path.as_path()).is_err());
        assert_eq!(std::fs::read_to_string(path.as_path()).unwrap(), "keep");
    }

    #[test]
    fn failed_serialization_does_not_replace_existing_file() {
        let path = TestPath::new("hdvb");
        std::fs::write(path.as_path(), "keep").unwrap();
        let frame = MaterializedFrame::from_inner(
            DataFrame::new(
                1,
                vec![Column::new(
                    "unsupported".into(),
                    vec![Series::new("item".into(), vec![1_i64])],
                )],
            )
            .unwrap(),
        );
        assert!(write_df_output(frame, path.as_path()).is_err());
        assert_eq!(std::fs::read_to_string(path.as_path()).unwrap(), "keep");
    }
}

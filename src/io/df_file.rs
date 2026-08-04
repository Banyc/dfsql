use super::atomic_file::StagedFile;
#[cfg(feature = "polars-backend")]
use super::atomic_file::stage_file;
#[cfg(feature = "polars-backend")]
use super::hdv_file;
use crate::{Frame, MaterializedFrame};
#[cfg(feature = "polars-backend")]
use anyhow::Context;
use anyhow::{anyhow, bail};
#[cfg(feature = "polars-backend")]
use polars::prelude::*;
use std::path::Path;
#[cfg(feature = "polars-backend")]
use std::{fs::File, io::BufWriter};

/// HDV is a tabular data format from the `hdv` crate.
#[derive(Clone, Copy, Debug)]
pub(super) enum FileFormat {
    Csv,
    Json,
    JsonLines,
    HdvBinary,
    HdvText,
}

impl FileFormat {
    pub(super) fn from_path(path: &Path) -> anyhow::Result<Self> {
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
    let format = FileFormat::from_path(path)?;
    #[cfg(not(feature = "polars-backend"))]
    panic!("{format:?} file operations require the 'polars-backend' feature");
    #[cfg(feature = "polars-backend")]
    match format {
        FileFormat::Csv => read_csv(path),
        FileFormat::Json => read_json(path),
        FileFormat::JsonLines => read_json_lines(path),
        FileFormat::HdvBinary => read_hdv_binary(path),
        FileFormat::HdvText => read_hdv_text(path),
    }
}

pub fn write_df_output(frame: MaterializedFrame, path: impl AsRef<Path>) -> anyhow::Result<()> {
    stage_df_output(frame, path)?.commit()
}

pub(crate) fn stage_df_output(
    frame: MaterializedFrame,
    path: impl AsRef<Path>,
) -> anyhow::Result<StagedFile> {
    let path = path.as_ref();
    let format = FileFormat::from_path(path)?;
    #[cfg(not(feature = "polars-backend"))]
    {
        let _ = frame;
        panic!("{format:?} file operations require the 'polars-backend' feature");
    }
    #[cfg(feature = "polars-backend")]
    let mut frame = frame;
    #[cfg(feature = "polars-backend")]
    stage_file(path, move |output| match format {
        FileFormat::Csv => write_csv(&mut frame, output),
        FileFormat::Json => write_json(&mut frame, output),
        FileFormat::JsonLines => write_json_lines(&mut frame, output),
        FileFormat::HdvBinary => hdv_file::write_hdv_binary(&frame, output),
        FileFormat::HdvText => hdv_file::write_hdv_text(&frame, output),
    })
}

#[cfg(feature = "polars-backend")]
fn open_input(path: &Path) -> anyhow::Result<File> {
    File::open(path).with_context(|| format!("failed to open '{}'", path.display()))
}

// ---- CSV ----

#[cfg(feature = "polars-backend")]
fn read_csv(path: &Path) -> anyhow::Result<Frame> {
    Ok(Frame::from_inner(
        LazyCsvReader::new(PlRefPath::try_from_path(path)?)
            .with_has_header(true)
            .with_infer_schema_length(None)
            .finish()?,
    ))
}

#[cfg(feature = "polars-backend")]
fn write_csv(frame: &mut MaterializedFrame, output: &mut BufWriter<File>) -> anyhow::Result<()> {
    CsvWriter::new(output).finish(frame.inner_mut())?;
    Ok(())
}

// ---- JSON ----

#[cfg(feature = "polars-backend")]
fn read_json(path: &Path) -> anyhow::Result<Frame> {
    Ok(Frame::from_inner(
        JsonReader::new(open_input(path)?)
            .with_json_format(JsonFormat::Json)
            .finish()?
            .lazy(),
    ))
}

#[cfg(feature = "polars-backend")]
fn write_json(frame: &mut MaterializedFrame, output: &mut BufWriter<File>) -> anyhow::Result<()> {
    JsonWriter::new(output)
        .with_json_format(JsonFormat::Json)
        .finish(frame.inner_mut())?;
    Ok(())
}

// ---- JSON Lines ----

#[cfg(feature = "polars-backend")]
fn read_json_lines(path: &Path) -> anyhow::Result<Frame> {
    Ok(Frame::from_inner(
        LazyJsonLineReader::new(PlRefPath::try_from_path(path)?)
            .with_infer_schema_length(None)
            .finish()?,
    ))
}

#[cfg(feature = "polars-backend")]
fn write_json_lines(
    frame: &mut MaterializedFrame,
    output: &mut BufWriter<File>,
) -> anyhow::Result<()> {
    JsonWriter::new(output)
        .with_json_format(JsonFormat::JsonLines)
        .finish(frame.inner_mut())?;
    Ok(())
}

// ---- HDV ----

#[cfg(feature = "polars-backend")]
fn read_hdv_binary(path: &Path) -> anyhow::Result<Frame> {
    Ok(Frame::from_inner(
        hdv_file::read_hdv_binary(open_input(path)?)?.lazy(),
    ))
}

#[cfg(feature = "polars-backend")]
fn read_hdv_text(path: &Path) -> anyhow::Result<Frame> {
    Ok(Frame::from_inner(
        hdv_file::read_hdv_text(open_input(path)?)?.lazy(),
    ))
}

#[cfg(all(test, feature = "polars-backend"))]
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
    fn unsupported_extension_does_not_truncate_existing_file() {
        let path = TestPath::new("unknown");
        std::fs::write(path.as_path(), "keep").unwrap();
        let frame = MaterializedFrame::from_inner(polars::df!("id" => [1_i64]).unwrap());
        assert!(write_df_output(frame, path.as_path()).is_err());
        assert_eq!(std::fs::read_to_string(path.as_path()).unwrap(), "keep");
    }
}

#[cfg(all(test, not(feature = "polars-backend")))]
#[test]
#[should_panic(expected = "file operations require the 'polars-backend' feature")]
fn recognized_format_panics_without_polars_backend() {
    let _ = read_df_file("input.csv");
}

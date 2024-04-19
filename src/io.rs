use std::{
    io::{BufRead, BufReader, Read, Write},
    path::Path,
};

use anyhow::bail;
use polars::prelude::*;

use crate::sql;

pub fn read_df_file(
    path: impl AsRef<Path>,
    infer_schema_length: Option<usize>,
) -> anyhow::Result<LazyFrame> {
    let Some(extension) = path.as_ref().extension() else {
        bail!(
            "No extension at the name of the file `{}`",
            path.as_ref().to_string_lossy()
        );
    };
    Ok(match extension.to_string_lossy().as_ref() {
        "csv" => LazyCsvReader::new(&path)
            .has_header(true)
            .with_infer_schema_length(infer_schema_length)
            .finish()?,
        "json" => {
            let file = std::fs::File::options().read(true).open(&path)?;
            JsonReader::new(file).finish()?.lazy()
        }
        "ndjson" | "jsonl" => LazyJsonLineReader::new(&path)
            .with_infer_schema_length(infer_schema_length)
            .finish()?,
        _ => bail!(
            "Unknown extension `{}` at the name of the file `{}`",
            extension.to_string_lossy(),
            path.as_ref().to_string_lossy()
        ),
    })
}

pub fn write_df_output(mut df: DataFrame, path: impl AsRef<Path>) -> anyhow::Result<()> {
    let Some(extension) = path.as_ref().extension() else {
        bail!(
            "No extension at the name of the file `{}`",
            path.as_ref().to_string_lossy()
        );
    };
    let output = std::fs::File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    match extension.to_string_lossy().as_ref() {
        "csv" => CsvWriter::new(output).finish(&mut df)?,
        "json" => JsonWriter::new(output).finish(&mut df)?,
        "ndjson" | "jsonl" => {
            bail!(
                "No `JsonLineWriter` available to write `{}`",
                path.as_ref().to_string_lossy()
            );
        }
        _ => bail!(
            "Unknown extension `{}` at the name of the file `{}`",
            extension.to_string_lossy(),
            path.as_ref().to_string_lossy()
        ),
    }
    Ok(())
}

pub fn write_repl_sql_output<'a>(
    sql: impl Iterator<Item = &'a String>,
    path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let mut output = std::fs::File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    for s in sql {
        output.write_all(s.as_bytes())?;
        output.write_all("\n".as_bytes())?;
    }
    Ok(())
}

pub fn read_repl_sql_file(path: impl AsRef<Path>) -> anyhow::Result<Vec<String>> {
    let file = std::fs::File::options().read(true).open(&path)?;
    let mut reader = BufReader::new(file);
    let mut lines = vec![];
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            // EOF
            break;
        }
        if line.ends_with('\n') {
            line.pop();
        }
        lines.push(line);
    }
    Ok(lines)
}

pub fn read_sql_file(path: impl AsRef<Path>) -> anyhow::Result<sql::S> {
    let mut file = std::fs::File::options().read(true).open(&path)?;
    let mut src = String::new();
    file.read_to_string(&mut src)?;
    drop(file);
    let s = sql::parse(&src)?;
    Ok(s)
}

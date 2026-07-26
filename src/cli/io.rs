use std::{
    io::{BufRead, BufReader, Read, Write},
    path::Path,
};

use crate::sql;

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

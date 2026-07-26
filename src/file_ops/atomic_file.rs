use anyhow::{Context, anyhow, bail};
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);

pub(crate) struct StagedFile {
    target: PathBuf,
    temporary: Option<PathBuf>,
}

impl StagedFile {
    pub(crate) fn commit(mut self) -> anyhow::Result<()> {
        let temporary = self
            .temporary
            .take()
            .expect("a staged file has a temporary path");
        if let Err(error) = fs::rename(&temporary, &self.target) {
            self.temporary = Some(temporary);
            return Err(error)
                .with_context(|| format!("failed to replace '{}'", self.target.display()));
        }
        Ok(())
    }

    pub(crate) fn commit_pair(first: Self, second: Self) -> anyhow::Result<()> {
        commit_pair(vec![first, second])
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        if let Some(temporary) = self.temporary.take() {
            let _ = fs::remove_file(&temporary);
        }
    }
}

pub(crate) fn stage_file(
    target: impl AsRef<Path>,
    write: impl FnOnce(&mut BufWriter<File>) -> anyhow::Result<()>,
) -> anyhow::Result<StagedFile> {
    let target = target.as_ref();
    let (temporary, file) = create_temporary(target, "tmp")?;
    let staged = StagedFile {
        target: target.to_owned(),
        temporary: Some(temporary),
    };
    let mut output = BufWriter::new(file);
    write(&mut output)?;
    output
        .flush()
        .with_context(|| format!("failed to flush staged output for '{}'", target.display()))?;
    output
        .get_ref()
        .sync_all()
        .with_context(|| format!("failed to sync staged output for '{}'", target.display()))?;
    drop(output);
    Ok(staged)
}

fn commit_pair(mut files: Vec<StagedFile>) -> anyhow::Result<()> {
    let unique_targets = files
        .iter()
        .map(|file| file.target.clone())
        .collect::<HashSet<_>>();
    if unique_targets.len() != files.len() {
        bail!("cannot atomically commit the same output path more than once");
    }
    let mut backups = Vec::with_capacity(files.len());
    for file in &files {
        let backup = backup_existing(&file.target)
            .map_err(|error| with_rollback(error, &files, &backups, 0))?;
        backups.push(backup);
    }
    for index in 0..files.len() {
        let temporary = files[index]
            .temporary
            .take()
            .expect("a staged file has a temporary path");
        if let Err(error) = fs::rename(&temporary, &files[index].target) {
            files[index].temporary = Some(temporary);
            let error = anyhow::Error::new(error).context(format!(
                "failed to replace output '{}'",
                files[index].target.display()
            ));
            return Err(with_rollback(error, &files, &backups, index));
        }
    }
    for backup in backups.into_iter().flatten() {
        let _ = fs::remove_file(backup);
    }
    Ok(())
}

fn backup_existing(target: &Path) -> anyhow::Result<Option<PathBuf>> {
    if !target
        .try_exists()
        .with_context(|| format!("failed to inspect output '{}'", target.display()))?
    {
        return Ok(None);
    }
    let backup = unused_sibling_path(target, "backup")?;
    fs::rename(target, &backup)
        .with_context(|| format!("failed to stage existing output '{}'", target.display()))?;
    Ok(Some(backup))
}

fn with_rollback(
    error: anyhow::Error,
    files: &[StagedFile],
    backups: &[Option<PathBuf>],
    installed: usize,
) -> anyhow::Error {
    match restore_targets(files, backups, installed) {
        Ok(()) => error,
        Err(rollback_error) => anyhow!("{error:#}; rollback failed: {rollback_error:#}"),
    }
}

fn restore_targets(
    files: &[StagedFile],
    backups: &[Option<PathBuf>],
    installed: usize,
) -> anyhow::Result<()> {
    let mut failures = Vec::new();
    for (index, (file, backup)) in files.iter().zip(backups).enumerate() {
        if index < installed
            && let Err(error) = fs::remove_file(&file.target)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            failures.push(format!(
                "failed to remove replacement '{}': {error}",
                file.target.display()
            ));
        }
        if let Some(backup) = backup
            && let Err(error) = fs::rename(backup, &file.target)
        {
            failures.push(format!(
                "failed to restore backup '{}' to '{}': {error}",
                backup.display(),
                file.target.display()
            ));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("{}", failures.join("; "))
    }
}

fn create_temporary(target: &Path, kind: &str) -> anyhow::Result<(PathBuf, File)> {
    for _ in 0..100 {
        let path = sibling_path(target, kind)?;
        match File::options().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to create staged output for '{}'", target.display())
                });
            }
        }
    }
    Err(anyhow!(
        "failed to allocate a staged output path for '{}'",
        target.display()
    ))
}

fn unused_sibling_path(target: &Path, kind: &str) -> anyhow::Result<PathBuf> {
    for _ in 0..100 {
        let path = sibling_path(target, kind)?;
        if !path
            .try_exists()
            .with_context(|| format!("failed to inspect temporary path '{}'", path.display()))?
        {
            return Ok(path);
        }
    }
    Err(anyhow!(
        "failed to allocate a backup path for '{}'",
        target.display()
    ))
}

fn sibling_path(target: &Path, kind: &str) -> anyhow::Result<PathBuf> {
    let name = target
        .file_name()
        .ok_or_else(|| anyhow!("output '{}' has no file name", target.display()))?
        .to_string_lossy();
    let sequence = NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed);
    let temporary_name = format!(".{name}.dfsql-{kind}-{}-{sequence}", std::process::id());
    let parent = target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Ok(parent.join(temporary_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "dfsql-atomic-file-{}-{}-{name}",
            std::process::id(),
            NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn failed_staging_preserves_existing_output() {
        let target = path("preserve");
        fs::write(&target, "original").unwrap();
        let result = stage_file(&target, |output| {
            output.write_all(b"partial")?;
            bail!("serialization failed")
        });
        assert!(result.is_err());
        assert_eq!(fs::read_to_string(&target).unwrap(), "original");
        fs::remove_file(target).unwrap();
    }

    #[test]
    fn pair_commit_replaces_both_outputs() {
        let first_path = path("first");
        let second_path = path("second");
        fs::write(&first_path, "old first").unwrap();
        fs::write(&second_path, "old second").unwrap();
        let first = stage_file(&first_path, |output| {
            output.write_all(b"new first")?;
            Ok(())
        })
        .unwrap();
        let second = stage_file(&second_path, |output| {
            output.write_all(b"new second")?;
            Ok(())
        })
        .unwrap();
        StagedFile::commit_pair(first, second).unwrap();
        assert_eq!(fs::read_to_string(&first_path).unwrap(), "new first");
        assert_eq!(fs::read_to_string(&second_path).unwrap(), "new second");
        fs::remove_file(first_path).unwrap();
        fs::remove_file(second_path).unwrap();
    }

    #[test]
    fn rollback_failures_are_reported() {
        let target = path("rollback-target");
        let missing_backup = path("missing-backup");
        let file = StagedFile {
            target: target.clone(),
            temporary: None,
        };
        let error = with_rollback(
            anyhow!("installation failed"),
            &[file],
            &[Some(missing_backup.clone())],
            0,
        );
        let message = error.to_string();
        assert!(message.contains("installation failed"));
        assert!(message.contains("rollback failed"));
        assert!(message.contains(&missing_backup.display().to_string()));
        assert!(message.contains(&target.display().to_string()));
    }
}

use anyhow::{Context, anyhow, bail};
use std::{
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
        install(&mut self)
    }

    pub(crate) fn commit_pair(first: Self, commit_marker: Self) -> anyhow::Result<()> {
        commit_pair(first, commit_marker)
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
    let permissions = match fs::metadata(target) {
        Ok(metadata) => Some(metadata.permissions()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect output '{}'", target.display()));
        }
    };
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
    if let Some(permissions) = permissions {
        output
            .get_ref()
            .set_permissions(permissions)
            .with_context(|| {
                format!(
                    "failed to preserve permissions for staged output '{}'",
                    target.display()
                )
            })?;
    }
    output
        .get_ref()
        .sync_all()
        .with_context(|| format!("failed to sync staged output for '{}'", target.display()))?;
    drop(output);
    Ok(staged)
}

fn install(file: &mut StagedFile) -> anyhow::Result<()> {
    let temporary = file
        .temporary
        .take()
        .expect("a staged file has a temporary path");
    if let Err(error) = fs::rename(&temporary, &file.target) {
        file.temporary = Some(temporary);
        return Err(error)
            .with_context(|| format!("failed to replace '{}'", file.target.display()));
    }
    Ok(())
}

fn commit_pair(mut first: StagedFile, mut commit_marker: StagedFile) -> anyhow::Result<()> {
    if first.target == commit_marker.target {
        bail!("cannot atomically commit the same output path more than once");
    }
    let backup = copy_existing(&first.target)?;
    if let Err(error) = install(&mut first) {
        if let Some(backup) = backup {
            let _ = fs::remove_file(&backup);
        }
        return Err(error);
    }
    if let Err(error) = install(&mut commit_marker) {
        return Err(with_rollback(error, &first.target, backup.as_deref()));
    }
    if let Some(backup) = backup {
        let _ = fs::remove_file(&backup);
    }
    Ok(())
}

fn with_rollback(error: anyhow::Error, target: &Path, backup: Option<&Path>) -> anyhow::Error {
    match restore_target(target, backup) {
        Ok(()) => error,
        Err(rollback_error) => anyhow!("{error:#}; rollback failed: {rollback_error:#}"),
    }
}

fn restore_target(target: &Path, backup: Option<&Path>) -> anyhow::Result<()> {
    if let Some(backup) = backup {
        fs::rename(backup, target).with_context(|| {
            format!(
                "failed to restore backup '{}' to '{}'",
                backup.display(),
                target.display()
            )
        })
    } else {
        match fs::remove_file(target) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error)
                .with_context(|| format!("failed to remove replacement '{}'", target.display())),
        }
    }
}

fn copy_existing(target: &Path) -> anyhow::Result<Option<PathBuf>> {
    if !target
        .try_exists()
        .with_context(|| format!("failed to inspect output '{}'", target.display()))?
    {
        return Ok(None);
    }
    let backup = unused_sibling_path(target, "backup")?;
    fs::copy(target, &backup)
        .with_context(|| format!("failed to back up existing output '{}'", target.display()))?;
    Ok(Some(backup))
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

    #[cfg(unix)]
    #[test]
    fn replacement_preserves_existing_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let target = path("permissions");
        fs::write(&target, "original").unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        stage_file(&target, |output| {
            output.write_all(b"replacement")?;
            Ok(())
        })
        .unwrap()
        .commit()
        .unwrap();
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );
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
    fn failed_pair_commit_keeps_the_previous_checkpoint() {
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
        fs::remove_file(second.temporary.as_ref().unwrap()).unwrap();
        assert!(StagedFile::commit_pair(first, second).is_err());
        assert_eq!(fs::read_to_string(&first_path).unwrap(), "old first");
        assert_eq!(fs::read_to_string(&second_path).unwrap(), "old second");
        fs::remove_file(first_path).unwrap();
        fs::remove_file(second_path).unwrap();
    }

    #[test]
    fn rollback_failures_are_reported() {
        let target = path("rollback-target");
        let missing_backup = path("missing-backup");
        let error = with_rollback(
            anyhow!("installation failed"),
            &target,
            Some(&missing_backup),
        );
        let message = error.to_string();
        assert!(message.contains("installation failed"));
        assert!(message.contains("rollback failed"));
        assert!(message.contains(&missing_backup.display().to_string()));
        assert!(message.contains(&target.display().to_string()));
    }
}

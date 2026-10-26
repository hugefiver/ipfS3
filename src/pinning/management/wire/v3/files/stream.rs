//! Blocking, same-handle v3 file IO. Run from the CLI's blocking IO boundary.
//! Partial output is deliberately retained, never published as a complete import
//! and never deleted by path (which might have been replaced by another actor).
use super::unavailable;
use crate::{
    pinning::management::wire::v3::{
        Projection, Reader, Writer,
        archive_cursor::{self, ReviewCursor, StagedImport},
    },
    store::pinning::management::native::meter::{Budget, Limits},
};
use anyhow::Result;

use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
};

pub(super) fn reserve(path: &str) -> Result<File> {
    #[cfg(windows)]
    {
        super::windows_private::create(path).map_err(|_| unavailable())
    }
    #[cfg(not(windows))]
    {
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path).map_err(|_| unavailable())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if file
                .metadata()
                .map_err(|_| unavailable())?
                .permissions()
                .mode()
                & 0o777
                != 0o600
            {
                return Err(unavailable());
            }
        }
        Ok(file)
    }
}

/// One budget follows staging, inspection, binding and projection. These are
/// work limits, not a whole-document materialization limit.
pub fn budget() -> Budget {
    Budget::new(limits())
}
/// Explicit ceilings for reopening an existing task; persisted limits cannot be
/// enlarged by either this helper or the checkpoint file.
pub fn limits() -> Limits {
    Limits {
        rows: 10_000_000,
        bytes: 8 * 1024 * 1024 * 1024,
        sql: 1_000_000,
        resident: 32 * 1024 * 1024,
        elapsed: std::time::Duration::from_secs(120),
    }
}
pub(crate) fn temporary() -> Result<File> {
    let parent = std::env::temp_dir()
        .canonicalize()
        .map_err(|_| unavailable())?;
    if !parent.is_dir() {
        return Err(unavailable());
    }
    let path = parent.join(format!("ipfs-s3-management-{}.spool", uuid::Uuid::new_v4()));
    let path = path.to_str().ok_or_else(unavailable)?;
    #[cfg(windows)]
    {
        super::windows_private::temporary(path).map_err(|_| unavailable())
    }
    #[cfg(not(windows))]
    {
        let file = reserve(path)?;
        // Only the exclusively reserved, task-generated name is unlinked; all
        // subsequent reads/writes remain on its protected original handle.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let owned = file.metadata().map_err(|_| unavailable())?;
            let named = std::fs::symlink_metadata(path).map_err(|_| unavailable())?;
            if (owned.dev(), owned.ino()) != (named.dev(), named.ino()) {
                return Err(unavailable());
            }
            std::fs::remove_file(path).map_err(|_| unavailable())?;
        }
        Ok(file)
    }
}

/// Local task spool only. Both the file and its private parent are verified on
/// retained handles; Windows sharing excludes deletion/replacement while open.
pub(crate) struct ReopenedTask {
    _file: File,
    _parent: File,
}
pub(crate) fn reopen_task(path: &std::path::Path) -> Result<ReopenedTask> {
    let parent = path.parent().ok_or_else(unavailable)?;
    #[cfg(windows)]
    {
        let parent = super::windows_private::reopen(parent.to_str().ok_or_else(unavailable)?, true)
            .map_err(|_| unavailable())?;
        let file = super::windows_private::reopen(path.to_str().ok_or_else(unavailable)?, false)
            .map_err(|_| unavailable())?;
        Ok(ReopenedTask {
            _file: file,
            _parent: parent,
        })
    }
    #[cfg(not(windows))]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let owner = temporary()?.metadata().map_err(|_| unavailable())?.uid();
        let parent_named = std::fs::symlink_metadata(parent).map_err(|_| unavailable())?;
        let named = std::fs::symlink_metadata(path).map_err(|_| unavailable())?;
        let parent = File::open(parent).map_err(|_| unavailable())?;
        let file = File::open(path).map_err(|_| unavailable())?;
        let directory = parent.metadata().map_err(|_| unavailable())?;
        let actual = file.metadata().map_err(|_| unavailable())?;
        if !directory.is_dir()
            || !actual.is_file()
            || actual.nlink() != 1
            || directory.uid() != owner
            || actual.uid() != owner
            || directory.permissions().mode() & 0o777 != 0o700
            || actual.permissions().mode() & 0o777 != 0o600
            || (directory.dev(), directory.ino()) != (parent_named.dev(), parent_named.ino())
            || (actual.dev(), actual.ino()) != (named.dev(), named.ino())
            || parent_named.file_type().is_symlink()
            || named.file_type().is_symlink()
        {
            return Err(unavailable());
        }
        Ok(ReopenedTask {
            _file: file,
            _parent: parent,
        })
    }
}

/// Reserve before publishing a DB confirmation. Both modes use exclusive-create
/// protected files; PrivateRecovery is a separate explicit caller choice.
pub fn writer(path: &str, projection: Projection, budget: Budget) -> Result<Writer<File>> {
    Writer::new(reserve(path)?, projection, budget).map_err(|_| unavailable())
}
pub fn finish(writer: Writer<File>) -> Result<Budget> {
    let (result, budget) = finish_metered(writer);
    result.map(|()| budget)
}
/// All finalization outcomes retain work. Failed/partial files are not removed.
pub fn finish_metered(writer: Writer<File>) -> (Result<()>, Budget) {
    let (file, budget) = writer.finalize();
    let result = file
        .map_err(|_| unavailable())
        .and_then(|file| file.sync_all().map_err(|_| unavailable()))
        .and_then(|()| budget.check().map_err(|_| unavailable()));
    (result, budget)
}
fn durable(file: File, budget: Budget) -> Result<Budget> {
    file.sync_all().map_err(|_| unavailable())?;
    budget.check().map_err(|_| unavailable())?;
    Ok(budget)
}

/// Export only after complete source verification, to a new protected handle.
pub fn export_archive(
    cursor: ReviewCursor<File>,
    output_path: &str,
    projection: Projection,
) -> Result<Budget> {
    let (file, budget) = cursor
        .export(reserve(output_path)?, projection)
        .map_err(|_| unavailable())?;
    durable(file, budget)
}

struct CopyInput<'a> {
    source: &'a mut File,
    spool: &'a mut File,
}
impl Read for CopyInput<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let n = self.source.read(bytes)?;
        self.spool.write_all(&bytes[..n])?;
        Ok(n)
    }
}

/// Only this successfully validated, same-handle spool can be handed to the
/// local-original binder. It contains no authority token or trusted applied data.
pub struct StagedInput {
    file: File,
    projection: Projection,
    budget: Budget,
}
impl StagedInput {
    pub fn budget(&self) -> &Budget {
        &self.budget
    }
    pub fn inspect(
        self,
    ) -> Result<(
        crate::pinning::management::wire::v3::document::DocumentHead,
        Self,
    )> {
        let (head, file, budget) = crate::pinning::management::wire::v3::document::inspect(
            self.file,
            self.projection,
            self.budget,
        )
        .map_err(|_| unavailable())?;
        Ok((
            head,
            Self {
                file,
                projection: self.projection,
                budget,
            },
        ))
    }
    pub fn imported(self) -> Result<StagedImport<File>> {
        archive_cursor::stage_import(self.file, self.projection, self.budget)
            .map_err(|_| unavailable())
    }
    pub fn cursor(self) -> Result<ReviewCursor<File>> {
        Ok(self.imported()?.into_cursor())
    }
}

/// Validate the complete file into a protected same-handle task spool. No parent
/// is created, no archive is collected in memory and no database is opened.
pub fn stage_temporary(
    source_path: &str,
    projection: Projection,
    budget: Budget,
) -> Result<StagedInput> {
    stage_file(source_path, temporary()?, projection, budget)
}
fn stage_file(
    source_path: &str,
    mut spool: File,
    projection: Projection,
    budget: Budget,
) -> Result<StagedInput> {
    let mut source = File::open(source_path).map_err(|_| unavailable())?;
    if !source.metadata().map_err(|_| unavailable())?.is_file() {
        return Err(unavailable());
    }
    let mut reader = Reader::new(
        CopyInput {
            source: &mut source,
            spool: &mut spool,
        },
        projection,
        budget,
    )
    .map_err(|_| unavailable())?;
    while reader.next_frame().map_err(|_| unavailable())?.is_some() {}
    let (_, mut budget) = reader.into_verified().map_err(|_| unavailable())?;
    let copied = spool.stream_position().map_err(|_| unavailable())?;
    budget.work.copied_bytes = budget
        .work
        .copied_bytes
        .checked_add(copied)
        .ok_or_else(unavailable)?;
    budget.work.output_bytes = budget
        .work
        .output_bytes
        .checked_add(copied)
        .ok_or_else(unavailable)?;
    spool.sync_all().map_err(|_| unavailable())?;
    spool.seek(SeekFrom::Start(0)).map_err(|_| unavailable())?;
    budget.check().map_err(|_| unavailable())?;
    Ok(StagedInput {
        file: spool,
        projection,
        budget,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pinning::management::wire::v3::{AppliedTrust, Frame, SCHEMA_VERSION};
    fn emit(writer: &mut Writer<File>, projection: Projection) {
        writer
            .frame(&Frame::Header {
                schema_version: SCHEMA_VERSION,
                projection,
                database_scope: "scope".into(),
                database_identity: "original identity".into(),
                principal: "original principal".into(),
                applied_trust: AppliedTrust::UntrustedClaimsOnly,
                catalog: vec!["original catalog".into()],
                created_at: "raw timestamp".into(),
                expires_at: None,
            })
            .unwrap();
        writer.frame(&Frame::Request { request: None }).unwrap();
        writer
            .frame(&Frame::Configuration {
                configuration: None,
            })
            .unwrap();
        writer
            .frame(&Frame::Acknowledgements { item_ids: None })
            .unwrap();
        writer
            .frame(&Frame::Summary {
                summary: Default::default(),
            })
            .unwrap();
    }
    #[test]
    fn private_export_and_staging_keep_same_protected_handle_and_reject_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("private.bin");
        let path = path.to_str().unwrap();
        let file = reserve(path).unwrap();
        #[cfg(windows)]
        super::super::windows_private::verify(&file).unwrap();
        let mut output = Writer::new(file, Projection::PrivateRecovery, Budget::default()).unwrap();
        emit(&mut output, Projection::PrivateRecovery);
        finish(output).unwrap();
        assert!(writer(path, Projection::PrivateRecovery, Budget::default()).is_err());
        let mut staged =
            stage_temporary(path, Projection::PrivateRecovery, Budget::default()).unwrap();
        #[cfg(windows)]
        super::super::windows_private::verify(&staged.file).unwrap();
        let mut staged_bytes = Vec::new();
        staged.file.read_to_end(&mut staged_bytes).unwrap();
        staged.file.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(std::fs::read(path).unwrap(), staged_bytes);
        let mut cursor = staged.cursor().unwrap();
        while cursor.next_event().unwrap().is_some() {}
        assert!(cursor.verified());
        assert!(writer(path, Projection::PrivateRecovery, Budget::default()).is_err());
    }
    #[test]
    fn missing_parent_and_invalid_or_private_input_never_produce_validated_staging() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        assert!(
            writer(
                missing.join("file").to_str().unwrap(),
                Projection::Review,
                Budget::default()
            )
            .is_err()
        );
        assert!(!missing.exists());
        let path = dir.path().join("private.bin");
        let mut output = writer(
            path.to_str().unwrap(),
            Projection::PrivateRecovery,
            Budget::default(),
        )
        .unwrap();
        emit(&mut output, Projection::PrivateRecovery);
        finish(output).unwrap();
        assert!(
            stage_temporary(
                path.to_str().unwrap(),
                Projection::Review,
                Budget::default()
            )
            .is_err()
        );
    }
}

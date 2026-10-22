//! Task-owned private serialization scratch, removed only by its owning handle.
use crate::store::pinning::management::{ManagementError, Result};
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
};

pub(super) struct Spool {
    pub file: File,
    path: PathBuf,
}
impl Spool {
    pub fn new() -> Result<Self> {
        let path =
            std::env::temp_dir().join(format!("ipfs-s3-history-{}.tmp", uuid::Uuid::new_v4()));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            // No sharing; delete on close also covers an interrupted process.
            options.share_mode(0).custom_flags(0x04000000);
        }
        let file = options.open(&path).map_err(|_| ManagementError::Database)?;
        Ok(Self { file, path })
    }
}
impl Drop for Spool {
    fn drop(&mut self) {
        #[cfg(not(windows))]
        let _ = std::fs::remove_file(&self.path);
        #[cfg(windows)]
        let _ = &self.path;
    }
}

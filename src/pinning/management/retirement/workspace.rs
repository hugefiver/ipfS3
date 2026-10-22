//! Task-owned protected parent for SQLite inventory pages and journals.
use super::*;
use std::{
    fs::File,
    path::{Path, PathBuf},
};
mod protected;

pub struct Workspace {
    path: PathBuf,
    guard: Option<File>,
    files: Vec<PathBuf>,
    handles: Vec<File>,
    lock: Option<File>,
}
impl Workspace {
    pub fn create() -> Result<Self> {
        let parent = std::env::temp_dir()
            .canonicalize()
            .map_err(|_| ManagementError::Database)?;
        if !parent.is_dir() {
            return Err(ManagementError::Database);
        }
        let path = parent.join(format!("ipfs-s3-inventory-{}", uuid::Uuid::new_v4()));
        Self::create_at(&path)
    }
    /// Creates exactly this new directory. Its parent must already exist.
    pub fn create_at(path: &Path) -> Result<Self> {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .canonicalize()
            .map_err(|_| ManagementError::Database)?;
        let name = path.file_name().ok_or(ManagementError::InvalidEvidence)?;
        let path = parent.join(name);
        let guard = super::super::wire::v3::files::private_directory(&path)
            .map_err(|_| ManagementError::Database)?;
        Ok(Self {
            path,
            guard,
            files: Vec::new(),
            handles: Vec::new(),
            lock: None,
        })
    }
    pub(super) fn open(path: &Path) -> Result<Self> {
        let metadata = std::fs::symlink_metadata(path).map_err(|_| ManagementError::Database)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ManagementError::InvalidEvidence);
        }
        let path = path.canonicalize().map_err(|_| ManagementError::Database)?;
        let guard = protected::open(&path, true)?;
        Ok(Self {
            path,
            guard: Some(guard),
            files: Vec::new(),
            handles: Vec::new(),
            lock: None,
        })
    }
    pub(super) async fn reserve_named(&mut self, name: &str) -> Result<(PathBuf, File)> {
        let path = self.named(name)?;
        let file =
            super::super::wire::v3::files::reserve(path.to_str().ok_or(ManagementError::Database)?)
                .await
                .map_err(|_| ManagementError::Database)?
                .into_std()
                .await;
        self.files.push(path.clone());
        Ok((path, file))
    }
    pub(super) fn open_named(&mut self, name: &str) -> Result<File> {
        let path = self.named(name)?;
        let file = protected::open(&path, false)?;
        self.handles
            .push(file.try_clone().map_err(|_| ManagementError::Database)?);
        if !self.files.contains(&path) {
            self.files.push(path);
        }
        Ok(file)
    }
    pub(super) async fn lock(&mut self, create: bool) -> Result<()> {
        let file = if create {
            self.reserve_named("planning.lock").await?.1
        } else {
            self.open_named("planning.lock")?
        };
        file.try_lock().map_err(|_| ManagementError::Changed)?;
        self.lock = Some(file);
        Ok(())
    }
    pub(super) fn named(&self, name: &str) -> Result<PathBuf> {
        if name.is_empty() || name.contains(['/', '\\', ':']) || matches!(name, "." | "..") {
            return Err(ManagementError::InvalidEvidence);
        }
        Ok(self.path.join(name))
    }
    pub async fn reserve(&mut self) -> Result<(PathBuf, File)> {
        let path = self.path.join(format!("{}.sqlite", uuid::Uuid::new_v4()));
        let file =
            super::super::wire::v3::files::reserve(path.to_str().ok_or(ManagementError::Database)?)
                .await
                .map_err(|_| ManagementError::Database)?
                .into_std()
                .await;
        self.files.push(path.clone());
        Ok((path, file))
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// Call after closing every task spool. Never remove unknown files or recurse.
    pub fn cleanup(mut self) -> Result<()> {
        let metadata =
            std::fs::symlink_metadata(&self.path).map_err(|_| ManagementError::Database)?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || self
                .path
                .canonicalize()
                .map_err(|_| ManagementError::Database)?
                != self.path
        {
            return Err(ManagementError::Database);
        }
        let mut owned = std::collections::BTreeSet::new();
        for file in &self.files {
            for suffix in ["", "-journal", "-wal", "-shm"] {
                owned.insert(PathBuf::from(format!(
                    "{}{suffix}",
                    file.to_str().ok_or(ManagementError::Database)?
                )));
            }
        }
        for entry in std::fs::read_dir(&self.path).map_err(|_| ManagementError::Database)? {
            let entry = entry.map_err(|_| ManagementError::Database)?;
            if !owned.contains(&entry.path()) {
                return Err(ManagementError::InvalidEvidence);
            }
        }
        self.handles.clear();
        self.lock.take();
        for file in &self.files {
            for suffix in ["", "-journal", "-wal", "-shm"] {
                let target = PathBuf::from(format!(
                    "{}{suffix}",
                    file.to_str().ok_or(ManagementError::Database)?
                ));
                match std::fs::symlink_metadata(&target) {
                    Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                        std::fs::remove_file(&target).map_err(|_| ManagementError::Database)?
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    _ => return Err(ManagementError::Database),
                }
            }
        }
        self.guard.take();
        std::fs::remove_dir(&self.path).map_err(|_| ManagementError::Database)
    }
}

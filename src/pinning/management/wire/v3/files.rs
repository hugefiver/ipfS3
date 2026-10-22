use anyhow::{Result, anyhow};

#[path = "files/stream.rs"]
pub mod stream;

fn unavailable() -> anyhow::Error {
    anyhow!("management artifact unavailable or incomplete (details withheld)")
}

pub async fn reserve(path: &str) -> Result<tokio::fs::File> {
    let path = path.to_owned();
    let file = tokio::task::spawn_blocking(move || stream::reserve(&path))
        .await
        .map_err(|_| unavailable())??;
    Ok(tokio::fs::File::from_std(file))
}

pub(crate) fn private_directory(path: &std::path::Path) -> Result<Option<std::fs::File>> {
    #[cfg(windows)]
    {
        windows_private::directory(path.to_str().ok_or_else(unavailable)?)
            .map(Some)
            .map_err(|_| unavailable())
    }
    #[cfg(not(windows))]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(path)
            .map_err(|_| unavailable())?;
        Ok(None)
    }
}

#[cfg(windows)]
mod windows_private {
    use std::{
        ffi::c_void,
        fs::File,
        io,
        mem::size_of,
        os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
        ptr::{addr_of_mut, null_mut},
    };

    const FILE_ALL_ACCESS: u32 = 0x001f_01ff;
    const SE_DACL_PRESENT: u16 = 0x0004;
    const SE_DACL_PROTECTED: u16 = 0x1000;

    #[repr(C)]
    struct SecurityAttributes {
        length: u32,
        descriptor: *mut c_void,
        inherit_handle: i32,
    }
    #[repr(C)]
    struct Acl {
        revision: u8,
        reserved: u8,
        size: u16,
        ace_count: u16,
        reserved2: u16,
    }
    #[repr(C)]
    struct AllowedAce {
        kind: u8,
        flags: u8,
        size: u16,
        mask: u32,
        sid_start: u32,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn LocalFree(memory: *mut c_void) -> *mut c_void;
        fn CreateFileW(
            path: *const u16,
            access: u32,
            sharing: u32,
            attributes: *const SecurityAttributes,
            disposition: u32,
            flags: u32,
            template: *mut c_void,
        ) -> *mut c_void;
        fn CreateDirectoryW(path: *const u16, attributes: *const SecurityAttributes) -> i32;
    }
    #[link(name = "advapi32")]
    unsafe extern "system" {
        fn OpenProcessToken(process: *mut c_void, access: u32, token: *mut *mut c_void) -> i32;
        fn GetTokenInformation(
            token: *mut c_void,
            class: i32,
            buffer: *mut c_void,
            length: u32,
            needed: *mut u32,
        ) -> i32;
        fn ConvertSidToStringSidW(sid: *mut c_void, text: *mut *mut c_void) -> i32;
        fn ConvertStringSecurityDescriptorToSecurityDescriptorW(
            text: *const u16,
            revision: u32,
            descriptor: *mut *mut c_void,
            length: *mut u32,
        ) -> i32;
        fn GetSecurityInfo(
            handle: *mut c_void,
            object_type: i32,
            information: u32,
            owner: *mut *mut c_void,
            group: *mut *mut c_void,
            dacl: *mut *mut Acl,
            sacl: *mut *mut Acl,
            descriptor: *mut *mut c_void,
        ) -> u32;
        fn IsValidSecurityDescriptor(descriptor: *mut c_void) -> i32;
        fn GetSecurityDescriptorControl(
            descriptor: *mut c_void,
            control: *mut u16,
            revision: *mut u32,
        ) -> i32;
        fn IsValidAcl(acl: *const Acl) -> i32;
        fn GetAce(acl: *const Acl, index: u32, ace: *mut *mut c_void) -> i32;
        fn EqualSid(first: *mut c_void, second: *mut c_void) -> i32;
        fn IsWellKnownSid(sid: *mut c_void, kind: i32) -> i32;
    }

    struct LocalMemory(*mut c_void);
    impl Drop for LocalMemory {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    LocalFree(self.0);
                }
            }
        }
    }

    fn denied() -> io::Error {
        io::Error::from(io::ErrorKind::PermissionDenied)
    }

    fn wide(text: &str) -> io::Result<Vec<u16>> {
        if text.contains('\0') {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        Ok(text.encode_utf16().chain(Some(0)).collect())
    }

    fn current_user() -> io::Result<Vec<usize>> {
        let mut token = null_mut();
        // TOKEN_QUERY; the process is not impersonating another principal.
        if unsafe { OpenProcessToken(GetCurrentProcess(), 0x0008, &mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let token = unsafe { OwnedHandle::from_raw_handle(token) };
        let mut needed = 0;
        unsafe {
            GetTokenInformation(token.as_raw_handle(), 1, null_mut(), 0, &mut needed);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(122) {
            return Err(error);
        }
        // TOKEN_USER starts with a SID pointer and needs pointer-aligned storage.
        let mut user = vec![0usize; (needed as usize).div_ceil(size_of::<usize>())];
        if unsafe {
            GetTokenInformation(
                token.as_raw_handle(),
                1,
                user.as_mut_ptr().cast(),
                needed,
                &mut needed,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(user)
    }

    fn user_sid(user: &[usize]) -> *mut c_void {
        user[0] as *mut c_void
    }

    fn descriptor(sid: *mut c_void) -> io::Result<LocalMemory> {
        security_descriptor(sid, false)
    }
    fn security_descriptor(sid: *mut c_void, directory: bool) -> io::Result<LocalMemory> {
        let mut text = LocalMemory(null_mut());
        if unsafe { ConvertSidToStringSidW(sid, &mut text.0) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let pointer = text.0.cast::<u16>();
        let mut length = 0;
        // ConvertSidToStringSidW returns an allocated, NUL-terminated SID string.
        while unsafe { *pointer.add(length) } != 0 {
            length += 1;
        }
        let sid = String::from_utf16(unsafe { std::slice::from_raw_parts(pointer, length) })
            .map_err(|_| denied())?;
        let flags = if directory { "OICI" } else { "" };
        let sddl = wide(&format!(
            "O:{sid}D:P(A;{flags};FA;;;SY)(A;{flags};FA;;;{sid})"
        ))?;
        let mut descriptor = LocalMemory(null_mut());
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                1,
                &mut descriptor.0,
                null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(descriptor)
    }

    fn verify_owner(file: &File, expected_owner: *mut c_void) -> io::Result<()> {
        verify_security(file, expected_owner, false)
    }
    fn verify_security(
        file: &File,
        expected_owner: *mut c_void,
        directory: bool,
    ) -> io::Result<()> {
        let mut descriptor = LocalMemory(null_mut());
        let mut owner = null_mut();
        let mut dacl = null_mut();
        // SE_FILE_OBJECT; OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION.
        let status = unsafe {
            GetSecurityInfo(
                file.as_raw_handle(),
                1,
                5,
                &mut owner,
                null_mut(),
                &mut dacl,
                null_mut(),
                &mut descriptor.0,
            )
        };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        let mut control = 0;
        let mut revision = 0;
        if unsafe { IsValidSecurityDescriptor(descriptor.0) } == 0
            || unsafe { GetSecurityDescriptorControl(descriptor.0, &mut control, &mut revision) }
                == 0
            || control & (SE_DACL_PRESENT | SE_DACL_PROTECTED)
                != (SE_DACL_PRESENT | SE_DACL_PROTECTED)
            || owner.is_null()
            || dacl.is_null()
            || unsafe { EqualSid(owner, expected_owner) } == 0
            || unsafe { IsValidAcl(dacl) } == 0
            || unsafe { (*dacl).ace_count } != 2
        {
            return Err(denied());
        }
        let (mut owner_allowed, mut system_allowed) = (false, false);
        for index in 0..2 {
            let mut ace = null_mut();
            if unsafe { GetAce(dacl, index, &mut ace) } == 0 {
                return Err(io::Error::last_os_error());
            }
            let ace = ace.cast::<AllowedAce>();
            // Exactly two non-inherited access-allowed ACEs, with no other grants.
            if unsafe {
                (*ace).kind != 0
                    || (*ace).flags != if directory { 3 } else { 0 }
                    || (*ace).mask != FILE_ALL_ACCESS
            } {
                return Err(denied());
            }
            let sid = unsafe { addr_of_mut!((*ace).sid_start).cast() };
            let is_owner = unsafe { EqualSid(sid, expected_owner) } != 0;
            let is_system = unsafe { IsWellKnownSid(sid, 22) } != 0; // WinLocalSystemSid
            if !is_owner && !is_system {
                return Err(denied());
            }
            owner_allowed |= is_owner;
            system_allowed |= is_system;
        }
        if !owner_allowed || !system_allowed {
            return Err(denied());
        }
        Ok(())
    }

    pub(super) fn create(path: &str) -> io::Result<File> {
        create_with_flags(path, false)
    }
    pub(super) fn directory(path: &str) -> io::Result<File> {
        let path = wide(path)?;
        let user = current_user()?;
        let descriptor = security_descriptor(user_sid(&user), true)?;
        let attributes = SecurityAttributes {
            length: size_of::<SecurityAttributes>() as u32,
            descriptor: descriptor.0,
            inherit_handle: 0,
        };
        if unsafe { CreateDirectoryW(path.as_ptr(), &attributes) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                0x0002_0000,
                3,
                null_mut(),
                3,
                0x0200_0000,
                null_mut(),
            )
        };
        if handle as isize == -1 {
            return Err(io::Error::last_os_error());
        }
        let file = unsafe { File::from_raw_handle(handle) };
        verify_security(&file, user_sid(&user), true)?;
        Ok(file)
    }
    pub(super) fn temporary(path: &str) -> io::Result<File> {
        create_with_flags(path, true)
    }
    pub(super) fn reopen(path: &str, directory: bool) -> io::Result<File> {
        let path = wide(path)?;
        let user = current_user()?;
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                if directory { 0x0002_0080 } else { 0x8002_0000 },
                3,
                null_mut(),
                3,
                0x0020_0000 | if directory { 0x0200_0000 } else { 0 },
                null_mut(),
            )
        };
        if handle as isize == -1 {
            return Err(io::Error::last_os_error());
        }
        let file = unsafe { File::from_raw_handle(handle) };
        let metadata = file.metadata()?;
        if metadata.file_type().is_symlink()
            || if directory {
                !metadata.is_dir()
            } else {
                !metadata.is_file()
            }
        {
            return Err(denied());
        }
        verify_security(&file, user_sid(&user), directory)?;
        Ok(file)
    }
    fn create_with_flags(path: &str, temporary: bool) -> io::Result<File> {
        let path = wide(path)?;
        let user = current_user()?;
        let descriptor = descriptor(user_sid(&user))?;
        let attributes = SecurityAttributes {
            length: size_of::<SecurityAttributes>() as u32,
            descriptor: descriptor.0,
            inherit_handle: 0,
        };
        // CREATE_NEW with a protected DACL at creation, never a post-write ACL fix.
        // READ_CONTROL permits verification on the same non-shared file handle.
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                0x8000_0000 | 0x4000_0000 | 0x0002_0000 | if temporary { 0x0001_0000 } else { 0 },
                0,
                &attributes,
                1,
                0x80 | if temporary { 0x0400_0000 } else { 0 },
                null_mut(),
            )
        };
        if handle == -1isize as *mut c_void {
            return Err(io::Error::last_os_error());
        }
        let file = unsafe { File::from_raw_handle(handle) };
        // Filesystems that cannot retain the private ACL fail closed before bytes.
        // Leave only an empty artifact; never delete a caller path on failure.
        verify_owner(&file, user_sid(&user))?;
        Ok(file)
    }

    #[cfg(test)]
    pub(super) fn verify(file: &File) -> io::Result<()> {
        let user = current_user()?;
        verify_owner(file, user_sid(&user))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn artifact_reservation_never_overwrites_or_creates_parent_directories() {
        let directory = tempfile::tempdir().unwrap();
        let existing = directory.path().join("PRIVATE_EXISTING_ARTIFACT.json");
        std::fs::write(&existing, b"PRIVATE_EXISTING_CONTENT").unwrap();
        let error = reserve(existing.to_str().unwrap()).await.err().unwrap();
        assert_eq!(
            std::fs::read(&existing).unwrap(),
            b"PRIVATE_EXISTING_CONTENT"
        );
        let text = format!("{error:?}");
        assert!(!text.contains("PRIVATE_EXISTING_ARTIFACT"));
        assert!(!text.contains("PRIVATE_EXISTING_CONTENT"));
        assert!(!text.contains(directory.path().to_str().unwrap()));

        let missing_parent = directory.path().join("PRIVATE_MISSING_PARENT");
        let error = reserve(missing_parent.join("artifact.json").to_str().unwrap())
            .await
            .err()
            .unwrap();
        assert!(!missing_parent.exists());
        assert!(!format!("{error:?}").contains("PRIVATE_MISSING_PARENT"));
    }

    #[tokio::test]
    async fn reserved_artifact_contains_only_explicitly_written_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("artifact.json");
        let mut file = reserve(path.to_str().unwrap()).await.unwrap();
        file.write_all(b"public-plan").await.unwrap();
        file.sync_all().await.unwrap();
        drop(file);
        assert_eq!(std::fs::read(&path).unwrap(), b"public-plan");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_artifact_is_owner_and_system_only_before_and_after_writing() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("private.json");
        let file = reserve(path.to_str().unwrap())
            .await
            .unwrap()
            .into_std()
            .await;
        assert_eq!(file.metadata().unwrap().len(), 0);
        windows_private::verify(&file)
            .expect("new artifact must have a protected owner/SYSTEM-only DACL");
        let mut file = tokio::fs::File::from_std(file);
        file.write_all(b"PRIVATE_CONFIRMATION_CONTENT")
            .await
            .unwrap();
        file.sync_all().await.unwrap();
        drop(file);
        let file = std::fs::File::open(&path).unwrap();
        windows_private::verify(&file).expect("persisted artifact must retain its private DACL");
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"PRIVATE_CONFIRMATION_CONTENT"
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_inherited_file_security_cannot_authorize_sensitive_writes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unverified-empty.json");
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap();
        assert!(windows_private::verify(&file).is_err());
        assert_eq!(file.metadata().unwrap().len(), 0);
    }
}

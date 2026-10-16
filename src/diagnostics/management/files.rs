use std::io;

use anyhow::{Result, anyhow};
use ipfs_s3_gateway::pinning::management::wire::MAX_WIRE_BYTES;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

fn unavailable() -> anyhow::Error {
    anyhow!("management artifact unavailable or incomplete (details withheld)")
}

pub async fn reserve(path: &str) -> Result<tokio::fs::File> {
    #[cfg(windows)]
    {
        let path = path.to_owned();
        let file = tokio::task::spawn_blocking(move || windows_private::create(&path))
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?;
        Ok(tokio::fs::File::from_std(file))
    }
    #[cfg(not(windows))]
    {
        let mut options = tokio::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        options.mode(0o600);
        // No mkdir, overwrite or error-path deletion of a possibly replaced file.
        options.open(path).await.map_err(|_| unavailable())
    }
}

pub async fn write(file: &mut tokio::fs::File, bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_WIRE_BYTES {
        return Err(unavailable());
    }
    file.write_all(bytes).await.map_err(|_| unavailable())?;
    file.sync_all().await.map_err(|_| unavailable())
}

struct ReportBuffer(Vec<u8>);

impl io::Write for ReportBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > (1024 * 1024usize).saturating_sub(self.0.len()) {
            return Err(io::Error::other("management report limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub async fn print(report: &impl Serialize) -> Result<()> {
    let mut buffer = ReportBuffer(Vec::new());
    serde_json::to_writer_pretty(&mut buffer, report)
        .map_err(|_| anyhow!("management report budget exceeded"))?;
    buffer.0.push(b'\n');
    let mut stdout = tokio::io::stdout();
    stdout
        .write_all(&buffer.0)
        .await
        .map_err(|_| anyhow!("management report output unavailable"))?;
    stdout
        .flush()
        .await
        .map_err(|_| anyhow!("management report output unavailable"))
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
        let sddl = wide(&format!("O:{sid}D:P(A;;FA;;;SY)(A;;FA;;;{sid})"))?;
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
            if unsafe { (*ace).kind != 0 || (*ace).flags != 0 || (*ace).mask != FILE_ALL_ACCESS } {
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
                0x4000_0000 | 0x0002_0000,
                0,
                &attributes,
                1,
                0x80,
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
        write(&mut file, b"public-plan").await.unwrap();
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
        write(&mut file, b"PRIVATE_CONFIRMATION_CONTENT")
            .await
            .unwrap();
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

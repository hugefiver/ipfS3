//! Reopen only an owner-private, non-reparse local checkpoint resource.
use super::*;

pub(super) fn open(path: &Path, directory: bool) -> Result<File> {
    let before = std::fs::symlink_metadata(path).map_err(|_| ManagementError::Database)?;
    if before.file_type().is_symlink()
        || before.is_dir() != directory
        || !directory && !before.is_file()
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let mut options = File::options();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options
            .share_mode(3)
            .custom_flags(0x0020_0000 | if directory { 0x0200_0000 } else { 0 });
    }
    let file = options.open(path).map_err(|_| ManagementError::Database)?;
    verify(&file, directory).map_err(|_| ManagementError::InvalidEvidence)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let after = std::fs::symlink_metadata(path).map_err(|_| ManagementError::Database)?;
        let held = file.metadata().map_err(|_| ManagementError::Database)?;
        if after.file_type().is_symlink()
            || before.dev() != held.dev()
            || before.ino() != held.ino()
            || after.dev() != held.dev()
            || after.ino() != held.ino()
        {
            return Err(ManagementError::Changed);
        }
    }
    Ok(file)
}

#[cfg(unix)]
fn verify(file: &File, directory: bool) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    unsafe extern "C" {
        fn geteuid() -> u32;
    }
    let metadata = file.metadata()?;
    if metadata.uid() != unsafe { geteuid() }
        || metadata.mode() & 0o777 != if directory { 0o700 } else { 0o600 }
    {
        return Err(std::io::ErrorKind::PermissionDenied.into());
    }
    Ok(())
}

#[cfg(windows)]
fn verify(file: &File, directory: bool) -> std::io::Result<()> {
    windows::verify(file, directory)
}

#[cfg(windows)]
mod windows {
    use std::{
        ffi::c_void,
        fs::File,
        io,
        mem::size_of,
        os::windows::{
            fs::MetadataExt,
            io::{AsRawHandle, FromRawHandle, OwnedHandle},
        },
        ptr::{addr_of_mut, null_mut},
    };
    #[repr(C)]
    struct Acl {
        revision: u8,
        reserved: u8,
        size: u16,
        count: u16,
        reserved2: u16,
    }
    #[repr(C)]
    struct Ace {
        kind: u8,
        flags: u8,
        size: u16,
        mask: u32,
        sid: u32,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn LocalFree(ptr: *mut c_void) -> *mut c_void;
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
        fn GetSecurityInfo(
            handle: *mut c_void,
            kind: i32,
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
    struct Descriptor(*mut c_void);
    impl Drop for Descriptor {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    LocalFree(self.0);
                }
            }
        }
    }
    fn denied() -> io::Error {
        io::ErrorKind::PermissionDenied.into()
    }
    fn user() -> io::Result<Vec<usize>> {
        let mut token = null_mut();
        if unsafe { OpenProcessToken(GetCurrentProcess(), 8, &mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let token = unsafe { OwnedHandle::from_raw_handle(token) };
        let mut needed = 0;
        unsafe {
            GetTokenInformation(token.as_raw_handle(), 1, null_mut(), 0, &mut needed);
        }
        if io::Error::last_os_error().raw_os_error() != Some(122) {
            return Err(io::Error::last_os_error());
        }
        let mut buffer = vec![0usize; (needed as usize).div_ceil(size_of::<usize>())];
        if unsafe {
            GetTokenInformation(
                token.as_raw_handle(),
                1,
                buffer.as_mut_ptr().cast(),
                needed,
                &mut needed,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(buffer)
    }
    pub(super) fn verify(file: &File, directory: bool) -> io::Result<()> {
        let metadata = file.metadata()?;
        if metadata.file_attributes() & 0x400 != 0 || metadata.is_dir() != directory {
            return Err(denied());
        }
        let user = user()?;
        let expected = user[0] as *mut c_void;
        let mut descriptor = Descriptor(null_mut());
        let mut owner = null_mut();
        let mut dacl = null_mut();
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
            || control & 0x1004 != 0x1004
            || owner.is_null()
            || dacl.is_null()
            || unsafe { EqualSid(owner, expected) } == 0
            || unsafe { IsValidAcl(dacl) } == 0
            || unsafe { (*dacl).count } != 2
        {
            return Err(denied());
        }
        let (mut owns, mut system) = (false, false);
        for index in 0..2 {
            let mut ace = null_mut();
            if unsafe { GetAce(dacl, index, &mut ace) } == 0 {
                return Err(io::Error::last_os_error());
            }
            let ace = ace.cast::<Ace>();
            if unsafe {
                (*ace).kind != 0
                    || (*ace).flags != if directory { 3 } else { 0 }
                    || (*ace).mask != 0x001f_01ff
            } {
                return Err(denied());
            }
            let sid = unsafe { addr_of_mut!((*ace).sid).cast() };
            let is_owner = unsafe { EqualSid(sid, expected) } != 0;
            let is_system = unsafe { IsWellKnownSid(sid, 22) } != 0;
            if !is_owner && !is_system {
                return Err(denied());
            }
            owns |= is_owner;
            system |= is_system;
        }
        if !owns || !system {
            return Err(denied());
        }
        Ok(())
    }
}

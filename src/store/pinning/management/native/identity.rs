//! Physical file identity, not path/mtime/size/page counters or a copied UUID.
use super::*;

#[cfg(windows)]
pub(super) fn file_identity(path:&str)->Result<String> {
    use std::{ffi::c_void, os::windows::io::AsRawHandle};
    #[repr(C)]
    struct FileIdInfo {volume:u64,id:[u8;16]}
    #[link(name="kernel32")]
    unsafe extern "system" {
        #[link_name="GetFileInformationByHandleEx"]
        fn get_file_information_by_handle_ex(handle:*mut c_void,class:u32,information:*mut c_void,size:u32)->i32;
    }
    let file=std::fs::File::open(path).map_err(|_|ManagementError::Unauthorized)?;
    let mut info=FileIdInfo {volume:0,id:[0;16]};
    // FILE_ID_INFO is a fixed Windows ABI buffer; the live File owns the handle
    // throughout the call. FileIdInfo=18 only reads filesystem identity.
    let success=unsafe {get_file_information_by_handle_ex(file.as_raw_handle(),18,(&mut info as *mut FileIdInfo).cast(),std::mem::size_of::<FileIdInfo>() as u32)};
    if success==0 || info.id.iter().all(|byte|*byte==0) {return Err(ManagementError::Unauthorized);}
    Ok(format!("{}:{}",info.volume,hex::encode(info.id)))
}

#[cfg(unix)]
pub(super) fn file_identity(path:&str)->Result<String> {
    use std::os::unix::fs::MetadataExt;
    let file=std::fs::File::open(path).map_err(|_|ManagementError::Unauthorized)?;
    let metadata=file.metadata().map_err(|_|ManagementError::Unauthorized)?;
    Ok(format!("{}:{}",metadata.dev(),metadata.ino()))
}

#[cfg(not(any(windows,unix)))]
pub(super) fn file_identity(_path:&str)->Result<String> {Err(ManagementError::Unauthorized)}

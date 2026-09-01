pub(crate) mod checksum;
pub mod config;
pub mod http;
pub mod matcher;
pub mod model;

pub const MAX_CORS_CONFIGURATION_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug)]
pub(crate) struct CorsPutBodyMetadata {
    pub len: usize,
    pub computed_md5: [u8; 16],
    pub computed_crc64nvme: [u8; 8],
    pub sdk_checksum_algorithm: SdkChecksumAlgorithmHeader,
    pub supplied_crc64nvme: Crc64NvmeHeader,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SdkChecksumAlgorithmHeader {
    Absent,
    Single,
    Invalid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Crc64NvmeHeader {
    Absent,
    Invalid,
    Value([u8; 8]),
}

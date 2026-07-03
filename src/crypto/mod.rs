//! AES-256-GCM encryption for per-object/per-chunk encryption.

pub mod aes_gcm;
pub mod chunker;
pub mod key;

#[allow(unused_imports)]
pub use key::{MasterKey, ObjectKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionMode {
    None,
    SseS3,
    SseC,
}

impl EncryptionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::SseS3 => "sse_s3",
            Self::SseC => "sse_c",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "sse_s3" => Self::SseS3,
            "sse_c" => Self::SseC,
            _ => Self::None,
        }
    }
}

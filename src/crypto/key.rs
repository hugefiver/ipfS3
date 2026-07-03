use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit};
use hkdf::Hkdf;
use rand::Rng;
use sha2::Sha256;

use crate::error::{AppError, AppResult};

pub struct MasterKey {
    bytes: [u8; 32],
}

impl MasterKey {
    pub fn from_hex(hex_str: &str) -> AppResult<Self> {
        let bytes = hex::decode(hex_str)
            .map_err(|e| AppError::Crypto(format!("invalid master key hex: {e}")))?;
        if bytes.len() != 32 {
            return Err(AppError::Crypto(format!(
                "master key must be 32 bytes, got {}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self { bytes: arr })
    }

    pub fn generate_object_key(&self) -> ObjectKey {
        let mut bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut bytes);
        ObjectKey { bytes }
    }

    pub fn wrap(&self, ok: &ObjectKey) -> AppResult<String> {
        let cipher = Aes256Gcm::new_from_slice(&self.bytes)
            .map_err(|e| AppError::Crypto(format!("aes init: {e}")))?;

        let mut nonce_bytes = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce_bytes);
        #[allow(deprecated)]
        let nonce = aes_gcm::Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, ok.bytes.as_ref())
            .map_err(|e| AppError::Crypto(format!("wrap: {e}")))?;

        let mut combined = Vec::with_capacity(12 + ciphertext.len());
        combined.extend_from_slice(&nonce_bytes);
        combined.extend_from_slice(&ciphertext);

        Ok(hex::encode(&combined))
    }

    pub fn unwrap(&self, wrapped_hex: &str) -> AppResult<ObjectKey> {
        let combined = hex::decode(wrapped_hex)
            .map_err(|e| AppError::Crypto(format!("invalid key_wrap hex: {e}")))?;
        if combined.len() < 12 + 16 {
            return Err(AppError::Crypto("wrapped key too short".into()));
        }

        let cipher = Aes256Gcm::new_from_slice(&self.bytes)
            .map_err(|e| AppError::Crypto(format!("aes init: {e}")))?;
        #[allow(deprecated)]
        let nonce = aes_gcm::Nonce::from_slice(&combined[..12]);
        let plaintext = cipher
            .decrypt(nonce, &combined[12..])
            .map_err(|_| AppError::AccessDenied("failed to unwrap object key".into()))?;

        if plaintext.len() != 32 {
            return Err(AppError::Crypto(format!(
                "unwrapped key not 32 bytes: {}",
                plaintext.len()
            )));
        }

        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&plaintext);
        Ok(ObjectKey { bytes })
    }
}

pub struct ObjectKey {
    pub bytes: [u8; 32],
}

impl ObjectKey {
    /// Derive a 12-byte GCM nonce via HKDF-SHA256.
    ///
    /// The HKDF `info` parameter is:
    ///   `[len(object_id) as u32 BE] || object_id UTF-8 || part_number BE || chunk_index BE`
    ///
    /// - The length prefix prevents object_id/chunk_index concatenation ambiguity.
    /// - `part_number` is 0 for single-object (non-multipart) uploads.
    /// - Including part_number prevents nonce reuse across parts of the same
    ///   multipart upload (where each part resets chunk_index to 0).
    pub fn derive_nonce(&self, object_id: &str, part_number: u32, chunk_index: u64) -> [u8; 12] {
        let hk = Hkdf::<Sha256>::new(None, &self.bytes);
        let id_bytes = object_id.as_bytes();
        let mut info = Vec::with_capacity(4 + id_bytes.len() + 4 + 8);
        info.extend_from_slice(&(id_bytes.len() as u32).to_be_bytes());
        info.extend_from_slice(id_bytes);
        info.extend_from_slice(&part_number.to_be_bytes());
        info.extend_from_slice(&chunk_index.to_be_bytes());
        let mut nonce = [0u8; 12];
        hk.expand(&info, &mut nonce)
            .expect("hkdf expand to 12 bytes always succeeds");
        nonce
    }
}

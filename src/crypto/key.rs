use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit};
use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::error::{AppError, AppResult};

const SSE_C_KEY_FINGERPRINT_PREFIX: &str = "v1:hmac-sha256:";
const SSE_C_KEY_FINGERPRINT_DOMAIN: &[u8] = b"ipfs-s3-gateway/sse-c-key-fingerprint/v1";

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

    pub fn sse_c_key_fingerprint(&self, key: &ObjectKey) -> String {
        format!(
            "{SSE_C_KEY_FINGERPRINT_PREFIX}{}",
            hex::encode(self.sse_c_key_fingerprint_mac(key))
        )
    }

    pub fn verify_sse_c_key_fingerprint(&self, stored: &str, key: &ObjectKey) -> AppResult<bool> {
        let stored_mac = stored
            .strip_prefix(SSE_C_KEY_FINGERPRINT_PREFIX)
            .ok_or_else(|| AppError::Crypto("invalid SSE-C key fingerprint prefix".into()))?;
        let stored_mac = hex::decode(stored_mac)
            .map_err(|_| AppError::Crypto("invalid SSE-C key fingerprint encoding".into()))?;

        if stored_mac.len() != 32 {
            return Err(AppError::Crypto(
                "SSE-C key fingerprint must be a 32-byte HMAC-SHA256 value".into(),
            ));
        }

        let candidate = self.sse_c_key_fingerprint_mac(key);
        Ok(bool::from(
            stored_mac.as_slice().ct_eq(candidate.as_slice()),
        ))
    }

    fn sse_c_key_fingerprint_mac(&self, key: &ObjectKey) -> [u8; 32] {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.bytes)
            .expect("HMAC-SHA256 accepts the fixed-size master key");
        mac.update(SSE_C_KEY_FINGERPRINT_DOMAIN);
        mac.update(&[0]);
        mac.update(&key.bytes);
        mac.finalize().into_bytes().into()
    }
}

pub struct ObjectKey {
    pub bytes: [u8; 32],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_c_fingerprint_matches_domain_separated_hmac_vector() {
        let master_key = MasterKey {
            bytes: core::array::from_fn(|index| index as u8),
        };
        let object_key = ObjectKey {
            bytes: core::array::from_fn(|index| (index + 32) as u8),
        };

        assert_eq!(
            master_key.sse_c_key_fingerprint(&object_key),
            "v1:hmac-sha256:75cbf759ac8006383c07ceaa9c93e1a0e8c6dda7f1a06cef0fdfc15f4656641c"
        );
    }

    #[test]
    fn sse_c_fingerprint_verification_accepts_matching_key_only() {
        let master_key = MasterKey { bytes: [3; 32] };
        let expected_key = ObjectKey { bytes: [7; 32] };
        let other_key = ObjectKey { bytes: [8; 32] };
        let stored = master_key.sse_c_key_fingerprint(&expected_key);

        assert!(
            master_key
                .verify_sse_c_key_fingerprint(&stored, &expected_key)
                .unwrap()
        );
        assert!(
            !master_key
                .verify_sse_c_key_fingerprint(&stored, &other_key)
                .unwrap()
        );
    }

    #[test]
    fn sse_c_fingerprint_verification_rejects_malformed_persisted_values() {
        let master_key = MasterKey { bytes: [3; 32] };
        let object_key = ObjectKey { bytes: [7; 32] };

        for stored in ["hmac-sha256:00", "v2:hmac-sha256:00", "v1:hmac-sha256:xyz"] {
            assert!(matches!(
                master_key.verify_sse_c_key_fingerprint(stored, &object_key),
                Err(AppError::Crypto(_))
            ));
        }
    }
}

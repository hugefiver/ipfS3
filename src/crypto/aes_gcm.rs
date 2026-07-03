use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use bytes::Bytes;

use crate::error::{AppError, AppResult};

use super::key::ObjectKey;

pub fn encrypt_chunk(ok: &ObjectKey, nonce: &[u8; 12], plaintext: &[u8]) -> AppResult<Bytes> {
    let cipher = Aes256Gcm::new_from_slice(&ok.bytes)
        .map_err(|e| AppError::Crypto(format!("aes init: {e}")))?;
    #[allow(deprecated)]
    let nonce = Nonce::from_slice(nonce);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| AppError::Crypto("aes-gcm encrypt failed".into()))?;
    let mut out = Vec::with_capacity(12 + ciphertext.len());
    out.extend_from_slice(nonce);
    out.extend_from_slice(&ciphertext);
    Ok(Bytes::from(out))
}

pub fn decrypt_chunk(ok: &ObjectKey, encrypted: &[u8]) -> AppResult<Bytes> {
    if encrypted.len() < 12 + 16 {
        return Err(AppError::Crypto("encrypted chunk too short".into()));
    }
    let cipher = Aes256Gcm::new_from_slice(&ok.bytes)
        .map_err(|e| AppError::Crypto(format!("aes init: {e}")))?;
    #[allow(deprecated)]
    let nonce = Nonce::from_slice(&encrypted[..12]);
    let plaintext = cipher.decrypt(nonce, &encrypted[12..]).map_err(|_| {
        AppError::Crypto("aes-gcm decrypt failed — wrong key or corrupted data".into())
    })?;
    Ok(Bytes::from(plaintext))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::key::ObjectKey;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let ok = ObjectKey { bytes: [42u8; 32] };
        let nonce = [0u8; 12];
        let plaintext = b"hello world, this is a test of AES-256-GCM encryption";

        let encrypted = encrypt_chunk(&ok, &nonce, plaintext).unwrap();
        let decrypted = decrypt_chunk(&ok, &encrypted).unwrap();

        assert_eq!(&decrypted[..], plaintext);
    }

    #[test]
    fn test_decrypt_with_wrong_key_fails() {
        let ok1 = ObjectKey { bytes: [1u8; 32] };
        let ok2 = ObjectKey { bytes: [2u8; 32] };
        let nonce = [0u8; 12];
        let plaintext = b"secret data";

        let encrypted = encrypt_chunk(&ok1, &nonce, plaintext).unwrap();
        let result = decrypt_chunk(&ok2, &encrypted);

        assert!(result.is_err());
    }
}

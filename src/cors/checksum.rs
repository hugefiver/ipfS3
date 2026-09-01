const REFLECTED_POLYNOMIAL: u64 = 0x9a6c9329ac4bc9b5;

pub(crate) fn crc64nvme(bytes: &[u8]) -> [u8; 8] {
    let mut checksum = u64::MAX;
    for &byte in bytes {
        checksum ^= u64::from(byte);
        for _ in 0..8 {
            checksum = if checksum & 1 == 1 {
                (checksum >> 1) ^ REFLECTED_POLYNOMIAL
            } else {
                checksum >> 1
            };
        }
    }
    (!checksum).to_be_bytes()
}

#[cfg(test)]
mod tests {
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    use super::crc64nvme;

    #[test]
    fn crc64nvme_matches_the_standard_known_vector() {
        let checksum = crc64nvme(b"123456789");

        assert_eq!(hex::encode(checksum), "ae8b14860a799888");
        assert_eq!(STANDARD.encode(checksum), "rosUhgp5mIg=");
    }
}

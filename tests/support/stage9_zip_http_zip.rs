//! Deterministic stored ZIP fixtures; shared output bytes deliberately share a CID.

pub fn shared_outputs_zip() -> Vec<u8> {
    stored_zip(&[
        ("first.txt".into(), vec![0]),
        ("second.txt".into(), vec![0]),
    ])
}

pub fn partial_zip() -> Vec<u8> {
    stored_zip(&[
        ("success.txt".into(), vec![0]),
        ("failed.txt".into(), vec![0]),
    ])
}

pub fn empty_zip() -> Vec<u8> {
    stored_zip(&[])
}

pub fn distinct_outputs_zip(count: usize) -> Vec<u8> {
    stored_zip(
        &(0..count)
            .map(|index| {
                (
                    format!("entry-{index:02}.txt"),
                    (index as u64).to_le_bytes().to_vec(),
                )
            })
            .collect::<Vec<_>>(),
    )
}

pub fn distinct_output_cid(index: usize) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest((index as u64).to_le_bytes());
    let hash = cid::multihash::Multihash::<64>::wrap(0x12, &digest).unwrap();
    cid::Cid::new_v1(0x55, hash).to_string()
}

pub fn distinct_output_cids(count: usize) -> Vec<&'static str> {
    (0..count)
        .map(|index| &*Box::leak(distinct_output_cid(index).into_boxed_str()))
        .collect()
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & (0_u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

fn stored_zip(entries: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut zip = Vec::new();
    let mut offsets = Vec::new();
    for (name, byte) in entries {
        offsets.push(zip.len() as u32);
        zip.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        zip.extend_from_slice(&20u16.to_le_bytes());
        zip.extend_from_slice(&[0; 8]);
        zip.extend_from_slice(&crc32(byte).to_le_bytes());
        zip.extend_from_slice(&(byte.len() as u32).to_le_bytes());
        zip.extend_from_slice(&(byte.len() as u32).to_le_bytes());
        zip.extend_from_slice(&(name.len() as u16).to_le_bytes());
        zip.extend_from_slice(&0u16.to_le_bytes());
        zip.extend_from_slice(name.as_bytes());
        zip.extend_from_slice(byte);
    }
    let central = zip.len() as u32;
    for ((name, byte), offset) in entries.iter().zip(offsets) {
        zip.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
        zip.extend_from_slice(&20u16.to_le_bytes());
        zip.extend_from_slice(&20u16.to_le_bytes());
        zip.extend_from_slice(&[0; 8]);
        zip.extend_from_slice(&crc32(byte).to_le_bytes());
        zip.extend_from_slice(&(byte.len() as u32).to_le_bytes());
        zip.extend_from_slice(&(byte.len() as u32).to_le_bytes());
        zip.extend_from_slice(&(name.len() as u16).to_le_bytes());
        zip.extend_from_slice(&0u16.to_le_bytes());
        zip.extend_from_slice(&[0; 12]);
        zip.extend_from_slice(&offset.to_le_bytes());
        zip.extend_from_slice(name.as_bytes());
    }
    let central_size = zip.len() as u32 - central;
    zip.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
    zip.extend_from_slice(&[0; 4]);
    zip.extend_from_slice(&(entries.len() as u16).to_le_bytes());
    zip.extend_from_slice(&(entries.len() as u16).to_le_bytes());
    zip.extend_from_slice(&central_size.to_le_bytes());
    zip.extend_from_slice(&central.to_le_bytes());
    zip.extend_from_slice(&0u16.to_le_bytes());
    zip
}

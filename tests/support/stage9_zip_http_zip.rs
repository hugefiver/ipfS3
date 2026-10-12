//! Deterministic stored ZIP fixtures; shared output bytes deliberately share a CID.

pub fn shared_outputs_zip() -> Vec<u8> {
    stored_zip(&["first.txt", "second.txt"])
}

pub fn partial_zip() -> Vec<u8> {
    stored_zip(&["success.txt", "failed.txt"])
}

pub fn empty_zip() -> Vec<u8> {
    stored_zip(&[])
}

fn stored_zip(names: &[&str]) -> Vec<u8> {
    let mut zip = Vec::new();
    let mut offsets = Vec::new();
    // Each file contains one NUL byte, whose CRC32 is d202ef8d.
    for name in names {
        offsets.push(zip.len() as u32);
        zip.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        zip.extend_from_slice(&20u16.to_le_bytes());
        zip.extend_from_slice(&[0; 8]);
        zip.extend_from_slice(&0xd202_ef8du32.to_le_bytes());
        zip.extend_from_slice(&1u32.to_le_bytes());
        zip.extend_from_slice(&1u32.to_le_bytes());
        zip.extend_from_slice(&(name.len() as u16).to_le_bytes());
        zip.extend_from_slice(&0u16.to_le_bytes());
        zip.extend_from_slice(name.as_bytes());
        zip.push(0);
    }
    let central = zip.len() as u32;
    for (name, offset) in names.iter().zip(offsets) {
        zip.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
        zip.extend_from_slice(&20u16.to_le_bytes());
        zip.extend_from_slice(&20u16.to_le_bytes());
        zip.extend_from_slice(&[0; 8]);
        zip.extend_from_slice(&0xd202_ef8du32.to_le_bytes());
        zip.extend_from_slice(&1u32.to_le_bytes());
        zip.extend_from_slice(&1u32.to_le_bytes());
        zip.extend_from_slice(&(name.len() as u16).to_le_bytes());
        zip.extend_from_slice(&0u16.to_le_bytes());
        zip.extend_from_slice(&[0; 12]);
        zip.extend_from_slice(&offset.to_le_bytes());
        zip.extend_from_slice(name.as_bytes());
    }
    let central_size = zip.len() as u32 - central;
    zip.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
    zip.extend_from_slice(&[0; 4]);
    zip.extend_from_slice(&(names.len() as u16).to_le_bytes());
    zip.extend_from_slice(&(names.len() as u16).to_le_bytes());
    zip.extend_from_slice(&central_size.to_le_bytes());
    zip.extend_from_slice(&central.to_le_bytes());
    zip.extend_from_slice(&0u16.to_le_bytes());
    zip
}

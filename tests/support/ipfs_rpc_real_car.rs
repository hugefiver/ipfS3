//! A bounded CARv1 fixture verifier, not a provider/transport implementation.
use anyhow::{Result, bail, ensure};
use cid::Cid;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Cursor};

pub fn canonical(value: &str) -> Result<String> {
    let cid = Cid::try_from(value)?;
    Ok(Cid::new_v1(cid.codec(), *cid.hash()).to_string())
}

pub fn raw_cid(bytes: &[u8]) -> Result<String> {
    let hash = cid::multihash::Multihash::<64>::wrap(0x12, &Sha256::digest(bytes))?;
    Ok(Cid::new_v1(0x55, hash).to_string())
}

fn varint(input: &mut &[u8]) -> Result<usize> {
    let mut value = 0_u64;
    for shift in (0..=63).step_by(7) {
        let byte = take(input, 1)?[0];
        ensure!(shift != 63 || byte <= 1, "CAR varint overflow");
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            ensure!(shift == 0 || byte != 0, "noncanonical CAR varint");
            return Ok(usize::try_from(value)?);
        }
    }
    bail!("unterminated CAR varint")
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    ensure!(input.len() >= len, "truncated CAR/CBOR");
    let (value, rest) = input.split_at(len);
    *input = rest;
    Ok(value)
}

fn cbor(input: &mut &[u8], major: u8) -> Result<usize> {
    let byte = take(input, 1)?[0];
    ensure!(byte >> 5 == major, "unexpected CAR header CBOR type");
    let value = match byte & 31 {
        value @ 0..=23 => u64::from(value),
        24 => u64::from(take(input, 1)?[0]),
        25 => u64::from(u16::from_be_bytes(take(input, 2)?.try_into()?)),
        26 => u64::from(u32::from_be_bytes(take(input, 4)?.try_into()?)),
        27 => u64::from_be_bytes(take(input, 8)?.try_into()?),
        _ => bail!("indefinite/invalid CAR header CBOR"),
    };
    Ok(usize::try_from(value)?)
}

fn single_root(mut header: &[u8], expected: &str) -> Result<()> {
    ensure!(cbor(&mut header, 5)? == 2, "CAR header must have two keys");
    let mut roots_seen = false;
    let mut version_seen = false;
    for _ in 0..2 {
        let len = cbor(&mut header, 3)?;
        match take(&mut header, len)? {
            b"version" => {
                ensure!(!version_seen && cbor(&mut header, 0)? == 1, "not CARv1");
                version_seen = true;
            }
            b"roots" => {
                ensure!(
                    !roots_seen && cbor(&mut header, 4)? == 1,
                    "not single-root CAR"
                );
                ensure!(cbor(&mut header, 6)? == 42, "root is not a CID");
                let len = cbor(&mut header, 2)?;
                let bytes = take(&mut header, len)?;
                ensure!(bytes.first() == Some(&0), "missing CID identity prefix");
                let root = Cid::try_from(&bytes[1..])?;
                ensure!(
                    canonical(&root.to_string())? == expected,
                    "CAR root mismatch"
                );
                roots_seen = true;
            }
            _ => bail!("unrecognized CAR header key"),
        }
    }
    ensure!(
        roots_seen && version_seen && header.is_empty(),
        "incomplete CAR header"
    );
    Ok(())
}

pub fn header<'a>(mut car: &'a [u8], root: &str) -> Result<&'a [u8]> {
    ensure!(car.len() <= 16 * 1024 * 1024, "fixture CAR exceeds budget");
    let len = varint(&mut car)?;
    let header = take(&mut car, len)?;
    single_root(header, &canonical(root)?)?;
    Ok(header)
}

/// Compare all exported blocks, irrespective of export traversal order. Every
/// block is SHA-256 verified. These fixtures deliberately use sha2-256 only.
pub fn blocks(mut car: &[u8], root: &str) -> Result<BTreeMap<String, Vec<u8>>> {
    ensure!(car.len() <= 16 * 1024 * 1024, "fixture CAR exceeds budget");
    let len = varint(&mut car)?;
    single_root(take(&mut car, len)?, &canonical(root)?)?;
    let mut blocks = BTreeMap::new();
    while !car.is_empty() {
        let len = varint(&mut car)?;
        let record = take(&mut car, len)?;
        let mut cursor = Cursor::new(record);
        let cid = Cid::read_bytes(&mut cursor)?;
        let data = &record[usize::try_from(cursor.position())?..];
        ensure!(cid.hash().code() == 0x12, "fixture block is not SHA-256");
        ensure!(
            cid.hash().digest() == Sha256::digest(data).as_slice(),
            "corrupt CAR block"
        );
        let key = canonical(&cid.to_string())?;
        ensure!(
            blocks.insert(key, data.to_vec()).is_none(),
            "duplicate CAR block"
        );
    }
    ensure!(
        blocks.contains_key(&canonical(root)?),
        "CAR root block missing"
    );
    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncated_or_noncanonical_frames_are_rejected() {
        assert!(varint(&mut &[128][..]).is_err());
        assert!(varint(&mut &[128, 0][..]).is_err());
        assert!(blocks(&[1, 0], &raw_cid(b"").unwrap()).is_err());
    }

    #[test]
    fn complete_raw_car_is_hash_checked_and_single_root() {
        let cid = Cid::try_from(raw_cid(b"hi").unwrap().as_str()).unwrap();
        let mut header = b"\xa2\x65roots\x81\xd8\x2a\x58\x25\x00".to_vec();
        header.extend(cid.to_bytes());
        header.extend(b"\x67version\x01");
        let mut car = vec![header.len() as u8];
        car.extend(header);
        car.push((cid.to_bytes().len() + 2) as u8);
        car.extend(cid.to_bytes());
        car.extend(b"hi");
        assert_eq!(blocks(&car, &cid.to_string()).unwrap().len(), 1);
        *car.last_mut().unwrap() = b'x';
        assert!(blocks(&car, &cid.to_string()).is_err());
    }
}

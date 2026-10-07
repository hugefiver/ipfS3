//! Fixture-only, bounded CARv1 hash verifier, adapted from the owned Stage5 helper.
use anyhow::{Result, bail, ensure};
use cid::Cid;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
};

pub fn canonical(value: &str) -> Result<String> {
    let cid = Cid::try_from(value)?;
    Ok(Cid::new_v1(cid.codec(), *cid.hash()).to_string())
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    ensure!(input.len() >= len, "truncated CAR/CBOR");
    let (value, rest) = input.split_at(len);
    *input = rest;
    Ok(value)
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
                ensure!(
                    canonical(&Cid::try_from(&bytes[1..])?.to_string())? == expected,
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
        ensure!(
            blocks
                .insert(canonical(&cid.to_string())?, data.to_vec())
                .is_none(),
            "duplicate CAR block"
        );
    }
    ensure!(
        blocks.contains_key(&canonical(root)?),
        "CAR root block missing"
    );
    closure(&blocks, root)?;
    Ok(blocks)
}

// Independent fixture verifier for the raw/dag-pb UnixFS DAGs used here. A CAR
// header plus individually correct hashes is NOT a complete DAG certificate.
fn pb_fields(mut bytes: &[u8]) -> Result<Vec<(usize, Vec<u8>)>> {
    let mut fields = Vec::new();
    while !bytes.is_empty() {
        let tag = varint(&mut bytes)?;
        ensure!(tag >> 3 != 0, "invalid protobuf field");
        match tag & 7 {
            0 => {
                varint(&mut bytes)?;
            }
            2 => {
                let len = varint(&mut bytes)?;
                fields.push((tag >> 3, take(&mut bytes, len)?.to_vec()));
            }
            _ => bail!("unsupported UnixFS protobuf wire type"),
        }
    }
    Ok(fields)
}

fn closure(blocks: &BTreeMap<String, Vec<u8>>, root: &str) -> Result<()> {
    let mut pending = vec![canonical(root)?];
    let mut seen = BTreeSet::new();
    while let Some(key) = pending.pop() {
        if !seen.insert(key.clone()) {
            continue;
        }
        let bytes = blocks
            .get(&key)
            .ok_or_else(|| anyhow::anyhow!("CAR linked block missing: {key}"))?;
        match Cid::try_from(key.as_str())?.codec() {
            0x55 => {}
            0x70 => {
                for (_, link) in pb_fields(bytes)?
                    .into_iter()
                    .filter(|(field, _)| *field == 2)
                {
                    let hashes: Vec<_> = pb_fields(&link)?
                        .into_iter()
                        .filter(|(field, _)| *field == 1)
                        .collect();
                    ensure!(
                        hashes.len() == 1,
                        "UnixFS link must contain exactly one CID"
                    );
                    let cid = Cid::try_from(hashes[0].1.as_slice())?;
                    pending.push(canonical(&cid.to_string())?);
                }
            }
            _ => bail!("unsupported fixture DAG codec"),
        }
    }
    ensure!(
        seen.len() == blocks.len(),
        "CAR contains unreachable blocks"
    );
    Ok(())
}

#[cfg(test)]
pub fn raw_cid(bytes: &[u8], codec: u64) -> String {
    let hash = cid::multihash::Multihash::<64>::wrap(0x12, &Sha256::digest(bytes)).unwrap();
    Cid::new_v1(codec, hash).to_string()
}

#[cfg(test)]
fn encode_varint(mut value: usize) -> Vec<u8> {
    let mut out = Vec::new();
    while value >= 128 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
    out
}

#[cfg(test)]
pub fn encode(roots: &[String], blocks: &[(String, Vec<u8>)]) -> Vec<u8> {
    assert!(roots.len() < 24);
    let mut header = b"\xa2\x65roots".to_vec();
    header.push(0x80 + roots.len() as u8);
    for root in roots {
        let bytes = Cid::try_from(root.as_str()).unwrap().to_bytes();
        header.extend([0xd8, 42, 0x58, (bytes.len() + 1) as u8, 0]);
        header.extend(bytes);
    }
    header.extend(b"\x67version\x01");
    let mut car = encode_varint(header.len());
    car.extend(header);
    for (cid, bytes) in blocks {
        let encoded = Cid::try_from(cid.as_str()).unwrap().to_bytes();
        car.extend(encode_varint(encoded.len() + bytes.len()));
        car.extend(encoded);
        car.extend(bytes);
    }
    car
}

#[cfg(test)]
pub fn directory(child: &str) -> (String, Vec<u8>) {
    let cid = Cid::try_from(child).unwrap().to_bytes();
    let mut link = vec![10];
    link.extend(encode_varint(cid.len()));
    link.extend(cid);
    link.extend(b"\x12\x05entry\x18\x05");
    let mut block = vec![10, 2, 8, 1, 18]; // UnixFS Directory + PBLink
    block.extend(encode_varint(link.len()));
    block.extend(link);
    (raw_cid(&block, 0x70), block)
}

#[cfg(test)]
pub fn invalid_cars(root: &str, blocks: &[(String, Vec<u8>)]) -> Vec<(&'static str, Vec<u8>)> {
    let roots = [root.to_owned()];
    let valid = encode(&roots, blocks);
    let mut truncated = valid.clone();
    truncated.pop();
    let mut trailer = valid.clone();
    trailer.push(128);
    let mut corrupt = valid;
    *corrupt.last_mut().unwrap() ^= 1;
    let mut duplicate = blocks.to_vec();
    duplicate.push(blocks[0].clone());
    let mut unreachable = blocks.to_vec();
    unreachable.push((raw_cid(b"unreachable", 0x55), b"unreachable".to_vec()));
    vec![
        ("missing-child", encode(&roots, &blocks[..1])),
        ("missing-root", encode(&roots, &blocks[1..])),
        (
            "multi-root",
            encode(&[root.to_owned(), blocks[1].0.clone()], blocks),
        ),
        ("wrong-root", encode(&[blocks[1].0.clone()], blocks)),
        ("duplicate", encode(&roots, &duplicate)),
        ("unreachable", encode(&roots, &unreachable)),
        ("truncated", truncated),
        ("trailer", trailer),
        ("corrupt", corrupt),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_provider_real_car_hash_and_frames_fail_closed() {
        let hash = cid::multihash::Multihash::<64>::wrap(0x12, &Sha256::digest(b"hi")).unwrap();
        let cid = Cid::new_v1(0x55, hash);
        let mut header = b"\xa2\x65roots\x81\xd8\x2a\x58\x25\x00".to_vec();
        header.extend(cid.to_bytes());
        header.extend(b"\x67version\x01");
        let mut car = vec![header.len() as u8];
        car.extend(header);
        car.push((cid.to_bytes().len() + 2) as u8);
        car.extend(cid.to_bytes());
        car.extend(b"hi");
        assert_eq!(blocks(&car, &cid.to_string()).unwrap().len(), 1);
        assert!(blocks(&car, &Cid::new_v1(0x70, hash).to_string()).is_err());
        *car.last_mut().unwrap() = b'x';
        assert!(blocks(&car, &cid.to_string()).is_err());
        assert!(varint(&mut &[128][..]).is_err());
        assert!(varint(&mut &[128, 0][..]).is_err());
    }

    #[test]
    fn cluster_provider_real_car_requires_complete_reachable_dag() {
        let child = raw_cid(b"child", 0x55);
        let (root, data) = directory(&child);
        let records = [(root.clone(), data), (child, b"child".to_vec())];
        assert_eq!(
            blocks(&encode(std::slice::from_ref(&root), &records), &root)
                .unwrap()
                .len(),
            2
        );
        for (case, car) in invalid_cars(&root, &records) {
            assert!(blocks(&car, &root).is_err(), "accepted invalid CAR: {case}");
        }
    }
}

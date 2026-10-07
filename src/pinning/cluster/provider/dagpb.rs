use super::ClusterWriteFailure as Failure;
use super::cbor::{canonical, invalid};
use cid::Cid;

pub(super) fn varint(bytes: &[u8]) -> Result<Option<(u64, usize)>, Failure> {
    let mut value = 0;
    for (i, &b) in bytes.iter().take(10).enumerate() {
        if i == 9 && b > 1 {
            return Err(invalid());
        }
        value |= u64::from(b & 127) << (i * 7);
        if b < 128 {
            if i > 0 && b == 0 {
                return Err(invalid());
            }
            return Ok(Some((value, i + 1)));
        }
    }
    if bytes.len() >= 10 {
        return Err(invalid());
    }
    Ok(None)
}

fn number(bytes: &mut &[u8]) -> Result<u64, Failure> {
    let (n, size) = varint(bytes)?.ok_or_else(invalid)?;
    *bytes = &bytes[size..];
    Ok(n)
}
fn data<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8], Failure> {
    let length: usize = number(bytes)?.try_into().map_err(|_| invalid())?;
    if length > bytes.len() {
        return Err(invalid());
    }
    let value = &bytes[..length];
    *bytes = &bytes[length..];
    Ok(value)
}
pub(super) fn links(mut bytes: &[u8]) -> Result<Vec<Cid>, Failure> {
    let mut links = Vec::new();
    let mut seen_data = false;
    while !bytes.is_empty() {
        match number(&mut bytes)? {
            10 if !seen_data => {
                data(&mut bytes)?;
                seen_data = true;
            }
            18 => links.push(link(data(&mut bytes)?)?),
            _ => return Err(invalid()),
        }
    }
    Ok(links)
}
fn link(mut bytes: &[u8]) -> Result<Cid, Failure> {
    let mut cid = None;
    let mut name = false;
    let mut size = false;
    while !bytes.is_empty() {
        match number(&mut bytes)? {
            10 if cid.is_none() => {
                let encoded = data(&mut bytes)?;
                let mut cursor = std::io::Cursor::new(encoded);
                let parsed = Cid::read_bytes(&mut cursor).map_err(|_| invalid())?;
                if cursor.position() != encoded.len() as u64 {
                    return Err(invalid());
                }
                cid = Some(canonical(parsed));
            }
            18 if !name => {
                std::str::from_utf8(data(&mut bytes)?).map_err(|_| invalid())?;
                name = true;
            }
            24 if !size => {
                number(&mut bytes)?;
                size = true;
            }
            _ => return Err(invalid()),
        }
    }
    cid.ok_or_else(invalid)
}

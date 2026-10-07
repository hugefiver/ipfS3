//! Bounded definite-length CARv1/DAG-CBOR reader, not a permissive JSON substitute.
use super::{ClusterFailureCode as Code, ClusterWriteFailure as Failure};
use cid::Cid;

pub(super) fn invalid() -> Failure {
    Failure::new(Code::InvalidCar)
}
pub(super) fn canonical(cid: Cid) -> Cid {
    Cid::new_v1(cid.codec(), *cid.hash())
}

pub(super) struct Cursor<'a> {
    pub bytes: &'a [u8],
    pub position: usize,
}
impl<'a> Cursor<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], Failure> {
        let end = self
            .position
            .checked_add(n)
            .filter(|n| *n <= self.bytes.len())
            .ok_or_else(invalid)?;
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }
    pub fn head(&mut self) -> Result<(u8, u64), Failure> {
        let first = self.take(1)?[0];
        let add = first & 31;
        let n = match add {
            0..=23 => add.into(),
            24 => self.take(1)?[0].into(),
            25 => u16::from_be_bytes(self.take(2)?.try_into().map_err(|_| invalid())?).into(),
            26 => u32::from_be_bytes(self.take(4)?.try_into().map_err(|_| invalid())?).into(),
            27 => u64::from_be_bytes(self.take(8)?.try_into().map_err(|_| invalid())?),
            _ => return Err(invalid()),
        };
        Ok((first >> 5, n))
    }
    pub fn sized(&mut self, major: u8) -> Result<&'a [u8], Failure> {
        let (kind, n) = self.head()?;
        if kind != major {
            return Err(invalid());
        }
        self.take(n.try_into().map_err(|_| invalid())?)
    }
    pub fn text(&mut self) -> Result<&'a str, Failure> {
        std::str::from_utf8(self.sized(3)?).map_err(|_| invalid())
    }
    pub fn cid(&mut self) -> Result<Cid, Failure> {
        if self.head()? != (6, 42) {
            return Err(invalid());
        }
        let bytes = self.sized(2)?;
        if bytes.first() != Some(&0) {
            return Err(invalid());
        }
        let mut cursor = std::io::Cursor::new(&bytes[1..]);
        let cid = Cid::read_bytes(&mut cursor).map_err(|_| invalid())?;
        if cursor.position() != (bytes.len() - 1) as u64 {
            return Err(invalid());
        }
        Ok(canonical(cid))
    }
    fn walk(&mut self, depth: usize, links: &mut Vec<Cid>) -> Result<(), Failure> {
        if depth > 128 {
            return Err(invalid());
        }
        let start = self.position;
        let (kind, n) = self.head()?;
        match kind {
            0 | 1 | 7 => {}
            2 | 3 => {
                let bytes = self.take(n.try_into().map_err(|_| invalid())?)?;
                if kind == 3 {
                    std::str::from_utf8(bytes).map_err(|_| invalid())?;
                }
            }
            4 | 5 => {
                if n > self.bytes.len() as u64 {
                    return Err(invalid());
                }
                for _ in 0..n {
                    if kind == 5 {
                        self.text()?;
                    }
                    self.walk(depth + 1, links)?;
                }
            }
            6 if n == 42 => {
                self.position = start;
                links.push(self.cid()?);
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }
}

pub(super) fn header(bytes: &[u8], expected: Cid) -> Result<(), Failure> {
    let mut c = Cursor::new(bytes);
    if c.head()? != (5, 2) {
        return Err(invalid());
    }
    let mut root = false;
    let mut version = false;
    for _ in 0..2 {
        match c.text()? {
            "roots" if !root => {
                if c.head()? != (4, 1) || c.cid()? != expected {
                    return Err(invalid());
                }
                root = true;
            }
            "version" if !version => {
                if c.head()? != (0, 1) {
                    return Err(invalid());
                }
                version = true;
            }
            _ => return Err(invalid()),
        }
    }
    if !root || !version || c.position != bytes.len() {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn links(bytes: &[u8]) -> Result<Vec<Cid>, Failure> {
    let mut cursor = Cursor::new(bytes);
    let mut links = Vec::new();
    cursor.walk(0, &mut links)?;
    if cursor.position != bytes.len() {
        return Err(invalid());
    }
    Ok(links)
}

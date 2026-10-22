//! Structural preflight counts nodes/depth and rejects duplicate keys even in
//! nested map DTOs whose serde map implementation would otherwise overwrite.
use crate::pinning::management::SafeError;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::{collections::HashSet, fmt};

struct Node<'a> {
    nodes: &'a mut usize,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for Node<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        *self.nodes += 1;
        if *self.nodes > 32768 || self.depth > 64 {
            return Err(de::Error::custom("frame limit"));
        }
        d.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Node<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded JSON")
    }
    fn visit_bool<E: de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: de::Error>(self, _: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq
            .next_element_seed(Node {
                nodes: self.nodes,
                depth: self.depth + 1,
            })?
            .is_some()
        {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut keys = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            *self.nodes += 1;
            if *self.nodes > 32768 || !keys.insert(key) {
                return Err(de::Error::custom("invalid field"));
            }
            map.next_value_seed(Node {
                nodes: self.nodes,
                depth: self.depth + 1,
            })?;
        }
        Ok(())
    }
}
pub(super) fn check(bytes: &[u8]) -> Result<(), SafeError> {
    resident_cost(bytes).map(|_| ())
}
pub(super) fn resident_cost(bytes: &[u8]) -> Result<usize, SafeError> {
    std::str::from_utf8(bytes).map_err(|_| SafeError::InvalidInput)?;
    let mut nodes = 0;
    let mut d = serde_json::Deserializer::from_slice(bytes);
    Node {
        nodes: &mut nodes,
        depth: 0,
    }
    .deserialize(&mut d)
    .and_then(|()| d.end())
    .map_err(|_| SafeError::InvalidInput)?;
    Ok(bytes.len() + nodes * 128)
}

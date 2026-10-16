//! Request/preparation JSON shares the management wire budgets, not R26's
//! separate 8 MiB/depth-32 contract. The private wire checker is not a public API.
use std::fmt;

use anyhow::Result;
use ipfs_s3_gateway::pinning::management::wire::{MAX_WIRE_BYTES, MAX_WIRE_DEPTH, MAX_WIRE_NODES};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

use super::input::invalid;

struct Node<'a> {
    nodes: &'a mut usize,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for Node<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        *self.nodes += 1;
        if *self.nodes > MAX_WIRE_NODES || self.depth > MAX_WIRE_DEPTH {
            return Err(de::Error::custom("management JSON limit"));
        }
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Node<'_> {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded management JSON")
    }
    fn visit_bool<E: de::Error>(self, _: bool) -> std::result::Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> std::result::Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> std::result::Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> std::result::Result<(), E> {
        Ok(())
    }
    fn visit_str<E: de::Error>(self, _: &str) -> std::result::Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<(), E> {
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<(), A::Error> {
        while sequence
            .next_element_seed(Node {
                nodes: self.nodes,
                depth: self.depth + 1,
            })?
            .is_some()
        {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
        while map
            .next_key_seed(Node {
                nodes: self.nodes,
                depth: self.depth + 1,
            })?
            .is_some()
        {
            map.next_value_seed(Node {
                nodes: self.nodes,
                depth: self.depth + 1,
            })?;
        }
        Ok(())
    }
}

pub fn check(bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_WIRE_BYTES {
        return Err(invalid());
    }
    let mut nodes = 0;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    Node {
        nodes: &mut nodes,
        depth: 0,
    }
    .deserialize(&mut deserializer)
    .map_err(|_| invalid())?;
    deserializer.end().map_err(|_| invalid())
}

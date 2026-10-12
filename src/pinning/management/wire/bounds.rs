use std::{fmt, io};

use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

use super::{MAX_WIRE_BYTES, MAX_WIRE_DEPTH, MAX_WIRE_NODES, SafeError};

#[derive(Default)]
pub(super) struct BoundedOutput {
    pub(super) bytes: Vec<u8>,
}

impl io::Write for BoundedOutput {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.len() > MAX_WIRE_BYTES.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("management wire limit"));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct Budget {
    nodes: usize,
    exceeded: bool,
}

struct Node<'a> {
    budget: &'a mut Budget,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for Node<'_> {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        self.budget.nodes += 1;
        if self.budget.nodes > MAX_WIRE_NODES || self.depth > MAX_WIRE_DEPTH {
            self.budget.exceeded = true;
            return Err(de::Error::custom("management wire limit"));
        }
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Node<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded management JSON")
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

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        while sequence
            .next_element_seed(Node {
                budget: self.budget,
                depth: self.depth + 1,
            })?
            .is_some()
        {}
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while map
            .next_key_seed(Node {
                budget: self.budget,
                depth: self.depth + 1,
            })?
            .is_some()
        {
            map.next_value_seed(Node {
                budget: self.budget,
                depth: self.depth + 1,
            })?;
        }
        Ok(())
    }
}

pub(super) fn check(input: &[u8]) -> Result<(), SafeError> {
    if input.len() > MAX_WIRE_BYTES {
        return Err(SafeError::LimitExceeded);
    }
    let mut budget = Budget::default();
    let mut deserializer = serde_json::Deserializer::from_slice(input);
    let result = Node {
        budget: &mut budget,
        depth: 0,
    }
    .deserialize(&mut deserializer)
    .and_then(|()| deserializer.end());
    if budget.exceeded {
        return Err(SafeError::LimitExceeded);
    }
    result.map_err(|_| SafeError::InvalidInput)
}

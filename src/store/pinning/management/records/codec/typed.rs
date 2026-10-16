//! Typed private decoding without an intermediate JSON value tree.
use super::*;
use serde::de::{DeserializeOwned, MapAccess, SeqAccess, Visitor};

/// Streaming serialization measurement; no serialized document is retained.
pub(in crate::store::pinning::management::records) fn measure<T: Serialize + ?Sized>(
    value: &T,
) -> Result<(usize, usize)> {
    #[derive(Default)]
    struct Counter {
        bytes: usize,
        nodes: usize,
        string: bool,
        escape: bool,
        scalar: bool,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(input.len())
                .ok_or_else(|| std::io::Error::other("budget"))?;
            for &byte in input {
                if self.string {
                    if self.escape {
                        self.escape = false;
                    } else if byte == b'\\' {
                        self.escape = true;
                    } else if byte == b'"' {
                        self.string = false;
                    }
                    continue;
                }
                match byte {
                    b'"' => {
                        self.nodes += 1;
                        self.string = true;
                        self.scalar = false;
                    }
                    b'{' | b'[' => {
                        self.nodes += 1;
                        self.scalar = false;
                    }
                    b'}' | b']' | b':' | b',' | b' ' | b'\r' | b'\n' | b'\t' => self.scalar = false,
                    _ => {
                        if !self.scalar {
                            self.nodes += 1;
                        }
                        self.scalar = true;
                    }
                }
            }
            Ok(input.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter::default();
    serde_json::to_writer(&mut counter, value).map_err(|_| ManagementError::Budget)?;
    Ok((counter.nodes, counter.bytes))
}

/// Counts keys before the duplicate-key pass allocates its map-local key set.
fn keys(input: &[u8]) -> usize {
    let mut index = 0;
    let mut keys = 0;
    while index < input.len() {
        if input[index] != b'"' {
            index += 1;
            continue;
        }
        index += 1;
        while index < input.len() {
            let byte = input[index];
            index += 1;
            if byte == b'\\' {
                index += 1;
            } else if byte == b'"' {
                break;
            }
        }
        while input.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        if input.get(index) == Some(&b':') {
            keys += 1;
        }
    }
    keys
}

pub(super) fn string_bytes(input: &[u8]) -> usize {
    string_allocations(input).0
}
// Unescaped JSON strings are borrowed by scan-only visitors. Escaped strings
// need decoder scratch; only object keys are retained by the uniqueness pass.
fn string_allocations(input: &[u8]) -> (usize, usize, usize) {
    let mut index = 0;
    let mut bytes = 0;
    let mut keys = 0;
    let mut scratch = 0;
    while index < input.len() {
        if input[index] != b'"' {
            index += 1;
            continue;
        }
        index += 1;
        let start = index;
        let mut escaped = false;
        let mut closed = false;
        while index < input.len() {
            let byte = input[index];
            index += 1;
            if byte == b'\\' {
                escaped = true;
                index += 1;
            } else if byte == b'"' {
                closed = true;
                let length = index - start - 1;
                bytes += length;
                if escaped {
                    scratch += length;
                }
                let mut next = index;
                while input.get(next).is_some_and(u8::is_ascii_whitespace) {
                    next += 1;
                }
                if input.get(next) == Some(&b':') {
                    keys += length;
                }
                break;
            }
        }
        if !closed {
            let length = input.len() - start;
            bytes += length;
            if escaped {
                scratch += length;
            }
        }
    }
    (bytes, keys, scratch)
}

#[track_caller]
pub(in crate::store::pinning::management::records) fn decode_json<T: DeserializeOwned>(
    input: &str,
    budget: &mut evidence::Budget,
) -> Result<T> {
    budget.bytes(input.len())?;
    deserialize_typed(input, budget)
}
/// Input, node scan and scratch bounds for the structural preflight and a
/// scan-only consumer. IgnoredAny skips its payload; endpoint strings may escape.
pub(in crate::store::pinning::management::records) fn charge_json_scan(
    input: &str,
    budget: &mut evidence::Budget,
) -> Result<()> {
    budget.bytes(input.len())?;
    let (_, _, scratch) = string_allocations(input.as_bytes());
    // check_structure uses deserialize_any, so escaped strings need scratch.
    budget.bytes(scratch)?;
    budget.nodes(count_nodes(
        input.as_bytes(),
        crate::pinning::management::wire::MAX_WIRE_NODES,
    )?)?;
    crate::pinning::management::wire::check_structure(input.as_bytes()).map_err(|error| {
        if error == crate::pinning::management::SafeError::LimitExceeded {
            ManagementError::Budget
        } else {
            ManagementError::InvalidEvidence
        }
    })?;
    budget.bytes(scratch)
}
/// Ordinary owned serde additionally materializes output strings. Unlike typed
/// decoding this charge does not impose a duplicate-field/unknown-field policy.
pub(in crate::store::pinning::management::records) fn charge_json_owned(
    input: &str,
    budget: &mut evidence::Budget,
) -> Result<()> {
    charge_json_scan(input, budget)?;
    budget.bytes(string_bytes(input.as_bytes()))
}
fn deserialize_typed<T: DeserializeOwned>(
    unpacked: &str,
    budget: &mut evidence::Budget,
) -> Result<T> {
    budget.nodes(count_nodes(
        unpacked.as_bytes(),
        crate::pinning::management::wire::MAX_WIRE_NODES,
    )?)?;
    budget.nodes(keys(unpacked.as_bytes()))?;
    let (strings, key_bytes, scratch) = string_allocations(unpacked.as_bytes());
    // Structural preflight decodes escaped strings into scratch.
    budget.bytes(scratch)?;
    crate::pinning::management::wire::check_structure(unpacked.as_bytes()).map_err(|error| {
        if error == crate::pinning::management::SafeError::LimitExceeded {
            ManagementError::Budget
        } else {
            ManagementError::InvalidEvidence
        }
    })?;
    // Unique needs scratch and owned map keys; its values retain no output.
    budget.bytes(scratch)?;
    budget.bytes(key_bytes)?;
    let _: Unique = serde_json::from_str(unpacked).map_err(|_| ManagementError::InvalidEvidence)?;
    // The final typed pass needs both scratch and owned output strings.
    budget.bytes(scratch)?;
    budget.bytes(strings)?;
    serde_json::from_str(unpacked).map_err(|_| ManagementError::InvalidEvidence)
}

struct Unique;
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Check;
        impl<'de> Visitor<'de> for Check {
            type Value = Unique;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("unique JSON fields")
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Unique, A::Error> {
                let mut keys = std::collections::BTreeSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !keys.insert(key) {
                        return Err(serde::de::Error::custom("duplicate field"));
                    }
                    map.next_value::<Unique>()?;
                }
                Ok(Unique)
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Unique, A::Error> {
                while seq.next_element::<Unique>()?.is_some() {}
                Ok(Unique)
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
        }
        deserializer.deserialize_any(Check)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_typed_native_strings_prepay_each_materialization_pass() {
        let text = format!("{}\n", "a".repeat(1000));
        for escaped_key in [false, true] {
            let row: crate::store::pinning::management::native::NativeRow = if escaped_key {
                [(text.clone(), None)].into()
            } else {
                [("metadata".into(), Some(text.clone()))].into()
            };
            let encoded = serde_json::to_string(&row).unwrap();
            // The escaped string needs preflight scratch, Unique scratch, then
            // typed scratch and owned output. A map key is also owned by Unique.
            let passes = if escaped_key { 5 } else { 4 };
            let lower_bound = encoded.len() + passes * text.len();
            let mut limited = evidence::Budget::default();
            limited
                .consume(
                    0,
                    crate::pinning::management::wire::MAX_WIRE_BYTES - lower_bound + 1,
                )
                .unwrap();
            assert!(matches!(
                decode_json::<crate::store::pinning::management::native::NativeRow>(
                    &encoded,
                    &mut limited
                ),
                Err(ManagementError::Budget)
            ));
            assert_eq!(
                decode_json::<crate::store::pinning::management::native::NativeRow>(
                    &encoded,
                    &mut evidence::Budget::default()
                )
                .unwrap(),
                row
            );
        }
    }
}

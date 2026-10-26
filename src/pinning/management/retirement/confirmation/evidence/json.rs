//! Progress JSON has byte arrays, not the ordinary wire-frame node limit.
use crate::{
    pinning::management::SafeError,
    store::pinning::management::native::meter::{Budget, ResidentGuard},
};
use serde::{
    Serialize,
    de::{self, DeserializeOwned, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use std::{collections::BTreeSet, fmt, io::Read};

type Result<T> = std::result::Result<T, SafeError>;

fn storage(_: crate::store::pinning::management::ManagementError) -> SafeError {
    SafeError::LimitExceeded
}

struct Scan<'a> {
    budget: &'a mut Budget,
    guards: Vec<ResidentGuard>,
    reserved: usize,
    used: usize,
    envelope: bool,
}
impl Scan<'_> {
    fn add<E: de::Error>(&mut self, bytes: usize) -> std::result::Result<(), E> {
        self.budget
            .charge(0, 0, 0, bytes as u64)
            .map_err(E::custom)?;
        self.used = self
            .used
            .checked_add(bytes)
            .ok_or_else(|| E::custom("resident limit"))?;
        while self.used > self.reserved {
            self.guards
                .push(self.budget.reserve(65536).map_err(E::custom)?);
            self.reserved += 65536;
        }
        Ok(())
    }
}
/// Only payload Source cursors need presence checks beyond their existing DTOs.
/// Envelopes use full shape comparison; arbitrary map keys never select a DTO.
#[derive(Clone, Copy)]
enum Location {
    Other,
    SourcePage,
    Cursor,
    Checkpoint,
    Route,
    Original,
}
impl Location {
    fn child(self, field: &str) -> Self {
        match (self, field) {
            (Self::SourcePage, "before" | "after") => Self::Cursor,
            (Self::Cursor, "source") => Self::Checkpoint,
            (Self::Checkpoint, "route") => Self::Route,
            (Self::Route, "original") => Self::Original,
            _ => Self::Other,
        }
    }
    fn required_nullable(self) -> &'static [&'static str] {
        match self {
            Self::Checkpoint => &["after", "row"],
            Self::Route => &["original"],
            Self::Original => &["secret_ref"],
            _ => &[],
        }
    }
}
struct Node<'a, 'b> {
    scan: &'a mut Scan<'b>,
    depth: usize,
    location: Location,
}
impl<'de> DeserializeSeed<'de> for Node<'_, '_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> std::result::Result<(), D::Error> {
        if self.depth > 64 {
            return Err(de::Error::custom("JSON depth"));
        }
        self.scan.budget.check().map_err(de::Error::custom)?;
        d.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Node<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded progress JSON")
    }
    fn visit_bool<E: de::Error>(self, _: bool) -> std::result::Result<(), E> {
        self.scan.add(if self.scan.envelope { 128 } else { 1 })
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> std::result::Result<(), E> {
        self.scan.add(if self.scan.envelope { 128 } else { 16 })
    }
    fn visit_u64<E: de::Error>(self, n: u64) -> std::result::Result<(), E> {
        self.scan.add(if self.scan.envelope {
            128
        } else if n <= 255 {
            2
        } else {
            16
        })
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> std::result::Result<(), E> {
        self.scan.add(if self.scan.envelope { 128 } else { 16 })
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<(), E> {
        self.scan.add(if self.scan.envelope { 128 } else { 32 })
    }
    fn visit_str<E: de::Error>(self, s: &str) -> std::result::Result<(), E> {
        self.scan.add(if self.scan.envelope {
            384 + 6 * s.len()
        } else {
            128 + 2 * s.len()
        })
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<(), A::Error> {
        self.scan.add(128)?;
        while seq
            .next_element_seed(Node {
                scan: self.scan,
                depth: self.depth + 1,
                location: Location::Other,
            })?
            .is_some()
        {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
        self.scan.add(128)?;
        let mut keys = BTreeSet::new();
        while let Some(key) = map.next_key::<String>()? {
            self.scan.add(256 + 2 * key.len())?;
            let location = self.location.child(&key);
            if !keys.insert(key) {
                return Err(de::Error::custom("duplicate field"));
            }
            map.next_value_seed(Node {
                scan: self.scan,
                depth: self.depth + 1,
                location,
            })?;
        }
        for field in self.location.required_nullable() {
            if !keys.contains(*field) {
                return Err(de::Error::missing_field(field));
            }
        }
        Ok(())
    }
}

pub(crate) fn preflight(raw: &[u8], budget: &mut Budget) -> Result<Vec<ResidentGuard>> {
    scan(raw, budget, false)
}
fn scan(raw: &[u8], budget: &mut Budget, envelope: bool) -> Result<Vec<ResidentGuard>> {
    let mut scan = Scan {
        budget,
        guards: Vec::new(),
        reserved: 0,
        used: 0,
        envelope,
    };
    let mut d = serde_json::Deserializer::from_slice(raw);
    let location = if envelope {
        Location::Other
    } else {
        Location::SourcePage
    };
    Node {
        scan: &mut scan,
        depth: 0,
        location,
    }
    .deserialize(&mut d)
    .map_err(|_| SafeError::InvalidInput)?;
    d.end().map_err(|_| SafeError::InvalidInput)?;
    scan.budget.check().map_err(storage)?;
    Ok(scan.guards)
}

/// Envelopes alone are small enough for shape comparison. This makes missing
/// nullable fields distinct from explicit null, including nested business DTOs.
pub(crate) fn envelope<T: DeserializeOwned + Serialize>(
    raw: &[u8],
    budget: &mut Budget,
) -> Result<(T, Vec<ResidentGuard>)> {
    let guards = scan(raw, budget, true)?;
    let _scratch = budget
        .reserve(raw.len().checked_mul(12).ok_or(SafeError::LimitExceeded)?)
        .map_err(storage)?;
    budget
        .charge(0, 0, 0, (raw.len() * 3) as u64)
        .map_err(storage)?;
    let value: T = typed(raw, budget)?;
    let original: serde_json::Value =
        serde_json::from_slice(raw).map_err(|_| SafeError::InvalidInput)?;
    let encoded = serde_json::to_value(&value).map_err(|_| SafeError::InvalidInput)?;
    fn shape(a: &serde_json::Value, b: &serde_json::Value) -> bool {
        match (a, b) {
            (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
                a.len() == b.len() && a.iter().all(|(k, a)| b.get(k).is_some_and(|b| shape(a, b)))
            }
            (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| shape(a, b))
            }
            _ => true,
        }
    }
    if !shape(&original, &encoded) {
        return Err(SafeError::InvalidInput);
    }
    budget.check().map_err(storage)?;
    Ok((value, guards))
}

pub(crate) fn typed<T: DeserializeOwned>(raw: &[u8], budget: &mut Budget) -> Result<T> {
    struct Checked<'a> {
        raw: &'a [u8],
        budget: &'a mut Budget,
        since: usize,
    }
    impl Read for Checked<'_> {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if self.since >= 4096 || self.raw.is_empty() {
                self.budget
                    .check()
                    .map_err(|_| std::io::Error::other("progress budget"))?;
                self.since = 0;
            }
            let n = self.raw.read(out)?;
            self.since += n;
            Ok(n)
        }
    }
    budget.check().map_err(storage)?;
    let value = serde_json::from_reader(Checked {
        raw,
        budget: &mut *budget,
        since: 0,
    })
    .map_err(|_| SafeError::InvalidInput)?;
    budget.check().map_err(storage)?;
    Ok(value)
}

/// Reads exactly one concatenated JSON object/array. No complete archive buffer,
/// no synthetic EOF: the next call must observe actual EOF or another full value.
pub(crate) fn next<R: Read>(input: &mut R, raw: &mut Vec<u8>, budget: &mut Budget) -> Result<bool> {
    raw.clear();
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escape = false;
    loop {
        let mut byte = [0];
        budget.check().map_err(storage)?;
        let n = input.read(&mut byte).map_err(|_| SafeError::InvalidInput)?;
        if n == 0 {
            return if raw.is_empty() {
                Ok(false)
            } else {
                Err(SafeError::InvalidInput)
            };
        }
        budget.charge(0, 0, 0, 1).map_err(storage)?;
        let b = byte[0];
        if raw.is_empty() && matches!(b, b' ' | b'\n' | b'\r' | b'\t') {
            continue;
        }
        if raw.is_empty() && b != b'{' && b != b'[' {
            return Err(SafeError::InvalidInput);
        }
        if raw.len() == 8 * 1024 * 1024 {
            return Err(SafeError::LimitExceeded);
        }
        raw.push(b);
        if quoted {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                quoted = false;
            }
        } else {
            match b {
                b'"' => quoted = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > 64 {
                        return Err(SafeError::LimitExceeded);
                    }
                }
                b'}' | b']' => {
                    depth = depth.checked_sub(1).ok_or(SafeError::InvalidInput)?;
                    if depth == 0 {
                        return Ok(true);
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::pinning::management::native::meter::Limits;
    use std::sync::atomic::Ordering;

    #[test]
    fn full_byte_fragment_is_typed_without_a_wire_frame_node_limit() {
        let expected = vec![255u8; 65536];
        let raw = serde_json::to_vec(&expected).unwrap();
        let mut budget = Budget::default();
        let _guard = preflight(&raw, &mut budget).unwrap();
        assert_eq!(typed::<Vec<u8>>(&raw, &mut budget).unwrap(), expected);
    }

    #[test]
    fn nested_duplicate_fields_and_omitted_nullable_fields_are_rejected() {
        let mut budget = Budget::default();
        assert!(preflight(br#"{"limits":{"a":1,"a":2}}"#, &mut budget).is_err());
        #[derive(Serialize, serde::Deserialize)]
        struct Nullable {
            value: Option<String>,
        }
        assert!(envelope::<Nullable>(b"{}", &mut budget).is_err());
        assert!(envelope::<Nullable>(br#"{"value":null}"#, &mut budget).is_ok());
    }

    #[test]
    fn provider_limit_names_are_not_checkpoint_field_discriminators() {
        let raw =
            br#"{"catalog_revision":{"priority":1,"max_bytes":0,"max_pins":0,"enabled":true}}"#;
        let mut budget = Budget::default();
        let (limits, _guard) =
            envelope::<crate::pinning::config::ProviderLimitMap>(raw, &mut budget).unwrap();
        assert!(limits["catalog_revision"].enabled);
    }

    #[test]
    fn sequential_values_require_real_eof_and_reject_truncated_or_trailing_data() {
        let mut raw = Vec::new();
        let mut budget = Budget::default();
        let mut input = &b"{}[] \n"[..];
        assert!(next(&mut input, &mut raw, &mut budget).unwrap());
        assert!(next(&mut input, &mut raw, &mut budget).unwrap());
        assert!(!next(&mut input, &mut raw, &mut budget).unwrap());
        assert!(next(&mut &b"[1"[..], &mut raw, &mut budget).is_err());
        assert!(next(&mut &b"garbage"[..], &mut raw, &mut budget).is_err());
    }

    #[test]
    fn parser_obeys_retained_work_resident_and_cancellation_limits() {
        let mut budget = Budget::new(Limits {
            bytes: 1,
            ..Default::default()
        });
        assert!(preflight(b"[1,2,3]", &mut budget).is_err());
        let mut budget = Budget::new(Limits {
            resident: 1,
            ..Default::default()
        });
        assert!(preflight(b"[]", &mut budget).is_err());
        let mut budget = Budget::default();
        budget.cancellation().store(true, Ordering::Relaxed);
        assert!(typed::<Vec<u8>>(b"[1]", &mut budget).is_err());
        assert!(next(&mut &b"[]"[..], &mut Vec::new(), &mut budget).is_err());
    }
}

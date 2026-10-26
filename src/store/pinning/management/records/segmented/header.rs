//! Allocation bounds for the string fields and string vectors in an archive header.
use super::*;
use native::meter::ResidentGuard;
use serde::de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor};
use std::{fmt, mem::size_of};

pub(crate) struct DecodedHeader {
    pub(crate) value: ArchiveHeader,
    pub(crate) memory: ResidentGuard,
}

impl DecodedHeader {
    pub(crate) fn decode(raw: &str, budget: &mut Budget) -> Result<Self> {
        if raw.len() > PAGE_BYTES {
            return Err(ManagementError::Budget);
        }
        // JSON unescaping may grow a temporary byte buffer while the raw input
        // remains owned by the caller. No header collections exist in preflight.
        let _scratch = budget.reserve(raw.len().checked_mul(2).ok_or(ManagementError::Budget)?)?;
        let mut decoder = serde_json::Deserializer::from_str(raw);
        let slots = serde::de::Deserializer::deserialize_any(&mut decoder, HeaderSize)
            .and_then(|slots| decoder.end().map(|()| slots))
            .map_err(|_| ManagementError::InvalidEvidence)?;
        drop(decoder);
        let vectors = slots
            .checked_mul(size_of::<String>())
            .ok_or(ManagementError::Budget)?;
        let retained = raw
            .len()
            .checked_add(vectors)
            .and_then(|bytes| bytes.checked_add(size_of::<ArchiveHeader>()))
            .ok_or(ManagementError::Budget)?;
        let memory = budget.reserve(retained)?;
        // A Vec reallocation can temporarily retain its old buffer as well.
        let _growth = budget.reserve(vectors / 2)?;
        let value = serde_json::from_str(raw).map_err(|_| ManagementError::InvalidEvidence)?;
        Ok(Self { value, memory })
    }
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum Field {
    DatabaseScope,
    DatabaseIdentity,
    Principal,
    Catalog,
    Request,
    Configuration,
    Acknowledgements,
    CreatedAt,
    ExpiresAt,
}

struct HeaderSize;
impl<'de> Visitor<'de> for HeaderSize {
    type Value = usize;
    fn expecting(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("an archive header")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<usize, A::Error> {
        let mut slots = 0usize;
        while let Some(field) = map.next_key::<Field>()? {
            match field {
                Field::Catalog | Field::Acknowledgements => {
                    let next = map.next_value_seed(StringSlots)?;
                    slots = slots
                        .checked_add(next)
                        .ok_or_else(|| de::Error::custom("header allocation limit"))?;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(slots)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<usize, A::Error> {
        let (mut field, mut slots) = (0usize, 0usize);
        loop {
            let next = if matches!(field, 3 | 6) {
                sequence.next_element_seed(StringSlots)?
            } else {
                sequence.next_element::<IgnoredAny>()?.map(|_| 0)
            };
            let Some(next) = next else { return Ok(slots) };
            slots = slots
                .checked_add(next)
                .ok_or_else(|| de::Error::custom("header allocation limit"))?;
            field = field
                .checked_add(1)
                .ok_or_else(|| de::Error::custom("header allocation limit"))?;
        }
    }
}

struct StringSlots;
impl<'de> de::DeserializeSeed<'de> for StringSlots {
    type Value = usize;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<usize, D::Error> {
        decoder.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for StringSlots {
    type Value = usize;
    fn expecting(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("an archive header string sequence")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<usize, A::Error> {
        let mut count = 0usize;
        while sequence.next_element::<IgnoredAny>()?.is_some() {
            count = count
                .checked_add(1)
                .ok_or_else(|| de::Error::custom("header allocation limit"))?;
        }
        if count == 0 {
            return Ok(0);
        }
        count
            .checked_mul(2)
            .map(|slots| slots.max(4))
            .ok_or_else(|| de::Error::custom("header allocation limit"))
    }
}

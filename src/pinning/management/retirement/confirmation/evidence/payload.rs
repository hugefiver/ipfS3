use super::*;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use std::fmt;
use super::super::super::{ArchivePart, source::attest::Cursor};

pub(super) struct Page {
    pub before: Cursor,
    pub after: Cursor,
    parts: Vec<Part>,
    pub members: Vec<NativeKey>,
    pub eof: Vec<String>,
}

pub(super) struct PartView<'a> {
    pub table: &'a str,
    pub key: &'a NativeKey,
    pub column: &'a str,
    pub offset: u64,
    pub length: Option<u64>,
    pub bytes: &'a [u8],
}
impl Page {
    pub(super) fn parts(&self) -> impl Iterator<Item = PartView<'_>> {
        self.parts.iter().flat_map(|part| -> Box<dyn Iterator<Item = PartView<'_>> + '_> {
            match part {
                Part::Flat(p) => Box::new(std::iter::once(PartView { table: &p.table, key: &p.key, column: &p.column, offset: p.offset, length: p.length, bytes: &p.bytes })),
                Part::Group(table,key,cells) => Box::new(cells.iter().map(move |(column,offset,length,bytes)| PartView { table, key, column, offset:*offset, length:*length, bytes })),
            }
        })
    }
}

#[derive(Default)]
struct Format(Option<String>);
impl<'de> Deserialize<'de> for Format {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self,D::Error> {
        String::deserialize(d).map(|s| Self(Some(s)))
    }
}

enum Part {
    Flat(ArchivePart),
    Group(String, NativeKey, Vec<(String, u64, Option<u64>, Vec<u8>)>),
}
impl<'de> Deserialize<'de> for Part {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Part;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("source part or grouped row") }
            fn visit_map<A: MapAccess<'de>>(self, map: A) -> std::result::Result<Part, A::Error> {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Flat {
                    table: String, key: NativeKey, column: String, offset: u64,
                    #[serde(deserialize_with = "Option::deserialize")]
                    length: Option<u64>,
                    bytes: Vec<u8>,
                }
                let p = Flat::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(Part::Flat(ArchivePart { table:p.table, key:p.key, column:p.column, offset:p.offset, length:p.length, bytes:p.bytes }))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> std::result::Result<Part, A::Error> {
                let (table, key, cells) = <(String,NativeKey,Vec<(String,u64,Option<u64>,Vec<u8>)>)>::deserialize(de::value::SeqAccessDeserializer::new(seq))?;
                Ok(Part::Group(table,key,cells))
            }
        }
        d.deserialize_any(V)
    }
}

impl<'de> Deserialize<'de> for Page {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields { #[serde(default)] format: Format, before: Cursor, after: Cursor, parts: Vec<Part>, members: Vec<NativeKey>, eof: Vec<String> }
        let f = Fields::deserialize(d)?;
        let grouped = match f.format.0.as_deref() { None => false, Some("S75-source-rows-1") => true, _ => return Err(de::Error::custom("source format")) };
        for part in &f.parts {
            match part {
                Part::Flat(_) if !grouped => {},
                Part::Group(_,_,cells) if grouped && !cells.is_empty() => {},
                _ => return Err(de::Error::custom("source representation")),
            }
        }
        Ok(Page {before:f.before,after:f.after,parts:f.parts,members:f.members,eof:f.eof})
    }
}

pub(super) enum PayloadRow {
    Wire(NativeKey),
    Comparison { key: Vec<String>, cells: Vec<Option<String>> },
}
impl<'de> Deserialize<'de> for PayloadRow {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = PayloadRow;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("wire key or comparison row") }
            fn visit_map<A: MapAccess<'de>>(self, mut m: A) -> std::result::Result<Self::Value,A::Error> {
                let (mut table,mut values,mut key,mut cells) = (None,None,None,None);
                while let Some(field) = m.next_key::<String>()? {
                    match field.as_str() {
                        "table" if table.is_none() => table = Some(m.next_value()?),
                        "values" if values.is_none() => values = Some(m.next_value()?),
                        "key" if key.is_none() => key = Some(m.next_value()?),
                        "cells" if cells.is_none() => cells = Some(m.next_value()?),
                        _ => return Err(de::Error::custom("payload field")),
                    }
                }
                match (table,values,key,cells) {
                    (Some(table),Some(values),None,None) => Ok(PayloadRow::Wire(NativeKey{table,values})),
                    (None,None,Some(key),Some(cells)) => Ok(PayloadRow::Comparison{key,cells}),
                    _ => Err(de::Error::custom("payload fields")),
                }
            }
        }
        d.deserialize_map(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::TransactionTrait;

    #[tokio::test]
    async fn flat_and_grouped_source_views_preserve_null_empty_and_private_bytes() {
        let fixture = crate::store::pinning::management::actions::tests::fixture("confirmation-history-shapes").await;
        let tx = fixture.db.begin().await.unwrap();
        let source = SourceCheckpoint::start(&tx, crate::pinning::management::RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot())).await.unwrap();
        tx.rollback().await.unwrap();
        let cursor = Cursor::new(source);
        let key = NativeKey {table:"pin_resource_history".into(),values:vec!["provider".into(),"秘密🔒".into(),"1".into()]};
        let parts = vec![
            ArchivePart {table:key.table.clone(),key:key.clone(),column:"ledger".into(),offset:0,length:None,bytes:vec![]},
            ArchivePart {table:key.table.clone(),key:key.clone(),column:"ledger".into(),offset:0,length:Some(0),bytes:vec![]},
            ArchivePart {table:key.table.clone(),key:key.clone(),column:"ledger".into(),offset:65535,length:Some(65538),bytes:"秘".as_bytes().to_vec()},
        ];
        let flat = serde_json::json!({"before":cursor,"after":cursor,"parts":parts,"members":[],"eof":[]});
        let mut grouped = flat.clone();
        grouped["format"] = "S75-source-rows-1".into();
        grouped["parts"] = serde_json::json!([[key.table,key,parts.iter().map(|part| (&part.column,part.offset,part.length,&part.bytes)).collect::<Vec<_>>()]]);
        for value in [&flat,&grouped] {
            let raw = serde_json::to_vec(value).unwrap();
            let mut budget = Budget::default();
            let _guard = json::preflight(&raw,&mut budget).unwrap();
            let decoded: Page = json::typed(&raw,&mut budget).unwrap();
            for (actual,expected) in decoded.parts().zip(&parts) {
                assert_eq!(actual.key,&expected.key);
                assert_eq!(actual.offset,expected.offset);
                assert_eq!(actual.length,expected.length);
                assert_eq!(actual.bytes,expected.bytes);
            }
        }
        let mut mixed = flat.clone();
        mixed["format"] = "S75-source-rows-1".into();
        assert!(serde_json::from_value::<Page>(mixed).is_err());
        let mut missing = flat;
        assert!(missing["parts"][0].as_object_mut().unwrap().remove("length").is_some());
        assert!(serde_json::from_slice::<Page>(&serde_json::to_vec(&missing).unwrap()).is_err());
        assert!(serde_json::from_value::<Page>(missing).is_err());
        grouped["format"] = "unknown".into();
        assert!(serde_json::from_value::<Page>(grouped).is_err());
    }
}

//! Item-local immutable SQL facts and the same-plan latest-output cursor.
use super::*;
use records::{RowHandle, RowView};
use serde::ser::{SerializeStruct, SerializeTuple};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct Key(pub String, pub Vec<String>);
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Input {
    Absent,
    Manifest,
    Output(usize, usize, String),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RowRef {
    Read(usize),
    Output(usize),
}
impl Serialize for Input {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let mut t = s.serialize_tuple(if matches!(self, Self::Output(..)) {
            4
        } else {
            1
        })?;
        match self {
            Self::Absent => t.serialize_element(&0u8)?,
            Self::Manifest => t.serialize_element(&1u8)?,
            Self::Output(p, o, h) => {
                t.serialize_element(&2u8)?;
                t.serialize_element(p)?;
                t.serialize_element(o)?;
                t.serialize_element(h)?;
            }
        }
        t.end()
    }
}
impl<'de> Deserialize<'de> for Input {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Input;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an exact input tuple")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Input, A::Error> {
                let tag: u8 = next(&mut a)?;
                let input = match tag {
                    0 => Input::Absent,
                    1 => Input::Manifest,
                    2 => Input::Output(next(&mut a)?, next(&mut a)?, next(&mut a)?),
                    _ => return Err(serde::de::Error::custom("input tag")),
                };
                end(&mut a)?;
                Ok(input)
            }
        }
        d.deserialize_seq(V)
    }
}
fn next<'de, A: serde::de::SeqAccess<'de>, T: Deserialize<'de>>(
    a: &mut A,
) -> std::result::Result<T, A::Error> {
    a.next_element()?
        .ok_or_else(|| serde::de::Error::custom("missing tuple field"))
}
fn end<'de, A: serde::de::SeqAccess<'de>>(a: &mut A) -> std::result::Result<(), A::Error> {
    if a.next_element::<serde::de::IgnoredAny>()?.is_some() {
        return Err(serde::de::Error::custom("extra tuple field"));
    }
    Ok(())
}
impl Serialize for RowRef {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Read(i) => (0u8, i).serialize(s),
            Self::Output(i) => (1u8, i).serialize(s),
        }
    }
}
impl<'de> Deserialize<'de> for RowRef {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let (tag, i) = <(u8, usize)>::deserialize(d)?;
        match tag {
            0 => Ok(Self::Read(i)),
            1 => Ok(Self::Output(i)),
            _ => Err(serde::de::Error::custom("row reference tag")),
        }
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Read(pub Key, pub Input);
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Output(pub Key, pub usize, pub Option<Vec<Option<String>>>);
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Birth(pub usize, pub usize);
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JobLink(pub usize, pub RowRef, pub RowRef);
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum Selector {
    Migration(String, String, String),
    Submit([String; 1]),
    Reservation(String, String, String, i64),
    Legacy(String, Vec<String>, String),
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct Dependency(pub String, pub Key, pub String, pub Selector);

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Allocate(RowRef, RowRef, RowRef, Option<RowRef>, Vec<RowRef>),
    Renew(RowRef, RowRef, Option<RowRef>, Vec<RowRef>, bool),
    Recover(RowRef, RowRef, Vec<(RowRef, Option<RowRef>)>),
    Migrate(RowRef, RowRef, RowRef, Option<RowRef>),
    Cancel(RowRef, RowRef),
    Retire(RowRef, Vec<RowRef>),
}
impl Serialize for Outcome {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Allocate(a, b, c, d, e) => ("allocate", a, b, c, d, e).serialize(s),
            Self::Renew(a, b, c, d, e) => ("renew", a, b, c, d, e).serialize(s),
            Self::Recover(a, b, c) => ("recover", a, b, c).serialize(s),
            Self::Migrate(a, b, c, d) => ("migrate", a, b, c, d).serialize(s),
            Self::Cancel(a, b) => ("cancel", a, b).serialize(s),
            Self::Retire(a, b) => ("retire", a, b).serialize(s),
        }
    }
}
impl<'de> Deserialize<'de> for Outcome {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Outcome;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a closed action outcome")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Outcome, A::Error> {
                let tag: String = next(&mut a)?;
                let o = match tag.as_str() {
                    "allocate" => Outcome::Allocate(
                        next(&mut a)?,
                        next(&mut a)?,
                        next(&mut a)?,
                        next(&mut a)?,
                        next(&mut a)?,
                    ),
                    "renew" => Outcome::Renew(
                        next(&mut a)?,
                        next(&mut a)?,
                        next(&mut a)?,
                        next(&mut a)?,
                        next(&mut a)?,
                    ),
                    "recover" => Outcome::Recover(next(&mut a)?, next(&mut a)?, next(&mut a)?),
                    "migrate" => {
                        Outcome::Migrate(next(&mut a)?, next(&mut a)?, next(&mut a)?, next(&mut a)?)
                    }
                    "cancel" => Outcome::Cancel(next(&mut a)?, next(&mut a)?),
                    "retire" => Outcome::Retire(next(&mut a)?, next(&mut a)?),
                    _ => return Err(serde::de::Error::custom("outcome tag")),
                };
                end(&mut a)?;
                Ok(o)
            }
        }
        d.deserialize_seq(V)
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ItemFacts(
    pub usize,
    pub Vec<Read>,
    pub Vec<Output>,
    pub Vec<Birth>,
    pub Vec<JobLink>,
    pub Outcome,
    pub Vec<Dependency>,
);

struct WithoutState<'a>(&'a records::CommittedAction);
impl Serialize for WithoutState<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let a = self.0;
        let mut s = s.serialize_struct(
            "CommittedAction",
            if a.root_releases.is_empty() { 5 } else { 6 },
        )?;
        s.serialize_field("receipt", &a.receipt)?;
        s.serialize_field("targets", &a.targets)?;
        if !a.root_releases.is_empty() {
            s.serialize_field("root_releases", &a.root_releases)?;
        }
        s.serialize_field("jobs", &a.jobs)?;
        s.serialize_field("migration_points", &a.migration_points)?;
        s.serialize_field("decision", &a.decision)?;
        s.end()
    }
}
pub(crate) fn identity(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    facts: &ItemFacts,
) -> Result<String> {
    use sha2::{Digest, Sha256};
    struct W(Sha256);
    impl std::io::Write for W {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.update(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut w = W(Sha256::new());
    w.0.update(b"stage7/facts/1\0");
    serde_json::to_writer(
        &mut w,
        &(
            binding.database_scope,
            binding.database_identity,
            binding.principal,
            &binding.request.plan_id,
            &action.receipt.binding.request.item_id,
            WithoutState(action),
            facts,
        ),
    )
    .map_err(|_| ManagementError::InvalidEvidence)?;
    binding.copy(&"0000000000000000000000000000000000000000000000000000000000000000")?;
    Ok(hex::encode(w.0.finalize()))
}

type ProducedRow = (usize, usize, String, Option<Arc<RowHandle>>);
#[derive(Default)]
pub(crate) struct Cursor(BTreeMap<Key, ProducedRow>);
impl Cursor {
    fn matches_input(&self, key: &Key, input: &Input, rows: records::FrozenRows<'_>) -> bool {
        if let Some((ordinal, slot, identity, _)) = self.0.get(key) {
            matches!(input,Input::Output(p,s,h) if p==ordinal&&s==slot&&h==identity)
        } else {
            let exists = rows.row_key(&key.0, &key.1).is_some();
            matches!(
                (exists, input),
                (true, Input::Manifest) | (false, Input::Absent)
            )
        }
    }
    pub(super) fn entries(&self) -> impl Iterator<Item = (&Key, Option<RowView<'_>>)> {
        self.0
            .iter()
            .map(|(key, (_, _, _, row))| (key, row.as_deref().map(RowView::Handle)))
    }
    pub(crate) fn input<'a>(
        &self,
        key: &Key,
        frame: impl Into<records::FrozenRows<'a>>,
        binding: records::BindingView<'_>,
    ) -> Result<Input> {
        if let Some((p, s, h, _)) = self.0.get(key) {
            binding.copy(&(2u8, p, s, h))?;
            Ok(Input::Output(*p, *s, h.clone()))
        } else {
            binding.copy(&[0u8])?;
            Ok(if frame.into().row_key(&key.0, &key.1).is_some() {
                Input::Manifest
            } else {
                Input::Absent
            })
        }
    }
    pub(crate) fn publish(
        &mut self,
        operation: &OperationRead,
        hash: &str,
        binding: records::BindingView<'_>,
    ) -> Result<()> {
        for (slot, (output, row)) in operation.facts.2.iter().zip(&operation.outputs).enumerate() {
            binding.copy(&(&output.0, hash))?;
            binding.with_decode_work(|work| work.nodes(2))?;
            self.0.insert(
                output.0.clone(),
                (operation.facts.0, slot, hash.into(), row.clone()),
            );
        }
        Ok(())
    }
}

/// Validated coordinates over shared original inputs and item outputs, not a
/// live worker permission.
pub(crate) struct OperationRead {
    pub(crate) facts: Arc<ItemFacts>,
    reads: Vec<Option<Arc<RowHandle>>>,
    outputs: Vec<Option<Arc<RowHandle>>>,
}
impl OperationRead {
    pub(super) fn output_rows(&self) -> impl Iterator<Item = (&Key, Option<RowView<'_>>)> {
        self.facts
            .2
            .iter()
            .zip(&self.outputs)
            .map(|(output, row)| (&output.0, row.as_deref().map(RowView::Handle)))
    }
    pub(crate) fn resolve(
        binding: records::BindingView<'_>,
        facts: Arc<ItemFacts>,
        cursor: &Cursor,
    ) -> Result<Self> {
        let frame = binding.frozen_rows()?;
        if facts.1.windows(2).any(|p| p[0].0 >= p[1].0)
            || facts.2.windows(2).any(|p| p[0].0 >= p[1].0)
        {
            return Err(ManagementError::Confirmation);
        }
        binding.with_decode_work(|work| work.nodes(2 + facts.1.len() + facts.2.len()))?;
        let mut reads = Vec::new();
        for Read(key, input) in &facts.1 {
            binding.with_decode_work(|work| work.nodes(1 + key.1.len()))?;
            if !cursor.matches_input(key, input, frame) {
                return Err(ManagementError::Confirmation);
            }
            let table = frame.table(&key.0).ok_or(ManagementError::Schema)?;
            if key.1.len() != table.primary_key.len() {
                return Err(ManagementError::Confirmation);
            }
            let row = if let Some((_, _, _, row)) = cursor.0.get(key) {
                row.clone()
            } else if let Some(rows) = binding.rows {
                rows.row(&key.0, &key.1).cloned()
            } else {
                let refs = key.1.iter().map(String::as_str).collect::<Vec<_>>();
                if let Some(row) = frame.row(&key.0, &refs) {
                    binding.copy(&row)?;
                    binding.with_decode_work(|work| work.nodes(2))?;
                    Some(Arc::new(RowHandle::Sql(Arc::new(
                        row.iter()
                            .map(|(column, value)| (column.clone(), value.clone()))
                            .collect(),
                    ))))
                } else {
                    None
                }
            };
            reads.push(row);
        }
        let mut outputs = Vec::new();
        for (output_slot, Output(key, slot, cells)) in facts.2.iter().enumerate() {
            if facts.1.get(*slot).is_none_or(|read| read.0 != *key) {
                return Err(ManagementError::Confirmation);
            }
            let table = frame.table(&key.0).ok_or(ManagementError::Schema)?;
            let row = if let Some(cells) = cells {
                if cells.len() != table.columns.len()
                    || table.primary_key.iter().zip(&key.1).any(|(column, value)| {
                        table
                            .columns
                            .iter()
                            .position(|c| c == column)
                            .and_then(|i| cells[i].as_deref())
                            != Some(value)
                    })
                {
                    return Err(ManagementError::Confirmation);
                }
                let layout = native::current_catalog()
                    .iter()
                    .find(|layout| {
                        layout.table == key.0
                            && layout.columns.as_slice() == table.columns.as_slice()
                    })
                    .ok_or(ManagementError::Schema)?;
                binding.with_decode_work(|work| work.nodes(2))?;
                Some(Arc::new(RowHandle::Output {
                    table: layout,
                    facts: facts.clone(),
                    slot: output_slot,
                }))
            } else {
                None
            };
            if reads[*slot].as_deref() == row.as_deref() {
                return Err(ManagementError::Confirmation);
            }
            outputs.push(row);
        }
        Ok(Self {
            facts,
            reads,
            outputs,
        })
    }
    pub(crate) fn row(&self, r: RowRef) -> Result<&RowHandle> {
        self.optional(r)?.ok_or(ManagementError::Confirmation)
    }
    pub(crate) fn optional(&self, r: RowRef) -> Result<Option<&RowHandle>> {
        match r {
            RowRef::Read(i) => self.reads.get(i),
            RowRef::Output(i) => self.outputs.get(i),
        }
        .map(|r| r.as_deref())
        .ok_or(ManagementError::Confirmation)
    }
    pub(crate) fn key(&self, r: RowRef) -> Result<&Key> {
        match r {
            RowRef::Read(i) => self.facts.1.get(i).map(|r| &r.0),
            RowRef::Output(i) => self.facts.2.get(i).map(|r| &r.0),
        }
        .ok_or(ManagementError::Confirmation)
    }
    pub(crate) fn before(&self, key: &Key) -> Result<Option<&RowHandle>> {
        let i = self
            .facts
            .1
            .binary_search_by(|r| r.0.cmp(key))
            .map_err(|_| ManagementError::Confirmation)?;
        self.optional(RowRef::Read(i))
    }
    pub(crate) fn after(&self, key: &Key) -> Result<Option<&RowHandle>> {
        if let Ok(i) = self.facts.2.binary_search_by(|r| r.0.cmp(key)) {
            self.optional(RowRef::Output(i))
        } else {
            self.before(key)
        }
    }
    pub(crate) fn outcome(&self) -> &Outcome {
        &self.facts.5
    }
}

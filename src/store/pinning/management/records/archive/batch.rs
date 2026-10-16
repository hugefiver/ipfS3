use super::*;
use std::sync::Arc;

/// Pure data owned by the enclosing decode budget. No successful validation is
/// retained: each consumer rechecks coordinates, membership and section identity.
#[derive(Default)]
pub(in crate::store::pinning::management::records) struct ReadBatch {
    pub(super) reviews: Vec<Arc<Review>>,
    rows: Vec<Arc<native::NativeRow>>,
    sql_rows: Vec<Arc<native::NativeRow>>,
    sources: Vec<Arc<StoredSource>>,
    cores: Vec<(Arc<StoredSource>, Arc<Core>)>,
    manifests: Vec<(Arc<StoredSource>, Arc<Manifest>)>,
    leaves: Vec<(Arc<StoredSource>, usize, Arc<Leaf>)>,
    locators: Vec<(Arc<native::NativeRow>, Arc<Locator>)>,
    bindings: Vec<(Arc<native::NativeRow>, Arc<Binding>)>,
}

pub(in crate::store::pinning::management::records) fn retain_row(
    row: native::NativeRow,
    work: &mut DecodeBudget,
) -> Result<Arc<native::NativeRow>> {
    work.check()?;
    if let Some(cached) = work
        .archive
        .rows
        .iter()
        .find(|cached| cached.as_ref() == &row)
    {
        return Ok(cached.clone());
    }
    work.nodes(2)?;
    let row = Arc::new(row);
    work.archive.rows.push(row.clone());
    Ok(row)
}
pub(in crate::store::pinning::management::records) fn retain_row_ref(
    row: &native::NativeRow,
    work: &mut DecodeBudget,
) -> Result<Arc<native::NativeRow>> {
    work.check()?;
    if let Some(cached) = work
        .archive
        .rows
        .iter()
        .find(|cached| cached.as_ref() == row)
    {
        return Ok(cached.clone());
    }
    work.copy(row)?;
    retain_row(row.clone(), work)
}
pub(in crate::store::pinning::management::records) fn sql_candidates(
    scope: &str,
    plan: &str,
    work: &mut DecodeBudget,
) -> Result<Vec<Arc<native::NativeRow>>> {
    let count = work
        .archive
        .sql_rows
        .iter()
        .filter(|row| {
            native::value(row, "database_scope") == Some(scope)
                && native::value(row, "plan_id") == Some(plan)
        })
        .count();
    work.nodes(1 + count)?;
    let mut candidates = Vec::with_capacity(count);
    for row in &work.archive.sql_rows {
        if native::value(row, "database_scope") != Some(scope)
            || native::value(row, "plan_id") != Some(plan)
        {
            continue;
        }
        candidates.push(row.clone());
    }
    Ok(candidates)
}
pub(in crate::store::pinning::management::records) fn remember_sql_row(
    row: Arc<native::NativeRow>,
    work: &mut DecodeBudget,
) -> Result<()> {
    if let Some(existing) = work.archive.sql_rows.iter_mut().find(|old| {
        ["database_scope", "plan_id", "item_id", "kind"]
            .iter()
            .all(|column| old.get(*column) == row.get(*column))
    }) {
        *existing = row;
        return Ok(());
    }
    work.nodes(1)?;
    work.archive.sql_rows.push(row);
    Ok(())
}
pub(in crate::store::pinning::management::records) fn sql_rows(
    work: &mut DecodeBudget,
) -> Result<Vec<Arc<native::NativeRow>>> {
    work.nodes(1 + work.archive.sql_rows.len())?;
    Ok(work.archive.sql_rows.clone())
}
pub(in crate::store::pinning::management::records) fn retain_view(
    row: RowView<'_>,
    work: &mut DecodeBudget,
) -> Result<Arc<native::NativeRow>> {
    work.check()?;
    if let Some(cached) = work
        .archive
        .rows
        .iter()
        .find(|cached| row == *cached.as_ref())
    {
        return Ok(cached.clone());
    }
    if let RowView::Handle(RowHandle::Sql(row)) = row {
        work.nodes(1)?;
        work.archive.rows.push(row.clone());
        return Ok(row.clone());
    }
    work.copy(&row)?;
    retain_row(
        row.iter()
            .map(|(column, value)| (column.clone(), value.clone()))
            .collect(),
        work,
    )
}

pub(in crate::store::pinning::management::records) fn frame_row(
    frame: &NativeFrame,
    key: &native::NativeKey,
    work: &mut DecodeBudget,
) -> Result<Arc<native::NativeRow>> {
    retain_view(
        FrozenRows::Frame(frame)
            .row_key(&key.table, &key.values)
            .ok_or(ManagementError::InvalidEvidence)?,
        work,
    )
}

pub(super) struct StoredSource {
    row: Arc<native::NativeRow>,
    pub sections: Sections,
    pub head: Head,
}
impl StoredSource {
    pub fn raw(&self) -> &str {
        native::value(&self.row, "native_before").expect("parsed source has original TEXT")
    }
}
#[derive(Deserialize)]
pub(super) struct Locator(pub Key, pub String, pub String);
#[derive(PartialEq, Eq)]
pub(in crate::store::pinning::management::records) enum Binding {
    Draft(Key, String),
    Confirmed(Key, String, DateTime<Utc>, SharedValue<Vec<String>>),
}
impl<'de> Deserialize<'de> for Binding {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Binding;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a draft or confirmed archive binding")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Binding, A::Error> {
                let tag: u8 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let key = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let hash = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                let binding = match tag {
                    0 => Binding::Draft(key, hash),
                    1 => Binding::Confirmed(
                        key,
                        hash,
                        seq.next_element()?
                            .ok_or_else(|| serde::de::Error::invalid_length(3, &self))?,
                        seq.next_element()?
                            .ok_or_else(|| serde::de::Error::invalid_length(4, &self))?,
                    ),
                    _ => return Err(serde::de::Error::custom("invalid binding state")),
                };
                if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::custom("extra binding field"));
                }
                Ok(binding)
            }
        }
        deserializer.deserialize_seq(Visitor)
    }
}

fn same_cell(left: &native::NativeRow, right: &native::NativeRow, column: &str) -> bool {
    [
        "database_scope",
        "plan_id",
        "item_id",
        "kind",
        "native_identity",
        "principal",
        column,
    ]
    .into_iter()
    .all(|name| left.get(name) == right.get(name))
}
pub(super) fn check_row(
    row: &native::NativeRow,
    key: &Key,
    scope: &str,
    id: &str,
    principal: &str,
) -> Result<()> {
    check_coordinates(row, &key.0, &key.1, scope, id, principal)
}
pub(super) fn check_coordinates(
    row: &native::NativeRow,
    table: &str,
    values: &[String],
    scope: &str,
    id: &str,
    principal: &str,
) -> Result<()> {
    if table != super::super::super::schema::RECORDS
        || values.len() != 4
        || values[0] != scope
        || values[3] != "action"
        || native::value(row, "native_identity") != Some(id)
        || native::value(row, "principal") != Some(principal)
        || !["database_scope", "plan_id", "item_id", "kind"]
            .iter()
            .zip(values)
            .all(|(column, value)| native::value(row, column) == Some(value))
    {
        return Err(ManagementError::Unauthorized);
    }
    Ok(())
}
pub(super) fn source(
    row: Arc<native::NativeRow>,
    key: &Key,
    scope: &str,
    id: &str,
    principal: &str,
    work: &mut DecodeBudget,
) -> Result<Arc<StoredSource>> {
    check_row(&row, key, scope, id, principal)?;
    let cached = work
        .archive
        .sources
        .iter()
        .find(|cached| same_cell(&cached.row, &row, "native_before"))
        .cloned();
    let source = if let Some(cached) = cached {
        cached
    } else {
        let raw = native::value(&row, "native_before").ok_or(ManagementError::InvalidEvidence)?;
        let parsed = Source::open(raw, key, scope, id, principal, work)?;
        work.nodes(2)?;
        let parsed = Arc::new(StoredSource {
            sections: parsed.sections,
            head: parsed.head,
            row,
        });
        work.archive.sources.push(parsed.clone());
        parsed
    };
    let head = &source.head;
    if head.0 != scope
        || head.1 != id
        || head.2 != principal
        || head.3 != *key
        || head.7.len() != source.sections.leaves.len()
    {
        return Err(ManagementError::Unauthorized);
    }
    Ok(source)
}
pub(super) fn core(source: &Arc<StoredSource>, work: &mut DecodeBudget) -> Result<Arc<Core>> {
    let raw = &source.raw()[source.sections.core.clone()];
    if !identity_matches(b"stage7/core/1\0", raw.as_bytes(), &source.head.5) {
        return Err(ManagementError::InvalidEvidence);
    }
    if let Some((_, core)) = work
        .archive
        .cores
        .iter()
        .find(|(cached, _)| Arc::ptr_eq(cached, source))
    {
        return Ok(core.clone());
    }
    work.nodes(5)?;
    let core = Arc::new(work.typed::<Core>(raw)?);
    work.archive.cores.push((source.clone(), core.clone()));
    Ok(core)
}
pub(super) fn manifest(
    source: &Arc<StoredSource>,
    work: &mut DecodeBudget,
) -> Result<Arc<Manifest>> {
    let raw = &source.raw()[source.sections.manifest.clone()];
    if !identity_matches(b"stage7/manifest/1\0", raw.as_bytes(), &source.head.6) {
        return Err(ManagementError::InvalidEvidence);
    }
    if let Some((_, manifest)) = work
        .archive
        .manifests
        .iter()
        .find(|(cached, _)| Arc::ptr_eq(cached, source))
    {
        return Ok(manifest.clone());
    }
    work.nodes(2)?;
    let manifest = Arc::new(work.typed::<Manifest>(raw)?);
    work.archive
        .manifests
        .push((source.clone(), manifest.clone()));
    Ok(manifest)
}
pub(super) fn leaf(
    source: &Arc<StoredSource>,
    version: &RowVersion,
    work: &mut DecodeBudget,
) -> Result<Arc<Leaf>> {
    let range = source
        .sections
        .leaves
        .get(version.2)
        .ok_or(ManagementError::InvalidEvidence)?;
    let raw = &source.raw()[range.clone()];
    if version.1 != source.head.3
        || version.3 != raw.len()
        || source.head.7.get(version.2) != Some(&version.4)
        || !identity_matches(b"stage7/leaf/1\0", raw.as_bytes(), &version.4)
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let leaf = owned_leaf(source, version.2, work)?;
    if leaf.1 != version.0 {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(leaf)
}
pub(super) fn owned_leaf(
    source: &Arc<StoredSource>,
    slot: usize,
    work: &mut DecodeBudget,
) -> Result<Arc<Leaf>> {
    let range = source
        .sections
        .leaves
        .get(slot)
        .ok_or(ManagementError::InvalidEvidence)?;
    let raw = &source.raw()[range.clone()];
    let hash = source
        .head
        .7
        .get(slot)
        .ok_or(ManagementError::InvalidEvidence)?;
    if !identity_matches(b"stage7/leaf/1\0", raw.as_bytes(), hash) {
        return Err(ManagementError::InvalidEvidence);
    }
    let cached = work
        .archive
        .leaves
        .iter()
        .find(|(cached, index, _)| Arc::ptr_eq(cached, source) && *index == slot)
        .map(|(_, _, leaf)| leaf.clone());
    let leaf = if let Some(cached) = cached {
        cached
    } else {
        work.nodes(2)?;
        let leaf = Arc::new(work.typed::<Leaf>(raw)?);
        work.archive
            .leaves
            .push((source.clone(), slot, leaf.clone()));
        leaf
    };
    Ok(leaf)
}
pub(super) fn locator(
    row: Arc<native::NativeRow>,
    work: &mut DecodeBudget,
) -> Result<Arc<Locator>> {
    if let Some((_, parsed)) = work
        .archive
        .locators
        .iter()
        .find(|(cached, _)| same_cell(cached, &row, "native_before"))
    {
        return Ok(parsed.clone());
    }
    let raw = native::value(&row, "native_before")
        .and_then(|raw| raw.strip_prefix(LOCATOR))
        .ok_or(ManagementError::InvalidEvidence)?;
    work.nodes(2)?;
    let locator = Arc::new(work.typed::<Locator>(raw)?);
    work.archive.locators.push((row, locator.clone()));
    Ok(locator)
}
pub(super) fn binding(
    row: Arc<native::NativeRow>,
    work: &mut DecodeBudget,
) -> Result<Arc<Binding>> {
    if let Some((_, parsed)) = work
        .archive
        .bindings
        .iter()
        .find(|(cached, _)| same_cell(cached, &row, "binding"))
    {
        return Ok(parsed.clone());
    }
    let raw = native::value(&row, "binding")
        .and_then(|raw| raw.strip_prefix(BINDING))
        .ok_or(ManagementError::Confirmation)?;
    work.nodes(3)?;
    let binding = Arc::new(work.typed::<Binding>(raw)?);
    work.archive.bindings.push((row, binding.clone()));
    Ok(binding)
}

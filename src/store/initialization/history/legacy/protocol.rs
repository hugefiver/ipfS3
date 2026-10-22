use super::*;
use std::ops::Range;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct Key(pub String, pub Vec<String>);
impl From<&NativeKey> for Key {
    fn from(key: &NativeKey) -> Self {
        Self(key.table.clone(), key.values.clone())
    }
}
impl Key {
    pub fn native(&self) -> NativeKey {
        NativeKey {
            table: self.0.clone(),
            values: self.1.clone(),
        }
    }
    pub fn check(&self, scope: &str) -> Result<()> {
        if self.0 != management::schema::RECORDS
            || self.1.len() != 4
            || self.1[0] != scope
            || self.1[3] != "action"
        {
            return Err(ManagementError::Confirmation);
        }
        Ok(())
    }
}
#[derive(Deserialize)]
pub(super) struct Head(
    pub String,
    pub String,
    pub String,
    pub Key,
    pub Vec<String>,
    pub String,
    pub String,
    pub Vec<String>,
);
#[derive(Deserialize)]
struct Version(Vec<String>, Key, usize, usize, String);
#[derive(Deserialize)]
struct Manifest(Vec<String>, Vec<(String, Vec<Version>)>);
#[derive(Deserialize)]
struct Leaf(usize, Vec<String>, Vec<Cell>);
#[derive(Deserialize)]
#[serde(untagged)]
enum Cell {
    Text(Option<String>),
    Before((Key, usize, String)),
}

#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum Binding {
    Draft((u8, Key, String)),
    Confirmed((u8, Key, String, DateTime<Utc>, Vec<String>)),
}
type ConfirmedBinding = (Option<DateTime<Utc>>, Option<Vec<String>>);

impl Binding {
    pub fn validate(
        self,
        key: &Key,
        hash: &str,
        from: DateTime<Utc>,
        until: DateTime<Utc>,
        entries: &[PlanEntry],
    ) -> Result<ConfirmedBinding> {
        match self {
            Self::Draft((0, k, h)) if &k == key && h == hash => Ok((None, None)),
            Self::Confirmed((1, k, h, at, acks))
                if &k == key && h == hash && at >= from && at < until =>
            {
                let expected = entries
                    .iter()
                    .filter(|entry| entry.status == model::PlanStatus::Candidate)
                    .map(|entry| entry.request.item_id.as_str())
                    .collect::<BTreeSet<_>>();
                let actual = acks.iter().map(String::as_str).collect::<BTreeSet<_>>();
                if expected != actual || acks.len() != actual.len() {
                    return Err(ManagementError::Confirmation);
                }
                Ok((Some(at), Some(acks)))
            }
            _ => Err(ManagementError::Confirmation),
        }
    }
}

pub(super) fn cell<'a>(row: &'a NativeRow, name: &str) -> Result<&'a str> {
    native::value(row, name).ok_or(ManagementError::InvalidEvidence)
}
fn hash(domain: &[u8], raw: &str) -> String {
    let mut h = Sha256::new();
    h.update(domain);
    h.update(raw.as_bytes());
    hex::encode(h.finalize())
}

pub(super) struct Source {
    original: Arc<NativeRow>,
    pub head: Head,
    manifest: Manifest,
    core: Range<usize>,
    leaves: Vec<Range<usize>>,
}
impl Source {
    fn raw(&self) -> &str {
        self.original["native_before"]
            .as_deref()
            .expect("validated source cell")
    }
    pub fn core(&self) -> &str {
        &self.raw()[self.core.clone()]
    }
    pub fn open(
        row: Arc<NativeRow>,
        key: &NativeKey,
        decode: &mut records::ReadSession,
    ) -> Result<Self> {
        let raw = cell(&row, "native_before")?;
        if raw.len() > native::MAX_BYTES || !raw.starts_with("S7F1\n") {
            return Err(ManagementError::InvalidEvidence);
        }
        let number = |start: usize| -> Result<usize> {
            let value = raw
                .get(start..start + 8)
                .ok_or(ManagementError::InvalidEvidence)?;
            if !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(ManagementError::InvalidEvidence);
            }
            usize::from_str_radix(value, 16).map_err(|_| ManagementError::InvalidEvidence)
        };
        let (h, c, m, n) = (number(5)?, number(13)?, number(21)?, number(29)?);
        if n > native::MAX_ROWS {
            return Err(ManagementError::Budget);
        }
        let end = 38 + n * 8;
        if raw.as_bytes().get(37) != Some(&b'\n') || raw.as_bytes().get(end) != Some(&b'\n') {
            return Err(ManagementError::InvalidEvidence);
        }
        let mut offset = end + 1;
        let mut section = |length: usize| -> Result<Range<usize>> {
            let end = offset
                .checked_add(length)
                .ok_or(ManagementError::InvalidEvidence)?;
            if raw.get(offset..end).is_none() {
                return Err(ManagementError::InvalidEvidence);
            }
            let range = offset..end;
            offset = end;
            Ok(range)
        };
        let head: Head = decode.typed(&raw[section(h)?])?;
        let core = section(c)?;
        let manifest_range = section(m)?;
        let manifest: Manifest = decode.typed(&raw[manifest_range.clone()])?;
        let leaves = (0..n)
            .map(|i| section(number(38 + i * 8)?))
            .collect::<Result<Vec<_>>>()?;
        if offset != raw.len()
            || head.3 != Key::from(key)
            || head.0 != cell(&row, "database_scope")?
            || head.1 != cell(&row, "native_identity")?
            || head.2 != cell(&row, "principal")?
            || head.4 != manifest.0
            || head.7.len() != leaves.len()
            || head.5 != hash(b"stage7/core/1\0", &raw[core.clone()])
            || head.6 != hash(b"stage7/manifest/1\0", &raw[manifest_range])
            || head
                .7
                .iter()
                .zip(&leaves)
                .any(|(digest, range)| *digest != hash(b"stage7/leaf/1\0", &raw[range.clone()]))
        {
            return Err(ManagementError::InvalidEvidence);
        }
        head.3.check(&head.0)?;
        let catalog = native::archived_catalog(&head.4)?;
        if manifest.1.len() != catalog.len() {
            return Err(ManagementError::InvalidEvidence);
        }
        let mut count = 0usize;
        for ((name, versions), table) in manifest.1.iter().zip(catalog) {
            if name != &table.table {
                return Err(ManagementError::InvalidEvidence);
            }
            count = count
                .checked_add(versions.len())
                .ok_or(ManagementError::Budget)?;
            if count > native::MAX_ROWS {
                return Err(ManagementError::Budget);
            }
            let mut keys = BTreeSet::new();
            for version in versions {
                version.1.check(&head.0)?;
                if version.0.len() != table.primary_key.len()
                    || !keys.insert(&version.0)
                    || version.3 > native::MAX_BYTES
                {
                    return Err(ManagementError::InvalidEvidence);
                }
                if version.1 != head.3
                    && !manifest.1.iter().any(|(table, rows)| {
                        table == &version.1.0 && rows.iter().any(|r| r.0 == version.1.1)
                    })
                {
                    return Err(ManagementError::InvalidEvidence);
                }
            }
        }
        Ok(Self {
            original: row,
            head,
            manifest,
            core,
            leaves,
        })
    }
    pub fn dependencies(&self, decode: &mut records::ReadSession) -> Result<BTreeSet<Key>> {
        let mut keys = self
            .manifest
            .1
            .iter()
            .flat_map(|(_, rows)| rows.iter().map(|row| row.1.clone()))
            .collect::<BTreeSet<_>>();
        for range in &self.leaves {
            let leaf: Leaf = decode.typed(&self.raw()[range.clone()])?;
            for cell in leaf.2 {
                if let Cell::Before((key, _, _)) = cell {
                    if key == self.head.3 {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    keys.insert(key);
                }
            }
        }
        Ok(keys)
    }
}

pub(super) fn owner<'a>(
    row: &NativeRow,
    key: &NativeKey,
    sources: &'a BTreeMap<Key, Source>,
    decode: &mut records::ReadSession,
) -> Result<&'a Source> {
    let raw = cell(row, "native_before")?;
    if let Some(raw) = raw.strip_prefix("S7L1\n") {
        let (key, core, manifest): (Key, String, String) = decode.typed(raw)?;
        let source = sources.get(&key).ok_or(ManagementError::InvalidEvidence)?;
        if source.head.5 != core
            || source.head.6 != manifest
            || source.head.0 != cell(row, "database_scope")?
            || source.head.1 != cell(row, "native_identity")?
            || source.head.2 != cell(row, "principal")?
            || key.1[1] != cell(row, "plan_id")?
        {
            return Err(ManagementError::Confirmation);
        }
        Ok(source)
    } else {
        sources
            .get(&Key::from(key))
            .ok_or(ManagementError::InvalidEvidence)
    }
}

pub(super) fn validate_graph(
    sources: &BTreeMap<Key, Source>,
    originals: &BTreeMap<NativeKey, Arc<NativeRow>>,
    decode: &mut records::ReadSession,
) -> Result<()> {
    let mut graph = BTreeMap::<Key, BTreeSet<Key>>::new();
    for (key, source) in sources {
        let mut edges = BTreeSet::new();
        for dep in source.dependencies(decode)? {
            if dep == *key {
                continue;
            }
            let row = originals
                .get(&dep.native())
                .ok_or(ManagementError::InvalidEvidence)?;
            let resolved = &owner(row, &dep.native(), sources, decode)?.head.3;
            if resolved == key {
                return Err(ManagementError::InvalidEvidence);
            }
            edges.insert(resolved.clone());
        }
        graph.insert(key.clone(), edges);
    }
    while !graph.is_empty() {
        let leaf = graph
            .iter()
            .find(|(_, edges)| edges.is_empty())
            .map(|(key, _)| key.clone())
            .ok_or(ManagementError::InvalidEvidence)?;
        graph.remove(&leaf);
        for edges in graph.values_mut() {
            edges.remove(&leaf);
        }
    }
    Ok(())
}

pub(super) fn materialize(
    root: &Source,
    sources: &BTreeMap<Key, Source>,
    originals: &BTreeMap<NativeKey, Arc<NativeRow>>,
    decode: &mut records::ReadSession,
    budget: &mut Budget,
) -> Result<(BTreeMap<NativeKey, NativeRow>, BTreeSet<NativeKey>)> {
    let catalog = native::archived_catalog(&root.head.4)?;
    let mut rows = BTreeMap::new();
    let mut total = 0usize;
    let mut before_dependencies = BTreeSet::new();
    for (ordinal, ((_, versions), table)) in root.manifest.1.iter().zip(catalog).enumerate() {
        for version in versions {
            let supplier = sources
                .get(&version.1)
                .ok_or(ManagementError::InvalidEvidence)?;
            if supplier.head.4 != root.head.4
                || supplier.head.0 != root.head.0
                || supplier.head.1 != root.head.1
                || supplier.head.2 != root.head.2
            {
                return Err(ManagementError::Confirmation);
            }
            let range = supplier
                .leaves
                .get(version.2)
                .ok_or(ManagementError::InvalidEvidence)?;
            let raw = &supplier.raw()[range.clone()];
            if raw.len() != version.3 || supplier.head.7[version.2] != version.4 {
                return Err(ManagementError::InvalidEvidence);
            }
            let leaf: Leaf = decode.typed(raw)?;
            if leaf.0 != ordinal || leaf.1 != version.0 || leaf.2.len() != table.columns.len() {
                return Err(ManagementError::InvalidEvidence);
            }
            let key = NativeKey {
                table: table.table.clone(),
                values: version.0.clone(),
            };
            let mut row = NativeRow::new();
            for (column, cell) in table.columns.iter().zip(leaf.2) {
                let value = match cell {
                    Cell::Text(value) => value,
                    Cell::Before((source, length, expected)) => {
                        before_dependencies.insert(key.clone());
                        if column != "native_before" || source != Key::from(&key) {
                            return Err(ManagementError::InvalidEvidence);
                        }
                        let original = originals
                            .get(&key)
                            .ok_or(ManagementError::InvalidEvidence)?;
                        let before = cell_value(original, "native_before")?;
                        let tuple = serde_json::to_vec(&(&source, "native_before", before.len()))
                            .map_err(|_| ManagementError::InvalidEvidence)?;
                        let mut digest = Sha256::new();
                        digest.update(b"stage7/before/1\0");
                        digest.update((tuple.len() as u64).to_be_bytes());
                        digest.update(&tuple);
                        digest.update(before.as_bytes());
                        if before.len() != length || hex::encode(digest.finalize()) != expected {
                            return Err(ManagementError::InvalidEvidence);
                        }
                        decode.copy(before)?;
                        Some(before.into())
                    }
                };
                total = total
                    .checked_add(value.as_ref().map_or(0, String::len))
                    .ok_or(ManagementError::Budget)?;
                if total > native::MAX_BYTES {
                    return Err(ManagementError::Budget);
                }
                row.insert(column.clone(), value);
            }
            if table
                .primary_key
                .iter()
                .zip(&key.values)
                .any(|(c, v)| native::value(&row, c) != Some(v))
            {
                return Err(ManagementError::InvalidEvidence);
            }
            budget.charge(0, 1, row.len() as u64, 0)?;
            if rows.insert(key, row).is_some() {
                return Err(ManagementError::InvalidEvidence);
            }
        }
    }
    Ok((rows, before_dependencies))
}
fn cell_value<'a>(row: &'a NativeRow, column: &str) -> Result<&'a str> {
    cell(row, column)
}

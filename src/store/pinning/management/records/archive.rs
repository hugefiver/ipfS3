//! Flat immutable row versions. A source supplies bytes, never membership or authority.
use super::*;
pub(in crate::store::pinning::management::records) use batch::{
    remember_sql_row, sql_candidates, sql_rows,
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, ops::Range};

pub(super) const FRAME: &str = "S7F1\n";
pub(super) const LOCATOR: &str = "S7L1\n";
pub(super) const BINDING: &str = "S7B1\n";
pub(super) const INVENTORY: &str = "S7I1\n";
const SHARED_BINDING: &str = "S7R2\n";

/// Non-anchor items bind the exact confirmed bytes stored once at the proposal
/// anchor. The existing native_before locator supplies its relational identity.
pub(super) fn shared_binding(raw: &str) -> String {
    format!(
        "{SHARED_BINDING}{}",
        identity(b"stage7/shared-confirmation/2\0", raw.as_bytes())
    )
}

pub(super) fn is_shared_binding(raw: &str) -> bool {
    raw.strip_prefix(SHARED_BINDING).is_some_and(|hash| {
        hash.len() == 64
            && hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

pub(crate) fn binding_matches(left: &str, right: &str) -> bool {
    left == right
        || is_shared_binding(left)
            && right.starts_with("S7B1\n[1,")
            && left == shared_binding(right)
        || is_shared_binding(right)
            && left.starts_with("S7B1\n[1,")
            && right == shared_binding(left)
}

pub(super) fn provider_review(
    source: &evidence::Evidence,
    root: &native::NativeRow,
    work: &mut DecodeBudget,
) -> Result<Option<ReviewHandle>> {
    let owner = read::owner(source, root, work)?;
    let core = read::core(&owner, work)?;
    if core.1.iter().any(|entry| {
        matches!(
            entry.capture,
            Some(crate::pinning::management::Capture::Provider(_))
        )
    }) {
        load_review(source, root, work).map(Some)
    } else {
        Ok(None)
    }
}

pub(super) fn encode_confirmed_proposal(
    raw: &str,
    now: DateTime<Utc>,
    acks: &[String],
    work: &mut DecodeBudget,
) -> Result<String> {
    let sections = Sections::parse(raw, work)?;
    let head: Head = work.typed(&raw[sections.head])?;
    let body = encoded(&(1, &head.3, &head.5, now, acks), work)?;
    work.bytes(BINDING.len() + body.len())?;
    Ok(format!("{BINDING}{body}"))
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct Key(pub String, pub Vec<String>);
impl Key {
    fn native(&self, work: &mut DecodeBudget) -> Result<native::NativeKey> {
        native_key(&self.0, &self.1, work)
    }
    fn action(&self, scope: &str) -> bool {
        self.0 == super::super::schema::RECORDS
            && self.1.len() == 4
            && self.1[0] == scope
            && self.1[3] == "action"
    }
}
fn native_key(
    table: &str,
    values: &[String],
    work: &mut DecodeBudget,
) -> Result<native::NativeKey> {
    #[derive(Serialize)]
    struct NativeKeyRef<'a> {
        table: &'a str,
        values: &'a [String],
    }
    work.copy(&NativeKeyRef { table, values })?;
    Ok(native::NativeKey {
        table: table.to_owned(),
        values: values.to_vec(),
    })
}

#[derive(Serialize, Deserialize)]
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
#[derive(Serialize, Deserialize)]
pub(super) struct RowVersion(pub Vec<String>, pub Key, pub usize, pub usize, pub String);
#[derive(Serialize, Deserialize)]
pub(super) struct Manifest(pub Vec<String>, pub Vec<(String, Vec<RowVersion>)>);
#[derive(Serialize, Deserialize)]
pub(crate) struct Leaf(pub usize, pub Vec<String>, pub Vec<Cell>);
#[derive(Serialize)]
#[serde(untagged)]
pub(crate) enum Cell {
    Text(Option<String>),
    Before(Key, usize, String),
}
impl<'de> Deserialize<'de> for Cell {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Cell;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an original SQL cell or exact immutable before cell")
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Cell, E> {
                Ok(Cell::Text(None))
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<Cell, E> {
                Ok(Cell::Text(Some(value.into())))
            }
            fn visit_string<E: serde::de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Cell, E> {
                Ok(Cell::Text(Some(value)))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Cell, A::Error> {
                let key = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let length = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let hash = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::invalid_length(4, &self));
                }
                Ok(Cell::Before(key, length, hash))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

/// Byte ranges borrow the one original SQL TEXT; no envelope JSON tree is built.
pub(super) struct Sections {
    pub head: Range<usize>,
    pub core: Range<usize>,
    pub manifest: Range<usize>,
    pub leaves: Vec<Range<usize>>,
}
impl Sections {
    pub(super) fn parse(raw: &str, work: &mut DecodeBudget) -> Result<Self> {
        if !raw.starts_with(FRAME) || raw.len() > native::MAX_BYTES {
            return Err(ManagementError::InvalidEvidence);
        }
        fn number(raw: &str, start: usize) -> Result<usize> {
            let text = raw
                .get(start..start + 8)
                .ok_or(ManagementError::InvalidEvidence)?;
            if !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(ManagementError::InvalidEvidence);
            }
            usize::from_str_radix(text, 16).map_err(|_| ManagementError::Budget)
        }
        let lengths = [number(raw, 5)?, number(raw, 13)?, number(raw, 21)?];
        let count = number(raw, 29)?;
        if count > native::MAX_ROWS {
            return Err(ManagementError::Budget);
        }
        let directory_end = 38usize
            .checked_add(count.checked_mul(8).ok_or(ManagementError::Budget)?)
            .ok_or(ManagementError::Budget)?;
        if raw.as_bytes().get(37) != Some(&b'\n')
            || raw.as_bytes().get(directory_end) != Some(&b'\n')
        {
            return Err(ManagementError::InvalidEvidence);
        }
        work.nodes(4 + count)?;
        let mut offset = directory_end + 1;
        fn section(raw: &str, offset: &mut usize, length: usize) -> Result<Range<usize>> {
            let end = offset.checked_add(length).ok_or(ManagementError::Budget)?;
            if raw.get(*offset..end).is_none() {
                return Err(ManagementError::InvalidEvidence);
            }
            let range = *offset..end;
            *offset = end;
            Ok(range)
        }
        let head = section(raw, &mut offset, lengths[0])?;
        let core = section(raw, &mut offset, lengths[1])?;
        let manifest = section(raw, &mut offset, lengths[2])?;
        let mut leaves = Vec::with_capacity(count);
        for slot in 0..count {
            leaves.push(section(raw, &mut offset, number(raw, 38 + 8 * slot)?)?);
        }
        if offset != raw.len() {
            return Err(ManagementError::InvalidEvidence);
        }
        Ok(Self {
            head,
            core,
            manifest,
            leaves,
        })
    }
}

pub(super) fn identity(domain: &[u8], bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(bytes);
    hex::encode(hash.finalize())
}
fn digest_matches(digest: impl AsRef<[u8]>, expected: &str) -> bool {
    let mut hex = [0; 64];
    hex::encode_to_slice(digest, &mut hex).is_ok() && expected.as_bytes() == hex
}
fn identity_matches(domain: &[u8], bytes: &[u8], expected: &str) -> bool {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(bytes);
    digest_matches(hash.finalize(), expected)
}
fn before_identity(key: &Key, raw: &str) -> Result<String> {
    Ok(hex::encode(before_digest(key, raw)?))
}
fn before_digest(key: &Key, raw: &str) -> Result<sha2::digest::Output<Sha256>> {
    let tuple = (key, "native_before", raw.len());
    let mut hash = Sha256::new();
    hash.update(b"stage7/before/1\0");
    hash.update((codec::measure(&tuple)?.1 as u64).to_be_bytes());
    struct Writer<'a>(&'a mut Sha256);
    impl std::io::Write for Writer<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Writer(&mut hash), &tuple)
        .map_err(|_| ManagementError::InvalidEvidence)?;
    hash.update(raw.as_bytes());
    Ok(hash.finalize())
}
fn encoded<T: Serialize>(value: &T, work: &mut DecodeBudget) -> Result<String> {
    let (_, length) = codec::measure(value)?;
    work.bytes(length)?;
    codec::serialize_bounded(value, crate::pinning::management::wire::MAX_WIRE_BYTES)
}

pub(super) fn assemble(
    head: &str,
    core: &str,
    manifest: &str,
    leaves: &[String],
    work: &mut DecodeBudget,
) -> Result<String> {
    if leaves.len() > native::MAX_ROWS {
        return Err(ManagementError::Budget);
    }
    let length = leaves.iter().try_fold(
        39 + 8 * leaves.len() + head.len() + core.len() + manifest.len(),
        |sum, leaf| sum.checked_add(leaf.len()).ok_or(ManagementError::Budget),
    )?;
    if length > native::MAX_BYTES {
        return Err(ManagementError::Budget);
    }
    work.bytes(length)?;
    use std::fmt::Write;
    let mut raw = String::with_capacity(length);
    writeln!(
        raw,
        "{FRAME}{:08x}{:08x}{:08x}{:08x}",
        head.len(),
        core.len(),
        manifest.len(),
        leaves.len()
    )
    .map_err(|_| ManagementError::InvalidEvidence)?;
    for leaf in leaves {
        write!(raw, "{:08x}", leaf.len()).map_err(|_| ManagementError::InvalidEvidence)?;
    }
    raw.push('\n');
    raw.push_str(head);
    raw.push_str(core);
    raw.push_str(manifest);
    for leaf in leaves {
        raw.push_str(leaf);
    }
    Ok(raw)
}

/// Source metadata is checked independently of its opaque core and manifest.
pub(super) struct Source {
    pub sections: Sections,
    pub head: Head,
}
impl Source {
    pub(super) fn open(
        raw: &str,
        key: &Key,
        scope: &str,
        identity: &str,
        principal: &str,
        work: &mut DecodeBudget,
    ) -> Result<Self> {
        let sections = Sections::parse(raw, work)?;
        let head: Head = work.typed(&raw[sections.head.clone()])?;
        if head.0 != scope
            || head.1 != identity
            || head.2 != principal
            || head.3 != *key
            || !key.action(scope)
            || head.7.len() != sections.leaves.len()
        {
            return Err(ManagementError::Unauthorized);
        }
        if !head
            .5
            .bytes()
            .chain(head.6.bytes())
            .chain(head.7.iter().flat_map(|hash| hash.bytes()))
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || head.5.len() != 64
            || head.6.len() != 64
            || head.7.iter().any(|hash| hash.len() != 64)
        {
            return Err(ManagementError::InvalidEvidence);
        }
        Ok(Self { sections, head })
    }
}

impl Manifest {
    pub(super) fn validate(
        &self,
        head: &Head,
        catalog: &[native::NativeTable],
        work: &mut DecodeBudget,
    ) -> Result<()> {
        work.check()?;
        if self.0 != head.4 || self.1.len() != catalog.len() {
            return Err(ManagementError::InvalidEvidence);
        }
        let mut count = 0usize;
        for ((name, rows), table) in self.1.iter().zip(catalog) {
            if name != &table.table {
                return Err(ManagementError::InvalidEvidence);
            }
            count = count
                .checked_add(rows.len())
                .ok_or(ManagementError::Budget)?;
            if count > native::MAX_ROWS {
                return Err(ManagementError::Budget);
            }
            for (position, row) in rows.iter().enumerate() {
                if row.0.len() != table.primary_key.len()
                    || row.3 > native::MAX_BYTES
                    || row.4.len() != 64
                    || !row
                        .4
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    || !row.1.action(&head.0)
                {
                    return Err(ManagementError::InvalidEvidence);
                }
                if rows[..position].iter().any(|earlier| earlier.0 == row.0) {
                    return Err(ManagementError::InvalidEvidence);
                }
            }
        }
        for (_, rows) in &self.1 {
            for row in rows {
                if row.1 != head.3 && !self.contains(&row.1) {
                    return Err(ManagementError::InvalidEvidence);
                }
            }
        }
        Ok(())
    }
    fn contains(&self, key: &Key) -> bool {
        self.1
            .iter()
            .find(|(table, _)| table == &key.0)
            .is_some_and(|(_, rows)| rows.iter().any(|row| row.0 == key.1))
    }
}

pub(super) fn raw_cell<'a>(
    cell: &'a Cell,
    column: &str,
    key: &Key,
    manifest: &Manifest,
    resolve: impl FnOnce(&Key) -> Result<&'a str>,
) -> Result<Option<&'a str>> {
    match cell {
        Cell::Text(raw) => Ok(raw.as_deref()),
        Cell::Before(source, length, hash) => {
            if column != "native_before"
                || source != key
                || source.1.first().is_none_or(|scope| !source.action(scope))
                || !manifest.contains(key)
            {
                return Err(ManagementError::InvalidEvidence);
            }
            let raw = resolve(source)?;
            if raw.len() != *length || !digest_matches(before_digest(source, raw)?, hash) {
                return Err(ManagementError::InvalidEvidence);
            }
            Ok(Some(raw))
        }
    }
}

mod batch;
mod core;
mod read;
mod review;
#[cfg(test)]
mod tests;
mod write;
pub use review::ReviewHandle;
pub(super) use review::load_review;
mod rows;
pub(super) use batch::Binding as StoredBinding;
pub(super) use batch::ReadBatch;
pub(super) use batch::{frame_row, retain_row, retain_row_ref, retain_view};
pub(super) use core::Core;
pub use core::{SharedValue, SnapshotRest};
pub(super) use read::{
    dependencies, encode_draft, encode_inventory, encode_locator, read_binding, validate_draft,
};
pub(super) use review::classify;
pub(super) use review::load_confirmed;
pub use review::{ConfirmedPlan, Review};
pub(crate) use rows::ArchivedRows;
pub(super) use write::encode_proposal;

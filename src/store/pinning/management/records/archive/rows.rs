//! Frozen manifest membership over shared original leaf cells.
use super::*;
use std::sync::Arc;

pub(crate) struct ArchivedRows {
    owner: Arc<batch::StoredSource>,
    manifest: Arc<Manifest>,
    catalog: &'static [native::NativeTable],
    rows: Vec<Vec<Arc<RowHandle>>>,
}
impl ArchivedRows {
    pub(super) fn recheck(
        &self,
        source: &evidence::Evidence,
        work: &mut DecodeBudget,
    ) -> Result<()> {
        for (ordinal, ((table, (_, versions)), rows)) in self
            .catalog
            .iter()
            .zip(&self.manifest.1)
            .zip(&self.rows)
            .enumerate()
        {
            if versions.len() != rows.len() {
                return Err(ManagementError::InvalidEvidence);
            }
            for (version, row) in versions.iter().zip(rows) {
                let supplier = batch::source(
                    source.archive_row(&version.1)?,
                    &version.1,
                    &self.owner.head.0,
                    &self.owner.head.1,
                    &self.owner.head.2,
                    work,
                )?;
                if supplier.head.4 != self.manifest.0 {
                    return Err(ManagementError::InvalidEvidence);
                }
                let leaf = batch::leaf(&supplier, version, work)?;
                let RowHandle::Archive {
                    leaf: cached,
                    terminals,
                    ..
                } = row.as_ref()
                else {
                    return Err(ManagementError::InvalidEvidence);
                };
                if !Arc::ptr_eq(cached, &leaf)
                    || leaf.0 != ordinal
                    || leaf.2.len() != table.columns.len()
                {
                    return Err(ManagementError::Changed);
                }
                for (slot, (column, cell)) in table.columns.iter().zip(&leaf.2).enumerate() {
                    if let Cell::Before(key, ..) = cell {
                        if key.0 != table.table || key.1 != version.0 {
                            return Err(ManagementError::InvalidEvidence);
                        }
                        let target = source.archive_row_ref(key)?;
                        batch::check_row(
                            target,
                            key,
                            &self.owner.head.0,
                            &self.owner.head.1,
                            &self.owner.head.2,
                        )?;
                        let raw = raw_cell(cell, column, key, &self.manifest, |_| {
                            native::value(target, "native_before")
                                .ok_or(ManagementError::InvalidEvidence)
                        })?;
                        if terminals
                            .iter()
                            .find(|(index, _)| *index == slot)
                            .and_then(|(_, row)| native::value(row, "native_before"))
                            != raw
                        {
                            return Err(ManagementError::Changed);
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub(crate) fn scope(&self) -> &str {
        &self.owner.head.0
    }
    pub(crate) fn identity(&self) -> &str {
        &self.owner.head.1
    }
    pub(crate) fn catalog(&self) -> &'static [native::NativeTable] {
        self.catalog
    }
    pub(crate) fn schema_versions(&self) -> &[String] {
        &self.manifest.0
    }
    pub(crate) fn row_ref(&self, table: &str, key: &[&str]) -> Option<&Arc<RowHandle>> {
        let ordinal = self
            .catalog
            .iter()
            .position(|layout| layout.table == table)?;
        let position = self.manifest.1[ordinal]
            .1
            .iter()
            .position(|version| version.0.iter().map(String::as_str).eq(key.iter().copied()))?;
        self.rows[ordinal].get(position)
    }
    pub(crate) fn row(&self, table: &str, key: &[String]) -> Option<&Arc<RowHandle>> {
        let ordinal = self
            .catalog
            .iter()
            .position(|layout| layout.table == table)?;
        let position = self.manifest.1[ordinal]
            .1
            .iter()
            .position(|version| version.0 == key)?;
        self.rows[ordinal].get(position)
    }
    pub(crate) fn rows(
        &self,
    ) -> impl Iterator<Item = (&native::NativeTable, &[String], &Arc<RowHandle>)> {
        self.catalog
            .iter()
            .zip(&self.manifest.1)
            .zip(&self.rows)
            .flat_map(|((table, (_, versions)), rows)| {
                versions
                    .iter()
                    .zip(rows)
                    .map(move |(version, row)| (table, version.0.as_slice(), row))
            })
    }
    pub(crate) fn original_cell(
        &self,
        table: &str,
        key: &[String],
        column: &str,
    ) -> Option<&Option<String>> {
        self.row(table, key)?.get(column)
    }
}

pub(super) fn load(
    source: &evidence::Evidence,
    owner: &Arc<batch::StoredSource>,
    manifest: Arc<Manifest>,
    catalog: &'static [native::NativeTable],
    work: &mut DecodeBudget,
) -> Result<ArchivedRows> {
    work.nodes(1 + catalog.len())?;
    let mut rows = Vec::with_capacity(catalog.len());
    for (ordinal, (table, (_, versions))) in catalog.iter().zip(&manifest.1).enumerate() {
        work.nodes(1 + versions.len())?;
        let mut members = Vec::with_capacity(versions.len());
        for version in versions {
            if version.1 != owner.head.3 && !manifest.contains(&version.1) {
                return Err(ManagementError::InvalidEvidence);
            }
            let supplier = batch::source(
                source.archive_row(&version.1)?,
                &version.1,
                &owner.head.0,
                &owner.head.1,
                &owner.head.2,
                work,
            )?;
            if supplier.head.4 != manifest.0 {
                return Err(ManagementError::InvalidEvidence);
            }
            let leaf = batch::leaf(&supplier, version, work)?;
            if leaf.0 != ordinal || leaf.2.len() != table.columns.len() {
                return Err(ManagementError::InvalidEvidence);
            }
            let mut terminals = Vec::new();
            for (slot, (column, cell)) in table.columns.iter().zip(&leaf.2).enumerate() {
                if let Cell::Before(key, ..) = cell {
                    if key.0 != table.table || key.1 != version.0 {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    let target = source.archive_row(key)?;
                    batch::check_row(&target, key, &owner.head.0, &owner.head.1, &owner.head.2)?;
                    raw_cell(cell, column, key, &manifest, |_| {
                        native::value(&target, "native_before")
                            .ok_or(ManagementError::InvalidEvidence)
                    })?;
                    work.nodes(2)?;
                    terminals.push((slot, target));
                }
            }
            work.nodes(2)?;
            let row = Arc::new(RowHandle::Archive {
                table,
                leaf,
                terminals,
            });
            if table
                .primary_key
                .iter()
                .zip(&version.0)
                .any(|(column, value)| native::value(row.as_ref(), column) != Some(value))
            {
                return Err(ManagementError::InvalidEvidence);
            }
            members.push(row);
        }
        rows.push(members);
    }
    work.nodes(1)?;
    Ok(ArchivedRows {
        owner: owner.clone(),
        manifest,
        catalog,
        rows,
    })
}

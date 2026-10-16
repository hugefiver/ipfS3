use super::*;
use sea_orm::{DatabaseBackend, Value as SqlValue};

impl Evidence {
    #[cfg(test)]
    pub(crate) async fn load<C: ConnectionTrait>(
        db: &C,
        scope: &str,
        identity: &str,
        plans: &[String],
    ) -> Result<Self> {
        Self::load_with_work(db, scope, identity, plans, SharedDecodeBudget::default()).await
    }
    pub(crate) async fn load_with_work<C: ConnectionTrait>(
        db: &C,
        scope: &str,
        identity: &str,
        plans: &[String],
        work: SharedDecodeBudget,
    ) -> Result<Self> {
        let mut evidence =
            Self::load_roots_with_work(db, scope, identity, plans, &[], work).await?;
        evidence.load_dependencies(db, plans).await?;
        Ok(evidence)
    }
    #[cfg(test)]
    pub(crate) async fn load_roots<C: ConnectionTrait>(
        db: &C,
        scope: &str,
        identity: &str,
        plans: &[String],
        candidates: &[(native::NativeRow, native::NativeRow)],
    ) -> Result<Self> {
        Self::load_roots_with_work(
            db,
            scope,
            identity,
            plans,
            candidates,
            SharedDecodeBudget::default(),
        )
        .await
    }
    pub(crate) async fn load_roots_with_work<C: ConnectionTrait>(
        db: &C,
        scope: &str,
        identity: &str,
        plans: &[String],
        candidates: &[(native::NativeRow, native::NativeRow)],
        work: SharedDecodeBudget,
    ) -> Result<Self> {
        Self::load_roots_reserved(db, scope, identity, plans, candidates, work).await
    }
    pub(in crate::store::pinning::management::records) async fn load_indexed<C: ConnectionTrait>(
        db: &C,
        scope: &str,
        identity: &str,
        keys: &[native::NativeKey],
        reserved_rows: usize,
        bytes: usize,
        work: SharedDecodeBudget,
    ) -> Result<Self> {
        native::check_raw_inventory(db).await?;
        work.lock()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .index_copy(&(scope, identity, keys))?;
        let mut evidence = Self {
            scope: scope.into(),
            identity: identity.into(),
            rows: Arc::default(),
            bytes,
            reserved_rows,
            locators: BTreeSet::new(),
            work,
        };
        for key in keys {
            if key.table != super::super::super::schema::RECORDS
                || key.values.len() != 4
                || key.values[3] != "action"
            {
                return Err(ManagementError::InvalidEvidence);
            }
            if evidence.locators.insert(key.clone()) {
                evidence.bytes = evidence
                    .bytes
                    .checked_add(key.values.iter().map(String::len).sum())
                    .ok_or(ManagementError::Budget)?;
            }
        }
        evidence.check_remaining(0, 0)?;
        let mut plans = Vec::new();
        for key in keys {
            evidence.budget()?.index_copy(&key.values[1])?;
            plans.push(key.values[1].clone());
        }
        evidence.read_plans(db, &plans).await?;
        if !evidence.locators.is_empty() {
            return Err(ManagementError::Changed);
        }
        evidence.load_dependencies(db, &plans).await?;
        Ok(evidence)
    }
    async fn load_roots_reserved<C: ConnectionTrait>(
        db: &C,
        scope: &str,
        identity: &str,
        plans: &[String],
        candidates: &[(native::NativeRow, native::NativeRow)],
        work: SharedDecodeBudget,
    ) -> Result<Self> {
        native::check_raw_inventory(db).await?;
        work.lock()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .index_copy(&(scope, identity))?;
        let mut evidence = Self {
            scope: scope.into(),
            identity: identity.into(),
            rows: Arc::default(),
            bytes: 0,
            reserved_rows: 0,
            locators: BTreeSet::new(),
            work,
        };
        evidence.check_remaining(0, 0)?;
        for (retirement, action) in candidates {
            let retirement_key = native::NativeKey {
                table: super::super::super::schema::RETIREMENTS.into(),
                values: vec![
                    native::value(retirement, "allocation_domain")
                        .ok_or(ManagementError::InvalidEvidence)?
                        .into(),
                ],
            };
            for (key, row) in [(retirement_key, retirement), (key(action)?, action)] {
                if let Some(existing) = evidence.rows.get(&key) {
                    if existing.as_ref() != row {
                        return Err(ManagementError::Changed);
                    }
                } else {
                    evidence.budget()?.copy(row)?;
                    evidence.insert(key, row.clone())?;
                }
            }
        }
        evidence.read_plans(db, plans).await?;
        Ok(evidence)
    }
    async fn read_plans<C: ConnectionTrait>(&mut self, db: &C, plans: &[String]) -> Result<()> {
        for plan in plans.iter().collect::<BTreeSet<_>>() {
            self.reuse_exact(db, plan).await?;
            let rows = self
                .read(
                    db,
                    "database_scope=$1 AND plan_id=$2 AND kind='action'",
                    vec![self.scope.clone().into(), plan.clone().into()],
                )
                .await?;
            for row in rows {
                let key = self.budget()?.key(&row)?;
                self.insert(key, row)?;
            }
        }
        Ok(())
    }
    async fn reuse_exact<C: ConnectionTrait>(&mut self, db: &C, plan: &str) -> Result<()> {
        let candidates = archive::sql_candidates(&self.scope, plan, &mut *self.budget()?)?;
        for row in candidates {
            let key = self.budget()?.key(&row)?;
            if self.rows.contains_key(&key) {
                continue;
            }
            let backend = db.get_database_backend();
            let equal = if backend == DatabaseBackend::Sqlite {
                "IS"
            } else {
                "IS NOT DISTINCT FROM"
            };
            // A memo only supplies bytes. Every cell, including NULL, private raw
            // bodies and current state, must still match the database exactly.
            let bytes = row.values().flatten().try_fold(0usize, |total, value| {
                total
                    .checked_add(value.len())
                    .ok_or(ManagementError::Budget)
            })?;
            self.budget()?.index_cells(row.len(), bytes)?;
            let predicate = row
                .keys()
                .enumerate()
                .map(|(i, column)| format!("CAST(\"{column}\" AS TEXT) {equal} ${}", i + 1))
                .collect::<Vec<_>>()
                .join(" AND ");
            let values = row
                .values()
                .map(|value| value.clone().into())
                .collect::<Vec<SqlValue>>();
            let matched = db
                .query_one(Statement::from_sql_and_values(
                    backend,
                    format!(
                        "SELECT COUNT(*) AS matches FROM pin_management_records WHERE {predicate}"
                    ),
                    values,
                ))
                .await
                .map_err(|_| ManagementError::Database)?
                .ok_or(ManagementError::Database)?
                .try_get::<i64>("", "matches")
                .map_err(|_| ManagementError::InvalidEvidence)?;
            if matched == 1 {
                self.insert_shared(key, row)?;
            } else if matched != 0 {
                return Err(ManagementError::InvalidEvidence);
            }
        }
        Ok(())
    }
    pub(crate) async fn load_dependencies<C: ConnectionTrait>(
        &mut self,
        db: &C,
        plans: &[String],
    ) -> Result<()> {
        let original = self.rows.clone();
        self.budget()?.index_copy(&(plans, &self.scope))?;
        let plans = plans.to_vec();
        let owned_scope = self.scope.clone();
        let work = self.work.clone();
        let dependencies = tokio::task::spawn_blocking(move || {
            dependencies(
                &original,
                &plans,
                &owned_scope,
                &mut *work.lock().map_err(|_| ManagementError::InvalidEvidence)?,
            )
        })
        .await
        .map_err(|_| ManagementError::InvalidEvidence)??;
        for key in dependencies {
            if self.rows.contains_key(&key) {
                continue;
            }
            self.reuse_exact(db, &key.values[1]).await?;
            if self.rows.contains_key(&key) {
                continue;
            }
            let mut rows = self
                .read(
                    db,
                    "database_scope=$1 AND plan_id=$2 AND item_id=$3 AND kind=$4",
                    key.values.iter().cloned().map(Into::into).collect(),
                )
                .await?;
            if rows.len() != 1 {
                return Err(ManagementError::InvalidEvidence);
            }
            self.insert(key, rows.remove(0))?;
        }
        Ok(())
    }
    async fn read<C: ConnectionTrait>(
        &self,
        db: &C,
        predicate: &str,
        mut values: Vec<SqlValue>,
    ) -> Result<Vec<native::NativeRow>> {
        // Already transferred full rows are charged once, including exact-cell targets.
        let mut predicate = format!("({predicate})");
        let mut known = Vec::new();
        for key in self
            .rows
            .keys()
            .filter(|key| key.table == super::super::super::schema::RECORDS)
        {
            let offset = values.len();
            known.push(format!(
                "(${},${},${},${})",
                offset + 1,
                offset + 2,
                offset + 3,
                offset + 4
            ));
            values.extend(key.values.iter().cloned().map(Into::into));
        }
        if !known.is_empty() {
            predicate.push_str(&format!(" AND NOT EXISTS (SELECT 1 FROM (VALUES {}) AS loaded WHERE loaded.column1=database_scope AND loaded.column2=plan_id AND loaded.column3=item_id AND loaded.column4=kind)",known.join(",")));
        }
        let table = native::current_catalog()
            .iter()
            .find(|table| table.table == super::super::super::schema::RECORDS)
            .ok_or(ManagementError::Schema)?;
        let backend = db.get_database_backend();
        let terms = table
            .columns
            .iter()
            .map(|column| {
                if backend == DatabaseBackend::Sqlite {
                    format!("LENGTH(CAST(COALESCE(CAST(\"{column}\" AS TEXT),'') AS BLOB))")
                } else {
                    format!("OCTET_LENGTH(COALESCE(CAST(\"{column}\" AS TEXT),''))::bigint")
                }
            })
            .collect::<Vec<_>>()
            .join("+");
        let mut pending = Vec::new();
        for key in &self.locators {
            let offset = values.len();
            pending.push(format!(
                "(${},${},${},${})",
                offset + 1,
                offset + 2,
                offset + 3,
                offset + 4
            ));
            values.extend(key.values.iter().cloned().map(Into::into));
        }
        let matched = if pending.is_empty() {
            "FALSE".into()
        } else {
            format!(
                "EXISTS (SELECT 1 FROM (VALUES {}) AS located WHERE located.column1=database_scope AND located.column2=plan_id AND located.column3=item_id AND located.column4=kind)",
                pending.join(",")
            )
        };
        let header = ["database_scope", "plan_id", "item_id", "kind"]
            .iter()
            .map(|column| {
                if backend == DatabaseBackend::Sqlite {
                    format!("LENGTH(CAST(\"{column}\" AS BLOB))")
                } else {
                    format!("OCTET_LENGTH(\"{column}\")::bigint")
                }
            })
            .collect::<Vec<_>>()
            .join("+");
        let sum = if backend == DatabaseBackend::Postgres {
            format!(
                "CAST(COALESCE(SUM(({terms})-(CASE WHEN {matched} THEN {header} ELSE 0 END)),0) AS BIGINT)"
            )
        } else {
            format!("COALESCE(SUM(({terms})-(CASE WHEN {matched} THEN {header} ELSE 0 END)),0)")
        };
        let new_rows =
            format!("CAST(COALESCE(SUM(CASE WHEN {matched} THEN 0 ELSE 1 END),0) AS BIGINT)");
        let measured=db.query_one(Statement::from_sql_and_values(backend,format!("SELECT COUNT(*) AS rows,{new_rows} AS new_rows,{sum} AS bytes,CAST(COALESCE(SUM({terms}),0) AS BIGINT) AS transfer_bytes FROM pin_management_records WHERE {predicate}"),values.clone())).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Database)?;
        let count = usize::try_from(
            measured
                .try_get::<i64>("", "rows")
                .map_err(|_| ManagementError::InvalidEvidence)?,
        )
        .map_err(|_| ManagementError::Budget)?;
        let bytes = usize::try_from(
            measured
                .try_get::<i64>("", "bytes")
                .map_err(|_| ManagementError::InvalidEvidence)?,
        )
        .map_err(|_| ManagementError::Budget)?;
        let added = usize::try_from(
            measured
                .try_get::<i64>("", "new_rows")
                .map_err(|_| ManagementError::InvalidEvidence)?,
        )
        .map_err(|_| ManagementError::Budget)?;
        let transfer_bytes = usize::try_from(
            measured
                .try_get::<i64>("", "transfer_bytes")
                .map_err(|_| ManagementError::InvalidEvidence)?,
        )
        .map_err(|_| ManagementError::Budget)?;
        self.check_remaining(added, bytes)?;
        // The driver owns the first SQL strings; try_get creates the second.
        // Reserve both transfers before issuing the body SELECT.
        {
            let mut work = self.budget()?;
            work.index_cells(
                count
                    .checked_mul(table.columns.len())
                    .ok_or(ManagementError::Budget)?,
                transfer_bytes,
            )?;
            work.index_cells(
                count
                    .checked_mul(table.columns.len())
                    .ok_or(ManagementError::Budget)?,
                transfer_bytes,
            )?;
            for _ in 0..count {
                work.copy(&table.columns)?;
            }
        }
        let fields = table
            .columns
            .iter()
            .map(|column| format!("CAST(\"{column}\" AS TEXT) AS \"{column}\""))
            .collect::<Vec<_>>()
            .join(",");
        let remaining_bytes = native::MAX_BYTES - self.bytes;
        let remaining_rows =
            native::MAX_ROWS - self.rows.len() - self.locators.len() - self.reserved_rows;
        let actual=db.query_all(Statement::from_sql_and_values(backend,format!("SELECT {fields} FROM pin_management_records WHERE ({predicate}) AND (SELECT {sum} FROM pin_management_records WHERE {predicate})<={remaining_bytes} AND (SELECT {new_rows} FROM pin_management_records WHERE {predicate})<={remaining_rows} AND (SELECT CAST(COALESCE(SUM({terms}),0) AS BIGINT) FROM pin_management_records WHERE {predicate})<={transfer_bytes} AND (SELECT COUNT(*) FROM pin_management_records WHERE {predicate})<={count} ORDER BY item_id LIMIT {}",native::MAX_ROWS+1),values)).await.map_err(|_|ManagementError::Database)?;
        if actual.len() != count {
            return Err(ManagementError::Changed);
        }
        actual
            .into_iter()
            .map(|row| {
                table
                    .columns
                    .iter()
                    .map(|column| {
                        Ok((
                            column.clone(),
                            row.try_get::<Option<String>>("", column)
                                .map_err(|_| ManagementError::InvalidEvidence)?,
                        ))
                    })
                    .collect()
            })
            .collect()
    }
    #[cfg(test)]
    pub(in crate::store::pinning::management::records) fn plan_rows(
        &self,
        plan: &str,
    ) -> Vec<native::NativeRow> {
        self.rows
            .iter()
            .filter(|(key, _)| {
                key.table == super::super::super::schema::RECORDS && key.values[1] == plan
            })
            .map(|(_, row)| row.as_ref().clone())
            .collect()
    }
}

fn dependencies(
    rows: &BTreeMap<native::NativeKey, Arc<native::NativeRow>>,
    plans: &[String],
    _scope: &str,
    work: &mut Budget,
) -> Result<BTreeSet<native::NativeKey>> {
    let mut dependencies = BTreeSet::new();
    // Exact-cell targets remain opaque strings: no recursive graph expansion.
    for (_, row) in rows.iter().filter(|(key, _)| {
        key.table == super::super::super::schema::RECORDS && plans.contains(&key.values[1])
    }) {
        let raw = native::value(row, "native_before").ok_or(ManagementError::InvalidEvidence)?;
        if raw.starts_with(archive::FRAME) {
            for key in archive::dependencies(row.clone(), work)? {
                work.nodes(1)?;
                dependencies.insert(key);
            }
            continue;
        }
        if raw.starts_with(archive::LOCATOR) {
            continue;
        }
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(dependencies)
}

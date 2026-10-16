//! SQL-native complete inventory. Time text is never round-tripped through chrono.
use super::{ManagementError, Result};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, Iterable, PrimaryKeyToColumn,
    Statement, sea_query::Iden,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
mod catalog_list;
mod history;
mod identity;
pub use catalog_list::CatalogList;
pub(crate) use history::{archived_catalog, current_catalog};

pub const MAX_ROWS: usize = 4096;
pub const MAX_BYTES: usize = 4 * 1024 * 1024;
pub type NativeRow = BTreeMap<String, Option<String>>;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeKey {
    pub table: String,
    pub values: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTable {
    pub table: String,
    pub columns: CatalogList<String>,
    pub primary_key: CatalogList<String>,
    pub rows: Vec<NativeRow>,
    pub definitions: CatalogList<super::catalog::NativeColumn>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeFrame {
    pub schema_versions: Vec<String>,
    pub database_scope: String,
    /// Actual connection-native identity, not an imported UUID or DSN.
    pub database_identity: String,
    pub tables: Vec<NativeTable>,
    pub frontier: Vec<NativeKey>,
    /// Private equality material is never written into plan/confirmation files.
    /// Imported frames missing it cannot equal a fresh in-process frame.
    #[serde(skip)]
    private: Vec<(NativeKey, std::sync::Arc<NativeRow>)>,
}

impl NativeFrame {
    pub fn validate(&self) -> Result<()> {
        let registry = history::archived_catalog(&self.schema_versions)?;
        if self.database_scope.is_empty()
            || self.database_identity.is_empty()
            || self.tables.len() != registry.len()
        {
            return Err(ManagementError::InvalidEvidence);
        }
        let mut count = 0_usize;
        let mut bytes = 0_usize;
        let mut frontier = self.frontier.iter();
        for (table, expected) in self.tables.iter().zip(registry) {
            if table.table != expected.table
                || table.columns != expected.columns
                || table.primary_key != expected.primary_key
                || table.definitions != expected.definitions
            {
                return Err(ManagementError::InvalidEvidence);
            }
            count = count
                .checked_add(table.rows.len())
                .ok_or(ManagementError::Budget)?;
            if count > MAX_ROWS {
                return Err(ManagementError::Budget);
            }
            for row in &table.rows {
                if !row.keys().eq(table.columns.iter()) {
                    return Err(ManagementError::InvalidEvidence);
                }
                let key = frontier.next().ok_or(ManagementError::InvalidEvidence)?;
                if key.table != table.table
                    || key.values.len() != table.primary_key.len()
                    || table
                        .primary_key
                        .iter()
                        .zip(&key.values)
                        .any(|(column, value)| {
                            row.get(column).and_then(Option::as_ref) != Some(value)
                        })
                {
                    return Err(ManagementError::InvalidEvidence);
                }
                let private = self
                    .private
                    .iter()
                    .find(|(candidate, _)| candidate == key)
                    .map(|(_, row)| row);
                for (column, public) in row {
                    if let Some(value) = private.and_then(|row| row.get(column)).unwrap_or(public) {
                        bytes = bytes
                            .checked_add(value.len())
                            .ok_or(ManagementError::Budget)?;
                        if bytes > MAX_BYTES {
                            return Err(ManagementError::Budget);
                        }
                    }
                }
            }
        }
        for (index, (key, row)) in self.private.iter().enumerate() {
            if self.private[..index]
                .iter()
                .any(|(earlier, _)| earlier == key)
            {
                return Err(ManagementError::InvalidEvidence);
            }
            let table = self
                .table(&key.table)
                .ok_or(ManagementError::InvalidEvidence)?;
            let public = table
                .rows
                .iter()
                .find(|row| {
                    key.values.len() == table.primary_key.len()
                        && table
                            .primary_key
                            .iter()
                            .zip(&key.values)
                            .all(|(column, expected)| value(row, column) == Some(expected))
                })
                .ok_or(ManagementError::InvalidEvidence)?;
            if row.keys().any(|column| !public.contains_key(column))
                || row.iter().any(|(column, value)| {
                    !confidential(&key.table, column) && public.get(column) != Some(value)
                })
            {
                return Err(ManagementError::InvalidEvidence);
            }
        }
        if frontier.next().is_some()
            || self
                .frontier
                .iter()
                .enumerate()
                .any(|(index, key)| self.frontier[..index].contains(key))
        {
            return Err(ManagementError::InvalidEvidence);
        }
        Ok(())
    }
    /// History can restore its original schema, but only the current complete
    /// catalog is a fresh execution fence. No missing cell is synthesized.
    pub(crate) fn validate_current(&self) -> Result<()> {
        self.validate()?;
        if self.schema_versions != history::current_versions() {
            return Err(ManagementError::Changed);
        }
        Ok(())
    }
    pub fn table(&self, name: &str) -> Option<&NativeTable> {
        self.tables.iter().find(|t| t.table == name)
    }
    pub fn row(&self, table: &str, key: &[&str]) -> Option<&NativeRow> {
        let table = self.table(table)?;
        table.rows.iter().find(|r| {
            table
                .primary_key
                .iter()
                .zip(key)
                .all(|(c, v)| r.get(c).and_then(Option::as_deref) == Some(*v))
                && key.len() == table.primary_key.len()
        })
    }
    /// Original private tuple for internal database records, never ordinary wire.
    pub(crate) fn original_row(&self, table: &str, key: &[&str]) -> Option<NativeRow> {
        let mut row = self.row(table, key)?.clone();
        for column in &self.table(table)?.columns {
            row.insert(
                column.clone(),
                self.original_value(table, key, column).map(str::to_owned),
            );
        }
        Some(row)
    }
    pub fn original_value<'a>(
        &'a self,
        table: &str,
        key: &[&str],
        column: &str,
    ) -> Option<&'a str> {
        self.private
            .iter()
            .find(|(k, _)| {
                k.table == table && k.values.iter().map(String::as_str).eq(key.iter().copied())
            })
            .and_then(|(_, row)| value(row, column))
            .or_else(|| self.row(table, key).and_then(|row| value(row, column)))
    }
    pub fn has_private_material(&self) -> bool {
        !self.private.is_empty()
    }
    /// Only this exact record's hidden cells can restore stored authority.
    pub(crate) fn private_record_exact<'a>(
        &'a self,
        key: &NativeKey,
    ) -> Option<impl Iterator<Item = (&'a String, Option<&'a String>)> + use<'a>> {
        if key.table != super::schema::RECORDS || key.values.len() != 4 {
            return None;
        }
        let hidden = &self
            .private
            .iter()
            .find(|(candidate, _)| candidate == key)?
            .1;
        if ["binding", "native_before", "result"]
            .iter()
            .any(|column| value(hidden, column).is_none())
        {
            return None;
        }
        let refs = key.values.iter().map(String::as_str).collect::<Vec<_>>();
        self.original_cells(&key.table, &refs)
    }
    #[cfg(test)]
    pub(crate) fn private_material(&self) -> Vec<(NativeKey, NativeRow)> {
        self.private
            .iter()
            .map(|(key, row)| (key.clone(), row.as_ref().clone()))
            .collect()
    }
    pub(crate) fn private_rows(&self) -> &[(NativeKey, std::sync::Arc<NativeRow>)] {
        &self.private
    }
    pub(crate) fn original_cells<'a>(
        &'a self,
        table: &str,
        key: &[&str],
    ) -> Option<impl Iterator<Item = (&'a String, Option<&'a String>)> + use<'a>> {
        let public = self.row(table, key)?;
        let private = self
            .private
            .iter()
            .find(|(candidate, _)| {
                candidate.table == table
                    && candidate
                        .values
                        .iter()
                        .map(String::as_str)
                        .eq(key.iter().copied())
            })
            .map(|(_, row)| row);
        Some(public.iter().map(move |(column, value)| {
            (
                column,
                private
                    .and_then(|row| row.get(column))
                    .unwrap_or(value)
                    .as_ref(),
            )
        }))
    }
    pub(crate) fn original_row_matches(
        &self,
        table: &str,
        key: &[&str],
        expected: &NativeRow,
    ) -> bool {
        self.original_cells(table, key).is_some_and(|cells| {
            cells.eq(expected
                .iter()
                .map(|(column, value)| (column, value.as_ref())))
        })
    }
    #[cfg(test)]
    pub(crate) fn restore_private(&mut self, private: Vec<(NativeKey, NativeRow)>) -> Result<()> {
        self.private = private
            .into_iter()
            .map(|(key, row)| (key, std::sync::Arc::new(row)))
            .collect();
        self.validate()
    }
    /// Inventory evidence excluding management records themselves; used only for
    /// evidence recheck so its own newly inserted attestation is not a phantom.
    pub fn evidence_inventory(&self) -> Self {
        let mut result = self.clone();
        result.tables.retain(|t| t.table != super::schema::RECORDS);
        result
            .frontier
            .retain(|k| k.table != super::schema::RECORDS);
        result
            .private
            .retain(|(k, _)| k.table != super::schema::RECORDS);
        result
    }
    /// Only the record layer supplies verified exact self-record keys. All other
    /// management rows, memberships and private material remain in the fence.
    pub(crate) fn without_records(mut self, keys: &[NativeKey]) -> Self {
        let frame = &mut self;
        if let Some(table) = frame
            .tables
            .iter_mut()
            .find(|table| table.table == super::schema::RECORDS)
        {
            let primary_key = &table.primary_key;
            table.rows.retain(|row| {
                !keys.iter().any(|key| {
                    key.table == table.table
                        && primary_key.iter().zip(&key.values).all(|(column, value)| {
                            row.get(column).and_then(Option::as_ref) == Some(value)
                        })
                })
            });
        }
        frame.frontier.retain(|key| !keys.contains(key));
        frame.private.retain(|(key, _)| !keys.contains(key));
        self
    }
}

fn entity<E: EntityTrait + Default>() -> NativeTable {
    let mut columns: Vec<_> = E::Column::iter().map(|c| Iden::to_string(&c)).collect();
    columns.sort();
    let table = E::default().table_name().to_owned();
    let mut definitions = E::Column::iter()
        .map(|column| {
            super::catalog::entity_column(&table, &Iden::to_string(&column), &column.def())
        })
        .collect::<Vec<_>>();
    definitions.sort_by(|a, b| a.name.cmp(&b.name));
    NativeTable {
        table,
        columns: columns.into(),
        primary_key: E::PrimaryKey::iter()
            .map(|k| Iden::to_string(&k.into_column()))
            .collect::<Vec<_>>()
            .into(),
        rows: vec![],
        definitions: definitions.into(),
    }
}
fn raw(table: &str, columns: &str, pk: &str) -> NativeTable {
    let mut columns: Vec<_> = columns.split(',').map(str::to_owned).collect();
    columns.sort();
    let definitions = columns
        .iter()
        .map(|column| super::catalog::raw_column(table, column))
        .collect::<Vec<_>>();
    NativeTable {
        table: table.into(),
        columns: columns.into(),
        primary_key: pk.split(',').map(str::to_owned).collect::<Vec<_>>().into(),
        rows: vec![],
        definitions: definitions.into(),
    }
}

pub fn registry() -> Vec<NativeTable> {
    use crate::store::entities::*;
    let mut tables = vec![
        entity::<bucket::Entity>(),
        entity::<object::Entity>(),
        entity::<object_version::Entity>(),
        entity::<pin_lease::Entity>(),
        entity::<pin_lease_target::Entity>(),
        entity::<remote_pin::Entity>(),
        entity::<pin_provider_usage::Entity>(),
        entity::<pin_provider_route::Entity>(),
        entity::<remote_pin_ledger::Entity>(),
        entity::<pin_job::Entity>(),
        entity::<crate::store::pinning::jobs::history::Entity>(),
        entity::<pin_invocation_route::Entity>(),
        entity::<pin_resource_history::Entity>(),
        entity::<pin_submit_observation::Entity>(),
        entity::<pin_replica_observation::Entity>(),
        entity::<pin_lease_requirement::Entity>(),
        entity::<pin_extension_decision::Entity>(),
        entity::<pin_legacy_migration::Entity>(),
        entity::<pin_legacy_migration_item::Entity>(),
        entity::<multipart_upload::Entity>(),
        entity::<multipart_part::Entity>(),
        entity::<import_job::Entity>(),
        entity::<import_job_target::Entity>(),
        entity::<import_job_result::Entity>(),
        entity::<import_destination::Entity>(),
        entity::<import_prefix_claim::Entity>(),
        entity::<zip_batch::Entity>(),
        entity::<zip_manifest_entry::Entity>(),
        entity::<zip_root_build::Entity>(),
        entity::<zip_root_reference::Entity>(),
        entity::<standard_mutation_lease::Entity>(),
        entity::<lifecycle_action::Entity>(),
        entity::<lifecycle_transition::Entity>(),
        entity::<physical_residency::Entity>(),
        entity::<version_residency::Entity>(),
        entity::<residency_reference::Entity>(),
        raw(
            "zip_v2_executions",
            "id,owner,source,token,request_fingerprint,request_contract,bucket,source_key,captured_options,input_sha256,input_art_cid,input_art_size,state,epoch,worker,lease_until,terminal_result,created_at,updated_at",
            "id",
        ),
        raw(
            "zip_v2_manifest",
            "batch_id,path,object_key,cid,size,error_code,created_at",
            "batch_id,path",
        ),
        raw(
            "zip_v2_targets",
            "batch_id,object_key,mutation_id,expected_generation,epoch",
            "batch_id,object_key",
        ),
        raw(
            "zip_v2_import_requests",
            "batch_id,owner,bucket,source_key,client_token,prefix,source_descriptor,expected_sha256,request_contract,job_state,receipt_metadata",
            "batch_id",
        ),
        raw(
            "zip_v2_mpu_intakes",
            "original_upload_id,active_upload_id,execution_id,owner,token,bucket,archive_key,target_prefix,request_fingerprint,captured_options,captured_config,rule_revision,created_at",
            "original_upload_id",
        ),
        raw(
            "zip_v2_mpu_completions",
            "upload_id,owner,token,execution_id,bucket,archive_key,part_contract,response_xml,response_headers",
            "upload_id",
        ),
        raw(
            super::schema::RECORDS,
            "database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at",
            "database_scope,plan_id,item_id,kind",
        ),
        raw(
            super::schema::RETIREMENTS,
            "allocation_domain,database_scope,plan_id,item_id,principal,native_identity,binding,inventory,disposition,unresolved,state,created_at,record_kind",
            "allocation_domain",
        ),
    ];
    if let Some(table) = tables.iter_mut().find(|t| t.table == "zip_batches") {
        table.columns.push("legacy_input_proof_json".into());
        table.columns.sort();
        table.definitions.push(super::catalog::raw_column(
            "zip_batches",
            "legacy_input_proof_json",
        ));
        table.definitions.sort_by(|a, b| a.name.cmp(&b.name));
    }
    tables.sort_by(|a, b| a.table.cmp(&b.table));
    tables
}

pub(crate) fn confidential(table: &str, column: &str) -> bool {
    matches!(
        column,
        "metadata"
            | "metadata_json"
            | "key_wrap"
            | "sse_c_key_fingerprint"
            | "source_value"
            | "source_descriptor"
            | "input_identity"
            | "legacy_input_proof_json"
            | "failure_message"
            | "last_error"
            | "last_error_text"
            | "first_error"
            | "safe_error"
            | "receipt_metadata"
            | "response_headers"
            | "captured_options"
            | "captured_config"
            | "request_contract"
            | "terminal_result"
            | "tags_json"
            | "response_xml"
    ) || table == "pin_management_records"
        && matches!(column, "binding" | "native_before" | "result")
        || table == "pin_management_retirements" && matches!(column, "binding" | "inventory")
        || table == "pin_legacy_migration_items"
            && matches!(column, "before_image" | "after_image" | "bindings")
        || table == "pin_legacy_migrations" && matches!(column, "declaration" | "confirmation")
        || matches!(column, "verification_receipt" | "last_error_redacted")
        || matches!(
            column,
            "pin_decision_json" | "zip_intent_json" | "root_capture_json"
        )
        || table == "pin_extension_decisions" && column == "snapshot"
        || table == "pin_resource_history" && column == "ledger"
}

pub async fn database_identity<C: ConnectionTrait>(db: &C) -> Result<String> {
    let sql = match db.get_database_backend() {
        DatabaseBackend::Sqlite => {
            "SELECT file AS identity FROM pragma_database_list WHERE name='main'"
        }
        DatabaseBackend::Postgres => {
            "SELECT CASE WHEN inet_server_addr() IS NULL OR inet_server_port() IS NULL THEN 'diagnostic:postgres-local' ELSE 'postgres:' || d.oid::text || ':' || n.oid::text || ':' || inet_server_addr()::text || ':' || inet_server_port()::text END AS identity FROM pg_catalog.pg_database d,pg_catalog.pg_namespace n WHERE d.datname=current_database() AND n.nspname=current_schema()"
        }
        _ => return Err(ManagementError::Schema),
    };
    let identity: String = db
        .query_one(Statement::from_string(db.get_database_backend(), sql))
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::InvalidEvidence)?
        .try_get("", "identity")
        .map_err(|_| ManagementError::InvalidEvidence)?;
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        if identity.is_empty() {
            return Ok("diagnostic:sqlite-memory".into());
        }
        return Ok(format!(
            "sqlite:file:{}",
            identity::file_identity(&identity)?
        ));
    }
    if identity.starts_with("diagnostic:") {
        return Ok(identity);
    }
    let allowed=db.query_one(Statement::from_string(DatabaseBackend::Postgres,"SELECT has_function_privilege('pg_catalog.pg_control_system()','EXECUTE') AND ((SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user) OR pg_has_role(current_user,'pg_monitor','MEMBER')) AS allowed")).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?.try_get::<bool>("","allowed").map_err(|_|ManagementError::InvalidEvidence)?;
    if !allowed {
        return Ok("diagnostic:postgres-system-identity".into());
    }
    let system = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT system_identifier::text AS identity FROM pg_catalog.pg_control_system()",
        ))
        .await;
    let Some(system) = system
        .ok()
        .flatten()
        .and_then(|row| row.try_get::<String>("", "identity").ok())
    else {
        return Ok("diagnostic:postgres-system-identity".into());
    };
    Ok(format!("{identity}:{system}"))
}

pub async fn capture<C: ConnectionTrait>(db: &C) -> Result<NativeFrame> {
    let work = super::records::SharedDecodeBudget::default();
    capture_with_work(db, super::records::DecodeWork::shared(&work)).await
}
pub(crate) async fn capture_with_work<C: ConnectionTrait>(
    db: &C,
    work: super::records::DecodeWork<'_>,
) -> Result<NativeFrame> {
    let versions = super::schema::validate(db).await?;
    let backend = db.get_database_backend();
    work.with(|work| work.nodes(1 + current_catalog().len()))?;
    for table in current_catalog() {
        work.copy(&table.table)?;
    }
    let mut tables = current_catalog().to_vec();
    let mut count = 0_u64;
    let mut bytes = 0_u64;
    super::catalog::validate(db, &tables).await?;
    // Bound all original text, including non-exportable cells, before fetching it.
    for table in &tables {
        let columns_sql = if backend == DatabaseBackend::Sqlite {
            "SELECT name AS column_name FROM pragma_table_info($1) ORDER BY name"
        } else {
            "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 ORDER BY column_name"
        };
        let columns: Vec<String> = db
            .query_all(Statement::from_sql_and_values(
                backend,
                columns_sql,
                [table.table.clone().into()],
            ))
            .await
            .map_err(|_| ManagementError::Schema)?
            .iter()
            .map(|r| r.try_get("", "column_name"))
            .collect::<std::result::Result<_, _>>()
            .map_err(|_| ManagementError::Schema)?;
        if columns.as_slice() != table.columns.as_slice() {
            return Err(ManagementError::Schema);
        }
        let terms: Vec<_> = table
            .columns
            .iter()
            .map(|c| {
                if backend == DatabaseBackend::Sqlite {
                    format!("LENGTH(CAST(COALESCE(CAST(\"{c}\" AS TEXT),'') AS BLOB))")
                } else {
                    format!("OCTET_LENGTH(COALESCE(CAST(\"{c}\" AS TEXT),''))::bigint")
                }
            })
            .collect();
        let row = db
            .query_one(Statement::from_string(
                backend,
                format!(
                    "SELECT COUNT(*) AS rows,COALESCE(SUM({}),0)::bigint AS bytes FROM \"{}\"",
                    terms.join("+"),
                    table.table
                )
                .replace(
                    "::bigint",
                    if backend == DatabaseBackend::Sqlite {
                        ""
                    } else {
                        "::bigint"
                    },
                ),
            ))
            .await
            .map_err(|_| ManagementError::Database)?
            .ok_or(ManagementError::InvalidEvidence)?;
        let records = u64::try_from(
            row.try_get::<i64>("", "rows")
                .map_err(|_| ManagementError::InvalidEvidence)?,
        )
        .map_err(|_| ManagementError::Budget)?;
        let text = u64::try_from(
            row.try_get::<i64>("", "bytes")
                .map_err(|_| ManagementError::InvalidEvidence)?,
        )
        .map_err(|_| ManagementError::Budget)?;
        count = count.checked_add(records).ok_or(ManagementError::Budget)?;
        bytes = bytes.checked_add(text).ok_or(ManagementError::Budget)?;
        if count > MAX_ROWS as u64 || bytes > MAX_BYTES as u64 {
            return Err(ManagementError::Budget);
        }
        if table.table != super::schema::RECORDS {
            for _ in 0..2 {
                work.index_cells(
                    (records as usize)
                        .checked_mul(table.columns.len())
                        .ok_or(ManagementError::Budget)?,
                    text as usize,
                )?;
            }
        }
    }
    work.with(|work| work.nodes(2))?;
    let mut frontier = Vec::new();
    let mut private = Vec::new();
    for table in &mut tables {
        if table.table == super::schema::RECORDS {
            for original in super::records::current_management_rows(db, work).await? {
                work.copy(&table.columns)?;
                work.with(|work| work.nodes(2 + table.primary_key.len()))?;
                let mut public = NativeRow::new();
                let mut hidden = false;
                for column in &table.columns {
                    let value = original
                        .get(column)
                        .ok_or(ManagementError::InvalidEvidence)?;
                    if confidential(&table.table, column) {
                        hidden |= value.is_some();
                        public.insert(column.clone(), None);
                    } else {
                        work.copy(value)?;
                        public.insert(column.clone(), value.clone());
                    }
                }
                work.copy(&table.table)?;
                for column in &table.primary_key {
                    work.copy(value(&original, column).ok_or(ManagementError::InvalidEvidence)?)?;
                }
                let key = NativeKey {
                    table: table.table.clone(),
                    values: table
                        .primary_key
                        .iter()
                        .map(|column| {
                            value(&original, column)
                                .map(str::to_owned)
                                .ok_or(ManagementError::InvalidEvidence)
                        })
                        .collect::<Result<_>>()?,
                };
                if hidden {
                    work.copy(&key)?;
                    work.with(|work| work.nodes(1))?;
                    private.push((key.clone(), original));
                }
                frontier.push(key);
                table.rows.push(public);
            }
            continue;
        }
        let fields = table
            .columns
            .iter()
            .map(|c| format!("CAST(\"{c}\" AS TEXT) AS \"{c}\""))
            .collect::<Vec<_>>()
            .join(",");
        let order = table
            .primary_key
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(",");
        let rows = db
            .query_all(Statement::from_string(
                backend,
                format!(
                    "SELECT {fields} FROM \"{}\" ORDER BY {order} LIMIT {}",
                    table.table,
                    MAX_ROWS + 1
                ),
            ))
            .await
            .map_err(|_| ManagementError::Database)?;
        for row in rows {
            work.copy(&table.columns)?;
            let mut native = NativeRow::new();
            let mut hidden = NativeRow::new();
            for column in &table.columns {
                let value: Option<String> = row
                    .try_get("", column)
                    .map_err(|_| ManagementError::InvalidEvidence)?;
                if confidential(&table.table, column) && value.is_some() {
                    work.copy(column)?;
                    hidden.insert(column.clone(), value);
                    native.insert(column.clone(), None);
                } else {
                    native.insert(column.clone(), value);
                }
            }
            work.copy(&table.table)?;
            work.with(|work| work.nodes(2 + table.primary_key.len()))?;
            for column in &table.primary_key {
                work.copy(
                    native
                        .get(column)
                        .and_then(Option::as_ref)
                        .ok_or(ManagementError::InvalidEvidence)?,
                )?;
            }
            let key = NativeKey {
                table: table.table.clone(),
                values: table
                    .primary_key
                    .iter()
                    .map(|c| {
                        native
                            .get(c)
                            .and_then(Clone::clone)
                            .ok_or(ManagementError::InvalidEvidence)
                    })
                    .collect::<Result<_>>()?,
            };
            if !hidden.is_empty() {
                work.copy(&key)?;
                work.with(|work| work.nodes(2))?;
                private.push((key.clone(), std::sync::Arc::new(hidden)));
            }
            frontier.push(key);
            table.rows.push(native);
        }
    }
    let scope = tables
        .iter()
        .find(|t| t.table == super::schema::RECORDS)
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
        .filter(|r| value(r, "kind") == Some("database_scope"))
        .collect::<Vec<_>>();
    if scope.len() != 1 {
        return Err(ManagementError::Schema);
    }
    work.copy(value(scope[0], "database_scope").ok_or(ManagementError::Schema)?)?;
    Ok(NativeFrame {
        schema_versions: versions,
        database_scope: value(scope[0], "database_scope")
            .ok_or(ManagementError::Schema)?
            .into(),
        database_identity: database_identity(db).await?,
        tables,
        frontier,
        private,
    })
}

/// Complete raw registry guard; selective readers must not substitute the size
/// of their selected management rows for the database-wide SQL inventory.
pub(crate) async fn check_raw_inventory<C: ConnectionTrait>(db: &C) -> Result<()> {
    raw_inventory_usage(db).await.map(|_| ())
}
pub async fn raw_inventory_usage<C: ConnectionTrait>(db: &C) -> Result<(usize, usize)> {
    let backend = db.get_database_backend();
    let mut rows = 0usize;
    let mut bytes = 0usize;
    for table in history::current_catalog() {
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
        let row=db.query_one(Statement::from_string(backend,format!("SELECT COUNT(*) AS rows,CAST(COALESCE(SUM({terms}),0) AS BIGINT) AS bytes FROM \"{}\"",table.table))).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
        let count = usize::try_from(
            row.try_get::<i64>("", "rows")
                .map_err(|_| ManagementError::InvalidEvidence)?,
        )
        .map_err(|_| ManagementError::Budget)?;
        let length = usize::try_from(
            row.try_get::<i64>("", "bytes")
                .map_err(|_| ManagementError::InvalidEvidence)?,
        )
        .map_err(|_| ManagementError::Budget)?;
        rows = rows.checked_add(count).ok_or(ManagementError::Budget)?;
        bytes = bytes.checked_add(length).ok_or(ManagementError::Budget)?;
        if rows > MAX_ROWS || bytes > MAX_BYTES {
            return Err(ManagementError::Budget);
        }
    }
    Ok((rows, bytes))
}

pub trait RawCells {
    fn raw_cell(&self, column: &str) -> Option<&Option<String>>;
}
impl RawCells for NativeRow {
    fn raw_cell(&self, column: &str) -> Option<&Option<String>> {
        self.get(column)
    }
}
impl<T: RawCells + ?Sized> RawCells for &T {
    fn raw_cell(&self, column: &str) -> Option<&Option<String>> {
        (*self).raw_cell(column)
    }
}
impl<T: RawCells + ?Sized> RawCells for &mut T {
    fn raw_cell(&self, column: &str) -> Option<&Option<String>> {
        (**self).raw_cell(column)
    }
}
impl<T: RawCells + ?Sized> RawCells for std::sync::Arc<T> {
    fn raw_cell(&self, column: &str) -> Option<&Option<String>> {
        self.as_ref().raw_cell(column)
    }
}
pub trait RowValue<'a> {
    fn value(self, column: &str) -> Option<&'a str>;
}
impl<'a, T: RawCells + ?Sized> RowValue<'a> for &'a T {
    fn value(self, column: &str) -> Option<&'a str> {
        self.raw_cell(column).and_then(Option::as_deref)
    }
}
pub fn value<'a>(row: impl RowValue<'a>, column: &str) -> Option<&'a str> {
    row.value(column)
}

/// Comparison helper only: original SQL text stays in every native fence.
pub fn parse_time(raw: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    if let Ok(time) = chrono::DateTime::parse_from_rfc3339(raw)
        .or_else(|_| chrono::DateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f%#z"))
        .or_else(|_| chrono::DateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f %:z"))
    {
        return Ok(time.with_timezone(&chrono::Utc));
    }
    // The registered SQLite timestamp migrations use UTC for naive SQL text.
    chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
        .map(|time| time.and_utc())
        .map_err(|_| ManagementError::InvalidEvidence)
}

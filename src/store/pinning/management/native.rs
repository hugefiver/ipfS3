//! SQL-native complete inventory. Time text is never round-tripped through chrono.
use std::collections::BTreeMap;
use sea_orm::{ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, Iterable, PrimaryKeyToColumn, Statement, sea_query::Iden};
use serde::{Deserialize, Serialize};
use super::{ManagementError, Result};
mod identity;

pub const MAX_ROWS: usize = 4096;
pub const MAX_BYTES: usize = 4 * 1024 * 1024;
pub type NativeRow = BTreeMap<String, Option<String>>;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeKey { pub table: String, pub values: Vec<String> }

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTable {
    pub table: String,
    pub columns: Vec<String>,
    pub primary_key: Vec<String>,
    pub rows: Vec<NativeRow>,
    pub definitions: Vec<super::catalog::NativeColumn>,
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
    private: Vec<(NativeKey, NativeRow)>,
}

impl NativeFrame {
    pub fn validate(&self)->Result<()> {
        let registry=registry();
        if self.database_scope.is_empty() || self.database_identity.is_empty() || self.tables.len()!=registry.len() {return Err(ManagementError::InvalidEvidence);}
        let mut count=0_usize;let mut bytes=0_usize;let mut frontier=Vec::new();
        for (table,expected) in self.tables.iter().zip(registry) {
            if table.table!=expected.table || table.columns!=expected.columns || table.primary_key!=expected.primary_key || table.definitions!=expected.definitions {return Err(ManagementError::InvalidEvidence);}
            count=count.checked_add(table.rows.len()).ok_or(ManagementError::Budget)?;
            if count>MAX_ROWS {return Err(ManagementError::Budget);}
            for row in &table.rows {
                if row.keys().collect::<Vec<_>>()!=table.columns.iter().collect::<Vec<_>>() {return Err(ManagementError::InvalidEvidence);}
                for value in row.values().flatten() {bytes=bytes.checked_add(value.len()).ok_or(ManagementError::Budget)?;if bytes>MAX_BYTES{return Err(ManagementError::Budget);}}
                frontier.push(NativeKey {table:table.table.clone(),values:table.primary_key.iter().map(|c|row.get(c).and_then(Clone::clone).ok_or(ManagementError::InvalidEvidence)).collect::<Result<_>>()?});
            }
        }
        for (_,row) in &self.private {for value in row.values().flatten(){bytes=bytes.checked_add(value.len()).ok_or(ManagementError::Budget)?;if bytes>MAX_BYTES{return Err(ManagementError::Budget);}}}
        if frontier!=self.frontier || frontier.iter().collect::<std::collections::BTreeSet<_>>().len()!=frontier.len() {return Err(ManagementError::InvalidEvidence);}
        Ok(())
    }
    pub fn table(&self, name: &str) -> Option<&NativeTable> { self.tables.iter().find(|t| t.table == name) }
    pub fn row(&self, table: &str, key: &[&str]) -> Option<&NativeRow> {
        let table = self.table(table)?;
        table.rows.iter().find(|r| table.primary_key.iter().zip(key).all(|(c,v)| r.get(c).and_then(Option::as_deref) == Some(*v)) && key.len() == table.primary_key.len())
    }
    /// Original private tuple for internal database records, never ordinary wire.
    pub(crate) fn original_row(&self,table:&str,key:&[&str])->Option<NativeRow> {
        let mut row=self.row(table,key)?.clone();
        for column in &self.table(table)?.columns {row.insert(column.clone(),self.original_value(table,key,column).map(str::to_owned));}
        Some(row)
    }
    pub fn original_value<'a>(&'a self,table:&str,key:&[&str],column:&str)->Option<&'a str> {
        self.private.iter().find(|(k,_)|k.table==table&&k.values.iter().map(String::as_str).eq(key.iter().copied()))
            .and_then(|(_,row)|value(row,column)).or_else(||self.row(table,key).and_then(|row|value(row,column)))
    }
    pub fn has_private_material(&self) -> bool { !self.private.is_empty() }
    pub(crate) fn private_material(&self)->Vec<(NativeKey,NativeRow)> {self.private.clone()}
    pub(crate) fn restore_private(&mut self,private:Vec<(NativeKey,NativeRow)>)->Result<()> {self.private=private;self.validate()}
    /// Inventory evidence excluding management records themselves; used only for
    /// evidence recheck so its own newly inserted attestation is not a phantom.
    pub fn evidence_inventory(&self) -> Self {
        let mut result = self.clone();
        result.tables.retain(|t| t.table != super::schema::RECORDS);
        result.frontier.retain(|k| k.table != super::schema::RECORDS);
        result.private.retain(|(k,_)| k.table != super::schema::RECORDS);
        result
    }
    /// Only the record layer supplies verified exact self-record keys. All other
    /// management rows, memberships and private material remain in the fence.
    pub(crate) fn without_records(&self, keys:&[NativeKey])->Self {
        let mut frame=self.clone();
        if let Some(table)=frame.tables.iter_mut().find(|table|table.table==super::schema::RECORDS) {
            let primary_key=table.primary_key.clone();
            table.rows.retain(|row|!keys.iter().any(|key|key.table==table.table && primary_key.iter().zip(&key.values).all(|(column,value)|row.get(column).and_then(Option::as_ref)==Some(value))));
        }
        frame.frontier.retain(|key|!keys.contains(key));
        frame.private.retain(|(key,_)|!keys.contains(key));
        frame
    }
}

fn entity<E: EntityTrait + Default>() -> NativeTable {
    let mut columns: Vec<_> = E::Column::iter().map(|c| Iden::to_string(&c)).collect(); columns.sort();
    let table=E::default().table_name().to_owned();
    let mut definitions=E::Column::iter().map(|column|super::catalog::entity_column(&table,&Iden::to_string(&column),&column.def())).collect::<Vec<_>>();definitions.sort_by(|a,b|a.name.cmp(&b.name));
    NativeTable { table, columns, primary_key: E::PrimaryKey::iter().map(|k| Iden::to_string(&k.into_column())).collect(), rows: vec![], definitions }
}
fn raw(table: &str, columns: &str, pk: &str) -> NativeTable {
    let mut columns: Vec<_> = columns.split(',').map(str::to_owned).collect(); columns.sort();
    let definitions=columns.iter().map(|column|super::catalog::raw_column(table,column)).collect();
    NativeTable { table: table.into(), columns, primary_key: pk.split(',').map(str::to_owned).collect(), rows: vec![], definitions }
}

pub fn registry() -> Vec<NativeTable> {
    use crate::store::entities::*;
    let mut tables = vec![
        entity::<bucket::Entity>(), entity::<object::Entity>(), entity::<object_version::Entity>(),
        entity::<pin_lease::Entity>(), entity::<pin_lease_target::Entity>(), entity::<remote_pin::Entity>(),
        entity::<pin_provider_usage::Entity>(), entity::<pin_provider_route::Entity>(),
        entity::<remote_pin_ledger::Entity>(), entity::<pin_job::Entity>(),
        entity::<crate::store::pinning::jobs::history::Entity>(),
        entity::<pin_invocation_route::Entity>(),
        entity::<pin_resource_history::Entity>(),
        entity::<pin_submit_observation::Entity>(), entity::<pin_replica_observation::Entity>(),
        entity::<pin_lease_requirement::Entity>(), entity::<pin_extension_decision::Entity>(),
        entity::<pin_legacy_migration::Entity>(), entity::<pin_legacy_migration_item::Entity>(),
        entity::<multipart_upload::Entity>(), entity::<multipart_part::Entity>(), entity::<import_job::Entity>(),
        entity::<import_job_target::Entity>(), entity::<import_job_result::Entity>(), entity::<import_destination::Entity>(),
        entity::<import_prefix_claim::Entity>(), entity::<zip_batch::Entity>(), entity::<zip_manifest_entry::Entity>(),
        entity::<zip_root_build::Entity>(), entity::<zip_root_reference::Entity>(),
        entity::<standard_mutation_lease::Entity>(), entity::<lifecycle_action::Entity>(), entity::<lifecycle_transition::Entity>(),
        entity::<physical_residency::Entity>(), entity::<version_residency::Entity>(), entity::<residency_reference::Entity>(),
        raw("zip_v2_executions", "id,owner,source,token,request_fingerprint,request_contract,bucket,source_key,captured_options,input_sha256,input_art_cid,input_art_size,state,epoch,worker,lease_until,terminal_result,created_at,updated_at", "id"),
        raw("zip_v2_manifest", "batch_id,path,object_key,cid,size,error_code,created_at", "batch_id,path"),
        raw("zip_v2_targets", "batch_id,object_key,mutation_id,expected_generation,epoch", "batch_id,object_key"),
        raw("zip_v2_import_requests", "batch_id,owner,bucket,source_key,client_token,prefix,source_descriptor,expected_sha256,request_contract,job_state,receipt_metadata", "batch_id"),
        raw("zip_v2_mpu_intakes", "original_upload_id,active_upload_id,execution_id,owner,token,bucket,archive_key,target_prefix,request_fingerprint,captured_options,captured_config,rule_revision,created_at", "original_upload_id"),
        raw("zip_v2_mpu_completions", "upload_id,owner,token,execution_id,bucket,archive_key,part_contract,response_xml,response_headers", "upload_id"),
        raw(super::schema::RECORDS, "database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at", "database_scope,plan_id,item_id,kind"),
        raw(super::schema::RETIREMENTS, "allocation_domain,database_scope,plan_id,item_id,principal,native_identity,binding,inventory,disposition,unresolved,state,created_at,record_kind", "allocation_domain"),
    ];
    if let Some(table) = tables.iter_mut().find(|t| t.table == "zip_batches") { table.columns.push("legacy_input_proof_json".into()); table.columns.sort();table.definitions.push(super::catalog::raw_column("zip_batches","legacy_input_proof_json"));table.definitions.sort_by(|a,b|a.name.cmp(&b.name)); }
    tables.sort_by(|a,b| a.table.cmp(&b.table)); tables
}

fn confidential(table: &str, column: &str) -> bool {
    matches!(column, "metadata" | "metadata_json" | "key_wrap" | "sse_c_key_fingerprint" | "source_value" | "source_descriptor" | "input_identity" | "legacy_input_proof_json" | "failure_message" | "last_error" | "last_error_text" | "first_error" | "safe_error" | "receipt_metadata" | "response_headers" | "captured_options" | "captured_config" | "request_contract" | "terminal_result" | "tags_json" | "response_xml")
        || table == "pin_management_records" && matches!(column, "binding" | "native_before" | "result")
        || table == "pin_management_retirements" && matches!(column, "binding" | "inventory")
        || table == "pin_legacy_migration_items" && matches!(column, "before_image" | "after_image" | "bindings")
        || table == "pin_legacy_migrations" && matches!(column,"declaration"|"confirmation")
        || matches!(column,"verification_receipt"|"last_error_redacted")
        || matches!(column,"pin_decision_json"|"zip_intent_json"|"root_capture_json")
        || table=="pin_extension_decisions" && column=="snapshot"
        || table == "pin_resource_history" && column=="ledger"
}

pub async fn database_identity<C: ConnectionTrait>(db: &C) -> Result<String> {
    let sql = match db.get_database_backend() {
        DatabaseBackend::Sqlite => "SELECT file AS identity FROM pragma_database_list WHERE name='main'",
        DatabaseBackend::Postgres => "SELECT CASE WHEN inet_server_addr() IS NULL OR inet_server_port() IS NULL THEN 'diagnostic:postgres-local' ELSE 'postgres:' || d.oid::text || ':' || n.oid::text || ':' || inet_server_addr()::text || ':' || inet_server_port()::text END AS identity FROM pg_catalog.pg_database d,pg_catalog.pg_namespace n WHERE d.datname=current_database() AND n.nspname=current_schema()",
        _ => return Err(ManagementError::Schema),
    };
    let identity:String=db.query_one(Statement::from_string(db.get_database_backend(), sql)).await.map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::InvalidEvidence)?.try_get("", "identity").map_err(|_| ManagementError::InvalidEvidence)?;
    if db.get_database_backend()==DatabaseBackend::Sqlite {
        if identity.is_empty() {return Ok("diagnostic:sqlite-memory".into());}
        return Ok(format!("sqlite:file:{}",identity::file_identity(&identity)?));
    }
    if identity.starts_with("diagnostic:") {return Ok(identity);}
    let allowed=db.query_one(Statement::from_string(DatabaseBackend::Postgres,"SELECT has_function_privilege('pg_catalog.pg_control_system()','EXECUTE') AND ((SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user) OR pg_has_role(current_user,'pg_monitor','MEMBER')) AS allowed")).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?.try_get::<bool>("","allowed").map_err(|_|ManagementError::InvalidEvidence)?;
    if !allowed {return Ok("diagnostic:postgres-system-identity".into());}
    let system=db.query_one(Statement::from_string(DatabaseBackend::Postgres,"SELECT system_identifier::text AS identity FROM pg_catalog.pg_control_system()")).await;
    let Some(system)=system.ok().flatten().and_then(|row|row.try_get::<String>("","identity").ok()) else{return Ok("diagnostic:postgres-system-identity".into())};
    Ok(format!("{identity}:{system}"))
}

pub async fn capture<C: ConnectionTrait>(db: &C) -> Result<NativeFrame> {
    let versions = super::schema::validate(db).await?;
    let backend = db.get_database_backend();
    let mut tables = registry(); let mut count = 0_u64; let mut bytes = 0_u64;
    super::catalog::validate(db,&tables).await?;
    // Bound all original text, including non-exportable cells, before fetching it.
    for table in &tables {
        let columns_sql = if backend == DatabaseBackend::Sqlite { "SELECT name AS column_name FROM pragma_table_info($1) ORDER BY name" } else { "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 ORDER BY column_name" };
        let columns: Vec<String> = db.query_all(Statement::from_sql_and_values(backend, columns_sql, [table.table.clone().into()])).await.map_err(|_| ManagementError::Schema)?
            .iter().map(|r| r.try_get("", "column_name")).collect::<std::result::Result<_,_>>().map_err(|_| ManagementError::Schema)?;
        if columns != table.columns { return Err(ManagementError::Schema); }
        let terms: Vec<_> = table.columns.iter().map(|c| if backend == DatabaseBackend::Sqlite { format!("LENGTH(CAST(COALESCE(CAST(\"{c}\" AS TEXT),'') AS BLOB))") } else { format!("OCTET_LENGTH(COALESCE(CAST(\"{c}\" AS TEXT),''))::bigint") }).collect();
        let row = db.query_one(Statement::from_string(backend, format!("SELECT COUNT(*) AS rows,COALESCE(SUM({}),0)::bigint AS bytes FROM \"{}\"", terms.join("+"), table.table).replace("::bigint", if backend == DatabaseBackend::Sqlite { "" } else { "::bigint" }))).await.map_err(|_| ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
        let records = u64::try_from(row.try_get::<i64>("", "rows").map_err(|_| ManagementError::InvalidEvidence)?).map_err(|_| ManagementError::Budget)?;
        let text = u64::try_from(row.try_get::<i64>("", "bytes").map_err(|_| ManagementError::InvalidEvidence)?).map_err(|_| ManagementError::Budget)?;
        count = count.checked_add(records).ok_or(ManagementError::Budget)?;
        bytes = bytes.checked_add(text).ok_or(ManagementError::Budget)?;
        if count > MAX_ROWS as u64 || bytes > MAX_BYTES as u64 { return Err(ManagementError::Budget); }
    }
    let mut frontier = Vec::new(); let mut private = Vec::new();
    for table in &mut tables {
        let fields = table.columns.iter().map(|c| format!("CAST(\"{c}\" AS TEXT) AS \"{c}\"")).collect::<Vec<_>>().join(",");
        let order = table.primary_key.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(",");
        let rows = db.query_all(Statement::from_string(backend, format!("SELECT {fields} FROM \"{}\" ORDER BY {order} LIMIT {}", table.table, MAX_ROWS+1))).await.map_err(|_| ManagementError::Database)?;
        for row in rows {
            let mut native = NativeRow::new(); let mut hidden = NativeRow::new();
            for column in &table.columns {
                let value: Option<String> = row.try_get("", column).map_err(|_| ManagementError::InvalidEvidence)?;
                if confidential(&table.table, column) && value.is_some() { hidden.insert(column.clone(), value); native.insert(column.clone(), None); }
                else { native.insert(column.clone(), value); }
            }
            let key = NativeKey { table: table.table.clone(), values: table.primary_key.iter().map(|c| native.get(c).and_then(Clone::clone).ok_or(ManagementError::InvalidEvidence)).collect::<Result<_>>()? };
            if !hidden.is_empty() { private.push((key.clone(), hidden)); }
            frontier.push(key); table.rows.push(native);
        }
    }
    let scope = tables.iter().find(|t| t.table == super::schema::RECORDS).ok_or(ManagementError::Schema)?.rows.iter().filter(|r| value(r,"kind") == Some("database_scope")).collect::<Vec<_>>();
    if scope.len() != 1 { return Err(ManagementError::Schema); }
    Ok(NativeFrame { schema_versions: versions, database_scope: value(scope[0],"database_scope").ok_or(ManagementError::Schema)?.into(), database_identity: database_identity(db).await?, tables, frontier, private })
}

pub fn value<'a>(row: &'a NativeRow, column: &str) -> Option<&'a str> { row.get(column).and_then(Option::as_deref) }

/// Comparison helper only: original SQL text stays in every native fence.
pub fn parse_time(raw:&str)->Result<chrono::DateTime<chrono::Utc>> {
    if let Ok(time)=chrono::DateTime::parse_from_rfc3339(raw).or_else(|_|chrono::DateTime::parse_from_str(raw,"%Y-%m-%d %H:%M:%S%.f%#z")).or_else(|_|chrono::DateTime::parse_from_str(raw,"%Y-%m-%d %H:%M:%S%.f %:z")) {return Ok(time.with_timezone(&chrono::Utc));}
    // The registered SQLite timestamp migrations use UTC for naive SQL text.
    chrono::NaiveDateTime::parse_from_str(raw,"%Y-%m-%d %H:%M:%S%.f").map(|time|time.and_utc()).map_err(|_|ManagementError::InvalidEvidence)
}

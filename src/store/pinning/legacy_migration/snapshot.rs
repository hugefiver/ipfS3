//! SQL-native text images: no chrono round trip or timestamp tolerance.
use super::{MAX_BYTES, MAX_ROWS, SCHEMA, TableImage, refused};
use crate::{error::AppResult, store::entities::*};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, EntityTrait, Iterable, PrimaryKeyToColumn, Statement,
    sea_query::Iden,
};
use sea_orm_migration::MigratorTrait;
use std::collections::BTreeSet;

const STAGE5: &str = "m20261004_000001_rpc_submission_ledger";
const STAGE6: &str = "m20261004_000002_cluster_replica_contract";
const ZIP_CAPTURE: &str = "m20261008_000001_zip_intent_capture";
const MANAGEMENT: &str = crate::store::pinning::management::schema::VERSION;

#[path = "snapshot_management.rs"]
mod management_catalog;

fn entity<E: EntityTrait + Default>() -> TableImage {
    let mut columns: Vec<_> = E::Column::iter().map(|c| Iden::to_string(&c)).collect();
    columns.sort();
    TableImage {
        table: E::default().table_name().into(),
        columns,
        primary_key: E::PrimaryKey::iter()
            .map(|k| Iden::to_string(&k.into_column()))
            .collect(),
        rows: vec![],
    }
}

fn raw(table: &str, columns: &str, pk: &str) -> TableImage {
    let mut columns: Vec<_> = columns.split(',').map(str::to_owned).collect();
    columns.sort();
    TableImage {
        table: table.into(),
        columns,
        primary_key: pk.split(',').map(str::to_owned).collect(),
        rows: vec![],
    }
}

pub(super) fn tables(versions: &[String]) -> Vec<TableImage> {
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
        entity::<multipart_upload::Entity>(),
        entity::<import_job::Entity>(),
        entity::<zip_batch::Entity>(),
        entity::<zip_manifest_entry::Entity>(),
        entity::<standard_mutation_lease::Entity>(),
        entity::<lifecycle_action::Entity>(),
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
    ];
    if versions.iter().any(|version| version == MANAGEMENT) {
        tables.extend([
            raw(
                "pin_management_records",
                "database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at",
                "database_scope,plan_id,item_id,kind",
            ),
            raw(
                "pin_management_retirements",
                "allocation_domain,database_scope,plan_id,item_id,principal,native_identity,binding,inventory,disposition,unresolved,state,created_at,record_kind",
                "allocation_domain",
            ),
        ]);
    }
    tables
}

pub(super) async fn schema<C: ConnectionTrait>(db: &C, writing: bool) -> AppResult<Vec<String>> {
    let rows = db
        .query_all(Statement::from_string(
            db.get_database_backend(),
            "SELECT version FROM seaql_migrations ORDER BY version LIMIT 65",
        ))
        .await?;
    let actual: Vec<String> = rows
        .iter()
        .map(|r| r.try_get("", "version"))
        .collect::<Result<_, _>>()?;
    let known: Vec<String> = crate::store::migrator::Migrator::migrations()
        .iter()
        .map(|m| m.name().to_owned())
        .collect();
    // Registered prefixes must end at an explicitly supported catalog shape.
    if !matches!(
        actual.last().map(String::as_str),
        Some(STAGE5 | STAGE6 | SCHEMA | ZIP_CAPTURE | MANAGEMENT)
    ) || actual != known[..actual.len().min(known.len())]
        || actual.len() > known.len()
    {
        return Err(refused("unknown or incomplete schema"));
    }
    if writing && !actual.iter().any(|v| v == SCHEMA) {
        return Err(refused(
            "upgrade schema through normal gateway migration before apply",
        ));
    }
    if actual.iter().any(|v| v == SCHEMA) {
        super::receipt_schema::validate(db).await?;
    } else {
        super::receipt_schema::absent(db).await?;
    }
    management_catalog::validate(db, actual.iter().any(|version| version == MANAGEMENT)).await?;
    Ok(actual)
}

pub(super) async fn read<C: ConnectionTrait>(
    db: &C,
    versions: &[String],
) -> AppResult<Vec<TableImage>> {
    let backend = db.get_database_backend();
    let stage6 = versions.iter().any(|v| v == STAGE6);
    let zip_capture = versions.iter().any(|v| v == ZIP_CAPTURE);
    let mut result = Vec::new();
    let mut count = 0;
    let mut bytes = 0;
    for mut table in tables(versions) {
        match (table.table.as_str(), zip_capture) {
            ("multipart_uploads" | "import_jobs", false) => {
                table.columns.retain(|c| c != "zip_intent_json");
            }
            ("zip_batches", true) => {
                table.columns.push("legacy_input_proof_json".into());
                table.columns.sort();
            }
            _ => {}
        }
        let expected_missing = !stage6
            && matches!(
                table.table.as_str(),
                "pin_replica_observations" | "pin_lease_requirements"
            );
        let sql = if backend == DatabaseBackend::Sqlite {
            "SELECT name AS column_name FROM pragma_table_info($1) ORDER BY name"
        } else {
            "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 ORDER BY column_name"
        };
        let columns: Vec<String> = db
            .query_all(Statement::from_sql_and_values(
                backend,
                sql,
                [table.table.clone().into()],
            ))
            .await?
            .iter()
            .map(|r| r.try_get("", "column_name"))
            .collect::<Result<_, _>>()?;
        if expected_missing {
            if !columns.is_empty() {
                return Err(refused("unexpected closure table for historical schema"));
            }
            table.columns.clear();
            result.push(table);
            continue;
        }
        if columns != table.columns {
            return Err(refused("unknown columns or missing closure table"));
        }
        let fields = columns
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
            .await?;
        count += rows.len();
        if count > MAX_ROWS {
            return Err(refused("complete inventory exceeds row budget"));
        }
        for row in rows {
            let mut native = super::NativeRow::new();
            for column in &columns {
                let value: Option<String> = row.try_get("", column)?;
                bytes += value.as_ref().map_or(0, String::len) + column.len();
                if bytes > MAX_BYTES {
                    return Err(refused("complete inventory exceeds byte budget"));
                }
                native.insert(column.clone(), value);
            }
            table.rows.push(native);
        }
        result.push(table);
    }
    normalize(&mut result);
    Ok(result)
}

pub(super) async fn identity<C: ConnectionTrait>(db: &C) -> AppResult<String> {
    let sql = if db.get_database_backend() == DatabaseBackend::Postgres {
        "SELECT current_database() || ':' || current_schema() || ':' || COALESCE(inet_server_addr()::text,'local') || ':' || COALESCE(inet_server_port()::text,'local') AS identity"
    } else {
        "SELECT 'sqlite:' || file AS identity FROM pragma_database_list WHERE name='main'"
    };
    Ok(db
        .query_one(Statement::from_string(db.get_database_backend(), sql))
        .await?
        .ok_or_else(|| refused("database scope unavailable"))?
        .try_get("", "identity")?)
}

pub(super) async fn frontier<C: ConnectionTrait>(
    db: &C,
    versions: &[String],
) -> AppResult<Vec<String>> {
    if !versions.iter().any(|v| v == SCHEMA) {
        return Ok(vec![]);
    }
    let frontier = db
        .query_all(Statement::from_string(
            db.get_database_backend(),
            "SELECT id FROM pin_legacy_migrations ORDER BY id LIMIT 10001",
        ))
        .await?
        .iter()
        .map(|r| r.try_get("", "id"))
        .collect::<Result<Vec<_>, _>>()?;
    if frontier.len() > MAX_ROWS {
        return Err(refused("complete receipt frontier exceeds row budget"));
    }
    Ok(frontier)
}

pub(super) fn normalize(images: &mut [TableImage]) {
    for image in images {
        let pk = image.primary_key.clone();
        image
            .rows
            .sort_by_key(|r| pk.iter().map(|k| r.get(k).cloned()).collect::<Vec<_>>());
    }
}

pub(super) fn distinct_keys(image: &TableImage) -> bool {
    let keys: BTreeSet<_> = image
        .rows
        .iter()
        .map(|r| super::model::key(image, r))
        .collect();
    keys.len() == image.rows.len()
}

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;

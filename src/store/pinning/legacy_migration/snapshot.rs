//! SQL-native text images: no chrono round trip or timestamp tolerance.
use super::{MAX_BYTES, MAX_ROWS, SCHEMA, TableImage, refused};
use crate::store::migrations::history;
use crate::{error::AppResult, store::entities::*};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, EntityTrait, Iterable, PrimaryKeyToColumn, QueryResult,
    Statement, sea_query::Iden,
};
use std::collections::{BTreeMap, BTreeSet};

const STAGE5: &str = "m20261004_000001_rpc_submission_ledger";
const STAGE6: &str = "m20261004_000002_cluster_replica_contract";
const ZIP_CAPTURE: &str = "m20261008_000001_zip_intent_capture";
const MANAGEMENT: &str = crate::store::pinning::management::schema::VERSION;

#[path = "snapshot_management.rs"]
pub(super) mod management_catalog;

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
    if history::has_final_layout(versions) {
        let cold = import_job::Column::iter()
            .filter(|column| column.is_request())
            .map(|column| Iden::to_string(&column))
            .collect::<BTreeSet<_>>();
        tables
            .iter_mut()
            .find(|table| table.table == "import_jobs")
            .expect("import jobs snapshot")
            .columns
            .retain(|name| !cold.contains(name));
        tables.push(entity::<import_job_request::Entity>());
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
    validate_versions(&actual, writing)?;
    if history::has_stage(&actual, SCHEMA) {
        super::receipt_schema::validate(db).await?;
    } else {
        super::receipt_schema::absent(db).await?;
    }
    management_catalog::validate_versions(db, &actual).await?;
    Ok(actual)
}

pub(super) fn validate_versions(actual: &[String], writing: bool) -> AppResult<()> {
    // Registered prefixes must end at an explicitly supported catalog shape.
    if !matches!(
        actual.last().map(String::as_str),
        Some(STAGE5 | STAGE6 | SCHEMA | ZIP_CAPTURE | MANAGEMENT | history::M23)
    ) || !(history::is_prefix(actual) || actual == [MANAGEMENT])
    {
        return Err(refused("unknown or incomplete schema"));
    }
    if writing && !history::has_stage(actual, SCHEMA) {
        return Err(refused(
            "upgrade schema through normal gateway migration before apply",
        ));
    }
    Ok(())
}

pub(super) async fn read<C: ConnectionTrait>(
    db: &C,
    versions: &[String],
) -> AppResult<Vec<TableImage>> {
    check_encoding(db).await?;
    let stage6 = history::has_stage(versions, STAGE6);
    let zip_capture = history::has_stage(versions, ZIP_CAPTURE);
    let shapes = tables(versions);
    let mut catalog = columns(db, &shapes).await?;
    let mut result = Vec::with_capacity(shapes.len());
    let mut count = 0;
    let mut bytes = 0;
    for mut table in shapes {
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
        let columns = catalog.remove(&table.table).unwrap_or_default();
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
        let (rows, table_bytes) =
            read_rows(db, &table, MAX_ROWS - count, MAX_BYTES - bytes).await?;
        count += rows.len();
        bytes += table_bytes;
        for row in rows {
            let mut native = super::NativeRow::new();
            for column in &table.columns {
                let value: Option<String> = row.try_get("", column)?;
                native.insert(column.clone(), value);
            }
            table.rows.push(native);
        }
        result.push(table);
    }
    normalize(&mut result);
    Ok(result)
}

async fn check_encoding<C: ConnectionTrait>(db: &C) -> AppResult<()> {
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        let encoding: String = db
            .query_one(Statement::from_string(
                db.get_database_backend(),
                "PRAGMA encoding",
            ))
            .await?
            .ok_or_else(|| refused("database encoding unavailable"))?
            .try_get("", "encoding")?;
        if encoding != "UTF-8" {
            return Err(refused("SQLite UTF-8 database encoding required"));
        }
    }
    Ok(())
}

/// The size scan and budgeted payload read share one statement snapshot.
/// Rejected tables return one metadata row with NULL payload cells. Accepted
/// payload bytes plus column-name bytes fit the caller's remaining budget.
/// SQLite callers must first validate the database's UTF-8 encoding.
/// The payload limit is the complete proven row count or zero, never a prefix.
async fn read_rows<C: ConnectionTrait>(
    db: &C,
    table: &TableImage,
    remaining_rows: usize,
    remaining_bytes: usize,
) -> AppResult<(Vec<QueryResult>, usize)> {
    let row_bytes = table
        .columns
        .iter()
        .map(|column| {
            let text = format!("CAST(\"{column}\" AS TEXT)");
            let length = if db.get_database_backend() == DatabaseBackend::Postgres {
                format!("octet_length(convert_to({text},'UTF8'))")
            } else {
                format!("length(CAST({text} AS BLOB))")
            };
            format!("CAST(COALESCE({length},0) AS BIGINT)+{}", column.len())
        })
        .collect::<Vec<_>>()
        .join("+");
    let order = table
        .primary_key
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let fits = format!("b.__r26_rows<={remaining_rows} AND b.__r26_bytes<={remaining_bytes}");
    let fields = table
        .columns
        .iter()
        .map(|c| format!("CAST(p.\"{c}\" AS TEXT) AS \"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let payload_order = table
        .primary_key
        .iter()
        .map(|c| format!("p.\"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "WITH sizes AS MATERIALIZED (SELECT {row_bytes} AS row_bytes FROM \"{}\" ORDER BY {order} LIMIT {}), budget AS MATERIALIZED (SELECT COUNT(*) AS __r26_rows, CAST(COALESCE(SUM(row_bytes),0) AS BIGINT) AS __r26_bytes FROM sizes), payload AS MATERIALIZED (SELECT {fields} FROM \"{}\" AS p ORDER BY {payload_order} LIMIT (SELECT CASE WHEN {fits} THEN b.__r26_rows ELSE 0 END FROM budget AS b)) SELECT b.__r26_rows,b.__r26_bytes,p.* FROM budget AS b LEFT JOIN payload AS p ON TRUE ORDER BY {payload_order}",
        table.table,
        remaining_rows + 1,
        table.table,
    );
    let rows = db
        .query_all(Statement::from_string(db.get_database_backend(), sql))
        .await?;
    let budget = rows
        .first()
        .ok_or_else(|| refused("native inventory budget unavailable"))?;
    let count = usize::try_from(budget.try_get::<i64>("", "__r26_rows")?)
        .map_err(|_| refused("invalid native row budget"))?;
    let bytes = usize::try_from(budget.try_get::<i64>("", "__r26_bytes")?)
        .map_err(|_| refused("invalid native byte budget"))?;
    if count > remaining_rows {
        return Err(refused("complete inventory exceeds row budget"));
    }
    if bytes > remaining_bytes {
        return Err(refused("complete inventory exceeds byte budget"));
    }
    if count == 0 {
        return Ok((Vec::new(), 0));
    }
    if rows.len() != count {
        return Err(refused("incomplete native inventory"));
    }
    Ok((rows, bytes))
}

/// Read the complete column catalog together, including missing-table evidence.
/// Every native image rechecks the current catalog.
async fn columns<C: ConnectionTrait>(
    db: &C,
    tables: &[TableImage],
) -> AppResult<BTreeMap<String, Vec<String>>> {
    let parameters: Vec<_> = (1..=tables.len()).map(|i| format!("${i}")).collect();
    let sql = if db.get_database_backend() == DatabaseBackend::Sqlite {
        let values = parameters
            .iter()
            .map(|p| format!("({p})"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "WITH closure(table_name) AS (VALUES {values}) SELECT closure.table_name,p.name AS column_name FROM closure CROSS JOIN pragma_table_info(closure.table_name) AS p ORDER BY closure.table_name,p.name"
        )
    } else {
        format!(
            "SELECT table_name,column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name IN ({}) ORDER BY table_name,column_name",
            parameters.join(",")
        )
    };
    let rows = db
        .query_all(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            tables.iter().map(|table| table.table.clone().into()),
        ))
        .await?;
    let mut columns: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows {
        columns
            .entry(row.try_get("", "table_name")?)
            .or_default()
            .push(row.try_get("", "column_name")?);
    }
    Ok(columns)
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
    if !history::has_stage(versions, SCHEMA) {
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
        let pk = &image.primary_key;
        image.rows.sort_by(|left, right| {
            pk.iter()
                .map(|k| left.get(k))
                .cmp(pk.iter().map(|k| right.get(k)))
        });
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

#[cfg(test)]
#[path = "snapshot_cost_tests.rs"]
mod cost_tests;

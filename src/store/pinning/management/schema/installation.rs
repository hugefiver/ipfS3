//! Cold installation semantics outside the management evidence projection.
use super::*;
mod expressions;

const TABLES: &[&str] = &[
    "bucket_cors_configs",
    "bucket_lifecycle_configs",
    "object_tags",
    "residency_backfill",
    "zip_mpu_replays",
];

fn checks(table: &str, pg: bool) -> &'static [&'static str] {
    match (table, pg) {
        ("bucket_lifecycle_configs", false) => &[
            "canonical_json IS NULL OR json_valid(canonical_json)",
            "revision>0",
            "scan_lease_epoch>=0",
        ],
        ("bucket_lifecycle_configs", true) => &[
            "((canonical_json IS NULL) OR pg_input_is_valid(canonical_json, 'jsonb'::text))",
            "revision>0",
            "scan_lease_epoch>=0",
        ],
        ("residency_backfill", false) => &[
            "id='hot_verification'",
            "cursor_version_row_id IS NULL OR length(cursor_version_row_id)>0",
            "claim_epoch>=0",
            "(lease_until IS NULL AND claimed_by IS NULL) OR (lease_until IS NOT NULL AND claimed_by IS NOT NULL AND length(claimed_by)>0)",
        ],
        ("residency_backfill", true) => &[
            "id='hot_verification'::text",
            "(cursor_version_row_id IS NULL) OR (length(cursor_version_row_id)>0)",
            "claim_epoch>=0",
            "((lease_until IS NULL) AND (claimed_by IS NULL)) OR ((lease_until IS NOT NULL) AND (claimed_by IS NOT NULL) AND (length(claimed_by)>0))",
        ],
        ("zip_mpu_replays", false) => &[
            "length(request_fingerprint)=64 AND length(request_contract)>0",
            "prepared_archive_cid IS NULL OR length(prepared_archive_cid)>0",
            "(archive_cid IS NULL AND archive_size IS NULL AND public_version_id IS NULL AND server_side_encryption IS NULL AND response_xml IS NULL AND response_headers_json IS NULL) OR (archive_cid IS NOT NULL AND prepared_archive_cid IS NOT NULL AND archive_cid=prepared_archive_cid AND length(archive_cid)>0 AND archive_size IS NOT NULL AND archive_size>=0 AND response_xml IS NOT NULL AND length(response_xml)>0 AND response_headers_json IS NOT NULL AND length(response_headers_json)>0 AND (public_version_id IS NULL OR length(public_version_id)>0) AND (server_side_encryption IS NULL OR server_side_encryption='AES256'))",
        ],
        ("zip_mpu_replays", true) => &[
            "(length(request_fingerprint)=64) AND (length(request_contract)>0)",
            "(prepared_archive_cid IS NULL) OR (length(prepared_archive_cid)>0)",
            "((archive_cid IS NULL) AND (archive_size IS NULL) AND (public_version_id IS NULL) AND (server_side_encryption IS NULL) AND (response_xml IS NULL) AND (response_headers_json IS NULL)) OR ((archive_cid IS NOT NULL) AND (prepared_archive_cid IS NOT NULL) AND (archive_cid=prepared_archive_cid) AND (length(archive_cid)>0) AND (archive_size IS NOT NULL) AND (archive_size>=0) AND (response_xml IS NOT NULL) AND (length(response_xml)>0) AND (response_headers_json IS NOT NULL) AND (length(response_headers_json)>0) AND ((public_version_id IS NULL) OR (length(public_version_id)>0)) AND ((server_side_encryption IS NULL) OR (server_side_encryption='AES256'::text)))",
        ],
        _ => &[],
    }
}

fn default(table: &str, column: &str) -> Option<&'static str> {
    match (table, column) {
        ("bucket_lifecycle_configs", "scan_lease_epoch")
        | ("residency_backfill", "claim_epoch") => Some("0"),
        ("residency_backfill", "completed") => Some("false"),
        _ => None,
    }
}

pub(super) async fn validate<C: ConnectionTrait>(db: &C) -> Result<()> {
    let backend = db.get_database_backend();
    let pg = backend == DatabaseBackend::Postgres;
    for table in TABLES {
        let actual = if pg {
            let rows = db.query_all(Statement::from_sql_and_values(backend,
                "SELECT CASE WHEN octet_length(pg_get_expr(k.conbin,k.conrelid))<=8192 THEN pg_get_expr(k.conbin,k.conrelid) END AS expression,k.convalidated,NOT k.condeferrable AND NOT k.condeferred AS immediate FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1 AND k.contype='c' LIMIT 33",
                [(*table).into()])).await.map_err(|_| ManagementError::Schema)?;
            let mut expressions = Vec::new();
            for row in rows {
                if !row
                    .try_get::<bool>("", "convalidated")
                    .map_err(|_| ManagementError::Schema)?
                    || !row
                        .try_get::<bool>("", "immediate")
                        .map_err(|_| ManagementError::Schema)?
                {
                    return Err(ManagementError::Schema);
                }
                expressions.push(expressions::canonical(
                    &row.try_get::<String>("", "expression")
                        .map_err(|_| ManagementError::Schema)?,
                )?);
            }
            expressions
        } else {
            let row = db.query_one(Statement::from_sql_and_values(backend,
                "SELECT CASE WHEN length(CAST(sql AS BLOB))<=65536 THEN sql END AS sql FROM main.sqlite_schema WHERE type='table' AND name=$1",
                [(*table).into()])).await.map_err(|_| ManagementError::Schema)?.ok_or(ManagementError::Schema)?;
            expressions::checks(
                &row.try_get::<String>("", "sql")
                    .map_err(|_| ManagementError::Schema)?,
            )?
        };
        let mut expected = checks(table, pg)
            .iter()
            .map(|sql| expressions::canonical(sql))
            .collect::<Result<Vec<_>>>()?;
        let mut actual = actual;
        actual.sort();
        expected.sort();
        if actual != expected {
            return Err(ManagementError::Schema);
        }

        let sql = if pg {
            "SELECT a.attname::text AS name,d.oid IS NULL OR octet_length(pg_get_expr(d.adbin,d.adrelid))<=1024 AS bounded,CASE WHEN octet_length(pg_get_expr(d.adbin,d.adrelid))<=1024 THEN pg_get_expr(d.adbin,d.adrelid) END AS expression FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum WHERE n.nspname=current_schema() AND c.relname=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attname LIMIT 129"
        } else {
            "SELECT name,dflt_value IS NULL OR length(CAST(dflt_value AS BLOB))<=1024 AS bounded,CASE WHEN length(CAST(dflt_value AS BLOB))<=1024 THEN dflt_value END AS expression FROM pragma_table_info($1) ORDER BY name LIMIT 129"
        };
        let columns = db
            .query_all(Statement::from_sql_and_values(
                backend,
                sql,
                [(*table).into()],
            ))
            .await
            .map_err(|_| ManagementError::Schema)?;
        if columns.is_empty() || columns.len() > 128 {
            return Err(ManagementError::Schema);
        }
        for column in columns {
            if !column
                .try_get::<bool>("", "bounded")
                .map_err(|_| ManagementError::Schema)?
            {
                return Err(ManagementError::Schema);
            }
            let name: String = column
                .try_get("", "name")
                .map_err(|_| ManagementError::Schema)?;
            let actual: Option<String> = column
                .try_get("", "expression")
                .map_err(|_| ManagementError::Schema)?;
            let actual = actual
                .as_deref()
                .map(expressions::canonical_default)
                .transpose()?;
            let expected = default(table, &name)
                .map(expressions::canonical_default)
                .transpose()?;
            if actual != expected {
                return Err(ManagementError::Schema);
            }
        }
    }
    Ok(())
}

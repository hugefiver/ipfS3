//! Status responses do not load source descriptors, object metadata or tags.
use crate::store::entities::import_job;
use crate::{
    error::{AppError, AppResult},
    pinning::decision::ExtensionDecision,
};
use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, FromQueryResult, JoinType,
    QueryFilter, QuerySelect, RelationTrait, TransactionTrait,
    sea_query::{Expr, Iden},
};

#[derive(FromQueryResult)]
pub struct JobStatus {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub source_type: String,
    pub state: String,
    pub phase: String,
    pub attempts: i32,
    pub providers_observed: i64,
    pub pin_nodes_processed: i64,
    pub pin_bytes_processed: i64,
    pub downloaded_bytes: i64,
    pub download_total: Option<i64>,
    pub ipfs_add_bytes: i64,
    pub logical_size: Option<i64>,
    pub entries_processed: i64,
    pub entries_succeeded: i64,
    pub entries_failed: i64,
    pub decompressed_bytes: i64,
    pub final_cid: Option<String>,
    pub failure_code: Option<String>,
    pub failure_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub decompress_prefix: Option<String>,
    pub root_capture_json: Option<String>,
    pub zip_intent_json: Option<String>,
    pub pin_decision_json: Option<String>,
    hot_oversized: bool,
    request_oversized: bool,
}

impl JobStatus {
    pub fn decision(&self) -> AppResult<Option<ExtensionDecision>> {
        super::jobs::decode_decision(
            (&self.id, &self.bucket, &self.key),
            super::jobs::DecisionFields {
                pin: self.pin_decision_json.as_deref(),
                zip: self.zip_intent_json.as_deref(),
                prefix: self.decompress_prefix.as_deref(),
                root: self.root_capture_json.as_deref(),
            },
        )
    }
}

pub(super) async fn get_for_path(
    db: &DatabaseConnection,
    job_id: &str,
    bucket: &str,
    key: &str,
) -> AppResult<Option<JobStatus>> {
    use import_job::Column;
    let columns = [
        Column::DecompressPrefix,
        Column::RootCaptureJson,
        Column::ZipIntentJson,
        Column::PinDecisionJson,
    ];
    let backend = db.get_database_backend();
    let hot_columns = [
        Column::Id,
        Column::Bucket,
        Column::Key,
        Column::SourceType,
        Column::State,
        Column::Phase,
        Column::Attempts,
        Column::ProvidersObserved,
        Column::PinNodesProcessed,
        Column::PinBytesProcessed,
        Column::DownloadedBytes,
        Column::DownloadTotal,
        Column::IpfsAddBytes,
        Column::LogicalSize,
        Column::EntriesProcessed,
        Column::EntriesSucceeded,
        Column::EntriesFailed,
        Column::DecompressedBytes,
        Column::FinalCid,
        Column::FailureCode,
        Column::FailureMessage,
        Column::CreatedAt,
        Column::UpdatedAt,
    ];
    let mut query = import_job::Entity::find_state()
        .select_only()
        .join(JoinType::InnerJoin, import_job::Relation::Request.def());
    let mut hot_large = Vec::new();
    for column in hot_columns {
        let limit = match column {
            Column::Id
            | Column::Bucket
            | Column::Key
            | Column::SourceType
            | Column::State
            | Column::Phase
            | Column::FinalCid
            | Column::FailureCode
            | Column::FailureMessage => 65536,
            Column::CreatedAt | Column::UpdatedAt => 128,
            _ => 32,
        };
        let field = format!("import_jobs.\"{}\"", Iden::to_string(&column));
        let bytes = if backend == DatabaseBackend::Sqlite {
            format!("length(CAST({field} AS BLOB))")
        } else {
            format!("octet_length(CAST({field} AS TEXT))")
        };
        query = query.column_as(
            Expr::cust(format!("CASE WHEN {bytes}<={limit} THEN {field} END")),
            column,
        );
        hot_large.push(format!("COALESCE({bytes},0)>{limit}"));
    }
    query = query.column_as(Expr::cust(hot_large.join(" OR ")), "hot_oversized");
    let mut large = Vec::new();
    for column in columns {
        let name = Iden::to_string(&column);
        let field = format!("import_job_requests.\"{name}\"");
        let bytes = if backend == DatabaseBackend::Sqlite {
            format!("length(CAST({field} AS BLOB))")
        } else {
            format!("octet_length({field})")
        };
        query = query.column_as(
            Expr::cust(format!("CASE WHEN {bytes}<=65536 THEN {field} END")),
            column,
        );
        large.push(format!("COALESCE({bytes},0)>65536"));
    }
    query = query
        .column_as(Expr::cust(large.join(" OR ")), "request_oversized")
        .filter(Column::Id.eq(job_id))
        .filter(Column::Bucket.eq(bucket))
        .filter(Column::Key.eq(key));
    let tx = db.begin().await?;
    if backend == DatabaseBackend::Postgres {
        query = query.lock_shared();
    }
    let Some(mut status) = query.into_model::<JobStatus>().one(&tx).await? else {
        tx.commit().await?;
        return Ok(None);
    };
    if status.hot_oversized {
        return Err(AppError::Internal(
            "import status exceeds the cell budget".into(),
        ));
    }
    if status.request_oversized {
        let ids = [status.id.clone()];
        let mut rows = super::requests::read_cells(&tx, &ids, &columns).await?;
        let values = rows.remove(&status.id).ok_or(AppError::NoSuchImportJob)?;
        for (column, value) in columns.into_iter().zip(values) {
            match column {
                Column::DecompressPrefix => status.decompress_prefix = value,
                Column::RootCaptureJson => status.root_capture_json = value,
                Column::ZipIntentJson => status.zip_intent_json = value,
                Column::PinDecisionJson => status.pin_decision_json = value,
                _ => unreachable!("status capture columns only"),
            }
        }
    }
    tx.commit().await?;
    Ok(Some(status))
}

#[cfg(test)]
impl From<import_job::Model> for JobStatus {
    fn from(job: import_job::Model) -> Self {
        Self {
            id: job.id,
            bucket: job.bucket,
            key: job.key,
            source_type: job.source_type,
            state: job.state,
            phase: job.phase,
            attempts: job.attempts,
            providers_observed: job.providers_observed,
            pin_nodes_processed: job.pin_nodes_processed,
            pin_bytes_processed: job.pin_bytes_processed,
            downloaded_bytes: job.downloaded_bytes,
            download_total: job.download_total,
            ipfs_add_bytes: job.ipfs_add_bytes,
            logical_size: job.logical_size,
            entries_processed: job.entries_processed,
            entries_succeeded: job.entries_succeeded,
            entries_failed: job.entries_failed,
            decompressed_bytes: job.decompressed_bytes,
            final_cid: job.final_cid,
            failure_code: job.failure_code,
            failure_message: job.failure_message,
            created_at: job.created_at,
            updated_at: job.updated_at,
            decompress_prefix: job.decompress_prefix,
            root_capture_json: job.root_capture_json,
            zip_intent_json: job.zip_intent_json,
            pin_decision_json: job.pin_decision_json,
            hot_oversized: false,
            request_oversized: false,
        }
    }
}

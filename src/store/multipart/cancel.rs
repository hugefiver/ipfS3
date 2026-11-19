use sea_orm::{ConnectionTrait, Statement};

use super::{AbortExactIncompleteUploadResult, abort_exact_incomplete_upload_in_transaction};
use crate::{
    error::{AppError, AppResult},
    lifecycle::model::MultipartUploadTargetIdentity,
    store::{database_clock::database_now, import::ownership, zip::ROOT_UPLOAD_ABORTED},
};

/// Only explicit and lifecycle Abort cancel work. Complete also deletes the upload,
/// using the plain exact-delete primitive. The caller holds the bucket lock.
pub(crate) async fn cancel_exact_incomplete_upload_in_transaction(
    tx: &impl ConnectionTrait,
    target: &MultipartUploadTargetIdentity,
) -> AppResult<AbortExactIncompleteUploadResult> {
    let backend = tx.get_database_backend();
    // Deleting the upload clears active_upload_id via the FK.
    let intake = tx
        .query_one(Statement::from_sql_and_values(
            backend,
            "SELECT i.owner,i.token FROM zip_v2_mpu_intakes i
             JOIN zip_v2_executions e ON e.id=i.execution_id AND e.source='mpu' AND e.owner=i.owner
               AND e.token=i.token AND e.bucket=i.bucket AND e.source_key=i.archive_key
             WHERE i.active_upload_id=$1 AND i.execution_id=i.original_upload_id
               AND i.bucket=$2 AND i.archive_key=$3 AND e.state IN ('pending','admitted','fenced')",
            [
                target.upload_id.as_str().into(),
                target.bucket.as_str().into(),
                target.key.as_str().into(),
            ],
        ))
        .await?;
    let result = abort_exact_incomplete_upload_in_transaction(tx, target).await?;
    let (AbortExactIncompleteUploadResult::Applied, Some(intake)) = (result, intake) else {
        return Ok(result);
    };
    let now = database_now(tx).await?;
    let id = target.upload_id.as_str();
    tx.execute(Statement::from_sql_and_values(
        backend,
        "UPDATE zip_v2_executions SET state='fenced',terminal_result='fenced',updated_at=$1
         WHERE id=$2 AND state IN ('pending','admitted')",
        [now.into(), id.into()],
    ))
    .await?;

    // This UPDATE serializes with the batch row lock held by root workers.
    let mirror = tx.query_one(Statement::from_sql_and_values(backend,
        "UPDATE zip_batches SET root_status='failed',root_error_code=$1,root_cid=NULL,updated_at=$2
         WHERE id=$3 AND state='open' AND source='mpu' AND fingerprint='pending'
           AND bucket=$4 AND archive_key=$5 AND owner=$6 AND token=$7
         RETURNING input_identity",
        [ROOT_UPLOAD_ABORTED.into(), now.into(), id.into(), target.bucket.as_str().into(),
         target.key.as_str().into(), intake.try_get::<String>("", "owner")?.into(),
         intake.try_get::<String>("", "token")?.into()],
    )).await?;
    if let Some(mirror) = mirror {
        let identity: String = mirror.try_get("", "input_identity")?;
        if identity != "pending" {
            let generation = identity
                .strip_prefix("zip-v2-source-gen:")
                .and_then(|v| v.parse().ok())
                .filter(|n| *n > 0)
                .ok_or(AppError::StaleContentMutation)?;
            let source = ownership::StandardMutationGuard {
                bucket: target.bucket.clone(),
                key: target.key.clone(),
                mutation_id: format!("zip-v2-source:{id}"),
                expected_generation: generation,
                mutation_prefix: None,
            };
            ownership::release_standard_mutation_in_transaction(tx, &source, now).await?;
        }
    }

    // A later owner may already have taken individual outputs. Match the recorded
    // token and generation on both tables, including leases that have expired.
    tx.execute(Statement::from_sql_and_values(backend,
        "UPDATE import_destinations SET mutation_id=NULL,mutation_prefix=NULL,updated_at=$1
         WHERE bucket=$2 AND owner_job_id IS NULL AND (key,mutation_id,generation) IN
           (SELECT object_key,mutation_id,expected_generation FROM zip_v2_targets WHERE batch_id=$3)",
        [now.into(), target.bucket.as_str().into(), id.into()],
    )).await?;
    tx.execute(Statement::from_sql_and_values(backend,
        "DELETE FROM standard_mutation_leases WHERE bucket=$1 AND (key,mutation_id,generation) IN
           (SELECT object_key,mutation_id,expected_generation FROM zip_v2_targets WHERE batch_id=$2)",
        [target.bucket.as_str().into(), id.into()],
    )).await?;
    Ok(result)
}

use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::{
    error::{AppError, AppResult},
    store::entities::import_job_result,
};

const MAX_RESULT_PAGE_SIZE: u64 = 1_000;

pub struct ResultPage {
    pub rows: Vec<import_job_result::Model>,
    pub next_sequence: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ZipResultRecord {
    pub key: String,
    pub cid: Option<String>,
    pub size: Option<i64>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

pub(crate) fn zip_publication_rows(
    job_id: &str,
    archive_key: &str,
    archive_cid: &str,
    archive_size: i64,
    records: &[ZipResultRecord],
) -> AppResult<Vec<import_job_result::ActiveModel>> {
    let mut rows = Vec::with_capacity(records.len().saturating_add(1));
    rows.push(import_job_result::ActiveModel {
        job_id: Set(job_id.to_owned()),
        sequence: Set(0),
        key: Set(archive_key.to_owned()),
        cid: Set(Some(archive_cid.to_owned())),
        size: Set(Some(archive_size)),
        error_code: Set(None),
        error_message: Set(None),
    });
    for (index, record) in records.iter().enumerate() {
        let sequence = i64::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .ok_or_else(|| AppError::Internal("ZIP import result sequence exhausted".to_owned()))?;
        rows.push(import_job_result::ActiveModel {
            job_id: Set(job_id.to_owned()),
            sequence: Set(sequence),
            key: Set(record.key.clone()),
            cid: Set(record.cid.clone()),
            size: Set(record.size),
            error_code: Set(record.error_code.clone()),
            error_message: Set(record.error_message.clone()),
        });
    }
    Ok(rows)
}

pub async fn page<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    after: Option<i64>,
    limit: u64,
) -> AppResult<ResultPage> {
    let limit = limit.clamp(1, MAX_RESULT_PAGE_SIZE);
    let mut query = import_job_result::Entity::find()
        .filter(import_job_result::Column::JobId.eq(job_id))
        .order_by_asc(import_job_result::Column::Sequence)
        .limit(limit + 1);
    if let Some(after) = after {
        query = query.filter(import_job_result::Column::Sequence.gt(after));
    }
    let mut rows = query.all(db).await?;
    let has_more = rows.len() > usize::try_from(limit).unwrap_or(usize::MAX);
    if has_more {
        rows.pop();
    }
    let next_sequence = has_more.then(|| {
        rows.last()
            .expect("positive page limits retain at least one result")
            .sequence
    });
    Ok(ResultPage {
        rows,
        next_sequence,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{TimeZone, Utc};
    use sea_orm::{ConnectionTrait, Database, EntityTrait, Set};

    use super::*;
    use crate::{
        import::ImportSource,
        pinning::tags::ObjectTag,
        store::{
            entities::import_job_result,
            import::jobs::{NewImportJob, insert_queued},
        },
    };

    async fn setup() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
            .await
            .unwrap();
        insert_queued(
            &db,
            NewImportJob {
                id: "job-1".to_owned(),
                bucket: "bucket".to_owned(),
                key: "key".to_owned(),
                source: ImportSource::Cid(
                    "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".to_owned(),
                ),
                request_fingerprint: "fingerprint".to_owned(),
                client_token: None,
                object_content_type: None,
                metadata: HashMap::new(),
                tags: Vec::<ObjectTag>::new(),
                decompress_prefix: None,
            },
            Utc.with_ymd_and_hms(2026, 7, 29, 0, 0, 0).single().unwrap(),
        )
        .await
        .unwrap();
        for sequence in 0..5 {
            import_job_result::Entity::insert(import_job_result::ActiveModel {
                job_id: Set("job-1".to_owned()),
                sequence: Set(sequence),
                key: Set(format!("key-{sequence}")),
                cid: Set(Some(format!("bafy-{sequence}"))),
                size: Set(Some(sequence)),
                error_code: Set(None),
                error_message: Set(None),
            })
            .exec(&db)
            .await
            .unwrap();
        }
        db
    }

    #[tokio::test]
    async fn cursor_pages_are_exclusive_ascending_bounded_and_truthful() {
        let db = setup().await;

        let first = page(&db, "job-1", None, 2).await.unwrap();
        assert_eq!(
            first
                .rows
                .iter()
                .map(|row| row.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(first.next_sequence, Some(1));

        let second = page(&db, "job-1", first.next_sequence, 2).await.unwrap();
        assert_eq!(
            second
                .rows
                .iter()
                .map(|row| row.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(second.next_sequence, Some(3));

        let last = page(&db, "job-1", second.next_sequence, 2).await.unwrap();
        assert_eq!(
            last.rows.iter().map(|row| row.sequence).collect::<Vec<_>>(),
            vec![4]
        );
        assert_eq!(last.next_sequence, None);

        let zero_limit = page(&db, "job-1", None, 0).await.unwrap();
        assert_eq!(zero_limit.rows.len(), 1);
        assert_eq!(zero_limit.next_sequence, Some(0));
    }

    #[test]
    fn zip_rows_are_archive_first_then_observation_order_with_contiguous_sequences() {
        let rows = zip_publication_rows(
            "job",
            "archive.zip",
            "bafy-archive",
            10,
            &[
                ZipResultRecord {
                    key: "out/success.txt".to_owned(),
                    cid: Some("bafy-success".to_owned()),
                    size: Some(7),
                    error_code: None,
                    error_message: None,
                },
                ZipResultRecord {
                    key: "out/failure.txt".to_owned(),
                    cid: None,
                    size: None,
                    error_code: Some("EntryUploadFailed".to_owned()),
                    error_message: Some("upload failed".to_owned()),
                },
            ],
        )
        .unwrap();

        assert_eq!(
            rows.iter()
                .map(|row| (*row.sequence.as_ref(), row.key.as_ref().clone()))
                .collect::<Vec<_>>(),
            vec![
                (0, "archive.zip".to_owned()),
                (1, "out/success.txt".to_owned()),
                (2, "out/failure.txt".to_owned()),
            ]
        );
    }
}

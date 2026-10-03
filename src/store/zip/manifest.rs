use chrono::Utc;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    QueryFilter, Set, TransactionTrait,
};
use std::collections::{HashMap, HashSet};

use super::{invalid, required, safe_code};
use crate::error::AppResult;
use crate::store::entities::{zip_batch, zip_manifest_entry};

#[derive(Clone, Debug)]
pub enum ManifestItem {
    Success {
        /// Unique durable binding path; may be a synthetic ordinal when the
        /// original S3 object key is not a representable UnixFS path.
        path: String,
        /// Exact published S3 key, independent of the durable binding path.
        object_key: String,
        cid: String,
        size: i64,
    },
    Failure {
        path: String,
        code: String,
    },
}

fn valid_path(path: &str) -> bool {
    required(path)
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
        && path
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

/// Stores the final (not raw extraction) path set once, before root side effects.
/// A failed ZIP entry carries only a safe code; no raw client/server error text.
pub async fn prepare_manifest(
    db: &DatabaseConnection,
    id: &str,
    items: &[ManifestItem],
) -> AppResult<()> {
    let tx = db.begin().await?;
    prepare_in_transaction(&tx, id, items, false).await?;
    tx.commit().await?;
    Ok(())
}

impl ManifestItem {
    /// v2 mirror: accept only an exact immutable replay before root work.
    /// Caller holds the bucket lock; roll back on any later failure. The
    /// legacy public wrapper remains one-shot. No new mod.rs export required.
    pub(crate) async fn prepare_manifest_in_transaction(
        tx: &DatabaseTransaction,
        id: &str,
        items: &[ManifestItem],
    ) -> AppResult<()> {
        prepare_in_transaction(tx, id, items, true).await
    }
}

async fn prepare_in_transaction(
    tx: &DatabaseTransaction,
    id: &str,
    items: &[ManifestItem],
    exact_mirror: bool,
) -> AppResult<()> {
    let batch = super::batch::locked_open_batch(tx, id).await?;
    if batch.root_revision != 0
        || (batch.manifest_prepared && !exact_mirror)
        || (exact_mirror && (batch.root_status != "pending" || batch.source_published))
    {
        return Err(super::stale());
    }
    let existing = if batch.manifest_prepared {
        Some(
            entries(tx, id)
                .await?
                .into_iter()
                .map(|entry| (entry.path.clone(), entry))
                .collect::<HashMap<_, _>>(),
        )
    } else {
        None
    };
    let mut paths = HashSet::new();
    for item in items {
        let (path, object_key, cid, size, error_code) = match item {
            ManifestItem::Success {
                path,
                object_key,
                cid,
                size,
            } if required(object_key) && required(cid) && *size >= 0 => (
                path,
                Some(object_key.clone()),
                Some(cid.clone()),
                Some(*size),
                None,
            ),
            ManifestItem::Failure { path, code } if safe_code(code) => {
                (path, None, None, None, Some(code.clone()))
            }
            _ => return Err(invalid()),
        };
        if !valid_path(path) || !paths.insert(path) {
            return Err(invalid());
        }
        if let Some(existing) = &existing {
            let old = existing.get(path).ok_or_else(super::stale)?;
            if old.object_key != object_key
                || old.cid != cid
                || old.size != size
                || old.error_code != error_code
                || old.version_row_id.is_some()
            {
                return Err(super::stale());
            }
            continue;
        }
        zip_manifest_entry::Entity::insert(zip_manifest_entry::ActiveModel {
            batch_id: Set(id.to_owned()),
            path: Set(path.clone()),
            object_key: Set(object_key),
            cid: Set(cid),
            size: Set(size),
            version_row_id: Set(None),
            error_code: Set(error_code),
            created_at: Set(Utc::now()),
        })
        .exec(tx)
        .await?;
    }
    if let Some(existing) = existing {
        return if existing.len() == paths.len() {
            Ok(())
        } else {
            Err(super::stale())
        };
    }
    zip_batch::ActiveModel {
        manifest_prepared: Set(true),
        updated_at: Set(Utc::now()),
        ..batch.into()
    }
    .update(tx)
    .await?;
    Ok(())
}

pub(super) async fn entries<C: sea_orm::ConnectionTrait>(
    db: &C,
    id: &str,
) -> AppResult<Vec<zip_manifest_entry::Model>> {
    Ok(zip_manifest_entry::Entity::find()
        .filter(zip_manifest_entry::Column::BatchId.eq(id))
        .all(db)
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{self, zip};
    use sea_orm::{ConnectionTrait, Database};

    #[tokio::test]
    async fn synthetic_path_binds_exact_version_without_rewriting_legacy_object_key() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        store::run_migrations(&db).await.unwrap();
        store::bucket::create(&db, "bucket", None).await.unwrap();
        zip::admit(
            &db,
            &zip::BatchAdmission {
                id: "batch".into(),
                owner: "owner".into(),
                source: "direct".into(),
                token: "token".into(),
                fingerprint: "fingerprint".into(),
                bucket: "bucket".into(),
                archive_key: "archive.zip".into(),
                input_identity: "input".into(),
                captured_options: "{}".into(),
            },
        )
        .await
        .unwrap();

        // The durable path is a unique binding ID, not a rewritten S3 key.
        assert!(
            prepare_manifest(
                &db,
                "batch",
                &[ManifestItem::Success {
                    path: "a//b".into(),
                    object_key: "out/a//b".into(),
                    cid: "cid".into(),
                    size: 2,
                }],
            )
            .await
            .is_err()
        );
        prepare_manifest(
            &db,
            "batch",
            &[ManifestItem::Success {
                path: "invalid/3".into(),
                object_key: "out/a//b".into(),
                cid: "cid".into(),
                size: 2,
            }],
        )
        .await
        .unwrap();
        db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('obj','bucket','out/a//b','cid',2,'cid')").await.unwrap();
        db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('version','bucket','out/a//b','object','obj',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        let tx = db.begin().await.unwrap();
        let binding = zip::binding_for_published_object(&tx, "invalid/3", "obj")
            .await
            .unwrap();
        zip::publish(
            &tx,
            "batch",
            &[binding],
            false,
            "{}",
            zip::RootOutcome::Failed {
                code: "invalid_manifest",
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let snapshot = zip::snapshot(&db, "batch").await.unwrap().unwrap();
        assert_eq!(snapshot.batch.root_status, "failed");
        assert_eq!(snapshot.entries[0].path, "invalid/3");
        assert_eq!(snapshot.entries[0].object_key.as_deref(), Some("out/a//b"));
        assert_eq!(
            snapshot.entries[0].version_row_id.as_deref(),
            Some("version")
        );
    }
}

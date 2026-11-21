use std::collections::{BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set, Statement,
    sea_query::{CaseStatement, Expr, OnConflict},
};
use uuid::Uuid;

use super::{CapturedZipPublication, PublicationObject, invalid_publication};
use crate::{
    error::{AppError, AppResult},
    pinning::zip_policy::{
        CapturedZipIntent, ZipBoundOutput, ZipSelectedLease, select_zip_intents,
    },
    residency::{KuboTier, ReferenceReason, StorageClass},
    store::{
        entities::{
            object, object_tag, object_version, physical_residency, residency_reference,
            version_residency,
        },
        object_version::{BucketVersioningState, NULL_VERSION_ID, PublicationResult},
        zip::{VersionBinding, binding_for_published_object, publication_lease::PublicationLease},
    },
};

const SQL_PARAMS: usize = 900;

struct NewObject<'a> {
    object: &'a PublicationObject,
    row_id: String,
    public_id: Option<String>,
}

pub(super) async fn publish_objects(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
    state: BucketVersioningState,
    now: DateTime<Utc>,
    maintenance: &mut PublicationLease<'_>,
    selection: &CapturedZipIntent,
) -> AppResult<(
    Vec<(String, PublicationResult)>,
    Vec<VersionBinding>,
    Vec<ZipSelectedLease>,
)> {
    let ordered = super::objects(request);
    let existing = existing_keys(tx, request.capture.bucket(), &ordered, maintenance).await?;
    prepare_hot_physical(tx, &ordered, now, maintenance).await?;

    let paths: HashMap<_, _> = request
        .manifest
        .successful
        .iter()
        .map(|entry| (entry.object_key.as_str(), entry.relative_path.as_str()))
        .collect();
    let mut fresh = Vec::with_capacity(ordered.len() - existing.len());
    let mut results = Vec::with_capacity(ordered.len());
    let mut bindings = Vec::with_capacity(paths.len());
    let mut source = None;
    let mut outputs = Vec::with_capacity(paths.len());
    let mut net_bytes = 0i128;
    for object in ordered {
        let path = paths.get(object.key.as_str()).copied();
        let (row_id, result) = if existing.contains(&object.key) {
            maintenance.checkpoint(tx).await?;
            let (result, delta) =
                super::super::write_object_version_and_update_lifecycle(tx, object, state, now)
                    .await?;
            net_bytes += i128::from(delta);
            let binding =
                binding_for_published_object(tx, path.unwrap_or("source"), &object.id).await?;
            (binding.version_row_id, result)
        } else {
            net_bytes += i128::from(object.logical_size);
            let row_id = Uuid::new_v4().to_string();
            let public_id =
                (state == BucketVersioningState::Enabled).then(|| Uuid::new_v4().to_string());
            let version_id = match state {
                BucketVersioningState::Unversioned => None,
                BucketVersioningState::Enabled => public_id.clone(),
                BucketVersioningState::Suspended => Some(NULL_VERSION_ID.to_owned()),
            };
            fresh.push(NewObject {
                object,
                row_id: row_id.clone(),
                public_id,
            });
            (
                row_id,
                PublicationResult {
                    object_id: object.id.clone(),
                    version_id,
                },
            )
        };
        let bound = ZipBoundOutput {
            object_id: object.id.clone(),
            key: object.key.clone(),
            bucket: object.bucket.clone(),
            cid: object.cid.clone(),
            logical_size: object.logical_size,
            version_id: row_id,
        };
        if let Some(path) = path {
            bindings.push(VersionBinding {
                path: path.to_owned(),
                version_row_id: bound.version_id.clone(),
            });
            outputs.push(bound);
        } else {
            source = Some(bound);
        }
        results.push((object.key.clone(), result));
    }

    crate::store::bucket::settle_usage(
        tx,
        request.capture.bucket(),
        net_bytes.try_into().map_err(|_| AppError::QuotaExceeded)?,
    )
    .await?;
    let selected = select_zip_intents(selection, &request.manifest, source.as_ref(), &outputs)
        .map_err(|error| invalid_publication(&error.to_string()))?;
    let snapshots = source.into_iter().zip(selected.source_decision).chain(
        selected
            .output_decisions
            .into_iter()
            .map(|(bound, mut decision)| {
                decision.control_revision = Uuid::new_v4().to_string();
                (bound, decision)
            }),
    );
    let mut decisions = snapshots
        .map(|(bound, decision)| {
            let snapshot = serde_json::to_string(&decision).map_err(|_| {
                AppError::Internal("extension decision serialization failed".into())
            })?;
            Ok((bound.version_id, snapshot))
        })
        .collect::<AppResult<HashMap<_, _>>>()?;

    insert_new_objects(tx, &fresh, &mut decisions, now, maintenance).await?;
    update_replaced_decisions(tx, decisions, maintenance).await?;
    if let Some(source) = &request.source
        && !request.source_tags.is_empty()
    {
        object_tag::Entity::insert_many(request.source_tags.iter().map(|tag| {
            object_tag::ActiveModel {
                object_id: Set(source.id.clone()),
                key: Set(tag.key.clone()),
                value: Set(tag.value.clone()),
            }
        }))
        .exec_without_returning(tx)
        .await?;
    }
    Ok((results, bindings, selected.leases))
}

async fn existing_keys(
    tx: &DatabaseTransaction,
    bucket: &str,
    ordered: &[&PublicationObject],
    maintenance: &mut PublicationLease<'_>,
) -> AppResult<HashSet<String>> {
    let mut keys = HashSet::new();
    for chunk in ordered.chunks(SQL_PARAMS - 1) {
        maintenance.checkpoint(tx).await?;
        let slots = (2..=chunk.len() + 1)
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>()
            .join(",");
        // Older objects can have a current object row without a version index row.
        let sql = format!(
            "SELECT key FROM object_versions WHERE bucket = $1 AND key IN ({slots}) \
             UNION SELECT key FROM objects WHERE bucket = $1 AND key IN ({slots}) AND is_latest = TRUE"
        );
        let values =
            std::iter::once(bucket.into()).chain(chunk.iter().map(|o| o.key.as_str().into()));
        for row in tx
            .query_all(Statement::from_sql_and_values(
                tx.get_database_backend(),
                sql,
                values,
            ))
            .await?
        {
            keys.insert(row.try_get("", "key")?);
        }
    }
    Ok(keys)
}

async fn prepare_hot_physical(
    tx: &DatabaseTransaction,
    ordered: &[&PublicationObject],
    now: DateTime<Utc>,
    maintenance: &mut PublicationLease<'_>,
) -> AppResult<()> {
    let cids = ordered
        .iter()
        .map(|o| o.cid.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    for chunk in cids.chunks(SQL_PARAMS / 8) {
        maintenance.checkpoint(tx).await?;
        physical_residency::Entity::insert_many(chunk.iter().map(|cid| {
            physical_residency::ActiveModel {
                tier: Set(KuboTier::Hot.as_db_str().to_owned()),
                cid: Set((*cid).to_owned()),
                node_identity: Set(None),
                verification_state: Set("pending".to_owned()),
                verification_receipt: Set(None),
                verified_at: Set(None),
                created_at: Set(now),
                updated_at: Set(now),
            }
        }))
        // PostgreSQL locks conflicts in the same CID order as new rows, without
        // changing an existing physical verification receipt or timestamp.
        .on_conflict(
            OnConflict::columns([
                physical_residency::Column::Tier,
                physical_residency::Column::Cid,
            ])
            .update_column(physical_residency::Column::UpdatedAt)
            .action_and_where(Expr::cust("FALSE"))
            .to_owned(),
        )
        .exec_without_returning(tx)
        .await?;
    }
    Ok(())
}

async fn insert_new_objects(
    tx: &DatabaseTransaction,
    fresh: &[NewObject<'_>],
    decisions: &mut HashMap<String, String>,
    now: DateTime<Utc>,
    maintenance: &mut PublicationLease<'_>,
) -> AppResult<()> {
    for chunk in fresh.chunks(SQL_PARAMS / 14) {
        maintenance.checkpoint(tx).await?;
        object::Entity::insert_many(chunk.iter().map(|planned| {
            let row = planned.object;
            object::ActiveModel {
                id: Set(row.id.clone()),
                bucket: Set(row.bucket.clone()),
                key: Set(row.key.clone()),
                cid: Set(row.cid.clone()),
                size: Set(row.logical_size),
                content_type: Set(row.content_type.clone()),
                etag: Set(row.etag.clone()),
                metadata: Set(row.metadata.clone()),
                encrypted: Set(row.encrypted),
                key_wrap: Set(row.key_wrap.clone()),
                sse_c_key_fingerprint: Set(row.sse_c_key_fingerprint.clone()),
                multipart: Set(row.multipart),
                is_latest: Set(true),
                created_at: Set(now),
            }
        }))
        .exec_without_returning(tx)
        .await?;
        object_version::Entity::insert_many(chunk.iter().map(|planned| {
            object_version::ActiveModel {
                id: Set(planned.row_id.clone()),
                bucket: Set(planned.object.bucket.clone()),
                key: Set(planned.object.key.clone()),
                version_id: Set(planned.public_id.clone()),
                kind: Set("object".to_owned()),
                object_id: Set(Some(planned.object.id.clone())),
                pin_decision: Set(decisions.remove(&planned.row_id)),
                sequence: Set(1),
                is_latest: Set(true),
                lifecycle_age_started_at: Set(now),
                became_noncurrent_at: Set(None),
                created_at: Set(now),
                updated_at: Set(now),
            }
        }))
        .exec_without_returning(tx)
        .await?;
        version_residency::Entity::insert_many(chunk.iter().map(|row| {
            version_residency::ActiveModel {
                version_row_id: Set(row.row_id.clone()),
                object_id: Set(row.object.id.clone()),
                primary_tier: Set(KuboTier::Hot.as_db_str().to_owned()),
                storage_class: Set(StorageClass::Standard.as_db_str().to_owned()),
                cid: Set(row.object.cid.clone()),
                revision: Set(1),
                created_at: Set(now),
                updated_at: Set(now),
            }
        }))
        .exec_without_returning(tx)
        .await?;
        residency_reference::Entity::insert_many(chunk.iter().map(|row| {
            residency_reference::ActiveModel {
                owner_kind: Set("version".to_owned()),
                owner_id: Set(row.row_id.clone()),
                reason: Set(ReferenceReason::RetainedVersion.as_db_str().to_owned()),
                version_row_id: Set(row.row_id.clone()),
                object_id: Set(row.object.id.clone()),
                tier: Set(KuboTier::Hot.as_db_str().to_owned()),
                cid: Set(row.object.cid.clone()),
                created_at: Set(now),
            }
        }))
        .exec_without_returning(tx)
        .await?;
    }
    Ok(())
}

async fn update_replaced_decisions(
    tx: &DatabaseTransaction,
    decisions: HashMap<String, String>,
    maintenance: &mut PublicationLease<'_>,
) -> AppResult<()> {
    let decisions = decisions.into_iter().collect::<Vec<_>>();
    for chunk in decisions.chunks(SQL_PARAMS / 3) {
        maintenance.checkpoint(tx).await?;
        let mut snapshots = CaseStatement::new();
        for (row_id, decision) in chunk {
            snapshots =
                snapshots.case(object_version::Column::Id.eq(row_id), Expr::value(decision));
        }
        object_version::Entity::update_many()
            .col_expr(
                object_version::Column::PinDecision,
                snapshots
                    .finally(Expr::col(object_version::Column::PinDecision))
                    .into(),
            )
            .filter(object_version::Column::Id.is_in(chunk.iter().map(|(row_id, _)| row_id)))
            .exec(tx)
            .await?;
    }
    Ok(())
}

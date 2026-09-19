use std::collections::HashMap;

use s3s::S3Result;
use sea_orm::{
    AccessMode, ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait,
    IsolationLevel, QueryFilter, TransactionTrait,
};

use crate::{
    error::{AppError, AppResult},
    residency::{KuboTier, StorageClass as ResidencyStorageClass, VerificationState},
    store::entities::{object, object_version, physical_residency, version_residency},
};

const LOOKUP_CHUNK_SIZE: usize = 250;

pub(crate) fn require_standard_write_headers(headers: &http::HeaderMap) -> S3Result<()> {
    for value in headers.get_all("x-amz-storage-class") {
        let value = value.to_str().map_err(|_| {
            s3s::s3_error!(
                InvalidRequest,
                "direct writes only support STANDARD storage class"
            )
        })?;
        require_standard_write(Some(&s3s::dto::StorageClass::from(value.to_owned())))?;
    }
    Ok(())
}

pub(super) fn require_standard_write(
    storage_class: Option<&s3s::dto::StorageClass>,
) -> S3Result<()> {
    if storage_class.is_some_and(|storage_class| storage_class.as_str() != "STANDARD") {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "direct writes only support STANDARD storage class"
        ));
    }
    Ok(())
}

pub(super) async fn classes_for_objects(
    db: &DatabaseConnection,
    objects: &[&object::Model],
) -> AppResult<HashMap<String, ResidencyStorageClass>> {
    if objects.is_empty() {
        return Ok(HashMap::new());
    }
    // All batched owner/residency/physical reads must see one publication
    // snapshot. PostgreSQL's default READ COMMITTED is not sufficient here;
    // SQLite BEGIN already retains the snapshot established by its first read.
    let txn = if db.get_database_backend() == DatabaseBackend::Sqlite {
        db.begin().await?
    } else {
        db.begin_with_config(
            Some(IsolationLevel::RepeatableRead),
            Some(AccessMode::ReadOnly),
        )
        .await?
    };
    let classes = classes_in_snapshot(&txn, objects).await?;
    txn.commit().await?;
    Ok(classes)
}

async fn classes_in_snapshot<C: ConnectionTrait>(
    db: &C,
    objects: &[&object::Model],
) -> AppResult<HashMap<String, ResidencyStorageClass>> {
    let mut selected = HashMap::with_capacity(objects.len());
    for &object in objects {
        if let Some(existing) = selected.insert(object.id.clone(), object)
            && existing != object
        {
            return Err(corrupt("conflicting selected immutable objects"));
        }
    }
    let mut object_ids = selected.keys().cloned().collect::<Vec<_>>();
    object_ids.sort();

    let mut versions_by_object = HashMap::with_capacity(object_ids.len());
    let mut object_by_version = HashMap::with_capacity(object_ids.len());
    for chunk in object_ids.chunks(LOOKUP_CHUNK_SIZE) {
        let rows = object_version::Entity::find()
            .filter(object_version::Column::ObjectId.is_in(chunk.to_vec()))
            .all(db)
            .await?;
        for version in rows {
            let object_id = version
                .object_id
                .clone()
                .ok_or_else(|| corrupt("content version has no immutable object"))?;
            let object = selected
                .get(&object_id)
                .ok_or_else(|| corrupt("version owner is not a selected immutable object"))?;
            if version.kind != "object"
                || version.bucket != object.bucket
                || version.key != object.key
            {
                return Err(corrupt(
                    "version owner does not match immutable object metadata",
                ));
            }
            if versions_by_object
                .insert(object_id.clone(), version.clone())
                .is_some()
            {
                return Err(corrupt("immutable object has multiple content versions"));
            }
            object_by_version.insert(version.id.clone(), object_id);
        }
    }

    let mut residencies_by_version = HashMap::with_capacity(object_ids.len());
    for chunk in object_ids.chunks(LOOKUP_CHUNK_SIZE) {
        let rows = version_residency::Entity::find()
            .filter(version_residency::Column::ObjectId.is_in(chunk.to_vec()))
            .all(db)
            .await?;
        merge_residencies(&mut residencies_by_version, rows)?;
    }
    let mut version_ids = object_by_version.keys().cloned().collect::<Vec<_>>();
    version_ids.sort();
    for chunk in version_ids.chunks(LOOKUP_CHUNK_SIZE) {
        let rows = version_residency::Entity::find()
            .filter(version_residency::Column::VersionRowId.is_in(chunk.to_vec()))
            .all(db)
            .await?;
        merge_residencies(&mut residencies_by_version, rows)?;
    }

    let mut residencies_by_object = HashMap::with_capacity(residencies_by_version.len());
    for residency in residencies_by_version.into_values() {
        let Some(version_object_id) = object_by_version.get(&residency.version_row_id) else {
            return Err(corrupt("residency owner version is missing"));
        };
        if version_object_id != &residency.object_id || !selected.contains_key(&residency.object_id)
        {
            return Err(corrupt("residency owner is not the exact content version"));
        }
        if residencies_by_object
            .insert(residency.object_id.clone(), residency)
            .is_some()
        {
            return Err(corrupt("immutable object has multiple residencies"));
        }
    }

    let mut physical_by_location = HashMap::with_capacity(residencies_by_object.len());
    let mut residency_cids = residencies_by_object
        .values()
        .map(|residency| residency.cid.clone())
        .collect::<Vec<_>>();
    residency_cids.sort();
    residency_cids.dedup();
    for chunk in residency_cids.chunks(LOOKUP_CHUNK_SIZE) {
        let rows = physical_residency::Entity::find()
            .filter(physical_residency::Column::Cid.is_in(chunk.to_vec()))
            .all(db)
            .await?;
        for physical in rows {
            let location = (physical.tier.clone(), physical.cid.clone());
            if physical_by_location.insert(location, physical).is_some() {
                return Err(corrupt("duplicate physical residency"));
            }
        }
    }

    let mut classes = HashMap::with_capacity(selected.len());
    for object_id in object_ids {
        let Some(version) = versions_by_object.get(&object_id) else {
            // Pre-residency object rows have no version index at all.
            classes.insert(object_id, ResidencyStorageClass::Standard);
            continue;
        };
        let Some(residency) = residencies_by_object.get(&object_id) else {
            // Indexed legacy live versions are the lifecycle scanner's explicit
            // no-residency branch and remain STANDARD until backfilled.
            classes.insert(object_id, ResidencyStorageClass::Standard);
            continue;
        };
        let object = selected[&object_id];
        if residency.version_row_id != version.id
            || residency.object_id != object.id
            || version.object_id.as_deref() != Some(object.id.as_str())
            || residency.cid != object.cid
            || version.bucket != object.bucket
            || version.key != object.key
        {
            return Err(corrupt(
                "residency identity does not match immutable object metadata",
            ));
        }

        let tier = KuboTier::from_db_str(&residency.primary_tier)?;
        let storage_class = ResidencyStorageClass::from_db_str(&residency.storage_class)?;
        if residency.revision <= 0
            || !matches!(
                (tier, storage_class),
                (KuboTier::Hot, ResidencyStorageClass::Standard)
                    | (KuboTier::Cold, ResidencyStorageClass::StandardIa)
            )
        {
            return Err(corrupt("invalid primary residency"));
        }
        let physical = physical_by_location
            .get(&(residency.primary_tier.clone(), residency.cid.clone()))
            .ok_or_else(|| corrupt("primary physical residency is missing"))?;
        validate_physical(physical, tier)?;
        classes.insert(object_id, storage_class);
    }
    Ok(classes)
}

fn merge_residencies(
    target: &mut HashMap<String, version_residency::Model>,
    rows: Vec<version_residency::Model>,
) -> AppResult<()> {
    for residency in rows {
        if let Some(existing) = target.insert(residency.version_row_id.clone(), residency.clone())
            && existing != residency
        {
            return Err(corrupt("residency changed during immutable lookup"));
        }
    }
    Ok(())
}

fn validate_physical(physical: &physical_residency::Model, tier: KuboTier) -> AppResult<()> {
    if KuboTier::from_db_str(&physical.tier)? != tier {
        return Err(corrupt("physical residency tier mismatch"));
    }
    let verification = VerificationState::from_db_str(&physical.verification_state)?;
    let valid_shape = match verification {
        VerificationState::Verified => {
            physical
                .node_identity
                .as_deref()
                .is_some_and(|value| !value.is_empty())
                && physical
                    .verification_receipt
                    .as_deref()
                    .is_some_and(|value| !value.is_empty())
                && physical.verified_at.is_some()
        }
        VerificationState::Pending | VerificationState::Failed => {
            physical.node_identity.is_none()
                && physical.verification_receipt.is_none()
                && physical.verified_at.is_none()
        }
    };
    if !valid_shape {
        return Err(corrupt("invalid physical verification shape"));
    }
    if tier == KuboTier::Cold && verification != VerificationState::Verified {
        return Err(corrupt("cold physical residency is not verified"));
    }
    if tier == KuboTier::Cold {
        crate::residency::router::validate_cold_receipt(
            &physical.cid,
            physical.node_identity.as_deref(),
            physical.verification_receipt.as_deref(),
        )?;
    }
    Ok(())
}

fn corrupt(message: &str) -> AppError {
    AppError::Internal(format!("invalid residency state: {message}"))
}

#[cfg(test)]
mod tests {
    use sea_orm::{ConnectionTrait, Database, DatabaseConnection, EntityTrait};

    use super::*;
    use crate::{error::AppError, store};

    async fn setup() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        store::run_migrations(&db).await.unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
            .await
            .unwrap();
        db
    }

    async fn insert_object(
        db: &DatabaseConnection,
        id: &str,
        key: &str,
        cid: &str,
        indexed: bool,
    ) -> object::Model {
        db.execute_unprepared(&format!(
            "INSERT INTO objects (id, bucket, key, cid, size, etag) \
             VALUES ('{id}', 'bucket', '{key}', '{cid}', 1, '{cid}')"
        ))
        .await
        .unwrap();
        if indexed {
            db.execute_unprepared(&format!(
                "INSERT INTO object_versions \
                 (id, bucket, key, kind, object_id, sequence, is_latest, \
                  lifecycle_age_started_at, created_at, updated_at) \
                 VALUES ('version-{id}', 'bucket', '{key}', 'object', '{id}', 1, TRUE, \
                         CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            ))
            .await
            .unwrap();
        }
        object::Entity::find_by_id(id)
            .one(db)
            .await
            .unwrap()
            .unwrap()
    }

    async fn insert_residency(
        db: &DatabaseConnection,
        object_id: &str,
        tier: &str,
        class: &str,
        cid: &str,
        verification_state: &str,
    ) {
        let receipt_json = serde_json::to_string(&crate::kubo::LocalResidencyVerificationReceipt {
            cid: cid.to_owned(),
            node_identity: "node".to_owned(),
        })
        .unwrap();
        let encoded_receipt = format!("'{receipt_json}'");
        let (node_identity, receipt, verified_at) = if verification_state == "verified" {
            ("'node'", encoded_receipt.as_str(), "CURRENT_TIMESTAMP")
        } else {
            ("NULL", "NULL", "NULL")
        };
        db.execute_unprepared(&format!(
            "INSERT INTO physical_residencies \
             (tier, cid, node_identity, verification_state, verification_receipt, verified_at, \
              created_at, updated_at) \
             VALUES ('{tier}', '{cid}', {}, '{verification_state}', {}, {}, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            node_identity, receipt, verified_at,
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO version_residencies \
             (version_row_id, object_id, primary_tier, storage_class, cid, revision, \
              created_at, updated_at) \
             VALUES ('version-{object_id}', '{object_id}', '{tier}', '{class}', '{cid}', 1, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        ))
        .await
        .unwrap();
    }

    fn assert_internal<T>(result: Result<T, AppError>) {
        assert!(matches!(result, Err(AppError::Internal(_))));
    }

    #[test]
    fn direct_writes_accept_only_standard_or_unspecified() {
        assert!(require_standard_write(None).is_ok());
        let standard = s3s::dto::StorageClass::from_static("STANDARD");
        assert!(require_standard_write(Some(&standard)).is_ok());

        let standard_ia = s3s::dto::StorageClass::from_static("STANDARD_IA");
        let error = require_standard_write(Some(&standard_ia)).unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidRequest");
        assert_eq!(
            error.message(),
            Some("direct writes only support STANDARD storage class")
        );
    }

    #[tokio::test]
    async fn legacy_objects_default_to_standard_only_when_residency_is_absent() {
        let db = setup().await;
        let object_only = insert_object(&db, "object-only", "old", "cid-old", false).await;
        let indexed = insert_object(&db, "indexed", "indexed", "cid-indexed", true).await;

        let classes = classes_for_objects(&db, &[&object_only, &indexed])
            .await
            .unwrap();
        assert_eq!(classes["object-only"], ResidencyStorageClass::Standard);
        assert_eq!(classes["indexed"], ResidencyStorageClass::Standard);
    }

    #[tokio::test]
    async fn same_cid_can_have_different_classes_by_immutable_object_id() {
        let db = setup().await;
        let cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
        let hot = insert_object(&db, "hot-object", "hot", cid, true).await;
        let cold = insert_object(&db, "cold-object", "cold", cid, true).await;
        insert_residency(&db, "hot-object", "hot", "STANDARD", cid, "verified").await;
        insert_residency(&db, "cold-object", "cold", "STANDARD_IA", cid, "verified").await;

        let classes = classes_for_objects(&db, &[&hot, &cold]).await.unwrap();
        assert_eq!(classes["hot-object"], ResidencyStorageClass::Standard);
        assert_eq!(classes["cold-object"], ResidencyStorageClass::StandardIa);
    }

    #[tokio::test]
    async fn corrupt_owner_class_tier_and_physical_state_never_default_to_standard() {
        let db = setup().await;
        let owner = insert_object(&db, "bad-owner", "owner", "cid-owner", true).await;
        insert_residency(&db, "bad-owner", "hot", "STANDARD", "cid-owner", "verified").await;
        db.execute_unprepared("PRAGMA foreign_keys = OFF")
            .await
            .unwrap();
        db.execute_unprepared(
            "UPDATE object_versions SET key = 'other' WHERE id = 'version-bad-owner'",
        )
        .await
        .unwrap();
        assert_internal(classes_for_objects(&db, &[&owner]).await);

        let db = setup().await;
        let class = insert_object(&db, "bad-class", "class", "cid-class", true).await;
        insert_residency(&db, "bad-class", "hot", "STANDARD", "cid-class", "verified").await;
        db.execute_unprepared("PRAGMA ignore_check_constraints = ON")
            .await
            .unwrap();
        db.execute_unprepared(
            "UPDATE version_residencies SET storage_class = 'STANDARD_IA' \
             WHERE object_id = 'bad-class'",
        )
        .await
        .unwrap();
        assert_internal(classes_for_objects(&db, &[&class]).await);

        let db = setup().await;
        let tier = insert_object(&db, "bad-tier", "tier", "cid-tier", true).await;
        insert_residency(&db, "bad-tier", "hot", "STANDARD", "cid-tier", "verified").await;
        db.execute_unprepared("PRAGMA foreign_keys = OFF")
            .await
            .unwrap();
        db.execute_unprepared("PRAGMA ignore_check_constraints = ON")
            .await
            .unwrap();
        db.execute_unprepared(
            "UPDATE version_residencies SET primary_tier = 'archive' \
             WHERE object_id = 'bad-tier'",
        )
        .await
        .unwrap();
        assert_internal(classes_for_objects(&db, &[&tier]).await);

        let db = setup().await;
        let missing = insert_object(&db, "missing-physical", "missing", "cid-missing", true).await;
        insert_residency(
            &db,
            "missing-physical",
            "hot",
            "STANDARD",
            "cid-missing",
            "verified",
        )
        .await;
        db.execute_unprepared("PRAGMA foreign_keys = OFF")
            .await
            .unwrap();
        db.execute_unprepared(
            "DELETE FROM physical_residencies WHERE tier = 'hot' AND cid = 'cid-missing'",
        )
        .await
        .unwrap();
        assert_internal(classes_for_objects(&db, &[&missing]).await);

        let db = setup().await;
        let physical = insert_object(&db, "bad-physical", "physical", "cid-physical", true).await;
        insert_residency(
            &db,
            "bad-physical",
            "hot",
            "STANDARD",
            "cid-physical",
            "verified",
        )
        .await;
        db.execute_unprepared("PRAGMA ignore_check_constraints = ON")
            .await
            .unwrap();
        db.execute_unprepared(
            "UPDATE physical_residencies SET verification_state = 'unknown' \
             WHERE tier = 'hot' AND cid = 'cid-physical'",
        )
        .await
        .unwrap();
        assert_internal(classes_for_objects(&db, &[&physical]).await);
    }

    #[tokio::test]
    async fn cold_residency_must_be_verified_with_a_receipt() {
        let db = setup().await;
        let pending = insert_object(&db, "cold-pending", "pending", "cid-pending", true).await;
        insert_residency(
            &db,
            "cold-pending",
            "cold",
            "STANDARD_IA",
            "cid-pending",
            "pending",
        )
        .await;
        assert_internal(classes_for_objects(&db, &[&pending]).await);

        let db = setup().await;
        let no_receipt =
            insert_object(&db, "cold-no-receipt", "no-receipt", "cid-no-receipt", true).await;
        insert_residency(
            &db,
            "cold-no-receipt",
            "cold",
            "STANDARD_IA",
            "cid-no-receipt",
            "verified",
        )
        .await;
        db.execute_unprepared("PRAGMA ignore_check_constraints = ON")
            .await
            .unwrap();
        db.execute_unprepared(
            "UPDATE physical_residencies SET verification_receipt = NULL \
             WHERE tier = 'cold' AND cid = 'cid-no-receipt'",
        )
        .await
        .unwrap();
        assert_internal(classes_for_objects(&db, &[&no_receipt]).await);
    }

    #[tokio::test]
    async fn lookup_batches_more_than_250_objects() {
        let db = setup().await;
        for index in 0..251 {
            insert_object(
                &db,
                &format!("object-{index}"),
                &format!("key-{index}"),
                &format!("cid-{index}"),
                true,
            )
            .await;
        }
        let objects = object::Entity::find().all(&db).await.unwrap();
        let refs = objects.iter().collect::<Vec<_>>();
        let classes = classes_for_objects(&db, &refs).await.unwrap();
        assert_eq!(classes.len(), 251);
        assert!(
            classes
                .values()
                .all(|class| *class == ResidencyStorageClass::Standard)
        );
    }

    #[tokio::test]
    async fn multiple_version_rows_for_one_object_are_corruption() {
        let db = setup().await;
        let object = insert_object(&db, "duplicate-owner", "duplicate", "cid", true).await;
        db.execute_unprepared(
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, \
              lifecycle_age_started_at, created_at, updated_at) \
             VALUES ('version-duplicate-owner-2', 'bucket', 'duplicate', 'v2', 'object', \
                     'duplicate-owner', 2, FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, \
                     CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();

        assert_internal(classes_for_objects(&db, &[&object]).await);
    }
}

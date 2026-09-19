use chrono::Utc;
use ipfs_s3_gateway::{
    import::SupersedeReason,
    pinning::{
        config::{LeaseDuration, ProviderLimitMap, ProviderLimits, ProviderMode},
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::ContentMode,
    },
    residency::{KuboTier, ResidencyLocation, StorageClass, VerificationState},
    store::{
        self,
        entities::{
            object_version, physical_residency, pin_lease, residency_reference, version_residency,
        },
        import::ownership::admit_content_mutation,
        object_version::{BucketVersioningState, PublicVersionId, VersionSelector},
        pinning::publication::{
            PinTargetSpec, PublicationObject, PublicationRequest,
            delete_version_with_leases_guarded, publish_object,
        },
        residency::{reference_summary_in_transaction, resolve_version_residency},
    },
};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    EntityTrait, PaginatorTrait, QueryFilter, Set, Statement, TransactionTrait,
};

const SHARED_CID: &str = "bafy-store-residency-shared";

async fn sqlite_db() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    db
}

async fn create_bucket(db: &DatabaseConnection, name: &str, state: BucketVersioningState) {
    store::bucket::create(db, name, None).await.unwrap();
    if state != BucketVersioningState::Unversioned {
        store::bucket::set_versioning_state(db, name, state)
            .await
            .unwrap();
    }
}

fn provider_limits() -> ProviderLimitMap {
    ProviderLimitMap::from([(
        "pinata".to_owned(),
        ProviderLimits {
            priority: 1,
            max_bytes: 10_000,
            max_pins: 100,
            enabled: true,
        },
    )])
}

fn publication_request(
    id: &str,
    bucket: &str,
    key: &str,
    cid: &str,
    with_lease: bool,
) -> PublicationRequest {
    let object = PublicationObject::from_put(
        id.to_owned(),
        bucket,
        key,
        cid.to_owned(),
        7,
        Some("application/octet-stream".to_owned()),
        None,
        false,
        None,
        None,
        Utc::now(),
    );
    let leases = with_lease
        .then(|| LeaseIntent {
            source: LeaseSource::Manual,
            policy_id: "policy:residency-publication".to_owned(),
            provider_mode: ProviderMode::One,
            providers: vec!["pinata".to_owned()],
            content_mode: ContentMode::Object,
            duration: LeaseDuration::parse("1h").unwrap(),
        })
        .into_iter()
        .collect();
    PublicationRequest {
        object: object.clone(),
        tags: Vec::new(),
        policy: PublicationPolicy {
            tags: Vec::new(),
            leases,
        },
        object_target: PinTargetSpec {
            cid: object.cid,
            logical_size: object.logical_size,
        },
    }
}

async fn publish(
    db: &DatabaseConnection,
    id: &str,
    bucket: &str,
    key: &str,
    cid: &str,
    with_lease: bool,
) -> store::object_version::PublicationResult {
    publish_object(
        db,
        publication_request(id, bucket, key, cid, with_lease),
        &provider_limits(),
    )
    .await
    .unwrap()
}

async fn guarded_delete(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    selector: VersionSelector,
) -> store::object_version::DeleteVersionResult {
    let now = Utc::now();
    let guard = admit_content_mutation(db, bucket, key, None, SupersedeReason::DeleteObject, now)
        .await
        .unwrap();
    delete_version_with_leases_guarded(db, bucket, key, selector, guard, now)
        .await
        .unwrap()
}

async fn version_for_object(db: &DatabaseConnection, object_id: &str) -> object_version::Model {
    object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(object_id))
        .one(db)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("object {object_id} must have a version row"))
}

async fn retained_reference_count(db: &DatabaseConnection, cid: &str) -> u64 {
    residency_reference::Entity::find()
        .filter(residency_reference::Column::Cid.eq(cid))
        .filter(residency_reference::Column::Reason.eq("retained_version"))
        .count(db)
        .await
        .unwrap()
}

async fn physical_count(db: &DatabaseConnection, tier: &str, cid: &str) -> u64 {
    physical_residency::Entity::find()
        .filter(physical_residency::Column::Tier.eq(tier))
        .filter(physical_residency::Column::Cid.eq(cid))
        .count(db)
        .await
        .unwrap()
}

async fn publication_shared_cid_scenario(db: &DatabaseConnection, shared_cid: &str) {
    create_bucket(db, "res-pub-shared-a", BucketVersioningState::Enabled).await;
    create_bucket(db, "res-pub-shared-b", BucketVersioningState::Enabled).await;

    for (id, bucket, key) in [
        ("res-shared-a-v1", "res-pub-shared-a", "same-key"),
        ("res-shared-a-v2", "res-pub-shared-a", "same-key"),
        ("res-shared-a-other", "res-pub-shared-a", "other-key"),
        ("res-shared-b", "res-pub-shared-b", "same-key"),
    ] {
        let result = publish(db, id, bucket, key, shared_cid, false).await;
        assert!(
            matches!(
                result.version_id.as_deref().map(PublicVersionId::parse_s3),
                Some(Ok(PublicVersionId::Opaque(_)))
            ),
            "enabled publication must expose an opaque version ID"
        );
        let version = version_for_object(db, id).await;
        let residency = resolve_version_residency(db, &version.id).await.unwrap();
        assert_eq!(residency.identity.version_row_id, version.id);
        assert_eq!(residency.identity.object_id, id);
        assert_eq!(residency.identity.cid, shared_cid);
        assert_eq!(residency.primary.tier, KuboTier::Hot);
        assert_eq!(residency.storage_class, StorageClass::Standard);
        assert_eq!(residency.revision, 1);
        assert_eq!(
            residency.physical.verification_state,
            VerificationState::Pending
        );
    }

    assert_eq!(physical_count(db, "hot", shared_cid).await, 1);
    assert_eq!(retained_reference_count(db, shared_cid).await, 4);
}

async fn null_overwrite_scenario(db: &DatabaseConnection, shared_cid: &str) {
    create_bucket(db, "res-null-overwrite", BucketVersioningState::Suspended).await;
    create_bucket(db, "res-null-peer", BucketVersioningState::Enabled).await;

    let old = publish(
        db,
        "res-null-old",
        "res-null-overwrite",
        "key",
        shared_cid,
        false,
    )
    .await;
    assert_eq!(old.version_id.as_deref(), Some("null"));
    publish(
        db,
        "res-null-peer-object",
        "res-null-peer",
        "key",
        shared_cid,
        false,
    )
    .await;
    let old_version = version_for_object(db, "res-null-old").await;
    let peer_version = version_for_object(db, "res-null-peer-object").await;
    let peer_before = resolve_version_residency(db, &peer_version.id)
        .await
        .unwrap();

    let replacement = publish(
        db,
        "res-null-new",
        "res-null-overwrite",
        "key",
        "bafy-store-residency-replacement",
        false,
    )
    .await;
    assert_eq!(replacement.version_id.as_deref(), Some("null"));

    assert!(
        object_version::Entity::find_by_id(&old_version.id)
            .one(db)
            .await
            .unwrap()
            .is_none(),
        "a null overwrite must remove only the displaced null version"
    );
    assert!(
        version_residency::Entity::find_by_id(&old_version.id)
            .one(db)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        resolve_version_residency(db, &peer_version.id)
            .await
            .unwrap(),
        peer_before
    );
    let replacement_version = version_for_object(db, "res-null-new").await;
    assert_eq!(
        resolve_version_residency(db, &replacement_version.id)
            .await
            .unwrap()
            .physical
            .verification_state,
        VerificationState::Pending
    );
    assert_eq!(physical_count(db, "hot", shared_cid).await, 1);
    assert_eq!(retained_reference_count(db, shared_cid).await, 1);
}

async fn enabled_marker_scenario(db: &DatabaseConnection, shared_cid: &str) {
    let bucket = "res-enabled-marker";
    let key = "key";
    create_bucket(db, bucket, BucketVersioningState::Enabled).await;
    publish(
        db,
        "res-enabled-marker-object",
        bucket,
        key,
        shared_cid,
        true,
    )
    .await;
    let content_version = version_for_object(db, "res-enabled-marker-object").await;
    let before = resolve_version_residency(db, &content_version.id)
        .await
        .unwrap();

    let deleted = guarded_delete(db, bucket, key, VersionSelector::Current).await;
    assert!(deleted.created_delete_marker);
    let marker_id = deleted
        .version_id
        .expect("enabled delete creates a marker ID");
    let marker = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq(bucket))
        .filter(object_version::Column::Key.eq(key))
        .filter(object_version::Column::VersionId.eq(marker_id))
        .one(db)
        .await
        .unwrap()
        .expect("enabled marker must be indexed");
    assert_eq!(marker.kind, "delete_marker");
    assert!(marker.is_latest);
    assert!(
        version_residency::Entity::find_by_id(&marker.id)
            .one(db)
            .await
            .unwrap()
            .is_none(),
        "delete markers must never gain residency"
    );
    assert_eq!(
        resolve_version_residency(db, &content_version.id)
            .await
            .unwrap(),
        before,
        "an enabled marker must preserve the noncurrent content residency"
    );

    let txn = db.begin().await.unwrap();
    let summary =
        reference_summary_in_transaction(&txn, &ResidencyLocation::new(KuboTier::Hot, shared_cid))
            .await
            .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(summary.retained_versions, 1);
    assert_eq!(summary.active_lease_targets, 1);
}

async fn suspended_marker_scenario(db: &DatabaseConnection, shared_cid: &str) {
    let bucket = "res-suspended-marker";
    let key = "target";
    create_bucket(db, bucket, BucketVersioningState::Enabled).await;
    publish(db, "res-suspended-opaque", bucket, key, shared_cid, false).await;
    let opaque_version = version_for_object(db, "res-suspended-opaque").await;
    let opaque_before = resolve_version_residency(db, &opaque_version.id)
        .await
        .unwrap();

    store::bucket::set_versioning_state(db, bucket, BucketVersioningState::Suspended)
        .await
        .unwrap();
    publish(db, "res-suspended-null", bucket, key, shared_cid, false).await;
    publish(db, "res-suspended-peer", bucket, "peer", shared_cid, false).await;
    let null_version = version_for_object(db, "res-suspended-null").await;
    let peer_version = version_for_object(db, "res-suspended-peer").await;
    let peer_before = resolve_version_residency(db, &peer_version.id)
        .await
        .unwrap();

    let deleted = guarded_delete(db, bucket, key, VersionSelector::Current).await;
    assert!(deleted.created_delete_marker);
    assert_eq!(deleted.version_id.as_deref(), Some("null"));
    assert!(
        object_version::Entity::find_by_id(&null_version.id)
            .one(db)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        version_residency::Entity::find_by_id(&null_version.id)
            .one(db)
            .await
            .unwrap()
            .is_none(),
        "a suspended delete must release the displaced null content"
    );
    assert_eq!(
        resolve_version_residency(db, &opaque_version.id)
            .await
            .unwrap(),
        opaque_before,
        "a suspended delete must not release opaque history"
    );
    assert_eq!(
        resolve_version_residency(db, &peer_version.id)
            .await
            .unwrap(),
        peer_before,
        "a suspended delete must not release another key sharing the CID"
    );
    let marker = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq(bucket))
        .filter(object_version::Column::Key.eq(key))
        .filter(object_version::Column::VersionId.is_null())
        .one(db)
        .await
        .unwrap()
        .expect("suspended null marker must be indexed");
    assert_eq!(marker.kind, "delete_marker");
    assert!(
        version_residency::Entity::find_by_id(marker.id)
            .one(db)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(physical_count(db, "hot", shared_cid).await, 1);
    assert_eq!(retained_reference_count(db, shared_cid).await, 2);
}

async fn move_residency_to_cold(
    db: &DatabaseConnection,
    version_row_id: &str,
) -> version_residency::Model {
    let current = version_residency::Entity::find_by_id(version_row_id)
        .one(db)
        .await
        .unwrap()
        .expect("published version must have residency");
    physical_residency::Entity::insert(physical_residency::ActiveModel {
        tier: Set("cold".to_owned()),
        cid: Set(current.cid.clone()),
        node_identity: Set(None),
        verification_state: Set("pending".to_owned()),
        verification_receipt: Set(None),
        verified_at: Set(None),
        created_at: Set(current.created_at),
        updated_at: Set(current.updated_at),
    })
    .exec(db)
    .await
    .unwrap();
    version_residency::Entity::update_many()
        .col_expr(
            version_residency::Column::PrimaryTier,
            sea_orm::sea_query::Expr::value("cold"),
        )
        .col_expr(
            version_residency::Column::StorageClass,
            sea_orm::sea_query::Expr::value("STANDARD_IA"),
        )
        .col_expr(
            version_residency::Column::Revision,
            sea_orm::sea_query::Expr::value(7_i64),
        )
        .filter(version_residency::Column::VersionRowId.eq(version_row_id))
        .exec(db)
        .await
        .unwrap();
    residency_reference::Entity::update_many()
        .col_expr(
            residency_reference::Column::Tier,
            sea_orm::sea_query::Expr::value("cold"),
        )
        .filter(residency_reference::Column::VersionRowId.eq(version_row_id))
        .filter(residency_reference::Column::OwnerKind.eq("version"))
        .exec(db)
        .await
        .unwrap();
    version_residency::Entity::find_by_id(version_row_id)
        .one(db)
        .await
        .unwrap()
        .unwrap()
}

async fn exact_delete_promotion_scenario(db: &DatabaseConnection, shared_cid: &str) {
    let bucket = "res-exact-promote";
    let key = "key";
    create_bucket(db, bucket, BucketVersioningState::Enabled).await;
    publish(db, "res-promote-old", bucket, key, shared_cid, true).await;
    let current = publish(db, "res-promote-current", bucket, key, shared_cid, true).await;
    let old_version = version_for_object(db, "res-promote-old").await;
    let current_version = version_for_object(db, "res-promote-current").await;
    let promoted_residency_before = move_residency_to_cold(db, &old_version.id).await;

    let current_public_id = current
        .version_id
        .expect("enabled publication must have a public version ID");
    let deleted = guarded_delete(
        db,
        bucket,
        key,
        VersionSelector::Exact(PublicVersionId::parse_s3(&current_public_id).unwrap()),
    )
    .await;
    assert_eq!(
        deleted.version_id.as_deref(),
        Some(current_public_id.as_str())
    );

    let promoted =
        store::object_version::resolve_version(db, bucket, key, &VersionSelector::Current)
            .await
            .unwrap();
    assert_eq!(promoted.id, old_version.id);
    let promoted_residency_after = version_residency::Entity::find_by_id(&old_version.id)
        .one(db)
        .await
        .unwrap()
        .expect("promoted content must retain its residency");
    assert_eq!(promoted_residency_after, promoted_residency_before);
    let resolved = resolve_version_residency(db, &old_version.id)
        .await
        .unwrap();
    assert_eq!(resolved.primary.tier, KuboTier::Cold);
    assert_eq!(resolved.storage_class, StorageClass::StandardIa);
    assert_eq!(resolved.revision, 7);
    assert!(
        version_residency::Entity::find_by_id(&current_version.id)
            .one(db)
            .await
            .unwrap()
            .is_none(),
        "exact deletion must release only the selected version"
    );

    let old_lease = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq("res-promote-old"))
        .one(db)
        .await
        .unwrap()
        .expect("older publication lease must exist");
    let deleted_lease = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq("res-promote-current"))
        .one(db)
        .await
        .unwrap()
        .expect("deleted publication lease must remain auditable");
    assert_eq!(old_lease.state, "active");
    assert_ne!(deleted_lease.state, "active");

    let txn = db.begin().await.unwrap();
    let summary =
        reference_summary_in_transaction(&txn, &ResidencyLocation::new(KuboTier::Cold, shared_cid))
            .await
            .unwrap();
    let hot =
        reference_summary_in_transaction(&txn, &ResidencyLocation::new(KuboTier::Hot, shared_cid))
            .await
            .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(summary.retained_versions, 1);
    assert_eq!(summary.active_lease_targets, 0);
    assert_eq!(hot.active_lease_targets, 1);
}

#[tokio::test]
async fn publication_attaches_pending_hot_residency_for_shared_cid_across_locations() {
    publication_shared_cid_scenario(&sqlite_db().await, SHARED_CID).await;
}

#[tokio::test]
async fn null_overwrite_releases_only_the_displaced_version_reference() {
    null_overwrite_scenario(&sqlite_db().await, SHARED_CID).await;
}

#[tokio::test]
async fn enabled_delete_marker_preserves_content_residency_and_lease() {
    enabled_marker_scenario(&sqlite_db().await, SHARED_CID).await;
}

#[tokio::test]
async fn suspended_delete_marker_releases_null_content_only() {
    suspended_marker_scenario(&sqlite_db().await, SHARED_CID).await;
}

#[tokio::test]
async fn exact_delete_promotion_preserves_revision_class_and_other_lease() {
    exact_delete_promotion_scenario(&sqlite_db().await, SHARED_CID).await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL (real PostgreSQL)"]
async fn postgres_reuses_publication_and_delete_residency_scenarios() {
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("IPFS_S3_TEST_POSTGRES_URL must be set for ignored PostgreSQL tests");
    let schema = format!("residency_publication_{}", uuid::Uuid::new_v4().simple());
    let mut options = ConnectOptions::new(url);
    options.max_connections(1).min_connections(1);
    let db = Database::connect(options).await.unwrap();
    db.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    db.execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();

    publication_shared_cid_scenario(&db, "bafy-pg-residency-publication-shared").await;
    null_overwrite_scenario(&db, "bafy-pg-residency-null-overwrite").await;
    enabled_marker_scenario(&db, "bafy-pg-residency-enabled-marker").await;
    suspended_marker_scenario(&db, "bafy-pg-residency-suspended-marker").await;
    exact_delete_promotion_scenario(&db, "bafy-pg-residency-exact-promotion").await;

    db.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        format!("DROP SCHEMA {schema} CASCADE"),
    ))
    .await
    .unwrap();
}

use super::*;
use crate::{
    import::SupersedeReason,
    kubo::LocalResidencyVerificationReceipt,
    residency::VerificationState,
    store::entities::{
        object, object_version, physical_residency, residency_reference, version_residency,
    },
};
use sea_orm::{ActiveValue::Set, EntityTrait, PaginatorTrait};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

async fn guard(db: &DatabaseConnection, key: &str) -> StandardMutationGuard {
    admit_content_mutation(
        db,
        "bucket",
        key,
        None,
        SupersedeReason::CopyObject,
        Utc::now(),
    )
    .await
    .unwrap()
}

async fn counts(db: &DatabaseConnection) -> (u64, u64, u64, u64) {
    (
        object::Entity::find().count(db).await.unwrap(),
        object_version::Entity::find().count(db).await.unwrap(),
        version_residency::Entity::find().count(db).await.unwrap(),
        residency_reference::Entity::find().count(db).await.unwrap(),
    )
}

async fn insert_verified_hot(db: &DatabaseConnection, node: &str) {
    let now = Utc::now();
    let receipt = serde_json::to_string(&LocalResidencyVerificationReceipt {
        node_identity: node.to_owned(),
        cid: CID.to_owned(),
    })
    .unwrap();
    physical_residency::Entity::insert(physical_residency::ActiveModel {
        tier: Set("hot".to_owned()),
        cid: Set(CID.to_owned()),
        node_identity: Set(Some(node.to_owned())),
        verification_state: Set("verified".to_owned()),
        verification_receipt: Set(Some(receipt)),
        verified_at: Set(Some(now)),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(db)
    .await
    .unwrap();
}

#[tokio::test]
async fn hot_receipt_rejects_existing_verified_binding_to_another_node_atomically() {
    let db = setup().await;
    insert_verified_hot(&db, "foreign-hot-node").await;
    let before = counts(&db).await;
    let publication = request(
        object("destination-object", "destination", CID, 7),
        vec![],
        vec![],
    );

    let error = publish_standard_object_with_hot_receipt(
        &db,
        publication,
        guard(&db, "destination").await,
        LocalResidencyVerificationReceipt {
            node_identity: "destination-hot-node".to_owned(),
            cid: CID.to_owned(),
        },
        &limits(),
    )
    .await
    .expect_err("a verified physical row must never be rebound to another node");

    assert!(matches!(error, AppError::Internal(_)));
    assert_eq!(counts(&db).await, before);
    assert!(
        object::Entity::find()
            .filter(object::Column::Bucket.eq("bucket"))
            .filter(object::Column::Key.eq("destination"))
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn matching_hot_receipt_is_attached_to_the_published_version() {
    let db = setup().await;
    let publication = request(
        object("destination-object", "destination", CID, 7),
        vec![],
        vec![],
    );

    publish_standard_object_with_hot_receipt(
        &db,
        publication,
        guard(&db, "destination").await,
        LocalResidencyVerificationReceipt {
            node_identity: "destination-hot-node".to_owned(),
            cid: CID.to_owned(),
        },
        &limits(),
    )
    .await
    .unwrap();

    let version = object_version::Entity::find()
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let resolved = crate::store::residency::resolve_version_residency(&db, &version.id)
        .await
        .unwrap();
    assert_eq!(
        resolved.physical.verification_state,
        VerificationState::Verified
    );
    assert_eq!(
        resolved.physical.node_identity.as_deref(),
        Some("destination-hot-node")
    );
}

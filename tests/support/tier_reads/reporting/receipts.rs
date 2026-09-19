use sea_orm::{ColumnTrait, QueryFilter};

use super::*;

#[tokio::test]
async fn signed_lists_reject_malformed_cold_receipt_like_get_and_head() {
    assert_rejected_receipt("not-json".to_owned()).await;
}

#[tokio::test]
async fn signed_lists_reject_cold_receipt_cid_mismatch_like_get_and_head() {
    assert_rejected_receipt(
        serde_json::to_string(&LocalResidencyVerificationReceipt {
            node_identity: COLD_NODE_ID.to_owned(),
            cid: HOT_VERSION_CID.to_owned(),
        })
        .unwrap(),
    )
    .await;
}

#[tokio::test]
async fn signed_lists_reject_cold_receipt_node_mismatch_like_get_and_head() {
    assert_rejected_receipt(
        serde_json::to_string(&LocalResidencyVerificationReceipt {
            node_identity: "another-node".to_owned(),
            cid: COLD_VERSION_CID.to_owned(),
        })
        .unwrap(),
    )
    .await;
}

async fn assert_rejected_receipt(receipt: String) {
    let harness = start_reporting_harness(HashMap::new()).await;
    let db = harness.state.store.db();
    store::bucket::set_versioning_state(db, BUCKET, BucketVersioningState::Enabled)
        .await
        .unwrap();
    let now = Utc::now();
    seed_physical(db, "cold", COLD_VERSION_CID, true, now).await;
    let seeded = seed_content_version(
        db,
        "receipt.bin",
        COLD_VERSION_CID,
        Some(SHARED_OLD_VERSION),
        1,
        true,
        "cold",
        "STANDARD_IA",
        10,
        now,
        None,
        EncryptionEnvelope::plain(),
    )
    .await;
    physical_residency::Entity::update_many()
        .col_expr(
            physical_residency::Column::VerificationReceipt,
            receipt.clone().into(),
        )
        .filter(physical_residency::Column::Tier.eq("cold"))
        .filter(physical_residency::Column::Cid.eq(COLD_VERSION_CID))
        .exec(db)
        .await
        .unwrap();
    let before = immutable_snapshot(db, &seeded).await;

    for method in [reqwest::Method::GET, reqwest::Method::HEAD] {
        let response = signed_request(
            &harness.endpoint,
            method,
            "receipt.bin",
            Some(SHARED_OLD_VERSION),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(response.headers().get("x-amz-storage-class").is_none());
    }
    for query in [vec![], vec![("list-type", "2")], vec![("versions", "")]] {
        let response = signed_bucket_get(&harness.endpoint, &query).await;
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(
            status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "{query:?}: {body}"
        );
        assert!(body.contains("<Code>InternalError</Code>"), "{body}");
        assert!(!body.contains("<StorageClass>"));
        assert!(!body.contains(&receipt));
        assert!(!body.contains(COLD_NODE_ID));
    }
    assert_eq!(immutable_snapshot(db, &seeded).await, before);
    assert!(
        harness._kubo.received_requests().await.unwrap().is_empty(),
        "invalid metadata must fail without Kubo/health IO"
    );
    harness.server.shutdown().await;
}

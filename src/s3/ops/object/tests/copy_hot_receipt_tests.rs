use super::*;
use crate::store::entities::{object, object_version, residency_reference, version_residency};
use sea_orm::{ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const HOT_NODE: &str = "QmPChd2hVbrJ6i1a7aDPgS6G9X4YuJ5sS7cGqf6ZkK3vYq";

async fn mount_local_verification(kubo: &MockServer, cid: &str) {
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .and(query_param("arg", cid))
        .and(query_param("type", "recursive"))
        .and(query_param("offline", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!(r#"{{"Keys":{{"{cid}":{{"Type":"recursive"}}}}}}"#)),
        )
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{cid}")))
        .and(query_param("with-local", "true"))
        .and(query_param("offline", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"Hash":"{cid}","WithLocality":true,"Local":true}}"#
        )))
        .mount(kubo)
        .await;
}

async fn mount_successful_transport(cold: &MockServer, hot: &MockServer) {
    mount_local_verification(cold, CID).await;
    mount_local_verification(hot, CID).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/export"))
        .and(query_param("arg", CID))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"car"))
        .mount(cold)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/import"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Root\":{{\"Cid\":{{\"/\":\"{CID}\"}},\"PinErrorMsg\":\"\"}}}}\n{{\"Stats\":{{\"BlockCount\":1}}}}\n"
        )))
        .mount(hot)
        .await;
}

async fn cold_source_state() -> (Arc<AppState>, MockServer, MockServer) {
    let hot = MockServer::start().await;
    let cold = MockServer::start().await;
    mount_node_identity(&cold, COLD_NODE_ID).await;
    mount_node_identity(&hot, HOT_NODE).await;
    let state = pinning_state_with_cold(hot.uri(), Some(cold.uri()), "request", "one", "").await;
    publish_versioned_read_object(
        &state,
        "source-object",
        "source.bin",
        CID,
        false,
        None,
        None,
        Vec::new(),
    )
    .await;
    move_version_to_verified_cold(&state, "source.bin", None, COLD_NODE_ID).await;
    (state, hot, cold)
}

async fn bind_existing_hot(state: &Arc<AppState>, node: &str) {
    use sea_orm::sea_query::Expr;

    let now = Utc::now();
    let receipt = serde_json::to_string(&crate::kubo::LocalResidencyVerificationReceipt {
        node_identity: node.to_owned(),
        cid: CID.to_owned(),
    })
    .unwrap();
    crate::store::entities::physical_residency::Entity::update_many()
        .col_expr(
            crate::store::entities::physical_residency::Column::VerificationState,
            Expr::value("verified"),
        )
        .col_expr(
            crate::store::entities::physical_residency::Column::NodeIdentity,
            Expr::value(Some(node.to_owned())),
        )
        .col_expr(
            crate::store::entities::physical_residency::Column::VerificationReceipt,
            Expr::value(Some(receipt)),
        )
        .col_expr(
            crate::store::entities::physical_residency::Column::VerifiedAt,
            Expr::value(Some(now)),
        )
        .col_expr(
            crate::store::entities::physical_residency::Column::UpdatedAt,
            Expr::value(now),
        )
        .filter(crate::store::entities::physical_residency::Column::Tier.eq("hot"))
        .filter(crate::store::entities::physical_residency::Column::Cid.eq(CID))
        .exec(state.store.db())
        .await
        .unwrap();
}

async fn publication_counts(state: &Arc<AppState>) -> (u64, u64, u64, u64) {
    let db = state.store.db();
    (
        object::Entity::find().count(db).await.unwrap(),
        object_version::Entity::find().count(db).await.unwrap(),
        version_residency::Entity::find().count(db).await.unwrap(),
        residency_reference::Entity::find().count(db).await.unwrap(),
    )
}

async fn assert_failed_copy_left_only_source(state: &Arc<AppState>, before: (u64, u64, u64, u64)) {
    assert_eq!(publication_counts(state).await, before);
    assert_eq!(
        crate::store::object::get_latest(state.store.db(), "bucket", "source.bin")
            .await
            .unwrap()
            .id,
        "source-object"
    );
    assert!(
        object::Entity::find()
            .filter(object::Column::Bucket.eq("bucket"))
            .filter(object::Column::Key.eq("destination.bin"))
            .one(state.store.db())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cold_copy_matching_hot_binding_publishes_verified_destination_that_get_reads_from_hot() {
    let (state, hot, cold) = cold_source_state().await;
    bind_existing_hot(&state, HOT_NODE).await;
    mount_successful_transport(&cold, &hot).await;

    copy_object(
        &state,
        copy_request_version(
            "source.bin",
            None,
            "destination.bin",
            http::HeaderMap::new(),
        ),
    )
    .await
    .expect("matching hot receipt publication");

    let destination_version = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("bucket"))
        .filter(object_version::Column::Key.eq("destination.bin"))
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    let residency = crate::store::residency::resolve_version_residency(
        state.store.db(),
        &destination_version.id,
    )
    .await
    .unwrap();
    assert_eq!(residency.physical.node_identity.as_deref(), Some(HOT_NODE));
    assert_eq!(
        residency.physical.verification_state,
        crate::residency::VerificationState::Verified
    );

    mount_cat_body(&hot, CID, b"body".to_vec()).await;
    let get = get_object(
        &state,
        get_object_request("destination.bin", None, http::HeaderMap::new()),
    )
    .await
    .expect("GET copied destination");
    assert_eq!(read_get_body(get).await, b"body");
}

#[tokio::test]
async fn cold_copy_rejects_foreign_verified_hot_binding_without_publication_rows() {
    let (state, hot, cold) = cold_source_state().await;
    bind_existing_hot(&state, "foreign-hot-node").await;
    mount_successful_transport(&cold, &hot).await;
    let before = publication_counts(&state).await;

    let error = copy_object(
        &state,
        copy_request_version(
            "source.bin",
            None,
            "destination.bin",
            http::HeaderMap::new(),
        ),
    )
    .await
    .expect_err("foreign hot binding must fence publication");

    assert_eq!(error.code().as_str(), "InternalError");
    assert_failed_copy_left_only_source(&state, before).await;
}

#[tokio::test]
async fn cold_copy_transport_failures_leave_no_destination_publication_rows() {
    for failure in ["export", "import", "pin"] {
        let (state, hot, cold) = cold_source_state().await;
        mount_local_verification(&cold, CID).await;
        let before = publication_counts(&state).await;

        Mock::given(method("POST"))
            .and(path("/api/v0/dag/export"))
            .respond_with(if failure == "export" {
                ResponseTemplate::new(503)
            } else {
                ResponseTemplate::new(200).set_body_bytes(b"car")
            })
            .mount(&cold)
            .await;
        if failure != "export" {
            Mock::given(method("POST"))
                .and(path("/api/v0/dag/import"))
                .respond_with(if failure == "import" {
                    ResponseTemplate::new(503)
                } else {
                    ResponseTemplate::new(200).set_body_string(format!(
                        "{{\"Root\":{{\"Cid\":{{\"/\":\"{CID}\"}},\"PinErrorMsg\":\"\"}}}}\n{{\"Stats\":{{\"BlockCount\":1}}}}\n"
                    ))
                })
                .mount(&hot)
                .await;
        }
        if failure == "pin" {
            Mock::given(method("POST"))
                .and(path("/api/v0/pin/ls"))
                .respond_with(ResponseTemplate::new(503))
                .mount(&hot)
                .await;
        }

        let error = copy_object(
            &state,
            copy_request_version(
                "source.bin",
                None,
                "destination.bin",
                http::HeaderMap::new(),
            ),
        )
        .await
        .expect_err("failed transport primitive must abort CopyObject");
        assert_eq!(error.code().as_str(), "InternalError", "failure={failure}");
        assert_failed_copy_left_only_source(&state, before).await;
    }
}

#[path = "support/management_wire_v3.rs"]
mod archive;
#[path = "support/stage7_management.rs"]
pub mod support;

use ipfs_s3_gateway::{
    pinning::management::{self as management, db_snapshot, evidence, wire, *},
    store::{
        self,
        pinning::management::{
            actions, native, records,
            transaction::{FINAL_WRITE_LIMIT, ManagementTransaction},
        },
    },
};
use sea_orm::ConnectionTrait;
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

#[tokio::test]
async fn file_confirmation_uses_trusted_original_and_applied_receipt_not_client_locator() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":support::NODE})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .and(query_param("offline", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
        .mount(&server)
        .await;
    let raw = support::config(&server.uri());
    let config = support::management_config(&raw);
    let (_directory, db) = support::database().await;
    support::setup(&db, &config).await;
    let identity = support::publish(&db, &config, "exact", false).await;
    db.execute_unprepared(
        "UPDATE objects SET metadata='{\"private\":\"DO_NOT_EXPORT_NATIVE_METADATA\"}'",
    )
    .await
    .unwrap();
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    evidence::prepare(
        &db,
        &config,
        &auth,
        &ipfs_s3_gateway::kubo::KuboClient::new(server.uri()),
        &evidence::PrepareRequest {
            record_id: "proof".into(),
            identity: identity.clone(),
            expected_node: support::NODE.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(5),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let request = support::request(identity, &config, true);
    let snapshot = db_snapshot::collect_with_config(&db, &config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Candidate);
    let acknowledgement = plan.confirmation(vec!["object".into()]);
    assert!(
        records::load_confirmed_authority(&db, &auth, &request, Default::default())
            .await
            .is_err()
    );

    let binding = records::confirm(
        &db,
        &config,
        &auth,
        (&request, &snapshot, &plan),
        acknowledgement.candidate_item_ids,
        chrono::Duration::minutes(2),
    )
    .await
    .unwrap();
    let bytes = archive::export(&binding);
    assert!(!String::from_utf8_lossy(&bytes).contains("DO_NOT_EXPORT_NATIVE_METADATA"));
    assert!(!String::from_utf8_lossy(&bytes).contains(&server.uri()));
    let mut private = false;
    binding
        .review()
        .visit_original_cells(|table, _, column, value| {
            private |= table == "objects" && column == "metadata" && value.is_some();
        });
    assert!(private);
    let resolved = archive::restore(&db, &auth, &request, &bytes)
        .await
        .unwrap();
    assert!(resolved == binding);
    let mut tx = ManagementTransaction::begin(&db, FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    tx.run_with(
        (&auth, &binding, &config),
        |tx, _, (checked_auth, checked_binding, checked_config)| {
            Box::pin(async move {
                records::revalidate_confirmed_in_transaction(
                    tx,
                    checked_config,
                    checked_auth,
                    checked_binding,
                )
                .await?;
                Ok(())
            })
        },
    )
    .await
    .unwrap();
    let applied = actions::apply_in_transaction(&mut tx, &config, &auth, &binding)
        .await
        .unwrap();
    let receipt = applied.items[0].receipt.clone();
    tx.commit().await.unwrap();
    let stored = records::lookup_applied(&db, &auth, &binding, "object")
        .await
        .unwrap()
        .unwrap();
    assert!(stored == receipt);
    assert!(
        records::lookup_applied(&db, &auth, &resolved, "object")
            .await
            .unwrap()
            .unwrap()
            == receipt
    );
    let forged = archive::rewrite(&bytes, |frame| {
        if let wire::v3::Frame::Acknowledgements { item_ids } = frame {
            *item_ids = Some(vec![]);
        }
    });
    assert!(matches!(forged, Err(SafeError::ConfirmationMismatch)));

    let (_other_directory, other) = support::database().await;
    support::setup(&other, &config).await;
    other
        .execute_unprepared(&format!(
            "UPDATE pin_management_records SET database_scope='{}' WHERE kind='database_scope'",
            auth.database_scope()
        ))
        .await
        .unwrap();
    assert!(auth.check(&other).await.is_err());
    let memory = store::connect_database("sqlite::memory:").await.unwrap();
    support::setup(&memory, &config).await;
    assert!(records::authorize_local(&raw, &memory).await.is_err());
    assert!(native::capture(&memory).await.is_ok());
    let _ = management::MAX_RECORDS;
}

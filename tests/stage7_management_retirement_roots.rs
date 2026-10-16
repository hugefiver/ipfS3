#[path = "support/stage7_management_actions.rs"]
pub mod support;
use ipfs_s3_gateway::{
    pinning::management::*,
    store::{
        entities::zip_root_reference,
        pinning::management::{actions, native, records},
        zip::{self, BatchAdmission, ManifestItem, RootOutcome, VersionBinding},
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, Statement, TransactionTrait};

async fn root(fixture: &support::Fixture, batch: &str, owner: &str) -> native::NativeKey {
    let object = fixture.publish(&format!("out/leaf-{batch}"), false).await;
    let archive = format!("{batch}.zip");
    let controls = ipfs_s3_gateway::zip::options::parse_authenticated_zip_controls(
        &http::HeaderMap::new(),
        &[("decompress-zip".into(), "out/".into())],
        &[],
        true,
        true,
    )
    .unwrap()
    .unwrap();
    let capture = ipfs_s3_gateway::pinning::zip_policy::capture_zip_intent(
        &fixture.config.pinning,
        &Default::default(),
        ipfs_s3_gateway::config::OptionalPinControlMode::Warn,
        ipfs_s3_gateway::pinning::zip_policy::ZipCaptureContext {
            bucket: "bucket",
            source_key: &archive,
            target_prefix: "out/",
            tags: &[],
        },
        &controls,
        ipfs_s3_gateway::pinning::decision::DecisionOrigin::new(owner, batch),
    )
    .unwrap();
    zip::admit(
        &fixture.db,
        &BatchAdmission {
            id: batch.into(),
            owner: owner.into(),
            source: "direct".into(),
            token: capture.controls().admission_token.clone(),
            fingerprint: batch.into(),
            bucket: "bucket".into(),
            archive_key: archive,
            input_identity: format!("input-{batch}"),
            captured_options: serde_json::json!({"zip_intent":capture}).to_string(),
        },
    )
    .await
    .unwrap();
    zip::prepare_manifest(
        &fixture.db,
        batch,
        &[ManifestItem::Success {
            path: "leaf".into(),
            object_key: object.key,
            cid: support::CID.into(),
            size: 10,
        }],
    )
    .await
    .unwrap();
    let claim = zip::claim_root(&fixture.db, batch, "root-fixture", 3600)
        .await
        .unwrap();
    zip::mark_invoked(&fixture.db, &claim).await.unwrap();
    zip::retain_candidate(&fixture.db, &claim, support::NODE, "hot", support::CID)
        .await
        .unwrap();
    zip::verify_root(
        &fixture.db,
        &claim,
        support::NODE,
        "hot",
        support::CID,
        "verified-fixture-receipt",
    )
    .await
    .unwrap();
    let tx = fixture.db.begin().await.unwrap();
    zip::publish(
        &tx,
        batch,
        &[VersionBinding {
            path: "leaf".into(),
            version_row_id: object.private_version_id,
        }],
        false,
        "{\"root_status\":\"complete\"}",
        RootOutcome::Verified {
            claim: claim.clone(),
            node_identity: support::NODE.into(),
            tier: "hot".into(),
            cid: support::CID.into(),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    native::NativeKey {
        table: "zip_root_references".into(),
        values: vec![
            batch.into(),
            claim.revision.to_string(),
            claim.epoch.to_string(),
            support::NODE.into(),
            "hot".into(),
            support::CID.into(),
        ],
    }
}

fn request(
    fixture: &support::Fixture,
    plan: &str,
    references: Vec<native::NativeKey>,
) -> PlanRequest {
    PlanRequest {
        plan_id: plan.into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            action: Action::RetireWith {
                selection: RetirementSelection::ReleaseRootReferences { references },
            },
            subject: Subject::Provider {
                route: RouteIdentity::from_original(
                    fixture.config.pinning.providers[0].route_snapshot(),
                ),
            },
        }],
    }
}

#[tokio::test]
async fn exact_root_release_preserves_shared_history_and_replays_without_business_writes() {
    let fixture = support::Fixture::new().await;
    let selected = root(&fixture, "selected", "owner-a").await;
    let shared = root(&fixture, "shared", "owner-b").await;
    let old = zip::snapshot(&fixture.db, "selected")
        .await
        .unwrap()
        .unwrap();
    let other = zip::snapshot(&fixture.db, "shared").await.unwrap().unwrap();
    let binding = fixture
        .confirm(&request(&fixture, "root-release", vec![selected.clone()]))
        .await;
    let result = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let after = zip::snapshot(&fixture.db, "selected")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.batch, old.batch);
    assert_eq!(after.builds, old.builds);
    assert_eq!(after.entries, old.entries);
    let mut released = after.references[0].clone();
    assert!(released.logical_released_at.take().is_some());
    assert_eq!(released, old.references[0]);
    assert_eq!(
        zip::snapshot(&fixture.db, "shared")
            .await
            .unwrap()
            .unwrap()
            .references,
        other.references
    );
    assert!(result.items[0].root_releases == vec![selected.clone()]);
    assert!(!result.items[0].root_releases.contains(&shared));
    assert_eq!(
        actions::status::lookup(&fixture.db, &fixture.auth, &binding, "provider")
            .await
            .unwrap()
            .unwrap()
            .state,
        records::ActionState::NeedsAttention
    );
    let before_replay = native::capture(&fixture.db).await.unwrap();
    let replay = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert!(replay.items == result.items);
    assert!(native::capture(&fixture.db).await.unwrap() == before_replay);
    assert_eq!(
        records::read_retirement(
            &fixture.db,
            &fixture.auth,
            &fixture.config.pinning.providers[0].name
        )
        .await
        .unwrap()
        .unwrap()
        .disposition,
        records::Disposition::Retain
    );
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/api/v0/pin/rm")
            .count(),
        0
    );
}

#[tokio::test]
async fn root_marker_cas_fault_rolls_back_the_retirement_and_receipt() {
    let fixture = support::Fixture::new().await;
    let selected = root(&fixture, "rollback", "owner").await;
    let before = zip_root_reference::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let binding = fixture
        .confirm(&request(&fixture, "root-fault", vec![selected]))
        .await;
    fixture.db.execute_unprepared("CREATE TRIGGER root_marker_fault BEFORE UPDATE OF logical_released_at ON zip_root_references BEGIN SELECT RAISE(ABORT,'marker fault'); END").await.unwrap();
    assert!(
        actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
            .await
            .is_err()
    );
    assert_eq!(
        zip_root_reference::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap(),
        before
    );
    assert!(
        records::lookup_committed(&fixture.db, &fixture.auth, &binding, "provider")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        records::read_retirement(
            &fixture.db,
            &fixture.auth,
            &fixture.config.pinning.providers[0].name
        )
        .await
        .unwrap()
        .is_none()
    );
    fixture
        .db
        .execute_unprepared("DROP TRIGGER root_marker_fault")
        .await
        .unwrap();
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert_eq!(
        actions::status::lookup(&fixture.db, &fixture.auth, &binding, "provider")
            .await
            .unwrap()
            .unwrap()
            .state,
        records::ActionState::Accepted
    );
}

#[tokio::test]
async fn confirmed_root_release_rejects_owner_or_private_receipt_drift() {
    for owner in [true, false] {
        let fixture = support::Fixture::new().await;
        let selected = root(&fixture, "drift", "owner").await;
        let binding = fixture
            .confirm(&request(&fixture, "root-drift", vec![selected]))
            .await;
        let sql = if owner {
            "UPDATE zip_batches SET owner=$1 WHERE id='drift'"
        } else {
            "UPDATE zip_root_references SET verification_receipt=$1 WHERE batch_id='drift'"
        };
        fixture
            .db
            .execute(Statement::from_sql_and_values(
                fixture.db.get_database_backend(),
                sql,
                ["changed".into()],
            ))
            .await
            .unwrap();
        assert!(
            actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
                .await
                .is_err()
        );
        assert!(
            zip_root_reference::Entity::find()
                .all(&fixture.db)
                .await
                .unwrap()
                .iter()
                .all(|reference| reference.logical_released_at.is_none())
        );
    }
}

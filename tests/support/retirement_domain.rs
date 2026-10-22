#[path = "stage7_management_actions.rs"]
pub mod existing;

use existing::Fixture;
use ipfs_s3_gateway::{
    pinning::management::*,
    store::{
        pinning::management::{ManagementError, native, records},
        zip::{self, BatchAdmission, ManifestItem, RootOutcome, VersionBinding},
    },
};
use sea_orm::{ConnectionTrait, Statement, TransactionTrait};
use std::time::Instant;

/// File-backed current/null versions and noncurrent history, outside the selected
/// resource family. Construction finishes before any retirement measurement.
pub async fn large_background(fixture: &Fixture) {
    const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    for start in (1..=100_000).step_by(1000) {
        let last = start + 999;
        let numbers = format!(
            "WITH RECURSIVE n(i) AS (SELECT {start} UNION ALL SELECT i+1 FROM n WHERE i<{last})"
        );
        let tx = fixture.db.begin().await.unwrap();
        for sql in [
            format!(
                "{numbers} INSERT INTO objects(id,bucket,key,cid,size,etag,encrypted,multipart,is_latest,metadata,created_at) SELECT 'retirement-background-'||i,'bucket','background/'||i,$1,10,$1,FALSE,FALSE,TRUE,'{{\"private\":\"背景原文\"}}',CURRENT_TIMESTAMP FROM n"
            ),
            format!(
                "{numbers} INSERT INTO object_versions(id,bucket,key,version_id,kind,object_id,sequence,is_latest,lifecycle_age_started_at,created_at,updated_at) SELECT 'retirement-background-version-'||i,'bucket','background/'||i,NULL,'object','retirement-background-'||i,1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM n WHERE $1 IS NOT NULL"
            ),
            format!(
                "{numbers} INSERT INTO objects(id,bucket,key,cid,size,etag,encrypted,multipart,is_latest,created_at) SELECT 'retirement-background-old-'||i,'bucket','background/'||i,$1,10,$1,FALSE,FALSE,FALSE,CURRENT_TIMESTAMP FROM n WHERE i%10=0"
            ),
            format!(
                "{numbers} INSERT INTO object_versions(id,bucket,key,version_id,kind,object_id,sequence,is_latest,lifecycle_age_started_at,created_at,updated_at) SELECT 'retirement-background-old-version-'||i,'bucket','background/'||i,'old-'||i,'object','retirement-background-old-'||i,0,FALSE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM n WHERE i%10=0 AND $1 IS NOT NULL"
            ),
        ] {
            tx.execute(Statement::from_sql_and_values(
                tx.get_database_backend(),
                sql,
                [CID.into()],
            ))
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
    }
    let count = fixture.db.query_one(Statement::from_string(
        fixture.db.get_database_backend(),
        "SELECT COUNT(*) AS n FROM object_versions WHERE is_latest=TRUE AND kind='object' AND version_id IS NULL AND id LIKE 'retirement-background-version-%'",
    )).await.unwrap().unwrap().try_get::<i64>("", "n").unwrap();
    assert_eq!(count, 100_000);
}

pub fn request(fixture: &Fixture, name: &str, selection: RetirementSelection) -> PlanRequest {
    PlanRequest {
        plan_id: name.into(),
        items: vec![ItemRequest {
            item_id: "retire".into(),
            action: Action::RetireWith { selection },
            subject: Subject::Provider {
                route: RouteIdentity::from_original(
                    fixture
                        .config
                        .pinning
                        .providers
                        .iter()
                        .find(|provider| provider.identity.provider_id == "fixture-provider")
                        .unwrap()
                        .route_snapshot(),
                ),
            },
        }],
    }
}

pub async fn confirm(fixture: &Fixture, request: &PlanRequest) -> records::ConfirmedBinding {
    let started = Instant::now();
    let local = db_snapshot::collect_with_config(&fixture.db, &fixture.config, request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let mut checkpoint = retirement::PlanningCheckpoint::create(
        &fixture.directory.path().join(&request.plan_id),
        &fixture.db,
        fixture.auth.principal(),
        request,
        &local,
        &retirement::RunLimits::default(),
    )
    .await
    .unwrap();
    let prepared = loop {
        match checkpoint
            .advance(&fixture.db, &retirement::RunLimits::default())
            .await
        {
            Ok(retirement::PlanningProgress::Incomplete) => {}
            Ok(retirement::PlanningProgress::Complete(prepared)) => break prepared,
            Err(error) => panic!(
                "{} planning failed with original limits after {:?}: {error:?}",
                request.plan_id,
                started.elapsed()
            ),
        }
    };
    eprintln!(
        "{} planning elapsed={:?} work={:?}",
        request.plan_id,
        started.elapsed(),
        prepared.task_work().await.unwrap()
    );
    assert!(
        prepared
            .plan
            .items()
            .iter()
            .all(|item| matches!(item.status, PlanStatus::Ready | PlanStatus::Candidate))
    );
    let acknowledgements: Vec<_> = prepared
        .plan
        .items()
        .iter()
        .filter(|item| item.status == PlanStatus::Candidate)
        .map(|item| item.request.item_id.clone())
        .collect();
    let confirmation_started = Instant::now();
    let result = loop {
        let advanced = prepared
            .advance_confirmation(
                &fixture.db,
                &fixture.config,
                &fixture.auth,
                request,
                &local,
                &retirement::RunLimits::default(),
            )
            .await;
        match advanced.progress {
            Ok(retirement::ConfirmationProgress::Incomplete) => {}
            Ok(retirement::ConfirmationProgress::Ready(ready)) => {
                let confirmed = records::confirm_ready(
                    &fixture.db,
                    &fixture.config,
                    &fixture.auth,
                    records::ReadyPublication {
                        ready: &ready,
                        acknowledgements: &acknowledgements,
                        ttl: chrono::Duration::minutes(5),
                    },
                    Default::default(),
                    &retirement::RunLimits::default(),
                )
                .await;
                break confirmed.progress.and_then(|progress| match progress {
                    retirement::ConfirmationProgress::Confirmed(binding) => Ok(*binding),
                    _ => Err(ManagementError::Confirmation),
                });
            }
            Ok(retirement::ConfirmationProgress::Confirmed(binding)) => break Ok(*binding),
            Err(error) => break Err(error),
        }
    };
    eprintln!(
        "{} confirmation elapsed={:?} planning_and_confirmation_elapsed={:?}",
        request.plan_id,
        confirmation_started.elapsed(),
        started.elapsed()
    );
    prepared.cleanup().await.unwrap();
    result.unwrap_or_else(|error| {
        panic!(
            "{} confirmation failed with original limits: {error:?}",
            request.plan_id
        )
    })
}

/// A verified root and its committed manifest, built using the ZIP store API.
pub async fn root(fixture: &Fixture, batch: &str) -> native::NativeKey {
    let object =
        existing::base::publish(&fixture.db, &fixture.config, &format!("out/{batch}"), false).await;
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
        ipfs_s3_gateway::pinning::decision::DecisionOrigin::new(batch, batch),
    )
    .unwrap();
    zip::admit(
        &fixture.db,
        &BatchAdmission {
            id: batch.into(),
            owner: batch.into(),
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
            cid: existing::CID.into(),
            size: 10,
        }],
    )
    .await
    .unwrap();
    let claim = zip::claim_root(&fixture.db, batch, "retirement-fixture", 3600)
        .await
        .unwrap();
    zip::mark_invoked(&fixture.db, &claim).await.unwrap();
    zip::retain_candidate(&fixture.db, &claim, existing::NODE, "hot", existing::CID)
        .await
        .unwrap();
    zip::verify_root(
        &fixture.db,
        &claim,
        existing::NODE,
        "hot",
        existing::CID,
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
            node_identity: existing::NODE.into(),
            tier: "hot".into(),
            cid: existing::CID.into(),
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
            existing::NODE.into(),
            "hot".into(),
            existing::CID.into(),
        ],
    }
}

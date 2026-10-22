//! Real PostgreSQL confirmation staging in one fresh, parent-owned schema.
#[path = "support/stage7_management.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use ipfs_s3_gateway::{
    kubo::KuboClient,
    pinning::management::{
        Action, ItemRequest, PlanRequest, PlanStatus, RetirementSelection, RouteIdentity, Snapshot,
        Subject, db_snapshot, evidence,
        local_config::ManagementConfig,
        retirement::{
            ArchiveCursor, ArchivePart, ConfirmationPhase, ConfirmationProgress, InventorySpool,
            PlanningCheckpoint, PlanningProgress, PreparedPlanning, RunLimits,
        },
    },
    store::pinning::{
        ledger,
        management::{ManagementError, native, records},
    },
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
};
use std::{path::Path, time::Duration};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn limits(pages: u64) -> RunLimits {
    let mut limits = RunLimits::default();
    limits.pages = pages;
    limits
}

async fn snapshot(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    request: &PlanRequest,
) -> Snapshot {
    db_snapshot::collect_with_config(db, config, request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap()
}

async fn prepare(
    mut task: PlanningCheckpoint,
    db: &DatabaseConnection,
) -> (Snapshot, PreparedPlanning) {
    let local = task.snapshot().clone();
    loop {
        if let PlanningProgress::Complete(prepared) = task.advance(db, &limits(512)).await.unwrap()
        {
            return (local, *prepared);
        }
    }
}

async fn originals(checkpoint: &Path) -> Vec<ArchivePart> {
    // SQLite is the product's actual local spool, not a replacement for the PG authority DB.
    let spool = InventorySpool::resume(&checkpoint.join("source-000.sqlite"))
        .await
        .unwrap();
    let mut cursor = ArchiveCursor::default();
    let mut parts = Vec::new();
    while let Some(part) = spool.read_part(&mut cursor).await.unwrap() {
        parts.push(part);
    }
    spool.close().await.unwrap();
    parts
}

#[derive(Debug, PartialEq, Eq)]
struct Prefix {
    archive_id: String,
    header: String,
    state: String,
    cursor: String,
    bytes: i64,
    chunks: Vec<(i64, Vec<u8>)>,
}

async fn prefix(db: &DatabaseConnection, request: &PlanRequest) -> Prefix {
    let rows = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "SELECT archive_id,header,state,digest,bytes,chunks FROM pin_management_archive WHERE archive_id LIKE 'checkpoint:%' AND header::jsonb #>> '{request,plan_id}'=$1",
        [request.plan_id.clone().into()],
    )).await.unwrap();
    assert_eq!(rows.len(), 1, "one durable confirmation prefix per pass");
    let row = &rows[0];
    let archive_id: String = row.try_get("", "archive_id").unwrap();
    let chunks = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "SELECT ordinal,payload FROM pin_management_archive_chunks WHERE archive_id=$1 ORDER BY ordinal",
        [archive_id.clone().into()],
    )).await.unwrap().into_iter().map(|row| {
        (row.try_get::<i64>("", "ordinal").unwrap(), row.try_get::<Vec<u8>>("", "payload").unwrap())
    }).collect::<Vec<_>>();
    assert_eq!(
        row.try_get::<i64>("", "chunks").unwrap(),
        chunks.len() as i64
    );
    assert_eq!(
        row.try_get::<i64>("", "bytes").unwrap(),
        chunks
            .iter()
            .map(|(_, bytes)| bytes.len() as i64)
            .sum::<i64>(),
    );
    for (ordinal, (stored, _)) in chunks.iter().enumerate() {
        assert_eq!(*stored, ordinal as i64);
    }
    Prefix {
        archive_id,
        header: row.try_get("", "header").unwrap(),
        state: row.try_get("", "state").unwrap(),
        cursor: row.try_get("", "digest").unwrap(),
        bytes: row.try_get("", "bytes").unwrap(),
        chunks,
    }
}

async fn no_authority(
    db: &DatabaseConnection,
    auth: &records::LocalAuthorization,
    request: &PlanRequest,
) {
    let row = db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "SELECT count(*) AS n FROM pin_management_records WHERE database_scope=$1 AND plan_id=$2 AND kind='action'",
        [auth.database_scope().into(), request.plan_id.clone().into()],
    )).await.unwrap().unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
    assert!(
        records::load_confirmed_authority(
            db,
            auth,
            request,
            records::SharedDecodeBudget::default()
        )
        .await
        .is_err(),
        "staging or even a sealed archive alone cannot authorize actions",
    );
}

#[tokio::test]
#[ignore = "requires parent-owned fresh IPFS_S3_STAGE75_CONFIRMATION_DATABASE_URL"]
async fn postgres_confirmation_pages_resume_without_publishing_partial_or_changed_sources() {
    let url = std::env::var("IPFS_S3_STAGE75_CONFIRMATION_DATABASE_URL")
        .expect("parent supplies a fresh exclusive stage7_ schema");
    let mut options = ConnectOptions::new(url.clone());
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    let db = Database::connect(options).await.unwrap();
    assert_eq!(db.get_database_backend(), DatabaseBackend::Postgres);
    let guard = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT current_schema() AS name, NOT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace) AS no_classes, NOT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=current_schema()::regnamespace) AS no_functions",
    )).await.unwrap().unwrap();
    assert!(
        guard
            .try_get::<String>("", "name")
            .unwrap()
            .starts_with("stage7_")
    );
    assert!(
        guard.try_get::<bool>("", "no_classes").unwrap(),
        "preserve occupied schemas"
    );
    assert!(
        guard.try_get::<bool>("", "no_functions").unwrap(),
        "preserve occupied schemas"
    );
    let exclusive = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT pg_try_advisory_lock(hashtextextended(current_database() || ':' || current_schema(), 0)) AS held",
    )).await.unwrap().unwrap().try_get::<bool>("", "held").unwrap();
    assert!(
        exclusive,
        "require exclusive ownership before migrations or fixture writes"
    );

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
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
        .mount(&server)
        .await;
    let mut raw = support::config(&server.uri());
    raw.storage.database_url = url;
    // A backfill needs a real, nonempty ACK. Retire a separate allocation domain.
    let mut retired = raw.pinning.providers[0].clone();
    retired.name = "retired".into();
    raw.pinning.providers.push(retired);
    let mut retired = raw.pinning_identity.providers[0].clone();
    retired.config_name = "retired".into();
    retired.provider_id = "retired-provider".into();
    retired.scope = "retired-account".into();
    retired.storage_domain = "retired-storage".into();
    raw.pinning_identity.providers.push(retired);
    let config = support::management_config(&raw);
    support::setup(&db, &config).await;
    let retired = config
        .pinning
        .providers
        .iter()
        .find(|provider| provider.identity.provider_id == "retired-provider")
        .unwrap();
    ledger::register_route(&db, &retired.name, &retired.identity)
        .await
        .unwrap();
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    let physical = native::scope::binding(&db).await.unwrap();
    assert_eq!(auth.database_identity(), physical.1);
    assert!(!auth.database_identity().starts_with("diagnostic:"));
    let object = support::publish(&db, &config, "confirmation-original", false).await;
    support::write_current_decision(&db, &config, &object).await;
    let metadata = serde_json::json!({"private":"PG_CONFIRMATION_ORIGINAL"}).to_string();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE objects SET metadata=$1::jsonb WHERE id=$2",
        [metadata.clone().into(), object.object_id.clone().into()],
    ))
    .await
    .unwrap();
    let metadata: String = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT metadata::text AS original FROM objects WHERE id=$1",
            [object.object_id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "original")
        .unwrap();
    evidence::prepare(
        &db,
        &config,
        &auth,
        &KuboClient::new(server.uri()),
        &evidence::PrepareRequest {
            record_id: "pg-confirmation-evidence".into(),
            identity: object.clone(),
            expected_node: support::NODE.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(15),
            directory: false,
        },
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let retire = ItemRequest {
        item_id: "retire".into(),
        subject: Subject::Provider {
            route: RouteIdentity::from_original(retired.route_snapshot()),
        },
        action: Action::RetireWith {
            selection: RetirementSelection::Retain,
        },
    };
    let mut request = support::request(object.clone(), &config, true);
    request.plan_id = "pg-confirmation-complete".into();
    request.items.insert(0, retire.clone());
    let directory = tempfile::tempdir().unwrap();
    let retained = directory.path().join("complete");
    let fresh = snapshot(&db, &config, &request).await;
    let task = PlanningCheckpoint::create(
        &retained,
        &db,
        auth.principal(),
        &request,
        &fresh,
        &limits(512),
    )
    .await
    .unwrap();
    let (local, prepared) = prepare(task, &db).await;
    assert!(
        prepared
            .plan
            .items()
            .iter()
            .all(|entry| !matches!(entry.status, PlanStatus::Blocked(_)))
    );
    let acks = prepared
        .plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(acks, vec!["object".to_owned()]);
    let first = prepared
        .advance_confirmation(&db, &config, &auth, &request, &local, &limits(1))
        .await;
    assert!(!first.effects.commit_unknown);
    assert!(first.effects.database_commits > 0);
    assert!(matches!(
        first.progress.unwrap(),
        ConfirmationProgress::Incomplete
    ));
    let accepted = prefix(&db, &request).await;
    assert_eq!(accepted.state, "staging");
    assert!(!accepted.chunks.is_empty());
    no_authority(&db, &auth, &request).await;

    let cancelled = limits(1);
    cancelled.cancellation.cancel();
    let stopped = prepared
        .advance_confirmation(&db, &config, &auth, &request, &local, &cancelled)
        .await;
    assert!(matches!(stopped.progress, Err(ManagementError::Budget)));
    assert_eq!(stopped.effects.database_commits, 0);
    assert!(!stopped.effects.commit_unknown);
    assert_eq!(prefix(&db, &request).await, accepted);
    no_authority(&db, &auth, &request).await;
    prepared.close().await.unwrap();
    let original = originals(&retained).await;
    assert!(original.iter().any(|part| part.table == "objects"
        && part.key.values == vec![object.object_id.clone()]
        && part.column == "metadata"
        && part.bytes == metadata.as_bytes()));

    let fresh = snapshot(&db, &config, &request).await;
    assert!(
        PlanningCheckpoint::committed(&retained, &db, &auth, &request, &fresh, &limits(512))
            .await
            .unwrap()
            .is_none()
    );
    let task = PlanningCheckpoint::resume(
        &retained,
        &db,
        auth.principal(),
        &request,
        &fresh,
        &limits(512),
    )
    .await
    .unwrap();
    let (local, prepared) = prepare(task, &db).await;
    let ready = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let next = prepared
                .advance_confirmation(&db, &config, &auth, &request, &local, &limits(8))
                .await;
            assert!(!next.effects.commit_unknown);
            match next.progress.unwrap() {
                ConfirmationProgress::Incomplete => no_authority(&db, &auth, &request).await,
                ConfirmationProgress::Ready(ready) => {
                    assert_eq!(next.phase, ConfirmationPhase::Ready);
                    break ready;
                }
                ConfirmationProgress::Confirmed(_) => {
                    panic!("page staging must not publish authority")
                }
            }
        }
    })
    .await
    .expect("resumed confirmation pages must reach Ready");
    let sealed = prefix(&db, &request).await;
    assert_eq!(sealed.state, "sealed");
    assert_eq!(sealed.archive_id, accepted.archive_id);
    assert_eq!(sealed.header, accepted.header);
    assert!(sealed.chunks.len() > accepted.chunks.len());
    assert!(sealed.chunks.starts_with(&accepted.chunks));
    no_authority(&db, &auth, &request).await;
    let cancelled = limits(512);
    cancelled.cancellation.cancel();
    let stopped = records::confirm_ready(
        &db,
        &config,
        &auth,
        records::ReadyPublication {
            ready: &ready,
            acknowledgements: &acks,
            ttl: chrono::Duration::minutes(5),
        },
        records::SharedDecodeBudget::default(),
        &cancelled,
    )
    .await;
    assert!(matches!(stopped.progress, Err(ManagementError::Budget)));
    assert_ne!(stopped.phase, ConfirmationPhase::Confirmed);
    assert_eq!(stopped.effects.database_commits, 0);
    assert!(!stopped.effects.commit_unknown);
    assert_eq!(prefix(&db, &request).await, sealed);
    no_authority(&db, &auth, &request).await;
    let rejected = records::confirm_ready(
        &db,
        &config,
        &auth,
        records::ReadyPublication {
            ready: &ready,
            acknowledgements: &[],
            ttl: chrono::Duration::minutes(5),
        },
        records::SharedDecodeBudget::default(),
        &limits(512),
    )
    .await;
    assert!(matches!(
        rejected.progress,
        Err(ManagementError::Confirmation)
    ));
    assert_ne!(rejected.phase, ConfirmationPhase::Confirmed);
    assert_eq!(rejected.effects.database_commits, 0);
    assert!(!rejected.effects.commit_unknown);
    assert_eq!(prefix(&db, &request).await, sealed);
    no_authority(&db, &auth, &request).await;
    let ttl = chrono::Duration::minutes(5);
    let final_step = records::confirm_ready(
        &db,
        &config,
        &auth,
        records::ReadyPublication {
            ready: &ready,
            acknowledgements: &acks,
            ttl,
        },
        records::SharedDecodeBudget::default(),
        &limits(512),
    )
    .await;
    assert_eq!(final_step.phase, ConfirmationPhase::Confirmed);
    assert_eq!(final_step.effects.database_commits, 1);
    assert!(!final_step.effects.commit_unknown);
    let confirmed = match final_step.progress.unwrap() {
        ConfirmationProgress::Confirmed(confirmed) => confirmed,
        _ => panic!("final transaction must publish the complete authority"),
    };
    assert_eq!(confirmed.expires_at - confirmed.confirmed_at, ttl);
    assert_eq!(
        confirmed.confirmation.candidate_item_ids.as_slice(),
        acks.as_slice()
    );
    assert_eq!(
        confirmed
            .review()
            .original_value("objects", &[&object.object_id], "metadata"),
        Some(metadata.as_str())
    );
    drop(ready);
    prepared.close().await.unwrap();
    let fresh = snapshot(&db, &config, &request).await;
    let recovered =
        PlanningCheckpoint::committed(&retained, &db, &auth, &request, &fresh, &limits(512))
            .await
            .unwrap()
            .unwrap();
    assert!(recovered == *confirmed);
    assert_eq!(recovered.confirmed_at, confirmed.confirmed_at);
    assert_eq!(recovered.expires_at, confirmed.expires_at);
    assert_eq!(
        recovered.confirmation.candidate_item_ids.as_slice(),
        acks.as_slice()
    );
    assert!(originals(&retained).await == original);
    assert_eq!(prefix(&db, &request).await, sealed);

    // A fresh pass shares the schema but never erases the completed pass or its history.
    let drift_request = PlanRequest {
        plan_id: "pg-confirmation-drift".into(),
        items: vec![retire],
    };
    let drift_path = directory.path().join("drift");
    let fresh = snapshot(&db, &config, &drift_request).await;
    let task = PlanningCheckpoint::create(
        &drift_path,
        &db,
        auth.principal(),
        &drift_request,
        &fresh,
        &limits(512),
    )
    .await
    .unwrap();
    let (local, prepared) = prepare(task, &db).await;
    let first = prepared
        .advance_confirmation(&db, &config, &auth, &drift_request, &local, &limits(1))
        .await;
    assert!(matches!(
        first.progress.unwrap(),
        ConfirmationProgress::Incomplete
    ));
    assert!(first.effects.database_commits > 0);
    assert!(!first.effects.commit_unknown);
    let drift_prefix = prefix(&db, &drift_request).await;
    assert_eq!(drift_prefix.state, "staging");
    assert_ne!(drift_prefix.archive_id, sealed.archive_id);
    support::publish(&db, &config, "source-drift", false).await;
    let rejected = prepared
        .advance_confirmation(&db, &config, &auth, &drift_request, &local, &limits(512))
        .await;
    assert!(matches!(rejected.progress, Err(ManagementError::Changed)));
    assert_ne!(rejected.phase, ConfirmationPhase::Confirmed);
    assert_eq!(rejected.effects.database_commits, 0);
    assert!(!rejected.effects.commit_unknown);
    assert_eq!(prefix(&db, &drift_request).await, drift_prefix);
    no_authority(&db, &auth, &drift_request).await;
    prepared.close().await.unwrap();
    assert!(PlanningCheckpoint::input(&drift_path).await.is_ok());
    assert!(originals(&retained).await == original);
    assert_eq!(prefix(&db, &request).await, sealed);
    let preserved = records::load_confirmed_authority(
        &db,
        &auth,
        &request,
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    assert!(preserved == recovered);
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|call| matches!(call.url.path(), "/api/v0/id" | "/api/v0/cat"))
    );
    db.close().await.unwrap();
}

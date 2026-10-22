use super::*;
use crate::pinning::{
    identity::{CleanupMode, ProviderRouteSnapshot},
    management::{
        ConfigurationBinding, RouteIdentity,
        retirement::{CurrentRetirementScan, RunLimits},
    },
};
use sea_orm::{Database, Statement, TransactionTrait};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

fn route() -> ProviderRouteSnapshot {
    ProviderRouteSnapshot {
        provider_id: "selected".into(),
        backend: "noop".into(),
        scope: "account".into(),
        storage_domain: "remote".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: "noop".into(),
        strategy: "cid".into(),
        cleanup: CleanupMode::Retain,
        confirmation: None,
    }
}

fn limits(pages: u64) -> RunLimits {
    let mut limits = RunLimits::default();
    limits.pages = pages;
    limits.elapsed = std::time::Duration::from_secs(120);
    limits
}

fn reserve(path: &std::path::Path) -> std::fs::File {
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).unwrap()
}

async fn database() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO pin_provider_routes VALUES ('selected',$1,'selected',TRUE)",
        [serde_json::to_string(&route()).unwrap().into()],
    ))
    .await
    .unwrap();
    db
}

async fn scan(db: &DatabaseConnection) -> (tempfile::TempDir, CurrentRetirementScan) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("status.sqlite");
    let scan = CurrentRetirementScan::start(
        db,
        RouteIdentity::from_original(route()),
        ConfigurationBinding {
            routes: vec![route()],
            policies: vec![],
            limits: Default::default(),
            zip_rules: None,
            source_node: None,
        },
        &path,
        reserve(&path),
        &limits(512),
    )
    .await
    .unwrap();
    (directory, scan)
}

async fn state(db: &DatabaseConnection) -> records::ActionState {
    let (_directory, mut scan) = scan(db).await;
    let (fence, state) = loop {
        if let Some(result) = scan.prepare_action_status(db, &limits(512)).await.unwrap() {
            break result;
        }
    };
    let tx = db.begin().await.unwrap();
    fence.current_status(&tx, &limits(512)).await.unwrap();
    tx.rollback().await.unwrap();
    state
}

async fn remote(db: &DatabaseConnection) {
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO remote_pins(provider,cid,request_id,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES('selected',$1,'resource',10,'pinned',1,0,CURRENT_TIMESTAMP)",
        [CID.into()])).await.unwrap();
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES('selected',$1,$2,'application_created','confirmed')",
        [CID.into(), serde_json::to_string(&route()).unwrap().into()])).await.unwrap();
}

#[tokio::test]
async fn canonical_aliases_must_all_be_retired_and_unknown_debt_remains_attention() {
    let db = database().await;
    assert_eq!(state(&db).await, records::ActionState::Accepted);
    let mut alias = route();
    alias.provider_id = "alias".into();
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO pin_provider_routes VALUES('alias',$1,'alias',FALSE)",
        [serde_json::to_string(&alias).unwrap().into()],
    ))
    .await
    .unwrap();
    assert_eq!(state(&db).await, records::ActionState::NeedsAttention);
    db.execute_unprepared("UPDATE pin_provider_routes SET retired=TRUE")
        .await
        .unwrap();
    remote(&db).await;
    assert_eq!(state(&db).await, records::ActionState::Accepted);
    db.execute_unprepared("UPDATE remote_pin_ledger SET effect='unknown'")
        .await
        .unwrap();
    assert_eq!(state(&db).await, records::ActionState::NeedsAttention);
    db.execute_unprepared("UPDATE remote_pin_ledger SET effect='confirmed'")
        .await
        .unwrap();
    assert_eq!(state(&db).await, records::ActionState::Accepted);
}

#[tokio::test]
async fn cleanup_jobs_queue_but_large_error_cells_and_unlocked_running_jobs_need_attention() {
    let db = database().await;
    remote(&db).await;
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,submit_phase,state,attempts,next_attempt_at,created_at,updated_at) VALUES('cleanup','unpin','selected',$1,1,NULL,'pending',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [CID.into()])).await.unwrap();
    assert_eq!(state(&db).await, records::ActionState::Queued);
    db.execute_unprepared("UPDATE pin_jobs SET last_error=hex(zeroblob(3145728))")
        .await
        .unwrap();
    assert_eq!(state(&db).await, records::ActionState::NeedsAttention);
    db.execute_unprepared("UPDATE pin_jobs SET last_error=NULL,state='running',locked_until=NULL")
        .await
        .unwrap();
    assert_eq!(state(&db).await, records::ActionState::NeedsAttention);
    db.execute_unprepared("UPDATE pin_jobs SET state='done'")
        .await
        .unwrap();
    assert_eq!(state(&db).await, records::ActionState::Accepted);
}

#[tokio::test]
async fn history_requires_confirmed_absence_and_unowned_roots_remain_responsibility() {
    let db = database().await;
    remote(&db).await;
    let ledger = records::current_model::<crate::store::entities::remote_pin_ledger::Entity, _>(
        &db,
        &["selected", CID],
        records::DecodeWork::shared(&records::SharedDecodeBudget::default()),
    )
    .await
    .unwrap()
    .unwrap();
    let envelope =
        serde_json::json!({"ledger":ledger,"request_id":"old-resource","confirmed_absent":false});
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO pin_resource_history VALUES('selected',$1,0,$2)",
        [CID.into(), envelope.to_string().into()],
    ))
    .await
    .unwrap();
    assert_eq!(state(&db).await, records::ActionState::NeedsAttention);
    let mut absent = envelope;
    absent["confirmed_absent"] = true.into();
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "UPDATE pin_resource_history SET ledger=$1",
        [absent.to_string().into()],
    ))
    .await
    .unwrap();
    assert_eq!(state(&db).await, records::ActionState::Accepted);
    db.execute_unprepared("INSERT INTO zip_batches(id,owner,source,token,fingerprint,bucket,archive_key,input_identity,captured_options,created_at,updated_at) VALUES('unattributed','another-owner','direct','token','fingerprint','bucket','archive','input','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO zip_root_builds(batch_id,revision,epoch,worker,lease_until,status,created_at,updated_at) VALUES('unattributed',1,1,'worker',CURRENT_TIMESTAMP,'verified',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO zip_root_references(batch_id,revision,epoch,node_identity,tier,cid,state,created_at,updated_at) VALUES('unattributed',1,1,'node','hot',$1,'retained',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [CID.into()])).await.unwrap();
    assert_eq!(state(&db).await, records::ActionState::NeedsAttention);
    db.execute_unprepared("UPDATE zip_root_references SET logical_released_at=CURRENT_TIMESTAMP")
        .await
        .unwrap();
    assert_eq!(state(&db).await, records::ActionState::Accepted);
}

#[tokio::test]
async fn partial_scan_is_not_completion_and_prepared_status_rejects_source_drift() {
    let db = database().await;
    let (_directory, mut scan) = scan(&db).await;
    assert!(
        scan.prepare_action_status(&db, &limits(0))
            .await
            .unwrap()
            .is_none()
    );
    let (fence, _) = loop {
        if let Some(result) = scan.prepare_action_status(&db, &limits(512)).await.unwrap() {
            break result;
        }
    };
    db.execute_unprepared("UPDATE pin_provider_routes SET retired=FALSE")
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(matches!(
        fence.current_status(&tx, &limits(512)).await,
        Err(ManagementError::Changed)
    ));
    tx.rollback().await.unwrap();
    assert!(matches!(
        scan.prepare_action_status(&db, &limits(512)).await,
        Err(ManagementError::InvalidEvidence)
    ));
}

async fn retirement_binding(fixture: &super::super::tests::Fixture) -> records::ConfirmedBinding {
    use crate::pinning::management::*;
    let request = PlanRequest {
        plan_id: "retirement-status-original".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(
                    fixture.config.pinning.providers[0].route_snapshot(),
                ),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let prepared = retirement::PreparedPlanning::collect(
        &fixture.db,
        &request,
        &snapshot,
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    let acknowledgements = prepared
        .plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    let binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &prepared.plan),
        acknowledgements,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    prepared.close().await.unwrap();
    binding
}

#[tokio::test]
async fn authentic_lookup_verifies_original_before_scan_and_reports_new_responsibility() {
    let fixture = super::super::tests::fixture("retirement-status-object").await;
    let binding = retirement_binding(&fixture).await;
    let directory = tempfile::tempdir().unwrap();
    let absent_path = directory.path().join("not-applied.sqlite");
    assert!(
        lookup_retirement_with_work(
            &fixture.db,
            &fixture.auth,
            &binding,
            "provider",
            &absent_path,
            reserve(&absent_path),
            &limits(512),
            records::SharedDecodeBudget::default()
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(std::fs::metadata(&absent_path).unwrap().len(), 0);
    let applied = super::super::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    // The in-transaction path does not consult even a disconnected database for
    // retirement responsibility; its result belongs to the executor's commit.
    assert_eq!(
        current_state_with_work(
            &DatabaseConnection::Disconnected,
            &applied.items[0],
            records::SharedDecodeBudget::default()
        )
        .await
        .unwrap(),
        applied.items[0].state
    );
    fixture
        .db
        .execute_unprepared("UPDATE pin_provider_routes SET retired=FALSE")
        .await
        .unwrap();
    let path = directory.path().join("current.sqlite");
    let mut status = lookup_retirement_with_work(
        &fixture.db,
        &fixture.auth,
        &binding,
        "provider",
        &path,
        reserve(&path),
        &limits(512),
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap()
    .unwrap();
    let current = loop {
        if let Some(action) = status.advance(&fixture.db, &limits(512)).await.unwrap() {
            break action;
        }
    };
    assert_eq!(current.state, records::ActionState::NeedsAttention);
    assert!(
        records::lookup_committed(&fixture.db, &fixture.auth, &binding, "provider")
            .await
            .unwrap()
            .as_ref()
            == Some(&applied.items[0])
    );
    assert!(matches!(
        lookup(&fixture.db, &fixture.auth, &binding, "provider").await,
        Err(ManagementError::InvalidEvidence)
    ));
}

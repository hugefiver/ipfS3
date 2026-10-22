//! Shared Submit provenance must remain complete before and after owner cancellation.
use ipfs_s3_gateway::{
    kubo::KuboClient,
    pinning::{
        coordinator::PinningCoordinator,
        management::{Action, local_config},
        worker::ManagementJobScope,
    },
    store::{
        Store,
        entities::{
            pin_invocation_route, pin_job, pin_lease, pin_lease_target, pin_replica_observation,
            pin_submit_observation, remote_pin,
        },
        pinning::{
            jobs, ledger,
            management::{
                actions::{
                    self,
                    job_authority::{self, JobPermit},
                },
                native::{self, NativeKey, meter::Budget},
                records::{
                    self,
                    locator::{self, AuthoritySelector},
                },
            },
        },
    },
};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, EntityTrait,
    QueryFilter, Statement, TransactionTrait,
};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "support/stage7_management_actions.rs"]
mod support;

#[tokio::test]
async fn sqlite_missing_shared_job_locator_is_fail_closed_before_and_after_owner_cancel() {
    shared_authority_completeness(support::Fixture::new().await).await;
}

#[tokio::test]
#[ignore = "requires a parent-owned fresh exclusive schema in IPFS_S3_STAGE7_APPLY_DATABASE_URL"]
async fn postgres_missing_shared_job_locator_is_fail_closed_before_and_after_owner_cancel() {
    let url = std::env::var("IPFS_S3_STAGE7_APPLY_DATABASE_URL")
        .expect("parent must supply a fresh exclusive PostgreSQL schema URL");
    let parsed = url::Url::parse(&url).unwrap();
    assert!(matches!(parsed.scheme(), "postgres" | "postgresql"));
    let mut options = ConnectOptions::new(url);
    options.max_connections(1).min_connections(1);
    let db = Database::connect(options).await.unwrap();
    let schema: String = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT current_schema() AS schema",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "schema")
        .unwrap();
    assert!(schema.starts_with("stage7_"));
    let claimed: bool = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT pg_try_advisory_lock(hashtextextended(current_database() || ':' || current_schema(), 0)) AS claimed"))
        .await.unwrap().unwrap().try_get("", "claimed").unwrap();
    assert!(claimed, "another runner owns this schema");
    let empty: bool = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT NOT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace)
            AND NOT EXISTS (SELECT 1 FROM pg_proc WHERE pronamespace=current_schema()::regnamespace)
            AND NOT EXISTS (SELECT 1 FROM pg_type WHERE typnamespace=current_schema()::regnamespace) AS empty"))
        .await.unwrap().unwrap().try_get("", "empty").unwrap();
    assert!(
        empty,
        "preserve occupied schemas; parent must supply a fresh schema"
    );
    let mut fixture = support::Fixture::new().await;
    support::base::setup(&db, &fixture.config).await;
    fixture.auth = records::authorize_local(&fixture.raw, &db).await.unwrap();
    fixture.db = db;
    shared_authority_completeness(fixture).await;
}

fn action_key(
    fixture: &support::Fixture,
    plan_id: &str,
    action: &records::CommittedAction,
) -> NativeKey {
    NativeKey {
        table: "pin_management_records".into(),
        values: vec![
            fixture.auth.database_scope().into(),
            plan_id.into(),
            action.receipt.binding.request.item_id.clone(),
            "action".into(),
        ],
    }
}

async fn shared_authority_completeness(mut fixture: support::Fixture) {
    // The real HTTP adapter makes an accidental provider call observable, unlike Noop.
    fixture.raw.pinning.providers[0].kind = "pinata".into();
    fixture.raw.pinning.providers[0].endpoint = Some(format!("{}/psa", fixture.server.uri()));
    fixture.raw.pinning.providers[0].token_env = Some("STAGE75_LOCATOR_MEET_TOKEN".into());
    let identity = &mut fixture.raw.pinning_identity.providers[0];
    identity.provider_id = "locator-meet-psa".into();
    identity.backend = "pinata".into();
    identity.api_profile = "pinata-psa".into();
    identity.secret_ref = Some("env:STAGE75_LOCATOR_MEET_TOKEN".into());
    fixture.config = local_config::compile(&fixture.raw, |_| Some("controlled-token".into()))
        .unwrap()
        .with_source_node(support::NODE.into())
        .unwrap();
    let provider = &fixture.config.pinning.providers[0];
    ledger::register_route(&fixture.db, &provider.name, &provider.identity)
        .await
        .unwrap();

    let unrelated = fixture
        .publish_cid(
            "ordinary-unrelated",
            "bafkreigh2akiscaildcqg2tnkyg4v3v55afkjp4hoyp5vzo5ibxq3d5x4q",
            true,
        )
        .await;
    let ordinary = pin_job::Entity::find()
        .filter(pin_job::Column::Cid.eq(&unrelated.cid))
        .filter(pin_job::Column::Operation.eq("submit"))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert!(
        job_authority::lookup(&fixture.db, &ordinary)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        job_authority::check_before_call(&fixture.db, &ordinary)
            .await
            .unwrap()
            .is_none()
    );
    let empty = locator::discover_authority(
        &fixture.db,
        &[AuthoritySelector::Job(ordinary.id.clone())],
        &mut Budget::default(),
    )
    .await
    .unwrap();
    assert!(empty.originals.is_empty());

    let first_owner = fixture
        .publish_cid("first-owner", support::CID, false)
        .await;
    let second_owner = fixture
        .publish_cid("second-owner", support::CID, false)
        .await;
    assert_ne!(first_owner.object_id, second_owner.object_id);
    assert_eq!(first_owner.cid, second_owner.cid);
    let mut birth_request = fixture.create_request("shared-submit", first_owner.clone(), false);
    birth_request.items[0].item_id = "a-first".into();
    let mut second_item = fixture
        .create_request("shared-submit", second_owner.clone(), false)
        .items
        .remove(0);
    second_item.item_id = "b-second".into();
    birth_request.items.push(second_item);
    let birth = fixture.confirm(&birth_request).await;
    let created = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &birth)
        .await
        .unwrap();
    assert_eq!(created.items.len(), 2);
    let first = created
        .items
        .iter()
        .find(|item| item.receipt.binding.request.item_id == "a-first")
        .unwrap();
    let second = created
        .items
        .iter()
        .find(|item| item.receipt.binding.request.item_id == "b-second")
        .unwrap();
    let submit = first
        .jobs
        .iter()
        .find(|authority| authority.directive == records::JobDirective::Submit)
        .expect("the first Create must publish a Submit authority");
    let shared = second
        .jobs
        .iter()
        .find(|authority| authority.job_id == submit.job_id)
        .expect("the second same-plan Create must non-vacuously reference the existing Submit job");
    assert!(shared == submit);
    assert!(first.receipt.job_ids.contains(&submit.job_id));
    assert!(second.receipt.job_ids.contains(&submit.job_id));
    let job = pin_job::Entity::find_by_id(submit.job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "pending");
    let submit_jobs = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("submit"))
        .filter(pin_job::Column::Cid.eq(&job.cid))
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(submit_jobs, vec![job.clone()]);
    let submit_key = action_key(&fixture, &birth_request.plan_id, first);
    let shared_key = action_key(&fixture, &birth_request.plan_id, second);
    assert_ne!(submit_key, shared_key);
    for item in [first, second] {
        assert!(
            records::lookup_committed(
                &fixture.db,
                &fixture.auth,
                &birth,
                &item.receipt.binding.request.item_id
            )
            .await
            .unwrap()
            .as_ref()
                == Some(item)
        );
    }

    for owner_cancelled in [false, true] {
        if owner_cancelled {
            let owner = submit.owner.as_ref().unwrap();
            assert!(
                owner.object_id == first_owner.object_id
                    || owner.object_id == second_owner.object_id
            );
            let request = fixture
                .existing_request("cancel-submit-owner", owner.clone(), Action::Cancel)
                .await;
            let binding = fixture.confirm(&request).await;
            actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
                .await
                .unwrap();
            let lease = pin_lease::Entity::find_by_id(submit.lease_id.clone().unwrap())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(lease.state, "cancelled");
        }
        assert!(
            job_authority::lookup(&fixture.db, &job)
                .await
                .unwrap()
                .as_ref()
                == Some(submit)
        );
        assert_permit(&fixture, &job, owner_cancelled).await;
        missing_shared_source(
            &fixture,
            &birth,
            &job,
            &ordinary,
            &submit_key,
            &shared_key,
            owner_cancelled,
        )
        .await;
        assert!(
            job_authority::lookup(&fixture.db, &job)
                .await
                .unwrap()
                .as_ref()
                == Some(submit)
        );
    }
}

async fn assert_permit(fixture: &support::Fixture, job: &pin_job::Model, owner_cancelled: bool) {
    let permit = job_authority::check_before_call(&fixture.db, job)
        .await
        .unwrap();
    assert!(matches!(
        (owner_cancelled, permit),
        (false, Some(JobPermit::Submit(_))) | (true, Some(JobPermit::LedgerOnly(_)))
    ));
    assert_eq!(
        job_authority::projection_allowed(&fixture.db, job)
            .await
            .unwrap(),
        !owner_cancelled
    );
}

async fn missing_shared_source(
    fixture: &support::Fixture,
    birth: &records::ConfirmedBinding,
    job: &pin_job::Model,
    ordinary: &pin_job::Model,
    submit_key: &NativeKey,
    shared_key: &NativeKey,
    owner_cancelled: bool,
) {
    let scope = Box::pin(ManagementJobScope::from_applied(
        &fixture.db,
        &fixture.auth,
        birth,
    ))
    .await
    .unwrap();
    let dispatchable = pin_job::Entity::find_by_id(job.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(dispatchable.state.as_str(), "pending" | "retry"));
    assert!(
        dispatchable.locked_until.is_none(),
        "worker must face real due work, not a terminal job"
    );
    assert!(dispatchable.next_attempt_at <= chrono::Utc::now());

    let selectors = [AuthoritySelector::Job(job.id.clone())];
    let complete = locator::discover_authority(&fixture.db, &selectors, &mut Budget::default())
        .await
        .unwrap();
    let submit_original = complete.originals.get(submit_key).unwrap();
    let shared_original = complete.originals.get(shared_key).unwrap();
    assert!(!submit_original.is_empty());
    assert!(!shared_original.is_empty());

    let shared_source = locator::source_key(shared_key).unwrap();
    let selector_key = serde_json::to_string(&job.id).unwrap();
    let deleted = fixture.db.execute(Statement::from_sql_and_values(
        fixture.db.get_database_backend(),
        "DELETE FROM pin_management_locator WHERE source_key=$1 AND selector_kind='job' AND selector_key=$2",
        [shared_source.clone().into(), selector_key.clone().into()],
    )).await.unwrap();
    assert_eq!(deleted.rows_affected(), 1);
    let remaining = fixture.db.query_all(Statement::from_sql_and_values(
        fixture.db.get_database_backend(),
        "SELECT source_key FROM pin_management_locator WHERE selector_kind='job' AND selector_key=$1 ORDER BY source_key",
        [selector_key.clone().into()],
    )).await.unwrap();
    assert!(
        remaining
            .iter()
            .any(|row| row.try_get::<String>("", "source_key").unwrap()
                == locator::source_key(submit_key).unwrap())
    );
    assert!(
        !remaining
            .iter()
            .any(|row| row.try_get::<String>("", "source_key").unwrap() == shared_source)
    );
    let pending = fixture.db.query_one(Statement::from_sql_and_values(
        fixture.db.get_database_backend(),
        "SELECT 1 AS n FROM pin_management_pending_scope WHERE source_key=$1 AND selector_kind='job' AND selector_key=$2",
        [shared_source.into(), selector_key.into()],
    )).await.unwrap();
    assert!(
        pending.is_some(),
        "deletion must preserve an independent job-scoped dirty tombstone"
    );
    assert!(
        locator::discover_authority(&fixture.db, &selectors, &mut Budget::default())
            .await
            .is_err()
    );
    assert!(job_authority::lookup(&fixture.db, job).await.is_err());
    assert!(
        job_authority::check_before_call(&fixture.db, job)
            .await
            .is_err()
    );
    assert!(
        job_authority::lookup(&fixture.db, ordinary)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        job_authority::check_before_call(&fixture.db, ordinary)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        locator::discover_authority(
            &fixture.db,
            &[AuthoritySelector::Job(ordinary.id.clone())],
            &mut Budget::default()
        )
        .await
        .unwrap()
        .originals
        .is_empty()
    );

    let before_jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    let before_leases = pin_lease::Entity::find().all(&fixture.db).await.unwrap();
    let before_targets = pin_lease_target::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let before_remote = remote_pin::Entity::find().all(&fixture.db).await.unwrap();
    let before_history = jobs::history::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let before_invocations = pin_invocation_route::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let before_submit_facts = pin_submit_observation::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let before_replica_facts = pin_replica_observation::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let before_http = fixture.server.received_requests().await.unwrap().len();
    let coordinator = PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(KuboClient::new(fixture.server.uri())),
    )
    .unwrap();
    let store = Store::new(fixture.db.clone());
    let worker = tokio::spawn(async move {
        Box::pin(coordinator.management_tick(&store, &scope, &CancellationToken::new())).await
    })
    .await
    .unwrap();
    assert!(
        worker.is_err(),
        "receipt-scoped execution must reject incomplete authority"
    );
    assert_eq!(
        fixture.server.received_requests().await.unwrap().len(),
        before_http
    );
    assert_eq!(
        pin_job::Entity::find().all(&fixture.db).await.unwrap(),
        before_jobs
    );
    assert_eq!(
        pin_lease::Entity::find().all(&fixture.db).await.unwrap(),
        before_leases
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap(),
        before_targets
    );
    assert_eq!(
        remote_pin::Entity::find().all(&fixture.db).await.unwrap(),
        before_remote
    );
    assert_eq!(
        jobs::history::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap(),
        before_history
    );
    assert_eq!(
        pin_invocation_route::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap(),
        before_invocations
    );
    assert_eq!(
        pin_submit_observation::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap(),
        before_submit_facts
    );
    assert_eq!(
        pin_replica_observation::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap(),
        before_replica_facts
    );

    let originals = native::capture(&fixture.db).await.unwrap();
    for (key, expected) in [(submit_key, submit_original), (shared_key, shared_original)] {
        let values: Vec<_> = key.values.iter().map(String::as_str).collect();
        let actual: native::NativeRow = originals
            .row(&key.table, &values)
            .unwrap()
            .keys()
            .map(|column| {
                (
                    column.clone(),
                    originals
                        .original_value(&key.table, &values, column)
                        .map(str::to_owned),
                )
            })
            .collect();
        assert_eq!(&actual, expected);
    }
    let tx = fixture.db.begin().await.unwrap();
    locator::rebuild_source(&tx, shared_key, &mut Budget::default())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(job_authority::lookup(&fixture.db, job).await.is_err());
    let tx = fixture.db.begin().await.unwrap();
    locator::rebuild_source(&tx, shared_key, &mut Budget::default())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let repaired = locator::discover_authority(&fixture.db, &selectors, &mut Budget::default())
        .await
        .unwrap();
    assert_eq!(repaired.originals.get(submit_key), Some(submit_original));
    assert_eq!(repaired.originals.get(shared_key), Some(shared_original));
    let seal = repaired
        .proof
        .seals
        .iter()
        .find(|seal| &seal.source == shared_key)
        .unwrap();
    assert_eq!(seal.source_generation, seal.sealed_source_generation);
    assert_eq!(seal.locator_generation, seal.sealed_locator_generation);
    assert!(
        job_authority::lookup(&fixture.db, job)
            .await
            .unwrap()
            .is_some()
    );
    assert_permit(fixture, job, owner_cancelled).await;
    assert!(
        job_authority::lookup(&fixture.db, ordinary)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture.server.received_requests().await.unwrap().len(),
        before_http
    );
}

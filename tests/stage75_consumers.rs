use ipfs_s3_gateway::{
    pinning::identity::{CleanupMode, ProviderIdentity},
    store::{
        self,
        entities::pin_job,
        pinning::{
            ledger,
            management::{
                actions::job_authority,
                native::{self, meter::Budget},
                records::locator::{self, AuthoritySelector},
            },
        },
    },
};
use sea_orm::EntityTrait;
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement, TransactionTrait};
#[allow(dead_code)]
#[path = "support/stage7_management_actions.rs"]
mod support;

fn ordinary_job(id: &str) -> pin_job::Model {
    let now = chrono::Utc::now();
    pin_job::Model {
        id: id.into(),
        operation: "submit".into(),
        provider: "ordinary".into(),
        cid: "cid".into(),
        lease_id: None,
        target_id: None,
        expected_generation: None,
        expected_remote_epoch: None,
        state: "pending".into(),
        attempts: 0,
        next_attempt_at: now,
        locked_until: None,
        submit_phase: None,
        last_error: None,
        created_at: now,
        updated_at: now,
    }
}

fn identity(provider: &str, retired: bool) -> ProviderIdentity {
    ProviderIdentity {
        provider_id: provider.into(),
        display_name: provider.into(),
        backend: "noop".into(),
        scope: "consumer-fixture".into(),
        storage_domain: "consumer-fixture".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: "noop".into(),
        strategy: "cid".into(),
        retired,
        cleanup: CleanupMode::Retain,
    }
}

#[tokio::test]
async fn ordinary_publication_worker_completes_with_unrelated_applied_history() {
    use ipfs_s3_gateway::{
        kubo::KuboClient,
        pinning::{coordinator::PinningCoordinator, worker::ManagementJobScope},
        store::{
            Store,
            entities::{pin_lease_target, remote_pin},
            pinning::{
                jobs,
                management::{actions, records},
            },
        },
    };
    use sea_orm::{ColumnTrait, QueryFilter};
    use sha2::Digest;
    use tokio_util::sync::CancellationToken;

    let fixture = support::Fixture::new().await;
    let history_object = fixture.publish("unrelated-history", false).await;
    let history_request = fixture.create_request("ordinary-history", history_object, false);
    let history_binding = fixture.confirm(&history_request).await;
    actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &history_binding,
    )
    .await
    .unwrap();
    let scope = Box::pin(ManagementJobScope::from_applied(
        &fixture.db,
        &fixture.auth,
        &history_binding,
    ))
    .await
    .unwrap();
    let coordinator = PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(KuboClient::new(fixture.server.uri())),
    )
    .unwrap();
    let store = Store::new(fixture.db.clone());
    let report = tokio::spawn(async move {
        Box::pin(coordinator.management_tick(&store, &scope, &CancellationToken::new())).await
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report.items[0].state, records::ActionState::RemoteComplete);

    let digest = sha2::Sha256::digest(b"0123456789");
    let cid = cid::Cid::new_v1(
        0x55,
        cid::multihash::Multihash::<64>::wrap(0x12, &digest).unwrap(),
    )
    .to_string();
    let ordinary = fixture.publish_cid("ordinary-native", &cid, true).await;
    let job = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("submit"))
        .filter(pin_job::Column::Cid.eq(&cid))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "pending");
    assert!(
        job_authority::lookup(&fixture.db, &job)
            .await
            .unwrap()
            .is_none()
    );
    fixture
        .settle_with_worker(job.lease_id.as_deref().unwrap())
        .await;
    let finished = pin_job::Entity::find_by_id(job.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.state, "done");
    assert!(finished.locked_until.is_none());
    let calls = jobs::submission_history(&fixture.db, &job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.submit_calls, 1);
    let remote = remote_pin::Entity::find_by_id((job.provider.clone(), cid))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote.status, "pinned");
    assert!(remote.request_id.is_some());
    let target = pin_lease_target::Entity::find_by_id(job.target_id.unwrap())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target.state, "pinned");
    let lease =
        ipfs_s3_gateway::store::entities::pin_lease::Entity::find_by_id(job.lease_id.unwrap())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(lease.owner_object_id, ordinary.object_id);
    assert!(
        records::lookup_committed(
            &fixture.db,
            &fixture.auth,
            &history_binding,
            &history_request.items[0].item_id,
        )
        .await
        .unwrap()
        .is_some()
    );
}

#[tokio::test]
async fn dirty_job_locator_is_not_ordinary_absence_and_does_not_block_an_unrelated_job() {
    let fixture = support::Fixture::new().await;
    let object = fixture.publish("consumer-job", false).await;
    let request = fixture.create_request("consumer-authority", object, false);
    let binding = fixture.confirm(&request).await;
    let result = ipfs_s3_gateway::store::pinning::management::actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &binding,
    )
    .await
    .unwrap();
    let authority = result.items[0]
        .jobs
        .iter()
        .find(|authority| {
            authority.directive
                == ipfs_s3_gateway::store::pinning::management::records::JobDirective::Submit
        })
        .unwrap();
    let job = pin_job::Entity::find_by_id(authority.job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert!(
        job_authority::lookup(&fixture.db, &job)
            .await
            .unwrap()
            .is_some()
    );
    let domain = locator::canonical_domain(&authority.route).unwrap();
    let routes = locator::discover_authority(
        &fixture.db,
        &[AuthoritySelector::RegisteredDomain(domain)],
        &mut Budget::default(),
    )
    .await
    .unwrap();
    assert!(!routes.originals.is_empty());
    assert!(
        routes
            .originals
            .keys()
            .all(|key| key.table == "pin_provider_routes")
    );
    let key = native::NativeKey {
        table: "pin_management_records".into(),
        values: vec![
            fixture.auth.database_scope().into(),
            request.plan_id.clone(),
            result.items[0].receipt.binding.request.item_id.clone(),
            "action".into(),
        ],
    };
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "DELETE FROM pin_management_locator WHERE source_key=$1 AND selector_kind='job'",
            [locator::source_key(&key).unwrap().into()],
        ))
        .await
        .unwrap();
    assert!(job_authority::lookup(&fixture.db, &job).await.is_err());
    assert!(
        job_authority::lookup(&fixture.db, &ordinary_job("unrelated"))
            .await
            .unwrap()
            .is_none()
    );
    let tx = fixture.db.begin().await.unwrap();
    locator::rebuild_source(&tx, &key, &mut Budget::default())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        job_authority::lookup(&fixture.db, &job)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn route_registration_seals_its_source_and_preserves_a_closed_canonical_domain() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let original = identity("original", true);
    let key = original.allocation_key();
    ledger::register_route(&db, &key, &original).await.unwrap();
    let tx = db.begin().await.unwrap();
    let selected = locator::load_authority(
        &tx,
        &[AuthoritySelector::Domain(key.clone())],
        &mut Budget::default(),
    )
    .await
    .unwrap();
    assert_eq!(selected.originals.len(), 1);
    let original_row = selected.originals.values().next().unwrap().clone();
    tx.rollback().await.unwrap();
    let alias = identity("new-alias", false);
    assert_eq!(alias.allocation_key(), key);
    ledger::register_route(&db, &key, &alias).await.unwrap();
    let tx = db.begin().await.unwrap();
    let selected = locator::load_authority(
        &tx,
        &[AuthoritySelector::Domain(key)],
        &mut Budget::default(),
    )
    .await
    .unwrap();
    assert_eq!(selected.originals.len(), 1);
    assert_eq!(selected.originals.values().next().unwrap(), &original_row);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn ordinary_registration_accepts_the_registered_lookup_index_but_not_a_modified_index() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let route = identity("receipt-integrity", false);
    ledger::register_route(&db, &route.allocation_key(), &route)
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared("DROP INDEX s75_legacy_reverses")
        .await
        .unwrap();
    tx.execute_unprepared("CREATE INDEX s75_legacy_reverses ON pin_legacy_migrations(id,reverses)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        ledger::register_route(&db, &route.allocation_key(), &route)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn narrow_registration_lookup_retains_dirty_and_missing_locator_detection() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let route = identity("narrow-route", false);
    let provider = route.allocation_key();
    ledger::register_route(&db, &provider, &route)
        .await
        .unwrap();
    let selector = AuthoritySelector::RegisteredRoute(provider.clone());
    let selected =
        locator::discover_authority(&db, std::slice::from_ref(&selector), &mut Budget::default())
            .await
            .unwrap();
    assert_eq!(selected.originals.len(), 1);
    assert!(
        selected
            .originals
            .keys()
            .all(|key| key.table == "pin_provider_routes")
    );
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "DELETE FROM pin_management_locator WHERE selector_kind='registered_route' AND selector_key=$1",
        [serde_json::to_string(&provider).unwrap().into()])).await.unwrap();
    assert!(
        locator::discover_authority(&db, &[selector], &mut Budget::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn ordinary_locator_reads_and_rebuilds_do_not_authorize_backfill() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let route = identity("backfill-route", false);
    let provider = route.allocation_key();
    ledger::register_route(&db, &provider, &route)
        .await
        .unwrap();
    let key = native::NativeKey {
        table: "pin_provider_routes".into(),
        values: vec![provider.clone()],
    };
    let tx = db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE pin_provider_routes SET display_name='changed' WHERE provider=$1",
        [provider.clone().into()],
    ))
    .await
    .unwrap();
    tx.execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
        .await
        .unwrap();
    assert!(
        locator::rebuild_source(&tx, &key, &mut Budget::default())
            .await
            .is_err()
    );
    assert!(
        locator::load_authority(
            &tx,
            &[AuthoritySelector::RegisteredRoute(provider)],
            &mut Budget::default()
        )
        .await
        .is_err()
    );
    let dirty = tx.query_one(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT 1 AS present FROM pin_management_source_control c WHERE source_key=$1 AND (sealed_source IS NULL OR sealed_source<>source_generation) AND EXISTS (SELECT 1 FROM pin_management_pending_scope p WHERE p.source_key=c.source_key)",
        [locator::source_key(&key).unwrap().into()])).await.unwrap();
    assert!(dirty.is_some());
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn preparation_version_lookup_preserves_original_evidence_and_requires_a_valid_cid() {
    for cid in ["cid", support::CID] {
        let db = store::connect_database("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        let (scope, database_identity) = native::scope::binding(&db).await.unwrap();
        let object = serde_json::json!({"id":"object", "bucket":"bucket", "key":"key", "cid":cid,
            "metadata":" { \"private\":null } "});
        let version = serde_json::json!({"id":"version", "object_id":"object", "bucket":"bucket",
            "key":"key", "version_id":null, "kind":"object"});
        let attestation = serde_json::json!({
            "identity":{"bucket":"bucket","key":"key","public_version":"null",
                "private_version_id":"version","object_id":"object","cid":cid},
            "object_tuple":object,"version_tuple":version,
            "source":{"node_identity":"node","local_reads_only":true,"idle_timeout_millis":1000},
            "stored_bytes":1,"proof":"file_clean_eof","observed_at":"2026-01-01T00:00:00Z",
            "expires_at":"2026-01-01T00:01:00Z"
        });
        serde_json::from_value::<ipfs_s3_gateway::pinning::management::evidence::ReadAttestation>(
            attestation.clone(),
        )
        .unwrap();
        let result = serde_json::json!([attestation, "http://private-node/"]).to_string();
        let before = serde_json::json!([object, version]).to_string();
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "INSERT INTO pin_management_records VALUES ($1,'prepare','version','evidence_preparation','operator',$2,'{}',$3,$4,'[]','[]','prepared','2026-01-01T00:00:00Z','2026-01-01T00:01:00Z','2026-01-01T00:00:00Z')",
            [scope.clone().into(),database_identity.into(),before.clone().into(),result.clone().into()]
        )).await.unwrap();
        let key = native::NativeKey {
            table: "pin_management_records".into(),
            values: vec![
                scope,
                "prepare".into(),
                "version".into(),
                "evidence_preparation".into(),
            ],
        };
        let tx = db.begin().await.unwrap();
        let sealed = locator::rebuild_source(&tx, &key, &mut Budget::default()).await;
        if cid == "cid" {
            assert!(ipfs_s3_gateway::pinning::provider::canonical_resource_cid(cid).is_err());
            assert_eq!(
                sealed,
                Err(store::pinning::management::ManagementError::InvalidEvidence)
            );
            tx.rollback().await.unwrap();
            continue;
        }
        sealed.unwrap();
        tx.commit().await.unwrap();
        let tx = db.begin().await.unwrap();
        let selected = locator::load_authority(
            &tx,
            &[
                AuthoritySelector::ExactVersion("version".into()),
                AuthoritySelector::ExactVersion("missing".into()),
            ],
            &mut Budget::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            selected.originals[&key]["native_before"].as_deref(),
            Some(before.as_str())
        );
        assert_eq!(
            selected.originals[&key]["result"].as_deref(),
            Some(result.as_str())
        );
        assert_eq!(
            selected.originals[&key]["expires_at"].as_deref(),
            Some("2026-01-01T00:01:00Z")
        );
        assert!(
            selected
                .proof
                .memberships
                .iter()
                .any(
                    |member| member.selector == AuthoritySelector::ExactVersion("missing".into())
                        && member.sources.is_empty()
                )
        );
        let frame = native::NativeFrame::from_scoped_reads(
            &[],
            std::slice::from_ref(&selected),
            &mut Budget::default(),
        )
        .unwrap();
        native::scope::revalidate_frame_scope(&tx, &frame, &mut Budget::default())
            .await
            .unwrap();
        tx.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn ordinary_legacy_lookup_honors_a_reversal_dependency() {
    use ipfs_s3_gateway::store::pinning::legacy_migration;
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let destination = identity("legacy-destination", false);
    ledger::register_route(&db, &destination.allocation_key(), &destination)
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',97,3)",
    )
    .await
    .unwrap();
    let declaration: legacy_migration::Declaration = serde_json::from_value(serde_json::json!({
        "version": 1, "database_scope": "fixture", "ttl_seconds": 300,
        "maintenance": {"all_writers_stopped": true, "all_workers_drained": true},
        "namespaces": [{"source": "old", "target": destination.allocation_key(),
            "backend": destination.backend, "scope": destination.scope,
            "resource_type": destination.resource_type(), "binding_only": false,
            "target_route": destination.route_snapshot(), "bindings": [{
                "table": "pin_provider_usage", "key": {"provider": "old"},
                "provider": "old", "historical_route": null
            }]}]
    }))
    .unwrap();
    let plan = legacy_migration::inventory(&db, &declaration)
        .await
        .unwrap();
    let receipt = legacy_migration::apply(&db, &declaration, &plan)
        .await
        .unwrap();
    assert!(
        legacy_migration::source_is_migrated(&db, "old")
            .await
            .unwrap()
    );
    legacy_migration::rollback(&db, &receipt.id, &plan)
        .await
        .unwrap();
    assert!(
        !legacy_migration::source_is_migrated(&db, "old")
            .await
            .unwrap()
    );
    assert_eq!(
        legacy_migration::historical_keys(
            &db,
            "pin_provider_usage",
            &destination.allocation_key(),
            None
        )
        .await
        .unwrap(),
        vec![destination.allocation_key()]
    );
}

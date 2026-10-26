use super::super::{file_database, management_catalog_tests};
use super::{complete, migration};
use crate::store::{
    self,
    entities::{pin_job, remote_pin_ledger},
    pinning::{
        ledger,
        management::{admission::authority, native, records},
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, Statement, TransactionTrait};
use std::collections::BTreeSet;

#[tokio::test]
async fn loaded_job_receipts_preserve_job_and_resource_entry_points_and_fresh_operation_state() {
    let (_directory, db) = file_database().await;
    store::run_migrations(&db).await.unwrap();
    let mut declaration = management_catalog_tests::declaration(&db).await;
    declaration.database_scope = native::scope::binding(&db).await.unwrap().0;
    db.execute_unprepared("INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,submit_phase,state,attempts,next_attempt_at,created_at,updated_at) VALUES ('receipt-job','unpin','old','job-cid',1,NULL,'pending',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES ('old','resource-cid',1,'reserved',1,0,CURRENT_TIMESTAMP)").await.unwrap();
    let declaration = complete(&db, declaration).await;
    let target = declaration.namespaces[0].target.clone();
    let plan = migration::inventory(&db, &declaration).await.unwrap();
    let applied = migration::apply(&db, &declaration, &plan).await.unwrap();
    let job = pin_job::Entity::find_by_id("receipt-job")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.provider, target);
    let work = records::SharedDecodeBudget::default();
    let context = records::DecodeWork::shared(&work);
    let candidates = authority::load(&db, &migration::job_receipt_selectors(&job), context)
        .await
        .unwrap();
    assert!(!candidates.originals.is_empty());
    for selector in migration::job_receipt_selectors(&job).iter().filter(|selector| matches!(selector, records::locator::AuthoritySelector::LegacyPoint(table, ..) | records::locator::AuthoritySelector::LegacyResource(table, ..) if table != "pin_jobs")) {
        assert!(candidates.proof.memberships.iter().find(|member| &member.selector == selector).unwrap().sources.is_empty());
    }
    let keys =
        migration::historical_job_keys_from_loaded(&db, &target, &job.id, &candidates, context)
            .await
            .unwrap();
    assert_eq!(
        keys.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from(["old".to_owned(), target.clone()])
    );
    assert_eq!(
        migration::historical_job_keys(&db, &target, &job.id, &())
            .await
            .unwrap()
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["old".to_owned(), target.clone()])
    );

    // A job-only receipt must restrict the original job even without a resource receipt.
    let mut original_job = job.clone();
    original_job.provider = "old".into();
    assert!(
        !ledger::job_route_matches(&db, &original_job, None)
            .await
            .unwrap()
    );
    let compound_work = records::SharedDecodeBudget::default();
    assert_eq!(
        ledger::job_io_and_route_with_work(
            &db,
            &original_job,
            None,
            records::DecodeWork::shared(&compound_work),
        )
        .await
        .unwrap(),
        ledger::JobAdmission::IoDenied
    );

    let mut resource_job = job.clone();
    resource_job.id = "new-resource-job".into();
    resource_job.operation = "reconcile".into();
    resource_job.provider = "old".into();
    resource_job.cid = "resource-cid".into();
    let resource_work = records::SharedDecodeBudget::default();
    let resource_context = records::DecodeWork::shared(&resource_work);
    let mut resource_candidates = authority::load(
        &db,
        &migration::job_receipt_selectors(&resource_job),
        resource_context,
    )
    .await
    .unwrap();
    assert!(!resource_candidates.originals.is_empty());
    assert!(
        resource_candidates
            .proof
            .memberships
            .iter()
            .find(|member| member.selector
                == records::locator::AuthoritySelector::LegacyJob(
                    "old".into(),
                    resource_job.id.clone()
                ))
            .unwrap()
            .sources
            .is_empty()
    );
    assert_eq!(
        migration::historical_job_keys_from_loaded(
            &db,
            "old",
            &resource_job.id,
            &resource_candidates,
            resource_context
        )
        .await
        .unwrap(),
        vec!["old".to_owned()]
    );
    let resource_selector = records::locator::AuthoritySelector::LegacyResource(
        "remote_pin_ledger".into(),
        "old".into(),
        Some(resource_job.cid.clone()),
    );
    let member_index = resource_candidates
        .proof
        .memberships
        .iter()
        .position(|member| member.selector == resource_selector)
        .unwrap();
    let membership = resource_candidates.proof.memberships.remove(member_index);
    assert!(
        migration::job_io_allowed_from_loaded(
            &db,
            &resource_job,
            &resource_candidates,
            resource_context,
        )
        .await
        .is_err()
    );
    resource_candidates
        .proof
        .memberships
        .insert(member_index, membership);
    let tx = db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(), "INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,submit_phase,state,attempts,next_attempt_at,created_at,updated_at) VALUES ($1,'reconcile','old','resource-cid',$2,NULL,'pending',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", [resource_job.id.clone().into(), resource_job.expected_remote_epoch.into()])).await.unwrap();
    let persisted_resource_job = pin_job::Entity::find_by_id(resource_job.id.clone())
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            &persisted_resource_job.operation,
            &persisted_resource_job.provider,
            &persisted_resource_job.cid,
            persisted_resource_job.expected_remote_epoch,
            persisted_resource_job.lease_id.as_deref(),
            persisted_resource_job.target_id.as_deref(),
            persisted_resource_job.expected_generation,
        ),
        (
            &resource_job.operation,
            &resource_job.provider,
            &resource_job.cid,
            resource_job.expected_remote_epoch,
            resource_job.lease_id.as_deref(),
            resource_job.target_id.as_deref(),
            resource_job.expected_generation,
        )
    );
    let resource_job = persisted_resource_job;
    ledger::capture_reservation(&tx, &target, &resource_job.cid)
        .await
        .unwrap();
    assert!(
        remote_pin_ledger::Entity::find_by_id(("old".to_owned(), resource_job.cid.clone()))
            .one(&tx)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        ledger::get(&tx, "old", &resource_job.cid)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        !ledger::job_route_matches(&tx, &resource_job, None)
            .await
            .unwrap()
    );
    // A resource-only alias must reach the target row and propagate its read error,
    // rather than treating the empty job membership as an empty receipt union.
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE remote_pin_ledger SET first_observed_at='not-a-timestamp' WHERE provider=$1 AND cid=$2",
        [target.clone().into(), resource_job.cid.clone().into()],
    ))
    .await
    .unwrap();
    let fault_work = records::SharedDecodeBudget::default();
    let fault_candidates = authority::load(
        &tx,
        &migration::job_receipt_selectors(&resource_job),
        records::DecodeWork::shared(&fault_work),
    )
    .await
    .unwrap();
    assert!(!fault_candidates.originals.is_empty());
    assert!(
        fault_candidates
            .proof
            .memberships
            .iter()
            .find(|member| member.selector
                == records::locator::AuthoritySelector::LegacyJob(
                    "old".into(),
                    resource_job.id.clone()
                ))
            .unwrap()
            .sources
            .is_empty()
    );
    drop(fault_candidates);
    assert!(
        remote_pin_ledger::Entity::find_by_id(("old".to_owned(), resource_job.cid.clone()))
            .one(&tx)
            .await
            .unwrap()
            .is_none()
    );
    assert!(ledger::get(&tx, "old", &resource_job.cid).await.is_err());
    assert!(
        !crate::store::pinning::management::admission::unregistered_legacy(&tx, "old")
            .await
            .unwrap()
    );
    assert!(
        ledger::job_route_matches(&tx, &resource_job, None)
            .await
            .is_err()
    );
    // The resource-only union is not ordinary. Its IO denial must short-circuit
    // the route read, which would otherwise fail on the target ledger above.
    let compound_work = records::SharedDecodeBudget::default();
    assert_eq!(
        ledger::job_io_and_route_with_work(
            &tx,
            &resource_job,
            None,
            records::DecodeWork::shared(&compound_work),
        )
        .await
        .unwrap(),
        ledger::JobAdmission::IoDenied
    );
    tx.rollback().await.unwrap();

    let fresh_work = records::SharedDecodeBudget::default();
    let fresh_context = records::DecodeWork::shared(&fresh_work);
    let before_change =
        authority::load(&db, &migration::job_receipt_selectors(&job), fresh_context)
            .await
            .unwrap();
    let tx = db.begin().await.unwrap();
    let source = records::locator::source_key(&native::NativeKey {
        table: "pin_legacy_migrations".into(),
        values: vec![applied.id.clone()],
    })
    .unwrap();
    let removed = tx
        .execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            "DELETE FROM pin_management_locator WHERE source_key=$1",
            [source.into()],
        ))
        .await
        .unwrap();
    assert!(removed.rows_affected() > 0);
    assert!(
        migration::historical_job_keys_from_loaded(
            &tx,
            &target,
            &job.id,
            &before_change,
            fresh_context
        )
        .await
        .is_err()
    );
    assert!(
        ledger::job_route_matches(&tx, &original_job, None)
            .await
            .is_err()
    );
    let compound_work = records::SharedDecodeBudget::default();
    assert!(
        ledger::job_io_and_route_with_work(
            &tx,
            &original_job,
            None,
            records::DecodeWork::shared(&compound_work),
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    migration::rollback(&db, &applied.id, &plan).await.unwrap();
    assert_eq!(
        migration::historical_job_keys_from_loaded(
            &db,
            &target,
            &job.id,
            &before_change,
            fresh_context
        )
        .await
        .unwrap(),
        vec![target.clone()]
    );
    assert_eq!(
        migration::historical_job_keys(&db, &target, &job.id, &())
            .await
            .unwrap(),
        vec![target.clone()]
    );
    let compound_work = records::SharedDecodeBudget::default();
    assert_eq!(
        ledger::job_io_and_route_with_work(
            &db,
            &original_job,
            None,
            records::DecodeWork::shared(&compound_work),
        )
        .await
        .unwrap_or_else(|error| {
            let work = compound_work.lock().unwrap();
            panic!(
                "post-rollback compound admission: error={error:?}, decode_usage={:?}, storage_usage={:?}",
                work.usage(),
                work.storage_usage(),
            );
        }),
        ledger::JobAdmission::RouteDenied
    );

    // A new target allocation still encounters the reversed resource receipts.
    // Its own immutable capture, current identity and epoch permit observation.
    let route = &declaration.namespaces[0].target_route;
    let identity = crate::pinning::identity::ProviderIdentity {
        provider_id: route.provider_id.clone(),
        display_name: "post-rollback allocation".into(),
        backend: route.backend.clone(),
        scope: route.scope.clone(),
        storage_domain: route.storage_domain.clone(),
        credential_revision: route.credential_revision,
        endpoint_revision: route.endpoint_revision,
        secret_ref: route.secret_ref.clone(),
        api_profile: route.api_profile.clone(),
        strategy: route.strategy.clone(),
        retired: false,
        cleanup: route.cleanup,
    };
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES ($1,'resource-cid',1,'reserved',1,0,CURRENT_TIMESTAMP)",
        [target.clone().into()])).await.unwrap();
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,submit_phase,state,attempts,next_attempt_at,created_at,updated_at) VALUES ('post-rollback-job','reconcile',$1,'resource-cid',1,NULL,'pending',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [target.clone().into()])).await.unwrap();
    ledger::capture_reservation(&db, &target, "resource-cid")
        .await
        .unwrap();
    ledger::capture_invocation(&db, "post-rollback-job", &target, "resource-cid")
        .await
        .unwrap();
    let post_rollback_job = pin_job::Entity::find_by_id("post-rollback-job")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let proof_work = records::SharedDecodeBudget::default();
    let proof = authority::load(
        &db,
        &migration::job_receipt_selectors(&post_rollback_job),
        records::DecodeWork::shared(&proof_work),
    )
    .await
    .unwrap();
    assert!(!proof.originals.is_empty());
    drop(proof);
    let compound_work = records::SharedDecodeBudget::default();
    assert_eq!(
        ledger::job_io_and_route_with_work(
            &db,
            &post_rollback_job,
            Some(&identity),
            records::DecodeWork::shared(&compound_work),
        )
        .await
        .unwrap(),
        ledger::JobAdmission::Allowed
    );
}

#[tokio::test]
async fn cleanup_compound_keeps_fresh_alias_identity_and_ordinary_ownership_rules() {
    use crate::pinning::identity::{CleanupMode, ProviderIdentity};
    let (_directory, db) = file_database().await;
    store::run_migrations(&db).await.unwrap();
    let runtime = crate::store::Store::new(db.clone());
    let mut declaration = management_catalog_tests::declaration(&db).await;
    let mut route = declaration.namespaces[0].target_route.clone();
    route.cleanup = CleanupMode::Managed;
    let identity = ProviderIdentity {
        provider_id: route.provider_id.clone(),
        display_name: "cleanup fixture".into(),
        backend: route.backend.clone(),
        scope: route.scope.clone(),
        storage_domain: route.storage_domain.clone(),
        credential_revision: route.credential_revision,
        endpoint_revision: route.endpoint_revision,
        secret_ref: route.secret_ref.clone(),
        api_profile: route.api_profile.clone(),
        strategy: route.strategy.clone(),
        retired: false,
        cleanup: route.cleanup,
    };
    ledger::register_route(&db, &declaration.namespaces[0].target, &identity)
        .await
        .unwrap();
    declaration.namespaces[0].target_route = route.clone();
    db.execute_unprepared("INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES ('old','cleanup-alias',1,'reserved',1,0,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO remote_pin_ledger(provider,cid,ownership,effect) VALUES ('old','cleanup-alias','unknown','reserved')").await.unwrap();
    let declaration = complete(&db, declaration).await;
    let target = declaration.namespaces[0].target.clone();
    let plan = migration::inventory(&db, &declaration).await.unwrap();
    migration::apply(&db, &declaration, &plan).await.unwrap();
    assert!(
        !ledger::cleanup_allowed(&db, "old", "cleanup-alias")
            .await
            .unwrap()
    );
    let work = records::SharedDecodeBudget::default();
    let tx = runtime
        .begin_runtime(records::DecodeWork::shared(&work))
        .await
        .unwrap();
    let (tx, cleanup) = ledger::prepare_runtime_unpin_effect(
        tx,
        "old",
        "cleanup-alias",
        records::DecodeWork::shared(&work),
    )
    .await
    .unwrap();
    assert!(!cleanup);
    tx.rollback().await.unwrap();

    let snapshot = serde_json::to_string(&route).unwrap();
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "UPDATE remote_pin_ledger SET route=$1,ownership='application_created' WHERE provider=$2 AND cid='cleanup-alias'",
        [snapshot.clone().into(), target.clone().into()])).await.unwrap();
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "UPDATE remote_pins SET epoch=2 WHERE provider=$1 AND cid='cleanup-alias'",
        [target.clone().into()],
    ))
    .await
    .unwrap();
    let release = serde_json::json!({"confirmed_absent":true,"ledger":{"provider":target,"cid":"cleanup-alias"}}).to_string();
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES ($1,'cleanup-alias',1,$2)",
        [target.clone().into(), release.into()])).await.unwrap();
    assert!(
        migration::resource_binding(&db, "old", "cleanup-alias")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        ledger::cleanup_allowed(&db, "old", "cleanup-alias")
            .await
            .unwrap()
    );
    assert!(
        ledger::cleanup_allowed(&db, &target, "cleanup-alias")
            .await
            .unwrap()
    );
    for provider in ["old", target.as_str()] {
        let work = records::SharedDecodeBudget::default();
        let tx = runtime
            .begin_runtime(records::DecodeWork::shared(&work))
            .await
            .unwrap();
        let (tx, cleanup) = ledger::prepare_runtime_unpin_effect(
            tx,
            provider,
            "cleanup-alias",
            records::DecodeWork::shared(&work),
        )
        .await
        .unwrap();
        assert!(cleanup);
        assert_eq!(
            ledger::get(&tx, provider, "cleanup-alias")
                .await
                .unwrap()
                .unwrap()
                .effect,
            "cleanup_pending"
        );
        tx.rollback().await.unwrap();
    }

    let tx = db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
        "UPDATE remote_pin_ledger SET first_observed_at='invalid-timestamp' WHERE provider=$1 AND cid='cleanup-alias'",
        [target.clone().into()])).await.unwrap();
    assert!(
        ledger::cleanup_allowed(&tx, "old", "cleanup-alias")
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();

    ledger::capture_reservation(&db, &target, "ordinary-cleanup")
        .await
        .unwrap();
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "UPDATE remote_pin_ledger SET ownership='application_created' WHERE provider=$1 AND cid='ordinary-cleanup'",
        [target.clone().into()])).await.unwrap();
    assert!(
        ledger::cleanup_allowed(&db, &target, "ordinary-cleanup")
            .await
            .unwrap()
    );
    for (ownership, profile, cleanup) in [
        ("external_existing", "noop", CleanupMode::Managed),
        ("unknown", "noop", CleanupMode::Managed),
        ("application_created", "rpc", CleanupMode::Managed),
        ("application_created", "cluster", CleanupMode::Managed),
        ("application_created", "noop", CleanupMode::Retain),
    ] {
        route.api_profile = profile.into();
        route.cleanup = cleanup;
        let work = records::SharedDecodeBudget::default();
        let tx = runtime
            .begin_runtime(records::DecodeWork::shared(&work))
            .await
            .unwrap();
        tx.execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            "UPDATE remote_pin_ledger SET route=$1,ownership=$2 WHERE provider=$3",
            [
                serde_json::to_string(&route).unwrap().into(),
                ownership.into(),
                target.clone().into(),
            ],
        ))
        .await
        .unwrap();
        assert!(
            !ledger::cleanup_allowed(&tx, &target, "ordinary-cleanup")
                .await
                .unwrap()
        );
        assert!(
            !ledger::cleanup_allowed(&tx, "old", "cleanup-alias")
                .await
                .unwrap()
        );
        let (tx, cleanup) = ledger::prepare_runtime_unpin_effect(
            tx,
            "old",
            "cleanup-alias",
            records::DecodeWork::shared(&work),
        )
        .await
        .unwrap();
        assert!(!cleanup);
        tx.rollback().await.unwrap();
    }
    let tx = db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE remote_pin_ledger SET route=NULL WHERE provider=$1",
        [target.into()],
    ))
    .await
    .unwrap();
    assert!(
        !ledger::cleanup_allowed(&tx, "old", "cleanup-alias")
            .await
            .unwrap()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn ordinary_compound_admission_rechecks_capture_route_epoch_and_shared_work() {
    use crate::pinning::management::wire;
    use crate::{error::AppError, pinning::identity::ProviderIdentity};
    use std::sync::{Arc, Mutex};
    let (_directory, db) = file_database().await;
    store::run_migrations(&db).await.unwrap();
    let declaration = management_catalog_tests::declaration(&db).await;
    let namespace = &declaration.namespaces[0];
    let route = &namespace.target_route;
    let identity = ProviderIdentity {
        provider_id: route.provider_id.clone(),
        display_name: "compound fixture".into(),
        backend: route.backend.clone(),
        scope: route.scope.clone(),
        storage_domain: route.storage_domain.clone(),
        credential_revision: route.credential_revision,
        endpoint_revision: route.endpoint_revision,
        secret_ref: route.secret_ref.clone(),
        api_profile: route.api_profile.clone(),
        strategy: route.strategy.clone(),
        retired: false,
        cleanup: route.cleanup,
    };
    let target = &namespace.target;
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES ($1,'ordinary-route',1,'queued',1,0,CURRENT_TIMESTAMP)",
        [target.clone().into()])).await.unwrap();
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,submit_phase,state,attempts,next_attempt_at,created_at,updated_at) VALUES ('ordinary-route-job','reconcile',$1,'ordinary-route',1,NULL,'pending',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [target.clone().into()])).await.unwrap();
    ledger::capture_reservation(&db, target, "ordinary-route")
        .await
        .unwrap();
    ledger::capture_invocation(&db, "ordinary-route-job", target, "ordinary-route")
        .await
        .unwrap();
    let job = pin_job::Entity::find_by_id("ordinary-route-job")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let work = records::SharedDecodeBudget::default();
    assert_eq!(
        ledger::job_io_and_route_with_work(
            &db,
            &job,
            Some(&identity),
            records::DecodeWork::shared(&work)
        )
        .await
        .unwrap(),
        ledger::JobAdmission::Allowed
    );
    let used = work.lock().unwrap().usage().0;
    let mut budget = records::DecodeBudget::default();
    budget.consume(wire::MAX_WIRE_NODES - used + 1, 0).unwrap();
    let limited = Arc::new(Mutex::new(budget));
    for _ in 0..2 {
        assert!(matches!(
            ledger::job_io_and_route_with_work(
                &db,
                &job,
                Some(&identity),
                records::DecodeWork::shared(&limited)
            )
            .await,
            Err(AppError::ManagementBudget)
        ));
    }
    let mut wrong_identity = identity.clone();
    wrong_identity.endpoint_revision += 1;
    let mut wrong_epoch = job.clone();
    wrong_epoch.expected_remote_epoch = Some(2);
    for (job, identity) in [
        (&job, Some(&wrong_identity)),
        (&wrong_epoch, Some(&identity)),
        (&job, None),
    ] {
        let work = records::SharedDecodeBudget::default();
        assert_eq!(
            ledger::job_io_and_route_with_work(
                &db,
                job,
                identity,
                records::DecodeWork::shared(&work)
            )
            .await
            .unwrap(),
            ledger::JobAdmission::RouteDenied
        );
    }
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared("DELETE FROM pin_invocation_routes WHERE job_id='ordinary-route-job'")
        .await
        .unwrap();
    let work = records::SharedDecodeBudget::default();
    assert_eq!(
        ledger::job_io_and_route_with_work(
            &tx,
            &job,
            Some(&identity),
            records::DecodeWork::shared(&work)
        )
        .await
        .unwrap(),
        ledger::JobAdmission::RouteDenied
    );
    tx.rollback().await.unwrap();
    let work = records::SharedDecodeBudget::default();
    assert_eq!(
        ledger::job_io_and_route_with_work(
            &db,
            &job,
            Some(&identity),
            records::DecodeWork::shared(&work)
        )
        .await
        .unwrap(),
        ledger::JobAdmission::Allowed
    );
}

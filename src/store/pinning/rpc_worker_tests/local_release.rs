use super::*;
use crate::store::pinning::management::{
    native::{NativeKey, meter::Budget},
    records::locator,
};

#[tokio::test]
async fn undispatched_kubo_release_waits_for_last_reference_then_reuses_cid_without_io() {
    for strategy in [RpcStrategy::Cid, RpcStrategy::Upload, RpcStrategy::Car] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        let fixture = fixture(target.uri(), &source, RpcProfile::Kubo, strategy).await;
        fixture.publish("second", CID).await;
        let db = fixture.store.db();
        let leases = pin_lease::Entity::find().all(db).await.unwrap();
        assert_eq!(leases.len(), 2);
        for (index, lease) in leases.iter().enumerate() {
            let tx = db.begin().await.unwrap();
            leases::cancel_lease(&tx, &lease.id, Utc::now())
                .await
                .unwrap();
            if index == 0 {
                let remote =
                    remote_pin::Entity::find_by_id((fixture.provider.clone(), CID.to_owned()))
                        .one(&tx)
                        .await
                        .unwrap()
                        .unwrap();
                assert_eq!(
                    leases::complete_no_request_remote_absence(
                        &tx,
                        &fixture.provider,
                        CID,
                        remote.epoch,
                        Utc::now()
                    )
                    .await
                    .unwrap(),
                    leases::NoRequestRemoteCompletion::Stale
                );
            }
            tx.commit().await.unwrap();
        }
        fixture.drain().await;
        let usage = store_quota::read_usage(db, &fixture.provider)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (0, 0));
        let history = pin_resource_history::Entity::find().all(db).await.unwrap();
        assert_eq!(history.len(), 1);
        let fact: serde_json::Value = serde_json::from_str(&history[0].ledger).unwrap();
        assert_eq!(fact["local_reservation_released"], true);
        assert_ne!(fact["unpin_accepted"], true);
        assert_ne!(fact["confirmed_absent"], true);
        assert!(fixture.evidence().await.is_empty());
        fixture.publish("third", CID).await;
        fixture.assert_held().await;
        let remote = remote_pin::Entity::find_by_id((fixture.provider.clone(), CID.to_owned()))
            .one(db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.status, "reserved");
        assert!(remote.epoch > history[0].epoch);
        assert!(target.received_requests().await.unwrap().is_empty());
        assert!(source.received_requests().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn kubo_expiring_at_rate_gate_releases_only_the_undispatched_reservation() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    let fixture = fixture(target.uri(), &source, RpcProfile::Kubo, RpcStrategy::Cid).await;
    let db = fixture.store.db();
    let claim = fixture.claim().await.unwrap();
    let id = claim.model.id.clone();
    let runtime = fixture
        .coordinator
        .provider_runtime(&fixture.provider)
        .unwrap();
    let gate = runtime.next_request_at.lock().await;
    let expire = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while ledger::submission::latest_rpc(db, &id)
                .await
                .unwrap()
                .is_none()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Submit never recorded its invocation");
        pin_lease::Entity::update_many()
            .col_expr(pin_lease::Column::ExpiresAt, Expr::value(Utc::now()))
            .exec(db)
            .await
            .unwrap();
        drop(gate);
    };
    let (result, ()) = tokio::join!(fixture.run(claim), expire);
    result.unwrap();
    assert_eq!(fixture.evidence().await[0].outcome, "not_submitted");
    expire_worker_leases(&fixture.store, Utc::now(), &mut String::new(), 1)
        .await
        .unwrap();

    // An earlier call without a receipt, or with an actual effect, is still debt.
    for earlier in [None, Some("unknown"), Some("matched")] {
        let tx = db.begin().await.unwrap();
        tx.execute_unprepared("UPDATE pin_submit_observations SET submit_call=2,id=job_id||':call:2'; UPDATE pin_submit_history SET submit_calls=2").await.unwrap();
        if let Some(outcome) = earlier {
            tx.execute(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
                "INSERT INTO pin_submit_observations (id,job_id,submit_call,provider,expected_cid,route,remote_epoch,claim_until,effect,resources,outcome,needs_attention,started_at,observed_at) SELECT job_id||':call:1',job_id,1,provider,expected_cid,route,remote_epoch,claim_until,'observed','[]',$1,FALSE,started_at,observed_at FROM pin_submit_observations WHERE submit_call=2",
                [outcome.into()])).await.unwrap();
        }
        assert!(matches!(
            jobs::resolve_no_request_submit_ambiguity(&tx, &fixture.provider, CID, Utc::now())
                .await
                .unwrap(),
            jobs::NoRequestSubmitAmbiguity::Wait { .. }
        ));
        tx.rollback().await.unwrap();
    }
    fixture.drain().await;
    let usage = store_quota::read_usage(db, &fixture.provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (0, 0));
    let lease = pin_lease::Entity::find().one(db).await.unwrap().unwrap();
    assert_eq!(lease.state, "expired");
    let history = jobs::submission_history(db, &id).await.unwrap().unwrap();
    assert_eq!(history.submit_calls, 1);
    assert_eq!(history.effect, "not_created");
    let archive = pin_resource_history::Entity::find()
        .one(db)
        .await
        .unwrap()
        .unwrap();
    let fact: serde_json::Value = serde_json::from_str(&archive.ledger).unwrap();
    assert_eq!(fact["local_reservation_released"], true);
    assert_ne!(fact["unpin_accepted"], true);
    assert_ne!(fact["confirmed_absent"], true);
    assert!(target.received_requests().await.unwrap().is_empty());
    assert!(source.received_requests().await.unwrap().is_empty());

    expect_kubo_cid_pin(&target).await;
    fixture.publish("next", CID).await;
    fixture.drain().await;
    fixture.assert_held().await;
    assert!(
        fixture
            .evidence()
            .await
            .iter()
            .any(|row| row.outcome == "matched")
    );
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/rm"))
        .and(query_param("arg", CID))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins":[CID]})))
        .expect(1)
        .mount(&target)
        .await;
    let next = pin_lease::Entity::find()
        .filter(pin_lease::Column::State.eq("active"))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    let tx = db.begin().await.unwrap();
    leases::cancel_lease(&tx, &next.id, Utc::now())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    fixture.drain().await;
    let usage = store_quota::read_usage(db, &fixture.provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (0, 0));
}

#[tokio::test]
async fn undispatched_kubo_release_rejects_prior_call_or_changed_route() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    let fixture = fixture(target.uri(), &source, RpcProfile::Kubo, RpcStrategy::Cid).await;
    let db = fixture.store.db();
    let lease = pin_lease::Entity::find().one(db).await.unwrap().unwrap();
    let tx = db.begin().await.unwrap();
    leases::cancel_lease(&tx, &lease.id, Utc::now())
        .await
        .unwrap();
    jobs::resolve_no_request_submit_ambiguity(&tx, &fixture.provider, CID, Utc::now())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let claim = fixture.claim().await.unwrap();
    assert_eq!(claim.model.operation, "unpin");
    fixture.run(claim).await.unwrap();

    let remote = remote_pin::Entity::find_by_id((fixture.provider.clone(), CID.to_owned()))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    let tx = db.begin().await.unwrap();
    assert_eq!(
        store_quota::confirmed_release(&tx, &fixture.provider, CID, remote.epoch, Utc::now())
            .await
            .unwrap(),
        store_quota::ConfirmedReleaseOutcome::Released
    );
    tx.rollback().await.unwrap();

    for (case, change) in [
        (
            "unknown receipt",
            "INSERT INTO pin_submit_observations (id,job_id,submit_call,provider,expected_cid,route,remote_epoch,claim_until,effect,resources,outcome,needs_attention,started_at,observed_at) SELECT j.id||':call:1',j.id,1,j.provider,j.cid,c.route,c.remote_epoch,CURRENT_TIMESTAMP,'observed','[]','unknown',FALSE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM pin_jobs j JOIN pin_invocation_routes c ON j.id=c.job_id WHERE j.operation='submit'",
        ),
        (
            "retired registration",
            "UPDATE pin_provider_routes SET retired=TRUE",
        ),
        (
            "changed registration",
            "UPDATE pin_provider_routes SET snapshot=json_set(snapshot,'$.endpoint_revision',2)",
        ),
        ("missing ledger", "DELETE FROM remote_pin_ledger"),
    ] {
        let tx = db.begin().await.unwrap();
        assert_eq!(
            tx.execute_unprepared(change).await.unwrap().rows_affected(),
            1
        );
        if case == "retired registration" {
            assert!(matches!(
                store_quota::confirmed_release(
                    &tx,
                    &fixture.provider,
                    CID,
                    remote.epoch,
                    Utc::now()
                )
                .await,
                Err(AppError::InvalidPinningRequest(_))
            ));
        }
        if matches!(case, "retired registration" | "changed registration") {
            locator::rebuild_source(
                &tx,
                &NativeKey {
                    table: "pin_provider_routes".into(),
                    values: vec![fixture.provider.clone()],
                },
                &mut Budget::default(),
            )
            .await
            .unwrap();
        }
        let outcome =
            store_quota::confirmed_release(&tx, &fixture.provider, CID, remote.epoch, Utc::now())
                .await;
        assert!(
            matches!(outcome, Ok(store_quota::ConfirmedReleaseOutcome::Stale)),
            "local release must be stale with {case}: {outcome:?}"
        );
        tx.rollback().await.unwrap();
    }
    fixture.assert_held().await;
    assert!(
        pin_resource_history::Entity::find()
            .all(db)
            .await
            .unwrap()
            .is_empty()
    );
}

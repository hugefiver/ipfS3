#[path = "support/stage6_cluster_fixture.rs"]
mod fixture;
use fixture::Fixture;
use ipfs_s3_gateway::store::{
    entities::{
        pin_lease_requirement, pin_provider_usage, pin_replica_observation, pin_submit_observation,
        remote_pin, remote_pin_ledger,
    },
    pinning::ledger,
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn stage6_registered_worker_min_is_independent_of_allocation_rf() {
    for (min, rf, mode, outcome, complete) in [
        (1, 2, 0, "Complete", true),
        (1, 2, 1, "Complete", true),
        (3, 2, 0, "Insufficient", false),
        (1, -1, 1, "Complete", true),
        (3, -1, 0, "Insufficient", false),
    ] {
        let mut fixture = Fixture::configured("rest", false, "sqlite::memory:", |config| {
            config.pinning.providers[0]
                .cluster
                .as_mut()
                .unwrap()
                .min_replicas = min;
        })
        .await;
        fixture.replication_factor.store(rf, Ordering::SeqCst);
        fixture.mode.store(mode, Ordering::SeqCst);
        let lease = fixture.put("object").await;
        let before = pin_lease_requirement::Entity::find_by_id(lease.clone())
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        fixture.start();
        let poll = fixture.wait(outcome, 0).await;
        fixture.stop().await;
        assert!(poll.projectable, "min={min}, RF={rf}");
        let resources: Vec<ipfs_s3_gateway::pinning::provider::ObservedResource> =
            serde_json::from_str(&poll.resources).unwrap();
        let evidence = resources[0].replica_evidence.as_ref().unwrap();
        assert_eq!(evidence.required_replicas, min as usize);
        assert_eq!(evidence.allocation.replication_factor_min, rf);
        assert_eq!(
            evidence.allocation.replication_factor_max, rf,
            "RF is not rewritten"
        );
        assert_eq!(evidence.pinned_peers.len(), if mode == 0 { 2 } else { 1 });
        let status = ledger::lease_status(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            status.any_provider_available, complete,
            "min={min}, RF={rf}"
        );
        assert_eq!(status.all_targets_pinned, complete);
        eprintln!(
            "STAGE6_CORRECTION registered_worker min={min} rf={rf} pinned={} outcome={} available={} complete={} rf_unchanged=true birth_unchanged=true",
            evidence.pinned_peers.len(),
            poll.outcome,
            status.any_provider_available,
            status.all_targets_pinned
        );
        assert_eq!(
            pin_lease_requirement::Entity::find_by_id(lease)
                .one(fixture.state.store.db())
                .await
                .unwrap()
                .unwrap(),
            before
        );
    }
}

#[tokio::test]
async fn stage6_signed_one_put_freezes_retired_backup_without_allocating_it() {
    use ipfs_s3_gateway::store::entities::{pin_job, pin_lease_target, pin_provider_route};
    for ordinary in [false, true] {
        let mut fixture = Fixture::configured("rest", true, "sqlite::memory:", |config| {
            config.pinning.policies[0].provider_mode = "one".into();
            config.pinning_identity.providers[1].scope = "pinset-b".into();
            config.pinning_identity.providers[1].storage_domain = "backup-b".into();
            config.pinning_identity.providers[1].retired = true;
            if ordinary {
                for provider in &mut config.pinning.providers {
                    provider.endpoint = provider
                        .cluster
                        .as_ref()
                        .map(|options| options.rest_endpoint.clone());
                    provider.kind = "ipfs_rpc".into();
                    provider.cluster = None;
                }
                for identity in &mut config.pinning_identity.providers {
                    identity.backend = "kubo".into();
                    identity.api_profile = "kubo".into();
                }
                config.pinning_rpc = toml::from_str(
                    r#"
                    [[providers]]
                    config_name = "cluster-a"
                    profile = "kubo"
                    allow_private_network = true
                    [[providers]]
                    config_name = "cluster-b"
                    profile = "kubo"
                    allow_private_network = true
                "#,
                )
                .unwrap();
            }
        })
        .await;
        let routes = pin_provider_route::Entity::find()
            .all(fixture.state.store.db())
            .await
            .unwrap();
        let a = routes.iter().find(|route| !route.retired).unwrap();
        let b = routes.iter().find(|route| route.retired).unwrap();
        let lease = fixture.put("object").await;
        let birth_before = pin_lease_requirement::Entity::find_by_id(lease.clone())
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        let birth = pin_lease_requirement::contract::get(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(birth.candidates.len(), 2);
        assert_eq!(birth.candidate_order.len(), 2);
        assert_eq!(
            birth.candidates[&b.provider].as_ref().unwrap(),
            &serde_json::from_str::<ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot>(
                &b.snapshot
            )
            .unwrap()
        );
        fixture.start();
        let completion = tokio::time::timeout(std::time::Duration::from_secs(12), async {
            while !ledger::lease_status(fixture.state.store.db(), &lease)
                .await
                .unwrap()
                .unwrap()
                .all_targets_pinned
            {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        if let Err(error) = completion {
            panic!(
                "formal registered worker confirms only A (ordinary={ordinary}): {error}; jobs={:?}; remotes={:?}; observations={:?}; requests={:?}",
                pin_job::Entity::find()
                    .all(fixture.state.store.db())
                    .await
                    .unwrap(),
                remote_pin::Entity::find()
                    .all(fixture.state.store.db())
                    .await
                    .unwrap(),
                pin_submit_observation::Entity::find()
                    .all(fixture.state.store.db())
                    .await
                    .unwrap(),
                fixture.rest.received_requests().await.unwrap()
            );
        }
        fixture.stop().await;
        let db = fixture.state.store.db();
        let targets = pin_lease_target::Entity::find().all(db).await.unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].provider, a.provider);
        let remotes = remote_pin::Entity::find().all(db).await.unwrap();
        assert_eq!(remotes.len(), 1);
        assert_eq!(remotes[0].provider, a.provider);
        let submits = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .all(db)
            .await
            .unwrap();
        assert_eq!(submits.len(), 1);
        assert_eq!(submits[0].provider, a.provider);
        let observations = pin_submit_observation::Entity::find()
            .all(db)
            .await
            .unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].provider, a.provider);
        assert_eq!(observations[0].submit_call, 1);
        assert!(!observations[0].needs_attention);
        assert_eq!(
            observations[0].outcome,
            if ordinary { "matched" } else { "accepted" }
        );
        assert_eq!(
            pin_lease_requirement::Entity::find_by_id(lease)
                .one(db)
                .await
                .unwrap()
                .unwrap(),
            birth_before
        );
        assert_eq!(
            fixture
                .rest
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|request| request.method == "POST"
                    && (!ordinary || request.url.path() == "/api/v0/pin/add"))
                .count(),
            1
        );
        eprintln!(
            "STAGE6_CORRECTION signed_put=200 provider={} one_allocated=1 one_submitted=1 retired_allocated=0 retired_submitted=0 frozen_birth_candidates=2",
            if ordinary { "kubo_rpc" } else { "cluster_rest" }
        );
    }
}

#[tokio::test]
async fn stage6_birth_min_is_independent_per_lease_in_both_drift_directions() {
    use ipfs_s3_gateway::pinning::identity::ConfirmationRequirement;
    for (old_min, new_min, dispatched) in [(2, 3, true), (3, 2, true), (2, 3, false), (3, 2, false)]
    {
        let mut fixture = Fixture::configured("rest", false, "sqlite::memory:", |config| {
            config.pinning.providers[0]
                .cluster
                .as_mut()
                .unwrap()
                .min_replicas = old_min;
        })
        .await;
        let first = fixture.put("old-birth").await;
        let before = pin_lease_requirement::Entity::find_by_id(first.clone())
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        let after = if dispatched {
            fixture.start();
            let original = fixture
                .wait(
                    if old_min == 2 {
                        "Complete"
                    } else {
                        "Insufficient"
                    },
                    0,
                )
                .await;
            fixture.stop().await;
            original.sequence
        } else {
            0
        };
        fixture
            .reconfigure(|config| {
                config.pinning.providers[0]
                    .cluster
                    .as_mut()
                    .unwrap()
                    .min_replicas = new_min
            })
            .await;
        let second = fixture.put("new-birth").await;
        fixture.due().await;
        fixture.start();
        fixture
            .wait(
                if old_min == 2 {
                    "Complete"
                } else {
                    "Insufficient"
                },
                after,
            )
            .await;
        fixture.stop().await;
        for (lease, min) in [(&first, old_min), (&second, new_min)] {
            let birth = pin_lease_requirement::contract::get(fixture.state.store.db(), lease)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                birth
                    .candidates
                    .values()
                    .next()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .confirmation,
                Some(ConfirmationRequirement::ClusterReplicas {
                    min_replicas: min,
                    max_age_secs: 2
                })
            );
            let status = ledger::lease_status(fixture.state.store.db(), lease)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                status.any_provider_available,
                min == 2,
                "each birth evaluates its own min, not adapter min"
            );
            assert_eq!(status.all_targets_pinned, min == 2);
            assert_eq!(status.pinned_targets, usize::from(min == 2));
        }
        assert_eq!(
            pin_lease_requirement::Entity::find_by_id(first)
                .one(fixture.state.store.db())
                .await
                .unwrap()
                .unwrap(),
            before
        );
        assert_eq!(
            fixture
                .rest
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|r| r.method == "POST")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn stage6_more_permissive_birth_age_cannot_revive_the_original_reader_stale_proof() {
    let mut fixture = Fixture::new("rest", false, "sqlite::memory:").await;
    let first = fixture.put("first").await;
    fixture.start();
    fixture.wait("Complete", 0).await;
    fixture.stop().await;
    fixture
        .reconfigure(|config| {
            config.pinning.providers[0]
                .cluster
                .as_mut()
                .unwrap()
                .verifier_limits
                .max_age_secs = Some(30);
        })
        .await;
    let second = fixture.put("second").await;
    assert!(
        ledger::lease_status(fixture.state.store.db(), &second)
            .await
            .unwrap()
            .unwrap()
            .all_targets_pinned
    );
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    for lease in [first, second] {
        assert!(
            !ledger::lease_status(fixture.state.store.db(), &lease)
                .await
                .unwrap()
                .unwrap()
                .any_provider_available,
            "serialization or a later 30-second contract cannot extend the original reader's 2-second proof"
        );
    }
}

#[tokio::test]
async fn stage6_full_birth_candidates_include_disabled_and_quota_blocked() {
    for (disabled, blocked) in [(true, false), (false, true)] {
        let mut fixture = Fixture::configured("rest", true, "sqlite::memory:", |config| {
            let identity = &mut config.pinning_identity.providers[1];
            identity.scope = "pinset-b".into();
            identity.storage_domain = "cluster-b".into();
            config.pinning.providers[1].enabled = !disabled;
            if disabled {
                config.pinning.policies[0].provider_mode = "one".into();
            }
            if blocked {
                config.pinning.providers[1].max_bytes = 1;
            }
        })
        .await;
        let lease = fixture.put("object").await;
        let birth = pin_lease_requirement::contract::get(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(birth.candidates.len(), 2);
        assert_eq!(birth.candidate_order.len(), 2);
        assert_eq!(birth.provider_mode, if disabled { "one" } else { "all" });
        assert_eq!(birth.content_mode, "full");
        assert_eq!(birth.content_cids, [fixture::CID.to_string()].into());
        assert!(
            birth
                .candidates
                .values()
                .all(|route| route.as_ref().unwrap().confirmation.is_some())
        );
        fixture.start();
        fixture.wait("Complete", 0).await;
        fixture.stop().await;
        let status = ledger::lease_status(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap();
        assert!(status.any_provider_available);
        assert_eq!(
            status.all_targets_pinned, disabled,
            "All keeps blocked candidates; One succeeds within its full frozen set"
        );
    }
}

#[tokio::test]
async fn stage6_route_credential_endpoint_and_protocol_drift_park_without_http() {
    use ipfs_s3_gateway::store::entities::pin_job;
    for drift in ["credential", "endpoint", "protocol"] {
        let mut fixture = Fixture::new("rest", false, "sqlite::memory:").await;
        let lease = fixture.put("object").await;
        fixture.start();
        fixture.wait("Complete", 0).await;
        fixture.stop().await;
        let reads = fixture.rest.received_requests().await.unwrap().len();
        let proxy = fixture.proxy.uri();
        fixture
            .reconfigure(|config| match drift {
                "credential" => config.pinning_identity.providers[0].credential_revision += 1,
                "endpoint" => config.pinning_identity.providers[0].endpoint_revision += 1,
                _ => {
                    let options = config.pinning.providers[0].cluster.as_mut().unwrap();
                    options.protocol = "proxy".into();
                    options.proxy_endpoint = Some(proxy);
                    config.pinning_identity.providers[0].api_profile = "cluster-proxy".into();
                }
            })
            .await;
        fixture.due().await;
        fixture.start();
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            loop {
                if pin_job::Entity::find()
                    .filter(pin_job::Column::Operation.eq("poll"))
                    .filter(pin_job::Column::LastError.is_not_null())
                    .one(fixture.state.store.db())
                    .await
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("incompatible historical Poll parks");
        fixture.stop().await;
        let parked = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert!(
            parked
                .last_error
                .as_deref()
                .unwrap()
                .contains("needs_attention")
        );
        assert!(parked.locked_until.is_none());
        assert_eq!(
            fixture.rest.received_requests().await.unwrap().len(),
            reads,
            "incompatible route never borrows today's credentials"
        );
        assert!(
            !ledger::lease_status(fixture.state.store.db(), &lease)
                .await
                .unwrap()
                .unwrap()
                .any_provider_available
        );
    }
}

#[tokio::test]
async fn stage6_late_poll_cannot_project_across_an_archived_release_even_with_same_cid_id() {
    late_poll_archived_release(Fixture::new("rest", false, "sqlite::memory:").await).await;
}

#[tokio::test]
#[ignore = "requires explicit fresh isolated PostgreSQL database per exact test"]
async fn stage6_cluster_postgres_late_poll_cannot_project_new_lifetime() {
    late_poll_archived_release(Fixture::postgres().await).await;
}

async fn late_poll_archived_release(mut fixture: Fixture) {
    use sea_orm::{ConnectionTrait, QuerySelect, TransactionTrait, sea_query::Expr};
    fixture.put("object").await;
    fixture.mode.store(1, Ordering::SeqCst);
    fixture.start();
    let partial = fixture.wait("Insufficient", 0).await;
    fixture.stop().await;
    fixture.mode.store(0, Ordering::SeqCst);
    fixture.poll_delay_ms.store(500, Ordering::SeqCst);
    let reads = fixture.poll_reads.load(Ordering::SeqCst);
    fixture.due().await;
    fixture.start();
    fixture.wait_read(reads).await;
    let db = fixture.state.store.db().clone();
    let txn = db.begin().await.unwrap();
    if db.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
        use ipfs_s3_gateway::store::entities::pin_job;
        pin_job::Entity::find_by_id(partial.job_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .unwrap()
            .unwrap();
    }
    let old = remote_pin::Entity::find().one(&txn).await.unwrap().unwrap();
    // Simulate an independently recorded release/handoff while the actual GET
    // is in flight. This is a DB fence test, not a claim of physical unpin proof.
    ledger::archive_confirmed_release(&txn, &old).await.unwrap();
    remote_pin::Entity::update_many()
        .col_expr(remote_pin::Column::Epoch, Expr::value(old.epoch + 1))
        .col_expr(remote_pin::Column::Status, Expr::value("reserved"))
        .exec(&txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let late = fixture
        .wait_job("Complete", partial.sequence, Some(&partial.job_id))
        .await;
    fixture.stop().await;
    assert!(!late.projectable);
    assert_eq!(late.remote_epoch, partial.remote_epoch);
    assert_eq!(late.route, partial.route);
    let current = remote_pin::Entity::find().one(&db).await.unwrap().unwrap();
    assert_eq!(current.epoch, old.epoch + 1);
    assert_eq!(
        current.status, "reserved",
        "old Complete cannot project a later lifetime"
    );
    assert_eq!(
        current.request_id, old.request_id,
        "Cluster's CID-shaped id alone is not lifetime proof"
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
    // Append-only rows survived the stale-context path; no destructive repair.
    assert!(
        db.execute_unprepared("DELETE FROM pin_replica_observations")
            .await
            .is_err()
    );
    if db.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
        eprintln!(
            "STAGE6_PG late_poll_complete=retained projectable=false archived_epoch={} new_lifetime_epoch={} same_request_id=true",
            late.remote_epoch, current.epoch
        );
    }
}

#[tokio::test]
async fn stage6_migration_append_only_history_survives_signed_delete_and_tagging_seam() {
    use ipfs_s3_gateway::store::migrations::m20261004_000002_cluster_replica_contract::Migration;
    use sea_orm::{ConnectionTrait, TransactionTrait};
    use sea_orm_migration::{MigrationTrait, SchemaManager};
    let mut fixture = Fixture::new("rest", false, "sqlite::memory:").await;
    let mut headers = http::HeaderMap::new();
    headers.insert("x-amz-tagging", "audit=stage6".parse().unwrap());
    let lease = fixture.put_with_headers("object", headers).await;
    fixture.start();
    let poll = fixture.wait("Complete", 0).await;
    fixture.stop().await;
    let db = fixture.state.store.db();
    let birth = pin_lease_requirement::Entity::find_by_id(lease.clone())
        .one(db)
        .await
        .unwrap()
        .unwrap();
    let reads = fixture.rest.received_requests().await.unwrap().len();
    let tags = fixture
        .s3(reqwest::Method::GET, "object", &[("tagging", "")])
        .await;
    assert_eq!(tags.status(), reqwest::StatusCode::OK);
    let body = tags.text().await.unwrap();
    assert!(body.contains("<Key>audit</Key><Value>stage6</Value>"));
    assert_eq!(
        fixture.rest.received_requests().await.unwrap().len(),
        reads,
        "tagging is a read of intent, not replica I/O"
    );
    for table in ["pin_lease_requirements", "pin_replica_observations"] {
        for statement in [
            format!(
                "UPDATE {table} SET {} = {}",
                if table.ends_with("requirements") {
                    "contract"
                } else {
                    "resources"
                },
                if table.ends_with("requirements") {
                    "contract"
                } else {
                    "resources"
                }
            ),
            format!("DELETE FROM {table}"),
        ] {
            let txn = db.begin().await.unwrap();
            assert!(
                txn.execute_unprepared(&statement).await.is_err(),
                "append-only trigger: {statement}"
            );
            txn.rollback().await.unwrap();
        }
    }
    assert!(Migration.down(&SchemaManager::new(db)).await.is_err());
    let response = fixture.s3(reqwest::Method::DELETE, "object", &[]).await;
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(
        pin_lease_requirement::Entity::find_by_id(lease)
            .one(db)
            .await
            .unwrap()
            .unwrap(),
        birth
    );
    assert_eq!(
        pin_replica_observation::Entity::find_by_id(poll.sequence)
            .one(db)
            .await
            .unwrap()
            .unwrap(),
        poll
    );
    assert!(
        fixture
            .rest
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.method != "DELETE")
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .one(db)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1,
        "Unknown still occupies quota after S3 delete"
    );
}

#[tokio::test]
async fn stage6_registered_proxy_sigv4_worker_n_of_n_loss_and_expiry() {
    let mut fixture = Fixture::new("proxy", false, "sqlite::memory:").await;
    let lease = fixture.put("object").await;
    assert!(
        !ledger::lease_status(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );
    fixture.mode.store(1, Ordering::SeqCst);
    fixture.start();
    let partial = fixture.wait("Insufficient", 0).await;
    assert!(partial.projectable);
    assert!(
        !ledger::lease_status(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );
    fixture.mode.store(0, Ordering::SeqCst);
    fixture.due().await;
    let complete = fixture.wait("Complete", partial.sequence).await;
    let status = ledger::lease_status(fixture.state.store.db(), &lease)
        .await
        .unwrap()
        .unwrap();
    assert!(status.any_provider_available && status.all_targets_pinned);
    fixture.mode.store(2, Ordering::SeqCst);
    fixture.due().await;
    let queued = fixture.wait("Insufficient", complete.sequence).await;
    assert!(
        !ledger::lease_status(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );
    let mut after = queued.sequence;
    for mode in [3, 4, 5] {
        let reads = fixture.rest.received_requests().await.unwrap().len();
        fixture.mode.store(mode, Ordering::SeqCst);
        fixture.due().await;
        let unknown = fixture.wait("Unknown", after).await;
        assert!(unknown.sequence > after);
        assert!(
            fixture.rest.received_requests().await.unwrap().len() > reads,
            "each negative case performs new HTTP"
        );
        after = unknown.sequence;
        assert!(
            !ledger::lease_status(fixture.state.store.db(), &lease)
                .await
                .unwrap()
                .unwrap()
                .all_targets_pinned
        );
    }
    fixture.mode.store(0, Ordering::SeqCst);
    fixture.due().await;
    let latest = fixture.wait("Complete", after).await;
    fixture.stop().await;
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    assert!(
        !ledger::lease_status(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available,
        "expired historical facts are not live proof"
    );
    assert!(latest.resources.contains("pinned_peers"));
    let writes = fixture.proxy.received_requests().await.unwrap();
    assert_eq!(
        writes
            .iter()
            .filter(|r| r.url.path() == "/api/v0/pin/add")
            .count(),
        1,
        "no re-POST"
    );
    assert!(writes.iter().all(|r| !r.url.path().contains("rm")));
    let ledger = remote_pin_ledger::Entity::find()
        .one(fixture.state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ledger.ownership, "unknown");
}

#[tokio::test]
async fn stage6_late_poll_lost_claim_keeps_original_fact_without_projection() {
    use chrono::{Duration, Utc};
    use ipfs_s3_gateway::store::entities::{pin_invocation_route, pin_job};
    use sea_orm::sea_query::Expr;
    let mut fixture = Fixture::new("rest", false, "sqlite::memory:").await;
    let lease = fixture.put("object").await;
    fixture.mode.store(1, Ordering::SeqCst);
    fixture.start();
    let partial = fixture.wait("Insufficient", 0).await;
    fixture.stop().await;
    fixture.mode.store(0, Ordering::SeqCst);
    fixture.poll_delay_ms.store(500, Ordering::SeqCst);
    let reads = fixture.poll_reads.load(Ordering::SeqCst);
    fixture.due().await;
    fixture.start();
    fixture.wait_read(reads).await;
    let job = pin_job::Entity::find_by_id(partial.job_id.clone())
        .one(fixture.state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "running");
    let capture = pin_invocation_route::Entity::find_by_id(job.id.clone())
        .one(fixture.state.store.db())
        .await
        .unwrap()
        .unwrap();
    let replacement_claim = Utc::now() + Duration::hours(1);
    pin_job::Entity::update_many()
        .col_expr(pin_job::Column::LockedUntil, Expr::value(replacement_claim))
        .filter(pin_job::Column::Id.eq(&job.id))
        .exec(fixture.state.store.db())
        .await
        .unwrap();
    let late = fixture.wait("Complete", partial.sequence).await;
    fixture.stop().await;
    assert!(
        !late.projectable,
        "lost claim records facts, never live authorization"
    );
    assert_eq!(late.remote_epoch, capture.remote_epoch);
    assert_eq!(late.route, capture.route);
    assert_eq!(late.claim_until, job.locked_until.unwrap());
    assert!(
        !ledger::lease_status(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );
    let current = pin_job::Entity::find_by_id(job.id)
        .one(fixture.state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.locked_until, Some(replacement_claim));
    assert_eq!(current.state, "running");
}

#[tokio::test]
async fn stage6_registered_rest_same_pinset_aliases_share_one_quota_resource() {
    let mut fixture = Fixture::new("rest", true, "sqlite::memory:").await;
    let first = fixture.put("first").await;
    let second = fixture.put("second").await;
    fixture.start();
    let original = fixture.wait("Complete", 0).await;
    fixture.stop().await;
    fixture.due().await;
    fixture.start();
    let repeated = fixture
        .wait_job("Complete", original.sequence, Some(&original.job_id))
        .await;
    fixture.stop().await;
    assert_eq!(repeated.remote_epoch, original.remote_epoch);
    assert_eq!(repeated.expected_generation, original.expected_generation);
    assert_eq!(
        pin_replica_observation::Entity::find_by_id(original.sequence)
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap(),
        original
    );
    let usage = pin_provider_usage::Entity::find()
        .all(fixture.state.store.db())
        .await
        .unwrap();
    assert_eq!(usage.iter().map(|r| r.reserved_pins).sum::<i64>(), 1);
    assert_eq!(
        remote_pin::Entity::find()
            .all(fixture.state.store.db())
            .await
            .unwrap()
            .len(),
        1
    );
    for lease in [first, second] {
        let birth = pin_lease_requirement::contract::get(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            birth.candidates.len(),
            1,
            "same-domain aliases deduplicate candidates"
        );
        assert!(
            ledger::lease_status(fixture.state.store.db(), &lease)
                .await
                .unwrap()
                .unwrap()
                .all_targets_pinned
        );
    }
    assert_eq!(
        fixture
            .rest
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.method == "POST")
            .count(),
        1
    );
}

#[tokio::test]
async fn stage6_poll_projection_rollback_keeps_history_and_remote_atomic_then_recovers() {
    poll_projection_rollback(Fixture::new("rest", false, "sqlite::memory:").await).await;
}

#[tokio::test]
#[ignore = "requires explicit fresh isolated PostgreSQL database per exact test"]
async fn stage6_cluster_postgres_typed_receipt_projection_rollback_then_recovers() {
    poll_projection_rollback(Fixture::postgres().await).await;
}

async fn poll_projection_rollback(mut fixture: Fixture) {
    use chrono::{Duration, Utc};
    use ipfs_s3_gateway::store::entities::{pin_job, pin_lease_target};
    use sea_orm::{ConnectionTrait, sea_query::Expr};
    let lease = fixture.put("object").await;
    fixture.mode.store(1, Ordering::SeqCst);
    fixture.start();
    let partial = fixture.wait("Insufficient", 0).await;
    fixture.stop().await;
    let db = fixture.state.store.db().clone();
    let remote = remote_pin::Entity::find().one(&db).await.unwrap().unwrap();
    let target = pin_lease_target::Entity::find()
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let ledger_before = remote_pin_ledger::Entity::find()
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let postgres = db.get_database_backend() == sea_orm::DatabaseBackend::Postgres;
    if postgres {
        // Sequence increments survive rollback, so this proves the real worker
        // reached the injected projection error (not merely a cancelled GET).
        db.execute_unprepared("CREATE SEQUENCE stage6_projection_abort_calls")
            .await
            .unwrap();
        db.execute_unprepared("CREATE FUNCTION stage6_projection_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status = 'pinned' THEN PERFORM nextval('stage6_projection_abort_calls'); RAISE EXCEPTION 'stage6 injected projection rollback'; END IF; RETURN NEW; END; $$").await.unwrap();
        db.execute_unprepared("CREATE TRIGGER stage6_projection_abort BEFORE UPDATE OF status ON remote_pins FOR EACH ROW EXECUTE FUNCTION stage6_projection_abort()").await.unwrap();
    } else {
        db.execute_unprepared("CREATE TRIGGER stage6_projection_abort BEFORE UPDATE OF status ON remote_pins WHEN NEW.status='pinned' BEGIN SELECT RAISE(ABORT, 'stage6 injected projection rollback'); END").await.unwrap();
    }
    let reads = fixture.poll_reads.load(Ordering::SeqCst);
    fixture.mode.store(0, Ordering::SeqCst);
    fixture.due().await;
    fixture.start();
    fixture.wait_read(reads).await;
    if postgres {
        tokio::time::timeout(std::time::Duration::from_secs(12), async {
            loop {
                let row = db
                    .query_one(sea_orm::Statement::from_string(
                        sea_orm::DatabaseBackend::Postgres,
                        "SELECT is_called FROM stage6_projection_abort_calls",
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                if row.try_get::<bool>("", "is_called").unwrap() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("actual typed Complete reached PG projection rollback trigger");
    }
    fixture.stop().await;
    assert!(
        pin_replica_observation::Entity::find()
            .filter(pin_replica_observation::Column::Sequence.gt(partial.sequence))
            .one(&db)
            .await
            .unwrap()
            .is_none(),
        "a failed projection transaction cannot publish its receipt"
    );
    assert_eq!(
        remote_pin::Entity::find().one(&db).await.unwrap().unwrap(),
        remote
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        target
    );
    assert_eq!(
        remote_pin_ledger::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        ledger_before
    );
    assert!(
        !ledger::lease_status(&db, &lease)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );
    db.execute_unprepared(if postgres {
        "DROP TRIGGER stage6_projection_abort ON remote_pins"
    } else {
        "DROP TRIGGER stage6_projection_abort"
    })
    .await
    .unwrap();
    if postgres {
        db.execute_unprepared("DROP FUNCTION stage6_projection_abort()")
            .await
            .unwrap();
        db.execute_unprepared("DROP SEQUENCE stage6_projection_abort_calls")
            .await
            .unwrap();
    }
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Utc::now() - Duration::seconds(1)),
        )
        .filter(pin_job::Column::Id.eq(&partial.job_id))
        .exec(&db)
        .await
        .unwrap();
    fixture.start();
    fixture.wait("Complete", partial.sequence).await;
    fixture.stop().await;
    assert!(
        ledger::lease_status(&db, &lease)
            .await
            .unwrap()
            .unwrap()
            .all_targets_pinned
    );
    if postgres {
        eprintln!(
            "STAGE6_PG typed_complete_projection_trigger=reached receipt_remote_target_ledger=rolled_back fresh_poll=recovered"
        );
    }
}

#[tokio::test]
async fn stage6_healthy_poll_owner_epoch_handoff_keeps_late_fact_and_fresh_new_poll() {
    healthy_poll_owner_handoff(Fixture::new("rest", false, "sqlite::memory:").await).await;
}

#[tokio::test]
#[ignore = "requires explicit fresh isolated PostgreSQL database per exact test"]
async fn stage6_cluster_postgres_late_poll_owner_handoff_fresh_successor() {
    healthy_poll_owner_handoff(Fixture::postgres().await).await;
}

async fn healthy_poll_owner_handoff(mut fixture: Fixture) {
    use ipfs_s3_gateway::store::entities::{pin_invocation_route, pin_job};
    use sea_orm::ConnectionTrait;
    let first = fixture.put("first").await;
    fixture.start();
    let original = fixture.wait("Complete", 0).await;
    fixture.stop().await;
    let second = fixture.put("second").await;
    fixture.poll_delay_ms.store(500, Ordering::SeqCst);
    let reads = fixture.poll_reads.load(Ordering::SeqCst);
    fixture.due().await;
    fixture.start();
    fixture.wait_read(reads).await;
    let capture = pin_invocation_route::Entity::find_by_id(original.job_id.clone())
        .one(fixture.state.store.db())
        .await
        .unwrap()
        .unwrap();
    let response = fixture.s3(reqwest::Method::DELETE, "first", &[]).await;
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
    let late = fixture
        .wait_job("Complete", original.sequence, Some(&original.job_id))
        .await;
    assert!(
        !late.projectable,
        "cancelled owner does not authorize current-owner projection"
    );
    assert_eq!(late.remote_epoch, capture.remote_epoch);
    assert_eq!(late.route, capture.route);
    let new = fixture.wait("Complete", late.sequence).await;
    fixture.stop().await;
    assert!(new.projectable);
    assert_eq!(new.lease_id.as_deref(), Some(second.as_str()));
    assert!(new.remote_epoch > capture.remote_epoch);
    assert_ne!(new.job_id, original.job_id);
    assert!(
        ledger::lease_status(fixture.state.store.db(), &second)
            .await
            .unwrap()
            .unwrap()
            .all_targets_pinned
    );
    if let Some(status) = ledger::lease_status(fixture.state.store.db(), &first)
        .await
        .unwrap()
    {
        assert!(!status.any_provider_available);
    }
    let job = pin_job::Entity::find_by_id(new.job_id)
        .one(fixture.state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        job.state, "pending",
        "healthy successor keeps bounded read-only work"
    );
    assert_eq!(
        fixture
            .rest
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.method == "POST")
            .count(),
        1
    );
    if fixture.state.store.db().get_database_backend() == sea_orm::DatabaseBackend::Postgres {
        eprintln!(
            "STAGE6_PG owner_handoff_late_complete=retained projectable=false new_poll=projectable new_job=true epoch_advanced=true provider_posts=1"
        );
    }
}

#[tokio::test]
async fn stage6_one_convergence_retains_unknown_debt_and_hands_off_winning_poll_generation() {
    use chrono::Utc;
    use ipfs_s3_gateway::store::{
        entities::{pin_job, pin_lease, pin_lease_target},
        pinning::leases,
    };
    use sea_orm::TransactionTrait;
    let mut fixture = Fixture::configured("rest", true, "sqlite::memory:", |config| {
        config.pinning.policies[0].provider_mode = "one".into();
        config.pinning.providers[0]
            .cluster
            .as_mut()
            .unwrap()
            .min_replicas = 3;
        config.pinning.providers[1].priority = 2;
        config.pinning_identity.providers[1].scope = "pinset-b".into();
        config.pinning_identity.providers[1].storage_domain = "cluster-b".into();
    })
    .await;
    let lease = fixture.put("object").await;
    fixture.start();
    let insufficient = fixture.wait("Insufficient", 0).await;
    fixture.stop().await;
    let db = fixture.state.store.db().clone();
    let born = pin_lease_requirement::contract::get(&db, &lease)
        .await
        .unwrap()
        .unwrap();
    let txn = db.begin().await.unwrap();
    let replacement = leases::fail_one_target(
        &txn,
        &lease,
        insufficient.target_id.as_ref().unwrap(),
        &born.candidate_order,
        fixture.state.pinning.provider_limits(),
        Utc::now(),
    )
    .await
    .unwrap()
    .unwrap();
    txn.commit().await.unwrap();
    fixture.due().await;
    fixture.start();
    let complete = fixture.wait("Complete", insufficient.sequence).await;
    let generation = tokio::time::timeout(std::time::Duration::from_secs(8), async {
        loop {
            let generation = pin_lease::Entity::find_by_id(lease.clone())
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .generation;
            if generation > complete.expected_generation.unwrap() {
                break generation;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("One convergence advances lease generation");
    let successor = fixture.wait("Complete", complete.sequence).await;
    fixture.stop().await;
    assert_eq!(successor.expected_generation, Some(generation));
    assert_ne!(successor.job_id, complete.job_id);
    assert_eq!(
        successor.target_id.as_deref(),
        Some(replacement.id.as_str())
    );
    assert!(
        ledger::lease_status(&db, &lease)
            .await
            .unwrap()
            .unwrap()
            .all_targets_pinned
    );
    let targets = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(&lease))
        .all(&db)
        .await
        .unwrap();
    assert_eq!(
        targets
            .iter()
            .filter(|target| target.state == "pinned")
            .count(),
        1
    );
    assert_eq!(
        targets
            .iter()
            .filter(|target| target.state == "released")
            .count(),
        1
    );
    let poll = pin_job::Entity::find_by_id(successor.job_id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(poll.state, "pending");
    assert_eq!(
        pin_provider_usage::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .iter()
            .map(|usage| usage.reserved_pins)
            .sum::<i64>(),
        2
    );
    assert!(
        fixture
            .rest
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method != "DELETE")
    );
}

#[tokio::test]
async fn stage6_registered_unknown_or_multiple_write_roots_remain_operator_debt() {
    for mode in [1, 2, 3] {
        let mut fixture = Fixture::new("proxy", false, "sqlite::memory:").await;
        fixture.write_mode.store(mode, Ordering::SeqCst);
        let lease = fixture.put("object").await;
        fixture.start();
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            loop {
                if pin_submit_observation::Entity::find()
                    .filter(pin_submit_observation::Column::ObservedAt.is_not_null())
                    .one(fixture.state.store.db())
                    .await
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        fixture.stop().await;
        let evidence = pin_submit_observation::Entity::find()
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert!(evidence.needs_attention);
        assert_ne!(evidence.outcome, "accepted");
        assert!(
            !ledger::lease_status(fixture.state.store.db(), &lease)
                .await
                .unwrap()
                .unwrap()
                .any_provider_available
        );
        assert!(
            pin_replica_observation::Entity::find()
                .filter(pin_replica_observation::Column::Phase.eq("poll"))
                .one(fixture.state.store.db())
                .await
                .unwrap()
                .is_none(),
            "unknown root cannot use expected-CID GET to wash footprint"
        );
        assert_eq!(fixture.proxy.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
#[ignore = "requires explicit fresh isolated PostgreSQL database per exact test"]
async fn stage6_cluster_postgres_replica_writer_locks_lease_before_target_remote_and_route() {
    use ipfs_s3_gateway::store::entities::{pin_lease, pin_lease_target, pin_provider_route};
    use sea_orm::{ConnectionTrait, DatabaseBackend, QuerySelect, Statement, TransactionTrait};
    let mut fixture = Fixture::postgres().await;
    let lease = fixture.put("object").await;
    fixture.mode.store(1, Ordering::SeqCst);
    fixture.start();
    let partial = fixture.wait("Insufficient", 0).await;
    fixture.stop().await;
    let db = fixture.state.store.db().clone();
    let holder = db.begin().await.unwrap();
    pin_lease::Entity::find_by_id(lease.clone())
        .lock_exclusive()
        .one(&holder)
        .await
        .unwrap()
        .unwrap();
    let holder_pid: i32 = holder
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT pg_backend_pid() AS pid",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "pid")
        .unwrap();
    let reads = fixture.poll_reads.load(Ordering::SeqCst);
    fixture.mode.store(0, Ordering::SeqCst);
    fixture.due().await;
    fixture.start();
    fixture.wait_read(reads).await;
    let blocked_query = tokio::time::timeout(std::time::Duration::from_secs(12), async {
        loop {
            if let Some(row) = db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres,
                "SELECT query FROM pg_stat_activity WHERE datname = current_database() AND $1 = ANY(pg_blocking_pids(pid)) AND wait_event_type = 'Lock' AND query LIKE '%pin_leases%'",
                [holder_pid.into()])).await.unwrap() {
                break row.try_get::<String>("", "query").unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }).await.expect("real registered replica writer must wait on the held lease frontier");
    // Blocking PID proves the row lock even if pg_stat_activity truncates the
    // SQL before its trailing FOR UPDATE clause. No product timeout is changed.
    // A competing writer can lock all downstream rows NOWAIT while the replica
    // writer is blocked on the lease. This observes native PG lock ordering,
    // rather than only checking generated SQL or racing a timed sleep.
    let probe = db.begin().await.unwrap();
    for sql in [
        "SELECT id FROM pin_lease_targets FOR UPDATE NOWAIT",
        "SELECT provider FROM remote_pins FOR UPDATE NOWAIT",
        "SELECT provider FROM pin_provider_usage FOR UPDATE NOWAIT",
        "SELECT provider FROM pin_provider_routes FOR UPDATE NOWAIT",
    ] {
        let rows = probe
            .query_all(Statement::from_string(DatabaseBackend::Postgres, sql))
            .await
            .unwrap_or_else(|error| panic!("downstream lock taken before lease: {sql}: {error}"));
        assert!(!rows.is_empty(), "probe must lock an actual row: {sql}");
    }
    probe.commit().await.unwrap();
    holder.commit().await.unwrap();
    let fresh = fixture
        .wait_job("Complete", partial.sequence, Some(&partial.job_id))
        .await;
    fixture.stop().await;
    assert!(fresh.projectable);
    assert_eq!(fresh.lease_id.as_deref(), Some(lease.as_str()));
    assert_eq!(
        pin_lease_target::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .state,
        "pinned"
    );
    assert!(
        pin_provider_route::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    eprintln!(
        "STAGE6_PG replica_writer_blocked_on_lease=true downstream_target_remote_usage_route_nowait=acquired receipt_projectable=true blocked_query={blocked_query}"
    );
}

#[tokio::test]
#[ignore = "requires explicit isolated PostgreSQL database selection"]
async fn stage6_cluster_postgres_registered_claim_and_replica_contract() {
    use chrono::{Duration, Timelike, Utc};
    use ipfs_s3_gateway::store::migrations::m20261004_000002_cluster_replica_contract::Migration;
    use ipfs_s3_gateway::store::{entities::pin_job, pinning::jobs};
    use sea_orm::{ConnectionTrait, DatabaseBackend, TransactionTrait, sea_query::Expr};
    use sea_orm_migration::{MigrationTrait, SchemaManager};
    let database = std::env::var("IPFS_S3_STAGE6_TEST_DATABASE_URL")
        .expect("explicit isolated PostgreSQL URL required");
    assert!(
        database.starts_with("postgres://") || database.starts_with("postgresql://"),
        "no SQLite fallback"
    );
    let preflight = ipfs_s3_gateway::store::connect_database(&database)
        .await
        .unwrap_or_else(|_| panic!("cannot connect to selected isolated PostgreSQL"));
    assert_eq!(preflight.get_database_backend(), DatabaseBackend::Postgres);
    if SchemaManager::new(&preflight)
        .has_table("pin_jobs")
        .await
        .unwrap()
    {
        assert!(
            pin_job::Entity::find()
                .one(&preflight)
                .await
                .unwrap()
                .is_none(),
            "requires fresh isolated Stage6 scope; preserve existing jobs and Unknown debt, do not reuse a failed fixture"
        );
    }
    preflight.close().await.unwrap();
    let mut fixture = Fixture::new("rest", false, &database).await;
    let mut headers = http::HeaderMap::new();
    headers.insert("x-amz-tagging", "audit=stage6".parse().unwrap());
    let lease = fixture.put_with_headers("object", headers).await;
    fixture.start();
    let poll = fixture.wait("Complete", 0).await;
    fixture.stop().await;
    assert!(poll.projectable);
    assert_eq!(poll.lease_id.as_deref(), Some(lease.as_str()));
    assert!(
        ledger::lease_status(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap()
            .all_targets_pinned
    );
    eprintln!(
        "STAGE6_PG registry=actual_appstate_no_fixture_override signed_put=200 registered_worker_poll={} outcome={} projectable={} lease={}",
        poll.sequence, poll.outcome, poll.projectable, lease
    );
    // Use an actual renewed nanosecond caller token. PG persists microseconds;
    // both fences must compare natively, not Rust equality or a time tolerance.
    let db = fixture.state.store.db();
    assert_eq!(db.get_database_backend(), DatabaseBackend::Postgres);
    let birth = pin_lease_requirement::Entity::find_by_id(lease.clone())
        .one(db)
        .await
        .unwrap()
        .unwrap();
    let claimed = jobs::claim_due_jobs(
        db,
        Utc::now() + Duration::seconds(60),
        Duration::seconds(30),
        10,
    )
    .await
    .unwrap();
    let claim = claimed
        .iter()
        .find(|job| job.model.id == poll.job_id)
        .expect("registered healthy Poll is reclaimable");
    let old = claim.model.locked_until.unwrap();
    let new = (Utc::now() + Duration::hours(1))
        .with_nanosecond(123_456_789)
        .unwrap();
    let txn = db.begin().await.unwrap();
    assert!(
        jobs::fence_job_claim(&txn, &poll.job_id, old)
            .await
            .unwrap()
    );
    let renewed = jobs::renew_job_claim(&txn, &poll.job_id, old, new, Utc::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        renewed, new,
        "exercise the native fence with the original nanosecond caller token"
    );
    let persisted = pin_job::Entity::find_by_id(poll.job_id.clone())
        .one(&txn)
        .await
        .unwrap()
        .unwrap()
        .locked_until
        .unwrap();
    assert_eq!(
        persisted,
        new.with_nanosecond(123_456_000).unwrap(),
        "PG stores the exact microsecond representation"
    );
    assert_ne!(
        persisted, renewed,
        "Rust equality would reject this valid caller claim"
    );
    assert!(
        jobs::fence_job_claim(&txn, &poll.job_id, renewed)
            .await
            .unwrap()
    );
    assert!(
        !jobs::fence_job_claim(&txn, &poll.job_id, old)
            .await
            .unwrap()
    );
    eprintln!(
        "STAGE6_PG native_fence caller={} persisted={} current=true previous=false",
        renewed, persisted
    );
    txn.rollback().await.unwrap();
    let txn = db.begin().await.unwrap();
    assert!(
        jobs::fence_job_claim(&txn, &poll.job_id, old)
            .await
            .unwrap(),
        "rollback restores exact prior claim"
    );
    txn.rollback().await.unwrap();
    assert_eq!(
        pin_job::Entity::find_by_id(poll.job_id.clone())
            .one(db)
            .await
            .unwrap()
            .unwrap()
            .locked_until,
        Some(old)
    );
    eprintln!("STAGE6_PG claim_rollback exact_previous_token_restored={old}");
    for delete in [false, true] {
        let txn = db.begin().await.unwrap();
        let result = if delete {
            pin_replica_observation::Entity::delete_by_id(poll.sequence)
                .exec(&txn)
                .await
                .map(|_| ())
        } else {
            pin_replica_observation::Entity::update_many()
                .col_expr(
                    pin_replica_observation::Column::Resources,
                    Expr::value("[]"),
                )
                .filter(pin_replica_observation::Column::Sequence.eq(poll.sequence))
                .exec(&txn)
                .await
                .map(|_| ())
        };
        let error = result.expect_err("PG immutable replica history trigger");
        assert!(
            error.to_string().contains("immutable pin history"),
            "must fail at the PG trigger: {error}"
        );
        txn.rollback().await.unwrap();
        let txn = db.begin().await.unwrap();
        let result = if delete {
            pin_lease_requirement::Entity::delete_by_id(lease.clone())
                .exec(&txn)
                .await
                .map(|_| ())
        } else {
            pin_lease_requirement::Entity::update_many()
                .col_expr(pin_lease_requirement::Column::Contract, Expr::value("{}"))
                .filter(pin_lease_requirement::Column::LeaseId.eq(&lease))
                .exec(&txn)
                .await
                .map(|_| ())
        };
        let error = result.expect_err("PG immutable birth contract trigger");
        assert!(
            error.to_string().contains("immutable pin history"),
            "must fail at the PG trigger: {error}"
        );
        txn.rollback().await.unwrap();
    }
    let error = Migration.down(&SchemaManager::new(db)).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Cluster history downgrade loses birth and replica evidence")
    );
    eprintln!(
        "STAGE6_PG migration=actual_appstate birth_and_replica_update_delete=rejected downgrade=rejected"
    );
    let reads = fixture.rest.received_requests().await.unwrap().len();
    let tags = fixture
        .s3(reqwest::Method::GET, "object", &[("tagging", "")])
        .await;
    assert_eq!(tags.status(), reqwest::StatusCode::OK);
    assert!(
        tags.text()
            .await
            .unwrap()
            .contains("<Key>audit</Key><Value>stage6</Value>")
    );
    assert_eq!(
        fixture.rest.received_requests().await.unwrap().len(),
        reads,
        "signed tagging reads intent without provider I/O"
    );
    let response = fixture.s3(reqwest::Method::DELETE, "object", &[]).await;
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(
        pin_lease_requirement::Entity::find_by_id(lease)
            .one(db)
            .await
            .unwrap()
            .unwrap(),
        birth
    );
    assert_eq!(
        pin_replica_observation::Entity::find_by_id(poll.sequence)
            .one(db)
            .await
            .unwrap()
            .unwrap(),
        poll
    );
    let requests = fixture.rest.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST")
            .count(),
        1,
        "no repeated provider submit"
    );
    assert!(requests.iter().all(|request| request.method != "DELETE"));
    assert_eq!(
        remote_pin_ledger::Entity::find()
            .one(db)
            .await
            .unwrap()
            .unwrap()
            .ownership,
        "unknown"
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .one(db)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1,
        "S3 delete cannot erase Unknown debt"
    );
    eprintln!(
        "STAGE6_PG signed_tagging=200 signed_delete=204 immutable_history=preserved provider_posts=1 provider_deletes=0 ownership=unknown reserved_pins=1"
    );
}

mod stage9_zip_support;

use ipfs_s3_gateway::store::{
    self,
    entities::{
        object, object_version, pin_invocation_route, pin_job, pin_lease, pin_lease_target,
        pin_provider_usage, pin_replica_observation, pin_resource_history, pin_submit_observation,
        remote_pin, remote_pin_ledger, standard_mutation_lease,
    },
    pinning::jobs,
};
use sea_orm::{ConnectionTrait, EntityTrait, PaginatorTrait};
use stage9_zip_support::{config, prepared, publish, setup};

async fn rejects_settlement_mutation(mutation: &str) {
    let db = setup().await;
    let config = config();
    let (request, batch, capture, guard) = prepared(&db, &config, vec![]).await;
    let before = store::zip::snapshot(&db, "batch").await.unwrap().unwrap();
    db.execute_unprepared(&format!(
        "CREATE TRIGGER mutate_pin_settlement AFTER DELETE ON standard_mutation_leases BEGIN {mutation} END"
    ))
    .await
    .unwrap();
    let result =
        stage9_zip_support::publish_captured(&db, &config, request, batch, &capture, guard).await;
    assert!(
        matches!(
            result,
            Err(ipfs_s3_gateway::error::AppError::InvalidPinningRequest(_))
        ),
        "settlement mutation must reach final pin validation: {mutation}; got {result:?}"
    );
    assert_eq!(object::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(
        pin_lease_target::Entity::find().count(&db).await.unwrap(),
        0
    );
    assert_eq!(pin_job::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(
        pin_invocation_route::Entity::find()
            .count(&db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        pin_provider_usage::Entity::find().count(&db).await.unwrap(),
        0
    );
    assert_eq!(remote_pin::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(
        remote_pin_ledger::Entity::find().count(&db).await.unwrap(),
        0
    );
    assert_eq!(jobs::history::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(
        pin_submit_observation::Entity::find()
            .count(&db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        pin_resource_history::Entity::find()
            .count(&db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        pin_replica_observation::Entity::find()
            .count(&db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        standard_mutation_lease::Entity::find()
            .count(&db)
            .await
            .unwrap(),
        1
    );
    let after = store::zip::snapshot(&db, "batch").await.unwrap().unwrap();
    assert_eq!(after.batch, before.batch);
    assert_eq!(after.entries, before.entries);
}

#[tokio::test]
async fn extra_owner_lease_at_final_settlement_rolls_back() {
    rejects_settlement_mutation(
        "INSERT INTO pin_leases(id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state)
         SELECT 'injected-lease',owner_object_id,CASE WHEN source='manual' THEN 'automatic' ELSE 'manual' END,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state
         FROM pin_leases LIMIT 1;",
    )
    .await;
}

#[tokio::test]
async fn foreign_owner_desired_target_at_final_settlement_rolls_back() {
    rejects_settlement_mutation(
        "INSERT INTO pin_leases(id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state)
         SELECT 'injected-lease','archive',source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state
         FROM pin_leases LIMIT 1;
         INSERT INTO pin_lease_targets(id,lease_id,provider,cid,logical_size,state,created_at,last_touched_at)
         SELECT 'injected-target','injected-lease',provider,cid,logical_size,'waiting',created_at,last_touched_at
         FROM pin_lease_targets LIMIT 1;",
    )
    .await;
}

#[tokio::test]
async fn unpinned_archive_owner_quota_waiting_target_at_final_settlement_rolls_back() {
    rejects_settlement_mutation(
        "INSERT INTO pin_leases(id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state)
         SELECT 'injected-archive-lease','archive',source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,'active'
         FROM pin_leases LIMIT 1;
         INSERT INTO pin_lease_targets(id,lease_id,provider,cid,logical_size,state,created_at,last_touched_at)
         SELECT 'injected-archive-target','injected-archive-lease',provider,'archive-cid',logical_size,'quota_waiting',created_at,last_touched_at
         FROM pin_lease_targets LIMIT 1;",
    )
    .await;
}

#[tokio::test]
async fn extra_target_at_final_settlement_rolls_back() {
    rejects_settlement_mutation(
        "INSERT INTO pin_lease_targets(id,lease_id,provider,cid,logical_size,state,created_at,last_touched_at)
         SELECT 'injected-target',lease_id,provider,'injected-cid',logical_size,'waiting',created_at,last_touched_at
         FROM pin_lease_targets LIMIT 1;",
    )
    .await;
}

#[tokio::test]
async fn unknown_submit_history_at_final_settlement_rolls_back() {
    rejects_settlement_mutation(
        "INSERT INTO pin_submit_history(job_id,correlation,api,strategy,effect,state,first_error,last_error,submit_calls,recovery_queries,started_at)
         SELECT id,NULL,'psa','standard','unknown','needs_attention',NULL,NULL,1,0,created_at
         FROM pin_jobs WHERE operation='submit' LIMIT 1;",
    )
    .await;
}

#[tokio::test]
async fn extra_resource_job_at_final_settlement_rolls_back() {
    rejects_settlement_mutation(
        "INSERT INTO pin_jobs(id,provider,cid,operation,lease_id,target_id,expected_generation,expected_remote_epoch,state,attempts,next_attempt_at,locked_until,submit_phase,created_at,updated_at,last_error)
         SELECT 'injected-job',provider,cid,operation,lease_id,target_id,expected_generation,expected_remote_epoch,state,attempts,next_attempt_at,locked_until,submit_phase,created_at,updated_at,last_error
         FROM pin_jobs LIMIT 1;",
    )
    .await;
}

#[tokio::test]
async fn resource_release_history_at_final_settlement_rolls_back() {
    rejects_settlement_mutation(
        "INSERT INTO pin_resource_history(provider,cid,epoch,ledger)
         SELECT provider,cid,1,'{}' FROM remote_pin_ledger LIMIT 1;",
    )
    .await;
}

#[tokio::test]
async fn unknown_submission_evidence_at_final_settlement_rolls_back() {
    rejects_settlement_mutation(
        "INSERT INTO pin_submit_observations(id,job_id,submit_call,provider,expected_cid,route,remote_epoch,claim_until,effect,resources,outcome,safe_error,needs_attention,started_at,observed_at)
         SELECT 'injected-evidence','historical-job',1,provider,'historical-cid',route,1,CURRENT_TIMESTAMP,'unknown','[]','unknown',NULL,1,CURRENT_TIMESTAMP,NULL
         FROM remote_pin_ledger LIMIT 1;",
    )
    .await;
}

#[tokio::test]
async fn replica_observation_at_final_settlement_rolls_back() {
    rejects_settlement_mutation(
        "INSERT INTO pin_replica_observations(job_id,provider,expected_cid,route,remote_epoch,claim_until,lease_id,target_id,expected_generation,phase,resources,outcome,safe_error,projectable,observed_at)
         SELECT 'historical-job',provider,cid,route,1,CURRENT_TIMESTAMP,NULL,NULL,NULL,'poll','[]','Unknown',NULL,0,CURRENT_TIMESTAMP
         FROM remote_pin_ledger LIMIT 1;",
    )
    .await;
}

#[tokio::test]
async fn expected_new_rpc_reservation_keeps_final_submission_barrier_clear() {
    use ipfs_s3_gateway::{
        config::Config,
        pinning::{config::ValidatedPinningConfig, coordinator::normalize_validated_config},
        store::pinning::ledger::submission::{AdmissionBarrier, admission_barrier},
    };
    let raw: Config = toml::from_str(
        r#"
        [decompress_zip]
        auto_pin_outputs = true
        [[pinning.providers]]
        name = 'remote'
        kind = 'ipfs_rpc'
        api = 'rpc'
        endpoint = 'https://rpc.example.invalid'
        strategy = 'cid'
        token_env = 'TEST_RPC_SECRET'
        enabled = true
        priority = 1
        max_bytes = 1000
        max_pins = 100
        [[pinning_rpc.providers]]
        config_name = 'remote'
        profile = 'kubo'
        auth = 'bearer'
        [pinning_identity]
        primary_storage_domain = 'local'
        [[pinning_identity.providers]]
        config_name = 'remote'
        provider_id = 'remote'
        display_name = 'RPC settlement fixture'
        backend = 'kubo'
        scope = 'isolated-node'
        storage_domain = 'remote'
        credential_revision = 1
        endpoint_revision = 1
        secret_ref = 'env:TEST_RPC_SECRET'
        api_profile = 'kubo'
        strategy = 'cid'
        cleanup = 'managed'
        [[pinning.policies]]
        bucket = 'bucket'
        prefix = 'out/'
        trigger = 'always'
        provider_mode = 'all'
        providers = ['remote']
        default_duration = '1h'
        max_duration = '2h'
        allow_decompressed = true
    "#,
    )
    .unwrap();
    let config = normalize_validated_config(
        ValidatedPinningConfig::from_config(&raw, |_| Some("test-token".into())).unwrap(),
    )
    .unwrap();
    let db = setup().await;
    let cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    let (request, batch, capture, guard) = stage9_zip_support::prepared_with_outputs(
        &db,
        &config,
        vec![],
        vec![("a".into(), stage9_zip_support::object("a", "out/a", cid))],
    )
    .await;
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();
    let ledgers = remote_pin_ledger::Entity::find().all(&db).await.unwrap();
    assert_eq!(ledgers.len(), 1);
    assert_eq!(ledgers[0].effect, "reserved");
    assert_eq!(
        admission_barrier(&db, &ledgers[0].provider).await.unwrap(),
        AdmissionBarrier::Clear
    );
    assert_eq!(jobs::history::Entity::find().count(&db).await.unwrap(), 0);
}

#[tokio::test]
async fn expected_new_pin_rows_commit_without_submission_history() {
    let db = setup().await;
    let config = config();
    let (request, batch, capture, guard) = prepared(&db, &config, vec![]).await;
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();
    assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 2);
    assert_eq!(
        pin_lease_target::Entity::find().count(&db).await.unwrap(),
        2
    );
    assert_eq!(pin_job::Entity::find().count(&db).await.unwrap(), 1);
    assert_eq!(jobs::history::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(
        store::zip::snapshot(&db, "batch")
            .await
            .unwrap()
            .unwrap()
            .batch
            .state,
        "published"
    );
}

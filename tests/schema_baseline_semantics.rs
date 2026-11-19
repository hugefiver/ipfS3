use ipfs_s3_gateway::store;
use sea_orm::{ConnectionTrait, DatabaseConnection, Statement};

#[path = "support/baseline_postgres.rs"]
mod postgres;

async fn behavior(db: &DatabaseConnection) {
    db.execute_unprepared("INSERT INTO buckets(name) VALUES('semantics'); INSERT INTO objects(id,bucket,key,cid,size,etag) VALUES('tagged','semantics','tagged','cid',1,'cid'); INSERT INTO object_tags(object_id,key,value) VALUES('tagged','k','v')")
        .await
        .unwrap();
    assert_eq!(
        db.execute_unprepared(
            "UPDATE residency_backfill SET claim_epoch=1 WHERE id='hot_verification'"
        )
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    for sql in [
        "UPDATE buckets SET versioning_status='unknown' WHERE name='semantics'",
        "INSERT INTO object_tags(object_id,key,value) VALUES('missing','k','v')",
        "INSERT INTO bucket_lifecycle_configs(bucket,canonical_json,revision,created_at,updated_at) VALUES('semantics','{bad',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO bucket_lifecycle_configs(bucket,canonical_json,revision,created_at,updated_at) VALUES('semantics','{}',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "UPDATE residency_backfill SET claim_epoch=-1",
        "UPDATE residency_backfill SET claimed_by='worker',lease_until=NULL",
    ] {
        assert!(
            db.execute_unprepared(sql).await.is_err(),
            "accepted invalid mutation: {sql}"
        );
    }
    db.execute_unprepared("INSERT INTO bucket_lifecycle_configs(bucket,canonical_json,revision,created_at,updated_at) VALUES('semantics','{}',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    for (sequence, (id, version, accepted)) in [
        ("null-1", "NULL", true),
        ("null-2", "NULL", false),
        ("v1", "'version-1'", true),
        ("v1-duplicate", "'version-1'", false),
        ("v2", "'version-2'", true),
    ]
    .into_iter()
    .enumerate()
    {
        let result = db.execute_unprepared(&format!("INSERT INTO object_versions(id,bucket,key,version_id,kind,sequence,is_latest,lifecycle_age_started_at,created_at,updated_at) VALUES('{id}','semantics','key',{version},'delete_marker',{sequence},FALSE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")).await;
        assert_eq!(result.is_ok(), accepted, "partial uniqueness: {id}");
    }
    db.execute_unprepared("INSERT INTO physical_residencies(tier,cid,verification_state,created_at,updated_at) VALUES('hot','transition-cid','pending',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); INSERT INTO lifecycle_actions(id,idempotency_key,bucket,object_key,config_revision,rule_id,action_kind,target_type,target_version_row_id,target_public_version_id,target_object_id,target_sequence,due_at,state,next_attempt_at,created_at,updated_at) VALUES('action','idem','semantics','key',1,'rule','transition_current','version','version','null','object',1,CURRENT_TIMESTAMP,'pending',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); INSERT INTO lifecycle_transitions(id,action_id,action_kind,bucket,object_key,config_revision,rule_id,target_version_row_id,target_public_version_id,target_object_id,target_sequence,source_tier,destination_tier,source_cid,destination_cid,source_residency_revision,expected_source_node_identity,expected_destination_node_identity,ownership_generation,checkpoint,created_at,updated_at) VALUES('transition','action','transition_current','semantics','key',1,'rule','version','null','object',1,'hot','cold','transition-cid','transition-cid',1,'hot-node','cold-node',1,'prepare',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    assert!(
        db.execute_unprepared(
            "UPDATE lifecycle_transitions SET expected_source_node_identity='other-node'"
        )
        .await
        .is_err()
    );
    assert!(
        db.execute_unprepared("DELETE FROM lifecycle_transitions")
            .await
            .is_err()
    );
    db.execute_unprepared("UPDATE lifecycle_actions SET state='cancelled',finished_at=CURRENT_TIMESTAMP; UPDATE lifecycle_transitions SET settlement_kind='cancelled',completed_at=CURRENT_TIMESTAMP; DELETE FROM lifecycle_transitions").await.unwrap();

    db.execute_unprepared("DELETE FROM buckets WHERE name='semantics'")
        .await
        .unwrap();
    let row = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT COUNT(*) AS n FROM bucket_lifecycle_configs",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
}

#[tokio::test]
async fn sqlite_initialization_installs_storage_constraints_and_guards() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    behavior(&db).await;
}

#[tokio::test]
#[ignore = "requires authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_initialization_installs_storage_constraints_and_guards() {
    postgres::with_schema(|db| async move {
        store::run_migrations(&db).await.unwrap();
        behavior(&db).await;
    })
    .await;
}

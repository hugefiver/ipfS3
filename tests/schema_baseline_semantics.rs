use ipfs_s3_gateway::store::{initialization, migrations::Historical};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, Statement, TransactionTrait};
use sea_orm_migration::MigratorTrait;
#[path = "support/baseline_catalog.rs"]
mod catalog;
#[path = "support/baseline_postgres.rs"]
mod postgres;

async fn behavior(db: &DatabaseConnection) {
    db.execute_unprepared("INSERT INTO buckets(name) VALUES('semantics')")
        .await
        .unwrap();
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
    db.execute_unprepared("INSERT INTO pin_lease_requirements(lease_id,owner_object_id,contract,created_at) VALUES('lease','owner','{}',CURRENT_TIMESTAMP)").await.unwrap();
    for sql in [
        "UPDATE pin_lease_requirements SET contract='changed'",
        "DELETE FROM pin_lease_requirements",
    ] {
        assert!(db.execute_unprepared(sql).await.is_err());
    }
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
    let mut previous = 0;
    for job in ["first", "second"] {
        let row = db.query_one(Statement::from_string(db.get_database_backend(), format!("INSERT INTO pin_replica_observations(job_id,provider,expected_cid,route,phase,resources,outcome,remote_epoch,claim_until,observed_at,projectable) VALUES('{job}','provider','cid','{{}}','submit','[]','unknown',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,FALSE) RETURNING sequence"))).await.unwrap().unwrap();
        let sequence: i64 = row.try_get("", "sequence").unwrap();
        assert!(sequence > previous);
        previous = sequence;
    }
    assert!(
        db.execute_unprepared("DELETE FROM pin_replica_observations")
            .await
            .is_err()
    );
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

    use ipfs_s3_gateway::store::pinning::management::{
        native::meter::Budget,
        records::segmented::{self, ArchiveHeader},
    };
    let header = ArchiveHeader {
        database_scope: "scope".into(),
        database_identity: "identity".into(),
        principal: "history".into(),
        catalog: vec![],
        request: String::new(),
        configuration: String::new(),
        acknowledgements: vec![],
        created_at: String::new(),
        expires_at: None,
    };
    let mut originals = Vec::new();
    for (id, size) in [("inline-proof", 3), ("segmented-proof", 65537)] {
        let tx = db.begin().await.unwrap();
        let bytes = vec![7; size];
        let reference = segmented::write_archive(
            &tx,
            id,
            &header,
            &mut bytes.as_slice(),
            &mut Budget::default(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        originals.push((reference, bytes));
    }
    for sql in [
        "UPDATE pin_management_inline_archive SET header='changed'",
        "DELETE FROM pin_management_inline_archive",
        "UPDATE pin_management_archive SET header='changed'",
        "DELETE FROM pin_management_archive",
        "UPDATE pin_management_archive_chunks SET payload=payload",
        "DELETE FROM pin_management_archive_chunks",
    ] {
        assert!(db.execute_unprepared(sql).await.is_err());
    }
    for (reference, expected) in originals {
        let mut actual = Vec::new();
        segmented::read_archive(db, &reference, &mut actual, &mut Budget::default())
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }
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
async fn sqlite_final_predicates_and_guard_behavior_match_history() {
    let old = Database::connect("sqlite::memory:").await.unwrap();
    let fresh = Database::connect("sqlite::memory:").await.unwrap();
    Historical::up(&old, None).await.unwrap();
    initialization::open(&fresh).await.unwrap();
    let expected = catalog::sqlite(&old).await;
    let actual = catalog::sqlite(&fresh).await;
    assert!(actual.keys().eq(expected.keys()));
    for (key, predicates) in actual {
        assert_eq!(predicates, expected[&key], "{key}");
    }
    behavior(&old).await;
    behavior(&fresh).await;
}

#[tokio::test]
#[ignore = "requires authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_final_catalog_and_guard_behavior_match_history() {
    postgres::with_schema(|old| async move {
        Historical::up(&old, None).await.unwrap();
        postgres::with_schema(|fresh| async move {
            initialization::open(&fresh).await.unwrap();
            let expected = catalog::postgres(&old).await;
            for (kind, actual) in catalog::postgres(&fresh).await {
                let old = &expected[&kind];
                let missing = old
                    .iter()
                    .filter(|v| !actual.contains(v))
                    .collect::<Vec<_>>();
                let extra = actual
                    .iter()
                    .filter(|v| !old.contains(v))
                    .collect::<Vec<_>>();
                assert!(
                    actual == *old,
                    "{kind}: missing={missing:?}; extra={extra:?}; counts={}/{}",
                    old.len(),
                    actual.len()
                );
            }
            behavior(&old).await;
            behavior(&fresh).await;
        })
        .await;
    })
    .await;
}

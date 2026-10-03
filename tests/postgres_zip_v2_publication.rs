//! Explicit PG17 publication evidence, not an extrapolation from legacy ZIP batches.
//! Run this target with IPFS_S3_TEST_POSTGRES_URL and --ignored. Each test owns
//! only a fresh UUID schema, including its fault-injection trigger.
use std::{collections::BTreeMap, future::Future, panic::AssertUnwindSafe, time::Duration};

use chrono::Utc;
use futures_util::FutureExt;
use ipfs_s3_gateway::{
    config::{PinningConfig, PolicyConfig, ProviderConfig},
    error::AppError,
    import::{ImportSource, SupersedeReason},
    pinning::{
        config::ValidatedPinningConfig,
        policy::PublicationPolicy,
        zip_policy::{
            ValidatedZipOutputRules, ZipOutputRuleConfig, ZipPublishedOutput, ZipRuleEffect,
            ZipTargets as PolicyTargets,
        },
    },
    store::{
        self,
        entities::{import_job, object_version, pin_lease, pin_lease_target, pin_provider_usage},
        import::{jobs::NewImportJob, ownership},
        pinning::{
            ledger,
            publication::{
                self, PublicationObject, ZipV2Publication, ZipV2PublicationResult, ZipV2Success,
                v2_execution as execution,
            },
        },
        zip,
    },
    zip::options::ZipTargets,
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, EntityTrait,
    Statement, TransactionTrait,
};
use sha2::{Digest, Sha256};

struct Pg {
    a: DatabaseConnection,
    b: DatabaseConnection,
    control: DatabaseConnection,
    monitor: DatabaseConnection,
}

// Same UUID-schema pattern as zip_v2_execution.rs. A third single-session
// connection holds the gate; the admin connection only observes PG blocking.
// Futures are joined, not detached, so a panic cannot leave a spawned writer.
async fn isolated<F, Fut>(body: F)
where
    F: FnOnce(Pg) -> Fut,
    Fut: Future<Output = ()>,
{
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("NOT RUN: set IPFS_S3_TEST_POSTGRES_URL to an authorized isolated PG17 database");
    let admin = Database::connect(&url).await.unwrap();
    let version: String = admin
        .query_one(sql("SHOW server_version"))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "server_version")
        .unwrap();
    assert!(
        version.starts_with("17."),
        "requires PostgreSQL 17, got {version}"
    );
    let schema = format!("zip_v2_pub_{}", uuid::Uuid::new_v4().simple());
    assert!(
        schema
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    );
    let exists = || {
        Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT 1 FROM pg_namespace WHERE nspname=$1",
            [schema.clone().into()],
        )
    };
    assert!(admin.query_one(exists()).await.unwrap().is_none());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let mut connections = Vec::new();
    let result = AssertUnwindSafe(async {
        for _ in 0..3 {
            let mut options = ConnectOptions::new(&url);
            options.min_connections(1).max_connections(1);
            let db = Database::connect(options).await.unwrap();
            connections.push(db.clone());
            db.execute_unprepared(&format!("SET search_path TO {schema}"))
                .await
                .unwrap();
            db.execute_unprepared("SET statement_timeout TO '8s'")
                .await
                .unwrap();
        }
        store::run_migrations(&connections[0]).await.unwrap();
        store::bucket::create(&connections[0], "bucket", None)
            .await
            .unwrap();
        store::bucket::set_versioning_state(
            &connections[0],
            "bucket",
            store::object_version::BucketVersioningState::Enabled,
        )
        .await
        .unwrap();
        body(Pg {
            a: connections[0].clone(),
            b: connections[1].clone(),
            control: connections[2].clone(),
            monitor: admin.clone(),
        })
        .await;
    })
    .catch_unwind()
    .await;
    // Close even after a body/setup panic; DROP and the absence check still run.
    let mut closed = Vec::new();
    for db in connections {
        closed.push(db.close().await);
    }
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    assert!(
        admin.query_one(exists()).await.unwrap().is_none(),
        "owned schema survived cleanup"
    );
    eprintln!("PostgreSQL {version}: owned schema {schema} dropped and absence verified");
    admin.close().await.unwrap();
    for result in closed {
        result.unwrap();
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

fn sql(text: &str) -> Statement {
    Statement::from_string(DatabaseBackend::Postgres, text)
}

async fn pid(db: &DatabaseConnection) -> i32 {
    db.query_one(sql("SELECT pg_backend_pid() AS pid"))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "pid")
        .unwrap()
}

async fn wait_for_bucket_blocking(monitor: &DatabaseConnection, holder: i32, waiters: &[i32]) {
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut all_blocked = true;
            for waiter in waiters {
                let row = monitor
                    .query_one(Statement::from_sql_and_values(
                        DatabaseBackend::Postgres,
                        // PG's second row-lock waiter may wait on the first
                        // waiter's tuple lock, which itself waits on the holder.
                        "WITH RECURSIVE blockers(pid) AS ( \
                            SELECT unnest(pg_blocking_pids($1)) UNION \
                            SELECT unnest(pg_blocking_pids(blockers.pid)) FROM blockers \
                         ) SELECT EXISTS(SELECT 1 FROM blockers WHERE pid=$2) \
                            AND wait_event_type='Lock' \
                            AND query LIKE '%buckets%FOR NO KEY UPDATE%' AS blocked \
                         FROM pg_stat_activity WHERE pid=$1",
                        [(*waiter).into(), holder.into()],
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                all_blocked &= row.try_get::<bool>("", "blocked").unwrap();
            }
            if all_blocked {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    if observed.is_err() {
        let mut states = Vec::new();
        for session in std::iter::once(&holder).chain(waiters) {
            let state: String = monitor
                .query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "SELECT format('pid=%s state=%s wait=%s/%s blockers=%s query=%s', \
                 pid,state,wait_event_type,wait_event,pg_blocking_pids(pid),query) AS observed \
                 FROM pg_stat_activity WHERE pid=$1",
                    [(*session).into()],
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get("", "observed")
                .unwrap();
            states.push(state);
        }
        panic!("publication never reached the observed PG bucket-lock gate: {states:?}");
    }
}

// Full row snapshots, not just counts: rollback must also restore generations,
// sequences, lifecycle/residency, jobs, quota and both immutable manifests.
async fn rows<C: ConnectionTrait>(db: &C) -> BTreeMap<String, Vec<String>> {
    let tables = db
        .query_all(sql(
            "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename",
        ))
        .await
        .unwrap();
    let mut snapshot = BTreeMap::new();
    for table in tables {
        let name: String = table.try_get("", "tablename").unwrap();
        assert!(name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
        let contents = db
            .query_all(sql(&format!(
                "SELECT to_jsonb(t)::text AS row FROM \"{name}\" t ORDER BY to_jsonb(t)::text",
            )))
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get("", "row").unwrap())
            .collect();
        snapshot.insert(name, contents);
    }
    snapshot
}

async fn count(db: &DatabaseConnection, table: &str) -> i64 {
    assert!(
        table
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    );
    db.query_one(sql(&format!("SELECT COUNT(*) AS n FROM \"{table}\"")))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

struct Policy {
    config: ValidatedPinningConfig,
    rules: ValidatedZipOutputRules,
}

impl Policy {
    async fn new(db: &DatabaseConnection) -> Self {
        let config = ValidatedPinningConfig::from_raw(
            &PinningConfig {
                providers: vec![ProviderConfig {
                    name: "remote".into(),
                    kind: "noop".into(),
                    enabled: true,
                    priority: 1,
                    max_bytes: 100,
                    max_pins: 10,
                    token_env: None,
                    endpoint: None,
                    api: None,
                    strategy: None,
                    upload_endpoint: None,
                    requests_per_second: None,
                }],
                policies: vec![PolicyConfig {
                    bucket: "bucket".into(),
                    prefix: "out/".into(),
                    trigger: "always".into(),
                    provider_mode: "one".into(),
                    providers: vec!["remote".into()],
                    default_duration: "1h".into(),
                    max_duration: "2h".into(),
                    allow_decompressed: false,
                }],
                ..Default::default()
            },
            |_| None,
        )
        .unwrap();
        let rules = ValidatedZipOutputRules::compile(
            &[ZipOutputRuleConfig {
                name: "outputs".into(),
                priority: 1,
                bucket: "bucket".into(),
                prefix: "out/".into(),
                effect: ZipRuleEffect::Allow,
                policy_id: Some(config.policies[0].identity.clone()),
            }],
            &config,
        )
        .unwrap();
        ledger::register_route(db, "remote", &config.providers[0].identity)
            .await
            .unwrap();
        Self { config, rules }
    }

    async fn publish(
        &self,
        db: &DatabaseConnection,
        request: ZipV2Publication,
    ) -> Result<ZipV2PublicationResult, AppError> {
        publication::publish_zip_v2(
            db,
            request,
            &self.rules,
            &self.config,
            &self.config.provider_limits,
        )
        .await
    }

    fn admission(&self, id: &str, source_key: &str, root: bool) -> execution::Admission {
        let contract = "{\"contract\":\"v2\"}";
        execution::Admission {
            id: id.into(),
            owner: "principal".into(),
            source: "direct".into(),
            token: "same-token".into(),
            request_fingerprint: hex::encode(Sha256::digest(contract.as_bytes())),
            request_contract: contract.into(),
            bucket: "bucket".into(),
            source_key: source_key.into(),
            captured_options: serde_json::json!({"options":{
                "publish_source":false,"publish_extracted":true,"targets":ZipTargets::Extracted,
                "token":"same-token","root_override":null,"root_enabled":root,"result_version":2},
                "rule_revision":self.rules.revision()})
            .to_string(),
        }
    }

    fn request(
        &self,
        claim: execution::Claim,
        outputs: &[(&str, &str, &str)],
        identity: &str,
    ) -> ZipV2Publication {
        let successes = outputs
            .iter()
            .enumerate()
            .map(|(i, (path, key, cid))| {
                let plan = self
                    .rules
                    .plan(
                        PolicyTargets {
                            source: false,
                            extracted: true,
                        },
                        None,
                        &[ZipPublishedOutput {
                            bucket: "bucket".into(),
                            key: (*key).into(),
                            cid: (*cid).into(),
                            version_id: "fixture-plan-only".into(),
                        }],
                    )
                    .unwrap();
                ZipV2Success {
                    object: published(&format!("{identity}-{i}"), key, cid),
                    path: (*path).into(),
                    object_key: (*key).into(),
                    cid: (*cid).into(),
                    size: 7,
                    policy: PublicationPolicy {
                        tags: vec![],
                        leases: plan.outputs[0]
                            .intents
                            .iter()
                            .map(|intent| intent.intent.clone())
                            .collect(),
                    },
                }
            })
            .collect();
        ZipV2Publication {
            claim,
            source: None,
            source_policy: None,
            source_guard: None,
            successes,
            targets: ZipTargets::Extracted,
            captured_rule_revision: Some(self.rules.revision().into()),
            root_outcome: zip::RootOutcome::Disabled,
            terminal_result: serde_json::json!({"result_version":2,"entries":outputs.len()})
                .to_string(),
        }
    }
}

fn published(id: &str, key: &str, cid: &str) -> PublicationObject {
    PublicationObject::from_put(
        id.into(),
        "bucket",
        key,
        cid.into(),
        7,
        None,
        None,
        false,
        None,
        None,
        Utc::now(),
    )
}

fn input_digest() -> String {
    hex::encode(Sha256::digest(
        b"identical complete ZIP input attested at the publication seam",
    ))
}

async fn claim_input(
    db: &DatabaseConnection,
    admission: &execution::Admission,
) -> execution::Claim {
    let row = execution::admit(db, admission).await.unwrap();
    let claim = execution::claim(db, &row.id, "gateway-A", 60)
        .await
        .unwrap()
        .unwrap();
    execution::bind_clean_input(db, &claim, &input_digest(), "input-cid", 99)
        .await
        .unwrap();
    claim
}

async fn admit_outputs(
    db: &DatabaseConnection,
    claim: &execution::Claim,
    outputs: &[(&str, &str, &str)],
) {
    let tx = db.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    let items = outputs
        .iter()
        .map(|(path, key, cid)| execution::ManifestItem::Success {
            path: (*path).into(),
            object_key: (*key).into(),
            cid: (*cid).into(),
            size: 7,
        })
        .collect::<Vec<_>>();
    let ids = outputs
        .iter()
        .map(|(_, key, _)| ((*key).into(), format!("guard-{key}")))
        .collect();
    execution::admit_manifest_in_transaction(&tx, claim, &items, &ids)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

async fn prepare(
    db: &DatabaseConnection,
    admission: &execution::Admission,
    outputs: &[(&str, &str, &str)],
) -> execution::Claim {
    let claim = claim_input(db, admission).await;
    admit_outputs(db, &claim, outputs).await;
    zip::admit(
        db,
        &zip::BatchAdmission {
            id: claim.batch_id.clone(),
            owner: admission.owner.clone(),
            source: admission.source.clone(),
            token: admission.token.clone(),
            fingerprint: "pending".into(),
            bucket: admission.bucket.clone(),
            archive_key: admission.source_key.clone(),
            input_identity: "pending".into(),
            captured_options: admission.captured_options.clone(),
        },
    )
    .await
    .unwrap();
    let items = outputs
        .iter()
        .map(|(path, key, cid)| zip::ManifestItem::Success {
            path: (*path).into(),
            object_key: (*key).into(),
            cid: (*cid).into(),
            size: 7,
        })
        .collect::<Vec<_>>();
    zip::prepare_manifest(db, &claim.batch_id, &items)
        .await
        .unwrap();
    claim
}

async fn expire(db: &DatabaseConnection, claim: &execution::Claim) {
    assert_eq!(db.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "UPDATE zip_v2_executions SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1 AND epoch=$2",
        [claim.batch_id.clone().into(), claim.epoch.into()],
    )).await.unwrap().rows_affected(), 1);
}

async fn assert_published(db: &DatabaseConnection, id: &str, expected: i64) {
    for table in [
        "objects",
        "object_versions",
        "zip_v2_targets",
        "pin_leases",
        "pin_lease_targets",
    ] {
        assert_eq!(count(db, table).await, expected, "unexpected {table} count");
    }
    assert_eq!(count(db, "zip_v2_executions").await, 1);
    let execution = execution::read(db, id).await.unwrap().unwrap();
    assert_eq!(execution.state, "completed");
    let batch = zip::snapshot(db, id).await.unwrap().unwrap();
    assert_eq!(batch.batch.state, "published");
    assert!(!batch.batch.source_published);
    assert_eq!(batch.batch.terminal_result, execution.terminal_result);
    assert_eq!(batch.entries.len(), expected as usize);
    let versions = object_version::Entity::find().all(db).await.unwrap();
    let leases = pin_lease::Entity::find().all(db).await.unwrap();
    let targets = pin_lease_target::Entity::find().all(db).await.unwrap();
    for entry in batch.entries {
        let version = versions
            .iter()
            .find(|v| Some(&v.id) == entry.version_row_id.as_ref())
            .unwrap();
        assert_eq!(Some(&version.key), entry.object_key.as_ref());
        assert!(version.is_latest && version.version_id.is_some());
        let lease = leases
            .iter()
            .find(|l| Some(&l.owner_object_id) == version.object_id.as_ref())
            .unwrap();
        assert_eq!(lease.source, "automatic");
        let target = targets.iter().find(|t| t.lease_id == lease.id).unwrap();
        assert_eq!(Some(&target.cid), entry.cid.as_ref());
        assert_eq!(target.logical_size, 7);
        assert_eq!(target.provider, "remote");
    }
    assert_eq!(count(db, "standard_mutation_leases").await, 0);
    let uncleared = db
        .query_one(sql(
            "SELECT COUNT(*) AS n FROM import_destinations WHERE mutation_id IS NOT NULL",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "n")
        .unwrap();
    assert_eq!(
        uncleared, 0,
        "publication left an exact mutation authority active"
    );
    let usage = pin_provider_usage::Entity::find_by_id("remote")
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (usage.reserved_bytes, usage.reserved_pins),
        (7 * expected, expected)
    );
    assert_eq!(count(db, "remote_pins").await, expected);
}

#[tokio::test]
#[ignore = "requires explicitly authorized IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn same_token_same_input_two_connections_commit_one_version_targets_and_quota() {
    isolated(|pg| async move {
        let policy = Policy::new(&pg.a).await;
        let mut a = policy.admission("request-A", "source.zip", false);
        let b = policy.admission("request-B", "source.zip", false);
        let barrier = tokio::sync::Barrier::new(2);
        let (first, second) = tokio::join!(
            async {
                barrier.wait().await;
                execution::admit(&pg.a, &a).await.unwrap()
            },
            async {
                barrier.wait().await;
                execution::admit(&pg.b, &b).await.unwrap()
            },
        );
        assert_eq!(first.id, second.id);
        a.id = first.id;
        let outputs = [("one", "out/one", "cid-one")];
        let claim = prepare(&pg.a, &a, &outputs).await;
        assert!(
            execution::claim(&pg.b, &claim.batch_id, "gateway-B", 60)
                .await
                .unwrap()
                .is_none()
        );
        let replay = execution::read_for_replay(&pg.b, &b, &input_digest())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.state, "admitted");
        let (a_pid, b_pid, holder_pid) =
            (pid(&pg.a).await, pid(&pg.b).await, pid(&pg.control).await);
        assert_ne!(a_pid, b_pid);
        let holder = pg.control.begin().await.unwrap();
        ownership::lock_bucket_for_ownership(&holder, "bucket")
            .await
            .unwrap();
        // Duplicate dispatch of the same live authority, with different object IDs.
        // Both must read the admitted hint and reach the real serialization fence.
        let (left, right, ()) = tokio::join!(
            policy.publish(&pg.a, policy.request(claim.clone(), &outputs, "object-A")),
            policy.publish(&pg.b, policy.request(claim.clone(), &outputs, "object-B")),
            async {
                wait_for_bucket_blocking(&pg.monitor, holder_pid, &[a_pid, b_pid]).await;
                holder.commit().await.unwrap();
            },
        );
        let results = [left, right];
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Ok(ZipV2PublicationResult::Published(_))))
                .count(),
            1,
            "duplicate publication outcomes: {results:?}"
        );
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Err(AppError::InvalidPinningRequest(_))))
                .count(),
            1,
            "loser must reject changed authority, not fail with an unrelated DB error: {results:?}"
        );
        assert_published(&pg.control, &claim.batch_id, 1).await;
        let before = rows(&pg.control).await;
        let replay = execution::read_for_replay(&pg.b, &b, &input_digest())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.state, "completed");
        assert!(replay.terminal_result.is_some());
        assert_eq!(
            rows(&pg.control).await,
            before,
            "replay changed publication or quota"
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires explicitly authorized IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn source_prefix_owners_are_not_superseded_by_v2_output_admission() {
    isolated(|pg| async move {
        let policy = Policy::new(&pg.a).await;
        let source = published("original-source", "source.zip", "original-cid");
        publication::publish_object(
            &pg.a,
            publication::PublicationRequest {
                object_target: publication::PinTargetSpec {
                    cid: source.cid.clone(),
                    logical_size: 7,
                },
                object: source,
                tags: vec![],
                policy: PublicationPolicy {
                    tags: vec![],
                    leases: vec![],
                },
            },
            &Default::default(),
        )
        .await
        .unwrap();
        let claim = claim_input(
            &pg.a,
            &policy.admission("source-protected", "source.zip", false),
        )
        .await;
        let source_guard = ownership::admit_content_and_prefix_mutation(
            &pg.a,
            "bucket",
            "source.zip",
            "out/",
            SupersedeReason::DecompressZip,
            store::database_clock::database_now(&pg.a).await.unwrap(),
        )
        .await
        .unwrap();
        let items = [execution::ManifestItem::Success {
            path: "one".into(),
            object_key: "out/one".into(),
            cid: "cid-one".into(),
            size: 7,
        }];
        let ids = BTreeMap::from([("out/one".into(), "output-mutation".into())]);
        // Both source-owning overlap forms have distinct production checks:
        // standard prefix mutation, then active import prefix/job ownership.
        let before = rows(&pg.a).await;
        let tx = pg.b.begin().await.unwrap();
        ownership::lock_bucket_for_ownership(&tx, "bucket")
            .await
            .unwrap();
        assert!(matches!(
            execution::admit_manifest_in_transaction(&tx, &claim, &items, &ids).await,
            Err(AppError::StaleContentMutation)
        ));
        tx.rollback().await.unwrap();
        assert_eq!(
            rows(&pg.a).await,
            before,
            "source prefix token or source version changed"
        );
        ownership::renew_standard_mutation(&pg.a, &source_guard)
            .await
            .unwrap();
        assert!(
            ownership::release_standard_mutation(&pg.a, &source_guard)
                .await
                .unwrap()
        );
        ownership::submit(
            &pg.a,
            NewImportJob {
                id: "source-import".into(),
                bucket: "bucket".into(),
                key: "source.zip".into(),
                source: ImportSource::Cid(
                    "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".into(),
                ),
                request_fingerprint: "source-import-fingerprint".into(),
                client_token: None,
                object_content_type: None,
                metadata: Default::default(),
                tags: vec![],
                decompress_prefix: Some("out/".into()),
            },
            store::database_clock::database_now(&pg.a).await.unwrap(),
        )
        .await
        .unwrap();
        let before = rows(&pg.a).await;
        let tx = pg.b.begin().await.unwrap();
        ownership::lock_bucket_for_ownership(&tx, "bucket")
            .await
            .unwrap();
        assert!(matches!(
            execution::admit_manifest_in_transaction(&tx, &claim, &items, &ids).await,
            Err(AppError::StaleContentMutation)
        ));
        tx.rollback().await.unwrap();
        assert_eq!(
            rows(&pg.a).await,
            before,
            "source import was superseded or partially admitted"
        );
        assert_eq!(
            import_job::Entity::find_by_id("source-import")
                .one(&pg.a)
                .await
                .unwrap()
                .unwrap()
                .state,
            "queued"
        );
        assert_eq!(
            execution::read(&pg.a, &claim.batch_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            "pending"
        );
        assert!(
            execution::read_targets(&pg.a, &claim)
                .await
                .unwrap()
                .is_empty()
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires explicitly authorized IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn final_authority_failure_rolls_back_every_row_and_valid_admitted_epoch_recovers() {
    isolated(|pg| async move {
        let policy = Policy::new(&pg.a).await;
        let outputs = [("one", "out/one", "cid-one")];
        let admission = policy.admission("late-failure", "source.zip", true);
        let old = prepare(&pg.a, &admission, &outputs).await;
        let root = zip::claim_root(&pg.a, &old.batch_id, "root-worker", 60).await.unwrap();
        zip::mark_invoked(&pg.a, &root).await.unwrap();
        zip::retain_candidate(&pg.a, &root, "hot-node", "hot", "verified-root").await.unwrap();
        zip::verify_root(&pg.a, &root, "hot-node", "hot", "verified-root", "verified-recursive-pin").await.unwrap();
        let mut request = policy.request(old.clone(), &outputs, "output");
        request.root_outcome = zip::RootOutcome::Verified {
            claim: root, node_identity: "hot-node".into(), tier: "hot".into(), cid: "verified-root".into(),
        };
        pg.control.execute_unprepared(r#"
            CREATE FUNCTION fail_final_authority() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.state = 'completed' THEN
                    IF (SELECT count(*) FROM objects) <> 1
                        OR (SELECT count(*) FROM object_versions) <> 1
                        OR (SELECT count(*) FROM pin_leases) <> 1
                        OR (SELECT count(*) FROM pin_lease_targets) <> 1
                        OR (SELECT count(*) FROM remote_pins) <> 1
                        OR NOT EXISTS (SELECT 1 FROM pin_provider_usage WHERE provider='remote'
                            AND reserved_bytes=7 AND reserved_pins=1)
                        OR NOT EXISTS (SELECT 1 FROM zip_batches WHERE id=NEW.id AND state='published')
                        OR NOT EXISTS (SELECT 1 FROM zip_manifest_entries WHERE batch_id=NEW.id
                            AND version_row_id IS NOT NULL)
                        OR NOT EXISTS (SELECT 1 FROM zip_root_references WHERE batch_id=NEW.id AND state='adopted')
                    THEN
                        RAISE EXCEPTION 'fault gate was reached before publication writes';
                    END IF;
                    RAISE EXCEPTION 'injected final authority failure';
                END IF;
                RETURN NEW;
            END $$
        "#).await.unwrap();
        pg.control.execute_unprepared("CREATE TRIGGER fail_final_authority BEFORE UPDATE ON zip_v2_executions \
            FOR EACH ROW EXECUTE FUNCTION fail_final_authority()").await.unwrap();
        let before = rows(&pg.a).await;
        let error = policy.publish(&pg.b, request.clone()).await.unwrap_err();
        assert!(matches!(&error, AppError::Database(_)) && format!("{error:?}").contains("injected final authority failure"),
            "publication did not fail at the final write gate: {error:?}");
        assert_eq!(rows(&pg.a).await, before, "late failure leaked publication or guard completion");
        let old_guards = execution::read_targets(&pg.a, &old).await.unwrap();
        pg.control.execute_unprepared("DROP TRIGGER fail_final_authority ON zip_v2_executions").await.unwrap();
        expire(&pg.a, &old).await;
        let new = execution::claim(&pg.b, &old.batch_id, "gateway-B", 60).await.unwrap().unwrap();
        assert!(new.epoch > old.epoch);
        assert_eq!(execution::read(&pg.a, &new.batch_id).await.unwrap().unwrap().state, "admitted");
        let new_guards = execution::read_targets(&pg.b, &new).await.unwrap();
        assert_eq!(new_guards.len(), old_guards.len());
        for (old, new) in old_guards.iter().zip(&new_guards) {
            assert_eq!((&new.key, new.expected_generation), (&old.key, old.expected_generation));
            assert_ne!(new.mutation_id, old.mutation_id, "takeover must rotate authority, not readmit it");
            assert!(!ownership::release_standard_mutation(&pg.a, old).await.unwrap());
        }
        let after_takeover = rows(&pg.a).await;
        assert!(matches!(policy.publish(&pg.a, request.clone()).await, Err(AppError::StaleContentMutation)));
        assert_eq!(rows(&pg.b).await, after_takeover, "old epoch damaged successor ownership");
        request.claim = new.clone();
        assert!(matches!(policy.publish(&pg.b, request).await.unwrap(), ZipV2PublicationResult::Published(rows) if rows.len()==1));
        assert_published(&pg.a, &new.batch_id, 1).await;
        let batch = zip::snapshot(&pg.a, &new.batch_id).await.unwrap().unwrap();
        assert_eq!(batch.batch.root_status, "complete");
        assert_eq!(batch.batch.root_cid.as_deref(), Some("verified-root"));
        assert_eq!(batch.references.iter().filter(|r| r.state=="adopted").count(), 1);
    }).await;
}

#[tokio::test]
#[ignore = "requires explicitly authorized IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn claim_expiring_behind_bucket_lock_cannot_publish_or_consume_intact_admission() {
    isolated(|pg| async move {
        let policy = Policy::new(&pg.a).await;
        let outputs = [("one", "out/one", "cid-one")];
        let old = prepare(&pg.a, &policy.admission("expires-at-gate", "source.zip", false), &outputs).await;
        let (waiter, holder_pid) = (pid(&pg.a).await, pid(&pg.control).await);
        let holder = pg.control.begin().await.unwrap();
        ownership::lock_bucket_for_ownership(&holder, "bucket").await.unwrap();
        let (result, after_expiry) = tokio::join!(
            policy.publish(&pg.a, policy.request(old.clone(), &outputs, "expired-output")),
            async {
                wait_for_bucket_blocking(&pg.monitor, holder_pid, &[waiter]).await;
                assert_eq!(holder.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres,
                    "UPDATE zip_v2_executions SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
                    [old.batch_id.clone().into()],
                )).await.unwrap().rows_affected(), 1);
                let after_expiry = rows(&holder).await;
                holder.commit().await.unwrap();
                after_expiry
            },
        );
        assert!(matches!(result, Err(AppError::StaleContentMutation)), "expired publication outcome: {result:?}");
        assert_eq!(rows(&pg.b).await, after_expiry, "expired publication changed more than the injected lease deadline");
        for table in ["objects", "object_versions", "pin_leases", "pin_lease_targets", "remote_pins", "pin_provider_usage"] {
            assert_eq!(count(&pg.b, table).await, 0, "expired claim wrote {table}");
        }
        assert_eq!(execution::read(&pg.b, &old.batch_id).await.unwrap().unwrap().state, "admitted");
        assert_eq!(zip::read(&pg.b, &old.batch_id).await.unwrap().unwrap().state, "open");
        assert_eq!(execution::read_targets(&pg.b, &old).await.unwrap().len(), 1);
        let new = execution::claim(&pg.b, &old.batch_id, "gateway-B", 60).await.unwrap().unwrap();
        assert!(new.epoch > old.epoch);
        assert!(matches!(policy.publish(&pg.b, policy.request(new.clone(), &outputs, "live-output")).await.unwrap(),
            ZipV2PublicationResult::Published(rows) if rows.len()==1));
        assert_published(&pg.a, &new.batch_id, 1).await;
    }).await;
}

#[tokio::test]
#[ignore = "requires explicitly authorized IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn one_displaced_exact_guard_after_hint_fences_whole_publication_without_recapture() {
    isolated(|pg| async move {
        let policy = Policy::new(&pg.a).await;
        let outputs = [("one", "out/one", "cid-one"), ("two", "out/two", "cid-two")];
        let old = prepare(
            &pg.a,
            &policy.admission("displaced", "source.zip", false),
            &outputs,
        )
        .await;
        let old_guards = execution::read_targets(&pg.a, &old).await.unwrap();
        let (waiter, holder_pid) = (pid(&pg.a).await, pid(&pg.control).await);
        let holder = pg.control.begin().await.unwrap();
        ownership::lock_bucket_for_ownership(&holder, "bucket")
            .await
            .unwrap();
        let (result, foreign) = tokio::join!(
            policy.publish(
                &pg.a,
                policy.request(old.clone(), &outputs, "fenced-output")
            ),
            async {
                wait_for_bucket_blocking(&pg.monitor, holder_pid, &[waiter]).await;
                let keys = ["out/two".into()].into_iter().collect();
                let ids = BTreeMap::from([("out/two".into(), "foreign-writer".into())]);
                let guards = ownership::admit_zip_outputs_without_source_in_transaction(
                    &holder,
                    "bucket",
                    "unrelated-source.zip",
                    &keys,
                    &ids,
                    store::database_clock::database_now(&holder).await.unwrap(),
                )
                .await
                .unwrap();
                holder.commit().await.unwrap();
                guards.into_iter().next().unwrap()
            },
        );
        assert!(
            matches!(result, Err(AppError::StaleContentMutation)),
            "lost-guard publication outcome: {result:?}"
        );
        for table in [
            "objects",
            "object_versions",
            "pin_leases",
            "pin_lease_targets",
            "remote_pins",
            "pin_provider_usage",
        ] {
            assert_eq!(
                count(&pg.b, table).await,
                0,
                "lost guard leaked partial {table}"
            );
        }
        let fenced = execution::read(&pg.b, &old.batch_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fenced.state, "fenced");
        assert_eq!(fenced.terminal_result.as_deref(), Some("fenced"));
        assert_eq!(fenced.epoch, old.epoch);
        assert_eq!(
            execution::read_targets(&pg.b, &old).await.unwrap(),
            old_guards
        );
        assert_eq!(
            zip::read(&pg.b, &old.batch_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            "open"
        );
        assert!(
            !ownership::release_standard_mutation(&pg.b, &old_guards[1])
                .await
                .unwrap()
        );
        ownership::renew_standard_mutation(&pg.b, &foreign)
            .await
            .unwrap();
        let before = rows(&pg.b).await;
        assert!(
            execution::claim(&pg.a, &old.batch_id, "gateway-C", 60)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            policy
                .publish(&pg.a, policy.request(old, &outputs, "retry"))
                .await,
            Err(AppError::InvalidPinningRequest(_))
        ));
        assert_eq!(
            rows(&pg.b).await,
            before,
            "fenced retry recaptured or cleared foreign authority"
        );
    })
    .await;
}

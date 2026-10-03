//! Real PG17 regression: A publishes source X/output Y, B publishes outputs X/Y,
//! in different buckets with previously unseen CIDs X < Y. AFTER INSERT gates
//! hold the actual unique-key insert locks, not simulated Rust-side locks.
//! Run with an authorized IPFS_S3_TEST_POSTGRES_URL and --ignored --nocapture.
use std::{collections::BTreeMap, future::Future, panic::AssertUnwindSafe, time::Duration};

use chrono::Utc;
use futures_util::FutureExt;
use ipfs_s3_gateway::{
    pinning::{
        config::ValidatedPinningConfig, policy::PublicationPolicy,
        zip_policy::ValidatedZipOutputRules,
    },
    residency::{KuboTier, VerificationState},
    store::{
        self,
        import::ownership,
        object_version::BucketVersioningState,
        pinning::publication::{
            self, PublicationObject, ZipV2Publication, ZipV2PublicationResult, ZipV2Success,
            v2_execution as execution,
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
    gate: DatabaseConnection,
    monitor: DatabaseConnection,
}

async fn isolated<F, Fut>(body: F)
where
    F: FnOnce(Pg) -> Fut,
    Fut: Future<Output = ()>,
{
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("set IPFS_S3_TEST_POSTGRES_URL to an explicitly authorized isolated PG17 database");
    let admin = Database::connect(&url).await.unwrap();
    let version: String = admin
        .query_one(sql("SHOW server_version"))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "server_version")
        .unwrap();
    assert!(version.starts_with("17."), "requires PG17, got {version}");
    let schema = format!("zip_v2_lock_{}", uuid::Uuid::new_v4().simple());
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
    let outcome = AssertUnwindSafe(async {
        for _ in 0..3 {
            let mut options = ConnectOptions::new(&url);
            options.min_connections(1).max_connections(1);
            let db = Database::connect(options).await.unwrap();
            connections.push(db.clone());
            db.execute_unprepared(&format!("SET search_path TO {schema}"))
                .await
                .unwrap();
            db.execute_unprepared("SET statement_timeout TO '20s'")
                .await
                .unwrap();
            // Leave enough time to observe the real lock graph before PG's
            // detector breaks it. A detected cycle is cancelled explicitly.
            db.execute_unprepared("SET deadlock_timeout TO '10s'")
                .await
                .unwrap();
        }
        store::run_migrations(&connections[0]).await.unwrap();
        for bucket in ["bucket-a", "bucket-b"] {
            store::bucket::create(&connections[0], bucket, None)
                .await
                .unwrap();
            store::bucket::set_versioning_state(
                &connections[0],
                bucket,
                BucketVersioningState::Enabled,
            )
            .await
            .unwrap();
        }
        body(Pg {
            a: connections[0].clone(),
            b: connections[1].clone(),
            gate: connections[2].clone(),
            monitor: admin.clone(),
        })
        .await;
    })
    .catch_unwind()
    .await;
    // Joined (not detached) writers are dropped before cleanup on a panic.
    // Close the gate first so a panic cannot leave writers waiting on our
    // UUID-keyed advisory locks while their connections are being closed.
    let mut closed = Vec::new();
    for connection in connections.into_iter().rev() {
        closed.push(connection.close().await);
    }
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    assert!(
        admin.query_one(exists()).await.unwrap().is_none(),
        "owned schema survived cleanup"
    );
    eprintln!("PostgreSQL {version}: owned schema {schema} dropped; absence verified");
    admin.close().await.unwrap();
    for result in closed {
        result.unwrap();
    }
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

fn sql(text: &str) -> Statement {
    Statement::from_string(DatabaseBackend::Postgres, text)
}

fn object(id: &str, bucket: &str, key: &str, cid: &str) -> PublicationObject {
    PublicationObject::from_put(
        id.into(),
        bucket,
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

fn empty_policy() -> PublicationPolicy {
    PublicationPolicy {
        tags: vec![],
        leases: vec![],
    }
}

async fn prepared(
    db: &DatabaseConnection,
    id: &str,
    bucket: &str,
    source_cid: Option<&str>,
    outputs: &[(&str, &str)],
) -> ZipV2Publication {
    let contract = "{\"contract\":\"v2-residency-lock-order\"}";
    let admission = execution::Admission {
        id: id.into(), owner: "principal".into(), source: "direct".into(), token: id.into(),
        request_fingerprint: hex::encode(Sha256::digest(contract.as_bytes())),
        request_contract: contract.into(), bucket: bucket.into(), source_key: "source.zip".into(),
        captured_options: serde_json::json!({"options":{
            "publish_source":source_cid.is_some(),"publish_extracted":true,"targets":ZipTargets::None,
            "token":id,"root_override":null,"root_enabled":false,"result_version":2},
            "rule_revision":null}).to_string(),
    };
    execution::admit(db, &admission).await.unwrap();
    let claim = execution::claim(db, id, "worker", 60)
        .await
        .unwrap()
        .unwrap();
    execution::bind_clean_input(
        db,
        &claim,
        &hex::encode(Sha256::digest(b"complete ZIP bytes")),
        source_cid.unwrap_or("unpublished-input"),
        7,
    )
    .await
    .unwrap();
    zip::admit(
        db,
        &zip::BatchAdmission {
            id: id.into(),
            owner: admission.owner,
            source: admission.source,
            token: admission.token,
            fingerprint: "pending".into(),
            bucket: bucket.into(),
            archive_key: admission.source_key,
            input_identity: "pending".into(),
            captured_options: admission.captured_options,
        },
    )
    .await
    .unwrap();
    let mirror = outputs
        .iter()
        .map(|(key, cid)| zip::ManifestItem::Success {
            path: (*key).into(),
            object_key: (*key).into(),
            cid: (*cid).into(),
            size: 7,
        })
        .collect::<Vec<_>>();
    zip::prepare_manifest(db, id, &mirror).await.unwrap();
    let tx = db.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&tx, bucket)
        .await
        .unwrap();
    let mutations = outputs
        .iter()
        .map(|(key, _)| ((*key).into(), format!("guard-{id}-{key}")))
        .collect::<BTreeMap<_, _>>();
    let source_guard = if source_cid.is_some() {
        Some(
            ownership::admit_zip_v2_source_in_transaction(
                &tx,
                bucket,
                "source.zip",
                &mutations.keys().cloned().collect(),
                &format!("zip-v2-source:{id}"),
                store::database_clock::database_now(&tx).await.unwrap(),
            )
            .await
            .unwrap(),
        )
    } else {
        None
    };
    let items = outputs
        .iter()
        .map(|(key, cid)| execution::ManifestItem::Success {
            path: (*key).into(),
            object_key: (*key).into(),
            cid: (*cid).into(),
            size: 7,
        })
        .collect::<Vec<_>>();
    execution::admit_manifest_in_transaction(&tx, &claim, &items, &mutations)
        .await
        .unwrap();
    if let Some(guard) = &source_guard {
        ownership::capture_zip_v2_source_guard_in_transaction(&tx, &claim, guard)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    ZipV2Publication {
        claim,
        source: source_cid.map(|cid| object(&format!("{id}-source"), bucket, "source.zip", cid)),
        source_policy: source_cid.map(|_| empty_policy()),
        source_guard,
        successes: outputs
            .iter()
            .map(|(key, cid)| ZipV2Success {
                object: object(&format!("{id}-{key}"), bucket, key, cid),
                path: (*key).into(),
                object_key: (*key).into(),
                cid: (*cid).into(),
                size: 7,
                policy: empty_policy(),
            })
            .collect(),
        targets: ZipTargets::None,
        captured_rule_revision: None,
        root_outcome: zip::RootOutcome::Disabled,
        terminal_result: "{\"result_version\":2}".into(),
    }
}

async fn install_gates(db: &DatabaseConnection, x: &str, y: &str) {
    // nextval is deliberately nontransactional: retries cannot erase evidence.
    // The GUC is transaction-local, so repeated INSERT ... ON CONFLICT in the
    // same publication counts once, but a replacement transaction counts again.
    db.execute_unprepared(&format!(
        r#"
        CREATE SEQUENCE a_attempts;
        CREATE SEQUENCE b_attempts;
        CREATE SEQUENCE a_source_install;
        CREATE FUNCTION count_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE actor text := current_setting('application_name');
        BEGIN
            IF current_setting('ipfs3_zip_lock.counted', true) IS DISTINCT FROM 'yes' THEN
                IF actor = 'zip-lock-a' THEN PERFORM nextval('a_attempts');
                ELSIF actor = 'zip-lock-b' THEN PERFORM nextval('b_attempts');
                ELSE RAISE EXCEPTION 'unexpected residency writer: %', actor;
                END IF;
                PERFORM set_config('ipfs3_zip_lock.counted', 'yes', true);
            END IF;
            IF actor = 'zip-lock-a' AND NEW.cid = '{x}' AND EXISTS (
                SELECT 1 FROM object_versions WHERE bucket='bucket-a' AND key='source.zip'
            ) THEN PERFORM nextval('a_source_install'); END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER count_attempt BEFORE INSERT ON physical_residencies
            FOR EACH ROW EXECUTE FUNCTION count_attempt();
        CREATE FUNCTION residency_gate() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF current_setting('application_name') = 'zip-lock-a' AND NEW.cid = '{y}' THEN
                PERFORM pg_advisory_xact_lock(hashtextextended(current_schema() || ':a', 0));
            ELSIF current_setting('application_name') = 'zip-lock-b' AND NEW.cid = '{x}' THEN
                PERFORM pg_advisory_xact_lock(hashtextextended(current_schema() || ':b', 0));
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER residency_gate AFTER INSERT ON physical_residencies
            FOR EACH ROW EXECUTE FUNCTION residency_gate();
    "#
    ))
    .await
    .unwrap();
    for actor in ['a', 'b'] {
        db.query_one(sql(&format!(
            "SELECT pg_advisory_lock(hashtextextended(current_schema() || ':{actor}', 0))"
        )))
        .await
        .unwrap();
    }
}

async fn release_gate(db: &DatabaseConnection, actor: char) {
    let unlocked: bool = db.query_one(sql(&format!(
        "SELECT pg_advisory_unlock(hashtextextended(current_schema() || ':{actor}', 0)) AS unlocked"
    ))).await.unwrap().unwrap().try_get("", "unlocked").unwrap();
    assert!(unlocked, "owned gate {actor} was not held");
}

async fn pid(db: &DatabaseConnection) -> i32 {
    db.query_one(sql("SELECT pg_backend_pid() AS pid"))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "pid")
        .unwrap()
}

#[derive(Debug)]
struct LockState {
    pid: i32,
    blockers: Vec<i32>,
    wait_type: Option<String>,
    wait_event: Option<String>,
    query: String,
}

impl LockState {
    fn blocked_by(&self, holder: i32, event: &str) -> bool {
        self.blockers.contains(&holder)
            && self.wait_type.as_deref() == Some("Lock")
            && self.wait_event.as_deref() == Some(event)
            && self.query.contains("physical_residencies")
    }
}

async fn lock_state(db: &DatabaseConnection, session: i32) -> LockState {
    let row = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT pid, pg_blocking_pids(pid) AS blockers, wait_event_type, wait_event, query \
         FROM pg_stat_activity WHERE pid=$1",
            [session.into()],
        ))
        .await
        .unwrap()
        .unwrap();
    LockState {
        pid: row.try_get("", "pid").unwrap(),
        blockers: row.try_get("", "blockers").unwrap(),
        wait_type: row.try_get("", "wait_event_type").unwrap(),
        wait_event: row.try_get("", "wait_event").unwrap(),
        query: row.try_get("", "query").unwrap(),
    }
}

async fn wait_for<F>(db: &DatabaseConnection, sessions: &[i32], predicate: F) -> Vec<LockState>
where
    F: Fn(&[LockState]) -> bool,
{
    let mut latest = Vec::new();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            latest.clear();
            for session in sessions {
                latest.push(lock_state(db, *session).await);
            }
            if predicate(&latest) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "expected PG lock graph never appeared: {latest:#?}"
    );
    for state in &latest {
        eprintln!("observed pid={} {state:?}", state.pid);
    }
    latest
}

async fn sequence(db: &DatabaseConnection, name: &str) -> (i64, bool) {
    assert!(matches!(
        name,
        "a_attempts" | "b_attempts" | "a_source_install"
    ));
    let row = db
        .query_one(sql(&format!("SELECT last_value, is_called FROM {name}")))
        .await
        .unwrap()
        .unwrap();
    (
        row.try_get("", "last_value").unwrap(),
        row.try_get("", "is_called").unwrap(),
    )
}

async fn assert_bindings(db: &DatabaseConnection, request: &ZipV2Publication) {
    let snapshot = execution::read(db, &request.claim.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.state, "completed");
    let mirror = zip::snapshot(db, &request.claim.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mirror.batch.state, "published");
    assert_eq!(mirror.batch.source_published, request.source.is_some());
    assert_eq!(mirror.batch.terminal_result, snapshot.terminal_result);
    assert_eq!(mirror.entries.len(), request.successes.len());
    let tx = db.begin().await.unwrap();
    for (path, object) in request.source.iter().map(|s| ("source", s)).chain(
        request
            .successes
            .iter()
            .map(|s| (s.path.as_str(), &s.object)),
    ) {
        let binding = zip::binding_for_published_object(&tx, path, &object.id)
            .await
            .unwrap();
        let residency = store::residency::resolve_version_residency(&tx, &binding.version_row_id)
            .await
            .unwrap();
        assert_eq!(residency.identity.object_id, object.id);
        assert_eq!(residency.identity.cid, object.cid);
        assert_eq!(residency.primary.tier, KuboTier::Hot);
        assert_eq!(
            residency.physical.verification_state,
            VerificationState::Pending
        );
        if path == "source" {
            let terminal: serde_json::Value =
                serde_json::from_str(snapshot.terminal_result.as_deref().unwrap()).unwrap();
            assert_eq!(terminal["source_cid"], object.cid);
            assert_eq!(terminal["source_size"], object.logical_size);
            assert_eq!(terminal["source_version_row_id"], binding.version_row_id);
            let version =
                store::entities::object_version::Entity::find_by_id(&binding.version_row_id)
                    .one(&tx)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(
                terminal["source_version_id"],
                serde_json::to_value(version.version_id).unwrap()
            );
        } else {
            let entry = mirror
                .entries
                .iter()
                .find(|entry| entry.path == path)
                .unwrap();
            assert_eq!(
                entry.version_row_id.as_deref(),
                Some(binding.version_row_id.as_str())
            );
            assert_eq!(entry.cid.as_deref(), Some(object.cid.as_str()));
        }
    }
    tx.commit().await.unwrap();
}

fn cid(bytes: &[u8]) -> String {
    cid::Cid::new_v1(
        0x55,
        cid::multihash::Multihash::<64>::wrap(0x12, &Sha256::digest(bytes)).unwrap(),
    )
    .to_string()
}

#[tokio::test]
#[ignore = "requires explicitly authorized IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn source_and_outputs_prelock_new_residencies_in_one_order_without_retry() {
    isolated(|pg| async move {
        let mut cids = [cid(b"new physical residency one"), cid(b"new physical residency two")];
        cids.sort();
        let [x, y] = cids;
        assert!(x < y);
        let a = prepared(&pg.a, "execution-a", "bucket-a", Some(&x), &[("output-y", &y)]).await;
        // Reverse the output input order as well: the frontier must sort it.
        let b = prepared(&pg.b, "execution-b", "bucket-b", None, &[("output-y", &y), ("output-x", &x)]).await;
        let count: i64 = pg.gate.query_one(sql("SELECT count(*) AS n FROM physical_residencies"))
            .await.unwrap().unwrap().try_get("", "n").unwrap();
        assert_eq!(count, 0, "X and Y must both be previously unseen CIDs");
        let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
        let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
        pg.a.execute_unprepared("SET application_name TO 'zip-lock-a'").await.unwrap();
        pg.b.execute_unprepared("SET application_name TO 'zip-lock-b'").await.unwrap();
        let (a_pid, b_pid, gate_pid) = (pid(&pg.a).await, pid(&pg.b).await, pid(&pg.gate).await);
        assert_ne!(a_pid, b_pid);
        assert_ne!(a_pid, gate_pid);
        assert_ne!(b_pid, gate_pid);
        install_gates(&pg.gate, &x, &y).await;
        eprintln!("X={x} < Y={y}; A pid={a_pid} source X/output Y; B pid={b_pid} outputs X/Y; gate pid={gate_pid}");
        let start_b = tokio::sync::Barrier::new(2);
        let (a_result, b_result, cycle) = tokio::join!(
            publication::publish_zip_v2(&pg.a, a.clone(), &rules, &config, &config.provider_limits),
            async {
                start_b.wait().await;
                publication::publish_zip_v2(&pg.b, b.clone(), &rules, &config, &config.provider_limits).await
            },
            async {
                // A's AFTER INSERT Y trigger cannot arrive without holding Y.
                wait_for(&pg.monitor, &[a_pid], |s| s[0].blocked_by(gate_pid, "advisory")).await;
                start_b.wait().await;
                let state = wait_for(&pg.monitor, &[b_pid], |s|
                    s[0].blocked_by(gate_pid, "advisory") || s[0].blocked_by(a_pid, "transactionid")).await;
                let missing_source_prelock = state[0].blocked_by(gate_pid, "advisory");
                release_gate(&pg.gate, 'b').await;
                if missing_source_prelock {
                    // Old code: B passed AFTER INSERT X, so owns X and now
                    // waits on A's Y. A's source install then waits on B's X.
                    wait_for(&pg.monitor, &[b_pid], |s| s[0].blocked_by(a_pid, "transactionid")).await;
                }
                release_gate(&pg.gate, 'a').await;
                if missing_source_prelock {
                    wait_for(&pg.monitor, &[a_pid, b_pid], |s|
                        s[0].blocked_by(b_pid, "transactionid") && s[1].blocked_by(a_pid, "transactionid")).await;
                    assert_eq!(sequence(&pg.gate, "a_source_install").await, (1, true),
                        "A must have reached source version installation, not a synthetic gate cycle");
                    // Break the observed lock cycle with nonretryable SQLSTATE
                    // 57014, rather than letting publication's 40P01 retry hide it.
                    let cancelled: bool = pg.monitor.query_one(Statement::from_sql_and_values(
                        DatabaseBackend::Postgres, "SELECT pg_cancel_backend($1) AS cancelled", [a_pid.into()]))
                        .await.unwrap().unwrap().try_get("", "cancelled").unwrap();
                    assert!(cancelled);
                    true
                } else {
                    // Fixed code: B cannot even insert X while A holds X/Y.
                    // Both gates are open; committing A is the only unblocker.
                    false
                }
            },
        );
        let attempts = (sequence(&pg.gate, "a_attempts").await, sequence(&pg.gate, "b_attempts").await);
        eprintln!("cycle={cycle}; attempts={attempts:?}; A={a_result:?}; B={b_result:?}");
        assert!(!cycle, "observed real PG residency deadlock: A(source X/output Y) waits for B(X), B(outputs X/Y) waits for A(Y); {a_result:?}");
        assert_eq!(attempts, ((1, true), (1, true)), "transaction retry must not make this regression green");
        assert!(matches!(a_result, Ok(ZipV2PublicationResult::Published(ref rows)) if rows.len() == 2));
        assert!(matches!(b_result, Ok(ZipV2PublicationResult::Published(ref rows)) if rows.len() == 2));
        assert_bindings(&pg.gate, &a).await;
        assert_bindings(&pg.gate, &b).await;
        for (table, expected) in [("objects", 4), ("object_versions", 4), ("version_residencies", 4),
            ("residency_references", 4), ("physical_residencies", 2), ("pin_leases", 0), ("standard_mutation_leases", 0)] {
            let actual: i64 = pg.gate.query_one(sql(&format!("SELECT count(*) AS n FROM {table}")))
                .await.unwrap().unwrap().try_get("", "n").unwrap();
            assert_eq!(actual, expected, "unexpected {table} count");
        }
        let active: i64 = pg.gate.query_one(sql("SELECT count(*) AS n FROM import_destinations WHERE mutation_id IS NOT NULL"))
            .await.unwrap().unwrap().try_get("", "n").unwrap();
        assert_eq!(active, 0, "publication left an exact ownership guard active");
    }).await;
}

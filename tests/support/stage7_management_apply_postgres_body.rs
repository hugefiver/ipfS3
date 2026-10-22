use ipfs_s3_gateway::{
    config::Config,
    kubo::KuboClient,
    pinning::management::*,
    store::pinning::management::{native, records},
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

pub struct Fixture {
    pub db: DatabaseConnection,
    pub writer: DatabaseConnection,
    pub server: MockServer,
    pub config: local_config::ManagementConfig,
    pub auth: records::LocalAuthorization,
    raw: Config,
    schema: String,
    schema_oid: i64,
    fault_sequence: bool,
    fault_function: bool,
    fault_trigger: bool,
}

impl Fixture {
    pub async fn new() -> Self {
        let url = std::env::var("IPFS_S3_STAGE7_APPLY_DATABASE_URL")
            .expect("parent must supply an empty, exclusive Stage7 PostgreSQL schema URL");
        let parsed = url::Url::parse(&url).expect("valid parent-selected database URL required");
        assert!(matches!(parsed.scheme(), "postgres" | "postgresql"));
        let db = connect(&url).await;
        let (schema, schema_oid) = scope(&db).await;
        // One runner may claim this parent-owned scope. No global fixture reset.
        let claimed: bool = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
            "SELECT pg_try_advisory_lock(hashtextextended(current_database() || ':' || current_schema(), 0)) AS claimed"))
            .await.unwrap().unwrap().try_get("", "claimed").unwrap();
        assert!(claimed, "another runner owns this Stage7 apply scope");
        let empty: bool = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
            "SELECT NOT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace)
                AND NOT EXISTS (SELECT 1 FROM pg_proc WHERE pronamespace=current_schema()::regnamespace)
                AND NOT EXISTS (SELECT 1 FROM pg_type WHERE typnamespace=current_schema()::regnamespace) AS empty"))
            .await.unwrap().unwrap().try_get("", "empty").unwrap();
        assert!(
            empty,
            "preserve occupied scope; parent must supply a fresh empty schema"
        );
        let writer = connect(&url).await;
        assert_eq!(scope(&writer).await, (schema.clone(), schema_oid));
        eprintln!(
            "native PG fixture: empty exclusive schema={schema} namespace_oid={schema_oid}; registered migrator starting"
        );

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"ID": foundation::NODE})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("offline", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
            .mount(&server)
            .await;
        let raw = foundation::config(&server.uri());
        let config = foundation::management_config(&raw);
        // Public fixture setup invokes the complete registered production migrator.
        foundation::setup(&db, &config).await;
        let auth = records::authorize_local(&raw, &db).await.unwrap();
        assert_eq!(
            native::database_identity(&writer).await.unwrap(),
            auth.database_identity()
        );
        Self {
            db,
            writer,
            server,
            config,
            auth,
            raw,
            schema,
            schema_oid,
            fault_sequence: false,
            fault_function: false,
            fault_trigger: false,
        }
    }

    pub async fn publish(&self, key: &str) -> ExactObject {
        let identity = foundation::publish(&self.db, &self.config, key, false).await;
        foundation::write_current_decision(&self.db, &self.config, &identity).await;
        identity
    }

    pub fn proof_request(&self, identity: &ExactObject) -> evidence::PrepareRequest {
        evidence::PrepareRequest {
            record_id: uuid::Uuid::new_v4().to_string(),
            identity: identity.clone(),
            expected_node: foundation::NODE.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(15),
            directory: false,
        }
    }

    pub async fn prepare(&self, request: &evidence::PrepareRequest) -> evidence::ReadAttestation {
        let before = self.request_counts().await;
        let proof = evidence::prepare(
            &self.db,
            &self.config,
            &self.auth,
            &KuboClient::new(self.server.uri()),
            request,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(proof.stored_bytes, Some(10));
        assert_eq!(
            self.request_counts().await,
            RequestCounts {
                ids: before.ids + 2,
                cats: before.cats + 1,
                total: before.total + 3,
            }
        );
        proof
    }

    pub async fn prepared_object(&self, key: &str) -> ExactObject {
        let identity = self.publish(key).await;
        self.prepare(&self.proof_request(&identity)).await;
        identity
    }

    pub fn create_request(&self, identity: ExactObject) -> PlanRequest {
        let mut request = foundation::request(identity, &self.config, false);
        request.plan_id = uuid::Uuid::new_v4().to_string();
        request
    }

    pub async fn confirm(&self, request: &PlanRequest) -> records::ConfirmedBinding {
        let before = native::capture(&self.db).await.unwrap();
        let calls = self.request_counts().await;
        let snapshot = db_snapshot::collect_with_config(&self.db, &self.config, request)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
        let plan = dry_run(request, &snapshot).unwrap();
        assert!(
            plan.items()
                .iter()
                .all(|item| item.status == PlanStatus::Ready)
        );
        assert!(
            native::capture(&self.db).await.unwrap() == before,
            "planning must be read-only"
        );
        let binding = records::confirm(
            &self.db,
            &self.config,
            &self.auth,
            (request, &snapshot, &plan),
            vec![],
            chrono::Duration::minutes(5),
        )
        .await
        .unwrap();
        assert_eq!(self.request_counts().await, calls);
        binding
    }

    pub async fn reauthorize_writer(&self) -> records::LocalAuthorization {
        records::authorize_local(&self.raw, &self.writer)
            .await
            .unwrap()
    }

    pub async fn request_counts(&self) -> RequestCounts {
        let requests = self.server.received_requests().await.unwrap();
        let ids = requests
            .iter()
            .filter(|r| r.url.path() == "/api/v0/id")
            .count();
        let cats = requests
            .iter()
            .filter(|r| r.url.path() == "/api/v0/cat")
            .count();
        assert!(requests.iter().all(|r| {
            r.method == "POST"
                && (r.url.path() == "/api/v0/id"
                    || (r.url.path() == "/api/v0/cat"
                        && r.url
                            .query_pairs()
                            .any(|(k, v)| k == "offline" && v == "true")))
        }));
        assert_eq!(ids + cats, requests.len(), "no provider, unpin or GC calls");
        RequestCounts {
            ids,
            cats,
            total: requests.len(),
        }
    }

    /// A direct receipt-table trigger is forbidden by native schema validation.
    /// This deferred lease trigger fails COMMIT only after the entire receipt,
    /// lease and job batch is visible in the same transaction. nextval survives
    /// rollback and distinguishes a real publication fault from early rejection.
    pub async fn install_receipt_fault(&mut self, plan_id: &str) {
        let plan_id = uuid::Uuid::parse_str(plan_id).unwrap().to_string();
        self.db
            .execute_unprepared("CREATE SEQUENCE stage7_management_apply_postgres_receipt_hit")
            .await
            .unwrap();
        self.fault_sequence = true;
        self.db.execute_unprepared(r#"
            CREATE FUNCTION stage7_management_apply_postgres_receipt_fault() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF (SELECT count(*) FROM pin_management_records
                    WHERE plan_id=TG_ARGV[0] AND kind='action' AND state='applied'
                        AND result IS NOT NULL AND completed_at IS NOT NULL)=2
                    AND (SELECT count(*) FROM pin_leases l WHERE l.id IN (
                        SELECT jsonb_array_elements_text(intent_ids::jsonb) FROM pin_management_records
                        WHERE plan_id=TG_ARGV[0] AND kind='action'))=2
                    AND EXISTS (SELECT 1 FROM pin_jobs j WHERE j.id IN (
                        SELECT jsonb_array_elements_text(job_ids::jsonb) FROM pin_management_records
                        WHERE plan_id=TG_ARGV[0] AND kind='action'))
                    AND NOT EXISTS (SELECT 1 FROM pin_management_records r
                        WHERE r.plan_id=TG_ARGV[0] AND r.kind='action' AND
                            (jsonb_array_length(r.job_ids::jsonb)=0 OR EXISTS (
                                SELECT 1 FROM jsonb_array_elements_text(r.job_ids::jsonb) AS ids(id)
                                LEFT JOIN pin_jobs j ON j.id=ids.id WHERE j.id IS NULL)))
                THEN
                    PERFORM nextval('stage7_management_apply_postgres_receipt_hit');
                    RAISE EXCEPTION USING ERRCODE='P0001', MESSAGE='Stage7 fixture publication fault';
                END IF;
                RETURN NEW;
            END $$"#).await.unwrap();
        self.fault_function = true;
        self.db
            .execute_unprepared(&format!(
                "CREATE CONSTRAINT TRIGGER stage7_management_apply_postgres_receipt_fault
                AFTER INSERT ON pin_leases DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
                EXECUTE FUNCTION stage7_management_apply_postgres_receipt_fault('{plan_id}')"
            ))
            .await
            .unwrap();
        self.fault_trigger = true;
    }

    pub async fn remove_receipt_fault(&mut self) {
        assert_eq!(
            scope(&self.db).await,
            (self.schema.clone(), self.schema_oid)
        );
        // Only exact objects successfully created by this fixture are removed.
        if self.fault_trigger {
            self.db
                .execute_unprepared(
                    "DROP TRIGGER stage7_management_apply_postgres_receipt_fault ON pin_leases",
                )
                .await
                .unwrap();
            self.fault_trigger = false;
        }
        if self.fault_function {
            self.db
                .execute_unprepared(
                    "DROP FUNCTION stage7_management_apply_postgres_receipt_fault()",
                )
                .await
                .unwrap();
            self.fault_function = false;
        }
        if self.fault_sequence {
            self.db
                .execute_unprepared("DROP SEQUENCE stage7_management_apply_postgres_receipt_hit")
                .await
                .unwrap();
            self.fault_sequence = false;
        }
    }

    pub async fn close(mut self) {
        self.remove_receipt_fault().await;
        self.writer.close().await.unwrap();
        self.db.close().await.unwrap();
        // Keep migrated schema and business rows for parent inspection/disposal.
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestCounts {
    pub ids: usize,
    pub cats: usize,
    pub total: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub leases: i64,
    pub targets: i64,
    pub requirements: i64,
    pub jobs: i64,
    pub remotes: i64,
    pub applied: i64,
    pub reserved_bytes: i64,
    pub reserved_pins: i64,
}

pub async fn counts(db: &DatabaseConnection) -> Counts {
    let row = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT (SELECT count(*) FROM pin_leases) AS leases,
            (SELECT count(*) FROM pin_lease_targets) AS targets,
            (SELECT count(*) FROM pin_lease_requirements) AS requirements,
            (SELECT count(*) FROM pin_jobs) AS jobs,
            (SELECT count(*) FROM remote_pins) AS remotes,
            (SELECT count(*) FROM pin_management_records WHERE kind='action' AND state='applied') AS applied,
            (SELECT coalesce(sum(reserved_bytes),0)::bigint FROM pin_provider_usage) AS reserved_bytes,
            (SELECT coalesce(sum(reserved_pins),0)::bigint FROM pin_provider_usage) AS reserved_pins"))
        .await.unwrap().unwrap();
    Counts {
        leases: row.try_get("", "leases").unwrap(),
        targets: row.try_get("", "targets").unwrap(),
        requirements: row.try_get("", "requirements").unwrap(),
        jobs: row.try_get("", "jobs").unwrap(),
        remotes: row.try_get("", "remotes").unwrap(),
        applied: row.try_get("", "applied").unwrap(),
        reserved_bytes: row.try_get("", "reserved_bytes").unwrap(),
        reserved_pins: row.try_get("", "reserved_pins").unwrap(),
    }
}

async fn connect(url: &str) -> DatabaseConnection {
    let mut options = ConnectOptions::new(url);
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    let db = Database::connect(options)
        .await
        .unwrap_or_else(|_| panic!("parent PostgreSQL scope unavailable"));
    assert_eq!(db.get_database_backend(), DatabaseBackend::Postgres);
    db.execute_unprepared("SET TIME ZONE 'UTC'").await.unwrap();
    db.execute_unprepared("SET statement_timeout='8s'")
        .await
        .unwrap();
    db
}

async fn scope(db: &DatabaseConnection) -> (String, i64) {
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT current_schema() AS scope, n.oid::bigint AS oid,
            n.nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) AS owned,
            current_schemas(false)=ARRAY[current_schema()]::name[] AS exclusive
            FROM pg_namespace n WHERE n.nspname=current_schema()",
        ))
        .await
        .unwrap()
        .expect("parent must select an existing schema");
    let schema: String = row.try_get("", "scope").unwrap();
    assert!(
        schema
            .strip_prefix("stage7_")
            .is_some_and(|suffix| !suffix.is_empty()
                && suffix
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'))
            && schema.len() <= 63,
        "refuse non-Stage7 scope"
    );
    assert!(
        row.try_get::<bool>("", "owned").unwrap(),
        "parent fixture must own its schema"
    );
    assert!(
        row.try_get::<bool>("", "exclusive").unwrap(),
        "search_path must select only the isolated schema"
    );
    (schema, row.try_get("", "oid").unwrap())
}

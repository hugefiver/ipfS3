use super::*;
use crate::store::entities::{bucket, multipart_part, pin_replica_observation, remote_pin_ledger};
use sea_orm::{
    Database, DatabaseBackend, DatabaseConnection, DbErr, ExecResult, QueryResult, Schema,
};
use std::sync::Mutex;

struct RecordingConnection<'a> {
    inner: &'a DatabaseConnection,
    backend: DatabaseBackend,
    statements: Mutex<Vec<Statement>>,
}

impl<'a> RecordingConnection<'a> {
    fn new(inner: &'a DatabaseConnection, backend: DatabaseBackend) -> Self {
        Self {
            inner,
            backend,
            statements: Mutex::default(),
        }
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for RecordingConnection<'_> {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.backend
    }

    async fn execute(&self, statement: Statement) -> std::result::Result<ExecResult, DbErr> {
        self.inner.execute(statement).await
    }

    async fn execute_unprepared(&self, sql: &str) -> std::result::Result<ExecResult, DbErr> {
        self.inner.execute_unprepared(sql).await
    }

    async fn query_one(
        &self,
        statement: Statement,
    ) -> std::result::Result<Option<QueryResult>, DbErr> {
        self.statements.lock().unwrap().push(statement.clone());
        if self.backend == DatabaseBackend::Postgres {
            return Err(DbErr::Custom("SQL capture only".into()));
        }
        self.inner.query_one(statement).await
    }

    async fn query_all(
        &self,
        statement: Statement,
    ) -> std::result::Result<Vec<QueryResult>, DbErr> {
        self.statements.lock().unwrap().push(statement.clone());
        self.inner.query_all(statement).await
    }
}

async fn lookup<E, C>(db: &C, key: &[&str]) -> Result<Option<E::Model>>
where
    E: EntityTrait + Default,
    E::Model: FromQueryResult,
    C: ConnectionTrait,
{
    current_model::<E, _>(db, key, DecodeWork::shared(&SharedDecodeBudget::default())).await
}

async fn create_table<E: EntityTrait>(db: &DatabaseConnection, entity: E) {
    let backend = db.get_database_backend();
    db.execute(backend.build(&Schema::new(backend).create_table_from_entity(entity)))
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_text_composite_key_searches_cover_budget_read_and_guards() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    create_table(&db, remote_pin_ledger::Entity).await;
    db.execute_unprepared(
        "INSERT INTO remote_pin_ledger(provider,cid,ownership,effect) VALUES ('01','1','owned','pinned')",
    )
    .await
    .unwrap();
    let recording = RecordingConnection::new(&db, DatabaseBackend::Sqlite);
    for (key, present) in [
        (["01", "1"], true),
        (["1", "1"], false),
        (["01", "01"], false),
    ] {
        let row = lookup::<remote_pin_ledger::Entity, _>(&recording, &key)
            .await
            .unwrap();
        assert_eq!(row.is_some(), present);
        if let Some(row) = row {
            assert_eq!((row.provider.as_str(), row.cid.as_str()), (key[0], key[1]));
        }
    }
    let statements = recording.statements.lock().unwrap().clone();
    assert_eq!(statements.len(), 6);
    for pair in statements.as_chunks::<2>().0 {
        for (statement, expected_accesses) in pair.iter().zip([1, 3]) {
            let mut explain = statement.clone();
            explain.sql = format!("EXPLAIN QUERY PLAN {}", explain.sql);
            let plans = db.query_all(explain).await.unwrap();
            let accesses = plans
                .iter()
                .map(|row| row.try_get::<String>("", "detail").unwrap())
                .filter(|detail| detail.contains("remote_pin_ledger"))
                .collect::<Vec<_>>();
            assert_eq!(accesses.len(), expected_accesses, "{accesses:?}");
            for detail in accesses {
                assert!(
                    detail.starts_with("SEARCH remote_pin_ledger ")
                        && detail.contains("provider=?")
                        && detail.contains("cid=?"),
                    "both primary-key columns must constrain each table access: {detail}"
                );
            }
        }
    }
}

#[tokio::test]
async fn postgres_keys_keep_cast_text_predicates_and_bound_values() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let recording = RecordingConnection::new(&db, DatabaseBackend::Postgres);
    assert!(matches!(
        lookup::<remote_pin_ledger::Entity, _>(&recording, &["01", "1"]).await,
        Err(ManagementError::Database)
    ));
    assert!(matches!(
        lookup::<multipart_part::Entity, _>(&recording, &["01", "1"]).await,
        Err(ManagementError::Database)
    ));
    assert!(matches!(
        lookup::<pin_replica_observation::Entity, _>(&recording, &["1"]).await,
        Err(ManagementError::Database)
    ));
    let statements = recording.statements.lock().unwrap();
    assert_eq!(statements.len(), 3);
    for (statement, predicate, values) in [
        (
            &statements[0],
            "CAST(\"provider\" AS TEXT)=$1 AND CAST(\"cid\" AS TEXT)=$2",
            vec!["01".into(), "1".into()],
        ),
        (
            &statements[1],
            "CAST(\"upload_id\" AS TEXT)=$1 AND CAST(\"part_number\" AS TEXT)=$2",
            vec!["01".into(), "1".into()],
        ),
        (
            &statements[2],
            "CAST(\"sequence\" AS TEXT)=$1",
            vec!["1".into()],
        ),
    ] {
        assert_eq!(statement.sql.split_once(" WHERE ").unwrap().1, predicate);
        assert_eq!(statement.values.as_ref().unwrap().0, values);
    }
}

#[tokio::test]
async fn sqlite_native_keys_preserve_text_and_integer_string_identity() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    create_table(&db, bucket::Entity).await;
    create_table(&db, multipart_part::Entity).await;
    create_table(&db, pin_replica_observation::Entity).await;
    db.execute_unprepared(
        "INSERT INTO buckets(name,created_at) VALUES ('01','2026-01-01T00:00:00Z');
         INSERT INTO multipart_parts(upload_id,part_number,cid,size,etag,uploaded_at)
         VALUES ('01',1,'cid',1,'etag','2026-01-01T00:00:00Z');
         INSERT INTO pin_replica_observations(sequence,job_id,provider,expected_cid,route,
         remote_epoch,claim_until,phase,resources,outcome,projectable,observed_at)
         VALUES (1,'job','provider','cid','route',1,'2026-01-01T00:00:00Z',
         'phase','[]','outcome',0,'2026-01-01T00:00:00Z')",
    )
    .await
    .unwrap();
    let bucket = lookup::<bucket::Entity, _>(&db, &["01"])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bucket.name, "01");
    assert!(
        lookup::<bucket::Entity, _>(&db, &["1"])
            .await
            .unwrap()
            .is_none()
    );
    let observation = lookup::<pin_replica_observation::Entity, _>(&db, &["1"])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observation.sequence, 1);
    let part = lookup::<multipart_part::Entity, _>(&db, &["01", "1"])
        .await
        .unwrap()
        .unwrap();
    assert_eq!((part.upload_id.as_str(), part.part_number), ("01", 1));
    for number in ["01", "+1", "1.0", "2"] {
        assert!(
            lookup::<pin_replica_observation::Entity, _>(&db, &[number])
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            lookup::<multipart_part::Entity, _>(&db, &["01", number])
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(
        lookup::<multipart_part::Entity, _>(&db, &["1", "1"])
            .await
            .unwrap()
            .is_none()
    );
    let recording = RecordingConnection::new(&db, DatabaseBackend::Sqlite);
    for key in [&[][..], &["01"][..], &["01", "1", "extra"][..]] {
        assert!(matches!(
            lookup::<multipart_part::Entity, _>(&recording, key).await,
            Err(ManagementError::Schema)
        ));
    }
    assert!(recording.statements.lock().unwrap().is_empty());
}

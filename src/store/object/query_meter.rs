use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::BTreeSet, sync::Mutex};

use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, ExecResult, QueryResult, Statement};

pub(crate) struct QueryMeter<'a, C> {
    pub inner: &'a C,
    pub reads: AtomicUsize,
    pub rows: AtomicUsize,
    text_bytes: AtomicUsize,
    max_rows: AtomicUsize,
    fields: Mutex<BTreeSet<&'static str>>,
}

impl<'a, C> QueryMeter<'a, C> {
    pub fn new(inner: &'a C) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
            rows: AtomicUsize::new(0),
            text_bytes: AtomicUsize::new(0),
            max_rows: AtomicUsize::new(0),
            fields: Mutex::new(BTreeSet::new()),
        }
    }

    pub fn counts(&self) -> (usize, usize) {
        (
            self.reads.load(Ordering::Relaxed),
            self.rows.load(Ordering::Relaxed),
        )
    }

    pub fn text_bytes(&self) -> usize {
        self.text_bytes.load(Ordering::Relaxed)
    }

    pub fn max_rows(&self) -> usize {
        self.max_rows.load(Ordering::Relaxed)
    }

    pub fn fields(&self) -> BTreeSet<&'static str> {
        self.fields.lock().unwrap().clone()
    }

    fn record(&self, row: &QueryResult) {
        let mut fields = self.fields.lock().unwrap();
        let mut bytes = 0;
        for field in [
            "id",
            "bucket",
            "key",
            "cid",
            "etag",
            "content_type",
            "key_wrap",
            "sse_c_key_fingerprint",
        ] {
            if let Ok(value) = row.try_get::<String>("", field) {
                fields.insert(field);
                bytes += value.len();
            }
        }
        if let Ok(value) = row.try_get::<serde_json::Value>("", "metadata") {
            fields.insert("metadata");
            bytes += value.to_string().len();
        }
        if row.try_get::<i64>("", "size").is_ok() {
            fields.insert("size");
        }
        if row
            .try_get::<chrono::DateTime<chrono::Utc>>("", "created_at")
            .is_ok()
        {
            fields.insert("created_at");
        }
        for field in ["encrypted", "multipart", "is_latest"] {
            if row.try_get::<bool>("", field).is_ok() {
                fields.insert(field);
            }
        }
        self.text_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl<C: ConnectionTrait> ConnectionTrait for QueryMeter<'_, C> {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.inner.get_database_backend()
    }

    async fn execute(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        self.inner.execute(statement).await
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.inner.execute_unprepared(sql).await
    }

    async fn query_one(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let row = self.inner.query_one(statement).await?;
        self.rows
            .fetch_add(usize::from(row.is_some()), Ordering::Relaxed);
        if let Some(row) = &row {
            self.record(row);
            self.max_rows.fetch_max(1, Ordering::Relaxed);
        }
        Ok(row)
    }

    async fn query_all(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let rows = self.inner.query_all(statement).await?;
        self.rows.fetch_add(rows.len(), Ordering::Relaxed);
        self.max_rows.fetch_max(rows.len(), Ordering::Relaxed);
        for row in &rows {
            self.record(row);
        }
        Ok(rows)
    }
}

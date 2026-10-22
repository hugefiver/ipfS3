use super::*;

#[tokio::test]
async fn shared_frozen_index_rechecks_current_private_principal() {
    use native::scope::{ScopeSpec, Selector, collect_scope};

    let fixture =
        crate::store::pinning::management::actions::tests::fixture("frozen-principal").await;
    let work = SharedDecodeBudget::default();
    let mut selectors = vec![Selector::RecordMembers {
        database_scope: fixture.auth.database_scope().into(),
        plan_id: fixture.binding.request.plan_id.clone(),
    }];
    selectors.extend(
        FrozenRows::Archive(fixture.binding.archived_rows())
            .all_rows()
            .filter(|(table, row)| {
                table.table == super::super::super::schema::RECORDS
                    && row.cell("kind") == Some("action")
            })
            .map(|(_, row)| Selector::RecordMembers {
                database_scope: row.cell("database_scope").unwrap().into(),
                plan_id: row.cell("plan_id").unwrap().into(),
            }),
    );
    let spec = ScopeSpec::new(selectors).unwrap();
    let mut budget = native::meter::Budget::default();
    let rows = collect_scope(&fixture.db, &spec, &mut budget)
        .await
        .unwrap()
        .into_native_frame(&mut budget)
        .unwrap();
    let source = Evidence::from_frozen_with_work(FrozenRows::Frame(&rows), work.clone()).unwrap();
    let root = source
        .archive_members(
            fixture.auth.database_scope(),
            &fixture.binding.request.plan_id,
        )
        .next()
        .unwrap();
    let restored = source.read_binding(root).unwrap();
    assert!(restored == fixture.binding);
    assert_eq!(native::value(root, "state"), Some("prepared"));
    assert_eq!(root.get("result"), Some(&None));
    assert_eq!(root.get("completed_at"), Some(&None));
    assert_eq!(
        restored.original_binding().unwrap(),
        native::value(root, "binding").unwrap()
    );
    let key = anchor_key(&fixture.binding).unwrap();
    assert!(
        FrozenRows::Frame(&rows)
            .row_key(&key.table, &key.values)
            .unwrap()
            == *root
    );
    let usage = work.lock().unwrap().usage();
    assert!(usage.0 > 0 && usage.1 > 0);
    let exhausted = SharedDecodeBudget::default();
    exhausted
        .lock()
        .unwrap()
        .consume(0, crate::pinning::management::wire::MAX_WIRE_BYTES - 1)
        .unwrap();
    assert!(matches!(
        Evidence::from_frozen_with_work(FrozenRows::Frame(&rows), exhausted),
        Err(ManagementError::Budget)
    ));
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_management_records SET principal='other-principal' WHERE plan_id=$1",
            [fixture.binding.request.plan_id.clone().into()],
        ))
        .await
        .unwrap();
    let changed = collect_scope(&fixture.db, &spec, &mut budget)
        .await
        .unwrap()
        .into_native_frame(&mut budget)
        .unwrap();
    let source = Evidence::from_frozen_with_work(FrozenRows::Frame(&changed), work).unwrap();
    let root = source
        .archive_members(
            fixture.auth.database_scope(),
            &fixture.binding.request.plan_id,
        )
        .next()
        .unwrap();
    assert!(matches!(
        source.read_binding(root),
        Err(ManagementError::Unauthorized)
    ));
}
use crate::pinning::management::{self as management, db_snapshot};
use crate::store::pinning::management::actions;
use sea_orm::{DatabaseBackend, DbErr, ExecResult, QueryResult, TransactionTrait};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct RecordingConnection<'a, C> {
    inner: &'a C,
    raw_bytes: AtomicUsize,
    locator_bytes: AtomicUsize,
    max_chunk_bytes: AtomicUsize,
    replacement: Option<String>,
    replaced: AtomicBool,
}
impl<C> RecordingConnection<'_, C> {
    fn new(inner: &C) -> RecordingConnection<'_, C> {
        RecordingConnection {
            inner,
            raw_bytes: AtomicUsize::new(0),
            locator_bytes: AtomicUsize::new(0),
            max_chunk_bytes: AtomicUsize::new(0),
            replacement: None,
            replaced: AtomicBool::new(false),
        }
    }
    fn observe(&self, rows: &[QueryResult]) {
        for row in rows {
            if let Ok(chunk) = row.try_get::<Vec<u8>>("", "chunk") {
                self.raw_bytes.fetch_add(chunk.len(), Ordering::Relaxed);
                self.max_chunk_bytes
                    .fetch_max(chunk.len(), Ordering::Relaxed);
            }
            for column in [
                "binding",
                "native_before",
                "result",
                "intent_ids",
                "job_ids",
            ] {
                if let Ok(Some(value)) = row.try_get::<Option<String>>("", column) {
                    self.raw_bytes.fetch_add(value.len(), Ordering::Relaxed);
                }
            }
            for column in ["database_scope", "plan_id", "item_id", "kind", "id"] {
                if let Ok(Some(value)) = row.try_get::<Option<String>>("", column) {
                    self.locator_bytes.fetch_add(value.len(), Ordering::Relaxed);
                }
            }
        }
    }
}
#[async_trait::async_trait]
impl<C: ConnectionTrait> ConnectionTrait for RecordingConnection<'_, C> {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.inner.get_database_backend()
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
        let row = self.inner.query_one(statement).await?;
        self.observe(row.as_slice());
        Ok(row)
    }
    async fn query_all(
        &self,
        statement: Statement,
    ) -> std::result::Result<Vec<QueryResult>, DbErr> {
        let lengths = statement.sql.contains("AS n0")
            && statement.sql.contains("FROM \"pin_management_records\"");
        let rows = self.inner.query_all(statement).await?;
        if lengths
            && let Some(plan) = &self.replacement
            && !self.replaced.swap(true, Ordering::Relaxed)
        {
            self.inner
                .execute(Statement::from_sql_and_values(
                    self.get_database_backend(),
                    "UPDATE pin_management_records SET binding=binding || $1 WHERE plan_id=$2 AND kind='action'",
                    [
                        " ".repeat(native::meter::PAGE_BYTES + 1).into(),
                        plan.clone().into(),
                    ],
                ))
                .await?;
        }
        self.observe(&rows);
        Ok(rows)
    }
}

#[tokio::test]
async fn submit_id_projection_rejects_oversized_cells_before_driver_transfer() {
    let fixture = actions::tests::fixture("index-submit-id-budget").await;
    let applied = actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let tx = fixture.db.begin().await.unwrap();
    tx.execute_unprepared("PRAGMA defer_foreign_keys=ON")
        .await
        .unwrap();
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE pin_jobs SET id=$1 WHERE id=$2",
        [
            "x".repeat(native::MAX_BYTES + 1).into(),
            applied.items[0].jobs[0].job_id.clone().into(),
        ],
    ))
    .await
    .unwrap();
    let recording = RecordingConnection::new(&tx);
    assert!(matches!(
        index::bounded_projection(
            &recording,
            "FROM pin_jobs WHERE operation='submit'",
            &[("id", "id")],
            "id",
            vec![],
            (native::MAX_ROWS, native::MAX_BYTES),
            DecodeWork::shared(&SharedDecodeBudget::default())
        )
        .await,
        Err(ManagementError::Budget)
    ));
    assert_eq!(recording.locator_bytes.load(Ordering::Relaxed), 0);
    tx.rollback().await.unwrap();
    let (rows, bytes) = index::bounded_projection(
        &fixture.db,
        "FROM pin_jobs WHERE operation='submit'",
        &[("id", "id")],
        "id",
        vec![],
        (native::MAX_ROWS, native::MAX_BYTES),
        DecodeWork::shared(&SharedDecodeBudget::default()),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(bytes, applied.items[0].jobs[0].job_id.len());
    assert_eq!(
        rows[0].try_get::<String>("", "id").unwrap(),
        applied.items[0].jobs[0].job_id
    );
}

#[tokio::test]
async fn scoped_read_rejects_oversized_private_cells_before_driver_transfer() {
    let fixture = actions::tests::fixture("scoped-raw-budget").await;
    let tx = fixture.db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE pin_management_records SET binding=binding || $1 WHERE plan_id=$2 AND kind='action'",
        [
            " ".repeat(native::meter::PAGE_BYTES + 1).into(),
            fixture.binding.request.plan_id.clone().into(),
        ],
    ))
    .await
    .unwrap();
    let recording = RecordingConnection::new(&tx);
    assert!(matches!(
        Evidence::load(
            &recording,
            &fixture.binding.database_scope,
            &fixture.binding.database_identity,
            std::slice::from_ref(&fixture.binding.request.plan_id)
        )
        .await,
        Err(ManagementError::Budget)
    ));
    assert_eq!(
        recording.raw_bytes.load(Ordering::Relaxed),
        0,
        "oversized cells must not reach the driver"
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn dependencies_preserve_originals_and_recheck_cached_suppliers() {
    let fixture = actions::tests::fixture("dependency-original").await;
    actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "dependency-consumer".into();
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = management::dry_run(&request, &snapshot).unwrap();
    let binding = super::super::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &plan),
        plan.items()
            .iter()
            .filter(|entry| entry.status == management::PlanStatus::Candidate)
            .map(|entry| entry.request.item_id.clone())
            .collect(),
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    let plans = std::slice::from_ref(&binding.request.plan_id);
    let mut source = Evidence::load_roots(
        &fixture.db,
        &binding.database_scope,
        &binding.database_identity,
        plans,
        &[],
    )
    .await
    .unwrap();
    let root = source.row(&anchor_key(&binding).unwrap()).unwrap().clone();
    let dependency_key = anchor_key(&fixture.binding).unwrap();
    let dependencies =
        archive::dependencies(Arc::new(root.clone()), &mut source.budget().unwrap()).unwrap();
    assert!(dependencies.contains(&dependency_key));
    assert!(!source.rows.contains_key(&dependency_key));
    let recording = RecordingConnection::new(&fixture.db);
    source.load_dependencies(&recording, plans).await.unwrap();
    assert!(recording.raw_bytes.load(Ordering::Relaxed) > 0);
    let dependency = source.row(&dependency_key).unwrap().clone();
    let restored = source.read_binding(&root).unwrap();
    assert!(restored == binding);
    let refs = dependency_key
        .values
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        restored
            .review()
            .original_value(&dependency_key.table, &refs, "native_before"),
        native::value(&dependency, "native_before")
    );
    let bytes = source.bytes;
    let transferred = recording.raw_bytes.load(Ordering::Relaxed);
    source.load_dependencies(&recording, plans).await.unwrap();
    assert_eq!(source.bytes, bytes);
    assert_eq!(recording.raw_bytes.load(Ordering::Relaxed), transferred);
    Arc::make_mut(&mut source.rows).remove(&dependency_key);
    assert!(matches!(
        source.read_binding(&root),
        Err(ManagementError::InvalidEvidence)
    ));
    let mut changed = dependency.clone();
    changed.insert("principal".into(), Some("other-principal".into()));
    Arc::make_mut(&mut source.rows).insert(dependency_key.clone(), Arc::new(changed));
    assert!(matches!(
        source.read_binding(&root),
        Err(ManagementError::Unauthorized)
    ));
    Arc::make_mut(&mut source.rows).insert(dependency_key, Arc::new(dependency));
    assert!(source.read_binding(&root).unwrap() == binding);

    let mut limited = Evidence::load_roots(
        &fixture.db,
        &binding.database_scope,
        &binding.database_identity,
        plans,
        &[],
    )
    .await
    .unwrap();
    let used = limited.work.lock().unwrap().usage().1;
    limited
        .work
        .lock()
        .unwrap()
        .consume(
            0,
            crate::pinning::management::wire::MAX_WIRE_BYTES - used - 1,
        )
        .unwrap();
    let recording = RecordingConnection::new(&fixture.db);
    assert!(matches!(
        limited.load_dependencies(&recording, plans).await,
        Err(ManagementError::Budget)
    ));
    assert_eq!(recording.raw_bytes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn actual_transfer_rechecks_a_replaced_row_against_remaining_raw_budget() {
    let fixture = actions::tests::fixture("lookup-replaced-cell").await;
    let tx = fixture.db.begin().await.unwrap();
    let mut recording = RecordingConnection::new(&tx);
    recording.replacement = Some(fixture.binding.request.plan_id.clone());
    assert!(matches!(
        Evidence::load(
            &recording,
            &fixture.binding.database_scope,
            &fixture.binding.database_identity,
            std::slice::from_ref(&fixture.binding.request.plan_id)
        )
        .await,
        Err(ManagementError::Changed)
    ));
    assert!(recording.replaced.load(Ordering::Relaxed));
    assert!(recording.raw_bytes.load(Ordering::Relaxed) > 0);
    assert!(recording.max_chunk_bytes.load(Ordering::Relaxed) <= native::meter::CELL_SLICE);
    tx.rollback().await.unwrap();
    let source = Evidence::load(
        &fixture.db,
        &fixture.binding.database_scope,
        &fixture.binding.database_identity,
        std::slice::from_ref(&fixture.binding.request.plan_id),
    )
    .await
    .unwrap();
    assert!(
        source
            .read_binding(source.row(&anchor_key(&fixture.binding).unwrap()).unwrap())
            .unwrap()
            == fixture.binding
    );
}

use super::*;

#[tokio::test]
async fn shared_frozen_index_rechecks_current_private_principal() {
    let fixture =
        crate::store::pinning::management::actions::tests::fixture("frozen-principal").await;
    let work = SharedDecodeBudget::default();
    let rows = CurrentRows::capture(&fixture.db, DecodeWork::shared(&work))
        .await
        .unwrap();
    let source = Evidence::from_frozen_with_work(FrozenRows::Current(&rows), work.clone()).unwrap();
    let root = source
        .archive_members(
            fixture.auth.database_scope(),
            &fixture.binding.request.plan_id,
        )
        .next()
        .unwrap();
    assert!(source.read_binding(root).is_ok());
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_management_records SET principal='other-principal' WHERE plan_id=$1",
            [fixture.binding.request.plan_id.clone().into()],
        ))
        .await
        .unwrap();
    let changed = CurrentRows::capture(&fixture.db, DecodeWork::shared(&work))
        .await
        .unwrap();
    let source = Evidence::from_frozen_with_work(FrozenRows::Current(&changed), work).unwrap();
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
    replacement: Option<String>,
    replace_locator: bool,
    replaced: AtomicBool,
}
impl<C> RecordingConnection<'_, C> {
    fn new(inner: &C) -> RecordingConnection<'_, C> {
        RecordingConnection {
            inner,
            raw_bytes: AtomicUsize::new(0),
            locator_bytes: AtomicUsize::new(0),
            replacement: None,
            replace_locator: false,
            replaced: AtomicBool::new(false),
        }
    }
    fn observe(&self, rows: &[QueryResult]) {
        for row in rows {
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
        if let Some(plan) = &self.replacement
            && row.as_ref().is_some_and(|row| {
                row.try_get::<i64>("", "rows").is_ok() && row.try_get::<i64>("", "bytes").is_ok()
            })
            && !self.replaced.swap(true, Ordering::Relaxed)
        {
            let sql = if self.replace_locator {
                "UPDATE pin_management_records SET plan_id=$1 WHERE plan_id=$2 AND kind='action'"
            } else {
                "UPDATE pin_management_records SET binding=binding || $1 WHERE plan_id=$2 AND kind='action'"
            };
            let expansion = if self.replace_locator { "x" } else { " " };
            self.inner
                .execute(Statement::from_sql_and_values(
                    self.get_database_backend(),
                    sql,
                    [
                        expansion.repeat(native::MAX_BYTES + 1).into(),
                        plan.clone().into(),
                    ],
                ))
                .await?;
        }
        self.observe(row.as_slice());
        Ok(row)
    }
    async fn query_all(
        &self,
        statement: Statement,
    ) -> std::result::Result<Vec<QueryResult>, DbErr> {
        let rows = self.inner.query_all(statement).await?;
        self.observe(&rows);
        Ok(rows)
    }
}

#[tokio::test]
async fn index_locator_transfer_is_bounded_and_rechecks_replacement() {
    let fixture = actions::tests::fixture("index-locator-budget").await;
    let applied = actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    use sea_orm::EntityTrait;
    let job = crate::store::entities::pin_job::Entity::find_by_id(
        applied.items[0].jobs[0].job_id.clone(),
    )
    .one(&fixture.db)
    .await
    .unwrap()
    .unwrap();
    for replace_after_probe in [false, true] {
        let tx = fixture.db.begin().await.unwrap();
        let mut recording = RecordingConnection::new(&tx);
        if replace_after_probe {
            recording.replacement = Some(fixture.binding.request.plan_id.clone());
            recording.replace_locator = true;
        } else {
            tx.execute(Statement::from_sql_and_values(
                tx.get_database_backend(),
                "UPDATE pin_management_records SET plan_id=$1 WHERE plan_id=$2 AND kind='action'",
                [
                    "x".repeat(native::MAX_BYTES + 1).into(),
                    fixture.binding.request.plan_id.clone().into(),
                ],
            ))
            .await
            .unwrap();
        }
        assert!(index::actions_for_job(&recording, &job).await.is_err());
        assert_eq!(
            recording.locator_bytes.load(Ordering::Relaxed),
            0,
            "oversized locator must not reach the driver"
        );
        assert_eq!(recording.raw_bytes.load(Ordering::Relaxed), 0);
        if replace_after_probe {
            assert!(recording.replaced.load(Ordering::Relaxed));
        }
        tx.rollback().await.unwrap();
    }
    let restored = index::actions_for_job(&fixture.db, &job).await.unwrap();
    assert_eq!(restored.len(), 1);
    assert!(restored[0].action_view() == &applied.items[0]);
}

#[tokio::test]
async fn index_locator_cells_overlap_the_complete_row_without_double_charging() {
    let fixture = actions::tests::fixture("index-locator-overlap").await;
    let applied = actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let source = Evidence::load(
        &fixture.db,
        &fixture.binding.database_scope,
        &fixture.binding.database_identity,
        std::slice::from_ref(&fixture.binding.request.plan_id),
    )
    .await
    .unwrap();
    let key = anchor_key(&fixture.binding).unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let frame = native::capture(&tx).await.unwrap();
    let raw_bytes = frame
        .tables
        .iter()
        .flat_map(|table| table.rows.iter().map(move |row| (table, row)))
        .map(|(table, row)| {
            let keys = table
                .primary_key
                .iter()
                .map(|column| native::value(row, column).unwrap())
                .collect::<Vec<_>>();
            frame
                .original_cells(&table.table, &keys)
                .unwrap()
                .filter_map(|(_, value)| value)
                .map(String::len)
                .sum::<usize>()
        })
        .sum::<usize>();
    let padding = native::MAX_BYTES - raw_bytes;
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE pin_management_records SET result=result || $1 WHERE plan_id=$2 AND kind='action'",
        [
            " ".repeat(padding).into(),
            fixture.binding.request.plan_id.clone().into(),
        ],
    ))
    .await
    .unwrap();
    let restored = index::action_at_key(&tx, &key).await.unwrap().unwrap();
    assert!(restored.action_view() == &applied.items[0]);
    let loaded = Evidence::load_indexed(
        &tx,
        &fixture.binding.database_scope,
        &fixture.binding.database_identity,
        std::slice::from_ref(&key),
        1,
        fixture.binding.database_scope.len(),
        Arc::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        loaded.bytes,
        source.bytes + padding + fixture.binding.database_scope.len()
    );
    assert_eq!(loaded.rows.len(), 1);
    assert!(loaded.locators.is_empty());
    assert_eq!(
        loaded
            .row(&key)
            .unwrap()
            .values()
            .flatten()
            .map(String::len)
            .sum::<usize>()
            + fixture.binding.database_scope.len(),
        loaded.bytes
    );
    tx.rollback().await.unwrap();
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
async fn lookup_rejects_large_original_cells_before_driver_transfer() {
    let fixture = actions::tests::fixture("lookup-raw-budget").await;
    actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let tx = fixture.db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE pin_management_records SET result=$1 WHERE plan_id=$2 AND kind='action'",
        [
            "x".repeat(native::MAX_BYTES + 1).into(),
            fixture.binding.request.plan_id.clone().into(),
        ],
    ))
    .await
    .unwrap();
    let recording = RecordingConnection::new(&tx);
    assert!(matches!(
        lookup_applied(
            &recording,
            &fixture.auth,
            &fixture.binding,
            &fixture.binding.request.items[0].item_id
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
async fn retirement_candidates_and_dependencies_are_charged_before_transfer() {
    let fixture = actions::tests::fixture("retirement-raw-budget").await;
    let route = fixture.config.pinning.providers[0].route_snapshot();
    let request = management::PlanRequest {
        plan_id: "retirement-raw-candidate".into(),
        items: vec![management::ItemRequest {
            item_id: "provider".into(),
            action: management::Action::Retire,
            subject: management::Subject::Provider {
                route: management::RouteIdentity::from_original(route),
            },
        }],
    };
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
        vec![],
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let frame = native::capture(&fixture.db).await.unwrap();
    let source = Evidence::from_frame(&frame).unwrap();
    let action = source.row(&anchor_key(&binding).unwrap()).unwrap().clone();
    let public = frame
        .table("pin_management_retirements")
        .unwrap()
        .rows
        .first()
        .unwrap();
    let mut retirement = frame
        .original_row(
            "pin_management_retirements",
            &[native::value(public, "allocation_domain").unwrap()],
        )
        .unwrap();
    let original_retirement = retirement.clone();
    retirement.insert(
        "inventory".into(),
        Some(format!(
            "{}{}",
            native::value(&retirement, "inventory").unwrap(),
            " ".repeat(native::MAX_BYTES / 2)
        )),
    );
    let tx = fixture.db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_records SET binding=binding || $1 WHERE plan_id=$2 AND kind='action'",[" ".repeat(native::MAX_BYTES/2).into(),fixture.binding.request.plan_id.clone().into()])).await.unwrap();
    let dependency = Evidence::load(
        &tx,
        &binding.database_scope,
        &binding.database_identity,
        std::slice::from_ref(&fixture.binding.request.plan_id),
    )
    .await
    .unwrap();
    let candidate_bytes = retirement
        .values()
        .chain(action.values())
        .flatten()
        .map(String::len)
        .sum::<usize>();
    assert!(candidate_bytes < native::MAX_BYTES);
    assert!(dependency.bytes < native::MAX_BYTES);
    assert!(candidate_bytes + dependency.bytes > native::MAX_BYTES);
    let recording = RecordingConnection::new(&tx);
    assert!(matches!(
        retirement::verify_retirement(
            &recording,
            &retirement,
            &action,
            &[(retirement.clone(), action.clone())]
        )
        .await,
        Err(ManagementError::Budget)
    ));
    assert_eq!(
        recording.raw_bytes.load(Ordering::Relaxed),
        0,
        "combined raw cells must be rejected before dependency transfer"
    );
    // A known exact-cell target is neither transferred nor charged twice.
    let known = dependency
        .row(&anchor_key(&fixture.binding).unwrap())
        .unwrap()
        .clone();
    let mut seeded = Evidence::load_roots(
        &tx,
        &binding.database_scope,
        &binding.database_identity,
        &[],
        &[
            (original_retirement.clone(), action.clone()),
            (original_retirement, known),
        ],
    )
    .await
    .unwrap();
    let charged = seeded.bytes;
    seeded
        .load_dependencies(&recording, std::slice::from_ref(&binding.request.plan_id))
        .await
        .unwrap();
    assert_eq!(seeded.bytes, charged);
    assert_eq!(recording.raw_bytes.load(Ordering::Relaxed), 0);
    tx.rollback().await.unwrap();
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
        Err(ManagementError::Changed | ManagementError::Budget)
    ));
    assert!(recording.replaced.load(Ordering::Relaxed));
    assert_eq!(recording.raw_bytes.load(Ordering::Relaxed), 0);
    tx.rollback().await.unwrap();
}

use super::*;
use native::meter::Limits;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use tokio::{runtime::Runtime, sync::oneshot};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Entered,
    Classifying,
    Classified,
    Finished(Option<ManagementError>),
    Joined,
}

type Hook = Arc<dyn Fn(Phase) + Send + Sync>;

tokio::task_local! {
    pub(super) static HOOK: Hook;
}

pub(super) fn at(hook: &Option<Hook>, phase: Phase) {
    if let Some(hook) = hook {
        hook(phase);
    }
}

fn source() -> (NativeKey, NativeRow) {
    (
        NativeKey {
            table: "pin_management_records".into(),
            values: vec![
                "scope".into(),
                "plan".into(),
                "object".into(),
                "action".into(),
            ],
        },
        [
            ("database_scope", "scope"),
            ("plan_id", "plan"),
            ("kind", "database_scope"),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), Some(value.into())))
        .collect(),
    )
}

fn budget() -> Budget {
    Budget::new(Limits {
        rows: 1,
        bytes: 1,
        sql: 1,
        resident: 1,
        ..Limits::default()
    })
}

fn assert_resident_charged(budget: &Budget) {
    assert!(budget.remaining_budget().unwrap().reserve(1).is_err());
}

fn queued_runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
}

struct OccupiedPool {
    release: mpsc::Sender<()>,
    task: JoinHandle<()>,
}

impl OccupiedPool {
    async fn new() -> Self {
        let (release, held) = mpsc::channel();
        let (started, ready) = oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = held.recv();
        });
        ready.await.unwrap();
        Self { release, task }
    }

    async fn release(self) {
        self.release.send(()).unwrap();
        self.task.await.unwrap();
    }
}

fn gate_at(phase: Phase) -> (Hook, oneshot::Receiver<()>, mpsc::Sender<()>) {
    let (arrived, ready) = oneshot::channel();
    let arrived = Mutex::new(Some(arrived));
    let (release, held) = mpsc::channel();
    let held = Mutex::new(held);
    let hook = Arc::new(move |current| {
        if current == phase {
            arrived.lock().unwrap().take().unwrap().send(()).unwrap();
            let _ = held.lock().unwrap().recv();
        }
    });
    (hook, ready, release)
}

#[tokio::test]
async fn classification_moves_rows_and_retains_guards_without_spending_remaining_work() {
    let mut budget = budget();
    budget.charge(1, 1, 1, 1).unwrap();
    let resident = vec![budget.reserve(1).unwrap()];
    let (key, row) = source();
    let key_allocation = key.values.as_ptr() as usize;
    let row_allocation = row["plan_id"].as_ref().unwrap().as_ptr() as usize;
    let (key, row, resident, classification) =
        classify_row(key, row, resident, &budget).await.unwrap();
    assert_eq!(key.values.as_ptr() as usize, key_allocation);
    assert_eq!(
        row["plan_id"].as_ref().unwrap().as_ptr() as usize,
        row_allocation
    );
    assert_eq!(
        classification.selectors,
        BTreeSet::from([AuthoritySelector::RecordMembers(
            "scope".into(),
            "plan".into()
        )])
    );
    assert_eq!(
        (budget.work.sql, budget.work.rows, budget.work.raw_bytes),
        (1, 1, 1)
    );
    assert_resident_charged(&budget);
    drop(resident);
    assert!(budget.remaining_budget().unwrap().reserve(1).is_ok());
    assert!(matches!(
        classify_row(key, NativeRow::new(), vec![], &budget).await,
        Err(ManagementError::InvalidEvidence)
    ));
}

#[tokio::test]
async fn stopped_budget_prevents_dispatch() {
    for stop in ["work", "cancel", "deadline"] {
        let mut budget = budget();
        let resident = vec![budget.reserve(1).unwrap()];
        match stop {
            "work" => assert!(budget.charge(2, 0, 0, 0).is_err()),
            "cancel" => budget.cancellation().store(true, Ordering::Relaxed),
            _ => assert!(
                budget
                    .restrict_remaining(1, 1, std::time::Duration::ZERO)
                    .is_err()
            ),
        }
        let (key, row) = source();
        let hook: Hook = Arc::new(|_| panic!("stopped work must not dispatch"));
        assert!(matches!(
            HOOK.scope(hook, classify_row(key, row, resident, &budget))
                .await,
            Err(ManagementError::Budget)
        ));
    }
}

#[test]
fn cancellation_while_queued_is_checked_before_classification() {
    queued_runtime().block_on(async {
        let occupied = OccupiedPool::new().await;
        let mut budget = budget();
        let resident = vec![budget.reserve(1).unwrap()];
        let classified = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let hook: Hook = {
            let classified = classified.clone();
            let stopped = stopped.clone();
            Arc::new(move |phase| match phase {
                Phase::Classifying => classified.store(true, Ordering::Relaxed),
                Phase::Finished(Some(ManagementError::Budget)) => {
                    stopped.store(true, Ordering::Relaxed)
                }
                _ => {}
            })
        };
        let (key, row) = source();
        let mut pending = Box::pin(HOOK.scope(hook, classify_row(key, row, resident, &budget)));
        assert!(futures_util::poll!(pending.as_mut()).is_pending());
        budget.cancellation().store(true, Ordering::Relaxed);
        occupied.release().await;
        assert!(matches!(pending.await, Err(ManagementError::Budget)));
        assert!(!classified.load(Ordering::Relaxed));
        assert!(stopped.load(Ordering::Relaxed));
    });
}

#[test]
fn dropping_queued_classification_aborts_it_and_releases_its_guards() {
    queued_runtime().block_on(async {
        let occupied = OccupiedPool::new().await;
        let mut budget = budget();
        let resident = vec![budget.reserve(1).unwrap()];
        let entered = Arc::new(AtomicBool::new(false));
        let hook: Hook = {
            let entered = entered.clone();
            Arc::new(move |_| entered.store(true, Ordering::Relaxed))
        };
        let (key, row) = source();
        let mut pending = Box::pin(HOOK.scope(hook, classify_row(key, row, resident, &budget)));
        assert!(futures_util::poll!(pending.as_mut()).is_pending());
        assert_resident_charged(&budget);
        drop(pending);
        occupied.release().await;
        tokio::task::spawn_blocking(|| ()).await.unwrap();
        assert!(!entered.load(Ordering::Relaxed));
        assert!(budget.remaining_budget().unwrap().reserve(1).is_ok());
    });
}

#[tokio::test]
async fn cancellation_during_running_classification_is_checked_at_exit() {
    let mut budget = budget();
    let resident = vec![budget.reserve(1).unwrap()];
    let cancel = budget.cancellation();
    let stopped = Arc::new(AtomicBool::new(false));
    let hook: Hook = {
        let stopped = stopped.clone();
        Arc::new(move |phase| match phase {
            Phase::Classified => cancel.store(true, Ordering::Relaxed),
            Phase::Finished(Some(ManagementError::Budget)) => {
                stopped.store(true, Ordering::Relaxed)
            }
            _ => {}
        })
    };
    let (key, row) = source();
    assert!(matches!(
        HOOK.scope(hook, classify_row(key, row, resident, &budget))
            .await,
        Err(ManagementError::Budget)
    ));
    assert!(stopped.load(Ordering::Relaxed));
}

#[test]
fn dropping_running_classification_retains_guards_until_cpu_work_finishes() {
    queued_runtime().block_on(async {
        let mut budget = budget();
        let resident = vec![budget.reserve(1).unwrap()];
        let observer = budget.remaining_budget().unwrap();
        let (hook, ready, release) = gate_at(Phase::Classifying);
        let continued = Arc::new(AtomicBool::new(false));
        let (key, row) = source();
        let caller = tokio::spawn(HOOK.scope(hook, {
            let continued = continued.clone();
            async move {
                classify_row(key, row, resident, &budget).await?;
                continued.store(true, Ordering::Relaxed);
                Ok::<_, ManagementError>(())
            }
        }));
        ready.await.unwrap();
        assert_resident_charged(&observer);
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_resident_charged(&observer);
        release.send(()).unwrap();
        tokio::task::spawn_blocking(|| ()).await.unwrap();
        assert!(observer.remaining_budget().unwrap().reserve(1).is_ok());
        assert!(!continued.load(Ordering::Relaxed));
    });
}

#[tokio::test]
async fn post_join_cancellation_precedes_errors_and_prevents_database_continuation() {
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement};
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("CREATE TABLE continuation (value INTEGER)")
        .await
        .unwrap();
    for (panic, cancel_after_join) in [(false, true), (true, true), (true, false)] {
        let budget = budget();
        let cancel = budget.cancellation();
        let hook: Hook = Arc::new(move |phase| match phase {
            Phase::Classifying if panic => panic!("classification task failed"),
            Phase::Joined if cancel_after_join => cancel.store(true, Ordering::Relaxed),
            _ => {}
        });
        let (key, _) = source();
        let result = HOOK
            .scope(hook, async {
                classify_row(key, NativeRow::new(), vec![], &budget).await?;
                db.execute_unprepared("INSERT INTO continuation VALUES (1)")
                    .await
                    .map_err(|_| ManagementError::Database)?;
                Ok::<_, ManagementError>(())
            })
            .await;
        assert_eq!(
            result,
            Err(if cancel_after_join {
                ManagementError::Budget
            } else {
                ManagementError::InvalidEvidence
            })
        );
    }
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT count(*) AS n FROM continuation",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
}

use super::*;
mod sealed;

enum FailedWrite {
    Error,
    Zero,
}

impl Write for FailedWrite {
    fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Error => Err(std::io::Error::other("test write failure")),
            Self::Zero => Ok(0),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn failed_initial_write_keeps_page_payment(output: FailedWrite) {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
    let (union_path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&union_path, file).await.unwrap();
    let mut limits = RunLimits::default();
    limits.total_rows = 100;
    limits.total_bytes = 1024;
    spool::task_run(&spools[0].db, &limits).await.unwrap();
    union.start_task(&limits).await.unwrap();
    let task = task_budget::TaskBudget::new(&spools, &union, (100, 1024));
    let pages_before = limits.pages_remaining();
    let pages = Arc::new(PageMeter::default());
    let budget = match TaskOutput::start(
        Some(&task),
        PageOutput::new(output, &limits, pages.clone()),
        Projection::Review,
        Budget::default(),
        true,
        &limits,
        Some(pages),
    )
    .await
    {
        Err((_, budget)) => budget,
        Ok(_) => panic!("initial write must fail"),
    };
    let pages_remaining = limits.pages_remaining();
    let paid = task.total().await.unwrap();
    drop(task);
    spools[0].close().await.unwrap();
    union.close().await.unwrap();
    drop(spools);
    drop(union);
    workspace.cleanup().unwrap();
    assert_eq!(budget.work.output_bytes, 0);
    assert_eq!(
        (pages_remaining, budget.work.pages, paid.pages),
        (pages_before - 1, 1, 1),
    );
}

#[tokio::test]
async fn initial_write_error_keeps_run_budget_and_task_pages_consistent() {
    failed_initial_write_keeps_page_payment(FailedWrite::Error).await;
}

#[tokio::test]
async fn initial_write_zero_keeps_run_budget_and_task_pages_consistent() {
    failed_initial_write_keeps_page_payment(FailedWrite::Zero).await;
}

#[test]
fn known_eof_does_not_consume_another_page() {
    let limits = RunLimits::default();
    let pages = Arc::new(PageMeter::default());
    let pages_before = limits.pages_remaining();
    let mut input = PageOutput::reader(std::io::empty(), &limits, pages.clone(), 0);
    let mut bytes = [0];
    assert_eq!(input.read(&mut bytes).unwrap(), 0);
    assert_eq!(input.read(&mut bytes).unwrap(), 0);
    assert_eq!(limits.pages_remaining(), pages_before);
    assert_eq!(pages.used.load(Ordering::Relaxed), 0);
}

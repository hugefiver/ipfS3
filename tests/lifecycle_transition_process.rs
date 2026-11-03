#[path = "support/lifecycle_transition_process.rs"]
mod lifecycle_transition_process;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "NOT RUN by default: requires PostgreSQL 17 or 18 and two independent, healthy Kubo RPC endpoints"]
async fn lifecycle_transition_process_crash_restart_f1() {
    lifecycle_transition_process::run().await;
}

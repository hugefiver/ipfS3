// Included by the binary's shutdown module, not a second integration target.
use super::*;

#[tokio::test(start_paused = true)]
async fn http_and_workers_share_one_budget() {
    let cancel = CancellationToken::new();
    let server_cancel = cancel.clone();
    let worker_cancel = cancel.clone();
    let started = tokio::time::Instant::now();
    drain(
        async move {
            server_cancel.cancelled().await;
            tokio::time::sleep(Duration::from_secs(20)).await;
            Ok(())
        },
        async { Ok(()) },
        cancel,
        async move {
            assert!(worker_cancel.is_cancelled());
            tokio::time::sleep(Duration::from_secs(20)).await;
        },
        EXIT_BUDGET,
    )
    .await
    .unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(20));
}

#[tokio::test(start_paused = true)]
async fn stalled_http_or_worker_hits_the_same_deadline() {
    for stalled_http in [true, false] {
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let started = tokio::time::Instant::now();
        let error = drain(
            async move {
                server_cancel.cancelled().await;
                if stalled_http {
                    std::future::pending::<()>().await;
                }
                Ok(())
            },
            async { Ok(()) },
            cancel,
            async move {
                if !stalled_http {
                    std::future::pending::<()>().await;
                }
            },
            EXIT_BUDGET,
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(started.elapsed(), EXIT_BUDGET);
    }
}

#[tokio::test]
async fn server_failure_also_cancels_and_joins_workers() {
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let joined = std::sync::atomic::AtomicBool::new(false);
    let error = drain(
        async { Err(io::Error::other("accept failed")) },
        std::future::pending(),
        cancel,
        async {
            assert!(worker_cancel.is_cancelled());
            joined.store(true, std::sync::atomic::Ordering::SeqCst);
        },
        EXIT_BUDGET,
    )
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), "accept failed");
    assert!(joined.load(std::sync::atomic::Ordering::SeqCst));
}

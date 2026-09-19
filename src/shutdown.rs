//! Process-owned cancellation, separate from request-owned mutation lease renewal.
use std::{future::Future, io, time::Duration};

use tokio_util::sync::CancellationToken;

/// One deadline for HTTP drain AND workers, not successive per-phase budgets.
/// Compose allows 40s so the application can enforce its own 30s deadline first.
pub const EXIT_BUDGET: Duration = Duration::from_secs(30);
pub const WORKER_GRACE: Duration = Duration::from_secs(25);

pub struct ShutdownSignal {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignal {
    /// Install Unix handlers before accepting requests or starting workers.
    pub fn install() -> io::Result<Self> {
        Ok(Self {
            #[cfg(unix)]
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            #[cfg(unix)]
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    pub async fn wait(mut self) -> io::Result<()> {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.interrupt.recv() => {},
                _ = self.terminate.recv() => {},
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = &mut self;
            tokio::signal::ctrl_c().await
        }
    }
}

/// Stop accepting/claiming immediately, then drain HTTP and workers concurrently.
/// Active HTTP operations retain their own MutationLease renewers until finished;
/// they must NOT be children of this process cancellation token.
pub async fn drain(
    server: impl Future<Output = io::Result<()>>,
    signal: impl Future<Output = io::Result<()>>,
    cancellation: CancellationToken,
    workers: impl Future<Output = ()>,
    budget: Duration,
) -> io::Result<()> {
    tokio::pin!(server);
    let mut signal_error = None;
    let server_result = tokio::select! {
        result = &mut server => Some(result),
        result = signal => {
            signal_error = result.err();
            None
        },
    };
    cancellation.cancel();
    tracing::info!(
        budget_secs = budget.as_secs(),
        "shutdown requested; draining HTTP and workers"
    );

    tokio::time::timeout(budget, async {
        let (result, ()) = tokio::join!(
            async {
                match server_result {
                    Some(result) => result,
                    None => server.await,
                }
            },
            workers,
        );
        result?;
        if let Some(error) = signal_error {
            return Err(error);
        }
        tracing::info!("shutdown drain complete");
        Ok(())
    })
    .await
    .unwrap_or_else(|_| {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "shutdown deadline exceeded",
        ))
    })
}

#[cfg(test)]
#[path = "../tests/shutdown/budget.rs"]
mod tests;

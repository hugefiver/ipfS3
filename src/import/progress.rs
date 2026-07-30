use std::{sync::Arc, time::Duration};

use chrono::Utc;
use sea_orm::TransactionTrait;
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    error::AppResult,
    import::{
        ImportClaim, ImportExecutionError, ImportPhase, ImportProgress, ImportState,
        execution_error::cancellation_error,
        pipeline::{AbortOnDropTask, JobCancellation},
    },
    kubo::KuboProgress,
    state::AppState,
    store::{entities::import_job, import::jobs},
};

enum ProgressCommand {
    Phase(ImportPhase, oneshot::Sender<Result<(), ()>>),
    DownloadTotal(Option<u64>),
    LogicalSize(u64),
    Extraction(ExtractionProgress),
    Finish(oneshot::Sender<Result<(), ()>>),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ExtractionProgress {
    pub entries_processed: u64,
    pub entries_succeeded: u64,
    pub entries_failed: u64,
    pub decompressed_bytes: u64,
}

pub(crate) struct ProgressReporter {
    commands: mpsc::Sender<ProgressCommand>,
    pub(crate) kubo: mpsc::Sender<KuboProgress>,
    pub(crate) download: watch::Sender<u64>,
    task: AbortOnDropTask,
}

impl ProgressReporter {
    pub(crate) fn start(
        state: Arc<AppState>,
        job: &import_job::Model,
        claim: ImportClaim,
        cancellation: JobCancellation,
        flush_interval: Duration,
    ) -> Self {
        let (commands, command_rx) = mpsc::channel(8);
        let (kubo, kubo_rx) = mpsc::channel(32);
        let (download, download_rx) = watch::channel(nonnegative_i64(job.downloaded_bytes));
        let progress = progress_from_job(job);
        let task = AbortOnDropTask::new(tokio::spawn(run_progress_consumer(
            state,
            claim,
            cancellation,
            flush_interval,
            progress,
            command_rx,
            kubo_rx,
            download_rx,
        )));
        Self {
            commands,
            kubo,
            download,
            task,
        }
    }

    pub(crate) async fn phase(
        &self,
        phase: ImportPhase,
        cancellation: &JobCancellation,
    ) -> Result<(), ImportExecutionError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        send_progress_command(
            &self.commands,
            ProgressCommand::Phase(phase, ack_tx),
            cancellation,
        )
        .await?;
        await_progress_ack(ack_rx, cancellation).await
    }

    pub(crate) async fn download_total(
        &self,
        total: Option<u64>,
        cancellation: &JobCancellation,
    ) -> Result<(), ImportExecutionError> {
        send_progress_command(
            &self.commands,
            ProgressCommand::DownloadTotal(total),
            cancellation,
        )
        .await
    }

    pub(crate) async fn logical_size(
        &self,
        size: u64,
        cancellation: &JobCancellation,
    ) -> Result<(), ImportExecutionError> {
        send_progress_command(
            &self.commands,
            ProgressCommand::LogicalSize(size),
            cancellation,
        )
        .await
    }

    pub(crate) async fn extraction(
        &self,
        extraction: ExtractionProgress,
        cancellation: &JobCancellation,
    ) -> Result<(), ImportExecutionError> {
        send_progress_command(
            &self.commands,
            ProgressCommand::Extraction(extraction),
            cancellation,
        )
        .await
    }

    pub(crate) async fn finish(
        self,
        cancellation: &JobCancellation,
    ) -> Result<(), ImportExecutionError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let sent = send_progress_command(
            &self.commands,
            ProgressCommand::Finish(ack_tx),
            cancellation,
        )
        .await;
        let acknowledged = match sent {
            Ok(()) => await_progress_ack(ack_rx, cancellation).await,
            Err(error) => Err(error),
        };
        drop(self.commands);
        drop(self.kubo);
        drop(self.download);
        self.task.join().await;
        acknowledged
    }
}

async fn send_progress_command(
    commands: &mpsc::Sender<ProgressCommand>,
    command: ProgressCommand,
    cancellation: &JobCancellation,
) -> Result<(), ImportExecutionError> {
    tokio::select! {
        biased;
        _ = cancellation.shutdown.cancelled() => Err(ImportExecutionError::Interrupted),
        _ = cancellation.ownership_lost.cancelled() => Err(ImportExecutionError::Superseded),
        result = commands.send(command) => result.map_err(|_| cancellation_error(cancellation)),
    }
}

async fn await_progress_ack(
    ack: oneshot::Receiver<Result<(), ()>>,
    cancellation: &JobCancellation,
) -> Result<(), ImportExecutionError> {
    tokio::select! {
        biased;
        _ = cancellation.shutdown.cancelled() => Err(ImportExecutionError::Interrupted),
        _ = cancellation.ownership_lost.cancelled() => Err(ImportExecutionError::Superseded),
        result = ack => match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(())) | Err(_) => Err(cancellation_error(cancellation)),
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_progress_consumer(
    state: Arc<AppState>,
    claim: ImportClaim,
    cancellation: JobCancellation,
    flush_interval: Duration,
    mut progress: ImportProgress,
    mut commands: mpsc::Receiver<ProgressCommand>,
    mut kubo: mpsc::Receiver<KuboProgress>,
    mut download: watch::Receiver<u64>,
) {
    let mut dirty = false;
    let mut providers_this_attempt = 0_u32;
    let mut interval = tokio::time::interval(flush_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = cancellation.shutdown.cancelled() => break,
            _ = cancellation.ownership_lost.cancelled() => break,
            command = commands.recv() => {
                let Some(command) = command else { break };
                match command {
                    ProgressCommand::Phase(phase, ack) => {
                        drain_progress(
                            &mut kubo,
                            &download,
                            &mut progress,
                            &mut providers_this_attempt,
                            &mut dirty,
                        );
                        let result = match flush_progress(&state, &claim, &progress, &mut dirty).await {
                            Ok(()) => update_phase(&state, &claim, phase).await,
                            Err(error) => Err(error),
                        };
                        if result.is_err() {
                            cancellation.ownership_lost.cancel();
                            let _ = ack.send(Err(()));
                            break;
                        }
                        let _ = ack.send(Ok(()));
                    }
                    ProgressCommand::DownloadTotal(total) => {
                        if let Some(total) = total {
                            progress.download_total = Some(progress.download_total.unwrap_or(0).max(total));
                            dirty = true;
                        }
                    }
                    ProgressCommand::LogicalSize(size) => {
                        progress.logical_size = Some(progress.logical_size.unwrap_or(0).max(size));
                        dirty = true;
                    }
                    ProgressCommand::Extraction(extraction) => {
                        progress.entries_processed = progress
                            .entries_processed
                            .max(extraction.entries_processed);
                        progress.entries_succeeded = progress
                            .entries_succeeded
                            .max(extraction.entries_succeeded);
                        progress.entries_failed = progress
                            .entries_failed
                            .max(extraction.entries_failed);
                        progress.decompressed_bytes = progress
                            .decompressed_bytes
                            .max(extraction.decompressed_bytes);
                        dirty = true;
                    }
                    ProgressCommand::Finish(ack) => {
                        drain_progress(
                            &mut kubo,
                            &download,
                            &mut progress,
                            &mut providers_this_attempt,
                            &mut dirty,
                        );
                        let result = flush_progress(&state, &claim, &progress, &mut dirty).await;
                        if result.is_err() {
                            cancellation.ownership_lost.cancel();
                        }
                        let _ = ack.send(result.map_err(|_| ()));
                        break;
                    }
                }
            }
            event = kubo.recv() => {
                if let Some(event) = event {
                    apply_kubo_progress(&mut progress, &mut providers_this_attempt, event);
                    dirty = true;
                }
            }
            changed = download.changed() => {
                if changed.is_ok() {
                    progress.downloaded_bytes = progress.downloaded_bytes.max(*download.borrow_and_update());
                    dirty = true;
                }
            }
            _ = interval.tick() => {
                drain_progress(
                    &mut kubo,
                    &download,
                    &mut progress,
                    &mut providers_this_attempt,
                    &mut dirty,
                );
                if flush_progress(&state, &claim, &progress, &mut dirty).await.is_err() {
                    cancellation.ownership_lost.cancel();
                    break;
                }
            }
        }
    }
}

async fn update_phase(state: &AppState, claim: &ImportClaim, phase: ImportPhase) -> AppResult<()> {
    let transaction = state.store.db().begin().await?;
    let result = jobs::update_phase(
        &transaction,
        &claim.job_id,
        &claim.worker_id,
        claim.claim_epoch,
        ImportState::Running,
        phase,
        Utc::now(),
    )
    .await;
    match result {
        Ok(()) => {
            transaction.commit().await?;
            Ok(())
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn flush_progress(
    state: &AppState,
    claim: &ImportClaim,
    progress: &ImportProgress,
    dirty: &mut bool,
) -> AppResult<()> {
    if !*dirty {
        return Ok(());
    }
    let transaction = state.store.db().begin().await?;
    let result = jobs::update_progress(
        &transaction,
        &claim.job_id,
        &claim.worker_id,
        claim.claim_epoch,
        claim.attempt,
        progress,
        Utc::now(),
    )
    .await;
    match result {
        Ok(()) => transaction.commit().await?,
        Err(error) => {
            let _ = transaction.rollback().await;
            return Err(error);
        }
    }
    *dirty = false;
    Ok(())
}

fn drain_progress(
    kubo: &mut mpsc::Receiver<KuboProgress>,
    download: &watch::Receiver<u64>,
    progress: &mut ImportProgress,
    providers_this_attempt: &mut u32,
    dirty: &mut bool,
) {
    while let Ok(event) = kubo.try_recv() {
        apply_kubo_progress(progress, providers_this_attempt, event);
        *dirty = true;
    }
    let downloaded = *download.borrow();
    if downloaded > progress.downloaded_bytes {
        progress.downloaded_bytes = downloaded;
        *dirty = true;
    }
}

pub(crate) fn apply_kubo_progress(
    progress: &mut ImportProgress,
    providers_this_attempt: &mut u32,
    event: KuboProgress,
) {
    match event {
        KuboProgress::ProviderObserved { .. } => {
            *providers_this_attempt = providers_this_attempt.saturating_add(1);
            progress.providers_observed = progress.providers_observed.max(*providers_this_attempt);
        }
        KuboProgress::AddBytes { bytes } => {
            progress.ipfs_add_bytes = progress.ipfs_add_bytes.max(bytes);
        }
        KuboProgress::PinProgress { nodes, bytes } => {
            progress.pin_nodes_processed = progress.pin_nodes_processed.max(nodes);
            progress.pin_bytes_processed = progress.pin_bytes_processed.max(bytes);
        }
    }
}

fn progress_from_job(job: &import_job::Model) -> ImportProgress {
    ImportProgress {
        providers_observed: u32::try_from(job.providers_observed).unwrap_or(0),
        pin_nodes_processed: nonnegative_i64(job.pin_nodes_processed),
        pin_bytes_processed: nonnegative_i64(job.pin_bytes_processed),
        downloaded_bytes: nonnegative_i64(job.downloaded_bytes),
        download_total: job.download_total.map(nonnegative_i64),
        ipfs_add_bytes: nonnegative_i64(job.ipfs_add_bytes),
        logical_size: job.logical_size.map(nonnegative_i64),
        entries_processed: nonnegative_i64(job.entries_processed),
        entries_succeeded: nonnegative_i64(job.entries_succeeded),
        entries_failed: nonnegative_i64(job.entries_failed),
        decompressed_bytes: nonnegative_i64(job.decompressed_bytes),
    }
}

fn nonnegative_i64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}
